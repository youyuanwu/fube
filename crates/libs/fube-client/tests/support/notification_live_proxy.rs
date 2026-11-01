//! Test-only clear-frame relay that loses one successful ACK, not native data.
//! No packet bodies are persisted or used as fixtures.
use fube_transport::{
    connection::{read_message, write_message},
    message::MessageId,
    serialization::Decoder,
    Error, Result,
};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};

type PageKey = ([u8; 16], u64, u64);
#[derive(Default)]
struct State {
    pages: HashMap<PageKey, usize>,
    requests: HashMap<MessageId, PageKey>,
    target: Option<PageKey>,
    dropped: usize,
    retries: usize,
    forwarded: usize,
    dropped_id: Option<MessageId>,
    retry_id: Option<MessageId>,
    forwarded_id: Option<MessageId>,
}

#[derive(Clone, Serialize)]
pub struct Attempt {
    pub request_id: MessageId,
    pub page_id: PageKey,
}

#[derive(Serialize)]
pub struct Evidence {
    pub successful_acks_dropped: usize,
    pub native_page_retries: usize,
    pub subsequent_target_acks_forwarded: usize,
    pub dropped_attempt: Option<Attempt>,
    pub retry_attempt: Option<Attempt>,
    pub forwarded_success_ack: Option<Attempt>,
}

impl State {
    fn page(&mut self, id: MessageId, key: PageKey, entries: u32) -> Result<()> {
        if id == MessageId::default() || self.requests.contains_key(&id) {
            return Err(Error::Protocol(
                "missing or colliding native relay request ID",
            ));
        }
        if self.requests.len() >= 512 || self.pages.len() >= 512 {
            return Err(Error::ResourceLimit("qualification proxy records"));
        }
        *self.pages.entry(key).or_default() += 1;
        self.requests.insert(id, key);
        if entries > 0 && self.target.is_none() {
            self.target = Some(key);
        }
        if Some(key) == self.target && self.dropped_id.is_some() {
            self.retries += 1;
            if self.retry_id.is_none() {
                self.retry_id = Some(id);
            }
        }
        Ok(())
    }

    fn ack(&mut self, id: MessageId, success: bool) -> (bool, bool) {
        if let Some(key) = self.requests.remove(&id) {
            if Some(key) == self.target && success {
                if self.dropped == 0 {
                    self.dropped = 1;
                    self.dropped_id = Some(id);
                    return (true, false);
                }
                if Some(id) == self.retry_id {
                    return (false, true);
                }
            }
        }
        (false, false)
    }

    fn forwarded(&mut self, id: MessageId) -> Result<()> {
        if Some(id) != self.retry_id {
            return Err(Error::Protocol(
                "native relay forwarded ACK witness mismatch",
            ));
        }
        self.forwarded += 1;
        self.forwarded_id = Some(id);
        Ok(())
    }

    fn evidence(&self) -> Evidence {
        let attempt = |id: Option<MessageId>| {
            id.zip(self.target).map(|(request_id, page_id)| Attempt {
                request_id,
                page_id,
            })
        };
        Evidence {
            successful_acks_dropped: self.dropped,
            native_page_retries: self.retries,
            subsequent_target_acks_forwarded: self.forwarded,
            dropped_attempt: attempt(self.dropped_id),
            retry_attempt: attempt(self.retry_id),
            forwarded_success_ack: attempt(self.forwarded_id),
        }
    }
}
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Notify,
}
pub struct LossProxy {
    pub address: SocketAddr,
    shared: Arc<Shared>,
    task: JoinHandle<Result<()>>,
}
impl LossProxy {
    pub async fn start(endpoint: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shared = Arc::new(Shared::default());
        let relay = shared.clone();
        let task = tokio::spawn(async move {
            let (client, _) = listener.accept().await?;
            let gateway = TcpStream::connect(endpoint).await?;
            client.set_nodelay(true)?;
            gateway.set_nodelay(true)?;
            let (mut client_read, mut client_write) = client.into_split();
            let (mut gateway_read, mut gateway_write) = gateway.into_split();
            let server = async {
                loop {
                    let message = read_message(&mut gateway_read).await?;
                    let properties = message.properties()?;
                    if properties.action == "ServiceNotificationRequest"
                        && properties.uncorrelated_reply
                    {
                        let (key, entries) = identity(&message.body)?;
                        {
                            relay
                                .state
                                .lock()
                                .unwrap()
                                .page(properties.id, key, entries)?;
                        }
                        relay.changed.notify_one();
                    }
                    write_message(&mut client_write, &message).await?;
                }
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            };
            let client = async {
                loop {
                    let message = read_message(&mut client_read).await?;
                    let properties = message.properties()?;
                    let mut discard = false;
                    let mut target_retry = false;
                    if properties.action == "ServiceNotificationReply" {
                        let mut d = Decoder::new(&message.body);
                        let success = d.object(|d| d.i64())? == 0;
                        d.finish()?;
                        (discard, target_retry) = relay
                            .state
                            .lock()
                            .unwrap()
                            .ack(properties.relates_to, success);
                        relay.changed.notify_one();
                    }
                    if !discard {
                        write_message(&mut gateway_write, &message).await?;
                        if target_retry {
                            relay
                                .state
                                .lock()
                                .unwrap()
                                .forwarded(properties.relates_to)?;
                            relay.changed.notify_one();
                        }
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            };
            tokio::try_join!(server, client)?;
            Ok(())
        });
        Ok(Self {
            address,
            shared,
            task,
        })
    }
    pub fn evidence(&self) -> Evidence {
        self.shared.state.lock().unwrap().evidence()
    }
    pub async fn changed(&self) {
        self.shared.changed.notified().await;
    }
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
impl Drop for LossProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn identity(body: &[u8]) -> Result<(PageKey, u32)> {
    let mut d = Decoder::new(body);
    d.object(|d| {
        d.pointer(|d, _| {
            let key = d.object(|d| {
                let (guid, index) = d.object(|d| Ok((d.guid()?, d.u64()?)))?;
                Ok((guid, index, d.u64()?))
            })?;
            d.object(|_| Ok(()))?;
            d.pointer(|_, _| Ok(()))?;
            Ok((key, d.u32()?))
        })
    })?
    .ok_or(Error::Protocol("null native proxy page"))
}

#[test]
fn retry_witness_is_the_dropped_targets_fresh_request_and_exact_success_ack() {
    let mut state = State::default();
    let page = ([1; 16], 2, 3);
    let original = MessageId {
        guid: [9; 16],
        index: 10,
    };
    let retry = MessageId {
        index: 11,
        ..original
    };
    state.page(original, page, 1).unwrap();
    assert_eq!(state.ack(original, true), (true, false));
    state
        .page(
            MessageId {
                index: 12,
                ..original
            },
            ([2; 16], 2, 3),
            1,
        )
        .unwrap();
    assert_eq!(state.evidence().native_page_retries, 0);
    state.page(retry, page, 1).unwrap();
    assert_eq!(state.ack(retry, true), (false, true));
    assert_eq!(state.evidence().subsequent_target_acks_forwarded, 0);
    state.forwarded(retry).unwrap();
    let evidence = state.evidence();
    assert_eq!(evidence.successful_acks_dropped, 1);
    assert_eq!(evidence.native_page_retries, 1);
    assert_eq!(evidence.subsequent_target_acks_forwarded, 1);
    let dropped = evidence.dropped_attempt.unwrap();
    let attempt = evidence.retry_attempt.unwrap();
    let ack = evidence.forwarded_success_ack.unwrap();
    assert_ne!(dropped.request_id, attempt.request_id);
    assert_eq!(attempt.request_id, ack.request_id);
    assert_eq!(dropped.page_id, attempt.page_id);
    assert_eq!(attempt.page_id, ack.page_id);
}

#[test]
fn failed_or_unrelated_ack_cannot_prove_target_retry_and_records_remain_bounded() {
    let mut state = State::default();
    let page = ([1; 16], 2, 3);
    let id = MessageId {
        guid: [9; 16],
        index: 1,
    };
    state.page(id, page, 1).unwrap();
    assert_eq!(state.ack(id, false), (false, false));
    assert_eq!(state.evidence().successful_acks_dropped, 0);
    for index in 2..514 {
        state.page(MessageId { index, ..id }, page, 1).unwrap();
    }
    assert!(matches!(
        state.page(MessageId { index: 514, ..id }, page, 1),
        Err(Error::ResourceLimit(_))
    ));
    assert!(matches!(
        state.page(MessageId { index: 2, ..id }, page, 1),
        Err(Error::Protocol(_))
    ));
}
