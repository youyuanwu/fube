//! Test-only clear-frame relay that loses one successful ACK, not native data.
//! No packet bodies are persisted or used as fixtures.
use mssf_transport::{
    connection::{read_message, write_message},
    message::MessageId,
    serialization::Decoder,
    Error, Result,
};
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
                            let mut state = relay.state.lock().unwrap();
                            if state.requests.len() >= 512 || state.pages.len() >= 512 {
                                return Err(Error::ResourceLimit("qualification proxy records"));
                            }
                            let count = state.pages.entry(key).or_default();
                            let retry = *count > 0;
                            *count += 1;
                            if retry {
                                state.retries += 1;
                            }
                            state.requests.insert(properties.id, key);
                            if entries > 0 && state.target.is_none() {
                                state.target = Some(key);
                            }
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
                    if properties.action == "ServiceNotificationReply" {
                        let mut d = Decoder::new(&message.body);
                        let success = d.object(|d| d.i64()).is_ok_and(|code| code == 0);
                        let mut state = relay.state.lock().unwrap();
                        if let Some(key) = state.requests.remove(&properties.relates_to) {
                            if Some(key) == state.target && success {
                                if state.dropped == 0 {
                                    state.dropped = 1;
                                    discard = true;
                                } else {
                                    state.forwarded += 1;
                                }
                            }
                        }
                        relay.changed.notify_one();
                    }
                    if !discard {
                        write_message(&mut gateway_write, &message).await?;
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
    pub fn evidence(&self) -> (usize, usize, usize) {
        let state = self.shared.state.lock().unwrap();
        (state.dropped, state.retries, state.forwarded)
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
