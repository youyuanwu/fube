//! Runtime-owned IPC envelopes, finite handoff and domain completion work.
//!
//! The transport knows neither Hosting/RAP actions nor runtime authority. SF
//! IpcClient.cpp:205-245 and ReceiverContext.cpp:25-39 (pin 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d)
//! distinguish identity-bearing sends from original-context correlated replies.

use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use fube_transport::{
    channel::{MessageChannel, MessageReceiver, ReplyContext},
    connection::MAX_FRAME_TIME,
    message::{Header, Message, MessageId, Properties},
    native_channel::{ChannelLimits, GatewayChannel, GatewayTarget, WritePredicate},
    native_demux::{
        ActorDemux, ActorRegistration, ChannelComposition, OutboundKind, RequestPolicy,
    },
    Error, Result,
};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex, Semaphore},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

#[cfg(test)]
#[path = "channel_adapter_tests.rs"]
mod tests;

type Work = Pin<Box<dyn Future<Output = ()> + Send>>;
type Handoff = Arc<dyn Fn(IpcMessage) -> Result<()> + Send + Sync>;
const OPERATIONS: usize = 32;

pub(crate) struct IpcMessage {
    pub message: Message,
    pub received_at: std::time::Instant,
    pub reply: Option<IpcResponse>,
}

pub(crate) struct IpcResponse {
    context: Box<dyn ReplyContext>,
    actor: i64,
}

fn envelope(actor: i64, action: Option<&str>, body: Vec<u8>) -> Result<Message> {
    let mut headers = vec![Header::actor(actor)?];
    if let Some(action) = action.filter(|action| !action.is_empty()) {
        headers.push(Header::action(action)?);
    }
    Ok(Message { headers, body })
}

impl IpcResponse {
    pub(crate) async fn reply(
        self,
        actor: i64,
        action: Option<&str>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<()> {
        if actor != self.actor {
            return Err(Error::InvalidInput("foreign IPC reply actor"));
        }
        self.context
            .reply(envelope(actor, action, body)?, deadline)
            .await
    }

    pub(crate) async fn reply_fenced(
        self,
        actor: i64,
        body: Vec<u8>,
        deadline: Instant,
        valid: WritePredicate,
    ) -> Result<()> {
        self.reply_with_action_fenced(actor, None, body, deadline, valid)
            .await
    }

    pub(crate) async fn reply_with_action_fenced(
        self,
        actor: i64,
        action: Option<&str>,
        body: Vec<u8>,
        deadline: Instant,
        valid: WritePredicate,
    ) -> Result<()> {
        if actor != self.actor {
            return Err(Error::InvalidInput("foreign IPC reply actor"));
        }
        self.context
            .reply_fenced(envelope(actor, action, body)?, deadline, valid)
            .await
    }
}

fn outbound(_: &Message, p: &Properties, kind: OutboundKind) -> Result<()> {
    let allowed = match kind {
        OutboundKind::Rpc => match p.actor {
            7 => matches!(
                p.action.as_str(),
                "StartRegisterApplicationHostRequest"
                    | "FinishRegisterApplicationHostRequest"
                    | "UnregisterApplicationHostRequest"
                    | "ReportHealth"
            ),
            9 => matches!(
                p.action.as_str(),
                "RegisterFabricRuntimeRequest"
                    | "UnregisterFabricRuntimeRequest"
                    | "RegisterServiceTypeRequest"
            ),
            15 => p.action.is_empty(),
            _ => false,
        },
        OutboundKind::Raw => {
            p.actor == 12
                && matches!(
                    p.action.as_str(),
                    "RAPReportLoad"
                        | "RAReportFault"
                        | "ProxyReplicaEndpointUpdated"
                        | "ReplicaOpenReply"
                        | "ReplicaCloseReply"
                        | "ProxyUpdateServiceDescriptionReply"
                        | "UpdateConfigurationReply"
                        | "ReplicatorBuildIdleReplicaReply"
                        | "ReplicatorRemoveIdleReplicaReply"
                        | "ReplicatorGetStatusReply"
                        | "ReplicatorUpdateEpochAndGetStatusReply"
                        | "CancelCatchupReplicaSetReply"
                )
        }
        OutboundKind::Reserved => {
            p.actor == 12
                && matches!(
                    p.action.as_str(),
                    "ReplicaOpenReply" | "ReplicaCloseReply" | "ProxyUpdateServiceDescriptionReply"
                )
        }
    };
    if allowed {
        Ok(())
    } else {
        Err(Error::InvalidInput("unauthorized runtime IPC submission"))
    }
}

fn incoming(_: &Message, p: &Properties) -> Result<()> {
    if p.id == MessageId::default() || p.fault.is_some() || p.uncorrelated_reply {
        return Err(Error::Protocol("invalid runtime IPC ingress"));
    }
    let allowed = match p.actor {
        8 => {
            p.expects_reply
                && matches!(
                    p.action.as_str(),
                    "InvalidateLeaseRequest" | "UpdateCodePackageContextRequest"
                )
        }
        12 => {
            (p.action == "RAPQuery" && p.expects_reply)
                || (!p.expects_reply
                    && matches!(
                        p.action.as_str(),
                        "ReplicaOpen"
                            | "ReplicaClose"
                            | "ProxyUpdateServiceDescription"
                            | "UpdateConfiguration"
                            | "ReplicatorBuildIdleReplica"
                            | "ReplicatorRemoveIdleReplica"
                            | "ReplicatorGetStatus"
                            | "ReplicatorUpdateEpochAndGetStatus"
                            | "CancelCatchupReplicaSet"
                            | "ProxyReplicaEndpointUpdatedReply"
                    ))
        }
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(Error::Protocol("unsupported runtime IPC actor/action"))
    }
}

fn response(_: &Message, p: &Properties) -> Result<()> {
    if (p.actor == 8 && p.action.is_empty()) || (p.actor == 12 && p.action == "RAPQueryReply") {
        Ok(())
    } else {
        Err(Error::InvalidInput("unauthorized runtime IPC response"))
    }
}

fn ordinary_reply(_: &Message, p: &Properties) -> Result<()> {
    if matches!(p.actor, 25 | 27) {
        Err(Error::Protocol(
            "upload envelope cannot complete runtime RPC",
        ))
    } else {
        Ok(())
    }
}

fn composition() -> Result<ChannelComposition> {
    Ok(ChannelComposition {
        outbound,
        ordinary_reply,
        actors: ActorDemux::new(
            [8, 12]
                .into_iter()
                .map(|actor| ActorRegistration {
                    actor,
                    authorize: incoming,
                })
                .collect(),
        )?,
        duplex: None,
        requests: Some(RequestPolicy {
            incoming,
            reply: response,
        }),
        ordered_ingress: true,
    })
}

struct Owner {
    channel: GatewayChannel,
    target: Arc<GatewayTarget>,
    work: mpsc::Sender<Work>,
    slots: Arc<Semaphore>,
    ordinary: Arc<Semaphore>,
    serving: Option<WritePredicate>,
    driver: Mutex<Option<JoinHandle<()>>>,
    stopped: watch::Receiver<bool>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        // The driver retains only the transport owner, never this adapter.
        // Last runtime-adapter drop still initiates finite cleanup immediately.
        self.channel.abort();
    }
}

/// Runtime domain composition, not a legacy Connection compatibility backend.
#[derive(Clone)]
pub(crate) struct IpcChannel(Arc<Owner>);

impl IpcChannel {
    pub(crate) async fn connect(
        address: SocketAddr,
        deadline: Instant,
        host_id: &str,
        handoff: Handoff,
        serving: Option<WritePredicate>,
    ) -> Result<Self> {
        let channel = GatewayChannel::connect_ipc_clear(
            address,
            composition()?,
            ChannelLimits {
                max_operations: OPERATIONS,
                max_replies: 32,
                max_ingress: 32,
                max_retained_bytes: 3 * 1024 * 1024 * 1024,
                ..ChannelLimits::default()
            },
            deadline,
            host_id,
        )
        .await?;
        let target = channel.resolve_target(&address.to_string())?;
        let receiver = channel.receiver()?;
        let (send, mut work) = mpsc::channel::<Work>(OPERATIONS);
        let (stopped, observed) = watch::channel(false);
        let transport = channel.clone();
        let ordinary = Arc::new(Semaphore::new(OPERATIONS));
        let slots = Arc::new(Semaphore::new(OPERATIONS));
        let driver_ordinary = ordinary.clone();
        let driver_slots = slots.clone();
        let driver = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let forever = Instant::now() + Duration::from_secs(365 * 24 * 3600);
            loop {
                tokio::select! {
                    biased;
                    _ = transport.wait_terminal(forever) => break,
                    joined = tasks.join_next(), if !tasks.is_empty() => {
                        if joined.is_some_and(|result| result.is_err()) {
                            transport.abort_with_error(Error::Protocol("runtime IPC completion worker failed"));
                            break;
                        }
                    }
                    task = work.recv() => match task {
                        Some(task) => { tasks.spawn(task); }
                        None => break,
                    },
                    received = receiver.receive(forever) => {
                        let result = received.and_then(|received| {
                            let actor = received.message.properties()?.actor;
                            let mut acknowledgment = received.handoff;
                            let result = handoff(IpcMessage {
                                message: received.message,
                                received_at: received.received_at.into_std(),
                                reply: received.reply.map(|context| IpcResponse { context, actor }),
                            });
                            if let Err(error) = &result { transport.abort_with_error(error.clone()); }
                            if let Some(guard) = acknowledgment.as_mut() { guard.acknowledge(); }
                            result
                        });
                        if let Err(error) = result { transport.abort_with_error(error); break; }
                    }
                }
            }
            transport.abort();
            driver_ordinary.close();
            driver_slots.close();
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            // Socket/started-frame cleanup remains transport-owned and bounded.
            let _ = transport
                .closed(Instant::now() + MAX_FRAME_TIME + Duration::from_secs(1))
                .await;
            stopped.send_replace(true);
        });
        Ok(Self(Arc::new(Owner {
            channel,
            target,
            work: send,
            slots,
            ordinary,
            serving,
            driver: Mutex::new(Some(driver)),
            stopped: observed,
        })))
    }

    pub(crate) fn abort(&self) {
        self.0.channel.abort();
    }
    pub(crate) fn abort_with_error(&self, error: Error) {
        self.0.channel.abort_with_error(error);
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.0.channel.terminal_error().is_some()
    }

    async fn join(&self) -> Result<()> {
        let mut driver = self.0.driver.lock().await;
        if let Some(task) = driver.as_mut() {
            let result = task.await;
            driver.take();
            result.map_err(|_| Error::Protocol("runtime IPC driver failed"))?;
        }
        Ok(())
    }
    pub(crate) async fn closed(&self) -> Error {
        let mut stopped = self.0.stopped.clone();
        while !*stopped.borrow_and_update() {
            if stopped.changed().await.is_err() {
                break;
            }
        }
        if let Err(error) = self.join().await {
            return error;
        }
        self.0.channel.terminal_error().unwrap_or(Error::Closed)
    }
    pub(crate) async fn close(&self) -> Result<()> {
        self.0
            .channel
            .close(Instant::now() + MAX_FRAME_TIME + Duration::from_secs(1))
            .await?;
        self.join().await
    }

    fn request_message(
        action: &str,
        actor: i64,
        headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Message> {
        let mut message = envelope(actor, Some(action), body)?;
        message.headers.extend(headers);
        let ticks = deadline
            .checked_duration_since(Instant::now())
            .ok_or(Error::Timeout)?
            .as_nanos()
            / 100;
        if ticks == 0 {
            return Err(Error::Timeout);
        }
        message
            .headers
            .push(Header::timeout(i64::try_from(ticks).map_err(|_| {
                Error::InvalidInput("runtime timeout exceeds native TimeSpan")
            })?)?);
        Ok(message)
    }

    pub(crate) async fn request(
        &self,
        action: &str,
        actor: i64,
        headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Message> {
        let _admission = tokio::time::timeout_at(deadline, self.0.ordinary.acquire())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)?;
        // Build remaining TimeoutHeader after admission, never extend deadline.
        // Cleanup has a distinct already-admitted authority: serving retirement
        // must not suppress the unregister exchanges needed to quiesce.
        let valid = if matches!(
            action,
            "UnregisterFabricRuntimeRequest" | "UnregisterApplicationHostRequest"
        ) {
            None
        } else {
            self.0.serving.clone()
        };
        let (request, _observation) = self.0.channel.request_tracked_fenced(
            &self.0.target,
            Self::request_message(action, actor, headers, body, deadline)?,
            deadline,
            valid,
        );
        request.await
    }

    /// Runtime-owned bounded decoding is polled independently of its observer.
    /// The generic held completion prevents any later frame delivery until
    /// protocol validation succeeds or this runtime retires the channel.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_validated<T: Send + 'static>(
        &self,
        action: &str,
        actor: i64,
        headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
        decode: fn(Message) -> Result<T>,
        valid: WritePredicate,
    ) -> Result<T> {
        let slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("runtime IPC completion slots"))?;
        let admission = tokio::time::timeout_at(deadline, self.0.ordinary.clone().acquire_owned())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)?;
        let message = Self::request_message(action, actor, headers, body, deadline)?;
        let channel = self.0.channel.clone();
        let target = self.0.target.clone();
        let (send, receive) = oneshot::channel();
        let (cancel, mut cancelled) = oneshot::channel::<()>();
        struct Cancel(Option<oneshot::Sender<()>>);
        impl Drop for Cancel {
            fn drop(&mut self) {
                self.0.take();
            }
        }
        let _cancel = Cancel(Some(cancel));
        self.0.work.try_send(Box::pin(async move {
            let result = tokio::select! {
                biased;
                _ = &mut cancelled => return,
                result = channel.request_held(&target, message, deadline, Some(valid)) => result,
            };
            let (result, guard) = match result {
                Ok((message, mut guard)) => {
                    let result = if Instant::now() >= deadline {
                        Err(Error::Timeout)
                    } else {
                        let decoded = decode(message);
                        if Instant::now() >= deadline && !matches!(decoded, Err(Error::Protocol(_))) {
                            Err(Error::Timeout)
                        } else { decoded }
                    };
                    if let Err(error @ Error::Protocol(_)) = &result {
                        channel.abort_with_error(error.clone());
                    }
                    guard.acknowledge();
                    (result, Some(guard))
                }
                Err(error) => (Err(error), None),
            };
            // Even an unobserved error keeps its finite runtime result slot.
            let _ = send.send((result, guard, slot, admission));
        })).map_err(|_| Error::ResourceLimit("runtime IPC completion queue"))?;
        let (result, _guard, _slot, _admission) = tokio::time::timeout_at(deadline, receive)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| self.0.channel.terminal_error().unwrap_or(Error::Closed))?;
        result
    }

    pub(crate) async fn send_one_way(
        &self,
        actor: i64,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        valid: WritePredicate,
    ) -> Result<()> {
        self.0
            .channel
            .send_reserved(
                &self.0.target,
                envelope(actor, Some(action), body)?,
                deadline,
                Some(valid),
            )
            .await
    }

    pub(crate) async fn send_one_way_fenced(
        &self,
        actor: i64,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        valid: WritePredicate,
    ) -> Result<()> {
        let _admission = self
            .0
            .ordinary
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("runtime report output admission"))?;
        self.0
            .channel
            .send_one_way_fenced(
                &self.0.target,
                envelope(actor, Some(action), body)?,
                deadline,
                valid,
            )
            .await
    }
}
