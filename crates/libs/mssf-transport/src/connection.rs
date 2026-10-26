use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot, watch, Notify, Semaphore},
    time::{sleep_until, timeout, timeout_at, Instant},
};

use crate::{
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE, MAX_FRAME_SIZE},
    message::{Header, ListenInstance, Message, MessageId, EXPECTS_REPLY},
    Error, Result,
};

/// Maximum admitted operations, including completed results awaiting delivery.
pub const MAX_PENDING_REQUESTS: usize = 32;
/// Maximum queued frames, including abandoned commands awaiting disposal.
pub const MAX_QUEUED_REQUESTS: usize = 32;
/// Reserved reverse-reply capacity, independent of ordinary requests.
pub const MAX_QUEUED_CONTROLS: usize = 32;
/// A started write or partial incoming frame must finish within this limit.
pub const MAX_FRAME_TIME: Duration = Duration::from_secs(30);
const MAX_UNMATCHED_REPLIES: usize = 1024;

#[cfg(test)]
#[path = "connection_duplex_tests.rs"]
mod duplex_tests;

pub fn deadline_after(timeout: Duration) -> Result<Instant> {
    let ticks = timeout.as_nanos() / 100;
    if ticks == 0 || ticks > i64::MAX as u128 {
        return Err(Error::InvalidInput(
            "timeout must be positive, at least 100ns, and fit native TimeSpan",
        ));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidInput("timeout exceeds monotonic clock range"))
}

fn remaining_ticks(deadline: Instant) -> Result<i64> {
    let ticks = deadline
        .checked_duration_since(Instant::now())
        .ok_or(Error::Timeout)?
        .as_nanos()
        / 100;
    if ticks == 0 {
        return Err(Error::Timeout);
    }
    i64::try_from(ticks).map_err(|_| Error::InvalidInput("timeout exceeds native TimeSpan"))
}

/// Reads one complete bounded clear frame. Cancelling a partial read requires
/// discarding the reader; Connection enforces this for its owned socket.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    read_message_tail(reader, &mut fixed, 0).await
}

async fn read_message_tail<R: AsyncRead + Unpin>(
    reader: &mut R,
    fixed: &mut [u8; FIXED_HEADER_SIZE],
    offset: usize,
) -> Result<Message> {
    reader.read_exact(&mut fixed[offset..]).await?;
    let (size, _) = validate_fixed(fixed)?;
    let mut bytes = vec![0; size];
    bytes[..FIXED_HEADER_SIZE].copy_from_slice(fixed);
    reader.read_exact(&mut bytes[FIXED_HEADER_SIZE..]).await?;
    Message::from_frame(Frame::decode(&bytes)?)
}

/// Writes a complete message; a cancelled partial write requires closing the
/// stream rather than sending another message after its missing suffix.
pub async fn write_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
    let bytes = message.to_frame()?.encode()?;
    writer.write_all(&bytes).await?;
    Ok(())
}

type Completion = Box<dyn FnOnce(Result<Message>) -> Option<Error> + Send>;

struct Pending {
    deadline: Instant,
    complete: Completion,
}

struct Inner {
    terminal: Option<Error>,
    remote: ListenInstance,
    next_index: u32,
    pending: HashMap<MessageId, Pending>,
    unmatched: usize,
    unmatched_total: u64,
}

struct Shared {
    inner: Mutex<Inner>,
    admission: Arc<Semaphore>,
    stop: Notify,
    changed: Notify,
    stopped: watch::Sender<bool>,
    reverse: Option<ReverseHandler>,
    controls: mpsc::Sender<Control>,
}

/// Bounded, nonblocking handoff executed on the transport reader.
/// Returning an error retires this connection. Only the supported native
/// notification envelope is routed here, never ordinary correlated replies.
pub type ReverseHandler = Arc<dyn Fn(ReverseRequest) -> Result<()> + Send + Sync>;

pub struct ReverseRequest {
    pub message: Message,
    pub context: ReplyContext,
}

/// A consuming reply capability bound to the original connection and request.
/// It does not keep a public connection owner alive.
pub struct ReplyContext {
    shared: Weak<Shared>,
    id: MessageId,
}

struct Control {
    bytes: Vec<u8>,
    deadline: Instant,
    complete: oneshot::Sender<Result<()>>,
}

impl ReplyContext {
    /// Queue and flush a native notification processing-status reply.
    pub async fn reply(self, body: Vec<u8>, deadline: Instant) -> Result<()> {
        let shared = self.shared.upgrade().ok_or(Error::Closed)?;
        if let Some(error) = shared.error() {
            return Err(error);
        }
        remaining_ticks(deadline)?;
        let message = Message {
            headers: vec![
                Header::actor(5)?,
                Header::action("ServiceNotificationReply")?,
                Header::relates_to(self.id)?,
                Header::boolean(crate::message::HIGH_PRIORITY, true)?,
            ],
            body,
        };
        let bytes = message.to_frame()?.encode()?;
        let (sender, receiver) = oneshot::channel();
        shared
            .controls
            .try_send(Control {
                bytes,
                deadline,
                complete: sender,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("reply/control queue"),
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        timeout_at(deadline, receiver)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| shared.error().unwrap_or(Error::Closed))?
    }
}

impl Shared {
    fn error(&self) -> Option<Error> {
        self.inner.lock().unwrap().terminal.clone()
    }

    fn complete(&self, id: MessageId, result: Result<Message>) -> Option<Error> {
        let pending = self.inner.lock().unwrap().pending.remove(&id);
        pending.and_then(|pending| (pending.complete)(result))
    }

    fn fail(&self, error: Error) {
        let pending = {
            let mut inner = self.inner.lock().unwrap();
            if inner.terminal.is_some() {
                return;
            }
            inner.terminal = Some(error.clone());
            std::mem::take(&mut inner.pending)
        };
        self.admission.close();
        self.stop.notify_one();
        for (_, pending) in pending {
            // Terminal fan-out cannot create a second connection failure.
            (pending.complete)(Err(error.clone()));
        }
    }
}

struct Handle {
    shared: Arc<Shared>,
    commands: mpsc::Sender<Outbound>,
    process_id: [u8; 16],
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.fail(Error::Closed);
    }
}

struct Outbound {
    id: MessageId,
    deadline: Instant,
    bytes: Vec<u8>,
    sent: SendStatus,
}

/// Race-free queued/started observation for uncertain remote mutations.
#[derive(Clone, Default)]
pub struct SendStatus(Arc<AtomicU8>);
impl SendStatus {
    /// Prevent an unsent frame starting. Returns true if writing already began.
    pub fn cancel_unsent(&self) -> bool {
        self.0
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_err_and(|state| state == 1)
    }
    fn start(&self) -> bool {
        self.0
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

struct RequestGuard {
    shared: Arc<Shared>,
    id: MessageId,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.shared.inner.lock().unwrap().pending.remove(&self.id);
    }
}

struct DriverExit(Arc<Shared>);

impl Drop for DriverExit {
    fn drop(&mut self) {
        self.0
            .fail(Error::Protocol("connection driver stopped unexpectedly"));
        self.0.stopped.send_replace(true);
    }
}

/// Cloneable, bounded clear-TCP connection. No reconnect or request replay.
///
/// The driver continuously receives controls/replies, independently of writes.
/// Dropping the final handle stops it; `close` additionally waits for its exit.
#[derive(Clone)]
pub struct Connection {
    handle: Arc<Handle>,
}

impl Connection {
    pub async fn connect_clear(endpoint: SocketAddr, deadline: Instant) -> Result<Self> {
        Self::connect_role(endpoint, deadline, None).await
    }

    /// Explicitly opt into native notification reverse requests on a dedicated socket.
    pub async fn connect_duplex_clear(
        endpoint: SocketAddr,
        deadline: Instant,
        handler: ReverseHandler,
    ) -> Result<Self> {
        Self::connect_role(endpoint, deadline, Some(handler)).await
    }

    async fn connect_role(
        endpoint: SocketAddr,
        deadline: Instant,
        handler: Option<ReverseHandler>,
    ) -> Result<Self> {
        remaining_ticks(deadline)?;
        let process_id = uuid::Uuid::new_v4().to_bytes_le();
        let id = MessageId {
            guid: process_id,
            index: 1,
        };
        let (stream, remote) = timeout_at(deadline, async {
            let mut stream = TcpStream::connect(endpoint).await?;
            stream.set_nodelay(true)?;
            write_message(
                &mut stream,
                &Message::listen_instance(id, &ListenInstance::default())?,
            )
            .await?;
            let message = read_message(&mut stream).await?;
            let properties = message.properties()?;
            if let Some(fault) = properties.fault.filter(|fault| fault.code != 0) {
                return Err(Error::Remote {
                    code: fault.code,
                    message: "native fault during connection establishment".into(),
                });
            }
            if properties.actor != 1 || !properties.action.is_empty() {
                return Err(Error::Protocol("expected remote ListenInstance control"));
            }
            let remote = ListenInstance::decode(&message.body)?;
            Ok((stream, remote))
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(Self::start_role(stream, remote, process_id, handler))
    }

    #[cfg(test)]
    fn start<S>(stream: S, remote: ListenInstance, process_id: [u8; 16]) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::start_role(stream, remote, process_id, None)
    }

    fn start_role<S>(
        stream: S,
        remote: ListenInstance,
        process_id: [u8; 16],
        reverse: Option<ReverseHandler>,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (controls, control_rx) = mpsc::channel(MAX_QUEUED_CONTROLS);
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                terminal: None,
                remote,
                next_index: 2,
                pending: HashMap::new(),
                unmatched: 0,
                unmatched_total: 0,
            }),
            admission: Arc::new(Semaphore::new(MAX_PENDING_REQUESTS)),
            stop: Notify::new(),
            changed: Notify::new(),
            stopped: watch::channel(false).0,
            reverse,
            controls,
        });
        let (commands, receiver) = mpsc::channel(MAX_QUEUED_REQUESTS);
        tokio::spawn(drive(stream, shared.clone(), receiver, control_rx));
        Self {
            handle: Arc::new(Handle {
                shared,
                commands,
                process_id,
            }),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.handle.shared.error().is_some()
    }

    /// The first terminal cause, including failures observed while idle.
    pub fn terminal_error(&self) -> Option<Error> {
        self.handle.shared.error()
    }

    /// Wait for driver exit without initiating shutdown.
    pub async fn closed(&self) -> Error {
        let mut stopped = self.handle.shared.stopped.subscribe();
        let _ = stopped.wait_for(|stopped| *stopped).await;
        self.terminal_error().unwrap_or(Error::Closed)
    }

    /// Signal immediate shared shutdown without waiting for driver exit.
    pub fn abort(&self) {
        self.handle.shared.fail(Error::Closed);
    }

    /// Snapshot of the latest supported peer ListenInstance control.
    pub fn remote_instance(&self) -> ListenInstance {
        self.handle.shared.inner.lock().unwrap().remote.clone()
    }

    /// Saturating diagnostic count; unmatched reply bodies are never retained.
    pub fn unmatched_reply_count(&self) -> u64 {
        self.handle.shared.inner.lock().unwrap().unmatched_total
    }

    pub async fn request(
        &self,
        action: &str,
        actor: i64,
        extra_headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Message> {
        self.request_decoded(action, actor, extra_headers, body, deadline, Ok)
            .await
    }

    /// Decode a correlated reply on the driver before delivering its result.
    ///
    /// `decode` must be a bounded, nonblocking wire decoder, not an application
    /// callback. Protocol errors retire the connection before another reply.
    pub async fn request_decoded<T: Send + 'static>(
        &self,
        action: &str,
        actor: i64,
        extra_headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
        decode: fn(Message) -> Result<T>,
    ) -> Result<T> {
        self.request_decoded_tracked(
            action,
            actor,
            extra_headers,
            body,
            deadline,
            decode,
            SendStatus::default(),
        )
        .await
    }

    /// Like request_decoded, with a cancellation-safe started-write marker.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_decoded_tracked<T: Send + 'static>(
        &self,
        action: &str,
        actor: i64,
        extra_headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
        decode: fn(Message) -> Result<T>,
        sent: SendStatus,
    ) -> Result<T> {
        let shared = &self.handle.shared;
        if let Some(error) = shared.error() {
            return Err(error);
        }
        remaining_ticks(deadline)?;
        let permit = timeout_at(deadline, shared.admission.clone().acquire_owned())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| shared.error().unwrap_or(Error::Closed))?;
        let slot = timeout_at(deadline, self.handle.commands.reserve())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| shared.error().unwrap_or(Error::Closed))?;
        let id = {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(error) = &inner.terminal {
                return Err(error.clone());
            }
            let Some(next) = inner.next_index.checked_add(1) else {
                drop(inner);
                let error = Error::Protocol("message ID index exhausted");
                shared.fail(error.clone());
                return Err(error);
            };
            let id = MessageId {
                guid: self.handle.process_id,
                index: inner.next_index,
            };
            inner.next_index = next;
            id
        };
        if body.len() > MAX_FRAME_SIZE {
            return Err(Error::InvalidInput("request body exceeds frame limit"));
        }
        let mut headers = vec![Header::action(action)?, Header::actor(actor)?];
        headers.extend(extra_headers);
        headers.push(Header::timeout(remaining_ticks(deadline)?)?);
        headers.push(Header::boolean(EXPECTS_REPLY, true)?);
        headers.push(Header::message_id(id)?);
        let bytes = Message { headers, body }.to_frame()?.encode()?;
        remaining_ticks(deadline)?;
        let (sender, receiver) = oneshot::channel();
        let state = Arc::downgrade(shared);
        let pending = Pending {
            deadline,
            complete: Box::new(move |reply| {
                let result = reply.and_then(|message| {
                    if Instant::now() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let result = decode(message);
                    if Instant::now() >= deadline && !matches!(result, Err(Error::Protocol(_))) {
                        Err(Error::Timeout)
                    } else {
                        result
                    }
                });
                let fatal = match &result {
                    Err(Error::Protocol(_)) => result.as_ref().err().cloned(),
                    _ => None,
                };
                if let (Some(error), Some(shared)) = (&fatal, state.upgrade()) {
                    shared.fail(error.clone());
                }
                // Keep admission until delivery, including an unpolled result.
                let _ = sender.send((result, permit));
                fatal
            }),
        };
        {
            let mut inner = shared.inner.lock().unwrap();
            if let Some(error) = &inner.terminal {
                return Err(error.clone());
            }
            inner.pending.insert(id, pending);
        }
        let _guard = RequestGuard {
            shared: shared.clone(),
            id,
        };
        slot.send(Outbound {
            id,
            deadline,
            bytes,
            sent,
        });
        shared.changed.notify_one();
        let (result, _permit) = timeout_at(deadline, receiver)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| shared.error().unwrap_or(Error::Closed))?;
        result
    }

    /// Close all clones and wait until the driver has released both I/O halves.
    /// Cancellation of this wait does not cancel shutdown.
    pub async fn close(&self) -> Result<()> {
        let mut stopped = self.handle.shared.stopped.subscribe();
        self.abort();
        stopped
            .wait_for(|stopped| *stopped)
            .await
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}

async fn drive<S>(
    stream: S,
    shared: Arc<Shared>,
    commands: mpsc::Receiver<Outbound>,
    controls: mpsc::Receiver<Control>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _exit = DriverExit(shared.clone());
    let (reader, writer) = tokio::io::split(stream);
    tokio::select! {
        biased;
        _ = shared.stop.notified() => {}
        result = async {
            tokio::try_join!(
                receive_loop(reader, &shared),
                send_loop(writer, &shared, commands, controls),
                expire_loop(&shared),
            )
        } => {
            shared.fail(result.err().unwrap_or(Error::Closed));
        }
    }
}

async fn receive_loop<R: AsyncRead + Unpin>(mut reader: R, shared: &Arc<Shared>) -> Result<()> {
    loop {
        let mut fixed = [0; FIXED_HEADER_SIZE];
        // An idle connection has no inactivity deadline. Once a frame starts,
        // retain this read future until completion or whole-connection shutdown.
        reader.read_exact(&mut fixed[..1]).await?;
        let reply = timeout(
            MAX_FRAME_TIME,
            read_message_tail(&mut reader, &mut fixed, 1),
        )
        .await
        .map_err(|_| Error::Timeout)??;
        let properties = reply.properties()?;
        if properties.actor == 1 || properties.actor == 29 {
            if let Some(fault) = properties.fault.filter(|fault| fault.code != 0) {
                return Err(Error::Remote {
                    code: fault.code,
                    message: "native connection control fault".into(),
                });
            }
            if properties.actor == 1 && properties.action.is_empty() {
                let remote = ListenInstance::decode(&reply.body)?;
                shared.inner.lock().unwrap().remote = remote;
                continue;
            }
            return Err(Error::Protocol(match properties.action.as_str() {
                "ScheduleClose" if properties.actor == 29 => {
                    "peer scheduled connection close; automatic recovery is disabled"
                }
                "Reconnect" if properties.actor == 29 => {
                    "session reconnect is unsupported by the clear profile"
                }
                "ConnectionAuth" | "ClaimsTokenError" if properties.actor == 29 => {
                    "security controls are unsupported by the clear profile"
                }
                _ => "unsupported transport control",
            }));
        }
        if properties.uncorrelated_reply {
            if let Some(handler) = &shared.reverse {
                if properties.actor != 5
                    || properties.action != "ServiceNotificationRequest"
                    || properties.id == MessageId::default()
                    || properties.relates_to != MessageId::default()
                    || properties.fault.is_some()
                {
                    return Err(Error::Protocol("unsupported reverse notification envelope"));
                }
                handler(ReverseRequest {
                    message: reply,
                    context: ReplyContext {
                        shared: Arc::downgrade(shared),
                        id: properties.id,
                    },
                })?;
                continue;
            }
        }
        if properties.uncorrelated_reply || properties.expects_reply {
            return Err(Error::Protocol("reverse requests are unsupported"));
        }
        let pending = {
            let mut inner = shared.inner.lock().unwrap();
            let pending = inner.pending.remove(&properties.relates_to);
            if pending.is_some() {
                inner.unmatched = 0;
            } else {
                inner.unmatched += 1;
                inner.unmatched_total = inner.unmatched_total.saturating_add(1);
                if inner.unmatched > MAX_UNMATCHED_REPLIES {
                    return Err(Error::Protocol("too many unmatched replies"));
                }
            }
            pending
        };
        if let Some(pending) = pending {
            if let Some(error) = (pending.complete)(Ok(reply)) {
                return Err(error);
            }
        }
    }
}

async fn send_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    shared: &Shared,
    mut commands: mpsc::Receiver<Outbound>,
    mut controls: mpsc::Receiver<Control>,
) -> Result<()> {
    loop {
        let command = tokio::select! {
            biased;
            control = controls.recv() => {
                let Some(control) = control else { return Err(Error::Closed); };
                if Instant::now() >= control.deadline {
                    let _ = control.complete.send(Err(Error::Timeout));
                    continue;
                }
                let result = timeout(MAX_FRAME_TIME, writer.write_all(&control.bytes))
                    .await.map_err(|_| Error::Timeout)?.map_err(Error::from);
                let _ = control.complete.send(result.clone());
                result?;
                continue;
            }
            command = commands.recv() => {
                let Some(command) = command else { return Err(Error::Closed); };
                command
            }
        };
        if !shared
            .inner
            .lock()
            .unwrap()
            .pending
            .contains_key(&command.id)
        {
            continue;
        }
        if Instant::now() >= command.deadline {
            shared.complete(command.id, Err(Error::Timeout));
            continue;
        }
        if !command.sent.start() {
            continue;
        }
        // Local request expiry must not truncate a started frame or retire
        // unrelated callers before the independent frame watchdog expires.
        timeout(MAX_FRAME_TIME, writer.write_all(&command.bytes))
            .await
            .map_err(|_| Error::Timeout)??;
    }
}

async fn expire_loop(shared: &Shared) -> Result<()> {
    loop {
        let deadline = shared
            .inner
            .lock()
            .unwrap()
            .pending
            .values()
            .map(|pending| pending.deadline)
            .min();
        if let Some(deadline) = deadline {
            tokio::select! {
                _ = sleep_until(deadline) => {}
                _ = shared.changed.notified() => continue,
            }
            let expired: Vec<_> = shared
                .inner
                .lock()
                .unwrap()
                .pending
                .iter()
                .filter(|(_, pending)| pending.deadline <= Instant::now())
                .map(|(id, _)| *id)
                .collect();
            for id in expired {
                shared.complete(id, Err(Error::Timeout));
            }
        } else {
            shared.changed.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::UNCORRELATED_REPLY;
    use tokio::{net::TcpListener, sync::oneshot};

    fn deadline() -> Instant {
        deadline_after(Duration::from_secs(2)).unwrap()
    }

    fn control() -> Message {
        Message::listen_instance(
            MessageId {
                guid: [1; 16],
                index: 1,
            },
            &ListenInstance {
                address: "127.0.0.1:19000".into(),
                instance: 123,
                nonce: [2; 16],
            },
        )
        .unwrap()
    }

    async fn establish_peer(stream: &mut TcpStream) {
        write_message(stream, &control()).await.unwrap();
        let local = read_message(stream).await.unwrap();
        let properties = local.properties().unwrap();
        assert_eq!(properties.actor, 1);
        assert_eq!(properties.action, "");
        assert!(properties.high_priority);
        assert_ne!(properties.id, MessageId::default());
        assert_eq!(
            ListenInstance::decode(&local.body).unwrap(),
            ListenInstance::default()
        );
    }

    fn response(id: MessageId) -> Message {
        Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("PingRequest").unwrap(),
                Header::relates_to(id).unwrap(),
            ],
            body: vec![1, 2, 3],
        }
    }

    async fn poll_pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    async fn until(mut condition: impl FnMut() -> bool) {
        timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver made bounded progress");
    }

    fn duplex_connection(capacity: usize) -> (Connection, tokio::io::DuplexStream) {
        let (stream, peer) = tokio::io::duplex(capacity);
        (
            Connection::start(stream, ListenInstance::default(), [9; 16]),
            peer,
        )
    }

    #[tokio::test]
    async fn idle_control_updates_snapshot_without_a_request_and_idle_eof_stops_driver() {
        let (connection, mut peer) = duplex_connection(64);
        write_message(&mut peer, &control()).await.unwrap();
        until(|| connection.remote_instance().instance == 123).await;
        assert!(!connection.is_closed());
        assert!(connection
            .handle
            .shared
            .inner
            .lock()
            .unwrap()
            .pending
            .is_empty());
        drop(peer);
        until(|| connection.is_closed()).await;
        assert!(matches!(connection.terminal_error(), Some(Error::Io(_))));
        connection.close().await.unwrap();
        assert!(*connection.handle.shared.stopped.borrow());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_out_of_order_replies_use_the_entire_id() {
        let (connection, mut peer) = duplex_connection(4096);
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..16 {
                requests.push(read_message(&mut peer).await.unwrap());
            }
            let first_id = requests[0].properties().unwrap().id;
            for wrong in [
                MessageId {
                    guid: [0; 16],
                    ..first_id
                },
                MessageId {
                    index: first_id.index + 1000,
                    ..first_id
                },
            ] {
                write_message(&mut peer, &response(wrong)).await.unwrap();
            }
            for request in requests.iter().rev() {
                let mut reply = response(request.properties().unwrap().id);
                reply.body = request.body.clone();
                write_message(&mut peer, &reply).await.unwrap();
            }
            write_message(&mut peer, &response(first_id)).await.unwrap();
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        });
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..16u8 {
            let clone = connection.clone();
            tasks.spawn(async move {
                assert_eq!(
                    clone
                        .request("test", 5, vec![], vec![index], deadline())
                        .await
                        .unwrap()
                        .body,
                    [index]
                );
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        until(|| connection.unmatched_reply_count() == 3).await;
        assert!(!connection.is_closed());
        connection.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reader_delivers_other_reply_while_writer_is_backpressured() {
        let (connection, mut peer) = duplex_connection(64);
        let first = connection.request("first", 5, vec![], vec![], deadline());
        tokio::pin!(first);
        poll_pending(first.as_mut()).await;
        let request = read_message(&mut peer).await.unwrap();
        let second = connection.request("blocked", 5, vec![], vec![7; 4096], deadline());
        tokio::pin!(second);
        poll_pending(second.as_mut()).await;
        let mut fixed = [0; FIXED_HEADER_SIZE];
        peer.read_exact(&mut fixed[..1]).await.unwrap();
        write_message(&mut peer, &response(request.properties().unwrap().id))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), &mut first)
                .await
                .unwrap()
                .unwrap()
                .body,
            [1, 2, 3]
        );
        assert!(!connection.is_closed());
        read_message_tail(&mut peer, &mut fixed, 1).await.unwrap();
        connection.close().await.unwrap();
        assert!(matches!(second.await, Err(Error::Closed)));
    }

    #[tokio::test]
    async fn partial_reader_survives_outbound_activity_without_restarting_a_frame() {
        let (connection, mut peer) = duplex_connection(256);
        let bytes = control().to_frame().unwrap().encode().unwrap();
        peer.write_all(&bytes[..7]).await.unwrap();
        tokio::task::yield_now().await;
        let request = connection.request("test", 5, vec![], vec![], deadline());
        tokio::pin!(request);
        poll_pending(request.as_mut()).await;
        let outgoing = read_message(&mut peer).await.unwrap();
        peer.write_all(&bytes[7..]).await.unwrap();
        until(|| connection.remote_instance().instance == 123).await;
        write_message(&mut peer, &response(outgoing.properties().unwrap().id))
            .await
            .unwrap();
        assert_eq!(request.await.unwrap().body, [1, 2, 3]);
        connection.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_and_expired_queued_requests_are_not_written() {
        let (connection, mut peer) = duplex_connection(64);
        let active = connection.request("blocked", 5, vec![], vec![7; 4096], deadline());
        tokio::pin!(active);
        poll_pending(active.as_mut()).await;
        let mut fixed = [0; FIXED_HEADER_SIZE];
        peer.read_exact(&mut fixed[..1]).await.unwrap();
        {
            let cancelled = connection.request("cancelled", 5, vec![], vec![9], deadline());
            tokio::pin!(cancelled);
            poll_pending(cancelled.as_mut()).await;
        }
        let expired = connection.request(
            "expired",
            5,
            vec![],
            vec![8],
            deadline_after(Duration::from_millis(10)).unwrap(),
        );
        tokio::pin!(expired);
        poll_pending(expired.as_mut()).await;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(expired.await, Err(Error::Timeout)));
        let survivor = connection.request("survivor", 5, vec![], vec![6], deadline());
        tokio::pin!(survivor);
        poll_pending(survivor.as_mut()).await;
        assert_eq!(
            read_message_tail(&mut peer, &mut fixed, 1)
                .await
                .unwrap()
                .body,
            vec![7; 4096]
        );
        let next = read_message(&mut peer).await.unwrap();
        assert_eq!(next.body, [6]);
        write_message(&mut peer, &response(next.properties().unwrap().id))
            .await
            .unwrap();
        survivor.await.unwrap();
        connection.close().await.unwrap();
        assert!(matches!(active.await, Err(Error::Closed)));
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_started_write_outlives_its_request_deadline_without_failing_neighbors() {
        for cancel in [true, false] {
            let (connection, mut peer) = duplex_connection(64);
            let mut abandoned = Some(Box::pin(connection.request(
                "abandoned",
                5,
                vec![],
                vec![7; 4096],
                Instant::now() + Duration::from_millis(10),
            )));
            poll_pending(abandoned.as_mut().unwrap().as_mut()).await;
            let mut fixed = [0; FIXED_HEADER_SIZE];
            peer.read_exact(&mut fixed[..1]).await.unwrap();
            if cancel {
                drop(abandoned.take());
                assert_eq!(
                    connection.handle.shared.admission.available_permits(),
                    MAX_PENDING_REQUESTS
                );
            }
            let survivor = connection.request(
                "survivor",
                5,
                vec![],
                vec![6],
                Instant::now() + Duration::from_secs(120),
            );
            tokio::pin!(survivor);
            poll_pending(survivor.as_mut()).await;
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
            assert!(
                !connection.is_closed(),
                "request expiry is not the frame watchdog"
            );
            if let Some(request) = abandoned.take() {
                assert!(matches!(request.await, Err(Error::Timeout)));
            }
            let complete = read_message_tail(&mut peer, &mut fixed, 1).await.unwrap();
            assert_eq!(complete.body, vec![7; 4096]);
            let next = read_message(&mut peer).await.unwrap();
            assert_eq!(next.body, [6]);
            write_message(&mut peer, &response(next.properties().unwrap().id))
                .await
                .unwrap();
            survivor.await.unwrap();
            connection.close().await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn admission_is_bounded_and_cancelled_waiters_release_capacity() {
        let (connection, mut peer) = duplex_connection(64);
        let end = Instant::now() + Duration::from_secs(120);
        let mut requests = Vec::new();
        for _ in 0..MAX_PENDING_REQUESTS {
            let mut request = Box::pin(connection.request("test", 5, vec![], vec![], end));
            poll_pending(request.as_mut()).await;
            requests.push(request);
        }
        assert_eq!(connection.handle.shared.admission.available_permits(), 0);
        assert_eq!(
            connection.handle.shared.inner.lock().unwrap().pending.len(),
            MAX_PENDING_REQUESTS
        );
        let extra = connection.request(
            "extra",
            5,
            vec![],
            vec![],
            Instant::now() + Duration::from_millis(10),
        );
        tokio::pin!(extra);
        poll_pending(extra.as_mut()).await;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(extra.await, Err(Error::Timeout)));
        drop(requests.pop());
        assert_eq!(connection.handle.shared.admission.available_permits(), 1);
        let mut next = Box::pin(connection.request("after-cancel", 5, vec![], vec![], end));
        poll_pending(next.as_mut()).await;
        assert_eq!(connection.handle.shared.admission.available_permits(), 0);
        drop(next);
        drop(requests);
        assert_eq!(
            connection.handle.shared.admission.available_permits(),
            MAX_PENDING_REQUESTS
        );
        // Only the possibly started first frame may remain on the wire.
        connection.close().await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.len() <= 64);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_output_queue_stays_bounded_and_queue_wait_has_a_deadline() {
        let (connection, mut peer) = duplex_connection(64);
        let end = Instant::now() + Duration::from_secs(120);
        let active = connection.request("active", 5, vec![], vec![7; 4096], end);
        tokio::pin!(active);
        poll_pending(active.as_mut()).await;
        peer.read_u8().await.unwrap();
        for _ in 0..MAX_QUEUED_REQUESTS {
            let queued = connection.request("cancelled", 5, vec![], vec![], end);
            tokio::pin!(queued);
            poll_pending(queued.as_mut()).await;
        }
        assert_eq!(connection.handle.commands.capacity(), 0);
        assert_eq!(
            connection.handle.shared.admission.available_permits(),
            MAX_PENDING_REQUESTS - 1
        );
        let blocked = connection.request(
            "queue-full",
            5,
            vec![],
            vec![],
            Instant::now() + Duration::from_millis(10),
        );
        tokio::pin!(blocked);
        poll_pending(blocked.as_mut()).await;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(blocked.await, Err(Error::Timeout)));
        assert_eq!(
            connection.handle.shared.admission.available_permits(),
            MAX_PENDING_REQUESTS - 1
        );
        connection.close().await.unwrap();
        assert!(matches!(active.await, Err(Error::Closed)));
    }

    #[tokio::test(start_paused = true)]
    async fn completed_unpolled_results_keep_admission_until_delivery_or_drop() {
        let (connection, mut peer) = duplex_connection(16384);
        let mut requests = Vec::new();
        for _ in 0..MAX_PENDING_REQUESTS {
            let mut request = Box::pin(connection.request("test", 5, vec![], vec![], deadline()));
            poll_pending(request.as_mut()).await;
            requests.push(request);
        }
        for _ in 0..MAX_PENDING_REQUESTS {
            let request = read_message(&mut peer).await.unwrap();
            write_message(&mut peer, &response(request.properties().unwrap().id))
                .await
                .unwrap();
        }
        until(|| {
            connection
                .handle
                .shared
                .inner
                .lock()
                .unwrap()
                .pending
                .is_empty()
        })
        .await;
        assert_eq!(connection.handle.shared.admission.available_permits(), 0);
        let blocked = connection.request(
            "no-permit",
            5,
            vec![],
            vec![],
            Instant::now() + Duration::from_millis(10),
        );
        tokio::pin!(blocked);
        poll_pending(blocked.as_mut()).await;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(blocked.await, Err(Error::Timeout)));
        assert_eq!(connection.handle.shared.admission.available_permits(), 0);
        requests.pop().unwrap().await.unwrap();
        assert_eq!(connection.handle.shared.admission.available_permits(), 1);
        drop(requests);
        assert_eq!(
            connection.handle.shared.admission.available_permits(),
            MAX_PENDING_REQUESTS
        );
        connection.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn started_write_and_partial_read_have_finite_progress_limits() {
        for partial_read in [true, false] {
            let (connection, mut peer) = duplex_connection(64);
            let request = connection.request(
                "blocked",
                5,
                vec![],
                vec![7; 4096],
                Instant::now() + Duration::from_secs(120),
            );
            tokio::pin!(request);
            if partial_read {
                peer.write_u8(0x12).await.unwrap();
            } else {
                poll_pending(request.as_mut()).await;
                peer.read_u8().await.unwrap();
            }
            tokio::task::yield_now().await;
            tokio::time::advance(MAX_FRAME_TIME + Duration::from_secs(1)).await;
            until(|| connection.is_closed()).await;
            assert!(matches!(connection.terminal_error(), Some(Error::Timeout)));
            connection.close().await.unwrap();
            assert!(*connection.handle.shared.stopped.borrow());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_connection_has_no_inactivity_timeout() {
        let (connection, mut peer) = duplex_connection(256);
        tokio::task::yield_now().await;
        tokio::time::advance(MAX_FRAME_TIME * 2).await;
        tokio::task::yield_now().await;
        assert!(!connection.is_closed());
        write_message(&mut peer, &control()).await.unwrap();
        until(|| connection.remote_instance().instance == 123).await;
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn unsupported_controls_and_reverse_requests_cannot_complete_a_matching_id() {
        for (actor, action, uncorrelated, expects_reply) in [
            (1, "unknown", false, false),
            (29, "ScheduleClose", false, false),
            (29, "Reconnect", false, false),
            (29, "ConnectionAuth", false, false),
            (29, "ClaimsTokenError", false, false),
            (29, "unknown", false, false),
            (5, "notification", true, false),
            (5, "request", false, true),
        ] {
            let (connection, mut peer) = duplex_connection(256);
            let request = connection.request("test", 5, vec![], vec![], deadline());
            tokio::pin!(request);
            poll_pending(request.as_mut()).await;
            let outgoing = read_message(&mut peer).await.unwrap();
            let mut incoming = response(outgoing.properties().unwrap().id);
            incoming.headers.push(Header::actor(actor).unwrap());
            incoming.headers.push(Header::action(action).unwrap());
            incoming
                .headers
                .push(Header::boolean(UNCORRELATED_REPLY, uncorrelated).unwrap());
            incoming
                .headers
                .push(Header::boolean(EXPECTS_REPLY, expects_reply).unwrap());
            write_message(&mut peer, &incoming).await.unwrap();
            assert!(
                matches!(request.await, Err(Error::Protocol(_))),
                "{actor}/{action}"
            );
            assert!(connection.is_closed());
            connection.close().await.unwrap();
            assert_eq!(
                peer.read(&mut [0]).await.unwrap(),
                0,
                "no reverse acknowledgment"
            );
        }
    }

    #[tokio::test]
    async fn idle_control_fault_preserves_code_and_malformed_control_closes() {
        for fault in [true, false] {
            let (connection, mut peer) = duplex_connection(256);
            let mut incoming = control();
            if fault {
                incoming.headers.push(Header::actor(29).unwrap());
                incoming
                    .headers
                    .push(Header::action("ScheduleClose").unwrap());
                incoming.headers.push(
                    Header::object(crate::message::FAULT, |encoder| {
                        encoder.i64(-12345);
                        encoder.boolean(false);
                        Ok(())
                    })
                    .unwrap(),
                );
                incoming.body.clear();
            } else {
                incoming.body.clear();
            }
            write_message(&mut peer, &incoming).await.unwrap();
            until(|| connection.is_closed()).await;
            if fault {
                assert!(matches!(
                    connection.terminal_error(),
                    Some(Error::Remote { code: -12345, .. })
                ));
            } else {
                assert!(matches!(
                    connection.terminal_error(),
                    Some(Error::Protocol(_))
                ));
            }
            connection.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn idle_unknown_replies_have_a_finite_diagnostic_budget() {
        let (connection, mut peer) = duplex_connection(256);
        for _ in 0..=MAX_UNMATCHED_REPLIES {
            write_message(&mut peer, &response(MessageId::default()))
                .await
                .unwrap();
        }
        until(|| connection.is_closed()).await;
        assert_eq!(
            connection.unmatched_reply_count(),
            (MAX_UNMATCHED_REPLIES + 1) as u64
        );
        assert!(matches!(
            connection.terminal_error(),
            Some(Error::Protocol("too many unmatched replies"))
        ));
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_decoded_success_closes_before_waiter_repoll_and_fails_neighbors() {
        let (connection, mut peer) = duplex_connection(4096);
        let bad =
            connection.request_decoded("bad", 5, vec![], vec![], deadline(), |_| -> Result<()> {
                Err(Error::Protocol("malformed success"))
            });
        let other = connection.request("other", 5, vec![], vec![], deadline());
        tokio::pin!(bad, other);
        poll_pending(bad.as_mut()).await;
        poll_pending(other.as_mut()).await;
        let first = read_message(&mut peer).await.unwrap();
        read_message(&mut peer).await.unwrap();
        write_message(&mut peer, &response(first.properties().unwrap().id))
            .await
            .unwrap();
        until(|| connection.is_closed()).await;
        assert!(matches!(
            bad.await,
            Err(Error::Protocol("malformed success"))
        ));
        assert!(matches!(
            other.await,
            Err(Error::Protocol("malformed success"))
        ));
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_is_shared_idempotent_and_cancelling_its_wait_does_not_cancel_shutdown() {
        let (connection, mut peer) = duplex_connection(256);
        let clone = connection.clone();
        drop(connection.clone());
        assert!(!connection.is_closed());
        let request = connection.request("pending", 5, vec![], vec![], deadline());
        tokio::pin!(request);
        poll_pending(request.as_mut()).await;
        read_message(&mut peer).await.unwrap();
        {
            let close = clone.close();
            tokio::pin!(close);
            poll_pending(close.as_mut()).await;
        }
        assert!(connection.is_closed());
        let (first, second) = tokio::join!(connection.close(), clone.close());
        first.unwrap();
        second.unwrap();
        assert!(*connection.handle.shared.stopped.borrow());
        assert!(matches!(request.await, Err(Error::Closed)));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn final_handle_drop_stops_an_idle_or_blocked_writer_driver() {
        for blocked in [false, true] {
            let (connection, mut peer) = duplex_connection(64);
            let mut stopped = connection.handle.shared.stopped.subscribe();
            if blocked {
                let request = connection.request("blocked", 5, vec![], vec![7; 4096], deadline());
                tokio::pin!(request);
                poll_pending(request.as_mut()).await;
                peer.read_u8().await.unwrap();
            }
            drop(connection);
            timeout(Duration::from_secs(1), stopped.wait_for(|value| *value))
                .await
                .unwrap()
                .unwrap();
            let mut remaining = Vec::new();
            peer.read_to_end(&mut remaining).await.unwrap();
            assert!(remaining.len() <= 64);
        }
    }

    #[tokio::test]
    async fn id_exhaustion_fails_closed_instead_of_wrapping() {
        let (connection, _peer) = duplex_connection(256);
        connection.handle.shared.inner.lock().unwrap().next_index = u32::MAX;
        assert!(matches!(
            connection
                .request("test", 5, vec![], vec![], deadline())
                .await,
            Err(Error::Protocol("message ID index exhausted"))
        ));
        connection.close().await.unwrap();
    }

    #[tokio::test]
    async fn wire_handles_every_split_and_coalesced_messages() {
        let message = control();
        let bytes = message.to_frame().unwrap().encode().unwrap();
        for split in 0..=bytes.len() {
            let (mut writer, mut reader) = tokio::io::duplex(7);
            let write = async {
                writer.write_all(&bytes[..split]).await.unwrap();
                tokio::task::yield_now().await;
                writer.write_all(&bytes[split..]).await.unwrap();
            };
            let read = async { assert_eq!(read_message(&mut reader).await.unwrap(), message) };
            tokio::join!(write, read);
        }
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_all(&bytes.repeat(2)).await.unwrap();
        for _ in 0..2 {
            assert_eq!(read_message(&mut reader).await.unwrap(), message);
        }
        drop(writer);
        assert!(read_message(&mut reader).await.is_err());
    }

    #[tokio::test]
    async fn invalid_fixed_header_fails_without_waiting_for_body() {
        for (size, provider) in [(u32::MAX, 0), (11, 0), (12, 1)] {
            let (mut writer, mut reader) = tokio::io::duplex(32);
            let mut fixed = [0; 12];
            fixed[..4].copy_from_slice(&size.to_le_bytes());
            fixed[4] = provider;
            writer.write_all(&fixed).await.unwrap();
            assert!(timeout(Duration::from_secs(1), read_message(&mut reader))
                .await
                .unwrap()
                .is_err());
        }
    }

    #[tokio::test]
    async fn sequential_requests_ignore_unknown_and_duplicate_replies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            establish_peer(&mut stream).await;
            let mut previous = None;
            for index in 2..5 {
                let request = read_message(&mut stream).await.unwrap();
                let properties = request.properties().unwrap();
                assert_eq!(properties.id.index, index);
                assert!(properties.expects_reply);
                if let Some(previous) = previous {
                    write_message(&mut stream, &response(previous))
                        .await
                        .unwrap();
                }
                let wrong_id = MessageId {
                    index: properties.id.index + 1000,
                    ..properties.id
                };
                write_message(&mut stream, &response(wrong_id))
                    .await
                    .unwrap();
                let wrong_guid = MessageId {
                    guid: [0xab; 16],
                    ..properties.id
                };
                write_message(&mut stream, &response(wrong_guid))
                    .await
                    .unwrap();
                write_message(&mut stream, &control()).await.unwrap();
                write_message(&mut stream, &response(properties.id))
                    .await
                    .unwrap();
                previous = Some(properties.id);
            }
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let connection = Connection::connect_clear(address, deadline())
            .await
            .unwrap();
        assert_eq!(connection.remote_instance().instance, 123);
        for _ in 0..3 {
            let reply = connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .unwrap();
            assert_eq!(reply.body, [1, 2, 3]);
        }
        connection.close().await.unwrap();
        connection.close().await.unwrap();
        assert!(connection.is_closed());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn timeout_removes_only_its_pending_request_and_late_reply_is_ignored() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            establish_peer(&mut stream).await;
            let expired = read_message(&mut stream).await.unwrap();
            let next = read_message(&mut stream).await.unwrap();
            write_message(&mut stream, &response(expired.properties().unwrap().id))
                .await
                .unwrap();
            write_message(&mut stream, &response(next.properties().unwrap().id))
                .await
                .unwrap();
            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        });
        let connection = Connection::connect_clear(address, deadline())
            .await
            .unwrap();
        let started = Instant::now();
        let result = connection
            .request(
                "PingRequest",
                5,
                vec![],
                vec![],
                deadline_after(Duration::from_millis(30)).unwrap(),
            )
            .await;
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!connection.is_closed());
        assert_eq!(
            connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .unwrap()
                .body,
            [1, 2, 3]
        );
        assert_eq!(connection.unmatched_reply_count(), 1);
        connection.close().await.unwrap();
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_started_write_finishes_frame_and_keeps_other_requests_usable() {
        let (stream, mut peer_stream) = tokio::io::duplex(64);
        let connection = Connection::start(stream, ListenInstance::default(), [9; 16]);
        let (observed_tx, observed_rx) = oneshot::channel();
        let (continue_tx, continue_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut fixed = [0; FIXED_HEADER_SIZE];
            peer_stream.read_exact(&mut fixed[..1]).await.unwrap();
            observed_tx.send(()).unwrap();
            continue_rx.await.unwrap();
            let cancelled = read_message_tail(&mut peer_stream, &mut fixed, 1)
                .await
                .unwrap();
            assert_eq!(cancelled.body, vec![7; 4096]);
            let next = read_message(&mut peer_stream).await.unwrap();
            write_message(
                &mut peer_stream,
                &response(cancelled.properties().unwrap().id),
            )
            .await
            .unwrap();
            write_message(&mut peer_stream, &response(next.properties().unwrap().id))
                .await
                .unwrap();
            assert_eq!(peer_stream.read(&mut [0]).await.unwrap(), 0);
        });
        {
            let request = connection.request("PingRequest", 5, vec![], vec![7; 4096], deadline());
            tokio::pin!(request);
            tokio::select! {
                result = &mut request => panic!("request unexpectedly completed: {result:?}"),
                result = observed_rx => result.unwrap(),
            }
        }
        assert!(!connection.is_closed());
        assert_eq!(
            connection.handle.shared.admission.available_permits(),
            MAX_PENDING_REQUESTS
        );
        continue_tx.send(()).unwrap();
        assert_eq!(
            connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .unwrap()
                .body,
            [1, 2, 3]
        );
        connection.close().await.unwrap();
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_and_invalid_reply_close_the_connection() {
        for malformed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                establish_peer(&mut stream).await;
                read_message(&mut stream).await.unwrap();
                if malformed {
                    let mut fixed = [0; 12];
                    fixed[..4].copy_from_slice(&12u32.to_le_bytes());
                    fixed[4] = 1;
                    stream.write_all(&fixed).await.unwrap();
                }
            });
            let connection = Connection::connect_clear(address, deadline())
                .await
                .unwrap();
            assert!(connection
                .request("PingRequest", 5, vec![], vec![], deadline())
                .await
                .is_err());
            assert!(connection.is_closed());
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn silent_establishment_has_a_finite_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_message(&mut stream).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), stream.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        assert!(matches!(
            Connection::connect_clear(address, deadline_after(Duration::from_millis(30)).unwrap())
                .await,
            Err(Error::Timeout)
        ));
        peer.await.unwrap();
    }

    #[test]
    fn deadline_configuration_is_explicit_and_bounded() {
        assert!(deadline_after(Duration::ZERO).is_err());
        assert!(deadline_after(Duration::from_nanos(99)).is_err());
        assert!(deadline_after(Duration::MAX).is_err());
        assert!(deadline_after(Duration::from_secs(30)).is_ok());
    }
}
