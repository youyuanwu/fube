//! One private physical generation. Handles submit owned work; the driver alone
//! polls I/O. There is no public connection API, inbox, RPC table or TLS adapter.

#![allow(
    dead_code,
    reason = "Private connection engine awaits the messaging layer"
)]

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    wire::{
        frame::{FrameError, FrameWireContext, FIXED_HEADER_LEN},
        headers::HeaderDecodeContext,
    },
    Error, Message,
};

pub(crate) mod driver;
mod native;

pub(crate) use native::{ListenInstance, LocalInstance, Role};

pub(crate) type ConnectionResult<T> = Result<T, Arc<ConnectionError>>;

#[derive(Debug)]
pub(crate) enum ConnectionError {
    Native(Error),
    Frame(FrameError),
    Control(fubst::Error),
    Io {
        phase: &'static str,
        source: io::Error,
    },
    Timeout(&'static str),
    Unsupported(&'static str),
    Cancelled,
    Closed,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => error.fmt(f),
            Self::Frame(error) => error.fmt(f),
            Self::Control(error) => write!(f, "native connection control: {error}"),
            Self::Io { phase, source } => write!(f, "connection {phase}: {source}"),
            Self::Timeout(phase) => write!(f, "connection {phase} timed out"),
            Self::Unsupported(feature) => write!(f, "unsupported native connection {feature}"),
            Self::Cancelled => f.write_str("send cancelled before commitment"),
            Self::Closed => f.write_str("physical connection closed"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Frame(error) => Some(error),
            Self::Control(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<Error> for Arc<ConnectionError> {
    fn from(error: Error) -> Self {
        Arc::new(ConnectionError::Native(error))
    }
}

impl From<FrameError> for Arc<ConnectionError> {
    fn from(error: FrameError) -> Self {
        Arc::new(ConnectionError::Frame(error))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Opening,
    Open,
    Draining,
    Aborting,
    Closed,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Ingress {
    None,
    Raw,
}

/// Execution policies, not standalone construction or retained-heap limits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConnectionConfig {
    pub(crate) wire: FrameWireContext,
    pub(crate) headers: HeaderDecodeContext,
    pub(crate) max_receive_frame_bytes: usize,
    /// None disables outgoing frame-size policy, not the native u32 width.
    pub(crate) max_send_frame_bytes: Option<usize>,
    /// Zero disables the native queued-wire-byte limit.
    pub(crate) send_queue_bytes: usize,
    pub(crate) max_pending_sends: usize,
    pub(crate) work_bytes: usize,
    pub(crate) work_steps: usize,
    pub(crate) frame_timeout: Duration,
    pub(crate) discard_timeout: Duration,
    pub(crate) max_discard_bytes: usize,
    pub(crate) write_timeout: Duration,
    pub(crate) drain_timeout: Duration,
    pub(crate) ingress: Ingress,
}

impl ConnectionConfig {
    pub(crate) fn validate(&self) -> ConnectionResult<()> {
        let valid_frame = |len| (FIXED_HEADER_LEN..=u32::MAX as usize).contains(&len);
        if !valid_frame(self.max_receive_frame_bytes)
            || self
                .max_send_frame_bytes
                .is_some_and(|len| !valid_frame(len))
            || [self.work_bytes, self.work_steps, self.max_pending_sends]
                .iter()
                .any(|value| *value == 0 || *value == usize::MAX)
            || [
                self.frame_timeout,
                self.discard_timeout,
                self.write_timeout,
                self.drain_timeout,
            ]
            .iter()
            .any(|duration| duration.is_zero() || Instant::now().checked_add(*duration).is_none())
        {
            return Err(Error::InvalidInput("invalid connection execution policy").into());
        }
        Ok(())
    }

    fn send_frame_limit(&self) -> usize {
        self.max_send_frame_bytes.unwrap_or(u32::MAX as usize)
    }
}

struct Operation {
    committed: bool,
    cancelled: bool,
    result: Option<ConnectionResult<()>>,
    settled_at: Option<Instant>,
    waker: Option<Waker>,
}

impl Operation {
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            committed: false,
            cancelled: false,
            result: None,
            settled_at: None,
            waker: None,
        }))
    }

    fn settle(&mut self, result: ConnectionResult<()>) -> Option<Waker> {
        if self.result.is_none() {
            self.result = Some(result);
            self.settled_at = Some(Instant::now());
            self.waker.take()
        } else {
            None
        }
    }
}

struct SendJob {
    message: Option<Message>,
    bytes: usize,
    deadline: Instant,
    operation: Arc<Mutex<Operation>>,
    hello: bool,
    authorization: Option<crate::peer::SendAuthorization>,
}

struct State {
    phase: Phase,
    terminal: Option<Arc<ConnectionError>>,
    queue: VecDeque<SendJob>,
    queued_bytes: usize,
    pending_sends: usize,
    handles: usize,
    drain_deadline: Option<Instant>,
    waker: Option<Waker>,
}

struct Shared {
    state: Mutex<State>,
    config: ConnectionConfig,
}

/// Submission ownership only. The driver never retains one of these guards.
pub(crate) struct ConnectionHandle {
    shared: Arc<Shared>,
}

impl Clone for ConnectionHandle {
    fn clone(&self) -> Self {
        self.shared.state.lock().unwrap().handles += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.handles -= 1;
        let wake = if state.handles == 0 && state.phase != Phase::Closed {
            state
                .terminal
                .get_or_insert_with(|| Arc::new(ConnectionError::Closed));
            state.phase = Phase::Aborting;
            state.waker.take()
        } else {
            None
        };
        drop(state);
        wake_if_some(wake);
    }
}

impl ConnectionHandle {
    pub(crate) fn observer(&self) -> ConnectionObserver {
        ConnectionObserver {
            shared: self.shared.clone(),
        }
    }

    pub(crate) fn phase(&self) -> Phase {
        self.shared.state.lock().unwrap().phase
    }

    pub(crate) fn queued_bytes(&self) -> usize {
        self.shared.state.lock().unwrap().queued_bytes
    }

    pub(crate) fn send(
        &self,
        message: Message,
        deadline: Instant,
    ) -> ConnectionResult<SendReceipt> {
        self.send_authorized(message, deadline, None)
    }

    pub(crate) fn send_authorized(
        &self,
        message: Message,
        deadline: Instant,
        authorization: Option<crate::peer::SendAuthorization>,
    ) -> ConnectionResult<SendReceipt> {
        if let Some(authorization) = &authorization {
            authorization.check()?;
        }
        let config = &self.shared.config;
        message.validate(&config.headers, config.send_frame_limit())?;
        if native::is_control_actor(message.headers().effective().actor.get()) {
            return Err(Error::InvalidInput("application send uses a native control Actor").into());
        }
        if deadline <= Instant::now() {
            return Err(Arc::new(ConnectionError::Timeout("send admission")));
        }
        let bytes = FIXED_HEADER_LEN + message.headers().encoded_len() + message.body().len();
        let mut state = self.shared.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        if state.phase != Phase::Open {
            return Err(Arc::new(ConnectionError::Closed));
        }
        let after = state
            .queued_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit("queued wire bytes"))?;
        let limit = if config.send_queue_bytes == 0 {
            usize::MAX
        } else {
            config.send_queue_bytes
        };
        if (after > limit && (config.max_send_frame_bytes.is_some() || state.queued_bytes != 0))
            || state.pending_sends == config.max_pending_sends
        {
            return Err(Error::ResourceLimit("send queue").into());
        }
        state
            .queue
            .try_reserve(1)
            .map_err(|_| Error::ResourceLimit("send queue allocation"))?;
        let operation = Operation::new();
        state.queue.push_back(SendJob {
            message: Some(message),
            bytes,
            deadline,
            operation: operation.clone(),
            hello: false,
            authorization,
        });
        state.queued_bytes = after;
        state.pending_sends += 1;
        let wake = state.waker.take();
        drop(state);
        wake_if_some(wake);
        Ok(SendReceipt {
            shared: self.shared.clone(),
            operation,
        })
    }

    pub(crate) fn drain(&self) -> ConnectionResult<()> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        match state.phase {
            Phase::Open => {
                state.phase = Phase::Draining;
                state.drain_deadline = Some(Instant::now() + self.shared.config.drain_timeout);
            }
            Phase::Draining | Phase::Closed => return Ok(()),
            Phase::Opening | Phase::Aborting => return Err(Arc::new(ConnectionError::Closed)),
        }
        let wake = state.waker.take();
        drop(state);
        wake_if_some(wake);
        Ok(())
    }

    pub(crate) fn abort(&self, error: Arc<ConnectionError>) {
        let mut state = self.shared.state.lock().unwrap();
        if state.phase != Phase::Closed {
            state.terminal.get_or_insert(error);
            state.phase = Phase::Aborting;
        }
        let wake = state.waker.take();
        drop(state);
        wake_if_some(wake);
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionObserver {
    shared: Arc<Shared>,
}
impl ConnectionObserver {
    pub(crate) fn error(&self) -> Option<Arc<ConnectionError>> {
        self.shared.state.lock().unwrap().terminal.clone()
    }
}

/// Complete-local-write observation. Cancellation wins only before the first
/// positive write under the same gate; dropping an observer cannot lose a suffix.
pub(crate) struct SendReceipt {
    shared: Arc<Shared>,
    operation: Arc<Mutex<Operation>>,
}

impl SendReceipt {
    pub(crate) fn attempt(&self) -> SendAttempt {
        SendAttempt {
            shared: self.shared.clone(),
            operation: self.operation.clone(),
        }
    }

    pub(crate) fn cancel(&self) -> bool {
        self.attempt().cancel()
    }
}

#[derive(Clone)]
pub(crate) struct SendAttempt {
    shared: Arc<Shared>,
    operation: Arc<Mutex<Operation>>,
}

impl SendAttempt {
    pub(crate) fn cancel(&self) -> bool {
        // Lock order is always connection -> operation, including socket polling.
        let mut state = self.shared.state.lock().unwrap();
        let mut operation = self.operation.lock().unwrap();
        let cancelled = !operation.committed && operation.result.is_none();
        if cancelled {
            operation.cancelled = true;
        }
        drop(operation);
        let wake = state.waker.take();
        drop(state);
        wake_if_some(wake);
        cancelled
    }

    pub(crate) fn settled_at(&self) -> Option<Instant> {
        self.operation.lock().unwrap().settled_at
    }

    pub(crate) fn disposition(&self) -> crate::AttemptDisposition {
        use crate::AttemptDisposition;
        let operation = self.operation.lock().unwrap();
        if operation.cancelled && !operation.committed {
            AttemptDisposition::Retracted
        } else {
            match &operation.result {
                Some(Ok(())) => AttemptDisposition::LocallyWritten,
                Some(Err(_)) => AttemptDisposition::Retired {
                    committed: operation.committed,
                },
                None if operation.committed => AttemptDisposition::Committed,
                None => AttemptDisposition::Uncommitted,
            }
        }
    }
}

impl Future for SendReceipt {
    type Output = ConnectionResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut operation = self.operation.lock().unwrap();
        if let Some(result) = &operation.result {
            Poll::Ready(result.clone())
        } else {
            operation.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for SendReceipt {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn wake_if_some(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(test)]
mod tests;
