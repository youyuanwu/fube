//! Crate-private established connection owner, above the shared raw primitives.
//!
//! Transport jobs and policy metadata share a coordinator, not responsibilities.
//! No RPC table, message-ID allocator, session identity or actor grant lives here.
//! Jobs carry already-admitted charges; there is no second admission/result queue.

use std::{
    collections::VecDeque,
    future::Future,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    task::Poll,
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{watch, Notify},
    task::JoinHandle,
    time::{timeout_at, Instant},
};

use super::{join_io, read_message, write_frame, Limits};
use crate::{
    message::{self, Header, ListenInstance, Message, MessageId, Properties, MAX_HEADER_COUNT},
    Error, Result,
};

pub(crate) type Validity = Arc<dyn Fn() -> bool + Send + Sync>;

/// A local transport receipt, never a remote protocol result.
pub(crate) enum Receipt {
    Written,
    Rejected,
    Failed(Error),
}

/// Synchronous policy hooks run under the coordinator, including the socket
/// poll/positive-visibility transaction. They cannot await or reenter the core.
pub(crate) trait Policy: Send + 'static {
    type Charge: Send + Sync + 'static;

    fn eligible(&self, token: u64, now: Instant) -> bool;
    fn started(&self, token: u64);
    fn receipt(&mut self, token: u64, receipt: Receipt, now: Instant);
    fn retire(&mut self);
}

pub(crate) struct WriteJob<C> {
    pub token: u64,
    pub bytes: Vec<u8>,
    pub deadline: Instant,
    pub valid: Option<Validity>,
    pub charge: C,
}

pub(crate) struct Transport<C> {
    pub terminal: Option<Error>,
    pub dispose_requested: bool,
    pub remote: ListenInstance,
    ordinary: VecDeque<WriteJob<C>>,
    reserved: VecDeque<WriteJob<C>>,
}

impl<C> Transport<C> {
    pub fn enqueue(&mut self, job: WriteJob<C>, reserved: bool) {
        if reserved {
            self.reserved.push_back(job);
        } else {
            self.ordinary.push_back(job);
        }
    }

    pub fn remove(&mut self, token: u64) {
        self.ordinary.retain(|job| job.token != token);
        self.reserved.retain(|job| job.token != token);
    }

    #[cfg(test)]
    pub fn queued(&self, token: u64) -> bool {
        self.ordinary
            .iter()
            .chain(&self.reserved)
            .any(|job| job.token == token)
    }
}

pub(crate) struct Coordinated<P: Policy> {
    pub transport: Transport<P::Charge>,
    pub policy: P,
}

// Policy-side convenience only; common engine algorithms use explicit fields.
impl<P: Policy> Deref for Coordinated<P> {
    type Target = P;
    fn deref(&self) -> &P {
        &self.policy
    }
}
impl<P: Policy> DerefMut for Coordinated<P> {
    fn deref_mut(&mut self) -> &mut P {
        &mut self.policy
    }
}

/// Nonowning synchronized state. It never stores an Owner or driver callback.
pub(crate) struct Core<P: Policy> {
    pub inner: Mutex<Coordinated<P>>,
    pub changed: Notify,
    pub stop: Notify,
    dispose: Notify,
    pub stopped: watch::Sender<bool>,
    /// Lock order: visibility -> coordinator -> policy tracking.
    /// Trusted validity samples run outside coordinator, inside visibility.
    pub visibility: Mutex<()>,
}

impl<P: Policy> Core<P> {
    pub fn new(policy: P, remote: ListenInstance) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Coordinated {
                transport: Transport {
                    terminal: None,
                    dispose_requested: false,
                    remote,
                    ordinary: VecDeque::new(),
                    reserved: VecDeque::new(),
                },
                policy,
            }),
            changed: Notify::new(),
            stop: Notify::new(),
            dispose: Notify::new(),
            stopped: watch::channel(false).0,
            visibility: Mutex::new(()),
        })
    }

    pub fn error(&self) -> Option<Error> {
        self.inner.lock().unwrap().transport.terminal.clone()
    }

    pub fn end(&self, error: Error, dispose: bool) {
        let _visibility = self.visibility.lock().unwrap();
        let mut inner = self.inner.lock().unwrap();
        inner.transport.dispose_requested |= dispose;
        if inner.transport.terminal.is_none() {
            inner.transport.terminal = Some(error);
            inner.transport.ordinary.clear();
            inner.transport.reserved.clear();
            inner.policy.retire();
        }
        drop(inner);
        self.stop.notify_waiters();
        if dispose {
            self.dispose.notify_waiters();
        }
        self.changed.notify_waiters();
    }

    async fn terminal(&self) {
        let stop = self.stop.notified();
        tokio::pin!(stop);
        stop.as_mut().enable();
        if self.error().is_none() {
            stop.await;
        }
    }

    async fn disposed(&self) {
        let disposed = self.dispose.notified();
        tokio::pin!(disposed);
        disposed.as_mut().enable();
        if !self.inner.lock().unwrap().transport.dispose_requested {
            disposed.await;
        }
    }
}

/// Owned, inline policy futures: no read-ahead queue and no guard across await.
pub(crate) trait Driver: Send + Sync + 'static {
    type Policy: Policy;

    fn core(&self) -> &Arc<Core<Self::Policy>>;
    fn receive(
        self: Arc<Self>,
        message: Message,
        props: Properties,
        received_at: Instant,
    ) -> impl Future<Output = Result<()>> + Send;
    fn maintain(self: Arc<Self>) -> impl Future<Output = ()> + Send;
}

/// The sole socket/task lifetime owner. Observers only borrow its retained join.
pub(crate) struct Owner<P: Policy> {
    core: Arc<Core<P>>,
    join: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl<P: Policy> Owner<P> {
    pub fn start<S, D>(stream: S, driver: Arc<D>, limits: Limits) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        D: Driver<Policy = P>,
    {
        let core = driver.core().clone();
        let join = tokio::spawn(drive(stream, driver, limits));
        Self {
            core,
            join: tokio::sync::Mutex::new(Some(join)),
        }
    }

    pub async fn closed(&self, deadline: Instant) -> Result<()> {
        timeout_at(deadline, async {
            let mut join = self.join.lock().await;
            if let Some(handle) = join.as_mut() {
                let result = handle.await;
                *join = None;
                result.map_err(|_| Error::Protocol("channel driver task failed"))?;
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
}

impl<P: Policy> Drop for Owner<P> {
    fn drop(&mut self) {
        self.core.end(Error::Closed, false);
    }
}

async fn sending<W, P>(mut writer: W, core: &Core<P>, limits: Limits) -> Result<()>
where
    W: AsyncWrite + Unpin,
    P: Policy,
{
    loop {
        let notified = core.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let job = {
            let mut inner = core.inner.lock().unwrap();
            if inner.transport.terminal.is_some() {
                return Ok(());
            }
            inner
                .transport
                .reserved
                .pop_front()
                .or_else(|| inner.transport.ordinary.pop_front())
        };
        if let Some(job) = job {
            // This local job, including its original charge, owns a selected
            // frame even after policy metadata is retired.
            let WriteJob {
                token,
                bytes,
                deadline,
                valid,
                charge: _charge,
            } = job;
            let result = write_frame(
                &mut writer,
                &bytes,
                deadline,
                limits,
                &core.changed,
                |write| {
                    let _visibility = core.visibility.lock().unwrap();
                    if valid.as_ref().is_some_and(|predicate| !predicate()) {
                        return None;
                    }
                    let inner = core.inner.lock().unwrap();
                    if inner.transport.terminal.is_some()
                        || !inner.policy.eligible(token, Instant::now())
                    {
                        return None;
                    }
                    let result = write();
                    if matches!(result, Poll::Ready(Ok(size)) if size > 0) {
                        inner.policy.started(token);
                    }
                    Some(result)
                },
            )
            .await;
            let receipt = match &result {
                Ok(true) => Receipt::Written,
                Ok(false) => Receipt::Rejected,
                Err(error) => Receipt::Failed(error.clone()),
            };
            core.inner
                .lock()
                .unwrap()
                .policy
                .receipt(token, receipt, Instant::now());
            core.changed.notify_waiters();
            result?;
        } else {
            notified.await;
        }
    }
}

async fn receiving<R: AsyncRead + Unpin, D: Driver>(
    mut reader: R,
    driver: &Arc<D>,
    limits: Limits,
) -> Result<()> {
    loop {
        let message = read_message(&mut reader, limits).await?;
        let received_at = Instant::now();
        let props = envelope(&message, true)?;
        if props.actor == 1 {
            let remote = control_instance(&message, &props)?;
            driver.core().inner.lock().unwrap().transport.remote = remote;
        } else if is_control(props.actor) {
            if let Some(fault) = props.fault.filter(|fault| fault.code != 0) {
                return Err(Error::Remote {
                    code: fault.code,
                    message: "native transport control fault".into(),
                });
            }
            return Err(Error::Protocol(
                "unsupported role/security/reconnect control; no recovery",
            ));
        } else {
            driver.clone().receive(message, props, received_at).await?;
        }
    }
}

struct DriverExit<P: Policy>(Arc<Core<P>>);
impl<P: Policy> Drop for DriverExit<P> {
    fn drop(&mut self) {
        self.0.end(Error::Closed, false);
        self.0.stopped.send_replace(true);
    }
}

async fn drive<S, D>(stream: S, driver: Arc<D>, limits: Limits)
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: Driver,
{
    let core = driver.core();
    let _exit = DriverExit(core.clone());
    let (reader, writer) = tokio::io::split(stream);
    let writing = async {
        // Only whole-socket disposal can cancel the started writer. The reader
        // is also retired; neither half nor a truncated stream can be reused.
        tokio::select! {
            biased;
            _ = core.disposed() => Ok(()),
            result = sending(writer, core, limits) => result,
        }
    };
    tokio::join!(
        join_io(
            receiving(reader, &driver, limits),
            writing,
            core.terminal(),
            |error| core.end(error, false),
            Error::Closed,
        ),
        driver.clone().maintain(),
    );
}

pub(crate) fn is_control(actor: i64) -> bool {
    matches!(actor, 1 | 21 | 29 | 35 | 36)
}

pub(crate) fn control_instance(message: &Message, props: &Properties) -> Result<ListenInstance> {
    if let Some(fault) = props.fault.filter(|fault| fault.code != 0) {
        return Err(Error::Remote {
            code: fault.code,
            message: "native connection control fault".into(),
        });
    }
    if props.actor != 1
        || !props.action.is_empty()
        || props.expects_reply
        || props.uncorrelated_reply
        || props.relates_to != MessageId::default()
    {
        return Err(Error::Protocol("expected native ListenInstance control"));
    }
    ListenInstance::decode(&message.body)
}

/// Caller fields stay unique; native scanning validates every RelatesTo record
/// and exposes the final value (pinned MessageHeaders.cpp:765-824). This is the
/// common Channel envelope, not an added constraint on public raw peer reading.
pub(crate) fn envelope(message: &Message, native_inbound: bool) -> Result<Properties> {
    let ids = [
        message::ACTOR,
        message::ACTION,
        message::MESSAGE_ID,
        message::RELATES_TO,
        message::EXPECTS_REPLY,
        message::UNCORRELATED_REPLY,
        message::FAULT,
        message::HIGH_PRIORITY,
        message::IPC,
    ];
    let mut seen: [Option<&Header>; 9] = [None; 9];
    let mut repeated = [false; 9];
    if message.headers.len() > MAX_HEADER_COUNT {
        return Err(Error::Protocol("header count exceeds limit"));
    }
    for header in &message.headers {
        if let Some(index) = ids.iter().position(|id| *id == header.id) {
            if native_inbound && header.id == message::RELATES_TO {
                continue;
            }
            if let Some(previous) = seen[index] {
                if !native_inbound
                    || !matches!(header.id, message::RELATES_TO | message::EXPECTS_REPLY)
                    || previous.value != header.value
                    || repeated[index]
                {
                    let reason = if native_inbound
                        && repeated[index]
                        && matches!(header.id, message::RELATES_TO | message::EXPECTS_REPLY)
                    {
                        "more than two native correlation headers"
                    } else {
                        match header.id {
                            message::RELATES_TO => "contradictory native RelatesTo header",
                            message::EXPECTS_REPLY => "contradictory native ExpectsReply header",
                            message::MESSAGE_ID => "duplicate native MessageId header",
                            message::ACTOR => "duplicate native Actor header",
                            message::ACTION => "duplicate native Action header",
                            _ => "duplicate or contradictory transport envelope header",
                        }
                    };
                    return Err(Error::Protocol(reason));
                }
                repeated[index] = true;
            }
            seen[index] = Some(header);
        }
    }
    message.properties()
}

pub(crate) fn has(message: &Message, id: u16) -> bool {
    message.headers.iter().any(|header| header.id == id)
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
