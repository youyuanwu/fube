//! Independent clear-TCP native notification sessions with owning filter guards.
use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use mssf_transport::{
    connection::{deadline_after, Connection, ReplyContext, SendStatus},
    message::{Header, Message, HIGH_PRIORITY},
    serialization::serde::{to_vec, CountedValues, ObjectArray, Pointer},
};
use tokio::{
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
    time::{timeout_at, Instant},
};
use uuid::Uuid;

use crate::{
    notification_protocol as wire,
    notification_state::{Memory, Progress, Receiver, MAX_CLEANUPS, MAX_FILTERS, MAX_PAGES},
    resolution::Generation,
    Error, GatewayDescription, NamingUri, Result, ServiceEndpoint, ServiceEndpointVersion,
    ServiceKind,
};

#[cfg(test)]
#[path = "notification_session_tests.rs"]
mod tests;

/// Failures scoped to notification clients and their registration guards.
#[derive(Debug, Clone)]
pub enum NotificationError {
    /// A shared transport, protocol, timeout, input, or native operation failure.
    Common(Error),
    /// Delivery cannot maintain notification continuity within its bounds.
    Lagged,
    /// A notification filter, bookkeeping, or command resource was exhausted.
    ResourceLimit(&'static str),
}

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Common(error) => write!(f, "{error}"),
            Self::Lagged => f.write_str("notification delivery capacity exceeded"),
            Self::ResourceLimit(resource) => {
                write!(f, "notification resource limit: {resource}")
            }
        }
    }
}

impl std::error::Error for NotificationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Common(error) => Some(error),
            Self::Lagged | Self::ResourceLimit(_) => None,
        }
    }
}

impl From<Error> for NotificationError {
    fn from(error: Error) -> Self {
        Self::Common(error)
    }
}

/// Result for notification construction, registration, delivery, and shutdown.
///
/// ```
/// use mssf_client::{Error, NotificationError, NotificationResult};
/// let result: NotificationResult<()> = Err(Error::Timeout.into());
/// assert!(matches!(result, Err(NotificationError::Common(Error::Timeout))));
/// ```
pub type NotificationResult<T> = std::result::Result<T, NotificationError>;

#[derive(Debug, Clone)]
pub struct ServiceNotificationFilter {
    pub name: NamingUri,
    pub name_prefix: bool,
    pub primary_only: bool,
}
/// Native endpoint data; empty endpoints invalidate knowledge, not prove deletion.
#[derive(Debug, Clone)]
pub struct ServiceNotification {
    pub service_name: NamingUri,
    pub partition_id: Uuid,
    pub kind: ServiceKind,
    pub endpoints: Vec<ServiceEndpoint>,
    pub version: ServiceEndpointVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationState {
    Registering,
    Active,
    Cleanup,
}
#[derive(Default)]
struct Registrations {
    filters: HashMap<u64, RegistrationState>,
    next: u64,
    closing: bool,
    closed: bool,
}
struct Cleanup {
    id: u64,
    deadline: Instant,
    complete: Option<oneshot::Sender<NotificationResult<()>>>,
}
struct Incoming {
    body: Vec<u8>,
    context: ReplyContext,
    memory: Memory,
}
struct Session {
    connection: Connection,
    id: String,
    timeout: Duration,
    state: Mutex<Registrations>,
    cleanup: mpsc::Sender<Cleanup>,
    receiver: Arc<Receiver>,
    wake: Notify,
}
impl Session {
    fn fail(&self, error: impl Into<NotificationError>) {
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return;
            }
            state.closed = true;
            state.filters.clear();
        }
        self.receiver.fail(error.into());
        self.connection.abort();
        self.wake.notify_one();
    }
    fn stop(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.filters.clear();
        }
        self.receiver.finish();
        self.connection.abort();
        self.wake.notify_one();
    }
    fn enqueue(
        &self,
        id: u64,
        deadline: Instant,
        complete: Option<oneshot::Sender<NotificationResult<()>>>,
    ) -> NotificationResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.closing {
            return Err(Error::Closed.into());
        }
        let Some(registration) = state.filters.get_mut(&id) else {
            return Err(Error::Closed.into());
        };
        if *registration != RegistrationState::Active {
            return Err(Error::Closed.into());
        }
        *registration = RegistrationState::Cleanup;
        let result = self.cleanup.try_send(Cleanup {
            id,
            deadline,
            complete,
        });
        drop(state);
        if result.is_err() {
            let error = NotificationError::ResourceLimit("automatic filter cleanup");
            self.fail(error.clone());
            return Err(error);
        }
        Ok(())
    }
    fn drop_registration(&self, id: u64) {
        let deadline = match deadline_after(self.timeout) {
            Ok(deadline) => deadline,
            Err(error) => {
                self.fail(error);
                return;
            }
        };
        // Closed/closing means owner shutdown already owns all cleanup.
        let _ = self.enqueue(id, deadline, None);
    }
    fn begin_close(&self, deadline: Instant) -> NotificationResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(self.receiver.common_error(Error::Closed));
        }
        state.closing = true;
        let active: Vec<_> = state
            .filters
            .iter()
            .filter(|(_, v)| **v == RegistrationState::Active)
            .map(|(id, _)| *id)
            .collect();
        for id in active {
            state.filters.insert(id, RegistrationState::Cleanup);
            if self
                .cleanup
                .try_send(Cleanup {
                    id,
                    deadline,
                    complete: None,
                })
                .is_err()
            {
                drop(state);
                let error = NotificationError::ResourceLimit("close cleanup admission");
                self.fail(error.clone());
                return Err(error);
            }
        }
        drop(state);
        self.wake.notify_one();
        Ok(())
    }
}

/// A standalone non-cloneable session, connection, and cancellation-safe receiver.
///
/// Filters are not retroactive. Register first, resolve initial state separately,
/// then compare versions by partition identity rather than arrival order.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<mssf_client::ServiceNotificationClient>();
/// ```
pub struct ServiceNotificationClient {
    session: Arc<Session>,
    worker: Option<JoinHandle<()>>,
    gateway: GatewayDescription,
}
impl ServiceNotificationClient {
    /// Establish an independent unsecured development connection and native session.
    /// No advertised private gateway address is dialed.
    pub async fn connect_clear(
        endpoint: SocketAddr,
        timeout: Duration,
    ) -> NotificationResult<Self> {
        let deadline = deadline_after(timeout)?;
        let receiver = Arc::new(Receiver::default());
        let (pages, incoming) = mpsc::channel(MAX_PAGES);
        let weak_receiver = Arc::downgrade(&receiver);
        let handler = Arc::new(move |request: mssf_transport::connection::ReverseRequest| {
            let receiver = weak_receiver.upgrade().ok_or(Error::Closed)?;
            // Queued input is charged now; shared Serde reserves owned storage
            // before allocation when the worker decodes the page.
            let result = (|| -> NotificationResult<()> {
                let bytes = request.message.body.capacity() + std::mem::size_of::<Incoming>();
                let memory = receiver.budget.reserve(bytes)?;
                pages
                    .try_send(Incoming {
                        body: request.message.body,
                        context: request.context,
                        memory,
                    })
                    .map_err(|error| match error {
                        mpsc::error::TrySendError::Full(_) => {
                            NotificationError::ResourceLimit("notification page queue")
                        }
                        mpsc::error::TrySendError::Closed(_) => Error::Closed.into(),
                    })
            })();
            result.map_err(|error| {
                // Preserve the domain cause before the generic driver retires.
                receiver.fail(error.clone());
                match error {
                    NotificationError::Common(error) => error,
                    _ => Error::ResourceLimit("reverse request ingress"),
                }
            })
        });
        let connection = Connection::connect_duplex_clear(endpoint, deadline, handler)
            .await
            .map_err(|error| receiver.common_error(error))?;
        let mut candidate = Candidate {
            connection: connection.clone(),
            armed: true,
        };
        let id = Uuid::new_v4().to_string();
        let body = to_vec(&wire::Connect {
            client: &id,
            generation: Generation::default(),
            versions: Pointer(Some(wire::Versions {
                ranges: ObjectArray(Vec::new()),
            })),
            filters: CountedValues(Vec::new()),
        })?;
        let connected = request(
            &connection,
            "NotificationClientConnectionRequest",
            body,
            deadline,
            wire::decode_connected,
            SendStatus::default(),
        )
        .await
        .map_err(|error| receiver.common_error(error))?;
        if let Some(error) = receiver.error() {
            return Err(error);
        }
        // A fresh session has no retained partitions to check against the delete
        // watermark. No reconnect/deletion synchronization is implied.
        let gateway = connected.description();
        let (cleanup, cleanups) = mpsc::channel(MAX_CLEANUPS);
        let session = Arc::new(Session {
            connection,
            id,
            timeout,
            state: Mutex::new(Registrations {
                next: 1,
                ..Default::default()
            }),
            cleanup,
            receiver,
            wake: Notify::new(),
        });
        let worker_session = session.clone();
        let exit = WorkerExit(session.clone());
        let worker = tokio::spawn(async move {
            let _exit = exit;
            run(
                worker_session,
                incoming,
                cleanups,
                Progress::new(connected.generation),
            )
            .await;
        });
        candidate.armed = false;
        Ok(Self {
            session,
            worker: Some(worker),
            gateway,
        })
    }

    /// Actual identity returned by this session, which may differ behind a load balancer.
    pub fn gateway(&self) -> &GatewayDescription {
        &self.gateway
    }

    /// Keep the returned guard alive for the desired subscription lifetime.
    pub async fn register_filter(
        &self,
        filter: &ServiceNotificationFilter,
        timeout: Option<Duration>,
    ) -> NotificationResult<ServiceNotificationRegistration> {
        let deadline = deadline_after(timeout.unwrap_or(self.session.timeout))?;
        let id = {
            let mut state = self.session.state.lock().unwrap();
            if state.closed || state.closing {
                return Err(self.session.receiver.common_error(Error::Closed));
            }
            if state.filters.len() == MAX_FILTERS {
                return Err(NotificationError::ResourceLimit("notification filters"));
            }
            let id = state.next;
            state.next = state
                .next
                .checked_add(1)
                .ok_or(NotificationError::ResourceLimit("filter IDs"))?;
            state.filters.insert(id, RegistrationState::Registering);
            id
        };
        let sent = SendStatus::default();
        let mut mutation = Mutation {
            session: Arc::downgrade(&self.session),
            id,
            sent: sent.clone(),
            armed: true,
        };
        let body = to_vec(&wire::Register {
            client: &self.session.id,
            filter: Pointer(Some(wire::Filter {
                id,
                name: filter.name.clone(),
                flags: wire::Flags {
                    bits: u32::from(filter.name_prefix) | (u32::from(filter.primary_only) << 1),
                },
            })),
        })?;
        let result = request(
            &self.session.connection,
            "RegisterServiceNotificationFilterRequest",
            body,
            deadline,
            wire::decode_registered,
            sent,
        )
        .await;
        match result {
            Ok(()) => {
                let mut state = self.session.state.lock().unwrap();
                if state.closed || state.closing {
                    return Err(self.session.receiver.common_error(Error::Closed));
                }
                state.filters.insert(id, RegistrationState::Active);
                mutation.armed = false;
                Ok(ServiceNotificationRegistration {
                    session: Arc::downgrade(&self.session),
                    id,
                    armed: true,
                })
            }
            Err(error) => {
                if matches!(error, Error::Remote { .. }) {
                    mutation.armed = false;
                    self.session.state.lock().unwrap().filters.remove(&id);
                }
                Err(self.session.receiver.common_error(error))
            }
        }
    }

    /// Local receive: cancellation never consumes an event or unregisters filters.
    /// On failure returns one terminal error, then None, even if the queue was full.
    pub async fn recv(&mut self) -> Option<NotificationResult<ServiceNotification>> {
        // A known transport failure takes priority over buffered updates even
        // when the worker has not yet been scheduled to observe driver exit.
        if let Some(error) = self.session.connection.terminal_error() {
            self.session.fail(error);
        }
        self.session.receiver.recv().await
    }

    /// Stop mutations, deregister under one deadline, and await owned task/socket exit.
    /// Socket shutdown still proceeds if cleanup fails or times out.
    pub async fn close(mut self, timeout: Option<Duration>) -> NotificationResult<()> {
        let deadline = match deadline_after(timeout.unwrap_or(self.session.timeout)) {
            Ok(deadline) => deadline,
            Err(error) => {
                self.session.fail(error.clone());
                self.join().await;
                return Err(error.into());
            }
        };
        let mut result = self.session.begin_close(deadline);
        if let Some(mut worker) = self.worker.take() {
            match timeout_at(deadline, &mut worker).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    self.session
                        .fail(Error::Protocol("notification worker failed"));
                    result = Err(Error::Protocol("notification worker failed").into());
                }
                Err(_) => {
                    self.session.fail(Error::Timeout);
                    let _ = worker.await;
                    result = Err(Error::Timeout.into());
                }
            }
        }
        self.session
            .connection
            .close()
            .await
            .map_err(|error| self.session.receiver.common_error(error))?;
        if let Some(error) = self.session.receiver.error() {
            return Err(error);
        }
        result
    }
    async fn join(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
        let _ = self.session.connection.close().await;
    }
}
impl Drop for ServiceNotificationClient {
    fn drop(&mut self) {
        self.session.stop();
    }
}

/// Owns one session-bound filter. Dropping initiates bounded asynchronous cleanup.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<mssf_client::ServiceNotificationRegistration>();
/// ```
#[must_use = "Keep the registration alive while notifications are needed"]
pub struct ServiceNotificationRegistration {
    session: Weak<Session>,
    id: u64,
    armed: bool,
}
impl ServiceNotificationRegistration {
    /// Consume the guard and await confirmation, without a second destructor cleanup.
    pub async fn unregister(mut self, timeout: Option<Duration>) -> NotificationResult<()> {
        let session = self.session.upgrade().ok_or(Error::Closed)?;
        let deadline = deadline_after(timeout.unwrap_or(session.timeout))?;
        let (sender, receiver) = oneshot::channel();
        session.enqueue(self.id, deadline, Some(sender))?;
        self.armed = false;
        timeout_at(deadline, receiver)
            .await
            .map_err(|_| session.receiver.common_error(Error::Timeout))?
            .map_err(|_| session.receiver.common_error(Error::Closed))?
    }
}
impl Drop for ServiceNotificationRegistration {
    fn drop(&mut self) {
        if self.armed {
            if let Some(session) = self.session.upgrade() {
                session.drop_registration(self.id);
            }
        }
    }
}
struct Mutation {
    session: Weak<Session>,
    id: u64,
    sent: SendStatus,
    armed: bool,
}
impl Drop for Mutation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(session) = self.session.upgrade() {
            if self.sent.cancel_unsent() {
                session.fail(Error::Protocol(
                    "notification registration outcome is uncertain",
                ));
            } else {
                session.state.lock().unwrap().filters.remove(&self.id);
            }
        }
    }
}
struct Candidate {
    connection: Connection,
    armed: bool,
}
impl Drop for Candidate {
    fn drop(&mut self) {
        if self.armed {
            self.connection.abort();
        }
    }
}
struct WorkerExit(Arc<Session>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.0
            .fail(Error::Protocol("notification worker stopped unexpectedly"));
    }
}
async fn request<T: Send + 'static>(
    connection: &Connection,
    action: &str,
    body: Vec<u8>,
    deadline: Instant,
    decode: fn(Message) -> Result<T>,
    sent: SendStatus,
) -> Result<T> {
    connection
        .request_decoded_tracked(
            action,
            5,
            vec![
                Header::activity(Uuid::new_v4().to_bytes_le())?,
                Header::version(1, 1)?,
                Header::boolean(HIGH_PRIORITY, true)?,
            ],
            body,
            deadline,
            decode,
            sent,
        )
        .await
}
async fn run(
    session: Arc<Session>,
    mut pages: mpsc::Receiver<Incoming>,
    mut cleanups: mpsc::Receiver<Cleanup>,
    mut progress: Progress,
) {
    loop {
        {
            let state = session.state.lock().unwrap();
            if state.closed {
                break;
            }
            if state.closing && state.filters.is_empty() {
                drop(state);
                session.stop();
                break;
            }
        }
        tokio::select! {
            biased;
            _ = session.wake.notified() => {}
            error = session.connection.closed() => { session.fail(error); break; }
            cleanup = cleanups.recv() => {
                let Some(cleanup)=cleanup else { session.fail(Error::Closed); break; };
                let body=to_vec(&wire::Unregister {client:&session.id,id:cleanup.id});
                let result=(match body {
                    Ok(body)=>request(&session.connection,"UnregisterServiceNotificationFilterRequest",body,cleanup.deadline,wire::decode_unregistered,SendStatus::default()).await,
                    Err(error)=>Err(error),
                }).map_err(|error|session.receiver.common_error(error));
                if result.is_ok() { session.state.lock().unwrap().filters.remove(&cleanup.id); }
                if let Some(complete)=cleanup.complete { let _=complete.send(result.clone()); }
                if let Err(error)=result { session.fail(error); break; }
            }
            incoming = pages.recv() => {
                let Some(mut incoming)=incoming else { session.fail(Error::Closed); break; };
                let decoded = wire::decode_page(&incoming.body, &mut |bytes| incoming.memory.grow(bytes));
                // Release input before commit releases the page reservation;
                // an ACK wait must not retain unaccounted raw page storage.
                drop(incoming.body);
                let result=decoded.and_then(|page|progress.commit(page,incoming.memory,&session.receiver));
                if let Err(error)=&result { session.receiver.fail(error.clone()); }
                let ack=match (wire::status(result.is_ok()),deadline_after(session.timeout)) {
                    (Ok(body),Ok(deadline))=>incoming.context.reply(body,deadline).await,
                    (Err(error),_)|(_,Err(error))=>Err(error),
                }.map_err(|error|session.receiver.common_error(error));
                if let Err(error)=result.and(ack) { session.fail(error); break; }
            }
        }
    }
    session.connection.abort();
    let _ = session.connection.close().await;
}
