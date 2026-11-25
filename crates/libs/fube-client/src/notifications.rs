//! Independently owned native notification sessions over furpc.
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

use fubst::serde::{CountedValues, ObjectArray, Pointer};
use furpc::{
    header, ActorId, Attempt, AttemptDisposition, Body, Channel, FubstDecoder, FubstEncoder,
    HeaderRecord, Headers, IngressLimits, IngressOptions, IngressResponse, IngressService,
    NotificationPolicy, Operation, Peer, Reply, Request, Response, ServiceBinding, Source,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
    time::{timeout_at, Instant},
};
use tower_service::Service;
use uuid::Uuid;

use crate::{
    notification_protocol as wire,
    notification_state::{Progress, Receiver, MAX_CLEANUPS, MAX_FILTERS},
    resolution::Generation,
    FurpcClient, FurpcClientError, GatewayDescription, NamingUri, ServiceEndpoint,
    ServiceEndpointVersion, ServiceKind,
};

#[cfg(test)]
#[path = "notification_session_tests.rs"]
mod tests;

/// Failures scoped to notification clients and their registration guards.
#[derive(Debug, Clone)]
pub enum NotificationError {
    /// An original furpc, native domain, codec, or gateway protocol failure.
    Common(FurpcClientError),
    /// The bounded application queue cannot maintain notification continuity.
    Lagged,
    /// A notification filter, bookkeeping, or command resource was exhausted.
    ResourceLimit(&'static str),
}

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Common(error) => error.fmt(f),
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

impl From<FurpcClientError> for NotificationError {
    fn from(error: FurpcClientError) -> Self {
        Self::Common(error)
    }
}

impl From<furpc::Error> for NotificationError {
    fn from(error: furpc::Error) -> Self {
        FurpcClientError::from_rpc(error).into()
    }
}

impl From<fubst::Error> for NotificationError {
    fn from(error: fubst::Error) -> Self {
        FurpcClientError::Decode(error).into()
    }
}

/// Result for notification construction, registration, delivery, and shutdown.
///
/// ```
/// use fube_client::{FurpcClientError, NotificationError, NotificationResult};
/// let result: NotificationResult<()> = Err(furpc::Error::Timeout("RPC").into());
/// assert!(matches!(result, Err(NotificationError::Common(
///     FurpcClientError::Native(furpc::Error::Timeout("RPC"))
/// ))));
/// ```
pub type NotificationResult<T> = std::result::Result<T, NotificationError>;

/// Independent native-session startup and subsequent operation budgets.
#[derive(Debug, Clone)]
pub struct NotificationClientOptions {
    pub startup_timeout: Duration,
    /// Default for session/filter operations, not the furpc ingress-handler timeout.
    pub default_timeout: Duration,
}

impl Default for NotificationClientOptions {
    fn default() -> Self {
        Self {
            startup_timeout: Duration::from_secs(30),
            default_timeout: Duration::from_secs(30),
        }
    }
}

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

struct Session {
    peer: Peer,
    id: String,
    timeout: Duration,
    state: Mutex<Registrations>,
    cleanup: mpsc::Sender<Cleanup>,
    receiver: Receiver,
    progress: Mutex<Option<Progress>>,
    initialized: Notify,
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
            self.receiver.fail(error.into());
        }
        self.initialized.notify_waiters();
        self.wake.notify_one();
    }

    fn stop(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.filters.clear();
        }
        self.receiver.finish();
        self.initialized.notify_waiters();
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
            return Err(self.receiver.common_error(furpc::Error::Closed));
        }
        let Some(registration) = state.filters.get_mut(&id) else {
            return Err(furpc::Error::Closed.into());
        };
        if *registration != RegistrationState::Active {
            return Err(furpc::Error::Closed.into());
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
        match deadline_after(self.timeout) {
            Ok(deadline) => {
                // Owner shutdown already owns cleanup when admission is closed.
                let _ = self.enqueue(id, deadline, None);
            }
            Err(error) => self.fail(error),
        }
    }

    fn begin_close(&self, deadline: Instant) -> NotificationResult<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(self.receiver.common_error(furpc::Error::Closed));
        }
        state.closing = true;
        let active: Vec<_> = state
            .filters
            .iter()
            .filter(|(_, value)| **value == RegistrationState::Active)
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

    async fn process(&self, page: wire::PageBody) -> NotificationResult<()> {
        let page = page
            .page
            .0
            .ok_or(FurpcClientError::Protocol("null notification page"))?;
        loop {
            let initialized = self.initialized.notified();
            tokio::pin!(initialized);
            initialized.as_mut().enable();
            if self.state.lock().unwrap().closed {
                return Err(self.receiver.common_error(furpc::Error::Closed));
            }
            if let Some(progress) = self.progress.lock().unwrap().as_mut() {
                return progress.commit(page, &self.receiver);
            }
            initialized.await;
        }
    }
}

/// A standalone non-cloneable native session over a dedicated furpc Channel.
///
/// Filters are not retroactive. Register first, resolve initial state separately,
/// then compare versions by partition identity rather than arrival order.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<fube_client::ServiceNotificationClient>();
/// ```
pub struct ServiceNotificationClient {
    session: Arc<Session>,
    worker: Option<JoinHandle<()>>,
    gateway: GatewayDescription,
}

impl ServiceNotificationClient {
    /// Configure an explicit unsecured native endpoint without I/O.
    /// `start` installs notification ingress on the established Channel.
    pub fn endpoint_clear(endpoint: SocketAddr) -> furpc::Result<furpc::Endpoint> {
        FurpcClient::endpoint_clear(endpoint)
    }

    /// Install furpc notification ingress and establish a fresh native session.
    ///
    /// Supply a dedicated Channel with no existing ingress service. Retain no
    /// outside clones if notification-owner drop must release its last owner.
    /// Session/filter requests bind to the initial Peer and never reconnect or
    /// replay. Ingress handler limits/timeouts come from the furpc Endpoint.
    ///
    /// ```
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use fube_client::{NotificationClientOptions, ServiceNotificationClient};
    /// let channel = ServiceNotificationClient::endpoint_clear("127.0.0.1:19000".parse()?)?
    ///     .connect().await?;
    /// let client = ServiceNotificationClient::start(channel, NotificationClientOptions::default()).await?;
    /// client.close(None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start(
        channel: Channel,
        options: NotificationClientOptions,
    ) -> NotificationResult<Self> {
        deadline_after(options.default_timeout)?;
        let deadline = deadline_after(options.startup_timeout)?;
        let peer = channel.peer(deadline).await?;
        let (cleanup, cleanups) = mpsc::channel(MAX_CLEANUPS);
        let session = Arc::new(Session {
            peer,
            id: Uuid::new_v4().to_string(),
            timeout: options.default_timeout,
            state: Mutex::new(Registrations {
                next: 1,
                ..Default::default()
            }),
            cleanup,
            receiver: Receiver::default(),
            progress: Mutex::new(None),
            initialized: Notify::new(),
            wake: Notify::new(),
        });
        let mut candidate = Candidate {
            session: session.clone(),
            armed: true,
        };
        channel.register_service(IngressService::new(
            NotificationIngress {
                session: Arc::downgrade(&session),
            },
            vec![ServiceBinding::actor(
                ActorId::new(5)?,
                IngressOptions::notifications(
                    NotificationPolicy::new(check_notification, check_acknowledgment),
                    IngressLimits::default(),
                ),
            )?],
        )?)?;
        let connected: wire::Connected = request(
            &session,
            "NotificationClientConnectionRequest",
            wire::Connect {
                client: &session.id,
                generation: Generation::default(),
                versions: Pointer(Some(wire::Versions {
                    ranges: ObjectArray(Vec::new()),
                })),
                filters: CountedValues(Vec::new()),
            },
            deadline,
            wire::connected,
            None,
        )
        .await?;
        connected.validate()?;
        let gateway = connected.description();
        *session.progress.lock().unwrap() = Some(Progress::new(connected.generation));
        session.initialized.notify_waiters();
        if let Some(error) = session.receiver.error() {
            return Err(error);
        }
        if Instant::now() >= deadline {
            return Err(furpc::Error::Timeout("notification startup").into());
        }
        let worker_session = session.clone();
        let worker = tokio::spawn(async move {
            let _exit = WorkerExit(worker_session.clone());
            run(worker_session, channel, cleanups).await;
        });
        candidate.armed = false;
        Ok(Self {
            session,
            worker: Some(worker),
            gateway,
        })
    }

    /// Actual native session gateway, which may differ behind a load balancer.
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
                return Err(self.session.receiver.common_error(furpc::Error::Closed));
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
        let attempt = Arc::new(Mutex::new(None));
        let mut mutation = Mutation {
            session: Arc::downgrade(&self.session),
            id,
            attempt: attempt.clone(),
            armed: true,
        };
        let result = request::<_, ()>(
            &self.session,
            "RegisterServiceNotificationFilterRequest",
            wire::Register {
                client: &self.session.id,
                filter: Pointer(Some(wire::Filter {
                    id,
                    name: filter.name.clone(),
                    flags: wire::Flags {
                        bits: u32::from(filter.name_prefix) | (u32::from(filter.primary_only) << 1),
                    },
                })),
            },
            deadline,
            wire::registered,
            Some(attempt),
        )
        .await;
        match result {
            Ok(()) => {
                let mut state = self.session.state.lock().unwrap();
                if state.closed || state.closing {
                    return Err(self.session.receiver.common_error(furpc::Error::Closed));
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
                match &error {
                    FurpcClientError::Domain(_)
                    | FurpcClientError::Native(furpc::Error::RemoteFault(_)) => {
                        mutation.armed = false;
                        self.session.state.lock().unwrap().filters.remove(&id);
                    }
                    FurpcClientError::Native(
                        furpc::Error::InvalidInput(_)
                        | furpc::Error::ResourceLimit(_)
                        | furpc::Error::Timeout(_)
                        | furpc::Error::Cancelled,
                    ) => {}
                    _ => self.session.fail(error.clone()),
                }
                Err(self.session.receiver.common_error(error))
            }
        }
    }

    /// Local cancellation-safe receive. Observed session failure emits once, then None.
    /// An idle physical disconnect is not a furpc Channel closure notification.
    pub async fn recv(&mut self) -> Option<NotificationResult<ServiceNotification>> {
        self.session.receiver.recv().await
    }

    /// Unregister filters under one deadline, stop the worker and release its Channel.
    /// This does not join furpc's physical connection teardown or revoke outside clones.
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
        if let Some(worker) = self.worker.as_mut() {
            match timeout_at(deadline, &mut *worker).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    let error = FurpcClientError::Protocol("notification worker failed");
                    self.session.fail(error.clone());
                    result = Err(error.into());
                }
                Err(_) => {
                    let error = furpc::Error::Timeout("notification close");
                    self.session.fail(error.clone());
                    if worker.await.is_err() {
                        self.session
                            .receiver
                            .fail(FurpcClientError::Protocol("notification worker failed").into());
                    }
                    result = Err(error.into());
                }
            }
        }
        self.worker = None;
        if let Some(error) = self.session.receiver.error() {
            return Err(error);
        }
        result
    }

    async fn join(&mut self) {
        if let Some(worker) = self.worker.as_mut() {
            if worker.await.is_err() {
                self.session
                    .receiver
                    .fail(FurpcClientError::Protocol("notification worker failed").into());
            }
        }
        self.worker = None;
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
/// requires_clone::<fube_client::ServiceNotificationRegistration>();
/// ```
#[must_use = "Keep the registration alive while notifications are needed"]
pub struct ServiceNotificationRegistration {
    session: Weak<Session>,
    id: u64,
    armed: bool,
}

impl ServiceNotificationRegistration {
    pub async fn unregister(mut self, timeout: Option<Duration>) -> NotificationResult<()> {
        let session = self.session.upgrade().ok_or(furpc::Error::Closed)?;
        let deadline = deadline_after(timeout.unwrap_or(session.timeout))?;
        let (sender, receiver) = oneshot::channel();
        session.enqueue(self.id, deadline, Some(sender))?;
        self.armed = false;
        timeout_at(deadline, receiver)
            .await
            .map_err(|_| {
                session
                    .receiver
                    .common_error(furpc::Error::Timeout("notification unregister"))
            })?
            .map_err(|_| session.receiver.common_error(furpc::Error::Closed))?
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

type TrackedAttempt = Arc<Mutex<Option<Attempt>>>;

struct Mutation {
    session: Weak<Session>,
    id: u64,
    attempt: TrackedAttempt,
    armed: bool,
}

impl Drop for Mutation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(session) = self.session.upgrade() {
            let attempt = self.attempt.lock().unwrap();
            if let Some(attempt) = attempt.as_ref() {
                attempt.cancel_uncommitted();
                if matches!(
                    attempt.disposition(),
                    AttemptDisposition::Committed
                        | AttemptDisposition::LocallyWritten
                        | AttemptDisposition::Retired { committed: true }
                ) {
                    session.fail(FurpcClientError::Protocol(
                        "notification registration outcome is uncertain",
                    ));
                    return;
                }
            }
            session.state.lock().unwrap().filters.remove(&self.id);
        }
    }
}

struct Candidate {
    session: Arc<Session>,
    armed: bool,
}

impl Drop for Candidate {
    fn drop(&mut self) {
        if self.armed {
            self.session.stop();
        }
    }
}

struct WorkerExit(Arc<Session>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.0.fail(FurpcClientError::Protocol(
            "notification worker stopped unexpectedly",
        ));
    }
}

struct SessionTransport {
    peer: Peer,
    attempt: Option<TrackedAttempt>,
}

impl Service<Request<Body>> for SessionTransport {
    type Response = Response<Body>;
    type Error = furpc::Error;
    type Future = Operation<Response<Body>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let deadline = request.deadline().expect("RpcClient supplies the deadline");
        let operation = self.peer.request(request, deadline);
        if let Some(attempt) = &self.attempt {
            *attempt.lock().unwrap() = Some(operation.attempt());
        }
        operation
    }
}

async fn request<I, O>(
    session: &Session,
    action: &'static str,
    body: I,
    deadline: Instant,
    validate: fn(&Response<Body>) -> std::result::Result<(), FurpcClientError>,
    attempt: Option<TrackedAttempt>,
) -> std::result::Result<O, FurpcClientError>
where
    I: Serialize + Send,
    O: DeserializeOwned + Send,
{
    let response = FurpcClient::new(SessionTransport {
        peer: session.peer.clone(),
        attempt,
    })
    .unary(
        Request::new(body).append_header(priority()?)?,
        FubstEncoder::<I>::default(),
        FubstDecoder::<O>::default(),
        action,
        validate,
        deadline,
    )
    .await?;
    Ok(response.into_body())
}

#[derive(Serialize)]
struct Priority {
    high: bool,
}

fn priority() -> furpc::Result<HeaderRecord> {
    HeaderRecord::native(header::HIGH_PRIORITY, &Priority { high: true })
}

fn deadline_after(timeout: Duration) -> furpc::Result<Instant> {
    if timeout.is_zero() {
        return Err(furpc::Error::InvalidInput(
            "notification timeout must be positive",
        ));
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(furpc::Error::InvalidInput("notification timeout too large"))
}

fn check_notification(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if common.actor.get() != 5
        || !common
            .action()
            .iter()
            .copied()
            .eq("ServiceNotificationRequest".encode_utf16())
        || common.message_id == furpc::MessageId::default()
        || common.relates_to != furpc::MessageId::default()
        || !common.expects_reply
        || !common.uncorrelated_reply
        || common.fault.code != 0
    {
        return Err(furpc::Error::Protocol(
            "unauthorized native notification envelope",
        ));
    }
    Ok(())
}

fn check_acknowledgment(headers: &Headers, _: &Source) -> furpc::Result<()> {
    let common = headers.effective();
    if common.actor.get() != 5
        || !common
            .action()
            .iter()
            .copied()
            .eq("ServiceNotificationReply".encode_utf16())
    {
        return Err(furpc::Error::Protocol("unauthorized notification response"));
    }
    Ok(())
}

#[derive(Clone)]
struct NotificationIngress {
    session: Weak<Session>,
}

impl Service<Request<Body>> for NotificationIngress {
    type Response = IngressResponse;
    type Error = furpc::Error;
    type Future = Pin<Box<dyn Future<Output = furpc::Result<IngressResponse>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<furpc::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let session = self.session.clone();
        Box::pin(async move {
            let session = session.upgrade().ok_or(furpc::Error::Closed)?;
            let mut guard = IngressCall {
                session: session.clone(),
                armed: true,
            };
            let result = async {
                if request
                    .context()
                    .map(|context| context.source().generation())
                    != Some(session.peer.info().generation)
                {
                    return Err(furpc::Error::Protocol(
                        "notification session generation changed",
                    ));
                }
                furpc::server::unary(
                    PageHandler(session.clone()),
                    request,
                    FubstDecoder::<wire::PageBody>::default(),
                    FubstEncoder::<wire::Status>::default(),
                )
                .await
            }
            .await;
            guard.armed = false;
            if let Err(error) = &result {
                session.fail(
                    session
                        .receiver
                        .common_error(FurpcClientError::from_rpc(error.clone())),
                );
            }
            result
        })
    }
}

struct IngressCall {
    session: Arc<Session>,
    armed: bool,
}

impl Drop for IngressCall {
    fn drop(&mut self) {
        if self.armed {
            self.session.fail(furpc::Error::Cancelled);
        }
    }
}

struct PageHandler(Arc<Session>);

impl Service<Request<wire::PageBody>> for PageHandler {
    type Response = Reply<wire::Status>;
    type Error = NotificationError;
    type Future = Pin<Box<dyn Future<Output = NotificationResult<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<NotificationResult<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<wire::PageBody>) -> Self::Future {
        let session = self.0.clone();
        Box::pin(async move {
            if let Err(error) = session.process(request.into_body()).await {
                session.fail(error.clone());
                return Err(error);
            }
            Ok(Reply::new(wire::Status { error: 0 })
                .append_header(header::actor(ActorId::new(5)?)?)?
                .append_header(header::action("ServiceNotificationReply")?)?
                .append_header(priority()?)?)
        })
    }
}

async fn run(session: Arc<Session>, _channel: Channel, mut cleanups: mpsc::Receiver<Cleanup>) {
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
            cleanup = cleanups.recv() => {
                let Some(cleanup) = cleanup else {
                    session.fail(furpc::Error::Closed);
                    break;
                };
                let result = tokio::select! {
                    biased;
                    _ = stopped(&session) => Err(session.receiver.common_error(furpc::Error::Closed)),
                    result = request::<_, ()>(
                        &session,
                        "UnregisterServiceNotificationFilterRequest",
                        wire::Unregister { client: &session.id, id: cleanup.id },
                        cleanup.deadline,
                        wire::unregistered,
                        None,
                    ) => result.map_err(|error| session.receiver.common_error(error)),
                };
                if result.is_ok() {
                    session.state.lock().unwrap().filters.remove(&cleanup.id);
                }
                if let Some(complete) = cleanup.complete {
                    let _ = complete.send(result.clone());
                }
                if let Err(error) = result {
                    session.fail(error);
                    break;
                }
            }
        }
    }
}

async fn stopped(session: &Session) {
    loop {
        let wake = session.wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if session.state.lock().unwrap().closed {
            return;
        }
        wake.await;
    }
}
