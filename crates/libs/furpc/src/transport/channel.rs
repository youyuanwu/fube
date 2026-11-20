use std::{
    future::poll_fn,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::FutureExt;
use http::Uri;
use tokio::{
    sync::{oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore},
    task::AbortHandle,
    time::{timeout_at, Instant},
};
use tower_service::Service;

use super::{
    io::{tcp_destination, validate_uri},
    session, validate_timeout, BoxFuture, Connected, ConnectionOptions, ListenerConfig,
    TcpConnector, TransportIo, TransportLimits,
};
use crate::{
    connection::Role,
    peer::{Generation, Reservation},
    service::{Registry, StoredService},
    Body, BoxError, Error, HandlerLimits, IngressResponse, IngressService, Operation, Peer,
    Request, Response, Result,
};

/// Inert addressing and transport configuration; constructing it does no I/O.
#[derive(Clone)]
pub struct Endpoint {
    uri: Uri,
    limits: TransportLimits,
    handler_limits: HandlerLimits,
    connect_timeout: Duration,
    options: ConnectionOptions,
    listener: ListenerConfig,
    service: Option<Arc<StoredService>>,
}
impl TryFrom<Uri> for Endpoint {
    type Error = Error;
    fn try_from(uri: Uri) -> Result<Self> {
        validate_uri(&uri)?;
        Ok(Self {
            uri,
            limits: TransportLimits::default(),
            handler_limits: HandlerLimits::default(),
            connect_timeout: Duration::from_secs(30),
            options: ConnectionOptions::managed(),
            listener: ListenerConfig::default(),
            service: None,
        })
    }
}
impl TryFrom<SocketAddr> for Endpoint {
    type Error = Error;
    fn try_from(address: SocketAddr) -> Result<Self> {
        if matches!(address, SocketAddr::V6(address) if address.scope_id() != 0 || address.flowinfo() != 0)
        {
            return Err(Error::InvalidInput(
                "scoped IPv6 address cannot be represented losslessly",
            ));
        }
        let endpoint = Self::from_shared(format!("tcp://{address}"))?;
        tcp_destination(&endpoint.uri)?;
        Ok(endpoint)
    }
}
impl Endpoint {
    pub fn from_shared(uri: impl Into<Bytes>) -> Result<Self> {
        let bytes = uri.into();
        if bytes.contains(&b'#') {
            return Err(Error::InvalidInput("URI fragments are not supported"));
        }
        let uri = Uri::from_maybe_shared(bytes)
            .map_err(|_| Error::InvalidInput("invalid endpoint URI"))?;
        Self::try_from(uri)
    }
    pub fn uri(&self) -> &Uri {
        &self.uri
    }
    pub fn limits(mut self, limits: TransportLimits) -> Self {
        self.limits = limits;
        self
    }
    pub fn handler_limits(mut self, limits: HandlerLimits) -> Self {
        self.handler_limits = limits;
        self
    }
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.options = options;
        self
    }
    /// Advertise a consumer-owned listener without changing the destination or I/O metadata.
    pub fn listen_address(mut self, address: impl Into<String>) -> Result<Self> {
        self.listener = self.listener.listen_address(address.into())?;
        Ok(self)
    }
    /// Select the local listener instance; requires listen_address before connect.
    /// This configures new connections, not a live connection reset.
    pub fn instance(mut self, instance: u64) -> Self {
        self.listener = self.listener.instance(instance);
        self
    }
    pub fn service<S>(mut self, service: IngressService<S>) -> Result<Self>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        if self.service.is_some() {
            return Err(Error::InvalidInput("ingress service already installed"));
        }
        self.service = Some(service.erase());
        Ok(self)
    }

    pub async fn connect(&self) -> Result<Channel> {
        tcp_destination(&self.uri)?;
        self.connect_with_connector(TcpConnector::new()).await
    }

    /// Retains this Send-only service for fresh I/O on initial/shared reconnect
    /// attempts. Each attempt readies and calls the same instance with the full
    /// original URI. It must remain reusable after call-future cancellation.
    pub async fn connect_with_connector<C, I>(&self, connector: C) -> Result<Channel>
    where
        C: Service<Uri, Response = Connected<I>> + Send + 'static,
        C::Future: Send + 'static,
        C::Error: Into<BoxError>,
        I: TransportIo,
    {
        self.limits.validate()?;
        self.handler_limits.validate()?;
        validate_timeout(self.connect_timeout)?;
        self.listener.validate(Role::Initiating, &self.limits)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| Error::transport("runtime", error))?;
        let attempt = Arc::new(Establishment::new());
        let logical = Arc::new(Logical {
            state: Mutex::new(LogicalState {
                current: Weak::new(),
                attempt: attempt.clone(),
            }),
            registry: Registry::new(
                self.service.clone(),
                self.handler_limits.clone(),
                self.options,
                &self.limits,
            )?,
            waiters: Arc::new(Semaphore::new(self.limits.max_peer_waiters)),
            demand: Notify::new(),
        });
        let (ready, result) = oneshot::channel();
        let task_logical = logical.clone();
        let endpoint = self.clone();
        let task = runtime.spawn(async move {
            if AssertUnwindSafe(supervise(endpoint, connector, task_logical.clone(), ready))
                .catch_unwind()
                .await
                .is_err()
            {
                task_logical.registry.stop(true);
                let error = Error::Internal("connection supervisor panicked");
                task_logical.registry.diagnose("supervision", &error);
                task_logical
                    .state
                    .lock()
                    .unwrap()
                    .attempt
                    .complete(Err(error));
            }
        });
        let channel = Channel {
            owner: Arc::new(Owner {
                logical,
                abort: task.abort_handle(),
            }),
            ready: Mutex::new(Ready::default()),
        };
        drop(task);
        result
            .await
            .map_err(|error| Error::transport("supervision", error))??;
        Ok(channel)
    }
}

struct Establishment {
    result: Mutex<Option<Result<Weak<Generation>>>>,
    changed: Notify,
}
impl Establishment {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            changed: Notify::new(),
        }
    }
    fn complete(&self, result: Result<Weak<Generation>>) {
        let mut current = self.result.lock().unwrap();
        if current.is_none() {
            *current = Some(result);
        }
        drop(current);
        self.changed.notify_waiters();
    }
    async fn wait(&self) -> Result<Weak<Generation>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self.result.lock().unwrap().clone() {
                return result;
            }
            changed.await;
        }
    }
}
struct LogicalState {
    current: Weak<Generation>,
    attempt: Arc<Establishment>,
}
struct Logical {
    state: Mutex<LogicalState>,
    registry: Arc<Registry>,
    waiters: Arc<Semaphore>,
    demand: Notify,
}
struct Owner {
    logical: Arc<Logical>,
    abort: AbortHandle,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.logical.registry.stop(true);
        self.logical
            .state
            .lock()
            .unwrap()
            .attempt
            .complete(Err(Error::Closed));
        self.abort.abort();
    }
}

/// Cheap-clone logical ownership. Peer capabilities never retain this owner.
/// Retain a Channel while its native operations or reverse ingress are needed.
pub struct Channel {
    owner: Arc<Owner>,
    // get_mut polls readiness without a lock; the mutex keeps Channel Sync.
    ready: Mutex<Ready>,
}
#[derive(Default)]
struct Ready {
    waiting: Option<BoxFuture<Prepared>>,
    value: Option<Prepared>,
}
enum Prepared {
    Reserved(Reservation),
    Failed(Error, Option<OwnedSemaphorePermit>),
}
impl Clone for Channel {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            ready: Mutex::new(Ready::default()),
        }
    }
}
impl Channel {
    pub async fn peer(&self, deadline: Instant) -> Result<Peer> {
        if Instant::now() >= deadline {
            return Err(Error::Timeout("peer acquisition"));
        }
        timeout_at(deadline, self.peer_inner())
            .await
            .map_err(|_| Error::Timeout("peer acquisition"))?
    }

    fn current(&self) -> Result<Option<Peer>> {
        let logical = &self.owner.logical;
        if logical.registry.is_stopping() {
            return Err(Error::Closed);
        }
        Ok(logical
            .state
            .lock()
            .unwrap()
            .current
            .upgrade()
            .filter(|core| core.is_open())
            .map(|core| core.peer(None)))
    }
    fn demand(&self) -> Arc<Establishment> {
        let logical = &self.owner.logical;
        let attempt = {
            let mut state = logical.state.lock().unwrap();
            if state.current.upgrade().is_some_and(|core| core.is_open()) {
                return state.attempt.clone();
            }
            let completed = state.attempt.result.lock().unwrap().is_some();
            if completed {
                state.attempt = Arc::new(Establishment::new());
            }
            state.attempt.clone()
        };
        logical.demand.notify_one();
        attempt
    }
    async fn peer_inner(&self) -> Result<Peer> {
        if let Some(peer) = self.current()? {
            return Ok(peer);
        }
        let logical = &self.owner.logical;
        let attempt = self.demand();
        let _waiter = logical
            .waiters
            .try_acquire()
            .map_err(|_| Error::ResourceLimit("peer waiters"))?;
        let current = attempt.wait().await?;
        let current = current
            .upgrade()
            .filter(|current| current.is_open())
            .ok_or(Error::Closed)?;
        Ok(current.peer(None))
    }

    async fn prepare(&self) -> Prepared {
        match self.current() {
            Ok(Some(peer)) => {
                return match peer.reserve() {
                    Ok(reservation) => Prepared::Reserved(reservation),
                    Err(error) => self.failed(error, None),
                }
            }
            Err(error) => return self.failed(error, None),
            Ok(None) => {}
        }
        let attempt = self.demand();
        let waiter = match self.owner.logical.waiters.clone().try_acquire_owned() {
            Ok(waiter) => waiter,
            Err(_) => return Prepared::Failed(Error::ResourceLimit("RPC readiness waiters"), None),
        };
        let result = attempt
            .wait()
            .await
            .and_then(|core| core.upgrade().ok_or(Error::Closed))
            .and_then(|core| core.peer(None).reserve());
        match result {
            Ok(reservation) => Prepared::Reserved(reservation),
            Err(error) => self.failed(error, Some(waiter)),
        }
    }
    fn failed(&self, error: Error, permit: Option<OwnedSemaphorePermit>) -> Prepared {
        match permit.or_else(|| self.owner.logical.waiters.clone().try_acquire_owned().ok()) {
            Some(permit) => Prepared::Failed(error, Some(permit)),
            None => Prepared::Failed(Error::ResourceLimit("RPC readiness outcomes"), None),
        }
    }
    pub fn register_service<S>(&self, service: IngressService<S>) -> Result<()>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        self.owner.logical.registry.install(service.erase())
    }
}

impl Service<Request<Body>> for Channel {
    type Response = Response<Body>;
    type Error = Error;
    type Future = BoxFuture<Result<Response<Body>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if !self.owner.logical.registry.managed {
            return Poll::Ready(Err(Error::Unsupported("RPC in raw-only mode")));
        }
        if self.owner.logical.registry.is_stopping() {
            *self.ready.get_mut().unwrap() = Ready::default();
            return Poll::Ready(Err(Error::Closed));
        }
        if self.ready.get_mut().unwrap().value.is_some() {
            return Poll::Ready(Ok(()));
        }
        if self.ready.get_mut().unwrap().waiting.is_none() {
            let channel = self.clone();
            self.ready.get_mut().unwrap().waiting =
                Some(Box::pin(async move { channel.prepare().await }));
        }
        let ready = self.ready.get_mut().unwrap();
        match ready
            .waiting
            .as_mut()
            .expect("readiness future")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(value) => {
                ready.waiting = None;
                ready.value = Some(value);
                Poll::Ready(Ok(()))
            }
        }
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let ready = self.ready.get_mut().unwrap().value.take();
        self.ready.get_mut().unwrap().waiting = None;
        let owner = self.owner.clone();
        Box::pin(async move {
            let _owner = owner;
            match ready {
                Some(Prepared::Reserved(reservation)) => {
                    let deadline = request
                        .deadline()
                        .ok_or(Error::InvalidInput("byte RPC requires a deadline"))?;
                    Operation::request(
                        reservation.peer,
                        request,
                        deadline,
                        Some(reservation.slot),
                        None,
                    )
                    .await
                }
                Some(Prepared::Failed(error, _permit)) => Err(error),
                None => Err(Error::InvalidInput("Channel call without readiness")),
            }
        })
    }
}

async fn supervise<C, I>(
    endpoint: Endpoint,
    mut connector: C,
    logical: Arc<Logical>,
    ready: oneshot::Sender<Result<()>>,
) where
    C: Service<Uri, Response = Connected<I>> + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
    I: TransportIo,
{
    let mut ready = Some(ready);
    loop {
        if logical.registry.is_stopping() {
            return;
        }
        let demand = logical.demand.notified();
        tokio::pin!(demand);
        demand.as_mut().enable();
        let attempt = {
            let state = logical.state.lock().unwrap();
            if state.attempt.result.lock().unwrap().is_none() {
                Some(state.attempt.clone())
            } else {
                None
            }
        };
        let Some(attempt) = attempt else {
            demand.await;
            continue;
        };
        let deadline = Instant::now() + endpoint.connect_timeout;
        let session = timeout_at(deadline, async {
            poll_fn(|cx| connector.poll_ready(cx))
                .await
                .map_err(|error| Error::transport("connector readiness", error))?;
            let connected = connector
                .call(endpoint.uri.clone())
                .await
                .map_err(|error| Error::transport("connector", error))?;
            session::establish(
                connected.io,
                connected.info,
                Role::Initiating,
                &endpoint.listener,
                logical.registry.clone(),
                &endpoint.limits,
                Duration::from_secs(30),
                deadline,
            )
            .await
        })
        .await
        .map_err(|_| Error::Timeout("establishment"))
        .and_then(|result| result);
        let session = match session {
            Ok(session) => session,
            Err(error) => {
                attempt.complete(Err(error.clone()));
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error));
                    return;
                }
                logical.registry.diagnose("reconnect", &error);
                continue;
            }
        };
        let current = Arc::downgrade(&session.core);
        {
            let mut state = logical.state.lock().unwrap();
            if logical.registry.is_stopping() {
                return;
            }
            state.current = current.clone();
        }
        attempt.complete(Ok(current.clone()));
        if let Some(ready) = ready.take() {
            if ready.send(Ok(())).is_err() {
                logical.registry.stop(true);
                return;
            }
        }
        let (_stop, stopped) = watch::channel(false);
        if let Err(error) = session.run(None, stopped).await {
            logical.registry.diagnose("dialed connection", &error);
        }
        let mut state = logical.state.lock().unwrap();
        if state.current.ptr_eq(&current) {
            state.current = Weak::new();
        }
    }
}
