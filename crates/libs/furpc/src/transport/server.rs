use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    future::{poll_fn, Future},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{watch, Semaphore},
    time::{timeout_at, Instant},
};
use tower_service::Service;

use super::{session, validate_timeout, Accept, BoxFuture, ConnectionOptions, TransportLimits};
use crate::{
    connection::Role,
    service::{Registry, StoredService},
    Body, BoxError, Error, HandlerLimits, IngressResponse, IngressService, Peer, Request, Result,
    SendPolicy,
};

type HookFactory = Box<dyn FnMut(Peer) -> BoxFuture<Result<()>> + Send>;

#[derive(Clone)]
pub(crate) struct ReadyHook {
    pub(crate) policy: SendPolicy,
    timeout: Duration,
    factory: Arc<Mutex<HookFactory>>,
}
impl ReadyHook {
    pub(crate) fn run(&self, peer: Peer) -> BoxFuture<Result<()>> {
        let deadline = Instant::now() + self.timeout;
        let future = (self.factory.lock().unwrap())(peer);
        Box::pin(async move {
            timeout_at(deadline, future)
                .await
                .map_err(|_| Error::Timeout("peer ready"))?
        })
    }
}

pub struct Server {
    config: ServerBuilder,
}
pub struct ServerBuilder {
    limits: TransportLimits,
    handler_limits: HandlerLimits,
    handshake_timeout: Duration,
    peer_ready_timeout: Duration,
    drain_timeout: Duration,
    service: Option<Arc<StoredService>>,
    hook: Option<ReadyHook>,
}
impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            limits: TransportLimits::default(),
            handler_limits: HandlerLimits::default(),
            handshake_timeout: Duration::from_secs(30),
            peer_ready_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(30),
            service: None,
            hook: None,
        }
    }
}
impl ServerBuilder {
    pub fn limits(mut self, limits: TransportLimits) -> Self {
        self.limits = limits;
        self
    }
    pub fn handler_limits(mut self, limits: HandlerLimits) -> Self {
        self.handler_limits = limits;
        self
    }
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }
    pub fn peer_ready_timeout(mut self, timeout: Duration) -> Self {
        self.peer_ready_timeout = timeout;
        self
    }
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
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
    pub fn on_peer_ready<S>(mut self, policy: SendPolicy, service: S) -> Result<Self>
    where
        S: Service<Peer, Response = ()> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        if self.hook.is_some() {
            return Err(Error::InvalidInput("peer-ready hook already installed"));
        }
        self.hook = Some(ReadyHook {
            policy,
            timeout: self.peer_ready_timeout,
            factory: Arc::new(Mutex::new(Box::new(move |peer| {
                let mut service = service.clone();
                Box::pin(async move {
                    poll_fn(|cx| service.poll_ready(cx))
                        .await
                        .map_err(|error| Error::handler("peer-ready readiness", error))?;
                    service
                        .call(peer)
                        .await
                        .map_err(|error| Error::handler("peer ready", error))
                })
            }))),
        });
        Ok(self)
    }
    pub fn build(mut self) -> Result<Server> {
        self.limits.validate()?;
        self.handler_limits.validate()?;
        for timeout in [
            self.handshake_timeout,
            self.peer_ready_timeout,
            self.drain_timeout,
        ] {
            validate_timeout(timeout)?;
        }
        if let Some(hook) = &mut self.hook {
            hook.timeout = self.peer_ready_timeout;
        }
        Ok(Server { config: self })
    }
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Owns acceptance and connection/service progress. Signal then await for
    /// graceful drain; dropping this future cancels work, without a join promise.
    /// Clean finite-source exhaustion is not a shutdown signal.
    pub async fn serve_with_shutdown<A, F>(
        self,
        acceptor: A,
        options: ConnectionOptions,
        signal: F,
    ) -> Result<()>
    where
        A: Accept,
        F: Future<Output = ()> + Send + 'static,
    {
        let config = self.config;
        let registry = Registry::new(
            config.service,
            config.handler_limits,
            options,
            &config.limits,
        )?;
        let connections = Arc::new(Semaphore::new(config.limits.max_connections));
        let handshakes = Arc::new(Semaphore::new(config.limits.max_handshakes));
        let (stop, _) = watch::channel(false);
        let mut sessions: FuturesUnordered<BoxFuture<Result<()>>> = FuturesUnordered::new();
        let mut acceptor = Some(acceptor);
        tokio::pin!(signal);
        loop {
            if acceptor.is_none() && sessions.is_empty() {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = &mut signal => {
                    drop(acceptor.take());
                    registry.stop(false);
                    stop.send_replace(true);
                    let deadline = Instant::now() + config.drain_timeout;
                    let result = timeout_at(deadline, async {
                        while let Some(result) = sessions.next().await {
                            if let Err(error) = result {
                                if matches!(error, Error::Timeout("drain")) {
                                    return Err(Error::Timeout("shutdown drain"));
                                }
                                registry.diagnose("draining connection", &error);
                            }
                        }
                        Ok(())
                    }).await.map_err(|_| Error::Timeout("shutdown drain")).and_then(|result| result);
                    if let Err(error) = &result { registry.fail(error.clone()); }
                    return result;
                }
                result = sessions.next(), if !sessions.is_empty() => {
                    if let Some(Err(error)) = result { registry.diagnose("accepted connection", &error); }
                }
                result = async {
                    let connection = connections.clone().acquire_owned().await
                        .map_err(|_| Error::Closed)?;
                    let handshake = handshakes.clone().acquire_owned().await
                        .map_err(|_| Error::Closed)?;
                    let accepted = acceptor.as_mut().expect("enabled acceptor").accept().await
                        .map_err(|error| Error::transport("accept", error))?;
                    Ok::<_, Error>((accepted, connection, handshake))
                }, if acceptor.is_some() => {
                    let (accepted, connection, handshake) = match result {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            registry.fail(error.clone());
                            return Err(error);
                        }
                    };
                    match accepted {
                        None => { acceptor = None; }
                        Some(accepted) => {
                            let registry = registry.clone();
                            let limits = config.limits.clone();
                            let hook = config.hook.clone();
                            let handshake_deadline = Instant::now() + config.handshake_timeout;
                            let drain_timeout = config.drain_timeout;
                            let mut stop = stop.subscribe();
                            sessions.push(Box::pin(async move {
                                let _connection = connection;
                                let session = tokio::select! {
                                    biased;
                                    _ = stop.changed() => return Ok(()),
                                    session = session::establish(
                                        accepted.io, accepted.info, Role::Accepted, registry,
                                        &limits, drain_timeout, handshake_deadline,
                                    ) => session?,
                                };
                                drop(handshake);
                                session.run(hook, stop).await
                            }));
                        }
                    }
                }
            }
        }
    }
}
