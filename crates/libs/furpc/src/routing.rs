use std::{
    sync::Arc,
    task::{Context, Poll},
};

use tower_service::Service;

use crate::{
    service::{InvocationState, RouteKey, ServiceFactory},
    transport::BoxFuture,
    ActorId, Body, BoxError, EnvelopeCheck, Error, IngressLimits, IngressOptions, IngressResponse,
    IngressService, NotificationPolicy, Request, RequestPolicy, Result, SendPolicy, ServiceBinding,
};

/// Independent request/reply authorization for one exact Actor/Action route.
#[derive(Clone)]
pub struct RequestRouteOptions(IngressOptions);
impl RequestRouteOptions {
    pub fn new(policy: RequestPolicy, limits: IngressLimits) -> Self {
        Self(IngressOptions::requests(policy, limits))
    }
    pub fn with_send_policy(mut self, policy: SendPolicy) -> Self {
        self.0 = self.0.with_send_policy(policy);
        self
    }
}

/// One-way authorization without native reply authority.
#[derive(Clone)]
pub struct OneWayRouteOptions(IngressOptions);
impl OneWayRouteOptions {
    pub fn new(check: EnvelopeCheck, limits: IngressLimits) -> Self {
        Self(IngressOptions::one_way(check, limits))
    }
    pub fn with_send_policy(mut self, policy: SendPolicy) -> Self {
        self.0 = self.0.with_send_policy(policy);
        self
    }
}

/// Native notification authorization, separate from ordinary route kinds.
#[derive(Clone)]
pub struct NotificationRouteOptions(IngressOptions);
impl NotificationRouteOptions {
    pub fn new(policy: NotificationPolicy, limits: IngressLimits) -> Self {
        Self(IngressOptions::notifications(policy, limits))
    }
    pub fn with_send_policy(mut self, policy: SendPolicy) -> Self {
        self.0 = self.0.with_send_policy(policy);
        self
    }
}

struct Route {
    key: Arc<RouteKey>,
    binding: ServiceBinding,
    factory: ServiceFactory,
}

/// Exact native Actor/Action dispatch to byte-facing Tower services.
///
/// Routes are codec-free configuration. There is no fallback, task, queue,
/// execution pool, or automatic error response. Clones share route prototypes,
/// not live service readiness. Install with [`Router::into_service`].
///
/// ```
/// use furpc::{
///     ActorId, Body, BoxError, IngressLimits, IngressResponse, IngressService,
///     OneWayRouteOptions, Request, Router,
/// };
/// use tower_service::Service;
///
/// fn routes<S>(service: S) -> furpc::Result<IngressService<Router>>
/// where
///     S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
///     S::Future: Send + 'static,
///     S::Error: Into<BoxError>,
/// {
///     Router::new().one_way(
///         ActorId::new(70000)?, "Event",
///         OneWayRouteOptions::new(|_, _| Ok(()), IngressLimits::default()),
///         service,
///     )?.into_service()
/// }
/// ```
#[derive(Clone, Default)]
pub struct Router {
    routes: Arc<Vec<Arc<Route>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a request/reply route. One-way options cannot grant a reply.
    ///
    /// ```compile_fail
    /// use furpc::*;
    /// use tower_service::Service;
    /// fn wrong<S>(service: S)
    /// where
    ///     S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
    ///     S::Future: Send + 'static,
    ///     S::Error: Into<BoxError>,
    /// {
    ///     let options = OneWayRouteOptions::new(|_, _| Ok(()), IngressLimits::default());
    ///     let _ = Router::new().request(ActorId::new(7).unwrap(), "x", options, service);
    /// }
    /// ```
    pub fn request<S>(
        self,
        actor: ActorId,
        action: impl Into<String>,
        options: RequestRouteOptions,
        service: S,
    ) -> Result<Self>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        self.insert(actor, action.into(), options.0, service)
    }

    /// Register a one-way route, independently of request/reply routes.
    ///
    /// ```compile_fail
    /// use furpc::*;
    /// use tower_service::Service;
    /// fn wrong<S>(service: S)
    /// where
    ///     S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
    ///     S::Future: Send + 'static,
    ///     S::Error: Into<BoxError>,
    /// {
    ///     let options = RequestRouteOptions::new(
    ///         RequestPolicy::new(|_, _| Ok(()), |_, _| Ok(())), IngressLimits::default(),
    ///     );
    ///     let _ = Router::new().one_way(ActorId::new(7).unwrap(), "x", options, service);
    /// }
    /// ```
    pub fn one_way<S>(
        self,
        actor: ActorId,
        action: impl Into<String>,
        options: OneWayRouteOptions,
        service: S,
    ) -> Result<Self>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        self.insert(actor, action.into(), options.0, service)
    }

    /// Register a native notification and its service-defined acknowledgment.
    /// Ordinary routes may use the same Actor/Action in their own traffic class.
    ///
    /// ```compile_fail
    /// use furpc::*;
    /// use tower_service::Service;
    /// fn wrong<S>(service: S)
    /// where
    ///     S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
    ///     S::Future: Send + 'static,
    ///     S::Error: Into<BoxError>,
    /// {
    ///     let options = RequestRouteOptions::new(
    ///         RequestPolicy::new(|_, _| Ok(()), |_, _| Ok(())), IngressLimits::default(),
    ///     );
    ///     let _ = Router::new().notification(ActorId::new(7).unwrap(), "x", options, service);
    /// }
    /// ```
    pub fn notification<S>(
        self,
        actor: ActorId,
        action: impl Into<String>,
        options: NotificationRouteOptions,
        service: S,
    ) -> Result<Self>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        self.insert(actor, action.into(), options.0, service)
    }

    fn insert<S>(
        mut self,
        actor: ActorId,
        action: String,
        options: IngressOptions,
        service: S,
    ) -> Result<Self>
    where
        S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
    {
        let mut units = Vec::new();
        units
            .try_reserve_exact(action.encode_utf16().count())
            .map_err(|_| Error::ResourceLimit("route action allocation"))?;
        units.extend(action.encode_utf16());
        let key = Arc::new(RouteKey {
            class: options.class(),
            actor,
            action: units,
        });
        let position = self.routes.binary_search_by(|route| {
            (
                route.key.class,
                route.key.actor.get(),
                route.key.action.as_slice(),
            )
                .cmp(&(key.class, actor.get(), key.action.as_slice()))
        });
        let index = position
            .err()
            .ok_or(Error::InvalidInput("duplicate Actor/Action route"))?;
        let binding = ServiceBinding::route(key.clone(), options)?;
        let route = Arc::new(Route {
            key,
            binding,
            factory: ServiceFactory::new(service),
        });
        if Arc::get_mut(&mut self.routes).is_none() {
            let mut routes = Vec::new();
            routes
                .try_reserve_exact(self.routes.len())
                .map_err(|_| Error::ResourceLimit("route table allocation"))?;
            routes.extend(self.routes.iter().cloned());
            self.routes = Arc::new(routes);
        }
        let routes = Arc::get_mut(&mut self.routes).expect("unique route configuration");
        routes
            .try_reserve(1)
            .map_err(|_| Error::ResourceLimit("route table allocation"))?;
        routes.insert(index, route);
        Ok(self)
    }

    /// Install immutable exact-action bindings; at least one route is required.
    pub fn into_service(self) -> Result<IngressService<Self>> {
        let mut bindings = Vec::new();
        bindings
            .try_reserve_exact(self.routes.len())
            .map_err(|_| Error::ResourceLimit("route binding allocation"))?;
        bindings.extend(self.routes.iter().map(|route| route.binding.clone()));
        IngressService::new(self, bindings)
    }
}

impl Service<Request<Body>> for Router {
    type Response = IngressResponse;
    type Error = Error;
    type Future = BoxFuture<Result<IngressResponse>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let routes = self.routes.clone();
        Box::pin(async move {
            let state = InvocationState::received(&request)?;
            let common = request.headers().effective();
            let index = routes
                .binary_search_by(|route| {
                    (
                        route.key.class,
                        route.key.actor.get(),
                        route.key.action.as_slice(),
                    )
                        .cmp(&(
                            state.class(),
                            common.actor.get(),
                            common.action(),
                        ))
                })
                .map_err(|_| Error::InvalidInput("request has no admitted router target"))?;
            let route = &routes[index];
            state.authorize_route(&route.key)?;
            state
                .run(async {
                    let response = state.route_call(&route.factory, request).await?;
                    state.check()?;
                    Ok(response)
                })
                .await
        })
    }
}
