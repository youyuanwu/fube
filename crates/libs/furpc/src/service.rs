use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    task::Poll,
    time::Duration,
};

use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::{timeout_at, Instant},
};
use tower_service::Service;

use crate::{
    connection::{driver::DropReason, ConnectionError},
    peer::Generation,
    transport::{validate_timeout, BoxFuture},
    wire::headers::HeaderDecodeContext,
    ActorId, Body, BoxError, Error, Headers, Message, Request, RequestContext, Result, SendPolicy,
    Source,
};

pub type EnvelopeCheck = fn(&Headers, &Source) -> Result<()>;

/// Independently authorizes inbound requests and their original-path replies.
#[derive(Clone, Copy)]
pub struct RequestPolicy {
    request: EnvelopeCheck,
    response: EnvelopeCheck,
}
impl RequestPolicy {
    pub fn new(request: EnvelopeCheck, response: EnvelopeCheck) -> Self {
        Self { request, response }
    }
}

#[derive(Debug, Clone)]
pub struct IngressLimits {
    pub max_body_bytes: usize,
}
impl Default for IngressLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: u32::MAX as usize - 12,
        }
    }
}

#[derive(Clone)]
pub struct IngressOptions {
    check: EnvelopeCheck,
    limits: IngressLimits,
    sends: Option<SendPolicy>,
    request: Option<RequestPolicy>,
}
impl IngressOptions {
    pub fn one_way(check: EnvelopeCheck, limits: IngressLimits) -> Self {
        Self {
            check,
            limits,
            sends: None,
            request: None,
        }
    }
    pub fn requests(policy: RequestPolicy, limits: IngressLimits) -> Self {
        Self {
            check: policy.request,
            limits,
            sends: None,
            request: Some(policy),
        }
    }
    pub fn with_send_policy(mut self, policy: SendPolicy) -> Self {
        self.sends = Some(policy);
        self
    }
}

#[derive(Clone)]
pub struct ServiceBinding {
    options: IngressOptions,
}
impl ServiceBinding {
    pub fn raw(options: IngressOptions) -> Result<Self> {
        if options.limits.max_body_bytes > u32::MAX as usize - 12 {
            return Err(Error::InvalidInput(
                "ingress body limit exceeds native frame capacity",
            ));
        }
        Ok(Self { options })
    }
    pub fn actor(actor: ActorId, _options: IngressOptions) -> Result<Self> {
        if matches!(actor.get(), 1 | 29 | 35) {
            return Err(Error::InvalidInput("native control Actor is reserved"));
        }
        Err(Error::Unsupported("Actor dispatcher"))
    }
}

/// Inert, cloneable configuration for the same Tower ingress on either role.
pub struct IngressService<S> {
    service: S,
    binding: ServiceBinding,
}
impl<S: Clone> Clone for IngressService<S> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            binding: self.binding.clone(),
        }
    }
}
impl<S> IngressService<S>
where
    S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    pub fn new(service: S, mut bindings: Vec<ServiceBinding>) -> Result<Self> {
        if bindings.len() != 1 {
            return Err(Error::InvalidInput(
                "exactly one raw ingress binding required",
            ));
        }
        Ok(Self {
            service,
            binding: bindings.pop().expect("one binding"),
        })
    }
    pub fn map_service<T>(self, layer: impl FnOnce(S) -> T) -> IngressService<T>
    where
        T: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
        T::Future: Send + 'static,
        T::Error: Into<BoxError>,
    {
        IngressService {
            service: layer(self.service),
            binding: self.binding,
        }
    }
    pub(crate) fn erase(self) -> Arc<StoredService> {
        Arc::new(StoredService {
            binding: self.binding,
            factory: Mutex::new(Box::new(Prototype(self.service))),
        })
    }
}

#[derive(Debug)]
pub enum IngressResponse {
    Reply(Message),
    NoReply,
}

#[derive(Debug, Clone)]
pub struct HandlerLimits {
    pub max_in_flight: usize,
    pub max_readiness_waiters: usize,
    pub max_encoded_body_bytes_per_response: usize,
    pub timeout: Duration,
}
impl Default for HandlerLimits {
    fn default() -> Self {
        Self {
            max_in_flight: 8,
            max_readiness_waiters: 4,
            max_encoded_body_bytes_per_response: u32::MAX as usize - 12,
            timeout: Duration::from_secs(30),
        }
    }
}
impl HandlerLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_in_flight == 0
            || self.max_in_flight > Semaphore::MAX_PERMITS
            || self.max_readiness_waiters > Semaphore::MAX_PERMITS
        {
            return Err(Error::InvalidInput("invalid handler execution counts"));
        }
        validate_timeout(self.timeout)
    }
}

async fn ready<S, Q>(service: &mut S, readiness: Arc<Semaphore>, phase: &'static str) -> Result<()>
where
    S: Service<Q>,
    S::Error: Into<BoxError>,
{
    let mut waiting = None;
    poll_fn(|cx| match service.poll_ready(cx) {
        Poll::Ready(result) => {
            waiting.take();
            Poll::Ready(result.map_err(|error| Error::handler(phase, error)))
        }
        Poll::Pending => {
            if waiting.is_none() {
                match readiness.clone().try_acquire_owned() {
                    Ok(permit) => waiting = Some(permit),
                    Err(_) => {
                        return Poll::Ready(Err(Error::ResourceLimit("service readiness waiters")))
                    }
                }
            }
            Poll::Pending
        }
    })
    .await
}

trait Factory: Send {
    fn make(
        &mut self,
        request: Request<Body>,
        readiness: Arc<Semaphore>,
    ) -> BoxFuture<Result<IngressResponse>>;
}
struct Prototype<S>(S);
impl<S> Factory for Prototype<S>
where
    S: Service<Request<Body>, Response = IngressResponse> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    fn make(
        &mut self,
        request: Request<Body>,
        readiness: Arc<Semaphore>,
    ) -> BoxFuture<Result<IngressResponse>> {
        let mut service = self.0.clone();
        Box::pin(async move {
            ready(&mut service, readiness, "service readiness").await?;
            service
                .call(request)
                .await
                .map_err(|error| Error::handler("handler", error))
        })
    }
}

pub(crate) struct StoredService {
    binding: ServiceBinding,
    factory: Mutex<Box<dyn Factory>>,
}

pub(crate) struct Registry {
    service: Mutex<Option<Arc<StoredService>>>,
    limits: HandlerLimits,
    active: Arc<Semaphore>,
    readiness: Arc<Semaphore>,
    stopping: AtomicBool,
    generations: Mutex<Vec<Weak<Generation>>>,
    diagnostics: AtomicU64,
    pub(crate) managed: bool,
    pub(crate) rpc_slots: Arc<Semaphore>,
    reply_slots: Arc<Semaphore>,
    max_receive_frame_bytes: usize,
}
impl Registry {
    pub(crate) fn new(
        service: Option<Arc<StoredService>>,
        limits: HandlerLimits,
        options: crate::ConnectionOptions,
        transport: &crate::TransportLimits,
    ) -> Result<Arc<Self>> {
        limits.validate()?;
        let reply_count = limits.max_in_flight;
        Ok(Arc::new(Self {
            service: Mutex::new(service),
            active: Arc::new(Semaphore::new(limits.max_in_flight)),
            readiness: Arc::new(Semaphore::new(
                limits.max_readiness_waiters.min(limits.max_in_flight),
            )),
            limits,
            stopping: AtomicBool::new(false),
            generations: Mutex::new(Vec::new()),
            diagnostics: AtomicU64::new(0),
            managed: options.is_managed(),
            rpc_slots: Arc::new(Semaphore::new(transport.max_pending_requests)),
            reply_slots: Arc::new(Semaphore::new(reply_count)),
            max_receive_frame_bytes: transport.max_receive_frame_bytes,
        }))
    }
    pub(crate) fn install(&self, service: Arc<StoredService>) -> Result<()> {
        let mut current = self.service.lock().unwrap();
        if self.is_stopping() {
            return Err(Error::Closed);
        }
        if current.is_some() {
            return Err(Error::InvalidInput("ingress service already installed"));
        }
        *current = Some(service);
        Ok(())
    }
    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    pub(crate) fn track(&self, generation: &Arc<Generation>) -> Result<()> {
        let mut generations = self.generations.lock().unwrap();
        if self.is_stopping() {
            return Err(Error::Closed);
        }
        generations.retain(|generation| generation.strong_count() != 0);
        generations
            .try_reserve(1)
            .map_err(|_| Error::ResourceLimit("connection bookkeeping"))?;
        generations.push(Arc::downgrade(generation));
        Ok(())
    }
    pub(crate) fn stop(&self, abort: bool) {
        self.stop_with(abort.then_some(Error::Closed));
    }
    pub(crate) fn fail(&self, error: Error) {
        self.stop_with(Some(error));
    }
    fn stop_with(&self, error: Option<Error>) {
        self.stopping.store(true, Ordering::Release);
        let generations = self.generations.lock().unwrap();
        for generation in generations.iter().filter_map(Weak::upgrade) {
            if let Some(error) = &error {
                generation
                    .handle
                    .abort(Arc::new(ConnectionError::Native(error.clone())));
            } else if let Err(error) = generation.handle.drain() {
                self.diagnose("connection drain", &Error::from(error));
            }
        }
    }
    pub(crate) fn diagnose(&self, phase: &'static str, error: &Error) {
        let count = self
            .diagnostics
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if count.is_power_of_two() {
            // Arbitrary error Display text can contain custom URI secrets.
            let (category, detail) = error.diagnostic();
            tracing::warn!(
                phase,
                occurrences = count,
                category,
                detail,
                "furpc scope diagnostic"
            );
        }
    }
    pub(crate) fn admit(
        &self,
        core: &Arc<Generation>,
        headers: &Headers,
        body_bytes: usize,
    ) -> std::result::Result<Dispatch, DropReason> {
        if self.is_stopping() {
            return Err(DropReason::Draining);
        }
        let service = self
            .service
            .lock()
            .unwrap()
            .clone()
            .ok_or(DropReason::NoDestination)?;
        let options = &service.binding.options;
        let common = headers.effective();
        let valid_kind = if options.request.is_some() {
            common.expects_reply && common.message_id != crate::MessageId::default()
        } else {
            !common.expects_reply
        };
        if body_bytes > options.limits.max_body_bytes || !valid_kind {
            self.diagnose(
                "ingress policy",
                &Error::Protocol("one-way envelope/body rejected"),
            );
            return Err(DropReason::PolicyRejected);
        }
        if let Err(error) = (options.check)(headers, &core.source) {
            self.diagnose("ingress authorization", &error);
            return Err(DropReason::PolicyRejected);
        }
        let permit = self
            .active
            .clone()
            .try_acquire_owned()
            .map_err(|_| DropReason::Capacity)?;
        let reply = options
            .request
            .map(|policy| {
                let slot = self
                    .reply_slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| DropReason::Capacity)?;
                Ok((common.message_id, policy.response, slot))
            })
            .transpose()?;
        let deadline = Instant::now() + self.limits.timeout;
        let state = Arc::new(InvocationState {
            live: AtomicBool::new(true),
            stopped: Notify::new(),
            kind: if reply.is_some() {
                InvocationKind::Request
            } else {
                InvocationKind::OneWay
            },
            deadline,
            readiness: self.readiness.clone(),
            max_body_bytes: options.limits.max_body_bytes,
            max_receive_frame_bytes: self.max_receive_frame_bytes,
            max_response_body_bytes: self.limits.max_encoded_body_bytes_per_response,
        });
        Ok(Dispatch {
            service,
            guard: Invocation {
                state,
                _permit: permit,
            },
            reply,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvocationKind {
    Request,
    OneWay,
}

#[derive(Debug)]
pub(crate) struct InvocationState {
    live: AtomicBool,
    stopped: Notify,
    kind: InvocationKind,
    deadline: Instant,
    readiness: Arc<Semaphore>,
    max_body_bytes: usize,
    max_receive_frame_bytes: usize,
    pub(crate) max_response_body_bytes: usize,
}
impl InvocationState {
    pub(crate) fn for_request(request: &Request<Body>, kind: InvocationKind) -> Result<Arc<Self>> {
        let context = request.context().ok_or(Error::InvalidInput(
            "typed ingress requires received context",
        ))?;
        let state = context.invocation.upgrade().ok_or(Error::Closed)?;
        state.check()?;
        if state.kind != kind {
            return Err(Error::InvalidInput("typed ingress exchange kind mismatch"));
        }
        if request.body().len() > state.max_body_bytes {
            return Err(Error::ResourceLimit("ingress body bytes"));
        }
        Message::from_received(request.headers().clone(), request.body().clone()).validate(
            &HeaderDecodeContext::native(),
            state.max_receive_frame_bytes,
        )?;
        Ok(state)
    }

    pub(crate) fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    pub(crate) fn check(&self) -> Result<()> {
        if !self.is_live() {
            return Err(Error::Closed);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::Timeout("handler"));
        }
        Ok(())
    }

    pub(crate) async fn ready<S, Q>(&self, service: &mut S) -> Result<()>
    where
        S: Service<Q>,
        S::Error: Into<BoxError>,
    {
        ready(service, self.readiness.clone(), "typed service readiness").await?;
        self.check()
    }

    pub(crate) async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        let stopped = self.stopped.notified();
        tokio::pin!(stopped);
        // Subscribe before checking liveness so guard retirement cannot be missed.
        stopped.as_mut().enable();
        self.check()?;
        tokio::select! {
            biased;
            _ = stopped => Err(Error::Closed),
            result = timeout_at(self.deadline, future) => {
                result.map_err(|_| Error::Timeout("handler"))?
            }
        }
    }
}

struct Invocation {
    state: Arc<InvocationState>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Invocation {
    fn drop(&mut self) {
        self.state.live.store(false, Ordering::Release);
        self.state.stopped.notify_waiters();
    }
}

pub(crate) struct Dispatch {
    service: Arc<StoredService>,
    guard: Invocation,
    reply: Option<(crate::MessageId, EnvelopeCheck, OwnedSemaphorePermit)>,
}
impl Dispatch {
    pub(crate) fn deadline(&self) -> Instant {
        self.guard.state.deadline
    }

    pub(crate) fn run(self, core: &Arc<Generation>, message: Message) -> BoxFuture<Result<()>> {
        let mut request = Request::new(message.body().clone());
        request.parts.headers = message.headers().clone();
        request.parts.context = Some(RequestContext {
            source: core.source.clone(),
            peer: self
                .service
                .binding
                .options
                .sends
                .map(|policy| core.peer(Some(policy))),
            deadline: self.deadline(),
            invocation: Arc::downgrade(&self.guard.state),
        });
        let readiness = core.registry.readiness.clone();
        let target = Arc::downgrade(core);
        Box::pin(async move {
            let deadline = self.deadline();
            let max_response = self.guard.state.max_response_body_bytes;
            let _guard = self.guard;
            if Instant::now() >= deadline {
                return Err(Error::Timeout("handler"));
            }
            if !request.context().is_some_and(RequestContext::is_live) {
                return Err(Error::Closed);
            }
            let future = self
                .service
                .factory
                .lock()
                .unwrap()
                .make(request, readiness);
            match timeout_at(deadline, future)
                .await
                .map_err(|_| Error::Timeout("handler"))??
            {
                IngressResponse::NoReply if self.reply.is_none() => Ok(()),
                IngressResponse::Reply(message) if self.reply.is_some() => {
                    if message.body().len() > max_response {
                        return Err(Error::ResourceLimit("encoded handler response body"));
                    }
                    let (id, policy, slot) = self.reply.expect("authorized reply");
                    let receipt = target
                        .upgrade()
                        .ok_or(Error::Closed)?
                        .reply(message, id, policy, deadline, slot)?;
                    timeout_at(deadline, receipt)
                        .await
                        .map_err(|_| Error::Timeout("reply write"))?
                        .map_err(Error::from)
                }
                _ => Err(Error::handler(
                    "response contract",
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "missing response or Reply without a reply grant",
                    ),
                )),
            }
        })
    }
}
