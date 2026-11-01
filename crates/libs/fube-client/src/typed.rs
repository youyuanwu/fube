//! Bounded handwritten adapters for ordinary gateway calls.
//!
//! [`GatewayClient`] borrows a foundation; it never clones, adopts or closes its
//! channel. [`crate::FabricClient`] is the separate designated lifecycle owner.
//! Adapters are sealed because an arbitrary synchronous codec cannot promise
//! finite allocation or nonblocking preparation/validation.

use std::{
    fmt,
    mem::size_of,
    sync::{Arc, Mutex},
};

use fube_transport::{
    channel::RpcChannel,
    frame::{MAX_FRAME_SIZE, MAX_HEADER_SIZE},
    message::{self, Header, Message},
};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    contracts::{
        CallContext, Client, ClientBinding, ClientError, ClientFuture, ClientRequest, ReplyError,
    },
    query_protocol::{self, Query},
    resolution::Refresh,
    *,
};

/// Valid native domain failure, distinct from an RPC transport Fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainFailure {
    pub code: i64,
    pub message: String,
}

impl fmt::Display for DomainFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native error {}: {}", self.code, self.message)
    }
}
impl std::error::Error for DomainFailure {}

/// Layered ordinary-operation result; excluded clients still use transport Result.
pub type ClientResult<T> = std::result::Result<T, ClientError<DomainFailure>>;

/// Conservative per-call reservation, not an allocation of this many bytes.
///
/// All accepted inputs (including spare capacity) are <=1 MiB. Wire body <=4 MiB.
/// The sealed schemas contain no recursive user-defined visitors. The static
/// table below checks each allocating element against its minimum consumed wire
/// extent (32 bytes of element per byte). Object-array records consume >=12
/// bytes; counted strings consume >=1. Node's 21 required fields and code
/// package's two required embedded objects strengthen their minimums.
///
/// Per decoded tree, geometric Vec growth/overlap is <=8x element storage
/// (256x wire), including the native pointer-slot conversion. Pairs-to-BTreeMap
/// conversion adds <=192x: at most 2N live/splitting nodes, <=1024 bytes/node,
/// and >=12 wire bytes/pair. The pinned Rust 1.99.1 BTreeMap has 11 key/value
/// slots plus 12 child pointers (alloc/collections/btree/node.rs:43-110).
/// UTF-16 scratch, UTF-8/string growth/copies and redundant NamingUri strings
/// add <=16x; type tags/decoder scratch add <=16x. Sum 480x <512x.
/// At most two allocation trees coexist during validation/map/endpoint
/// conversion. Request/state, encoded/reply wire/header buffers and fixed
/// scratch fit another 16 MiB. Thus 2*512*4 MiB +16 MiB is reserved.
///
/// Collection (4096), string (32768 units), object (4 MiB) and depth (32)
/// limits are checked before allocating. This conservative accounting is not
/// an RSS estimate. Changing a schema or pinned container requires rechecking
/// the table/proof; arbitrary decoder callbacks cannot assert this bound.
pub const CLIENT_SLOT_BYTES: usize = 4 * 1024 * 1024 * 1024 + 16 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 1024 * 1024;

// Array framing is checked before allocating (serialization.rs:397-409).
// Even null native pointer slots occupy their enclosing 12-byte object. The
// two larger records have required, non-optional minimum extents as documented
// above; absent version suffixes do not remove those fields/embedded objects.
macro_rules! element_bound {
    ($type:ty, $minimum:expr) => {
        const _: () = assert!(size_of::<$type>() <= 32 * $minimum);
    };
}
element_bound!(Node, 12 + 21);
element_bound!(DeployedCodePackage, 12 + 2 * 12);
element_bound!(NodeDeactivationTask, 12);
element_bound!(Option<SafetyCheck>, 12);
element_bound!(Application, 12);
element_bound!(Service, 12);
element_bound!(Partition, 12);
element_bound!(Replica, 12);
element_bound!(DeployedApplication, 12);
element_bound!(DeployedServicePackage, 12);
element_bound!(DeployedReplica, 12);
element_bound!(HealthEvent, 12);
element_bound!(Option<HealthEvaluation>, 12);
element_bound!(ServiceEndpoint, 1);
element_bound!(String, 1);
element_bound!((String, String), 12);
const _: () = assert!(22 * size_of::<String>() + 13 * size_of::<usize>() + 16 <= 1024);

/// Fixed finite admission limits. Exhaustion returns immediately, without waiters.
#[derive(Debug, Clone, Copy)]
pub struct ClientLimits {
    pub max_calls: usize,
    pub max_retained_bytes: usize,
}
impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            max_calls: 8,
            max_retained_bytes: 8 * CLIENT_SLOT_BYTES,
        }
    }
}

#[derive(Default)]
struct Usage {
    calls: usize,
    bytes: usize,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct ClientBudget {
    limits: ClientLimits,
    usage: Arc<Mutex<Usage>>,
}
impl ClientBudget {
    fn new(limits: ClientLimits) -> Result<Self> {
        if limits.max_calls == 0
            || limits.max_retained_bytes < CLIENT_SLOT_BYTES
            || limits.max_calls.checked_mul(CLIENT_SLOT_BYTES).is_none()
        {
            return Err(Error::InvalidInput("invalid finite client limits"));
        }
        Ok(Self {
            limits,
            usage: Arc::new(Mutex::new(Usage::default())),
        })
    }
    fn reserve(&self) -> Result<Guard> {
        let mut usage = self.usage.lock().unwrap();
        let bytes = usage
            .bytes
            .checked_add(CLIENT_SLOT_BYTES)
            .ok_or(Error::ResourceLimit("client retained bytes"))?;
        if usage.calls == self.limits.max_calls || bytes > self.limits.max_retained_bytes {
            return Err(Error::ResourceLimit("client retained count/bytes"));
        }
        usage.calls += 1;
        usage.bytes = bytes;
        Ok(Guard(self.clone()))
    }
}
struct Guard(ClientBudget);
impl Drop for Guard {
    fn drop(&mut self) {
        let mut usage = self.0.usage.lock().unwrap();
        usage.calls -= 1;
        usage.bytes -= CLIENT_SLOT_BYTES;
    }
}

/// Explicitly borrowed typed RPC observer. Dropping it does not close workers.
pub struct GatewayClient<'a, C: RpcChannel + ?Sized> {
    binding: ClientBinding<'a, C>,
    budget: ClientBudget,
}
impl<'a, C: RpcChannel + ?Sized> GatewayClient<'a, C> {
    pub fn new(binding: ClientBinding<'a, C>, limits: ClientLimits) -> Result<Self> {
        Ok(Self {
            binding,
            budget: ClientBudget::new(limits)?,
        })
    }
}
impl<C: RpcChannel + ?Sized> Client for GatewayClient<'_, C> {
    type Channel = C;
    fn binding(&self) -> ClientBinding<'_, C> {
        ClientBinding {
            channel: self.binding.channel,
            target: self.binding.target,
        }
    }
    fn call<'a, R: ClientRequest>(
        &'a self,
        request: R,
        context: CallContext,
    ) -> ClientFuture<'a, R::Response, R::DomainError> {
        call(self.binding(), &self.budget, request, context)
    }
}

pub(crate) fn default_budget() -> ClientBudget {
    ClientBudget::new(ClientLimits::default()).expect("valid fixed client defaults")
}

pub(crate) fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::Timeout)
    } else {
        Ok(())
    }
}

pub(crate) fn call<'a, C: RpcChannel + ?Sized, R: ClientRequest>(
    binding: ClientBinding<'a, C>,
    budget: &'a ClientBudget,
    request: R,
    context: CallContext,
) -> ClientFuture<'a, R::Response, R::DomainError> {
    Box::pin(async move {
        check_deadline(context.deadline).map_err(ClientError::Local)?;
        let _guard = budget.reserve().map_err(ClientError::Local)?;
        check_input(&request).map_err(ClientError::Local)?;
        let (message, state) = request.encode(&context).map_err(ClientError::Local)?;
        check_deadline(context.deadline).map_err(ClientError::Local)?;
        let reply = binding
            .channel
            .request(binding.target, message, context.deadline)
            .await
            .map_err(ClientError::Transport)?;
        check_deadline(context.deadline).map_err(ClientError::Local)?;
        validate_reply_headers(&reply, R::forwarded_reply).map_err(|error| match error {
            Error::Protocol(reason) => ClientError::Protocol(reason),
            _ => ClientError::Protocol("invalid reply metadata"),
        })?;
        let result = R::decode(state, reply, &context).map_err(|error| match error {
            ReplyError::Protocol(reason) => ClientError::Protocol(reason),
            ReplyError::Domain(error) => ClientError::Domain(error),
        });
        check_deadline(context.deadline).map_err(ClientError::Local)?;
        result
    })
}

pub(crate) mod sealed {
    pub trait Request {
        fn forwarded_reply(_: &fube_transport::message::Message) -> bool {
            false
        }
        fn input_bytes(&self) -> usize;
    }
    pub trait Parameters {
        fn input_bytes(&self) -> usize;
    }
}

pub(crate) fn check_input<R: sealed::Request>(request: &R) -> Result<()> {
    if request.input_bytes() > MAX_INPUT_BYTES {
        Err(Error::ResourceLimit("typed request owned capacity"))
    } else {
        Ok(())
    }
}

fn wire(
    action: &'static str,
    mut headers: Vec<Header>,
    body: Vec<u8>,
    context: &CallContext,
) -> Result<Message> {
    check_deadline(context.deadline)?;
    headers.push(Header::actor(5)?);
    headers.push(Header::action(action)?);
    headers.push(Header::activity(Uuid::new_v4().to_bytes_le())?);
    headers.push(Header::version(1, 1)?);
    let remaining = context.deadline.saturating_duration_since(Instant::now());
    let ticks = i64::try_from(remaining.as_nanos() / 100)
        .map_err(|_| Error::InvalidInput("timeout exceeds native signed ticks"))?;
    headers.push(Header::timeout(ticks)?);
    Ok(Message { headers, body })
}

fn validate_reply_headers(reply: &Message, allowed: fn(&Message) -> bool) -> Result<()> {
    if reply.body.capacity() > MAX_FRAME_SIZE || reply.headers.len() > message::MAX_HEADER_COUNT {
        return Err(Error::Protocol(
            "typed reply allocation exceeds wire bounds",
        ));
    }
    let mut bytes = 0usize;
    let mut relations: [Option<&Header>; 3] = [None; 3];
    let mut relation_count = 0;
    for (index, header) in reply.headers.iter().enumerate() {
        bytes = bytes
            .checked_add(header.value.capacity() + 4)
            .ok_or(Error::Protocol("reply header overflow"))?;
        if header.id == message::RELATES_TO {
            if relation_count == 3
                || (relation_count == 2 && relations[1].is_some_and(|h| h.value != header.value))
                || header.read(message::MessageId::decode)? == message::MessageId::default()
            {
                return Err(Error::Protocol("invalid typed RelatesTo composition"));
            }
            relations[relation_count] = Some(header);
            relation_count += 1;
            continue;
        }
        // Validate the fields owned by this channel/client profile. Opaque
        // extensions (e.g. native Idempotent 0x8008) remain byte-preserved and
        // confer no retry/ACK/role authority; native composition can repeat
        // them. Do not invent a universal domain-header schema.
        if !matches!(
            header.id,
            message::ACTOR
                | message::ACTION
                | message::MESSAGE_ID
                | message::EXPECTS_REPLY
                | message::FAULT
                | message::HIGH_PRIORITY
                | message::IPC
                | message::UNCORRELATED_REPLY
                | message::ACTIVITY
                | message::TIMEOUT
                | message::CLIENT_VERSION
        ) {
            continue;
        }
        let mut prior = reply.headers[..index].iter().filter(|h| h.id == header.id);
        if let Some(first) = prior.next() {
            // Only the unchanged identical-two ExpectsReply exception remains
            // outside the precisely bounded native forwarded-query relation.
            if prior.next().is_some()
                || header.id != message::EXPECTS_REPLY
                || first.value != header.value
            {
                return Err(Error::Protocol("duplicate typed reply header"));
            }
        }
    }
    if bytes > MAX_HEADER_SIZE {
        return Err(Error::Protocol("reply header bytes exceed limit"));
    }
    if relation_count == 3
        || (relation_count == 2 && relations[0].unwrap().value != relations[1].unwrap().value)
    {
        let props = reply.properties()?;
        let has = |id| reply.headers.iter().any(|h| h.id == id);
        if !allowed(reply)
            || props.fault.is_some()
            || props.expects_reply
            || props.uncorrelated_reply
            || has(message::IPC)
        {
            return Err(Error::Protocol("unauthorized typed forwarded query reply"));
        }
    }
    Ok(())
}

fn decoded<T>(result: Result<T>) -> std::result::Result<T, ReplyError<DomainFailure>> {
    result.map_err(|error| match error {
        Error::Remote { code, message } => ReplyError::Domain(DomainFailure { code, message }),
        Error::Protocol(reason) => ReplyError::Protocol(reason),
        _ => ReplyError::Protocol("malformed typed domain reply"),
    })
}

fn domain_failure(reply: &Message) -> Result<()> {
    let properties = reply.properties()?;
    if properties.action != "ClientOperationFailure" {
        return Ok(());
    }
    if properties.actor != 5 {
        return Err(Error::Protocol(
            "operation failure is not from NamingGateway",
        ));
    }
    // The domain code is required. Native's code-only trailing suffix is valid;
    // a present diagnostic must decode. Excluded Connection policy is unchanged.
    #[derive(serde::Deserialize)]
    struct Failure {
        code: i64,
        #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
        diagnostic: String,
    }
    let Failure { code, diagnostic } =
        fube_transport::serialization::serde::from_slice(&reply.body)?;
    if code == 0 {
        return Err(Error::Protocol(
            "operation failure has no nonzero error code",
        ));
    }
    Err(Error::Remote {
        code,
        message: diagnostic,
    })
}

fn decode_domain<T>(
    reply: &Message,
    decode: impl FnOnce(&Message) -> Result<T>,
) -> std::result::Result<T, ReplyError<DomainFailure>> {
    decoded(domain_failure(reply).and_then(|()| decode(reply)))
}

#[derive(Debug, Clone, Copy)]
pub struct PingRequest;
impl sealed::Request for PingRequest {
    fn input_bytes(&self) -> usize {
        size_of::<Self>()
    }
}
impl ClientRequest for PingRequest {
    type Response = GatewayDescription;
    type ReplyState = ();
    type DomainError = DomainFailure;
    fn encode(self, context: &CallContext) -> Result<(Message, ())> {
        Ok((wire("PingRequest", vec![], vec![], context)?, ()))
    }
    fn decode(
        _: (),
        reply: Message,
        _: &CallContext,
    ) -> std::result::Result<Self::Response, ReplyError<DomainFailure>> {
        decode_domain(&reply, protocol::decode_ping)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NameOperation {
    Exists,
    Create,
    Delete,
}
/// Checked naming input. Wire preparation occurs only inside the typed call.
#[derive(Debug, Clone)]
pub struct NameRequest {
    pub name: NamingUri,
    pub operation: NameOperation,
}
impl sealed::Request for NameRequest {
    fn input_bytes(&self) -> usize {
        size_of::<Self>() + self.name.capacity()
    }
}
/// Naming results distinguish reads from explicit writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameResponse {
    Exists(bool),
    Written,
}
impl ClientRequest for NameRequest {
    type Response = NameResponse;
    type ReplyState = NameOperation;
    type DomainError = DomainFailure;
    fn encode(self, context: &CallContext) -> Result<(Message, NameOperation)> {
        let (action, headers) = match self.operation {
            NameOperation::Exists => ("NameExistsRequest", vec![]),
            NameOperation::Create => ("CreateNameRequest", vec![]),
            NameOperation::Delete => (
                "DeleteNameRequest",
                vec![Header::boolean(protocol::DELETE_NAME_HEADER, true)?],
            ),
        };
        Ok((
            wire(
                action,
                headers,
                protocol::encode_name(self.name.as_str())?,
                context,
            )?,
            self.operation,
        ))
    }
    fn decode(
        operation: NameOperation,
        reply: Message,
        _: &CallContext,
    ) -> std::result::Result<NameResponse, ReplyError<DomainFailure>> {
        decode_domain(&reply, |reply| match operation {
            NameOperation::Exists => protocol::decode_name_exists(reply).map(NameResponse::Exists),
            _ => protocol::decode_name_write(reply).map(|()| NameResponse::Written),
        })
    }
}

impl<P: Query + sealed::Parameters + Send + 'static> sealed::Request for PagedQuery<P> {
    fn forwarded_reply(reply: &Message) -> bool {
        <P::Reply as query_protocol::Reply>::forwarded_reply(reply)
    }
    fn input_bytes(&self) -> usize {
        size_of::<Self>()
            + self.parameters.input_bytes()
            + self
                .paging
                .continuation_token
                .as_ref()
                .map_or(0, String::capacity)
    }
}
impl<P: Query + sealed::Parameters + Send + 'static> sealed::Request for QueryDescription<P> {
    fn forwarded_reply(reply: &Message) -> bool {
        <P::Reply as query_protocol::Reply>::forwarded_reply(reply)
    }
    fn input_bytes(&self) -> usize {
        size_of::<Self>() + self.parameters.input_bytes()
    }
}
macro_rules! queries {
    ($request:ty, $parameters:ty, $response:ty, $s:ident, $paging:expr) => {
        impl ClientRequest for $request {
            type Response = $response;
            type ReplyState = ();
            type DomainError = DomainFailure;
            fn encode($s, context: &CallContext) -> Result<(Message, ())> {
                Ok((wire("QueryRequest", vec![], query_protocol::encode_query(&$s.parameters, $paging)?, context)?, ()))
            }
            fn decode(_: (), reply: Message, _: &CallContext) -> std::result::Result<Self::Response, ReplyError<DomainFailure>> {
                decode_domain(&reply, query_protocol::decode_reply::<$parameters>)
            }
        }
    };
}
queries!(
    NodeQueryDescription,
    NodeQueryParameters,
    NodeListPage,
    self,
    Some(&self.paging)
);
queries!(
    ApplicationQueryDescription,
    ApplicationQueryParameters,
    ApplicationListPage,
    self,
    Some(&self.paging)
);
queries!(
    ServiceQueryDescription,
    ServiceQueryParameters,
    ServiceListPage,
    self,
    Some(&self.paging)
);
queries!(
    PartitionQueryDescription,
    PartitionQueryParameters,
    PartitionListPage,
    self,
    Some(&self.paging)
);
queries!(
    ReplicaQueryDescription,
    ReplicaQueryParameters,
    ReplicaListPage,
    self,
    Some(&self.paging)
);
queries!(
    DeployedApplicationQueryDescription,
    DeployedApplicationQueryParameters,
    DeployedApplicationListPage,
    self,
    Some(&self.paging)
);
queries!(
    NodeHealthQueryDescription,
    NodeHealthQueryParameters,
    NodeHealth,
    self,
    None
);
queries!(
    DeployedServicePackageQueryDescription,
    DeployedServicePackageQueryParameters,
    Vec<DeployedServicePackage>,
    self,
    None
);
queries!(
    DeployedCodePackageQueryDescription,
    DeployedCodePackageQueryParameters,
    Vec<DeployedCodePackage>,
    self,
    None
);
queries!(
    DeployedReplicaQueryDescription,
    DeployedReplicaQueryParameters,
    Vec<DeployedReplica>,
    self,
    None
);
queries!(
    QueryDescription<movement::MovePrimary>,
    movement::MovePrimary,
    (),
    self,
    None
);

macro_rules! inputs {
    ($ty:ty, $s:ident, $bytes:expr) => {
        impl sealed::Parameters for $ty {
            fn input_bytes(&$s) -> usize { $bytes }
        }
    };
}
fn optional(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}
inputs!(NodeQueryParameters, self, optional(&self.node_name));
inputs!(NodeHealthQueryParameters, self, self.node_name.capacity());
inputs!(
    ApplicationQueryParameters,
    self,
    match &self.filter {
        ApplicationListFilter::Name(name) => name.capacity(),
        ApplicationListFilter::TypeName(name) => name.capacity(),
        _ => 0,
    }
);
inputs!(
    ServiceQueryParameters,
    self,
    self.application_name.capacity()
        + match &self.filter {
            ServiceListFilter::Name(name) => name.capacity(),
            ServiceListFilter::TypeName(name) => name.capacity(),
            _ => 0,
        }
);
inputs!(PartitionQueryParameters, self, self.service_name.capacity());
inputs!(ReplicaQueryParameters, self, 0);
inputs!(
    DeployedApplicationQueryParameters,
    self,
    self.node_name.capacity()
        + self
            .application_name
            .as_ref()
            .map_or(0, NamingUri::capacity)
);
inputs!(
    DeployedServicePackageQueryParameters,
    self,
    self.node_name.capacity()
        + self.application_name.capacity()
        + optional(&self.service_manifest_name)
);
inputs!(
    DeployedCodePackageQueryParameters,
    self,
    self.node_name.capacity()
        + self.application_name.capacity()
        + optional(&self.service_manifest_name)
        + optional(&self.code_package_name)
);
inputs!(
    DeployedReplicaQueryParameters,
    self,
    self.node_name.capacity()
        + self.application_name.capacity()
        + optional(&self.service_manifest_name)
);
inputs!(movement::MovePrimary, self, self.target.capacity());

/// Exact selection and minimal previous native refresh state, not a client cache.
#[derive(Debug, Clone)]
pub struct ResolutionRequest {
    name: NamingUri,
    key: PartitionKey,
    previous: Option<Refresh>,
}
impl ResolutionRequest {
    pub fn new(
        name: &NamingUri,
        key: &PartitionKey,
        previous: Option<&ResolvedServicePartition>,
    ) -> Result<Self> {
        key.validate()?;
        if let Some(previous) = previous {
            previous.validate_previous(name, key)?;
        }
        // Clone only checked bounded identity/version metadata, never endpoints.
        Ok(Self {
            name: NamingUri::parse(name.as_str())?,
            key: key.clone(),
            previous: previous.map(|p| p.refresh.clone()),
        })
    }
}
fn key_capacity(key: &PartitionKey) -> usize {
    match key {
        PartitionKey::Named(name) => name.capacity(),
        _ => 0,
    }
}
impl sealed::Request for ResolutionRequest {
    fn input_bytes(&self) -> usize {
        size_of::<Self>()
            + self.name.capacity()
            + key_capacity(&self.key)
            + self.previous.as_ref().map_or(0, |p| {
                p.name.capacity() + p.partition.name.capacity() + p.version.name.capacity()
            })
    }
}
impl ClientRequest for ResolutionRequest {
    type Response = ResolvedServicePartition;
    type ReplyState = (NamingUri, PartitionKey);
    type DomainError = DomainFailure;
    fn encode(self, context: &CallContext) -> Result<(Message, Self::ReplyState)> {
        let body =
            resolution_protocol::encode_refresh(&self.name, &self.key, self.previous.as_ref())?;
        Ok((
            wire("ResolveServiceRequest", vec![], body, context)?,
            (self.name, self.key),
        ))
    }
    fn decode(
        (name, key): Self::ReplyState,
        reply: Message,
        _: &CallContext,
    ) -> std::result::Result<ResolvedServicePartition, ReplyError<DomainFailure>> {
        decoded(domain_failure(&reply).and_then(|()| {
            let result = resolution_protocol::decode(reply)?;
            if result.service_name != name || !key.selects(&result.partition_information) {
                return Err(Error::Protocol(
                    "resolution result does not match requested selection",
                ));
            }
            Ok(result)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fube_transport::channel::{ChannelFuture, MessageChannel};
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
        time::Duration,
    };

    struct Stub {
        calls: AtomicUsize,
        closes: AtomicUsize,
        ready: AtomicBool,
        result: Mutex<Option<Result<Message>>>,
        changed: tokio::sync::Notify,
    }
    impl Stub {
        fn new(ready: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
                ready: AtomicBool::new(ready),
                result: Mutex::new(None),
                changed: tokio::sync::Notify::new(),
            }
        }
    }
    impl MessageChannel for Stub {
        type Target = ();
        fn resolve_target(&self, _: &str) -> Result<Arc<()>> {
            Ok(Arc::new(()))
        }
        fn send_one_way<'a>(&'a self, _: &'a (), _: Message, _: Instant) -> ChannelFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn close(&self, _: Instant) -> ChannelFuture<'_, ()> {
            Box::pin(async {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }
    impl RpcChannel for Stub {
        fn request<'a>(&'a self, _: &'a (), _: Message, _: Instant) -> ChannelFuture<'a, Message> {
            Box::pin(async {
                self.calls.fetch_add(1, Ordering::SeqCst);
                loop {
                    let notified = self.changed.notified();
                    if self.ready.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                self.result.lock().unwrap().take().unwrap_or_else(|| {
                    Ok(Message {
                        headers: vec![],
                        body: vec![],
                    })
                })
            })
        }
    }
    struct Probe {
        encoded: Arc<AtomicUsize>,
        decode_delay: Duration,
    }
    impl sealed::Request for Probe {
        fn input_bytes(&self) -> usize {
            size_of::<Self>()
        }
    }
    impl ClientRequest for Probe {
        type Response = ();
        type ReplyState = Duration;
        type DomainError = DomainFailure;
        fn encode(self, _: &CallContext) -> Result<(Message, Duration)> {
            self.encoded.fetch_add(1, Ordering::SeqCst);
            Ok((
                Message {
                    headers: vec![],
                    body: vec![],
                },
                self.decode_delay,
            ))
        }
        fn decode(
            delay: Duration,
            _: Message,
            _: &CallContext,
        ) -> std::result::Result<(), ReplyError<DomainFailure>> {
            std::thread::sleep(delay);
            Ok(())
        }
    }
    fn context() -> CallContext {
        CallContext {
            deadline: Instant::now() + Duration::from_secs(5),
        }
    }
    fn probe(encoded: &Arc<AtomicUsize>) -> Probe {
        Probe {
            encoded: encoded.clone(),
            decode_delay: Duration::ZERO,
        }
    }
    async fn pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn lazy_deadline_and_nonowning_observer() {
        let channel = Stub::new(true);
        let client = GatewayClient::new(
            ClientBinding {
                channel: &channel,
                target: &(),
            },
            ClientLimits::default(),
        )
        .unwrap();
        let encoded = Arc::new(AtomicUsize::new(0));
        let unpolled = client.call(probe(&encoded), context());
        assert_eq!(encoded.load(Ordering::SeqCst), 0);
        assert_eq!(channel.calls.load(Ordering::SeqCst), 0);
        drop(unpolled);
        assert!(matches!(
            client
                .call(
                    probe(&encoded),
                    CallContext {
                        deadline: Instant::now()
                    }
                )
                .await,
            Err(ClientError::Local(Error::Timeout))
        ));
        assert_eq!(encoded.load(Ordering::SeqCst), 0);
        assert_eq!(client.budget.usage.lock().unwrap().calls, 0);
        client.call(probe(&encoded), context()).await.unwrap();
        let late = Probe {
            encoded: encoded.clone(),
            decode_delay: Duration::from_millis(20),
        };
        assert!(matches!(
            client
                .call(
                    late,
                    CallContext {
                        deadline: Instant::now() + Duration::from_millis(5)
                    }
                )
                .await,
            Err(ClientError::Local(Error::Timeout))
        ));
        drop(client);
        assert_eq!(channel.closes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn retained_ready_calls_exhaust_count_and_bytes_until_drop_or_transfer() {
        for limits in [
            ClientLimits::default(),
            ClientLimits {
                max_calls: 9,
                max_retained_bytes: 8 * CLIENT_SLOT_BYTES,
            },
        ] {
            let channel = Stub::new(false);
            let client = GatewayClient::new(
                ClientBinding {
                    channel: &channel,
                    target: &(),
                },
                limits,
            )
            .unwrap();
            let encoded = Arc::new(AtomicUsize::new(0));
            let mut calls: Vec<_> = (0..8)
                .map(|_| client.call(probe(&encoded), context()))
                .collect();
            for call in &mut calls {
                pending(call.as_mut()).await;
            }
            assert_eq!(channel.calls.load(Ordering::SeqCst), 8);
            channel.ready.store(true, Ordering::SeqCst);
            channel.changed.notify_waiters();
            // Completion availability does not transfer any result until poll.
            assert!(matches!(
                client.call(probe(&encoded), context()).await,
                Err(ClientError::Local(Error::ResourceLimit(_)))
            ));
            assert_eq!(encoded.load(Ordering::SeqCst), 8);
            drop(calls.pop());
            client.call(probe(&encoded), context()).await.unwrap();
            calls.pop().unwrap().await.unwrap();
            drop(calls);
            assert_eq!(client.budget.usage.lock().unwrap().calls, 0);

            channel.ready.store(false, Ordering::SeqCst);
            let mut late = client.call(
                probe(&encoded),
                CallContext {
                    deadline: Instant::now() + Duration::from_millis(5),
                },
            );
            pending(late.as_mut()).await;
            tokio::time::sleep(Duration::from_millis(15)).await;
            channel.ready.store(true, Ordering::SeqCst);
            channel.changed.notify_waiters();
            assert!(matches!(
                late.await,
                Err(ClientError::Local(Error::Timeout))
            ));
        }
    }

    #[tokio::test]
    async fn layered_errors_capacity_and_native_duplicate_headers() {
        let channel = Stub::new(true);
        let client = GatewayClient::new(
            ClientBinding {
                channel: &channel,
                target: &(),
            },
            ClientLimits::default(),
        )
        .unwrap();
        let encoded = Arc::new(AtomicUsize::new(0));
        *channel.result.lock().unwrap() = Some(Err(Error::Remote {
            code: -123,
            message: "native transport Fault".into(),
        }));
        assert!(matches!(
            client.call(probe(&encoded), context()).await,
            Err(ClientError::Transport(Error::Remote { code: -123, .. }))
        ));
        #[derive(serde::Serialize)]
        struct Failure {
            code: i64,
            diagnostic: String,
        }
        let failure = |body| Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("ClientOperationFailure").unwrap(),
            ],
            body,
        };
        *channel.result.lock().unwrap() = Some(Ok(failure(
            fube_transport::serialization::serde::to_vec(&Failure {
                code: -456,
                diagnostic: "domain".into(),
            })
            .unwrap(),
        )));
        assert!(matches!(
            client.call(PingRequest, context()).await,
            Err(ClientError::Domain(DomainFailure { code: -456, .. }))
        ));
        *channel.result.lock().unwrap() = Some(Ok(failure(vec![])));
        assert!(matches!(
            client.call(PingRequest, context()).await,
            Err(ClientError::Protocol(_))
        ));
        let mut oversized = String::with_capacity(MAX_INPUT_BYTES + 1);
        oversized.push_str("N0010");
        assert!(matches!(
            client
                .call(
                    NodeHealthQueryDescription::new(NodeHealthQueryParameters::new(oversized)),
                    context()
                )
                .await,
            Err(ClientError::Local(Error::ResourceLimit(_)))
        ));
        assert_eq!(client.budget.usage.lock().unwrap().calls, 0);
        let id = message::MessageId {
            guid: [1; 16],
            index: 2,
        };
        let header = Header::relates_to(id).unwrap();
        let mut reply = Message {
            headers: vec![header.clone(), header.clone()],
            body: vec![],
        };
        validate_reply_headers(&reply, |_| false).unwrap();
        reply.headers.push(header);
        assert!(validate_reply_headers(&reply, |_| false).is_err());
        let actor = Header::actor(5).unwrap();
        reply.headers = vec![actor.clone(), actor];
        assert!(validate_reply_headers(&reply, |_| false).is_err());
    }

    #[test]
    fn fixed_schema_allocation_elements_fit_the_documented_bound() {
        for size in [
            size_of::<Node>(),
            size_of::<NodeDeactivationTask>(),
            size_of::<SafetyCheck>(),
            size_of::<Application>(),
            size_of::<Service>(),
            size_of::<Partition>(),
            size_of::<Replica>(),
            size_of::<DeployedApplication>(),
            size_of::<DeployedServicePackage>(),
            size_of::<DeployedCodePackage>(),
            size_of::<DeployedReplica>(),
            size_of::<HealthEvent>(),
            size_of::<HealthEvaluation>(),
            size_of::<ServiceEndpoint>(),
        ] {
            assert!(size <= 1024, "fixed heap element is {size} bytes");
        }
        const {
            assert!(2 * 512 * MAX_FRAME_SIZE + 16 * MAX_INPUT_BYTES <= CLIENT_SLOT_BYTES);
        }
        assert!(ClientBudget::new(ClientLimits {
            max_calls: 0,
            max_retained_bytes: CLIENT_SLOT_BYTES
        })
        .is_err());
    }

    #[tokio::test]
    async fn typed_allocating_lengths_are_rejected_before_unbounded_decode() {
        use fube_transport::serialization::{serde::to_vec, Encoder, MAX_STRING_UNITS};
        #[derive(serde::Serialize)]
        struct Instance(NodeId, u64);
        #[derive(serde::Serialize)]
        struct Gateway(String, Instance, String);
        #[derive(serde::Serialize)]
        struct Ping {
            gateway: Gateway,
        }
        let channel = Stub::new(true);
        let client = GatewayClient::new(
            ClientBinding {
                channel: &channel,
                target: &(),
            },
            ClientLimits::default(),
        )
        .unwrap();
        // A valid maximum string, then same-sized count edits. Outer scopes
        // remain well framed: rejection must reach the allocating length check.
        let body = to_vec(&Ping {
            gateway: Gateway(
                "g".repeat(MAX_STRING_UNITS),
                Instance(NodeId { high: 1, low: 2 }, 3),
                "N0010".into(),
            ),
        })
        .unwrap();
        let ping = |body| Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("PingRequest").unwrap(),
                Header::version(1, 1).unwrap(),
            ],
            body,
        };
        *channel.result.lock().unwrap() = Some(Ok(ping(body.clone())));
        assert_eq!(
            client
                .call(PingRequest, context())
                .await
                .unwrap()
                .address
                .len(),
            MAX_STRING_UNITS
        );
        let count = body
            .windows(4)
            .position(|w| w == [0x8d, 0x82, 0x80, 0x00])
            .unwrap();
        for ending in [1, 0x7f] {
            let mut invalid = body.clone();
            invalid[count + 3] = ending;
            *channel.result.lock().unwrap() = Some(Ok(ping(invalid)));
            assert!(matches!(
                client.call(PingRequest, context()).await,
                Err(ClientError::Protocol(_))
            ));
            assert_eq!(client.budget.usage.lock().unwrap().calls, 0);
        }

        let mut encoder = Encoder::new();
        encoder
            .object(|e| {
                e.i64(2);
                e.pointer(Some(&1i32.to_le_bytes()), |e| {
                    e.object_array(&[(); 4096], |_, _| Ok(()))?;
                    e.pointer(None, |_| Ok(()))
                })?;
                e.i64(0);
                e.pointer(None, |_| Ok(()))
            })
            .unwrap();
        let mut invalid = encoder.finish();
        let count = invalid
            .windows(3)
            .position(|w| w == [0x80, 0xa0, 0x00])
            .unwrap();
        invalid[count + 2] = 1; // 4097, above collection limit; body extent unchanged.
        *channel.result.lock().unwrap() = Some(Ok(Message {
            headers: vec![
                Header::actor(5).unwrap(),
                Header::action("QueryReply").unwrap(),
            ],
            body: invalid,
        }));
        assert!(matches!(
            client
                .call(NodeQueryDescription::default(), context())
                .await,
            Err(ClientError::Protocol(_))
        ));
        assert_eq!(client.budget.usage.lock().unwrap().calls, 0);
        assert_eq!(channel.closes.load(Ordering::SeqCst), 0);
    }
}
