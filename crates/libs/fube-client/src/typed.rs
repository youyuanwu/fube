//! Legacy bounded adapters for the remaining ordinary gateway calls.
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
    query_protocol,
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
/// bytes; counted strings consume >=1.
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

// fubst checks array framing before allocating.
// Even null native pointer slots occupy their enclosing 12-byte object.
macro_rules! element_bound {
    ($type:ty, $minimum:expr) => {
        const _: () = assert!(size_of::<$type>() <= 32 * $minimum);
    };
}
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
    pub(crate) fn new(limits: ClientLimits) -> Result<Self> {
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
        validate_reply_headers(&reply).map_err(|error| match error {
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

fn validate_reply_headers(reply: &Message) -> Result<()> {
    if reply.body.capacity() > MAX_FRAME_SIZE || reply.headers.len() > message::MAX_HEADER_COUNT {
        return Err(Error::Protocol(
            "typed reply allocation exceeds wire bounds",
        ));
    }
    let mut bytes = 0usize;
    for (index, header) in reply.headers.iter().enumerate() {
        bytes = bytes
            .checked_add(header.value.capacity() + 4)
            .ok_or(Error::Protocol("reply header overflow"))?;
        if header.id == message::RELATES_TO {
            header.read(message::MessageId::decode)?;
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
            // ExpectsReply retains the identical-two native exception.
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
    // a present diagnostic must decode. Runtime IPC/domain error policy is separate.
    let protocol::GatewayFailureBody { code, diagnostic } = fubst::serde::from_slice(&reply.body)?;
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

impl sealed::Request for QueryDescription<movement::MovePrimary> {
    fn input_bytes(&self) -> usize {
        size_of::<Self>() + self.parameters.target.capacity()
    }
}
impl ClientRequest for QueryDescription<movement::MovePrimary> {
    type Response = ();
    type ReplyState = ();
    type DomainError = DomainFailure;

    fn encode(self, context: &CallContext) -> Result<(Message, ())> {
        Ok((
            wire(
                "QueryRequest",
                vec![],
                query_protocol::encode_query(&self.parameters, None)?,
                context,
            )?,
            (),
        ))
    }

    fn decode(
        _: (),
        reply: Message,
        _: &CallContext,
    ) -> std::result::Result<(), ReplyError<DomainFailure>> {
        decode_domain(
            &reply,
            query_protocol::decode_reply::<movement::MovePrimary>,
        )
    }
}

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

    #[test]
    fn fixed_schema_allocation_elements_fit_the_documented_bound() {
        let size = size_of::<ServiceEndpoint>();
        assert!(size <= 1024, "fixed heap element is {size} bytes");
        const {
            assert!(2 * 512 * MAX_FRAME_SIZE + 16 * MAX_INPUT_BYTES <= CLIENT_SLOT_BYTES);
        }
        assert!(ClientBudget::new(ClientLimits {
            max_calls: 0,
            max_retained_bytes: CLIENT_SLOT_BYTES
        })
        .is_err());
    }
}
