use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use furpc::{
    header, ActorId, Body, BoxError, Channel, Codec, CodecError, EncodeBuf, Encoder, Endpoint,
    FubstCodec, HeaderId, HeaderRecord, Request, Response, RpcClient,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::Instant;
use tower_service::Service;

use crate::{
    protocol,
    query_protocol::{self, Query, QueryRequest, Reply},
    resolution_protocol, ApplicationListPage, ApplicationQueryDescription,
    DeployedApplicationListPage, DeployedApplicationQueryDescription, DeployedCodePackage,
    DeployedCodePackageQueryDescription, DeployedReplica, DeployedReplicaQueryDescription,
    DeployedServicePackage, DeployedServicePackageQueryDescription, GatewayDescription,
    MovePrimaryQueryDescription, NamingUri, NodeHealth, NodeHealthQueryDescription, NodeListPage,
    NodeQueryDescription, PartitionListPage, PartitionQueryDescription, ReplicaListPage,
    ReplicaQueryDescription, ResolutionRequest, ResolvedServicePartition, ServiceListPage,
    ServiceQueryDescription,
};

const ACTIVITY: u16 = 0x8020;
const TIMEOUT: u16 = 0x8023;
const CLIENT_VERSION: u16 = 0x8025;

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

/// Errors from the experimental furpc-backed client, without legacy transport conversion.
#[derive(Debug, Clone)]
pub enum FurpcClientError {
    Native(furpc::Error),
    Decode(fubst::Error),
    Protocol(&'static str),
    Domain(DomainFailure),
}
impl fmt::Display for FurpcClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => write!(f, "furpc client: {error}"),
            Self::Decode(error) => write!(f, "gateway decode: {error}"),
            Self::Protocol(reason) => write!(f, "gateway protocol: {reason}"),
            Self::Domain(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for FurpcClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Domain(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}
impl From<furpc::Error> for FurpcClientError {
    fn from(error: furpc::Error) -> Self {
        Self::Native(error)
    }
}
impl From<fubst::Error> for FurpcClientError {
    fn from(error: fubst::Error) -> Self {
        Self::Decode(error)
    }
}
impl From<query_protocol::ReplyError> for FurpcClientError {
    fn from(error: query_protocol::ReplyError) -> Self {
        match error {
            query_protocol::ReplyError::Protocol(reason) => Self::Protocol(reason),
            query_protocol::ReplyError::Domain(failure) => Self::Domain(failure),
        }
    }
}
impl FurpcClientError {
    fn from_rpc(error: furpc::Error) -> Self {
        // RpcClient preserves byte-service and codec errors as their original sources.
        match &error {
            furpc::Error::Transport { .. } => {
                if let Some(error) = std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<Self>())
                {
                    return error.clone();
                }
            }
            furpc::Error::Codec {
                phase: "decode",
                source,
            } => {
                if let Some(error) = std::error::Error::source(source)
                    .and_then(|source| source.downcast_ref::<fubst::Error>())
                {
                    return Self::Decode(*error);
                }
            }
            _ => {}
        }
        Self::Native(error)
    }
}
type Result<T> = std::result::Result<T, FurpcClientError>;

/// Typed gateway stub over a byte-facing Tower service.
///
/// Gateway queries, Ping, naming operations and resolution use furpc directly,
/// without the retired FabricClient façade or typed budgets.
/// The default transport is a furpc Channel; clones share its lifecycle and
/// dropping the last owner closes it. Later calls may reconnect, but a submitted
/// call is never replayed. Custom transports need not be Clone, Sync or static.
/// Clone a cloneable client explicitly before concurrent calls.
///
/// ```no_run
/// use std::time::Duration;
/// use fube_client::FurpcClient;
/// use furpc::Request;
/// use tokio::time::Instant;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let channel = FurpcClient::endpoint_clear("127.0.0.1:19000".parse()?)?
///     .connect().await?;
/// let mut client = FurpcClient::new(channel);
/// let deadline = Instant::now() + Duration::from_secs(30);
/// let gateway = client.ping(Request::new(()), deadline).await?.into_body();
/// # let _ = gateway;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FurpcClient<T = Channel> {
    inner: RpcClient<GatewayService<T>>,
}
impl FurpcClient<Channel> {
    /// Explicitly select an unsecured native gateway; performs no I/O.
    pub fn endpoint_clear(address: SocketAddr) -> furpc::Result<Endpoint> {
        Endpoint::try_from(address)
    }
}
impl<T> FurpcClient<T> {
    /// Bind locally without a handshake, readiness polling, or transport cloning.
    pub fn new(transport: T) -> Self {
        Self {
            inner: RpcClient::new(GatewayService {
                transport,
                validate_reply: check_ping_reply,
            }),
        }
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner().transport
    }
}
impl<T> FurpcClient<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    /// Discover the gateway's identity without retry or HTTP fallback.
    ///
    /// The earlier of the explicit and request-local deadlines covers encoding,
    /// readiness/reconnect, submission and receipt, using furpc settlement rules.
    /// The unit request encodes to zero bytes, not a serialized unit object.
    /// Channel transports require managed mode; raw-only mode returns Unsupported.
    pub async fn ping(
        &mut self,
        request: Request<()>,
        deadline: Instant,
    ) -> Result<Response<GatewayDescription>> {
        let response = self
            .unary(
                request,
                FubstCodec::<(), protocol::PingReplyBody>::default(),
                "PingRequest",
                check_ping_reply,
                deadline,
            )
            .await?;
        let (parts, body) = response.into_parts();
        let gateway = body.into_gateway().map_err(FurpcClientError::Protocol)?;
        Ok(Response::from_parts(parts, gateway))
    }

    /// Test a checked naming URI without retry or a user-service state query.
    pub async fn name_exists(
        &mut self,
        request: Request<NamingUri>,
        deadline: Instant,
    ) -> Result<Response<bool>> {
        let response = self
            .unary(
                request,
                FubstCodec::<NamingUri, protocol::NameExistsReplyBody>::default(),
                "NameExistsRequest",
                check_name_reply,
                deadline,
            )
            .await?;
        Ok(response.map(|body| body.exists))
    }

    /// Explicitly create a checked name, including implicit ancestors if needed.
    ///
    /// An already explicitly created name remains a native error. There is no
    /// retry or replay: timeout, cancellation or disconnection after submission
    /// can leave the write outcome unknown, even if the name appears absent.
    pub async fn create_name(
        &mut self,
        request: Request<NamingUri>,
        deadline: Instant,
    ) -> Result<Response<()>> {
        self.unary(
            request,
            FubstCodec::<NamingUri, ()>::default(),
            "CreateNameRequest",
            check_name_reply,
            deadline,
        )
        .await
    }

    /// Explicitly delete one checked name, without recursive deletion or replay.
    ///
    /// Missing and nonempty names remain native errors. Native may remove empty
    /// implicit ancestors. Timeout, cancellation or disconnection after submission
    /// does not prove that deletion did not execute.
    pub async fn delete_name(
        &mut self,
        request: Request<NamingUri>,
        deadline: Instant,
    ) -> Result<Response<()>> {
        let request = request.append_header(HeaderRecord::native(
            HeaderId::new(protocol::DELETE_NAME_HEADER)?,
            &DeleteNameHeader { explicit: true },
        )?)?;
        self.unary(
            request,
            FubstCodec::<NamingUri, ()>::default(),
            "DeleteNameRequest",
            check_name_reply,
            deadline,
        )
        .await
    }

    /// Ask native FM to move a Primary to an explicit node with placement constraints.
    ///
    /// There is no force/ignore option, retry or replay. Timeout, cancellation or
    /// disconnection after submission can leave the mutation outcome unknown.
    /// Successful completion is not an observed placement transition; query
    /// placement/discovery separately, without treating it as an atomic snapshot.
    pub async fn move_primary(
        &mut self,
        request: Request<MovePrimaryQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<()>> {
        self.query(request, deadline).await
    }

    /// Resolve a checked selection without an endpoint cache, retry or replay.
    ///
    /// Previous-result identity/version metadata is sent exactly as captured;
    /// an observed endpoint address does not retarget this client's gateway.
    /// This is not an atomic placement snapshot. Response metadata and on-time
    /// settlement retain the same semantics as other furpc operations.
    pub async fn resolve_service_partition(
        &mut self,
        request: Request<ResolutionRequest>,
        deadline: Instant,
    ) -> Result<Response<ResolvedServicePartition>> {
        let name = request.body().name.clone();
        let key = request.body().key.clone();
        let response = self
            .unary(
                request,
                ResolutionCodec::default(),
                "ResolveServiceRequest",
                check_resolution_reply,
                deadline,
            )
            .await?;
        let (parts, reply) = response.into_parts();
        let result = reply.into_partition().map_err(FurpcClientError::Protocol)?;
        if result.service_name != name || !key.selects(&result.partition_information) {
            return Err(FurpcClientError::Protocol(
                "resolution result does not match requested selection",
            ));
        }
        Ok(Response::from_parts(parts, result))
    }

    /// Get one best-effort native node page, without automatic paging or retries.
    ///
    /// Inspect the returned continuation token even when no page size was supplied.
    /// Name/status selectors and paging use the existing native query models.
    pub async fn get_node_list(
        &mut self,
        request: Request<NodeQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<NodeListPage>> {
        self.query(request, deadline).await
    }

    /// Get one native application page, without automatic paging or retries.
    ///
    /// Selectors and parameter exclusion follow the shared application profile.
    /// Inspect the continuation token even when no page size was supplied.
    pub async fn get_application_list(
        &mut self,
        request: Request<ApplicationQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<ApplicationListPage>> {
        self.query(request, deadline).await
    }

    /// Get one native service page scoped to an ordinary application.
    ///
    /// The System application profile is unsupported. Inspect the continuation
    /// token; native subquery count limits do not guarantee an aggregate page cap.
    pub async fn get_service_list(
        &mut self,
        request: Request<ServiceQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<ServiceListPage>> {
        self.query(request, deadline).await
    }

    /// Get one service-scoped partition page, not an endpoint-resolution snapshot.
    ///
    /// Native paging supports continuation tokens, but not MaxResults. There is
    /// no automatic paging or retry; a nil partition selector is omitted.
    pub async fn get_partition_list(
        &mut self,
        request: Request<PartitionQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<PartitionListPage>> {
        self.query(request, deadline).await
    }

    /// Get one page of stateful replicas or stateless instances for a partition.
    ///
    /// Native paging supports continuation tokens, but not MaxResults. Signed
    /// IDs and status masks are preserved, without automatic paging or retries.
    pub async fn get_replica_list(
        &mut self,
        request: Request<ReplicaQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<ReplicaListPage>> {
        self.query(request, deadline).await
    }

    /// Get one node-local deployed-application page with optional health.
    ///
    /// Follow continuations explicitly; neither count nor message-size paging
    /// is automatic. This is not the cluster-wide application inventory.
    pub async fn get_deployed_application_list(
        &mut self,
        request: Request<DeployedApplicationQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<DeployedApplicationListPage>> {
        self.query(request, deadline).await
    }

    /// List service packages for one application on a selected node, without paging.
    pub async fn get_deployed_service_package_list(
        &mut self,
        request: Request<DeployedServicePackageQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<Vec<DeployedServicePackage>>> {
        self.query(request, deadline).await
    }

    /// List unpaged code packages, including both native entry-point records.
    pub async fn get_deployed_code_package_list(
        &mut self,
        request: Request<DeployedCodePackageQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<Vec<DeployedCodePackage>>> {
        self.query(request, deadline).await
    }

    /// List unpaged node/application-scoped replicas and stateless instances.
    ///
    /// Native replies use RS Actor 13, not NamingGateway or RA. This is a
    /// deployment observation, not service endpoint resolution.
    pub async fn get_deployed_replica_list(
        &mut self,
        request: Request<DeployedReplicaQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<Vec<DeployedReplica>>> {
        self.query(request, deadline).await
    }

    /// Get unpaged native node health with optional policy and event filtering.
    ///
    /// An unhealthy aggregate is successful data, not a domain error. Omitted
    /// policy inherits cluster policy; an explicit false policy overrides it.
    pub async fn get_node_health(
        &mut self,
        request: Request<NodeHealthQueryDescription>,
        deadline: Instant,
    ) -> Result<Response<NodeHealth>> {
        self.query(request, deadline).await
    }

    async fn query<Q: QueryRequest>(
        &mut self,
        request: Request<Q>,
        deadline: Instant,
    ) -> Result<Response<<Q::Parameters as Query>::Output>> {
        let response = self
            .unary(
                request,
                QueryCodec::<Q>::new(),
                "QueryRequest",
                check_query_reply::<Q>,
                deadline,
            )
            .await?;
        let (parts, body) = response.into_parts();
        let output =
            <Q::Parameters as Query>::Reply::decode_body(body, Q::Parameters::validate_record)?;
        Ok(Response::from_parts(parts, output))
    }

    async fn unary<C>(
        &mut self,
        request: Request<C::Encode>,
        codec: C,
        action: &'static str,
        validate_reply: fn(&Response<Body>) -> Result<()>,
        deadline: Instant,
    ) -> Result<Response<C::Decode>>
    where
        C: Codec + Send,
        C::Encode: Send,
        C::Decode: Send,
        C::Encoder: Send,
        C::Decoder: Send,
    {
        let deadline = request
            .deadline()
            .map_or(deadline, |local| local.min(deadline));
        timeout_ticks(deadline)?;
        let request = request
            .append_header(header::actor(ActorId::new(5)?)?)?
            .append_header(header::action(action)?)?
            .append_header(HeaderRecord::native(
                HeaderId::new(ACTIVITY)?,
                &ActivityHeader {
                    activity: ActivityId(uuid::Uuid::new_v4().to_bytes_le(), 0),
                },
            )?)?
            .append_header(HeaderRecord::native(
                HeaderId::new(CLIENT_VERSION)?,
                &ProtocolVersion(1, 1),
            )?)?;
        self.inner.get_mut().validate_reply = validate_reply;
        self.inner
            .call(request, codec, deadline)
            .await
            .map_err(FurpcClientError::from_rpc)
    }
}

#[derive(Clone)]
struct GatewayService<T> {
    transport: T,
    validate_reply: fn(&Response<Body>) -> Result<()>,
}
impl<T> Service<Request<Body>> for GatewayService<T>
where
    T: Service<Request<Body>, Response = Response<Body>> + Send,
    T::Future: Send,
    T::Error: Into<BoxError>,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = GatewayFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), BoxError>> {
        self.transport.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        match stamp_timeout(request) {
            Ok(request) => GatewayFuture::Call {
                future: Box::pin(self.transport.call(request)),
                validate_reply: self.validate_reply,
            },
            Err(error) => GatewayFuture::Failed(Some(error)),
        }
    }
}

enum GatewayFuture<F> {
    Call {
        future: Pin<Box<F>>,
        validate_reply: fn(&Response<Body>) -> Result<()>,
    },
    Failed(Option<furpc::Error>),
}
impl<F, E> Future for GatewayFuture<F>
where
    F: Future<Output = std::result::Result<Response<Body>, E>>,
    E: Into<BoxError>,
{
    type Output = std::result::Result<Response<Body>, BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Call {
                future,
                validate_reply,
            } => {
                let response = std::task::ready!(future.as_mut().poll(cx)).map_err(Into::into)?;
                validate_reply(&response)?;
                Poll::Ready(Ok(response))
            }
            Self::Failed(error) => Poll::Ready(Err(error
                .take()
                .expect("polled completed gateway call")
                .into())),
        }
    }
}

fn timeout_ticks(deadline: Instant) -> furpc::Result<i64> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let ticks = i64::try_from(remaining.as_nanos() / 100)
        .map_err(|_| furpc::Error::InvalidInput("timeout exceeds native signed ticks"))?;
    if ticks == 0 {
        return Err(furpc::Error::Timeout("gateway"));
    }
    Ok(ticks)
}

fn stamp_timeout(request: Request<Body>) -> furpc::Result<Request<Body>> {
    let deadline = request.deadline().ok_or(furpc::Error::InvalidInput(
        "gateway request requires a deadline",
    ))?;
    let ticks = timeout_ticks(deadline)?;
    request.append_header(HeaderRecord::native(
        HeaderId::new(TIMEOUT)?,
        &TimeoutHeader { ticks },
    )?)
}

type QueryBody<Q> = <<<Q as QueryRequest>::Parameters as Query>::Reply as Reply>::Body;
type ResolutionCodec = FubstCodec<ResolutionRequest, resolution_protocol::ResolveReply>;

struct QueryCodec<Q>(PhantomData<fn(Q)>);

impl<Q> QueryCodec<Q> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Q: QueryRequest> Codec for QueryCodec<Q> {
    type Encode = Q;
    type Decode = QueryBody<Q>;
    type Encoder = Self;
    type Decoder = <FubstCodec<(), QueryBody<Q>> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        Self::new()
    }

    fn decoder(&mut self) -> Self::Decoder {
        FubstCodec::<(), QueryBody<Q>>::default().decoder()
    }
}
impl<Q: QueryRequest> Encoder for QueryCodec<Q> {
    type Item = Q;

    fn encode(
        &mut self,
        query: Self::Item,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), CodecError> {
        let bytes = query_protocol::encode_query_body(
            query.parameters(),
            query.paging(),
            fubst::Limits {
                max_bytes: dst.remaining(),
                ..Default::default()
            },
        )?;
        let body = Body::from_bytes(bytes).map_err(|error| match error {
            furpc::Error::ResourceLimit(_) => CodecError::resource_limit(error),
            _ => CodecError::invalid_payload(error),
        })?;
        dst.append_body(body)
    }
}

#[derive(Serialize)]
struct ActivityHeader {
    activity: ActivityId,
}
#[derive(Serialize)]
struct ActivityId(#[serde(with = "fubst::serde::guid")] [u8; 16], u64);
#[derive(Serialize, Deserialize)]
struct ProtocolVersion(i64, i64);
#[derive(Serialize)]
struct TimeoutHeader {
    ticks: i64,
}
#[derive(Serialize)]
struct DeleteNameHeader {
    explicit: bool,
}

fn decode_body<T: DeserializeOwned>(body: &Body) -> Result<T> {
    let mut segments = body.segments();
    let first = segments.next().unwrap_or(&[]);
    if segments.next().is_none() {
        return Ok(fubst::serde::from_slice(first)?);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(body.len())
        .map_err(|_| furpc::Error::ResourceLimit("gateway decode input"))?;
    for segment in body.segments() {
        bytes.extend_from_slice(segment);
    }
    Ok(fubst::serde::from_slice(&bytes)?)
}

fn action_is(response: &Response<Body>, action: &str) -> bool {
    response
        .headers()
        .effective()
        .action()
        .iter()
        .copied()
        .eq(action.encode_utf16())
}

fn check_gateway_failure(response: &Response<Body>) -> Result<()> {
    if action_is(response, "ClientOperationFailure") {
        check_gateway_actor(response)?;
        let failure: protocol::GatewayFailureBody = decode_body(response.body())?;
        if failure.code == 0 {
            return Err(FurpcClientError::Protocol(
                "operation failure has no nonzero error code",
            ));
        }
        return Err(FurpcClientError::Domain(DomainFailure {
            code: failure.code,
            message: failure.diagnostic,
        }));
    }
    Ok(())
}

fn check_gateway_actor(response: &Response<Body>) -> Result<()> {
    if response.headers().effective().actor != ActorId::new(5)? {
        return Err(FurpcClientError::Protocol(
            "reply is not from NamingGateway",
        ));
    }
    Ok(())
}

fn check_ping_reply(response: &Response<Body>) -> Result<()> {
    check_gateway_actor(response)?;
    check_gateway_failure(response)?;
    if !action_is(response, "PingRequest") {
        return Err(FurpcClientError::Protocol("unexpected ping reply action"));
    }
    let mut version = None;
    for record in response
        .headers()
        .raw_records()
        .filter(|record| record.id().get() == CLIENT_VERSION)
    {
        version = Some(fubst::serde::from_slice::<ProtocolVersion>(record.value())?);
    }
    if !matches!(version, Some(ProtocolVersion(1, minor)) if minor >= 1) {
        return Err(FurpcClientError::Protocol(
            "ping requires protocol version 1.1 or newer 1.x",
        ));
    }
    Ok(())
}

fn check_name_reply(response: &Response<Body>) -> Result<()> {
    check_gateway_actor(response)?;
    check_gateway_failure(response)?;
    if !action_is(response, "NameOperationReply") {
        return Err(FurpcClientError::Protocol("unexpected naming reply action"));
    }
    Ok(())
}

fn check_resolution_reply(response: &Response<Body>) -> Result<()> {
    check_gateway_actor(response)?;
    check_gateway_failure(response)?;
    if !action_is(response, "ResolveServiceRequest") {
        return Err(FurpcClientError::Protocol(
            "unexpected resolution reply action",
        ));
    }
    Ok(())
}

fn check_query_reply<Q: QueryRequest>(response: &Response<Body>) -> Result<()> {
    check_gateway_failure(response)?;
    let headers = response.headers();
    <Q::Parameters as Query>::Reply::ENVELOPE
        .check(
            headers
                .first_raw(header::ACTOR)
                .map(|_| headers.effective().actor.get()),
            headers
                .first_raw(header::ACTION)
                .map(|_| headers.effective().action().iter().copied()),
        )
        .map_err(FurpcClientError::Protocol)
}
