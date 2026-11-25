# furpc: Actor/Action service routing

**Status:** proposed; see the [design summary](README.md).

Defines optional exact Actor/Action matching and byte-facing Tower dispatch
above native control/correlation classification and bounded ingress. Typed
body adaptation belongs to separate
[server RPC adapters](Services.md#typed-rpc-adapters), not the router.
It owns no socket, independent execution scope, or hidden inbox.

## Minimal service router

Router, RequestRouteOptions, OneWayRouteOptions, NotificationRouteOptions and
whole-Actor service bindings are implemented. Notification routes use a separate
native traffic class with the same service runner.

`Router` implements the [server service boundary](Services.md#service-boundary-and-native-bindings).
Configuration is fallible and immutable after installation. Cloning shares
route/service configuration, not duplicated live work.
`into_service` returns `IngressService<Self>` for `Endpoint::service`,
`Channel::register_service`, or `ServerBuilder::service`. It selects Actor-dispatched
application ingress and cannot be combined with a raw binding in the same
traffic class. Custom IngressService composition may select different
dispatchers for ordinary and notification classes; Router-generated bindings
are Actor-based. The shared transport does not require this router or ordinary
Actor dispatch.

Follow [axum's service routing][axum-router] and [tonic's service
router][tonic-router]: the router selects an already-constructed service.
It neither constructs typed handlers nor selects a body format. A raw service
can be registered without a body serializer. Native request, one-way and
notification kinds retain distinct option types for ingress and reply authorization; those are
not codec choices or HTTP methods.

Signatures with complete service bounds:

```rust,ignore
impl RequestRouteOptions {
    pub fn new(policy: RequestPolicy, limits: IngressLimits) -> Self;
    pub fn with_send_policy(self, policy: SendPolicy) -> Self;
}

impl OneWayRouteOptions {
    pub fn new(check: EnvelopeCheck, limits: IngressLimits) -> Self;
    pub fn with_send_policy(self, policy: SendPolicy) -> Self;
}

impl NotificationRouteOptions {
    pub fn new(policy: NotificationPolicy, limits: IngressLimits) -> Self;
    pub fn with_send_policy(self, policy: SendPolicy) -> Self;
}

impl Router {
    pub fn new() -> Self;

    pub fn request<S>(
        self, actor: ActorId, action: impl Into<String>,
        options: RequestRouteOptions, service: S,
    ) -> Result<Self>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;

    pub fn one_way<S>(
        self, actor: ActorId, action: impl Into<String>,
        options: OneWayRouteOptions, service: S,
    ) -> Result<Self>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;

    pub fn notification<S>(
        self, actor: ActorId, action: impl Into<String>,
        options: NotificationRouteOptions, service: S,
    ) -> Result<Self>
    where
        S: tower_service::Service<Request<Body>, Response = IngressResponse>
            + Clone + Send + Sync + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>;

    pub fn into_service(self) -> Result<IngressService<Self>>;
}

impl tower_service::Service<Request<Body>> for Router {
    type Response = IngressResponse;
    type Error = Error;
    type Future = std::pin::Pin<Box<
        dyn std::future::Future<Output = Result<IngressResponse>> + Send + 'static
    >>;

    fn poll_ready(
        &mut self, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>>;
    fn call(&mut self, request: Request<Body>) -> Self::Future;
}
```

Every registered service accepts `Request<Body>` and returns `IngressResponse`.
The router has no application-body type parameters, serialization bounds,
body-format defaults, or format-selecting registration methods. A method
service may use server::unary/one_way, custom byte processing, or compatible middleware;
that choice is opaque to routing.

Request routes accept only `RequestRouteOptions`; one-way routes accept only
`OneWayRouteOptions`; native notification routes accept only
`NotificationRouteOptions`. Interchanging these is a Rust type error, not a
runtime configuration branch. All reuse existing ingress limits and send policies.
`HandlerLimits` remains owner-scope-wide for one logical Channel or server,
not a per-route, per-clone, or per-generation execution pool.
Actor registration and `ServiceBinding::actor` use the shared `IngressOptions`
to select a whole actor's delivery policy. Explicit raw bindings use those
same options without selecting an Actor; they bypass this router entirely.

Grouping several actions under one actor does not widen any route's policies
or allowances. Stored service configuration is Clone + Send + Sync so the
immutable route table can be shared. Its future only needs Send; objects
created inside that future need not inherit the configuration's Clone/Sync
bounds. Registration does not infer native bindings from the service's Rust
types.

### Readiness and invocation ownership

The router owns no independent admission pool or hidden queue. Its outer
`poll_ready` may report ready because the request selects the target only in
`call`. Once matched, clone the chosen service before polling its readiness
and call that same prepared instance, never a fresh clone after readiness.
Normalize readiness/call errors with their sources and route phase.

Selected-service readiness runs inside the existing admitted invocation and
its deadline. Pending readiness consumes the runner's shared bounded waiter
allowance; zero means fail-fast and exhaustion returns `Error::ResourceLimit`.
Outer middleware, routed services, and any inner method service reuse that
accounting instead of creating independent waiting pools or double-counting
one invocation. Cancellation drops unused readiness state.

The router checks live received provenance and validates the current edited
header view against the original scope before dispatch. A request route
invokes its service only after independent normal-request authorization has
produced the runner's reply ticket. A notification route instead requires
original native-notification authorization and its acknowledgment ticket.
The required native ID, exchange kind and ticket come from the original
admitted invocation, not the current editable header view.
Missing original authorization rejects before user
work. One-way routes do not acquire a ticket merely from flags, and no
route kind can reclassify traffic by editing headers. The router returns response
data; only the runner completes the original reply. A service returning the
wrong response kind is an explicit handler contract failure, not a fabricated
ACK.

### Matching and unknown routes

Select ordinary or notification traffic class from the private native
classification first, then match exact `(ActorId, Action)` using the validated
effective native common properties. Ordinary request and one-way routes still
share one namespace; they cannot claim the same key. A notification may use
the same Actor/Action as an ordinary route because it has a distinct native
traffic class, authorization and immutable route identity. This is not a
user-editable routing field.

Header edits do not change the original admitted binding,
route identity, or exchange kind. For router-generated exact-action bindings,
an edit selecting a different key, whether registered or not, is a handler
contract error before any target service is invoked. Never borrow another
route's policy, reply authority, or resource allowance. A custom whole-actor
service may interpret rewritten Action metadata within its original actor
binding; it does not re-enter native demux or gain another actor's authority.

Preserve all records unless explicitly edited and validate every recognized
occurrence; the first matching duplicate is not an alternative route. Compare
Action as exact native UTF-16 content, without case folding, normalization,
or lossy conversion. Missing Action cannot match a nonempty configured action.

Reject duplicate route keys within a traffic class and native control actors
at configuration time. Whole-Actor and exact-action claims cannot overlap
within that class. Raw/Actor conflicts are likewise class-local; no miss falls
through to the other class.
Keep exact route matching above native parsing. There are no extra fixed
route-count, Actor-count or action-storage construction ceilings, and no
wildcard, prefix, or implicit fallback routes in the initial router.

Within Actor-dispatched ingress, unknown actors retain native diagnosed-drop
behavior.
An unmatched action for a service-owned actor is a diagnosed route miss with
no fabricated reply, not an automatic connection-fatal wire error. No reply
grant is minted by guessing an action or matching an older duplicate. Where a
body is discarded, the same
bounded drain/integrity rules apply. Domain-specific unsupported-operation
replies require explicit protocol handling, not a universal error encoder.
Neither actor nor action misses fall back to raw ingress or another dispatcher.
Missing Actor remains absent in the header records; native effective Empty
semantics do not create a routing requirement for explicitly raw protocols.

## Routing limits

Route construction validates keys and conflicts and reports allocation errors.
It does not impose a retained-capacity quota. Applications own the size of
their route configuration.

Runtime handler concurrency, readiness, explicit output restrictions, and request
deadlines are separately bounded by [HandlerLimits](Services.md#handler-limits).
Routing cannot widen native connection/frame/send-queue limits.

## Acceptance and deferred work

Cover actor/action selection, native duplicate-property semantics, duplicate
and conflicting registrations, raw service dispatch, private reply binding,
and absence of fabricated ACKs or generic error replies. Request/one-way
and notification option interchange must fail to typecheck. Exercise native width boundaries,
invalid durations and checked arithmetic. Runtime
cases use the [handler execution limits](Services.md#handler-limits), not
another router-owned admission pool.

Cover preservation of unedited ordered/unknown headers, source, restricted
connection, deadline, and shared storage through body mapping and parts
reconstruction. Bounded header edits must work on received requests and parts,
with current duplicate lookups and no mutation of shared raw messages.
Malformed edits, route/binding retargeting, and context-free router calls must
reject explicitly. Outbound construction must not manufacture received
context, and reusing data/context must not acquire another completion ticket,
widen send/reply policy, or extend admitted work through shutdown.

Exercise chosen-service readiness/call instance affinity, shared waiter
exhaustion, timeout/commit races, service errors, graceful drain, dropped
execution, bounded application queue composition without duplicate delivery,
and separation of native notification routes from ordinary one-way/request
routes, including equal Actor/Action keys in different classes. Notification
handler replies must use the original runner-owned acknowledgment ticket;
NoReply, cancellation and errors must never fabricate an acknowledgment.
Composition cases with the separate RPC adapters cover typed bodies and their
budgets; routing must also work without those adapters. Native exchanges remain
required for claimed runtime/protocol support.

Keep the router above the shared native messaging core. Replication uses
explicit raw ingress and its own ReplicationActor demultiplexing, while
client reverse ingress may use raw or Actor-dispatched ordinary/notification services; a
future client service integration reuses these components rather than forks
the protocol. State extractors, macros, wildcard/fallback routing, automatic
domain error mapping, and actor lifecycle machinery are deferred.

The removed declaration-only `fube-transport::routing` module was not an implemented
foundation to transplant. Implement this router against native header
semantics rather than inheriting that draft's independent restrictions.

[axum-router]: https://docs.rs/axum/latest/axum/struct.Router.html#method.route_service
[tonic-router]: https://docs.rs/tonic/latest/tonic/transport/server/struct.Router.html#method.add_service
