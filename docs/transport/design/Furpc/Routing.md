# furpc: Actor/Action service routing

Router is optional exact native Actor/Action dispatch to byte-facing Tower
services. It owns no socket, codec, admission pool, queue, or automatic error
encoder. Public route options and dispatch live in
[routing.rs](../../../../crates/libs/furpc/src/routing.rs).

## Minimal service router

Router registration selects a service and one of three distinct option types:
request/reply, ordinary one-way, or native notification. The options carry
ingress/response policy and optional independent SendPolicy, not a body format.
Typed adaptation happens inside the selected service through
[server adapters](Services.md#typed-rpc-adapters).

`into_service` produces the same IngressService accepted by Endpoint, Channel,
and ServerBuilder. Router-generated bindings are Actor-based; raw ingress
bypasses this router. Raw and Actor bindings cannot mix within a traffic class.

Routes are immutable installed configuration. Clones share route prototypes,
not live readiness reservations. Stored route services require Clone + Send +
Sync and Send futures; values created inside their invocation do not inherit
those configuration bounds.

## Readiness and invocation ownership

The outer Router reports ready without choosing a target. `call` validates
received invocation state, chooses the originally admitted route, clones that
service before readiness, then calls the same prepared instance.

Selected-service readiness is part of the existing invocation deadline.
Furpc adds no router-owned waiter or handler-concurrency quota; consumers own
those policies. Pending readiness does not block driver progress.

Direct calls with missing/stale context, another router's route identity,
or headers that retarget the original binding reject before the target service
runs. A route match cannot mint a reply ticket or widen the admitted policy.
The runner alone completes returned response data.

## Matching and unknown routes

Select native ordinary/notification class from private invocation state, then
match effective `(ActorId, Action)`. Action comparison is exact native UTF-16:
no normalization, lossy conversion, case folding, prefix, wildcard, or implicit
fallback.

Ordinary request and one-way routes share a namespace. A notification can use
the same Actor/Action because it belongs to a separate native class.
Duplicate keys, overlapping whole-Actor/exact-action bindings, class-local
raw/Actor conflicts, and reserved control actors reject during configuration.

Every recognized duplicate header remains subject to native validation; an
earlier visible occurrence is not an alternate route. Middleware cannot
rewrite a request onto another exact route or borrow its permissions.
A custom whole-Actor service may interpret rewritten Action metadata within
its original Actor binding without re-entering native admission.

Unknown actors and actions are diagnosed misses, not fallback delivery or
fabricated replies. Discard retains frame-integrity requirements.
Domain-specific unsupported-operation responses require explicit protocol
handling.

Binding selection and captured route identity live in
[service.rs](../../../../crates/libs/furpc/src/service.rs).

## Routing limits

Route construction checks keys/conflicts and reports allocation failures.
It adds no fixed route-count or action-storage quota. Applications own their
configuration size and runtime execution backpressure.

Ingress body policy, handler deadline/output policy, and native
connection/frame/send-queue limits remain independently enforced.
Router cannot widen them or create another invocation by reassembling request
parts.

Executable matching, metadata-edit, readiness, and drain examples live in
[routing tests](../../../../crates/libs/furpc/tests/transport/routing.rs).
