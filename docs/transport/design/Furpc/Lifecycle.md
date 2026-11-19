# furpc: Shutdown and cancellation

**Status:** proposed; see the [design summary](README.md).

Defines tonic-shaped serving completion, Channel lifetime, and cooperative
cleanup. Normal server shutdown is signal-and-await, not a separate public
task-retirement or closure-observation protocol.

## Scope and state

Each Channel owns a stable logical destination, retained connector, and its
current/reconnecting/retiring physical work plus ingress-service execution
through shared handles; server
execution owns accepted work, optional peer-ready futures, and configured
service invocations. Peer
capabilities, sources, native operations, messages, and received requests
do not keep either scope open. RpcClient inherits ownership from its transport;
the built-in Channel owns dialed work, while arbitrary services have their own
ownership contracts. Application queue workers have their own owners.
Notification ingress shares the service runner, not a separately owned native
receiver. Buffer lifetime and transport queue accounting are distinct.

Each physical generation retains the irreversible states:

```text
Opening -> Open -> Draining -> Closed
    \        \         \----> Aborting -> Closed
     \--------\-------------> Aborting -> Closed
```

Drain and abort remain internal connection states, not additional public
lifecycle controllers. A disconnected Channel is not a closed logical scope;
it can reconnect on later demand without reopening any retired physical stream.

| Operation | Contract |
| --- | --- |
| Graceful server shutdown | Signal, stop acceptance and new dispatch, then await admitted work and active connection completion within the configured drain timeout. |
| Normal client use | Retain a Channel or its owning RPC client/service future while work is needed; await required results before releasing the last owner. |
| Owner/execution drop | Stop new work and request cancellation of owned work. This is not graceful shutdown or an awaitable cleanup barrier. |
| Connection abort | Irreversibly close the generation; queued output may be discarded and committed output truncated. Never reuse that stream. |

## Channel lifetime

There are no public client drain/abort methods or independent closure observers.
Endpoint contains all dialed configuration; `connect` returns an owning Channel,
following tonic's shared-handle pattern. Clones and submitted service futures retain
the dialed owner; `RpcClient<Channel>` owns its transport. Last-owner release
closes admission and requests cancellation without flushing or joining tasks.
Native background drivers must not retain this public owner guard themselves,
or an idle connection could keep itself alive forever.

Physical failure retires that generation and fails its unsettled native work;
it does not dispose the Channel's connector, registry, or bounded ingress.
The class-local raw or Actor dispatchers and ordinary/notification bindings
remain fixed for that logical scope; reconnect cannot switch dispatchers or
redirect old deliveries.
IngressService configuration survives, but its old-generation invocations and
private reply/acknowledgment tickets are invalidated/cancelled, not replayed.
Application queues retain requests only under the original context/deadline
and cancellation; retained bytes cannot preserve a live completion ticket.
Later readiness or peer acquisition can share a new attempt. Notification
ingress and application queue waits are not reconnect demands. Started reconnect
work may continue after one waiter drops while owners remain, under its finite
establishment timeout; no detached per-waiter
connector task is created. Last-owner release cancels current establishment,
closes logical admission/registrations, prevents new publication/redial, and
retires all current/retiring work without a universal join guarantee.
That cancellation includes inbound service readiness, handlers, and replies.
They retain no public owner guard; installing a reverse service cannot itself
keep the Channel alive or demand an idle reconnect. Application configuration
must avoid a cycle by capturing its own owning Channel.

Native users retain that same Channel while awaiting channel.peer(deadline)
or using send/request/notification operations. Peer never retains a dialed or
accepted owner. Retaining it cannot prevent last-owner cancellation or
server shutdown. Endpoint clones contain only configuration
references and cannot keep an earlier Channel alive. Multiple Channels have
independent lifetimes.
Endpoint clones may retain inert service prototypes, never live handler
futures or another connect's ingress runner.
Receiving a physical error does not make old Peer/SendPermit values or private
completion tickets follow a new generation; they remain retired capabilities.

Await a send's write receipt, an RPC response, or a protocol-specific semantic
ACK before releasing the last Channel owner when that completion matters.
None is a general client-shutdown receipt. Retained messages and results keep their
shared backing storage alive until the last reference is dropped.

## Server drain

The single [serving API](Connections.md#endpoint-server-and-built-in-adapters)
follows [tonic's shutdown model][tonic-serve]. Completion of the supplied signal
drops the acceptor immediately, stops new application dispatch/submission, and
begins graceful drain under one configured finite `drain_timeout`.
Already-admitted service futures and their original replies or notification
acknowledgments may complete; native controls and pending RPC/notification
correlation continue as needed.
No new peer-ready hook is admitted after shutdown begins. Previously admitted
hooks remain tracked within the earlier of their original peer-ready deadline
and the drain deadline; they are cancelled on connection failure or forced
scope cancellation. They do not gain permission for new application sends
during drain.

Reply and notification-acknowledgment completion use the runner's existing
private tickets and protected completion counts. A context-derived
Peer has no special drain exemption: new one-way sends, requests, and
notifications through it are rejected once general submission closes.
The same applies to a Peer obtained from `on_peer_ready`.
Retaining a context cannot reopen admission. A protocol needing additional
semantic sends must arrange those before signaling shutdown or use a
separately owned scope.

Awaiting serving waits for admitted service/peer-ready work and active connection
completion, not just accept-loop exit. Application queue workers are separately
owned, but keep them available while admitted service responses drain and
stop/dispose them explicitly. Separately deferred one-way processing needing
new semantic sends must quiesce before signaling if those sends must complete.
Notification acknowledgments use the original admitted tickets instead of
new general sends. Application-spawned tasks are not automatically adopted
or awaited.

Clean finite accept-source exhaustion stops acceptance but lets yielded
establishments and connections finish normally; it does not initiate shutdown.
An idle established peer can therefore keep serving pending.

## Cancellation and drop

For graceful shutdown, complete the signal and continue awaiting serving.
Dropping an active serving future instead drops its acceptor and requests
cancellation/abort of its connection work. Dropping an unpolled future only
releases its supplied configuration and inputs.

Cancellation, fatal serving errors, and drain expiry do not promise that every
background task has exited or every destructor has run before control returns.
Cleanup may continue asynchronously while the Tokio runtime remains active.
There is no retained scope-result record, independent completion observer, or
guaranteed post-drop join barrier. A dropped future produces no later serving
result; cancellation of a spawned server task is observed through Tokio's task
handle, not a furpc shutdown receipt.

This relaxation does not weaken native stream safety. Once a connection is
aborted its stream is never reused, and an abandoned committed frame cannot
be followed by another frame on that stream. Retained allocations follow their
ordinary owners; cancellation does not invalidate surviving byte views.
An outer `timeout_at` cancels the caller's wait and drops serving on expiry;
use the shutdown signal instead when graceful completion is required.

## Completion results

Serving returns `Result<()>`, following tonic and [axum][axum-serve], without a
second shutdown result wrapper.

| Outcome | Serving result |
| --- | --- |
| Graceful drain or clean finite-source completion | `Ok(())` after admitted work and active connections finish. |
| Drain timeout expires | `Err` with `Timeout` and a shutdown-drain phase, after requesting cancellation of remaining work. |
| Fatal accept-source or serving/supervision failure | `Err` preserving the reported structured cause and phase, with remaining work cancelled. |
| Serving future dropped or its task aborted | No serving result; cancellation is not a successful graceful exit. |

Normal success is not a guarantee of application side-effect success, remote
receipt, or destruction of caller-retained values. Errors and cancellation are
not physical-retirement receipts. Preserve the causal error being returned;
cleanup must not replace it with a secondary generic Closed.

Ordinary connect, handshake, peer-connection, and handler errors remain in
operation/connection paths and bounded diagnostics. They do not accumulate in
a shutdown summary or make normal serving completion fail. Unexpected task
failures must be diagnosed; failures observed by serving that prevent further
execution return an error. The API does not collect every child-task outcome
into a retained scope report.
Peer-ready readiness/call failures or timeout retire the corresponding
connection with a preserved peer-ready cause, not the listener or whole
server. Success leaves the connection active; it is not an implicit domain
acknowledgment or a separate startup receipt.

## Internal task ownership

Follow [Tokio's shutdown model][tokio-shutdown] without exposing task
supervision as another application protocol:

- Use cancellation signaling, such as a private `CancellationToken`, distinct
  from active-connection completion tracking. A signal requests work to stop;
  it does not prove termination.
- Keep bounded task/completion bookkeeping while serving. Completion guards,
  a `JoinSet`, or a `TaskTracker` may support normal drain. Collect task results
  or emit bounded diagnostics as appropriate; do not retain an unbounded list.
  No particular tracker is a public guarantee.
- Coordinate admission and completion registration so normal drain cannot
  finish before registered work. [`TaskTracker::close`][task-tracker] alone does
  not prevent new work from being spawned.
- Owner/execution drop signals or aborts already-registered work; it must not
  require spawning a cleanup coordinator from `Drop`. Do not launch an
  untracked worker for every cancellation.
- Keep tasks, futures and buffers owned until their work exits, including after
  cancellation or an error; do not attach allocation-budget leases.

User code remains cooperative. Blocking handlers cannot be forcibly preempted;
application-owned tasks require application-owned cancellation and waiting.
Keep the runtime alive while graceful shutdown is awaited. Runtime destruction
can abort tasks, and furpc promises no cleanup progress after its executor stops.

[tokio-shutdown]: https://tokio.rs/tokio/topics/shutdown
[task-tracker]: https://docs.rs/tokio-util/latest/tokio_util/task/task_tracker/struct.TaskTracker.html#method.close
[tonic-serve]: https://docs.rs/tonic/latest/tonic/transport/server/struct.Router.html#method.serve_with_shutdown
[axum-serve]: https://docs.rs/axum/latest/axum/serve/struct.WithGracefulShutdown.html
