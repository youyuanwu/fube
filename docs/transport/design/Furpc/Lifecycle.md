# furpc: Shutdown and cancellation

Channel lifetime is shared ownership. Server shutdown is signal-and-await.
Cancellation requests cleanup; it is not a separate public physical-retirement
protocol.

## Scope and state

A Channel owns its logical destination, retained connector, current/reconnecting
work, and ingress configuration through shared handles. Server execution owns
acceptance, accepted sessions, service invocations, and peer-ready work.
Physical generations have irreversible opening/open/draining/aborting/closed
state; a failed generation cannot reopen.

A disconnected logical Channel may establish another generation on later
demand. That does not revive old operations, Peers, or tickets. Source,
Message, Body, received requests, and native operations do not own the scope.
Application queues/workers have separate owners.

Ownership implementation lives in
[channel.rs](../../../../crates/libs/furpc/src/transport/channel.rs),
[server.rs](../../../../crates/libs/furpc/src/transport/server.rs), and
[session.rs](../../../../crates/libs/furpc/src/transport/session.rs).

## Channel lifetime

Channel clones and submitted byte-service futures retain the dialed owner.
Last-owner release closes admission and requests cancellation; it does not
flush or join outstanding work. There is no public client drain/abort or
independent closure observer.

Native drivers and ingress jobs do not retain the public owner guard.
Installing reverse ingress therefore cannot keep an otherwise idle Channel
alive. Application configuration must avoid a cycle by capturing its own
owning Channel.

Physical failure cancels generation-local invocations and unsettled native
work while retaining logical connector/service configuration for later demand.
No payload, partial write, correlation, or completion authority migrates.
An escaped Peer remains bound to its retired generation.

Await required local writes, replies, or domain ACKs before releasing the last
owner when those outcomes matter. None of those results is a general shutdown
receipt. Byte aliases retain only storage.

## Server drain

Complete the signal supplied to `Server::serve_with_shutdown` and continue
awaiting serving for graceful completion. The signal drops the acceptor,
stops new application admission/submission, and starts one absolute drain
deadline.

Already-admitted service futures and their original replies/notification
acknowledgments may finish while native controls and pending correlation
continue. Admitted peer-ready work remains tracked, bounded by its original
deadline and drain; no new hook work is admitted.

The runner's private tickets authorize completion after general submission
closes. Context/hook Peers have no exception for new semantic sends, RPCs,
or notifications. Domains requiring deferred semantic ACKs must quiesce that
work before signaling transport shutdown.

Keep application queue workers available while admitted service responses
drain, then stop them explicitly. Serving does not automatically adopt or join
application-spawned child jobs.

Clean finite accept-source exhaustion is different from shutdown: it stops
new acceptance but lets yielded establishments/connections finish normally.
An idle established peer can keep serving pending.

## Cancellation and drop

Dropping an unpolled serving future releases supplied configuration/I/O inputs
without having started acceptance. Dropping active serving instead disposes
the acceptor and cancels/aborts owned connection work. It does not initiate a
successful graceful drain or produce a later serving result.

Initial connect cancellation also cancels unpublished work, including a result
not handed to its owner. Dropping one reconnect waiter does not cancel the
shared attempt needed by other Channel owners; last-owner release does.

Cancellation, fatal serving errors, and drain expiry do not promise every
background destructor/task has completed before return. Cleanup may continue
while Tokio remains alive. There is no independent post-drop join barrier or
retained scope-result record.

An aborted stream is never reused. Committed output cannot be abandoned and
followed by another frame on the same stream. Surviving byte views remain valid
under ordinary ownership.

## Completion results

Serving returns the existing `Result<()>`, without another completion wrapper.

| Outcome | Meaning |
| --- | --- |
| Normal success | Accepted sessions and admitted tracked work finished after graceful drain or finite-source completion. |
| Shutdown-drain timeout | Remaining work is cancelled and a phase-specific Timeout returns. |
| Fatal accept/serving error | Preserve the causal error and cancel remaining owned work. |
| Serving future dropped | No serving result or successful-cleanup assertion. |

Individual connection, handshake, handler, and peer-ready errors are diagnosed
in their own paths rather than accumulated into a shutdown summary.
Peer-ready failure retires its connection, not unrelated acceptance.
Normal serving success does not imply domain side-effect success, remote
receipt, or destruction of caller-retained values.

## Internal task ownership

Connection drivers and service futures remain explicitly owned while active.
Cancellation signaling and completion observation are different mechanisms.
The serving future tracks accepted session futures; a session concurrently
drives I/O, native pending work, handlers, and its optional hook.

Drop signals or aborts already-owned work rather than depending on spawning
a cleanup worker from a destructor. Application code remains cooperative:
blocking handlers cannot be forcibly preempted, and runtime destruction
prevents further cleanup guarantees.

Executable drain/drop/finite-source behavior lives in
[transport tests](../../../../crates/libs/furpc/tests/transport.rs),
[typed-server tests](../../../../crates/libs/furpc/tests/transport/typed_server.rs),
and [notification tests](../../../../crates/libs/furpc/tests/transport/notifications.rs).
