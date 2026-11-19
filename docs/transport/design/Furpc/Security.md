# furpc: Deferred security design

**Status:** proposed; see the [design summary](README.md).

The initial implementation is clear TCP only. This document reserves
native negotiation/protection boundaries without requiring TLS code,
dependencies, or qualification in the initial delivery.

## Security phases

**Design coverage; implementation deferred.** The following requirements apply
to a future TLS/claims implementation. The initial clear-TCP driver must
preserve the ownership, buffering, and phase boundaries needed to add it, but
does not need to implement TLS state or negotiation.

Native whole-frame TLS begins with TLS tokens carried inside clear Fabric
security messages, then transitions to raw TLS records whose plaintext contains
Fabric frames. A stock TLS connector attached to TCP byte zero is insufficient.
Use rustls's low-level interface for this adaptation, not a new TLS engine.

Track separately:

- Socket and drain state.
- Transmit and receive wire phases.
- TLS handshake and certificate trust.
- Negotiation-metadata verification and promised authorization status.
- Claims submission/authorization, when supported.
- Domain readiness, owned above the connection.

Preserve final wrapped handshake output, ordering, and unread bytes at the
transition. Continue handling TLS-generated output, alerts, and closure while
application traffic is idle. No application message bypasses the configured
authorization release gate.
The accepted [`on_peer_ready` hook](Connections.md#accepted-peer-readiness-hook)
is published only after those configured gates, unlike native's earlier
raw connection-accepted notification. Its explicit SendPolicy does not prove
peer identity, grant domain admission, or bypass security. In the initial
clear profile, a ready Peer still carries no fabricated authentication.

Every replacement physical generation repeats the configured native/security
establishment and authorization gates before publication. Logical Channel
continuity does not carry an old generation's authentication, claims, wire
phase, or domain-session readiness into its replacement. Reconnect preserves
the configured security profile and cannot retry through a weaker provider or
clear-TCP downgrade. These remain deferred TLS/claims implementation requirements.

Unsupported providers, protection modes, renewal, or controls fail explicitly.
They never cause fallback to clear TCP. Provider approval, identity policy,
revocation behavior, and runtime interoperability remain separate release
requirements. Gateway ping and notification synchronization are not security
state.

Endpoint URI schemes are addressing syntax, not native security negotiation.
The built-in connector accepts only the documented clear `tcp://` form;
`https` or a custom connector's secure-looking scheme cannot imply Fabric TLS,
claims, peer authentication, or an authorization bypass. Native security
configuration remains explicit and unsupported profiles fail before
application traffic. A logical URI or loopback-looking host also does not
replace actual IPC socket/host checks. Do not put credentials in authorities
or log sensitive custom URI path/query data as connection diagnostics.
