# furpc: Security boundary

Furpc implements clear transport only. Native TLS, certificate trust, claims,
and protected-session authorization are not supported. Unsupported security
providers/controls fail explicitly; there is no fallback to clear transport.

The actual clear wire profile is constructed in
[transport/mod.rs](../../../../crates/libs/furpc/src/transport/mod.rs).
Provider validation lives in
[frame.rs](../../../../crates/libs/furpc/src/wire/frame.rs), and native
control handling in
[driver.rs](../../../../crates/libs/furpc/src/connection/driver.rs).
There is no placeholder TLS provider or public security configuration.

## Identity and authorization

URI, socket diagnostics, ListenInstance, and domain session identity are
different values. Clear transport does not authenticate any advertised
identity. A loopback-looking name or custom adapter label is not proof of
locality; IPC consumers own approved-adapter and actual socket/host checks.

Ingress checks, SendPolicy, original invocation tickets, and generation binding
constrain use of local capabilities. They are not cryptographic authentication.
Peer readiness means native transport establishment, not domain admission.

Endpoint URI schemes are addressing syntax. Built-in `tcp` has no implicit
TLS mode, and a custom encrypted byte stream does not implement Fabric native
TLS negotiation. Credentials do not belong in endpoint authorities.

## Native TLS and claims design constraints

The transport layering preserves these constraints for protected transport;
they are not an implementation sequence or a supported profile.

Native whole-frame TLS starts with tokens carried inside clear Fabric security
messages, then transitions to TLS records whose plaintext contains Fabric
frames. A stock TLS connector starting at TCP byte zero cannot substitute for
that protocol.

Transmit and receive wire phases, handshake/trust, negotiation verification,
claims authorization, and domain readiness are distinct state. The connection
progress owner must preserve final wrapped output and unread bytes exactly
once across transitions, and continue TLS output/alert/closure progress while
application traffic is idle.

Application release and accepted-peer readiness must wait for the configured
authorization gates. Each replacement generation repeats its own negotiation;
logical Channel continuity cannot transfer authentication, claims, or domain
readiness from an old generation. Failure cannot retry through a weaker
provider.

Commitment and complete-local-write accounting must include nonretractable
security output. Accepting plaintext into a TLS engine is not socket completion;
the associated ordered ciphertext must be written before issuing a receipt.
Security buffers and phase transitions remain connection-owned, outside body
codecs and routing.

Trust policy, identity binding, revocation, provider/protection support, and
native runtime interoperability are independent requirements. Neither clear
model exchanges nor encrypted custom I/O establish those properties.
