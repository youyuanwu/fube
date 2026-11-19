//! Service Fabric native RPC transport.
//!
//! This initial slice implements private fixed-header and checksum primitives
//! only. Connections, messages, RPC, and TLS are not implemented yet.

mod wire;
