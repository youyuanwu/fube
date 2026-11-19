//! Service Fabric native RPC transport.
//!
//! Currently implements private framing/checksum primitives and borrowed,
//! ordered native-header decoding. Connections, owned messages, RPC, and TLS
//! are not implemented yet.

mod wire;
