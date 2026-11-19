//! Service Fabric native RPC transport.
//!
//! Currently implements private framing, native-header decoding, byte budgets,
//! and leased segmented bodies. Connections, owned messages, RPC, and TLS are
//! not implemented yet.

mod body;
mod resource;
mod wire;
