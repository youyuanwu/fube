//! Service Fabric native RPC transport.
//!
//! Standalone messages, ordered native headers, and leased segmented bodies.
//! Private framing and byte accounting form the transport foundation.
//! Connections, RPC, and TLS are not implemented yet.

mod body;
pub mod construction;
mod error;
pub mod header;
mod message;
mod resource;
mod wire;

pub use error::{Error, Result};
pub use header::{ActorId, CommonHeaders, HeaderId, HeaderRecord, HeaderRef, Headers};
pub use message::{Body, Message, MessageBuilder};
pub use wire::headers::{Fault, MessageId};
