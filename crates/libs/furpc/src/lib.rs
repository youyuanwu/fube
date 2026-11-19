//! Service Fabric native RPC transport.
//!
//! Standalone messages, ordered native headers, and shared segmented bodies.
//! Private admission-gated framing and cooperative segmented output form the
//! transport foundation.
//! Connections, RPC, and TLS are not implemented yet.

mod body;
mod error;
pub mod header;
mod message;
mod wire;

pub use error::{Error, Result};
pub use header::{ActorId, CommonHeaders, HeaderId, HeaderRecord, HeaderRef, Headers};
pub use message::{Body, Message, MessageBuilder};
pub use wire::headers::{Fault, MessageId};
