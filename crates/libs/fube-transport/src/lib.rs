//! Linux-only experimental bounded subset of the Service Fabric native TCP protocol.
//!
//! [`Endpoint`] configures an explicitly trusted native connection; [`Channel`]
//! is the existing [`native_channel::GatewayChannel`] backend, not a second
//! transport. Adapters supply immutable authorization and retain domain/session
//! ownership above the separate raw/RPC/ingress/reply capabilities.

#[cfg(not(target_os = "linux"))]
compile_error!("fube supports Linux only");

pub mod channel;
pub mod connection;
mod endpoint;
pub mod error;
pub mod frame;
pub mod message;
pub mod native_channel;
pub mod native_demux;
pub mod native_ipc;
pub mod raw_session;
pub mod routing;
pub mod serialization;

pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use native_channel::{ChannelLimits, GatewayChannel as Channel};
