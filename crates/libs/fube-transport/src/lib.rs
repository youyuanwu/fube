//! Linux-only experimental bounded subset of the Service Fabric native TCP protocol.

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
