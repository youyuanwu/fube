//! Linux-only experimental bounded subset of the Service Fabric native TCP protocol.
//!
//! [`Endpoint`] configures a plain-TCP native connection; [`Channel`]
//! is the existing [`native_channel::GatewayChannel`] backend, not a second
//! transport. [`accept_unsecured_tcp`] establishes an already accepted
//! socket without IPC session policy. Adapters supply immutable authorization
//! and retain domain/session ownership above the separate raw/RPC/ingress/reply
//! capabilities.

#[cfg(not(target_os = "linux"))]
compile_error!("fube supports Linux only");

mod accepted_channel;
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

pub use accepted_channel::accept_unsecured_tcp;
pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use native_channel::{ChannelLimits, GatewayChannel as Channel};
