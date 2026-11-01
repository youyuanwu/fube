//! Linux-only experimental bounded subset of the Service Fabric native TCP protocol.

#[cfg(not(target_os = "linux"))]
compile_error!("fube supports Linux only");

pub mod channel;
pub mod connection;
pub mod error;
pub mod frame;
pub mod message;
pub mod native_channel;
pub mod routing;
pub mod serialization;

pub use error::{Error, Result};
