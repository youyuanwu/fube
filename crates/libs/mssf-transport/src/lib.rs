//! Experimental bounded subset of the Service Fabric native TCP protocol.

pub mod connection;
pub mod error;
pub mod frame;
pub mod message;
pub mod serialization;

pub use error::{Error, Result};
