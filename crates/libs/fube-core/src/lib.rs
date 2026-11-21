//! Shared Rust SDK contracts, native protocol identifiers and separate host execution facilities.
//! No runtime, RAP or replication-engine implementation is owned by this crate.

mod error;
pub mod hosting;
pub mod protocol;
pub mod sdk;

pub use error::{Error, ErrorKind, RejectedRegistrationInput, Result};
pub use sdk::*;
