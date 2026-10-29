//! Bounded asynchronous typed-record storage for the RA P1 seam.
//!
//! Accepted operations outlive their callers. Dropping a wait is not rollback.
//! Keep the [`Operation`] to observe its result, or drain with [`Store::close`].
//! This crate does not implement RA entities, recovery or lease scheduling.

#[cfg(not(target_os = "linux"))]
compile_error!("fube-ra-store supports Linux only");

mod api;
mod memory;
mod sqlite;
mod supervisor;

pub use api::*;
pub use supervisor::Store;

#[cfg(test)]
mod sqlite_tests;
#[cfg(test)]
mod tests;
