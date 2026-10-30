//! Bounded asynchronous typed-record storage for the RA P1 seam.
//!
//! Accepted operations outlive their callers. Dropping a wait is not rollback.
//! Keep the [`Operation`] to observe its result, or drain with [`Store::close`].
//! This crate does not implement RA entities, recovery or lease scheduling.
//!
//! ```
//! use fube_ra_store::{Backend, KeyValueStore, MemoryBackend, Record, RecordId, Request, Settings, Store};
//! use std::sync::Arc;
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> fube_ra_store::Result<()> {
//!     let backend: Box<dyn Backend> = Box::new(MemoryBackend::new());
//!     let store: Arc<dyn KeyValueStore> =
//!         Arc::new(Store::new(backend, Settings::default()).await?);
//!     let ticket = store.submit(Request::Insert(Record {
//!         key: RecordId::new("example/v1", "entity-1"),
//!         value: b"opaque payload".to_vec(),
//!     }))?;
//!     ticket.wait().await?;
//!     assert!(store.close().await?.is_clean());
//!     Ok(())
//! }
//! ```

#[cfg(not(target_os = "linux"))]
compile_error!("fube-ra-store supports Linux only");

mod api;
mod backend;
mod memory;
mod sqlite;
mod supervisor;

pub use api::*;
pub use backend::{Backend, BackendContext};
pub use memory::MemoryBackend;
pub use sqlite::SqliteBackend;
pub use supervisor::Store;

#[cfg(test)]
mod sqlite_tests;
#[cfg(test)]
mod tests;
