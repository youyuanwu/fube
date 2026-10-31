//! Selected in-memory Hosting2 contracts at native pin
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
//!
//! This serialized, explicitly driven model owns no processes, sockets, leases,
//! factories or real package usage. Success is a registration fact, not serving
//! authority. Public processors and their split table/state steps are distinct.
//! Owned outputs are delivered by the caller, outside the core.
#![forbid(unsafe_code)]
#[cfg(not(target_os = "linux"))]
compile_error!("fube-hosting models the pinned Linux Hosting profile only");

mod collaborators;
mod core;
mod events;
mod host;
mod identity;
mod lookup;
mod model;
mod runtime;
mod types;

pub use collaborators::*;
pub use core::*;
pub use events::*;
pub use host::*;
pub use identity::*;
pub use lookup::*;
pub use model::*;
pub use runtime::*;
pub use types::*;
