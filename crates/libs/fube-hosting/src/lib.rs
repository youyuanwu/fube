//! Selected in-memory Hosting2 contracts at native pin
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
//!
//! This serialized, explicitly driven model owns no processes, sockets, leases,
//! factories or real package usage. Success is a registration fact, not serving
//! authority. Public processors and their split table/state steps are distinct.
//! Owned outputs are delivered by the caller, outside the core.
//!
//! # Driving the model
//! ```
//! use fube_hosting::{Hosting, Limits, ManualClock};
//! let mut core = Hosting::new(Limits::default())?;
//! core.add_host("modeled-host".into())?;
//! core.start_host("modeled-host", &ManualClock(0), 100)?;
//! let finish = core.finish_host("modeled-host", true)?;
//! assert!(finish.reply.is_ok());
//! assert_eq!(core.is_host_registered("modeled-host")?, true);
//! // Public duplicate Finish succeeds even though the state method rejects it.
//! let duplicate = core.finish_host("modeled-host", true)?;
//! assert!(duplicate.reply.is_ok());
//! assert!(duplicate.state_method.unwrap().is_err());
//! # Ok::<(), fube_hosting::Error>(())
//! ```
//! For races use the separate Start arm/state, Finish find/state and timeout
//! find/mark/remove handles, not wall-clock sleeps. `LookupModel` has no default
//! successful package outcome. `LookupResult` retains candidate and sequence
//! independently from its result. Drain each event lane and obligations under
//! a separately bounded delivery owner; draining is not external completion.
//! `close_core` reports retained work, not successful process cleanup.
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
