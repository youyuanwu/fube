//! Selected in-memory Hosting2 contracts at native pin
//! `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
//!
//! This serialized, explicitly driven model owns no processes, sockets, leases,
//! factories. H2 [`Residency`] supplies actual admitted accounting, not deployment.
//! Success is a registration fact, not serving
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
//! Raw records/comparison probes are not validated registrations. Core
//! admission enforces native application/package constructor preconditions;
//! source assertions become explicit local invariant failures. The public
//! activation map compares native rendered string keys, including their aliases,
//! rather than substituting a structured package key.
//!
//! # Port contracts
//!
//! [`RegistrationLookup`], [`PackageUsage`], [`PackageLifecycle`] and
//! [`HostingEventSource`] document the narrow native-derived node boundary.
//! H2 usage and [`PackageScheduling`] have a deterministic implementation;
//! physical lifecycle remains definition-only. H1 model premises are not
//! production operation inputs.
//! [`H1LookupAdapter`] preserves controlled lookup; [`H2LookupAdapter`] replaces
//! scheduling with actual admitted accounting while activation stays controlled.
//! [`Hosting`] supplies typed owned event drains. Node composition, not this
//! crate, projects outputs to RA.
//!
//! # Request-only lookup with explicit H1 observations
//! ```
//! use fube_hosting::*;
//! let mut core = Hosting::new(Limits::default())?;
//! let id = TypeInstance {
//!     service_type: ServiceTypeId {
//!         package: PackageId {
//!             application: ApplicationId { type_name: "App".into(), number: 1 },
//!             name: "Pkg".into(),
//!         },
//!         name: "Widget".into(),
//!     },
//!     activation: Activation::SHARED,
//!     public_activation: String::new(),
//! };
//! core.declare_type(id.clone(), "package-component".into(), Ok(()))?;
//! let input = LookupRequest {
//!     service_type: id.service_type.clone(),
//!     version: VersionInstance::default(),
//!     activation: id.activation,
//!     service_name: "fabric:/app/service".into(),
//!     application_name: "fabric:/app".into(),
//!     should_activate: true,
//! };
//! // A controlled observation, NOT an operational PackageLifecycle implementation.
//! let model = LookupModel {
//!     activation: Some(ActivationOutcome::Synchronous(Ok(()))),
//!     ..LookupModel::default()
//! };
//! let result = RegistrationLookup::lookup(
//!     &mut H1LookupAdapter::new(&mut core, &model), &input,
//! )?;
//! assert_eq!(result.result, Err(Error::TypeNotRegistered));
//! assert!(result.candidate.is_none());
//! assert!(result.sequence.is_some()); // state read wrote it even on this error
//! // Modeled activation is an obligation, not a Registered event or residency.
//! assert!(matches!(core.take_obligations().as_slice(), [Obligation::Activation { .. }]));
//! assert!(HostingEventSource::take_closed_registered_events(&mut core).is_empty());
//! assert!(HostingEventSource::take_enabled_disabled_events(&mut core).is_empty());
//! assert_eq!(core.require_live_composition(), Err(Error::UnsupportedCapability));
//! # Ok::<(), Error>(())
//! ```
#![forbid(unsafe_code)]
#[cfg(not(target_os = "linux"))]
compile_error!("fube-hosting models the pinned Linux Hosting profile only");

mod collaborators;
pub mod contracts;
mod core;
mod events;
mod host;
mod identity;
mod lookup;
mod model;
mod residency;
mod runtime;
mod types;

pub use collaborators::*;
pub use contracts::*;
pub use core::*;
pub use events::*;
pub use host::*;
pub use identity::*;
pub use lookup::*;
pub use model::*;
pub use residency::*;
pub use runtime::*;
pub use types::*;
