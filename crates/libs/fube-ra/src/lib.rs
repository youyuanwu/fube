//! Native reliability protocol foundation, not a running RA or serving authority.
//!
//! Native source baseline: `3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d`.
//! See `docs/cluster/NativeRaProtocol.md` for source contracts and staged gaps.

#[cfg(not(target_os = "linux"))]
compile_error!("fube-ra supports Linux only");

pub mod entity;
pub mod protocol;
