// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Private source-derived A0 kernels, not an A1 runtime construction surface.
//!
//! Native authority: Service Fabric 51a7703a1f83749a9234d3ceb4296d8622e42cca.
//! Controlled lower/callback seams exercise algorithms without claiming native
//! execution, mixed-replica interoperability or persisted recovery.

#![allow(dead_code, reason = "A0 kernels are private, awaiting A1 integration")]

pub(crate) mod named;
pub(crate) mod record;
