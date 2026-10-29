// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

use std::{
    ffi::c_void,
    panic::{catch_unwind, AssertUnwindSafe},
};
use windows_core::{Interface, Result, HRESULT};

#[cfg(test)]
pub(crate) const S_OK: HRESULT = HRESULT(0);
#[cfg(test)]
pub(crate) const E_NOINTERFACE: HRESULT = HRESULT(0x8000_4002u32 as i32);
pub(crate) const E_POINTER: HRESULT = HRESULT(0x8000_4003u32 as i32);
pub(crate) const E_INVALIDARG: HRESULT = HRESULT(0x8007_0057u32 as i32);
pub(crate) const E_UNEXPECTED: HRESULT = HRESULT(0x8000_ffffu32 as i32);
pub(crate) const E_NOTIMPL: HRESULT = HRESULT(0x8000_4001u32 as i32);

pub(crate) fn guard<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| Err(E_UNEXPECTED.into()))
}

/// # Safety
/// A non-null output must be writable for one nullable interface.
pub(crate) unsafe fn clear_out<T: Interface>(out: *mut Option<T>) -> Result<()> {
    if out.is_null() {
        return Err(E_POINTER.into());
    }
    unsafe { out.write(None) };
    Ok(())
}

/// # Safety
/// A non-null output must be writable for one nullable interface.
pub(crate) unsafe fn return_interface<T: Interface>(
    out: *mut Option<T>,
    value: Option<T>,
) -> Result<()> {
    unsafe { clear_out(out)? };
    unsafe { out.write(value) };
    Ok(())
}

/// # Safety
/// A successful non-null output owns one reference to the requested interface.
pub(crate) unsafe fn nullable_result<T: Interface>(
    status: HRESULT,
    raw: *mut c_void,
) -> Result<Option<T>> {
    // On failure the COM output has no ownership guarantee.
    status.ok()?;
    Ok(if raw.is_null() {
        None
    } else {
        Some(unsafe { T::from_raw(raw) })
    })
}
