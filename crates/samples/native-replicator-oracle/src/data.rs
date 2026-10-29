// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE.service-fabric-apps-rs at the repository root.

//! Owned immutable segments exposed using the published native buffer layout.

use std::{ptr, slice};

use mssf_com::{
    FabricRuntime::{IFabricOperationData, IFabricOperationData_Impl},
    FabricTypes::FABRIC_OPERATION_DATA_BUFFER,
};
use windows_core::{implement, Result};

use crate::com;

pub const MAX_SEGMENTS: usize = 1024;
pub const MAX_OPERATION_BYTES: usize = 1024 * 1024;

#[implement(IFabricOperationData)]
struct Segments {
    _bytes: Box<[Box<[u8]>]>,
    buffers: Box<[FABRIC_OPERATION_DATA_BUFFER]>,
}

// Descriptors point into stable owned boxes; the COM contract borrows them read-only.
unsafe impl Send for Segments {}
unsafe impl Sync for Segments {}

/// Empty data and empty individual segments remain non-null operation objects.
pub fn operation_data(segments: Vec<Vec<u8>>) -> Result<IFabricOperationData> {
    if segments.len() > MAX_SEGMENTS {
        return Err(com::E_INVALIDARG.into());
    }
    let total = segments.iter().try_fold(0usize, |total, segment| {
        total.checked_add(segment.len()).ok_or(com::E_INVALIDARG)
    })?;
    if total > MAX_OPERATION_BYTES {
        return Err(com::E_INVALIDARG.into());
    }
    let bytes: Box<[Box<[u8]>]> = segments.into_iter().map(Vec::into_boxed_slice).collect();
    let buffers = bytes
        .iter()
        .map(|bytes| FABRIC_OPERATION_DATA_BUFFER {
            BufferSize: bytes.len() as u32,
            // Native ValidateOperation rejects NULL even for a zero-byte
            // segment. The owned empty slice has a non-null zero-length pointer.
            Buffer: bytes.as_ptr().cast_mut(),
        })
        .collect();
    Ok(Segments {
        _bytes: bytes,
        buffers,
    }
    .into())
}

impl IFabricOperationData_Impl for Segments_Impl {
    // The generated ABI thunk requires a valid descriptor-output pointer.
    fn GetData(&self, count: *mut u32) -> Result<*mut FABRIC_OPERATION_DATA_BUFFER> {
        if count.is_null() {
            return Err(com::E_POINTER.into());
        }
        unsafe { count.write(self.buffers.len() as u32) };
        Ok(if self.buffers.is_empty() {
            ptr::null_mut()
        } else {
            self.buffers.as_ptr().cast_mut()
        })
    }
}

/// Copy a native operation while retaining its logical segment boundaries.
///
/// # Safety
/// The COM implementation must return readable descriptor and buffer storage
/// valid until the operation is released, as required by IFabricOperationData.
pub unsafe fn read_segments(data: &IFabricOperationData) -> Result<Vec<Vec<u8>>> {
    let mut count = 0;
    let buffers = unsafe { data.GetData(&mut count)? };
    unsafe { read_buffers(count, buffers) }
}

/// # Safety
/// The operation must honor the native borrowed buffer lifetime contract.
pub unsafe fn read_operation(
    operation: &mssf_com::FabricRuntime::IFabricOperation,
) -> Result<Vec<Vec<u8>>> {
    let mut count = 0;
    let buffers = unsafe { operation.GetData(&mut count)? };
    unsafe { read_buffers(count, buffers) }
}

unsafe fn read_buffers(
    count: u32,
    buffers: *mut FABRIC_OPERATION_DATA_BUFFER,
) -> Result<Vec<Vec<u8>>> {
    if count as usize > MAX_SEGMENTS {
        return Err(com::E_INVALIDARG.into());
    }
    if count == 0 {
        return Ok(Vec::new());
    }
    if buffers.is_null() {
        return Err(com::E_POINTER.into());
    }
    let descriptors = unsafe { slice::from_raw_parts(buffers, count as usize) };
    let mut total = 0usize;
    for descriptor in descriptors {
        total = total
            .checked_add(descriptor.BufferSize as usize)
            .ok_or(com::E_INVALIDARG)?;
        if total > MAX_OPERATION_BYTES {
            return Err(com::E_INVALIDARG.into());
        }
        if descriptor.BufferSize != 0 && descriptor.Buffer.is_null() {
            return Err(com::E_POINTER.into());
        }
    }
    Ok(descriptors
        .iter()
        .map(|descriptor| {
            if descriptor.BufferSize == 0 {
                Vec::new()
            } else {
                unsafe {
                    slice::from_raw_parts(descriptor.Buffer, descriptor.BufferSize as usize)
                        .to_vec()
                }
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_replication_validation_accepts_nonnull_zero_length_segments() {
        let segments = vec![vec![], vec![1, 2, 3], vec![]];
        let data = operation_data(segments.clone()).unwrap();
        let mut count = 0;
        let buffers = unsafe { data.GetData(&mut count) }.unwrap();
        assert_eq!(count, 3);
        let descriptors = unsafe { slice::from_raw_parts(buffers, count as usize) };
        assert_eq!(
            descriptors.iter().map(|b| b.BufferSize).collect::<Vec<_>>(),
            vec![0, 3, 0]
        );
        // EnumOperators.cpp ValidateOperation requires every Buffer != NULL;
        // it does not dereference a zero-length segment.
        assert!(descriptors.iter().all(|b| !b.Buffer.is_null()));
        assert_eq!(unsafe { read_segments(&data) }.unwrap(), segments);
        let empty = operation_data(vec![]).unwrap();
        assert!(unsafe { empty.GetData(&mut count) }.unwrap().is_null());
        assert_eq!(count, 0);
    }
}
