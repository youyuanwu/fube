// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Narrow native status identities at the A0/SDK boundary.
//!
//! Native TransactionBase.cpp:192–209 retries exactly four statuses.
//! SfStatus.h defines NTSTATUS; StatusConverter.cpp:15–67 maps SF HRESULTs.
//! Do not treat every resource exhaustion or provider error as queue pressure.

use crate::Error;
use fube_runtime::ErrorKind;

/// Source statuses directly needed by the supported transaction paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Status {
    /// Quorum admission pressure.
    NoWriteQuorum,
    /// Native primary role rejection.
    NotPrimary,
    /// Reconfiguration admission pressure.
    ReconfigurationPending,
    /// Transient queue admission pressure.
    QueueFull,
    /// Invalid native operation/tail.
    InvalidOperation,
    /// Weak manager expired.
    ObjectClosed,
    /// Retry-boundary timeout.
    Timeout,
    /// Native service admission pressure.
    ServiceTooBusy,
    /// State/creation-role validation failed.
    TransactionNotActive,
    /// Permanent message sizing rejection.
    OperationTooLarge,
    /// Concurrent Reading state conflict.
    MultithreadedTransactions,
    /// System abort or failed accepted completion.
    TransactionAborted,
    /// Retry-boundary cancellation.
    Cancelled,
    /// Checked argument/registration rejection.
    InvalidParameter,
    /// Permanent capacity rejection.
    InsufficientResources,
    /// Explicitly excluded API.
    NotImplemented,
    /// Source copy corruption.
    InternalDbCorruption,
}

impl Status {
    /// Exact native HRESULT/NTSTATUS boundary value, not a Rust ordinal.
    pub(crate) fn code(self) -> i64 {
        let bits: u32 = match self {
            Self::NoWriteQuorum => 0x80071bc5,
            Self::NotPrimary => 0x80071bc6,
            Self::ReconfigurationPending => 0x80071bca,
            Self::QueueFull => 0x80071bcb,
            Self::InvalidOperation => 0x80071bfd,
            Self::ObjectClosed => 0x80071bfe,
            Self::Timeout => 0x80071bff,
            Self::ServiceTooBusy => 0x80071c25,
            Self::TransactionNotActive => 0x80071c26,
            Self::OperationTooLarge => 0x80071c2f,
            Self::MultithreadedTransactions => 0x80071c46,
            Self::TransactionAborted => 0x80071c4b,
            Self::Cancelled => 0x80004004,
            Self::InvalidParameter => 0x80070057,
            Self::InsufficientResources => 0x8007000e,
            Self::NotImplemented => 0x80004001,
            Self::InternalDbCorruption => 0xc00000e4,
        };
        bits as i32 as i64
    }

    /// Fallible SDK representation retains exact numeric source identity.
    pub(crate) fn error(self) -> Error {
        Error::Remote {
            code: self.code(),
            message: format!("{self:?}"),
        }
    }

    /// Preserve exact four-status retry set, never classify by text.
    pub(crate) fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::NoWriteQuorum
                | Self::ReconfigurationPending
                | Self::QueueFull
                | Self::ServiceTooBusy
        )
    }
}

/// Classify SDK failures without collapsing generic resource exhaustion.
pub(crate) fn retryable(error: &Error) -> bool {
    if let Some(code) = error.native_code() {
        // Native SF NTSTATUS and signed/unsigned HRESULT representations.
        let bits = code as u32;
        return [0x80071bc5, 0x80071bca, 0x80071bcb, 0x80071c25]
            .into_iter()
            .any(|hr| bits == hr || bits == (hr & 0xffff) | 0xe0430000);
    }

    matches!(
        error.kind(),
        ErrorKind::NoWriteQuorum
            | ErrorKind::ReconfigurationPending
            | ErrorKind::ReplicationQueueFull
    )
}

/// Project accepted Apply/replication results after required Unlock invocation.
///
/// TransactionManager.cpp:498–518 maps either accepted failure to
/// TransactionAborted. Unlock failure is reported separately.
pub(crate) fn accepted_result(
    apply: &crate::Result<()>,
    replication: &crate::Result<()>,
) -> crate::Result<()> {
    if apply.is_err() || replication.is_err() {
        Err(Status::TransactionAborted.error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_completion_remaps_failures_but_admission_keeps_retry_identity() {
        assert!(accepted_result(&Ok(()), &Ok(())).is_ok());
        for failure in [
            Status::NoWriteQuorum,
            Status::QueueFull,
            Status::ObjectClosed,
            Status::InsufficientResources,
            Status::OperationTooLarge,
        ] {
            for (apply, replication) in [
                (Err(failure.error()), Ok(())),
                (Ok(()), Err(failure.error())),
            ] {
                let mapped = accepted_result(&apply, &replication).unwrap_err();
                assert_eq!(
                    mapped.native_code(),
                    Some(Status::TransactionAborted.code())
                );
                assert!(!retryable(&mapped));
            }
            assert_eq!(failure.error().native_code(), Some(failure.code()));
            assert_eq!(retryable(&failure.error()), failure.is_retryable());
        }
    }

    #[test]
    fn exact_source_retry_status_table_and_native_ntstatus() {
        for status in [
            Status::NoWriteQuorum,
            Status::ReconfigurationPending,
            Status::QueueFull,
            Status::ServiceTooBusy,
        ] {
            assert!(status.is_retryable());
            assert!(retryable(&status.error()));
            let native = (status.code() as u32 & 0xffff) | 0xe0430000;
            assert!(retryable(&Error::Remote {
                code: native as i32 as i64,
                message: String::new()
            }));
        }
        for status in [
            Status::OperationTooLarge,
            Status::InsufficientResources,
            Status::NotPrimary,
            Status::ObjectClosed,
            Status::TransactionAborted,
            Status::Cancelled,
            Status::TransactionNotActive,
            Status::MultithreadedTransactions,
        ] {
            assert!(!status.is_retryable());
            assert!(!retryable(&status.error()));
        }
        assert!(!retryable(&Error::ResourceLimit("queue full")));
        assert!(!retryable(&Error::new(
            ErrorKind::Provider,
            "ServiceTooBusy"
        )));
        assert_eq!(
            Status::TransactionAborted.error().native_code(),
            Some(-2147017653)
        );
        assert_ne!(
            Status::TransactionNotActive.code(),
            Status::InvalidOperation.code()
        );
    }
}
