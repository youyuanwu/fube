//! Public FABRIC service-kind and access-status domains in native deployed-replica
//! query records at revision 3988b451, not RAP-internal access-state codes.

native_enum!(NativeServiceKind {
    Invalid = 0,
    Stateless = 1,
    Stateful = 2,
});

native_enum!(NativeServicePartitionAccessStatus {
    Invalid = 0,
    Granted = 1,
    ReconfigurationPending = 2,
    NotPrimary = 3,
    NoWriteQuorum = 4,
});

impl From<crate::ServicePartitionAccessStatus> for NativeServicePartitionAccessStatus {
    fn from(status: crate::ServicePartitionAccessStatus) -> Self {
        match status {
            crate::ServicePartitionAccessStatus::Invalid => Self::Invalid,
            crate::ServicePartitionAccessStatus::Granted => Self::Granted,
            crate::ServicePartitionAccessStatus::ReconfigurationPending => {
                Self::ReconfigurationPending
            }
            crate::ServicePartitionAccessStatus::NotPrimary => Self::NotPrimary,
            crate::ServicePartitionAccessStatus::NoWriteQuorum => Self::NoWriteQuorum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn query_scalars_preserve_signed_long64_numeric_json_and_unknown_codes() {
        for (kind, code) in [
            (NativeServiceKind::Invalid, 0),
            (NativeServiceKind::Stateless, 1),
            (NativeServiceKind::Stateful, 2),
        ] {
            check_scalar(kind, code);
        }
        for (status, code) in [
            (NativeServicePartitionAccessStatus::Invalid, 0),
            (NativeServicePartitionAccessStatus::Granted, 1),
            (
                NativeServicePartitionAccessStatus::ReconfigurationPending,
                2,
            ),
            (NativeServicePartitionAccessStatus::NotPrimary, 3),
            (NativeServicePartitionAccessStatus::NoWriteQuorum, 4),
        ] {
            check_scalar(status, code);
        }
        for code in [i64::MIN, -1, 3, 4, 5, i64::MAX] {
            check_scalar(NativeServiceKind::from(code), code);
            check_scalar(NativeServicePartitionAccessStatus::from(code), code);
        }
    }

    #[test]
    fn sdk_access_conversion_uses_public_fabric_codes() {
        for (sdk, code) in [
            (crate::ServicePartitionAccessStatus::Invalid, 0),
            (crate::ServicePartitionAccessStatus::Granted, 1),
            (
                crate::ServicePartitionAccessStatus::ReconfigurationPending,
                2,
            ),
            (crate::ServicePartitionAccessStatus::NotPrimary, 3),
            (crate::ServicePartitionAccessStatus::NoWriteQuorum, 4),
        ] {
            check_scalar(NativeServicePartitionAccessStatus::from(sdk), code);
        }
    }
}
