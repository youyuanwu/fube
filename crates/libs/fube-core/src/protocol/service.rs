//! Public FABRIC service-description and deployed-replica query scalar domains
//! at revision 3988b451. Access codes are not RAP-internal access-state codes.

native_enum!(NativeServiceKind {
    Invalid = 0,
    Stateless = 1,
    Stateful = 2,
});

native_enum!(NativeServiceCorrelationScheme {
    Invalid = 0,
    Affinity = 1,
    AlignedAffinity = 2,
    NonAlignedAffinity = 3,
});

native_enum!(NativeServiceLoadMetricWeight {
    Zero = 0,
    Low = 1,
    Medium = 2,
    High = 3,
});

native_enum!(NativePlacementPolicyType {
    Invalid = 0,
    InvalidDomain = 1,
    RequiredDomain = 2,
    PreferredPrimaryDomain = 3,
    RequiredDomainDistribution = 4,
    NonPartiallyPlaceService = 5,
});

/// ServiceDescription's uint defaultMoveCost_, not a LONG64 scalar enum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum NativeMoveCost {
    #[default]
    Zero,
    Low,
    Medium,
    High,
    Unrecognized(u32),
}

impl NativeMoveCost {
    pub const fn code(self) -> u32 {
        match self {
            Self::Zero => 0,
            Self::Low => 1,
            Self::Medium => 2,
            Self::High => 3,
            Self::Unrecognized(code) => code,
        }
    }

    pub const fn is_known(self) -> bool {
        !matches!(self, Self::Unrecognized(_))
    }
}

impl From<u32> for NativeMoveCost {
    fn from(code: u32) -> Self {
        match code {
            0 => Self::Zero,
            1 => Self::Low,
            2 => Self::Medium,
            3 => Self::High,
            code => Self::Unrecognized(code),
        }
    }
}

impl From<crate::MoveCost> for NativeMoveCost {
    fn from(cost: crate::MoveCost) -> Self {
        match cost {
            crate::MoveCost::Zero => Self::Zero,
            crate::MoveCost::Low => Self::Low,
            crate::MoveCost::Medium => Self::Medium,
            crate::MoveCost::High => Self::High,
        }
    }
}

impl serde::Serialize for NativeMoveCost {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.code())
    }
}

impl<'de> serde::Deserialize<'de> for NativeMoveCost {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <u32 as serde::Deserialize>::deserialize(deserializer).map(Self::from)
    }
}

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

    #[test]
    fn service_description_scalars_preserve_signed_long64_numeric_json_and_unknown_codes() {
        for (scheme, code) in [
            (NativeServiceCorrelationScheme::Invalid, 0),
            (NativeServiceCorrelationScheme::Affinity, 1),
            (NativeServiceCorrelationScheme::AlignedAffinity, 2),
            (NativeServiceCorrelationScheme::NonAlignedAffinity, 3),
        ] {
            check_scalar(scheme, code);
        }
        for (weight, code) in [
            (NativeServiceLoadMetricWeight::Zero, 0),
            (NativeServiceLoadMetricWeight::Low, 1),
            (NativeServiceLoadMetricWeight::Medium, 2),
            (NativeServiceLoadMetricWeight::High, 3),
        ] {
            check_scalar(weight, code);
        }
        for (kind, code) in [
            (NativePlacementPolicyType::Invalid, 0),
            (NativePlacementPolicyType::InvalidDomain, 1),
            (NativePlacementPolicyType::RequiredDomain, 2),
            (NativePlacementPolicyType::PreferredPrimaryDomain, 3),
            (NativePlacementPolicyType::RequiredDomainDistribution, 4),
            (NativePlacementPolicyType::NonPartiallyPlaceService, 5),
        ] {
            check_scalar(kind, code);
        }
        for code in [i64::MIN, -1, 4, 5, 6, i64::MAX] {
            check_scalar(NativeServiceCorrelationScheme::from(code), code);
            check_scalar(NativeServiceLoadMetricWeight::from(code), code);
            check_scalar(NativePlacementPolicyType::from(code), code);
        }
    }

    #[test]
    fn move_cost_preserves_uint32_wire_numeric_json_and_unknown_upper_half_values() {
        for (cost, code) in [
            (NativeMoveCost::Zero, 0u32),
            (NativeMoveCost::Low, 1),
            (NativeMoveCost::Medium, 2),
            (NativeMoveCost::High, 3),
            (NativeMoveCost::Unrecognized(4), 4),
            (NativeMoveCost::Unrecognized(0x8000_0000), 0x8000_0000),
            (NativeMoveCost::Unrecognized(u32::MAX), u32::MAX),
        ] {
            let bytes = fubst::serde::to_vec(&code).unwrap();
            assert_eq!(fubst::serde::to_vec(&cost).unwrap(), bytes);
            assert_eq!(
                fubst::serde::from_slice::<NativeMoveCost>(&bytes).unwrap(),
                cost
            );
            assert_eq!(serde_json::to_value(cost).unwrap(), serde_json::json!(code));
            assert_eq!(
                serde_json::from_value::<NativeMoveCost>(serde_json::json!(code)).unwrap(),
                cost
            );
            let long64 = fubst::serde::to_vec(&i64::from(code)).unwrap();
            assert!(fubst::serde::from_slice::<NativeMoveCost>(&long64).is_err());
        }
        for json in [
            serde_json::json!(-1),
            serde_json::json!(u64::from(u32::MAX) + 1),
        ] {
            assert!(serde_json::from_value::<NativeMoveCost>(json).is_err());
        }
    }

    #[test]
    fn sdk_move_cost_conversion_preserves_the_pinned_four_value_profile() {
        for (sdk, code) in [
            (crate::MoveCost::Zero, 0u32),
            (crate::MoveCost::Low, 1),
            (crate::MoveCost::Medium, 2),
            (crate::MoveCost::High, 3),
        ] {
            assert_eq!(
                fubst::serde::to_vec(&NativeMoveCost::from(sdk)).unwrap(),
                fubst::serde::to_vec(&code).unwrap()
            );
        }
    }
}
