//! Reliability/FaultType at native revision 3988b451, distinct from SDK codes.

native_enum!(NativeFaultType {
    Invalid = 0,
    Transient = 1,
    Permanent = 2,
});

impl From<crate::FaultType> for NativeFaultType {
    fn from(fault: crate::FaultType) -> Self {
        match fault {
            crate::FaultType::Invalid => Self::Invalid,
            crate::FaultType::Transient => Self::Transient,
            crate::FaultType::Permanent => Self::Permanent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn fault_types_preserve_signed_long64_and_numeric_json_including_unknowns() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, i64::MAX] {
            check_scalar(NativeFaultType::from(code), code);
        }
    }

    #[test]
    fn sdk_fault_conversion_uses_native_codes_not_public_discriminants() {
        for (sdk, code) in [
            (crate::FaultType::Invalid, 0i64),
            (crate::FaultType::Transient, 1),
            (crate::FaultType::Permanent, 2),
        ] {
            check_scalar(NativeFaultType::from(sdk), code);
        }
    }
}
