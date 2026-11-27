//! Hosting2/ApplicationHostType, ServiceModel/ServicePackageActivationContext
//! and Common/ActivityType scalar domains at native revision 3988b451.

native_enum!(NativeApplicationHostType {
    NonActivated = 0,
    ActivatedSingleCodePackage = 1,
    ActivatedMultiCodePackage = 2,
    ActivatedInProcess = 3,
});
native_enum!(NativeServicePackageActivationMode {
    SharedProcess = 0,
    ExclusiveProcess = 1,
});
native_enum!(NativeActivityType {
    Unknown = 0,
    ClientReportFaultEvent = 1,
    ServiceReportFaultEvent = 2,
    ReplicaEvent = 3,
    ServicePackageEvent = 4,
});

impl std::str::FromStr for NativeApplicationHostType {
    type Err = crate::Error;

    fn from_str(value: &str) -> crate::Result<Self> {
        [
            ("NonActivated", Self::NonActivated),
            (
                "Activated_SingleCodePackage",
                Self::ActivatedSingleCodePackage,
            ),
            (
                "Activated_MultiCodePackage",
                Self::ActivatedMultiCodePackage,
            ),
            ("Activated_InProcess", Self::ActivatedInProcess),
        ]
        .into_iter()
        .find(|(name, _)| value.eq_ignore_ascii_case(name))
        .map(|(_, kind)| kind)
        .ok_or(crate::Error::InvalidInput(
            "invalid native application host type",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn hosting_scalars_preserve_signed_long64_and_numeric_json_including_unknowns() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, 4, 5, i64::MAX] {
            check_scalar(NativeApplicationHostType::from(code), code);
            check_scalar(NativeServicePackageActivationMode::from(code), code);
            check_scalar(NativeActivityType::from(code), code);
        }
    }

    #[test]
    fn host_type_environment_names_are_checked_and_case_insensitive() {
        for (name, expected) in [
            ("NonActivated", NativeApplicationHostType::NonActivated),
            (
                "activated_singlecodepackage",
                NativeApplicationHostType::ActivatedSingleCodePackage,
            ),
            (
                "Activated_MultiCodePackage",
                NativeApplicationHostType::ActivatedMultiCodePackage,
            ),
            (
                "ACTIVATED_INPROCESS",
                NativeApplicationHostType::ActivatedInProcess,
            ),
        ] {
            assert_eq!(name.parse::<NativeApplicationHostType>().unwrap(), expected);
        }
        for name in [
            "",
            "1",
            "Activated_Unknown",
            " Activated_SingleCodePackage ",
        ] {
            assert!(name.parse::<NativeApplicationHostType>().is_err());
        }
    }
}
