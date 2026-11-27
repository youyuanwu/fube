//! Native HealthReport state and report-kind domains at revision 3988b451.

native_enum!(NativeHealthState {
    Invalid = 0,
    Ok = 1,
    Warning = 2,
    Error = 3,
    Unknown = 0xffff,
});

native_enum!(NativeHealthReportKind {
    Invalid = 0,
    StatefulServiceReplica = 1,
    StatelessServiceInstance = 2,
    Partition = 3,
    Node = 4,
    Service = 5,
    Application = 6,
    DeployedApplication = 7,
    DeployedServicePackage = 8,
    Cluster = 9,
});

impl From<crate::HealthState> for NativeHealthState {
    fn from(state: crate::HealthState) -> Self {
        match state {
            crate::HealthState::Invalid => Self::Invalid,
            crate::HealthState::Ok => Self::Ok,
            crate::HealthState::Warning => Self::Warning,
            crate::HealthState::Error => Self::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn health_states_preserve_signed_long64_numeric_json_and_native_unknown() {
        for (state, code) in [
            (NativeHealthState::Invalid, 0),
            (NativeHealthState::Ok, 1),
            (NativeHealthState::Warning, 2),
            (NativeHealthState::Error, 3),
            (NativeHealthState::Unknown, 0xffff),
        ] {
            check_scalar(state, code);
        }
        for code in [i64::MIN, -1, 4, 0xfffe, 0x1_0000, i64::MAX] {
            check_scalar(NativeHealthState::Unrecognized(code), code);
        }
    }

    #[test]
    fn sdk_health_conversion_uses_explicit_native_codes() {
        for (sdk, code) in [
            (crate::HealthState::Invalid, 0),
            (crate::HealthState::Ok, 1),
            (crate::HealthState::Warning, 2),
            (crate::HealthState::Error, 3),
        ] {
            check_scalar(NativeHealthState::from(sdk), code);
        }
    }

    #[test]
    fn health_report_kinds_preserve_signed_long64_numeric_json_and_unknown_codes() {
        for (kind, code) in [
            (NativeHealthReportKind::Invalid, 0),
            (NativeHealthReportKind::StatefulServiceReplica, 1),
            (NativeHealthReportKind::StatelessServiceInstance, 2),
            (NativeHealthReportKind::Partition, 3),
            (NativeHealthReportKind::Node, 4),
            (NativeHealthReportKind::Service, 5),
            (NativeHealthReportKind::Application, 6),
            (NativeHealthReportKind::DeployedApplication, 7),
            (NativeHealthReportKind::DeployedServicePackage, 8),
            (NativeHealthReportKind::Cluster, 9),
        ] {
            check_scalar(kind, code);
        }
        for code in [i64::MIN, -1, 10, i64::MAX] {
            check_scalar(NativeHealthReportKind::Unrecognized(code), code);
        }
    }
}
