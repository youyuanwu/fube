//! ServiceModel::UpgradeType at native revision 3988b451, not upgrade admission.

native_enum!(NativeUpgradeType {
    Invalid = 0,
    Rolling = 1,
    RollingForceRestart = 2,
    RollingNotificationOnly = 3,
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn upgrade_type_preserves_signed_long64_numeric_json_and_unknown_codes() {
        for (kind, code) in [
            (NativeUpgradeType::Invalid, 0),
            (NativeUpgradeType::Rolling, 1),
            (NativeUpgradeType::RollingForceRestart, 2),
            (NativeUpgradeType::RollingNotificationOnly, 3),
        ] {
            check_scalar(kind, code);
        }
        for code in [i64::MIN, -91, -1, 4, i64::MAX] {
            check_scalar(NativeUpgradeType::from(code), code);
        }
    }
}
