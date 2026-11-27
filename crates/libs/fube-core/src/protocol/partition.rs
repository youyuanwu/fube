//! FABRIC_SERVICE_PARTITION_KIND in native ConsistencyUnitDescription fields
//! at revision 3988b451, not the service-description partition-scheme domain.

native_enum!(NativePartitionKind {
    Invalid = 0,
    Singleton = 1,
    Int64Range = 2,
    Named = 3,
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn partition_kinds_preserve_signed_long64_and_numeric_json_including_unknowns() {
        for code in [i64::MIN, -1, 0, 1, 2, 3, 4, i64::MAX] {
            check_scalar(NativePartitionKind::from(code), code);
        }
    }
}
