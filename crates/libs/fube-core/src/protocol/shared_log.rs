//! KTL shared-log creation flags at native revision 3988b451.
//! DWORD metadata, not LONG64 enums or storage configuration admission.

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
    pub struct NativeSharedLogCreationFlags: u32 {
        const NONE = 0;
        const SPARSE_FILE = 1;
    }
}

impl serde::Serialize for NativeSharedLogCreationFlags {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.bits())
    }
}

impl<'de> serde::Deserialize<'de> for NativeSharedLogCreationFlags {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <u32 as serde::Deserialize>::deserialize(deserializer).map(Self::from_bits_retain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_log_creation_flags_preserve_uint32_numeric_json_and_unknown_bits() {
        for bits in [0u32, 1, 2, 3, 0x8000_0000, 0x8000_0001, u32::MAX] {
            let flags = match bits {
                0 => NativeSharedLogCreationFlags::NONE,
                1 => NativeSharedLogCreationFlags::SPARSE_FILE,
                bits => NativeSharedLogCreationFlags::from_bits_retain(bits),
            };
            let bytes = fubst::serde::to_vec(&bits).unwrap();
            assert_eq!(fubst::serde::to_vec(&flags).unwrap(), bytes);
            assert_eq!(
                fubst::serde::from_slice::<NativeSharedLogCreationFlags>(&bytes).unwrap(),
                flags
            );
            assert_eq!(
                serde_json::to_value(flags).unwrap(),
                serde_json::json!(bits)
            );
            assert_eq!(
                serde_json::from_value::<NativeSharedLogCreationFlags>(serde_json::json!(bits))
                    .unwrap(),
                flags
            );
            let long64 = fubst::serde::to_vec(&i64::from(bits)).unwrap();
            assert!(fubst::serde::from_slice::<NativeSharedLogCreationFlags>(&long64).is_err());
        }
        for json in [
            serde_json::json!(-1),
            serde_json::json!(u64::from(u32::MAX) + 1),
        ] {
            assert!(serde_json::from_value::<NativeSharedLogCreationFlags>(json).is_err());
        }
    }
}
