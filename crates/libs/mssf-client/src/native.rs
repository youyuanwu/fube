//! Field mappings over the generic bounded codec; no domain policy belongs here.

use mssf_transport::serialization::serde::{NativeEnum, ObjectArray, Pointer};
use serde::{Deserialize, Serialize};

pub(crate) mod object_array {
    use super::*;

    pub fn serialize<T: Serialize, S: serde::Serializer>(
        value: &[T],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        ObjectArray(value).serialize(serializer)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<T>, D::Error> {
        ObjectArray::<Vec<T>>::deserialize(deserializer).map(|array| array.0)
    }
}

pub(crate) mod tagged_pointer_slots {
    use super::*;
    use serde::ser::SerializeSeq;

    #[derive(Serialize, Deserialize)]
    struct Slot<T> {
        value: Pointer<NativeEnum<T>>,
    }

    struct Slots<'a, T>(&'a [T]);

    impl<T: Serialize> Serialize for Slots<'_, T> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
            for value in self.0 {
                sequence.serialize_element(&Slot {
                    value: Pointer(Some(NativeEnum(value))),
                })?;
            }
            sequence.end()
        }
    }

    pub fn serialize<T: Serialize, S: serde::Serializer>(
        value: &[T],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }
        ObjectArray(Slots(value)).serialize(serializer)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<T>, D::Error> {
        if deserializer.is_human_readable() {
            return Vec::<T>::deserialize(deserializer);
        }
        let ObjectArray(slots) = ObjectArray::<Vec<Slot<T>>>::deserialize(deserializer)?;
        slots
            .into_iter()
            .map(|slot| {
                slot.value
                    .0
                    .map(|NativeEnum(value)| value)
                    .ok_or_else(|| serde::de::Error::custom("null required native pointer"))
            })
            .collect()
    }
}

macro_rules! strict_enum {
    ($name:ident { $($variant:ident = $code:literal),* $(,)? }) => {
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                serializer.serialize_i64(match self { $(Self::$variant => $code,)* })
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
                match <i64 as serde::Deserialize>::deserialize(deserializer)? {
                    $($code => Ok(Self::$variant),)*
                    _ => Err(serde::de::Error::custom(concat!("unsupported ", stringify!($name)))),
                }
            }
        }
    };
}
pub(crate) use strict_enum;
