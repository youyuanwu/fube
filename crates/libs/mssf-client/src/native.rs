//! Field mappings over the generic bounded codec; no domain policy belongs here.

use mssf_transport::serialization::serde::{NativeEnum, ObjectArray, Pointer};
use serde::{Deserialize, Serialize};

pub(crate) mod untagged {
    use super::*;
    use mssf_transport::serialization::serde::Untagged;

    pub fn deserialize<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        if deserializer.is_human_readable() {
            return T::deserialize(deserializer);
        }
        Untagged::<T>::deserialize(deserializer).map(|Untagged(value)| value)
    }

    pub fn deserialize_trailing<'de, T: Deserialize<'de> + Default, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        #[derive(Default, Deserialize)]
        #[serde(bound(deserialize = "T: Deserialize<'de>"))]
        struct Value<T>(#[serde(deserialize_with = "deserialize")] T);

        super::trailing_default::deserialize::<Value<T>, D>(deserializer).map(|Value(value)| value)
    }
}

pub(crate) mod string_pointer {
    use super::*;

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        if deserializer.is_human_readable() {
            return Option::<String>::deserialize(deserializer);
        }
        #[derive(Deserialize)]
        struct Value {
            value: String,
        }

        Pointer::<Value>::deserialize(deserializer)
            .map(|pointer| pointer.0.map(|value| value.value))
    }

    pub fn deserialize_trailing<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<String>, D::Error> {
        #[derive(Default, Deserialize)]
        struct Value(#[serde(deserialize_with = "deserialize")] Option<String>);

        super::trailing_default::deserialize::<Value, D>(deserializer).map(|Value(value)| value)
    }
}

pub(crate) mod guid {
    use super::*;
    use mssf_transport::serialization::serde::Guid;

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<uuid::Uuid, D::Error> {
        if deserializer.is_human_readable() {
            return uuid::Uuid::deserialize(deserializer);
        }
        Guid::deserialize(deserializer).map(|guid| uuid::Uuid::from_bytes_le(guid.0))
    }
}

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

pub(crate) mod trailing_default {
    use super::*;
    use mssf_transport::serialization::serde::Trailing;

    pub fn deserialize<'de, T: Deserialize<'de> + Default, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<T, D::Error> {
        if deserializer.is_human_readable() {
            return T::deserialize(deserializer);
        }
        Trailing::<T>::deserialize(deserializer).map(|Trailing(value)| value.unwrap_or_default())
    }
}

pub(crate) mod string_map {
    use super::*;
    use std::collections::BTreeMap;

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, String>, D::Error> {
        if deserializer.is_human_readable() {
            return BTreeMap::deserialize(deserializer);
        }
        #[derive(Deserialize)]
        struct Pair(String, String);
        let ObjectArray(pairs) = ObjectArray::<Vec<Pair>>::deserialize(deserializer)?;
        let mut values = BTreeMap::new();
        for Pair(key, value) in pairs {
            if values.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate native string-map key"));
            }
        }
        Ok(values)
    }

    pub fn deserialize_trailing<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<String, String>, D::Error> {
        #[derive(Default, Deserialize)]
        struct Map(#[serde(deserialize_with = "deserialize")] BTreeMap<String, String>);
        super::trailing_default::deserialize::<Map, D>(deserializer).map(|Map(values)| values)
    }
}

pub(crate) mod derived_guid {
    use super::*;
    use mssf_transport::serialization::serde::Derived;

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<uuid::Uuid, D::Error> {
        if deserializer.is_human_readable() {
            return uuid::Uuid::deserialize(deserializer);
        }
        #[derive(Deserialize)]
        struct Fields {
            #[serde(deserialize_with = "super::guid::deserialize")]
            id: uuid::Uuid,
        }
        let Derived(fields) = Derived::<Fields>::deserialize(deserializer)?;
        Ok(fields.id)
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
    (
        $(#[$meta:meta])*
        $visibility:vis enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident = $code:literal),* $(,)?
        }
    ) => {
        // Native codes are Serde values, not Rust discriminants.
        $(#[$meta])*
        $visibility enum $name {
            $($(#[$variant_meta])* $variant,)*
        }

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
