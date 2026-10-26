//! Declarative Serde structs and flattened selectors into bounded query strings.
//!
//! Values use their ordinary JSON representations. Field adapters explicitly
//! select numeric flag masks and embedded JSON without per-request serializers.

use mssf_transport::serialization::{MAX_COLLECTION_COUNT, MAX_STRING_UNITS};
use serde::{
    de::{value::MapAccessDeserializer, DeserializeSeed, IntoDeserializer, MapAccess, Visitor},
    ser::{Impossible, SerializeMap, SerializeStruct},
    Deserialize, Serialize, Serializer,
};
use std::{fmt, marker::PhantomData};

use crate::{Error, Result};

const JSON_ARGUMENT: &str = "$fabric::QueryJson";

pub(crate) fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

pub(crate) fn is_none_or_empty<T: AsRef<str>>(value: &Option<T>) -> bool {
    value.as_ref().is_none_or(|value| value.as_ref().is_empty())
}

pub(crate) fn is_none_or_nil(id: &Option<uuid::Uuid>) -> bool {
    id.is_none_or(|id| id.is_nil())
}

pub(crate) fn is_none_or_zero(id: &Option<i64>) -> bool {
    id.is_none_or(|id| id == 0)
}

pub(crate) fn validate_node_name(name: &str, error_message: &'static str) -> Result<()> {
    const MAX_NODE_NAME_UNITS: usize = 4096;
    // Bound caller work before scanning UTF-8 or counting UTF-16 units.
    if name.is_empty()
        || name.len() > MAX_NODE_NAME_UNITS * 3
        || name.contains('\0')
        || name.encode_utf16().count() > MAX_NODE_NAME_UNITS
    {
        return Err(Error::InvalidInput(error_message));
    }
    Ok(())
}

pub(crate) mod json {
    use super::*;

    pub fn serialize<T: Serialize + ?Sized, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(JSON_ARGUMENT, value)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<T, D::Error> {
        T::deserialize(deserializer)
    }
}

pub(crate) mod flags {
    use super::*;

    pub fn serialize<T: bitflags::Flags, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        T::Bits: Serialize,
    {
        value.bits().serialize(serializer)
    }

    pub fn deserialize<'de, T: bitflags::Flags, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<T, D::Error>
    where
        T::Bits: Deserialize<'de>,
    {
        T::Bits::deserialize(deserializer).map(T::from_bits_retain)
    }
}

pub(crate) mod flags_or_none {
    use super::*;

    pub fn serialize<T: bitflags::Flags, S: Serializer>(
        value: &T,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        T::Bits: Serialize,
    {
        if value.is_empty() {
            serializer.serialize_none()
        } else {
            flags::serialize(value, serializer)
        }
    }

    pub fn deserialize<'de, T: bitflags::Flags, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<T, D::Error>
    where
        T::Bits: Deserialize<'de> + Default,
    {
        Option::<T::Bits>::deserialize(deserializer)
            .map(|bits| T::from_bits_retain(bits.unwrap_or_default()))
    }
}

/// Only a genuinely empty selector map defaults; malformed selectors stay errors.
pub(crate) fn deserialize_selector<'de, T, D>(deserializer: D) -> std::result::Result<T, D::Error>
where
    T: Deserialize<'de> + Default,
    D: serde::Deserializer<'de>,
{
    struct Selector<T>(PhantomData<T>);
    impl<'de, T: Deserialize<'de> + Default> Visitor<'de> for Selector<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("zero or one query selector")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<T, A::Error> {
            let Some(key) = map.next_key::<String>()? else {
                return Ok(T::default());
            };
            let mut map = FirstKey {
                key: Some(key),
                map,
            };
            let value = T::deserialize(MapAccessDeserializer::new(&mut map))?;
            if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("multiple query selectors"));
            }
            Ok(value)
        }
    }
    deserializer.deserialize_map(Selector(PhantomData))
}

struct FirstKey<A> {
    key: Option<String>,
    map: A,
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for FirstKey<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, A::Error> {
        match self.key.take() {
            Some(key) => seed.deserialize(key.into_deserializer()).map(Some),
            None => self.map.next_key_seed(seed),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, A::Error> {
        self.map.next_value_seed(seed)
    }
}

#[derive(Default)]
pub(crate) struct QueryArguments {
    pub values: Vec<(String, String)>,
}

impl QueryArguments {
    pub fn append(&mut self, value: &impl Serialize) -> Result<()> {
        value.serialize(self)
    }

    fn field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<()> {
        if let Some(value) = value.serialize(ArgumentValue)? {
            if self.values.len() >= MAX_COLLECTION_COUNT {
                return Err(Error::InvalidInput("object-array count limit exceeded"));
            }
            if self.values.iter().any(|(name, _)| name == key) {
                return Err(Error::InvalidInput("duplicate query argument"));
            }
            self.values.push((bounded_string(key)?, value));
        }
        Ok(())
    }
}

fn bounded_string(value: &str) -> Result<String> {
    if value.len() > MAX_STRING_UNITS * 3 || value.encode_utf16().count() > MAX_STRING_UNITS {
        return Err(Error::InvalidInput("UTF-16 string limit exceeded"));
    }
    Ok(value.to_owned())
}

macro_rules! reject_scalar {
    ($message:literal; $($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, _: $ty) -> Result<Self::Ok> {
            Err(Error::InvalidInput($message))
        })*
    };
}

macro_rules! reject_compound {
    () => {
        fn serialize_unit(self) -> Result<Self::Ok> {
            Err(Error::InvalidInput("unsupported query argument value"))
        }
        fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok> {
            self.serialize_unit()
        }
        fn serialize_unit_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
        ) -> Result<Self::Ok> {
            self.serialize_unit()
        }
        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: &T,
        ) -> Result<Self::Ok> {
            self.serialize_unit()
        }
        fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
            Err(Error::InvalidInput(
                "query argument sequences are unsupported",
            ))
        }
        fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
            Err(Error::InvalidInput("query argument tuples are unsupported"))
        }
        fn serialize_tuple_struct(
            self,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            Err(Error::InvalidInput(
                "query argument tuple structs are unsupported",
            ))
        }
        fn serialize_tuple_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Err(Error::InvalidInput(
                "query argument variants are unsupported",
            ))
        }
        fn serialize_struct_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeStructVariant> {
            Err(Error::InvalidInput(
                "query argument variants are unsupported",
            ))
        }
    };
}

impl<'a> Serializer for &'a mut QueryArguments {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Impossible<(), Error>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = QueryMap<'a>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    reject_scalar! {
        "query arguments must be a struct or map";
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16),
        serialize_i32(i32), serialize_i64(i64), serialize_i128(i128),
        serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_u128(u128), serialize_f32(f32),
        serialize_f64(f64), serialize_char(char), serialize_str(&str),
        serialize_bytes(&[u8]),
    }
    reject_compound!();

    fn serialize_none(self) -> Result<()> {
        Err(Error::InvalidInput(
            "query arguments must be a struct or map",
        ))
    }
    fn serialize_some<T: ?Sized + Serialize>(self, _: &T) -> Result<()> {
        self.serialize_none()
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, _: &'static str, _: &T) -> Result<()> {
        self.serialize_none()
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(QueryMap {
            arguments: self,
            key: None,
        })
    }
}

impl SerializeStruct for &mut QueryArguments {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.field(key, value)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct QueryMap<'a> {
    arguments: &'a mut QueryArguments,
    key: Option<String>,
}

impl SerializeMap for QueryMap<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        if self.key.is_some() {
            return Err(Error::InvalidInput("query map has key without value"));
        }
        self.key = Some(key.serialize(StringKey)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let key = self
            .key
            .take()
            .ok_or(Error::InvalidInput("query map value has no key"))?;
        self.arguments.field(&key, value)
    }

    fn serialize_entry<K: Serialize + ?Sized, V: Serialize + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<()> {
        self.serialize_key(key)?;
        self.serialize_value(value)
    }

    fn end(self) -> Result<()> {
        if self.key.is_some() {
            return Err(Error::InvalidInput("query map has key without value"));
        }
        Ok(())
    }
}

struct StringKey;

impl Serializer for StringKey {
    type Ok = String;
    type Error = Error;
    type SerializeSeq = Impossible<String, Error>;
    type SerializeTuple = Impossible<String, Error>;
    type SerializeTupleStruct = Impossible<String, Error>;
    type SerializeTupleVariant = Impossible<String, Error>;
    type SerializeMap = Impossible<String, Error>;
    type SerializeStruct = Impossible<String, Error>;
    type SerializeStructVariant = Impossible<String, Error>;

    reject_scalar! {
        "query argument keys must be strings";
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16),
        serialize_i32(i32), serialize_i64(i64), serialize_i128(i128),
        serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_u128(u128), serialize_f32(f32),
        serialize_f64(f64), serialize_char(char), serialize_bytes(&[u8]),
    }
    reject_compound!();

    fn serialize_str(self, value: &str) -> Result<String> {
        bounded_string(value)
    }
    fn serialize_none(self) -> Result<String> {
        Err(Error::InvalidInput("query argument keys must be strings"))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<String> {
        self.serialize_none()
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<String> {
        self.serialize_none()
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::InvalidInput("query argument keys must be strings"))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Err(Error::InvalidInput("query argument keys must be strings"))
    }
}

struct ArgumentValue;

macro_rules! integer_value {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, value: $ty) -> Result<Self::Ok> {
            Ok(Some(value.to_string()))
        })*
    };
}

impl Serializer for ArgumentValue {
    type Ok = Option<String>;
    type Error = Error;
    type SerializeSeq = Impossible<Self::Ok, Error>;
    type SerializeTuple = Impossible<Self::Ok, Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Error>;
    type SerializeMap = Impossible<Self::Ok, Error>;
    type SerializeStruct = Impossible<Self::Ok, Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Error>;

    integer_value! {
        serialize_i8(i8), serialize_i16(i16), serialize_i32(i32),
        serialize_i64(i64), serialize_i128(i128), serialize_u8(u8),
        serialize_u16(u16), serialize_u32(u32), serialize_u64(u64),
        serialize_u128(u128),
    }
    reject_compound!();

    fn serialize_bool(self, value: bool) -> Result<Self::Ok> {
        Ok(Some(value.to_string()))
    }
    fn serialize_str(self, value: &str) -> Result<Self::Ok> {
        bounded_string(value).map(Some)
    }
    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(None)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok> {
        value.serialize(self)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        if name == JSON_ARGUMENT {
            let json = serde_json::to_string(value)
                .map_err(|_| Error::InvalidInput("failed to serialize query JSON argument"))?;
            return bounded_string(&json).map(Some);
        }
        value.serialize(self)
    }
    fn serialize_f32(self, _: f32) -> Result<Self::Ok> {
        Err(Error::InvalidInput("query argument floats are unsupported"))
    }
    fn serialize_f64(self, _: f64) -> Result<Self::Ok> {
        Err(Error::InvalidInput("query argument floats are unsupported"))
    }
    fn serialize_char(self, _: char) -> Result<Self::Ok> {
        Err(Error::InvalidInput("query argument chars are unsupported"))
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok> {
        Err(Error::InvalidInput("query argument bytes are unsupported"))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Err(Error::InvalidInput(
            "nested query argument objects are unsupported",
        ))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::InvalidInput(
            "nested query argument maps are unsupported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HealthEventsFilter, HealthEventsQueryFilter, NamingUri, NodeHealthPolicy, NodeStatusFilter,
    };

    #[test]
    fn new_query_structs_need_only_stock_serde_declarations() {
        #[derive(Serialize)]
        enum Selector {
            Category(String),
        }
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Options {
            enabled: bool,
        }
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Declared {
            address: NamingUri,
            #[serde(flatten)]
            selector: Selector,
            #[serde(with = "flags")]
            mask: NodeStatusFilter,
            #[serde(with = "json")]
            options: Options,
        }
        let query = Declared {
            address: "fabric:/App".parse().unwrap(),
            selector: Selector::Category("exact".into()),
            mask: NodeStatusFilter::from_bits_retain(u32::MAX),
            options: Options { enabled: false },
        };
        assert_eq!(
            serde_json::to_value(&query).unwrap(),
            serde_json::json!({
                "Address": "fabric:/App", "Category": "exact", "Mask": u32::MAX,
                "Options": { "Enabled": false },
            }),
        );
        let mut arguments = QueryArguments::default();
        arguments.append(&query).unwrap();
        assert_eq!(
            arguments.values,
            [
                ("Address", "fabric:/App"),
                ("Category", "exact"),
                ("Mask", "4294967295"),
                ("Options", r#"{"Enabled":false}"#),
            ]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
        );
    }

    #[test]
    fn flat_maps_preserve_scalars_options_and_reject_non_string_keys() {
        let mut arguments = QueryArguments::default();
        arguments
            .append(&std::collections::BTreeMap::from([
                ("Absent", None),
                ("Zero", Some(0u32)),
            ]))
            .unwrap();
        assert_eq!(arguments.values, [("Zero".into(), "0".into())]);
        assert!(matches!(
            QueryArguments::default().append(&std::collections::BTreeMap::from([(1u32, 2u32)])),
            Err(Error::InvalidInput("query argument keys must be strings"))
        ));
    }

    #[test]
    fn map_rows_reject_bad_order_duplicates_and_native_bounds() {
        let mut arguments = QueryArguments::default();
        let mut map = (&mut arguments).serialize_map(None).unwrap();
        assert!(matches!(
            map.serialize_value(&1u32),
            Err(Error::InvalidInput("query map value has no key"))
        ));
        map.serialize_key("Key").unwrap();
        assert!(matches!(
            map.serialize_key("Other"),
            Err(Error::InvalidInput("query map has key without value"))
        ));
        assert!(matches!(
            map.end(),
            Err(Error::InvalidInput("query map has key without value"))
        ));
        let mut map = (&mut arguments).serialize_map(None).unwrap();
        map.serialize_entry("Key", &1u32).unwrap();
        assert!(matches!(
            map.serialize_entry("Key", &2u32),
            Err(Error::InvalidInput("duplicate query argument"))
        ));
        assert!(matches!(
            map.serialize_entry(&"a".repeat(MAX_STRING_UNITS + 1), &1u32),
            Err(Error::InvalidInput("UTF-16 string limit exceeded"))
        ));
        map.end().unwrap();
        assert_eq!(arguments.values, [("Key".into(), "1".into())]);
        let mut arguments = QueryArguments {
            values: (0..MAX_COLLECTION_COUNT - 1)
                .map(|index| (index.to_string(), String::new()))
                .collect(),
        };
        let mut map = (&mut arguments).serialize_map(None).unwrap();
        map.serialize_entry("Last", &false).unwrap();
        assert!(matches!(
            map.serialize_entry("Overflow", &false),
            Err(Error::InvalidInput("object-array count limit exceeded"))
        ));
        map.end().unwrap();
        assert_eq!(arguments.values.len(), MAX_COLLECTION_COUNT);
    }

    #[test]
    fn explicit_json_rows_use_native_names_and_numeric_masks() {
        let policy = NodeHealthPolicy {
            consider_warning_as_error: false,
        };
        assert_eq!(
            json::serialize(&policy, ArgumentValue).unwrap().as_deref(),
            Some(r#"{"ConsiderWarningAsError":false}"#),
        );
        assert_eq!(
            serde_json::to_value(policy).unwrap(),
            serde_json::json!({ "ConsiderWarningAsError": false }),
        );
        for (filter, expected) in [
            (HealthEventsFilter::DEFAULT, r#"{"HealthStateFilter":0}"#),
            (HealthEventsFilter::NONE, r#"{"HealthStateFilter":1}"#),
            (HealthEventsFilter::ALL, r#"{"HealthStateFilter":65535}"#),
            (
                HealthEventsFilter::from_bits_retain(u32::MAX),
                r#"{"HealthStateFilter":4294967295}"#,
            ),
        ] {
            assert_eq!(
                json::serialize(&HealthEventsQueryFilter::from(filter), ArgumentValue)
                    .unwrap()
                    .as_deref(),
                Some(expected),
            );
            assert!(serde_json::to_value(filter).unwrap().is_string());
        }
    }

    #[test]
    fn explicit_json_preserves_escaping_and_bounds_encoded_utf16() {
        assert_eq!(
            json::serialize(&vec!["quote\"\\\n", "\u{1f980}"], ArgumentValue)
                .unwrap()
                .as_deref(),
            Some("[\"quote\\\"\\\\\\n\",\"\u{1f980}\"]"),
        );
        for (unit, count) in [
            ("a", MAX_STRING_UNITS - 2),
            ("\u{1f980}", (MAX_STRING_UNITS - 2) / 2),
            ("\0", (MAX_STRING_UNITS - 2) / 6),
        ] {
            let text = unit.repeat(count);
            let encoded = json::serialize(&text, ArgumentValue).unwrap().unwrap();
            assert_eq!(encoded.encode_utf16().count(), MAX_STRING_UNITS);
            assert!(matches!(
                json::serialize(&(text + unit), ArgumentValue),
                Err(Error::InvalidInput("UTF-16 string limit exceeded"))
            ));
        }
    }

    #[test]
    fn json_failures_are_explicit_and_json_is_not_a_root_row() {
        struct InvalidJson;
        impl Serialize for InvalidJson {
            fn serialize<S: Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("deliberate JSON failure"))
            }
        }
        assert!(matches!(
            json::serialize(&InvalidJson, ArgumentValue),
            Err(Error::InvalidInput(
                "failed to serialize query JSON argument"
            ))
        ));
        assert!(matches!(
            json::serialize(&NodeHealthPolicy::default(), &mut QueryArguments::default()),
            Err(Error::InvalidInput(
                "query arguments must be a struct or map"
            ))
        ));
    }

    #[test]
    fn named_scalars_options_newtypes_and_flags_preserve_order_and_spelling() {
        #[derive(Serialize)]
        struct Count(u64);
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Fields<'a> {
            text: &'a str,
            absent: Option<String>,
            explicit_false: bool,
            signed: i64,
            count: Count,
            #[serde(with = "flags")]
            flags: NodeStatusFilter,
            present: Option<&'a str>,
        }
        let mut arguments = QueryArguments::default();
        arguments
            .append(&Fields {
                text: "  opaque+/\0\u{1f980}  ",
                absent: None,
                explicit_false: false,
                signed: i64::MIN,
                count: Count(u64::MAX),
                flags: NodeStatusFilter::from_bits_retain(u32::MAX),
                present: Some(""),
            })
            .unwrap();
        assert_eq!(
            arguments.values,
            [
                ("Text", "  opaque+/\0\u{1f980}  "),
                ("ExplicitFalse", "false"),
                ("Signed", "-9223372036854775808"),
                ("Count", "18446744073709551615"),
                ("Flags", "4294967295"),
                ("Present", ""),
            ]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
        );
    }

    #[test]
    fn bounds_measure_utf16_and_reject_before_copying_oversized_strings() {
        #[derive(Serialize)]
        struct Text<'a> {
            value: &'a str,
        }
        for text in [
            "a".repeat(MAX_STRING_UNITS),
            "\u{ffff}".repeat(MAX_STRING_UNITS),
            "\u{1f980}".repeat(MAX_STRING_UNITS / 2),
        ] {
            let mut arguments = QueryArguments::default();
            arguments.append(&Text { value: &text }).unwrap();
            assert_eq!(arguments.values[0].1, text);
            let too_long = text + "x";
            assert!(matches!(
                QueryArguments::default().append(&Text { value: &too_long }),
                Err(Error::InvalidInput(_))
            ));
        }
    }

    #[test]
    fn duplicates_and_collection_overflow_are_errors() {
        #[derive(Serialize)]
        struct Field {
            key: u32,
        }
        let mut arguments = QueryArguments::default();
        arguments.append(&Field { key: 1 }).unwrap();
        assert!(matches!(
            arguments.append(&Field { key: 2 }),
            Err(Error::InvalidInput("duplicate query argument"))
        ));
        let mut arguments = QueryArguments {
            values: (0..MAX_COLLECTION_COUNT - 1)
                .map(|n| (n.to_string(), String::new()))
                .collect(),
        };
        arguments.append(&Field { key: 1 }).unwrap();
        #[derive(Serialize)]
        struct Extra {
            extra: bool,
        }
        assert!(matches!(
            arguments.append(&Extra { extra: true }),
            Err(Error::InvalidInput("object-array count limit exceeded"))
        ));
        assert_eq!(arguments.values.len(), MAX_COLLECTION_COUNT);
    }

    #[test]
    fn unsupported_shapes_fail_instead_of_becoming_json_or_debug_strings() {
        #[derive(Serialize)]
        struct Value<T> {
            value: T,
        }
        fn rejects(value: impl Serialize) {
            assert!(matches!(
                QueryArguments::default().append(&Value { value }),
                Err(Error::InvalidInput(_))
            ));
        }
        #[derive(Serialize)]
        enum Variant {
            Unit,
            Newtype(u32),
            Tuple(u32, u32),
            Struct { value: u32 },
        }
        #[derive(Serialize)]
        struct Tuple(u32, u32);
        rejects(1.5f32);
        rejects(f64::NAN);
        rejects('x');
        rejects(());
        rejects(Some(()));
        rejects(vec![1u32]);
        rejects((1u32, 2u32));
        rejects(Tuple(1, 2));
        rejects(Variant::Unit);
        rejects(Variant::Newtype(1));
        rejects(Variant::Tuple(1, 2));
        rejects(Variant::Struct { value: 1 });
        rejects(Value { value: 1 });
        rejects(std::collections::BTreeMap::from([("key", 1)]));
        for root in [
            QueryArguments::default().append(&1u32),
            QueryArguments::default().append(&None::<u32>),
            QueryArguments::default().append(&Some(1u32)),
            QueryArguments::default().append(&vec![1u32]),
        ] {
            assert!(matches!(root, Err(Error::InvalidInput(_))));
        }
    }
}
