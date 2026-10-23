//! Serde structs of optional scalars into bounded, named native query strings.
//!
//! Unlike the positional Fabric serializer, field names matter here. Nested
//! values require explicit projections (for example, embedded health JSON).

use mssf_transport::serialization::{MAX_COLLECTION_COUNT, MAX_STRING_UNITS};
use serde::{
    ser::{Impossible, SerializeStruct},
    Serialize, Serializer,
};

use crate::{Error, Result};

#[derive(Default)]
pub(crate) struct QueryArguments {
    pub values: Vec<(String, String)>,
}

impl QueryArguments {
    pub fn append(&mut self, value: &impl Serialize) -> Result<()> {
        value.serialize(self)
    }
}

fn bounded_string(value: &str) -> Result<String> {
    if value.len() > MAX_STRING_UNITS * 3 || value.encode_utf16().count() > MAX_STRING_UNITS {
        return Err(Error::InvalidInput("UTF-16 string limit exceeded"));
    }
    Ok(value.to_owned())
}

macro_rules! reject_scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, _: $ty) -> Result<Self::Ok> {
            Err(Error::InvalidInput("query arguments must be a struct"))
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
        fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
            Err(Error::InvalidInput("query argument maps are unsupported"))
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
        fn is_human_readable(&self) -> bool {
            false
        }
    };
}

impl Serializer for &mut QueryArguments {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Impossible<(), Error>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<(), Error>;

    reject_scalar! {
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16),
        serialize_i32(i32), serialize_i64(i64), serialize_i128(i128),
        serialize_u8(u8), serialize_u16(u16), serialize_u32(u32),
        serialize_u64(u64), serialize_u128(u128), serialize_f32(f32),
        serialize_f64(f64), serialize_char(char), serialize_str(&str),
        serialize_bytes(&[u8]),
    }
    reject_compound!();

    fn serialize_none(self) -> Result<()> {
        Err(Error::InvalidInput("query arguments must be a struct"))
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
}

impl SerializeStruct for &mut QueryArguments {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
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

    fn end(self) -> Result<()> {
        Ok(())
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
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeStatusFilter;

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
            QueryArguments::default().append(&std::collections::BTreeMap::from([("key", 1)])),
        ] {
            assert!(matches!(root, Err(Error::InvalidInput(_))));
        }
    }
}
