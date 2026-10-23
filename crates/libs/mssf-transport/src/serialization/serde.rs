//! Serde over the bounded native Fabric codec, not a general-purpose wire format.
//!
//! Structs (including tuple structs) are ordered native objects. Supported scalars
//! are bool, u16/i32/u32/i64/u64 and UTF-16 strings. Newtypes are transparent.
//! Use [`Guid`], [`ByteArray`] and [`ObjectArray`] for native-specific metadata.
//! `bitflags` types with Serde enabled use their underlying integer directly
//! when its width is supported. Unknown bits are retained; validate domain rules
//! separately. Both encoding and decoding are non-human-readable.
//! Generic sequences, tuples, maps, Option, enums, unit, floats, chars and other
//! integer widths are unsupported. Owned decoding is supported; strings/bytes
//! are not borrowed from the UTF-16/native input.
//!
//! Native pointers, tags, scopes and versioned suffixes use the explicit codec
//! plus [`to_fields`]/[`from_fields`]. An ordinary Option is **not** a pointer or
//! an absent suffix. Default/skip/flatten/tagging attributes and shape-changing
//! custom serializers are outside the positional wire-preserving contract;
//! attributes invisible to a format backend cannot universally be detected.
//! Renames do not affect bytes. Supported unknown tails are skipped, not retained.
//!
//! ```
//! use mssf_transport::serialization::serde::{to_vec, from_slice, Guid};
//! use serde::{Serialize, Deserialize};
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Payload { address: String, instance: u64, nonce: Guid }
//! let value = Payload { address: "node".into(), instance: 7, nonce: Guid([0; 16]) };
//! assert_eq!(from_slice::<Payload>(&to_vec(&value).unwrap()).unwrap(), value);
//! ```

use super::{Decoder, Encoder, MAX_OBJECT_SIZE, MAX_STRING_UNITS};
use crate::{Error, Result};
use ::serde::{
    de::{DeserializeSeed, SeqAccess, Visitor},
    ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTupleStruct},
    Deserialize, Serialize,
};
use std::fmt;

const GUID_ADAPTER: &str = "$fabric::Guid";
const ARRAY_ADAPTER: &str = "$fabric::ObjectArray";

/// GUID bytes in native Windows field order (e.g. `Uuid::to_bytes_le()`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Guid(pub [u8; 16]);

struct GuidBytes<'a>(&'a [u8; 16]);
impl Serialize for GuidBytes<'_> {
    fn serialize<S: ::serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}
impl Serialize for Guid {
    fn serialize<S: ::serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(GUID_ADAPTER, &GuidBytes(&self.0))
    }
}

/// Explicit native byte-array metadata; accepts borrowed slices when encoding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ByteArray<T = Vec<u8>>(pub T);
impl<T: AsRef<[u8]>> Serialize for ByteArray<T> {
    fn serialize<S: ::serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

/// Explicit native object-array metadata, including for empty collections.
///
/// Wrap a Vec or slice for encoding; decode into `ObjectArray<Vec<T>>`.
/// Each element must describe a struct-shaped native object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectArray<T>(pub T);
impl<T: Serialize> Serialize for ObjectArray<T> {
    fn serialize<S: ::serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(ARRAY_ADAPTER, &self.0)
    }
}

struct BytesVisitor;
impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("native bytes")
    }
    fn visit_byte_buf<E: ::serde::de::Error>(
        self,
        value: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, E> {
        Ok(value)
    }
}
impl<'de> Deserialize<'de> for ByteArray {
    fn deserialize<D: ::serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor).map(Self)
    }
}
struct GuidVisitor;
impl<'de> Visitor<'de> for GuidVisitor {
    type Value = Guid;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("native GUID")
    }
    fn visit_newtype_struct<D: ::serde::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Guid, D::Error> {
        let bytes = d.deserialize_byte_buf(BytesVisitor)?;
        let value = bytes
            .try_into()
            .map_err(|_| ::serde::de::Error::custom("GUID length"))?;
        Ok(Guid(value))
    }
}
impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_newtype_struct(GUID_ADAPTER, GuidVisitor)
    }
}
struct ArrayVisitor<T>(std::marker::PhantomData<T>);
impl<'de, T: Deserialize<'de>> Visitor<'de> for ArrayVisitor<T> {
    type Value = ObjectArray<Vec<T>>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("native object array")
    }
    fn visit_newtype_struct<D: ::serde::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        Vec::<T>::deserialize(d).map(ObjectArray)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for ObjectArray<Vec<T>> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_newtype_struct(ARRAY_ADAPTER, ArrayVisitor(std::marker::PhantomData))
    }
}

/// Encode one native value. Struct roots include object framing; scalar roots do not.
pub fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    value.serialize(Serializer {
        encoder: &mut encoder,
        mode: Mode::Value,
    })?;
    Ok(encoder.finish())
}

/// Decode exactly one native value, rejecting bytes remaining outside that value.
pub fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    let mut decoder = Decoder::new(bytes);
    let value = T::deserialize(Deserializer {
        decoder: &mut decoder,
        mode: Mode::Value,
    })?;
    decoder.finish()?;
    Ok(value)
}

/// Encode a struct's fields inline in an already-open native object scope.
///
/// No new object/scope is introduced. On error prior bytes and depth are retained.
pub fn to_fields<T: Serialize + ?Sized>(encoder: &mut Encoder, value: &T) -> Result<()> {
    if encoder.depth == 0 {
        return Err(encode_error());
    }
    let start = encoder.bytes.len();
    let depth = encoder.depth;
    let result = value.serialize(Serializer {
        encoder,
        mode: Mode::Fields,
    });
    if result.is_err() {
        encoder.bytes.truncate(start);
        encoder.depth = depth;
    }
    result
}

/// Decode fixed ordered struct fields inside the current scope, without closing it.
///
/// Missing fields are errors. Use `Decoder::is_scope_end` explicitly for versioned
/// suffixes. Like the codec, errors restore enclosing limits, not input position.
pub fn from_fields<'de, T: Deserialize<'de>>(decoder: &mut Decoder<'de>) -> Result<T> {
    if decoder.depth == 0 {
        return Err(decode_error());
    }
    T::deserialize(Deserializer {
        decoder,
        mode: Mode::Fields,
    })
}

fn encode_error() -> Error {
    Error::InvalidInput("unsupported native Serde mapping or shape")
}
fn decode_error() -> Error {
    Error::Protocol("unsupported native Serde mapping or shape")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Value,
    Fields,
    Guid,
    Array,
    Object,
}

struct Serializer<'a> {
    encoder: &'a mut Encoder,
    mode: Mode,
}

#[derive(Default)]
struct BoundedText {
    text: String,
    units: usize,
    failed: bool,
}

impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.failed || value.len() > MAX_STRING_UNITS * 3 - self.text.len() {
            self.failed = true;
            return Err(fmt::Error);
        }
        let units = value.encode_utf16().count();
        if units > MAX_STRING_UNITS - self.units {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.units += units;
        self.text.push_str(value);
        Ok(())
    }
}

macro_rules! serialize_scalar {
    ($name:ident, $ty:ty, $method:ident) => {
        fn $name(self, value: $ty) -> Result<()> {
            if self.mode != Mode::Value {
                return Err(encode_error());
            }
            self.encoder.$method(value);
            Ok(())
        }
    };
}
macro_rules! reject_serialize {
    ($($name:ident($($arg:ident: $ty:ty),*));* $(;)?) => {$(
        fn $name(self, $(_: $ty),*) -> Result<()> { Err(encode_error()) }
    )*};
}
impl<'a> ::serde::Serializer for Serializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Impossible<(), Error>;

    serialize_scalar!(serialize_bool, bool, boolean);
    serialize_scalar!(serialize_u16, u16, u16);
    serialize_scalar!(serialize_i32, i32, i32);
    serialize_scalar!(serialize_u32, u32, u32);
    serialize_scalar!(serialize_i64, i64, i64);
    serialize_scalar!(serialize_u64, u64, u64);
    reject_serialize! {
        serialize_i8(v: i8); serialize_i16(v: i16); serialize_u8(v: u8);
        serialize_i128(v: i128); serialize_u128(v: u128);
        serialize_f32(v: f32); serialize_f64(v: f64); serialize_char(v: char);
        serialize_none(); serialize_unit(); serialize_unit_struct(n: &'static str);
        serialize_unit_variant(n: &'static str, i: u32, v: &'static str);
    }
    fn serialize_str(self, value: &str) -> Result<()> {
        if self.mode != Mode::Value {
            return Err(encode_error());
        }
        self.encoder.string(value)
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<()> {
        if self.mode != Mode::Value {
            return Err(encode_error());
        }
        let mut text = BoundedText::default();
        if fmt::write(&mut text, format_args!("{value}")).is_err() || text.failed {
            return Err(Error::InvalidInput(
                "native string formatting failed or exceeded limit",
            ));
        }
        self.encoder.string(&text.text)
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<()> {
        match self.mode {
            Mode::Value => self.encoder.byte_array(value),
            Mode::Guid => {
                let bytes = value.try_into().map_err(|_| encode_error())?;
                self.encoder.guid(bytes);
                Ok(())
            }
            _ => Err(encode_error()),
        }
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<()> {
        Err(encode_error())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        let mode = match (self.mode, name) {
            (Mode::Value, GUID_ADAPTER) => Mode::Guid,
            (Mode::Value, ARRAY_ADAPTER) => Mode::Array,
            (_, GUID_ADAPTER | ARRAY_ADAPTER) => return Err(encode_error()),
            _ => self.mode,
        };
        value.serialize(Self {
            encoder: self.encoder,
            mode,
        })
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<()> {
        Err(encode_error())
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if self.mode != Mode::Array {
            return Err(encode_error());
        }
        let len = len.ok_or_else(encode_error)?;
        let start = self.encoder.bytes.len();
        let depth = self.encoder.depth;
        self.encoder.object_array_header(len)?;
        Ok(Compound {
            encoder: self.encoder,
            start,
            depth,
            object: false,
            inline: false,
            remaining: len,
            complete: false,
        })
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Err(encode_error())
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.compound(len)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(encode_error())
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(encode_error())
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.compound(len)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(encode_error())
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
impl<'a> Serializer<'a> {
    fn compound(self, len: usize) -> Result<Compound<'a>> {
        if !matches!(self.mode, Mode::Value | Mode::Fields | Mode::Object) {
            return Err(encode_error());
        }
        let depth = self.encoder.depth;
        let object = self.mode != Mode::Fields;
        let start = if object {
            self.encoder.begin_object(&[])?
        } else {
            self.encoder.bytes.len()
        };
        Ok(Compound {
            encoder: self.encoder,
            start,
            depth,
            object,
            inline: !object,
            remaining: len,
            complete: false,
        })
    }
}
struct Compound<'a> {
    encoder: &'a mut Encoder,
    start: usize,
    depth: usize,
    object: bool,
    inline: bool,
    remaining: usize,
    complete: bool,
}
impl Compound<'_> {
    fn field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        if self.remaining == 0 {
            return Err(encode_error());
        }
        self.remaining -= 1;
        let mode = if self.object || self.inline {
            Mode::Value
        } else {
            Mode::Object
        };
        value.serialize(Serializer {
            encoder: self.encoder,
            mode,
        })?;
        if (self.object || self.inline || self.depth > 0)
            && self.encoder.bytes.len() - self.start > MAX_OBJECT_SIZE
        {
            return Err(Error::InvalidInput("object size limit exceeded"));
        }
        Ok(())
    }
    fn finish(mut self) -> Result<()> {
        if self.remaining != 0 {
            return Err(encode_error());
        }
        if self.object {
            self.encoder.end_object(self.start)?;
        }
        self.complete = true;
        Ok(())
    }
}
impl Drop for Compound<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.encoder.bytes.truncate(self.start);
            self.encoder.depth = self.depth;
        }
    }
}
impl SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<()> {
        self.field(value)
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}
impl SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.field(value)
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}
impl SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.field(value)
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}

struct Deserializer<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    mode: Mode,
}

macro_rules! deserialize_scalar {
    ($name:ident, $method:ident, $visit:ident) => {
        fn $name<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
            if self.mode != Mode::Value {
                return Err(decode_error());
            }
            visitor.$visit(self.decoder.$method()?)
        }
    };
}
macro_rules! reject_deserialize {
    ($($name:ident),* $(,)?) => {$(
        fn $name<V: Visitor<'de>>(self, _: V) -> Result<V::Value> { Err(decode_error()) }
    )*};
}
impl<'de> ::serde::Deserializer<'de> for Deserializer<'_, 'de> {
    type Error = Error;
    deserialize_scalar!(deserialize_bool, boolean, visit_bool);
    deserialize_scalar!(deserialize_u16, u16, visit_u16);
    deserialize_scalar!(deserialize_i32, i32, visit_i32);
    deserialize_scalar!(deserialize_u32, u32, visit_u32);
    deserialize_scalar!(deserialize_i64, i64, visit_i64);
    deserialize_scalar!(deserialize_u64, u64, visit_u64);
    deserialize_scalar!(deserialize_string, string, visit_string);
    reject_deserialize!(
        deserialize_any,
        deserialize_i8,
        deserialize_i16,
        deserialize_u8,
        deserialize_i128,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_option,
        deserialize_unit,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any
    );
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let value = match self.mode {
            Mode::Value => self.decoder.byte_array()?,
            Mode::Guid => self.decoder.guid()?.to_vec(),
            _ => return Err(decode_error()),
        };
        visitor.visit_byte_buf(value)
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_bytes(visitor)
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, _: V) -> Result<V::Value> {
        Err(decode_error())
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        let mode = match (self.mode, name) {
            (Mode::Value, GUID_ADAPTER) => Mode::Guid,
            (Mode::Value, ARRAY_ADAPTER) => Mode::Array,
            (_, GUID_ADAPTER | ARRAY_ADAPTER) => return Err(decode_error()),
            _ => self.mode,
        };
        visitor.visit_newtype_struct(Self {
            decoder: self.decoder,
            mode,
        })
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if self.mode != Mode::Array {
            return Err(decode_error());
        }
        let count = self.decoder.object_array_count()?;
        let mut access = Access {
            decoder: self.decoder,
            remaining: count,
            objects: true,
        };
        let value = visitor.visit_seq(&mut access)?;
        if access.remaining != 0 {
            return Err(decode_error());
        }
        Ok(value)
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value> {
        Err(decode_error())
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.structure(len, visitor)
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.structure(fields.len(), visitor)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        Err(decode_error())
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
impl<'de> Deserializer<'_, 'de> {
    fn structure<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        if !matches!(self.mode, Mode::Value | Mode::Fields | Mode::Object) {
            return Err(decode_error());
        }
        let read = |decoder: &mut Decoder<'de>| {
            visitor.visit_seq(Access {
                decoder,
                remaining: len,
                objects: false,
            })
        };
        if self.mode == Mode::Fields {
            read(self.decoder)
        } else {
            self.decoder.object(read)
        }
    }
}
struct Access<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    remaining: usize,
    objects: bool,
}
impl<'de> SeqAccess<'de> for Access<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if !self.objects && self.decoder.is_scope_end()? {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(Deserializer {
            decoder: self.decoder,
            mode: if self.objects {
                Mode::Object
            } else {
                Mode::Value
            },
        })
        .map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        // Do not turn an advertised count or schema field count into a reservation.
        None
    }
}

#[cfg(test)]
mod tests;
