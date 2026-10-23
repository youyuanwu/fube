//! Serde over the bounded native Fabric codec, not a general-purpose wire format.
//!
//! Structs (including tuple structs) are ordered native objects. Supported scalars
//! are bool, u16/i32/u32/i64/u64 and UTF-16 strings. Newtypes are transparent.
//! Use [`Guid`], [`ByteArray`] and [`ObjectArray`] for native-specific metadata.
//! `bitflags` types with Serde enabled use their underlying integer directly
//! when its width is supported. Unknown bits are retained; validate domain rules
//! separately. Both encoding and decoding are non-human-readable.
//! Bare sequences, tuples, maps, Option, enums, unit, floats, chars and other
//! integer widths are unsupported. Owned decoding is supported; strings/bytes
//! are not borrowed from the UTF-16/native input.
//!
//! Native pointers, tags, scopes and versioned suffixes use explicit adapters:
//! [`Pointer`], [`Typed`]/[`NativeType`], [`Inline`], [`Derived`] and [`Trailing`].
//! [`CountedStrings`] handles UInt32-counted string sequences.
//! [`FixedTag`] and [`Untagged`] automatically validate struct tags.
//! [`NativeEnum`] uses explicitly renamed i32 variant codes for polymorphic
//! objects and checks their matching i64 body kinds. [`NumericEnum`] is separate:
//! a strict i64 scalar mapping for derived unit enums. Variant ordinals and Rust
//! discriminants are never inferred as native codes.
//! An ordinary Option is **not** a pointer or
//! an absent suffix. Default/skip/flatten/enum-representation attributes and shape-changing
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
//!
//! Derived polymorphism requires no `NativeType` implementation or body-kind field:
//!
//! ```
//! use mssf_transport::serialization::serde::{from_slice, to_vec, NativeEnum, Pointer};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! enum Evaluation {
//!     #[serde(rename = "1")]
//!     Event { description: String },
//!     #[serde(rename = "0")]
//!     Invalid,
//! }
//! let value = Pointer(Some(NativeEnum(Evaluation::Event { description: "event".into() })));
//! let bytes = to_vec(&value).unwrap();
//! assert_eq!(from_slice::<Pointer<NativeEnum<Evaluation>>>(&bytes).unwrap(), value);
//! ```

use super::{Decoder, Encoder, MAX_COLLECTION_COUNT, MAX_OBJECT_SIZE, MAX_STRING_UNITS};
use crate::{Error, Result};
use ::serde::{
    de::{DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
    ser::{
        Impossible, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTupleStruct,
        SerializeTupleVariant,
    },
    Deserialize, Serialize,
};
use std::fmt;

mod adapters;
pub use adapters::{
    CountedStrings, Derived, FixedTag, Inline, NativeEnum, NativeType, NumericEnum, Pointer,
    Trailing, Typed, Untagged,
};

const GUID_ADAPTER: &str = "$fabric::Guid";
const ARRAY_ADAPTER: &str = "$fabric::ObjectArray";
const POINTER_ADAPTER: &str = "$fabric::Pointer";
const TYPED_ADAPTER: &str = "$fabric::Typed";
const INLINE_ADAPTER: &str = "$fabric::Inline";
const DERIVED_ADAPTER: &str = "$fabric::Derived";
const TRAILING_ADAPTER: &str = "$fabric::Trailing";
const STRINGS_ADAPTER: &str = "$fabric::CountedStrings";
const ENUM_ADAPTER: &str = "$fabric::NativeEnum";
const NUMERIC_ENUM_ADAPTER: &str = "$fabric::NumericEnum";

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
    fn visit_bytes<E: ::serde::de::Error>(self, value: &[u8]) -> std::result::Result<Vec<u8>, E> {
        Ok(value.to_vec())
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
        type_info: None,
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
        type_info: None,
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
    Pointer,
    Typed,
    TypedPointer,
    TypeInfo,
    Contents,
    Trailing,
    Strings,
    StringElement,
    Enum,
    EnumPointer,
    EnumContents,
    NumericEnum,
}

fn variant_code(name: &str) -> Option<i64> {
    if name.len() > 20 {
        return None;
    }
    let code = name.parse::<i64>().ok()?;
    (code.to_string() == name).then_some(code)
}

fn variant_name(
    code: i64,
    variants: &'static [&'static str],
    numeric: bool,
) -> Result<&'static str> {
    let mut selected = None;
    for &name in variants {
        let value = variant_code(name).ok_or_else(decode_error)?;
        if !numeric && i32::try_from(value).is_err() {
            return Err(decode_error());
        }
        if value == code {
            if selected.is_some() {
                return Err(Error::Protocol("duplicate native enum code"));
            }
            selected = Some(name);
        }
    }
    selected.ok_or(Error::Protocol("unsupported native enum code"))
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
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

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
        serialize_unit(); serialize_unit_struct(n: &'static str);
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<()> {
        if self.mode == Mode::NumericEnum {
            self.encoder
                .i64(variant_code(variant).ok_or_else(encode_error)?);
            return Ok(());
        }
        self.enum_compound(variant, 0)?.finish()
    }
    fn serialize_str(self, value: &str) -> Result<()> {
        if !matches!(self.mode, Mode::Value | Mode::StringElement) {
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
            Mode::TypeInfo => self.encoder.begin_object(value).map(|_| ()),
            _ => Err(encode_error()),
        }
    }
    fn serialize_none(self) -> Result<()> {
        match self.mode {
            Mode::Pointer => self.encoder.pointer(None, |_| unreachable!()),
            Mode::Trailing => Ok(()),
            _ => Err(encode_error()),
        }
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        let mode = match self.mode {
            Mode::Pointer => Mode::Pointer,
            Mode::Trailing => Mode::Value,
            _ => return Err(encode_error()),
        };
        value.serialize(Self {
            encoder: self.encoder,
            mode,
        })
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        let mode = match (self.mode, name) {
            (Mode::Value, GUID_ADAPTER) => Mode::Guid,
            (Mode::Value, ARRAY_ADAPTER) => Mode::Array,
            (Mode::Value, POINTER_ADAPTER) => Mode::Pointer,
            (Mode::Value | Mode::Object, TYPED_ADAPTER) => Mode::Typed,
            (Mode::Pointer, TYPED_ADAPTER) => Mode::TypedPointer,
            (Mode::Value | Mode::Object, ENUM_ADAPTER) => Mode::Enum,
            (Mode::Pointer, ENUM_ADAPTER) => Mode::EnumPointer,
            (Mode::Value, NUMERIC_ENUM_ADAPTER) => Mode::NumericEnum,
            (Mode::Value, TRAILING_ADAPTER) if self.encoder.depth > 0 => Mode::Trailing,
            (Mode::Value, INLINE_ADAPTER | DERIVED_ADAPTER) if self.encoder.depth > 0 => {
                if name == DERIVED_ADAPTER {
                    self.encoder.next_scope()?;
                }
                Mode::Fields
            }
            (Mode::Value, STRINGS_ADAPTER) => Mode::Strings,
            (
                _,
                GUID_ADAPTER | ARRAY_ADAPTER | POINTER_ADAPTER | TYPED_ADAPTER | TRAILING_ADAPTER
                | INLINE_ADAPTER | DERIVED_ADAPTER | STRINGS_ADAPTER | ENUM_ADAPTER
                | NUMERIC_ENUM_ADAPTER,
            ) => return Err(encode_error()),
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
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        let compound = self.enum_compound(variant, 0)?;
        value.serialize(Serializer {
            encoder: compound.encoder,
            mode: Mode::Fields,
        })?;
        compound.finish()
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if !matches!(self.mode, Mode::Array | Mode::Strings) {
            return Err(encode_error());
        }
        let len = len.ok_or_else(encode_error)?;
        let start = self.encoder.bytes.len();
        let depth = self.encoder.depth;
        let framing = if self.mode == Mode::Array {
            self.encoder.object_array_header(len)?;
            Framing::Array
        } else {
            if len > MAX_COLLECTION_COUNT {
                return Err(Error::InvalidInput("counted-string limit exceeded"));
            }
            self.encoder.u32(len as u32);
            Framing::Strings
        };
        Ok(Compound {
            encoder: self.encoder,
            start,
            depth,
            framing,
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
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.enum_compound(variant, len)
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
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.enum_compound(variant, len)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
impl<'a> Serializer<'a> {
    fn enum_compound(self, variant: &'static str, len: usize) -> Result<Compound<'a>> {
        if !matches!(self.mode, Mode::Enum | Mode::EnumPointer) {
            return Err(encode_error());
        }
        let code: i32 = variant_code(variant)
            .ok_or_else(encode_error)?
            .try_into()
            .map_err(|_| encode_error())?;
        let pointer = self.mode == Mode::EnumPointer;
        let start = self.encoder.bytes.len();
        let depth = self.encoder.depth;
        if pointer {
            self.encoder.bytes.push(super::POINTER);
        }
        if let Err(error) = self.encoder.begin_object(&code.to_le_bytes()) {
            self.encoder.bytes.truncate(start);
            return Err(error);
        }
        self.encoder.i64(i64::from(code));
        Ok(Compound {
            encoder: self.encoder,
            start,
            depth,
            framing: Framing::Object { pointer },
            remaining: len,
            complete: false,
        })
    }

    fn compound(self, len: usize) -> Result<Compound<'a>> {
        if !matches!(
            self.mode,
            Mode::Value
                | Mode::Fields
                | Mode::Object
                | Mode::Pointer
                | Mode::Typed
                | Mode::TypedPointer
        ) {
            return Err(encode_error());
        }
        let depth = self.encoder.depth;
        let start = self.encoder.bytes.len();
        let framing = match self.mode {
            Mode::Fields => Framing::Fields,
            Mode::Typed | Mode::TypedPointer => {
                if len != 2 {
                    return Err(encode_error());
                }
                let pointer = self.mode == Mode::TypedPointer;
                if pointer {
                    self.encoder.bytes.push(super::POINTER);
                }
                Framing::Typed { pointer }
            }
            Mode::Pointer => {
                self.encoder.bytes.push(super::POINTER);
                if let Err(error) = self.encoder.begin_object(&[]) {
                    self.encoder.bytes.truncate(start);
                    return Err(error);
                }
                Framing::Object { pointer: true }
            }
            _ => {
                self.encoder.begin_object(&[])?;
                Framing::Object { pointer: false }
            }
        };
        Ok(Compound {
            encoder: self.encoder,
            start,
            depth,
            framing,
            remaining: len,
            complete: false,
        })
    }
}
#[derive(Clone, Copy)]
enum Framing {
    Object { pointer: bool },
    Typed { pointer: bool },
    Fields,
    Array,
    Strings,
}
struct Compound<'a> {
    encoder: &'a mut Encoder,
    start: usize,
    depth: usize,
    framing: Framing,
    remaining: usize,
    complete: bool,
}
impl Compound<'_> {
    fn field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        if self.remaining == 0 {
            return Err(encode_error());
        }
        self.remaining -= 1;
        let mode = match self.framing {
            Framing::Array => Mode::Object,
            Framing::Strings => Mode::StringElement,
            Framing::Typed { .. } if self.remaining == 1 => Mode::TypeInfo,
            Framing::Typed { .. } => Mode::Fields,
            _ => Mode::Value,
        };
        value.serialize(Serializer {
            encoder: self.encoder,
            mode,
        })?;
        if (!matches!(self.framing, Framing::Array | Framing::Strings) || self.depth > 0)
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
        if let Framing::Object { pointer } | Framing::Typed { pointer } = self.framing {
            self.encoder.end_object(self.start + usize::from(pointer))?;
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
impl SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.field(value)
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}
impl SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, _: &'static str, value: &T) -> Result<()> {
        self.field(value)
    }
    fn end(self) -> Result<()> {
        self.finish()
    }
}

struct Deserializer<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    mode: Mode,
    type_info: Option<&'de [u8]>,
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
        deserialize_unit,
        deserialize_map,
        deserialize_identifier,
        deserialize_ignored_any
    );
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.mode, Mode::Value | Mode::StringElement) {
            return Err(decode_error());
        }
        visitor.visit_string(self.decoder.string()?)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.mode {
            Mode::Pointer => {
                let mut visitor = Some(visitor);
                let value = self.decoder.pointer(|decoder, type_info| {
                    visitor
                        .take()
                        .ok_or_else(decode_error)?
                        .visit_some(Deserializer {
                            decoder,
                            mode: Mode::Contents,
                            type_info: Some(type_info),
                        })
                })?;
                match value {
                    Some(value) => Ok(value),
                    None => visitor.ok_or_else(decode_error)?.visit_none(),
                }
            }
            Mode::Trailing if self.decoder.is_scope_end()? => visitor.visit_none(),
            Mode::Trailing => visitor.visit_some(Self {
                decoder: self.decoder,
                mode: Mode::Value,
                type_info: None,
            }),
            _ => Err(decode_error()),
        }
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
        if name == ENUM_ADAPTER {
            return match self.mode {
                Mode::Value | Mode::Object => self.decoder.typed_object(|decoder, type_info| {
                    visitor.visit_newtype_struct(Deserializer {
                        decoder,
                        mode: Mode::EnumContents,
                        type_info: Some(type_info),
                    })
                }),
                Mode::Contents => visitor.visit_newtype_struct(Self {
                    decoder: self.decoder,
                    mode: Mode::EnumContents,
                    type_info: self.type_info,
                }),
                _ => Err(decode_error()),
            };
        }
        if name == TYPED_ADAPTER {
            return match self.mode {
                Mode::Value | Mode::Object => self.decoder.typed_object(|decoder, type_info| {
                    visitor.visit_seq(TypeAccess {
                        decoder,
                        type_info,
                        remaining: 2,
                    })
                }),
                Mode::Contents => visitor.visit_seq(TypeAccess {
                    decoder: self.decoder,
                    type_info: self.type_info.ok_or_else(decode_error)?,
                    remaining: 2,
                }),
                _ => Err(decode_error()),
            };
        }
        let mode = match (self.mode, name) {
            (Mode::Value, GUID_ADAPTER) => Mode::Guid,
            (Mode::Value, ARRAY_ADAPTER) => Mode::Array,
            (Mode::Value, POINTER_ADAPTER) => Mode::Pointer,
            (Mode::Value, NUMERIC_ENUM_ADAPTER) => Mode::NumericEnum,
            (Mode::Value, TRAILING_ADAPTER) if self.decoder.depth > 0 => Mode::Trailing,
            (Mode::Value, INLINE_ADAPTER | DERIVED_ADAPTER) if self.decoder.depth > 0 => {
                if name == DERIVED_ADAPTER {
                    self.decoder.next_scope()?;
                }
                Mode::Fields
            }
            (Mode::Value, STRINGS_ADAPTER) => Mode::Strings,
            (
                _,
                GUID_ADAPTER | ARRAY_ADAPTER | POINTER_ADAPTER | TRAILING_ADAPTER | INLINE_ADAPTER
                | DERIVED_ADAPTER | STRINGS_ADAPTER | NUMERIC_ENUM_ADAPTER,
            ) => return Err(decode_error()),
            _ => self.mode,
        };
        visitor.visit_newtype_struct(Self {
            decoder: self.decoder,
            mode,
            type_info: self.type_info,
        })
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.mode, Mode::Array | Mode::Strings) {
            return Err(decode_error());
        }
        let count = if self.mode == Mode::Array {
            self.decoder.object_array_count()?
        } else {
            let count = self.decoder.u32()? as usize;
            if count > MAX_COLLECTION_COUNT || count > self.decoder.limit - self.decoder.position {
                return Err(Error::Protocol("counted-string limit or extent exceeded"));
            }
            count
        };
        let mut access = Access {
            decoder: self.decoder,
            remaining: count,
            mode: if self.mode == Mode::Array {
                Mode::Object
            } else {
                Mode::StringElement
            },
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
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let numeric = self.mode == Mode::NumericEnum;
        let code = match self.mode {
            Mode::EnumContents => {
                let tag: [u8; 4] = self
                    .type_info
                    .ok_or_else(decode_error)?
                    .try_into()
                    .map_err(|_| Error::Protocol("native enum requires an i32 type tag"))?;
                let expected = i64::from(i32::from_le_bytes(tag));
                let kind = self.decoder.i64()?;
                if kind != expected {
                    return Err(Error::Protocol("native enum tag and kind disagree"));
                }
                kind
            }
            Mode::NumericEnum => self.decoder.i64()?,
            _ => return Err(decode_error()),
        };
        let variant = variant_name(code, variants, numeric)?;
        visitor.visit_enum(NativeEnumAccess {
            decoder: self.decoder,
            variant,
            numeric,
        })
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
impl<'de> Deserializer<'_, 'de> {
    fn structure<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        if !matches!(
            self.mode,
            Mode::Value | Mode::Fields | Mode::Object | Mode::Contents
        ) {
            return Err(decode_error());
        }
        if self.mode == Mode::Contents && self.type_info != Some(&[]) {
            return Err(Error::Protocol(
                "unexpected untyped pointer type information",
            ));
        }
        let read = |decoder: &mut Decoder<'de>| {
            visitor.visit_seq(Access {
                decoder,
                remaining: len,
                mode: Mode::Value,
            })
        };
        if matches!(self.mode, Mode::Fields | Mode::Contents) {
            read(self.decoder)
        } else {
            self.decoder.object(read)
        }
    }
}
struct Access<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    remaining: usize,
    mode: Mode,
}
impl<'de> SeqAccess<'de> for Access<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(Deserializer {
            decoder: self.decoder,
            mode: self.mode,
            type_info: None,
        })
        .map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        // Do not turn an advertised count or schema field count into a reservation.
        None
    }
}

struct NativeEnumAccess<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    variant: &'static str,
    numeric: bool,
}
impl<'a, 'de> EnumAccess<'de> for NativeEnumAccess<'a, 'de> {
    type Error = Error;
    type Variant = Self;
    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self)> {
        let variant = seed.deserialize(
            ::serde::de::value::BorrowedStrDeserializer::<Error>::new(self.variant),
        )?;
        Ok((variant, self))
    }
}
impl<'de> VariantAccess<'de> for NativeEnumAccess<'_, 'de> {
    type Error = Error;
    fn unit_variant(self) -> Result<()> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        if self.numeric {
            return Err(decode_error());
        }
        seed.deserialize(Deserializer {
            decoder: self.decoder,
            mode: Mode::Fields,
            type_info: None,
        })
    }
    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        if self.numeric {
            return Err(decode_error());
        }
        Deserializer {
            decoder: self.decoder,
            mode: Mode::Fields,
            type_info: None,
        }
        .structure(len, visitor)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.tuple_variant(fields.len(), visitor)
    }
}

struct TypeAccess<'a, 'de> {
    decoder: &'a mut Decoder<'de>,
    type_info: &'de [u8],
    remaining: usize,
}
impl<'de> SeqAccess<'de> for TypeAccess<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        match self.remaining {
            2 => {
                self.remaining -= 1;
                seed.deserialize(::serde::de::value::BorrowedBytesDeserializer::<Error>::new(
                    self.type_info,
                ))
                .map(Some)
            }
            1 => {
                self.remaining -= 1;
                seed.deserialize(Deserializer {
                    decoder: self.decoder,
                    mode: Mode::Fields,
                    type_info: None,
                })
                .map(Some)
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
