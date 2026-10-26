use super::*;
use ::serde::{de::DeserializeSeed, ser::SerializeTupleStruct};
use std::marker::PhantomData;

/// Serde field adapter for `[u8; 16]` GUIDs in native Windows byte order.
///
/// Use `#[serde(with = "mssf_transport::serialization::serde::guid")]`.
/// Native encoding uses [`Guid`] metadata; human-readable formats use arrays.
pub mod guid {
    use super::Guid;
    use ::serde::{Deserialize, Serialize};

    pub fn serialize<S: ::serde::Serializer>(
        value: &[u8; 16],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            Guid(*value).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D: ::serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<[u8; 16], D::Error> {
        if deserializer.is_human_readable() {
            <[u8; 16]>::deserialize(deserializer)
        } else {
            Guid::deserialize(deserializer).map(|value| value.0)
        }
    }
}

macro_rules! field_adapter {
    ($name:ident, $marker:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name<T>(pub T);

        impl<T: Serialize> Serialize for $name<T> {
            fn serialize<S: ::serde::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                s.serialize_newtype_struct($marker, &self.0)
            }
        }
        impl<'de, T: Deserialize<'de>> Deserialize<'de> for $name<T> {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                struct V<T>(PhantomData<T>);
                impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
                    type Value = $name<T>;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(stringify!($name))
                    }
                    fn visit_newtype_struct<D: ::serde::Deserializer<'de>>(
                        self,
                        d: D,
                    ) -> std::result::Result<Self::Value, D::Error> {
                        T::deserialize(d).map($name)
                    }
                }
                d.deserialize_newtype_struct($marker, V(PhantomData))
            }
        }
    };
}

field_adapter!(
    Inline,
    INLINE_ADAPTER,
    "Ordered struct fields in the current scope, without an additional object."
);
field_adapter!(
    Derived,
    DERIVED_ADAPTER,
    "Enter the next native base/derived scope, then read/write ordered struct fields."
);
field_adapter!(
    CountedStrings,
    STRINGS_ADAPTER,
    "A bounded native UInt32 count followed by strings, not an object array. Encode a Vec/slice; decode into `CountedStrings<Vec<String>>`."
);
field_adapter!(
    CountedValues,
    VALUES_ADAPTER,
    "A bounded native UInt32 count followed by values. In particular, C++ vectors of shared pointers use `CountedValues<Vec<Pointer<T>>>`, not object-array slots."
);
field_adapter!(
    NativeEnum,
    ENUM_ADAPTER,
    "A tagged native enum object. Derive Serde on the enum and rename every variant to its canonical i32 code. The adapter writes/validates the i32 type tag and matching i64 kind, then reads/writes variant fields inline."
);
field_adapter!(
    NumericEnum,
    NUMERIC_ENUM_ADAPTER,
    "A strict native i64 scalar enum. Use unit variants renamed to canonical signed-i64 codes. Unknown codes fail; declaration ordinals are never used. For unknown-value retention, use an explicit i64 conversion instead."
);

/// A struct-shaped native object with an automatically checked i32 type tag.
///
/// No `NativeType` implementation is needed. The tag is not a body field.
///
/// ```
/// use mssf_transport::serialization::serde::{from_slice, to_vec, FixedTag};
/// use serde::{Deserialize, Serialize};
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct Fields { value: String }
/// let value = FixedTag::<_, 16>(Fields { value: "item".into() });
/// let bytes = to_vec(&value).unwrap();
/// assert_eq!(from_slice::<FixedTag<Fields, 16>>(&bytes).unwrap(), value);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedTag<T, const TAG: i32>(pub T);

/// A struct-shaped native object that requires empty native type information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Untagged<T>(pub T);

fn deserialize_fixed<'de, D: ::serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
    expected: &[u8],
) -> std::result::Result<T, D::Error> {
    struct V<'a, T> {
        expected: &'a [u8],
        marker: PhantomData<T>,
    }
    impl<'de, T: Deserialize<'de>> Visitor<'de> for V<'_, T> {
        type Value = T;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("fixed native type information and fields")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<T, A::Error> {
            let ByteArray(tag) = a
                .next_element()?
                .ok_or_else(|| ::serde::de::Error::custom("missing native tag"))?;
            if tag != self.expected {
                return Err(::serde::de::Error::custom(
                    "unexpected native type information",
                ));
            }
            a.next_element()?
                .ok_or_else(|| ::serde::de::Error::custom("missing native fields"))
        }
    }
    d.deserialize_newtype_struct(
        TYPED_ADAPTER,
        V {
            expected,
            marker: PhantomData,
        },
    )
}
impl<T: Serialize, const TAG: i32> Serialize for FixedTag<T, TAG> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_newtype_struct(
            TYPED_ADAPTER,
            &TypeFields {
                type_info: &TAG.to_le_bytes(),
                value: &self.0,
            },
        )
    }
}
impl<'de, T: Deserialize<'de>, const TAG: i32> Deserialize<'de> for FixedTag<T, TAG> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        deserialize_fixed(d, &TAG.to_le_bytes()).map(Self)
    }
}
impl<T: Serialize> Serialize for Untagged<T> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_newtype_struct(
            TYPED_ADAPTER,
            &TypeFields {
                type_info: &[],
                value: &self.0,
            },
        )
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Untagged<T> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        deserialize_fixed(d, &[]).map(Self)
    }
}

/// A struct-shaped native object with raw type information retained separately.
///
/// Unlike [`FixedTag`] or [`Untagged`], this accepts any bounded type information.
/// The caller must validate it according to the operation's compatibility policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTagged<T> {
    pub type_info: Vec<u8>,
    pub value: T,
}

impl<T: Serialize> Serialize for RawTagged<T> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_newtype_struct(
            TYPED_ADAPTER,
            &TypeFields {
                type_info: &self.type_info,
                value: &self.value,
            },
        )
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for RawTagged<T> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
            type Value = RawTagged<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("raw native type information and fields")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let ByteArray(type_info) = a
                    .next_element()?
                    .ok_or_else(|| ::serde::de::Error::custom("missing native type information"))?;
                let value = a
                    .next_element()?
                    .ok_or_else(|| ::serde::de::Error::custom("missing native object fields"))?;
                Ok(RawTagged { type_info, value })
            }
        }
        d.deserialize_newtype_struct(TYPED_ADAPTER, V(PhantomData))
    }
}

/// A required native object-array field whose count must be zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmptyObjectArray;

#[derive(Serialize)]
struct ForbiddenObject {}

impl<'de> Deserialize<'de> for ForbiddenObject {
    fn deserialize<D: ::serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(::serde::de::Error::custom(
            "expected an empty native object array",
        ))
    }
}

impl Serialize for EmptyObjectArray {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        ObjectArray(&[] as &[ForbiddenObject]).serialize(s)
    }
}

impl<'de> Deserialize<'de> for EmptyObjectArray {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        ObjectArray::<Vec<ForbiddenObject>>::deserialize(d).map(|_| Self)
    }
}

/// A nullable native object pointer. This never represents an absent field.
///
/// Plain struct targets require empty type information. Use `Pointer<Typed<T>>`
/// or the `FixedTag`, `Untagged`, `RawTagged` and `NativeEnum` adapters for tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pointer<T>(pub Option<T>);

/// An optional positional suffix, absent only at the current scope's real end.
///
/// Use only for trailing fields. `None` writes no bytes, unlike `Pointer(None)`.
/// After an absent suffix, all following fields in that scope must be absent.
/// Malformed/truncated present values are errors, not absence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trailing<T>(pub Option<T>);

macro_rules! optional_adapter {
    ($name:ident, $marker:ident) => {
        impl<T: Serialize> Serialize for $name<T> {
            fn serialize<S: ::serde::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                s.serialize_newtype_struct($marker, &self.0)
            }
        }
        impl<'de, T: Deserialize<'de>> Deserialize<'de> for $name<T> {
            fn deserialize<D: ::serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                struct V<T>(PhantomData<T>);
                impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
                    type Value = $name<T>;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(stringify!($name))
                    }
                    fn visit_newtype_struct<D: ::serde::Deserializer<'de>>(
                        self,
                        d: D,
                    ) -> std::result::Result<Self::Value, D::Error> {
                        Option::<T>::deserialize(d).map($name)
                    }
                }
                d.deserialize_newtype_struct($marker, V(PhantomData))
            }
        }
    };
}
optional_adapter!(Pointer, POINTER_ADAPTER);
optional_adapter!(Trailing, TRAILING_ADAPTER);

/// Native tagged-object dispatch, independent of Serde enum variant ordinals.
///
/// This is an escape hatch for exceptional layouts/raw tag policies. Prefer
/// [`FixedTag`], [`Untagged`] or [`NativeEnum`] for automatically derived schemas;
/// use [`RawTagged`] when only raw type-information retention is needed.
///
/// `deserialize_fields` receives the bounded raw tag before reading any fields.
/// Select the supported schema, deserialize its fields with `D`, and enforce
/// tag/kind agreement. Reject unsupported structural tags explicitly; formats
/// with deliberately permissive tag policies may retain the tag for validation.
/// `type_info` returns an encoding error for values without a valid native tag.
///
/// ```
/// use mssf_transport::serialization::serde::{
///     from_slice, to_vec, NativeType, Pointer, Typed,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Serialize, Deserialize)]
/// struct Event { kind: i64, text: String }
///
/// impl NativeType for Event {
///     fn type_info(&self) -> mssf_transport::Result<&[u8]> {
///         if self.kind != 1 {
///             return Err(mssf_transport::Error::InvalidInput("invalid event kind"));
///         }
///         Ok(&[1, 0, 0, 0])
///     }
///     fn deserialize_fields<'de, D: serde::Deserializer<'de>>(
///         tag: &[u8], d: D,
///     ) -> Result<Self, D::Error> {
///         if tag != 1i32.to_le_bytes() {
///             return Err(serde::de::Error::custom("unsupported event tag"));
///         }
///         let value = Self::deserialize(d)?;
///         if value.kind != 1 {
///             return Err(serde::de::Error::custom("event tag/kind mismatch"));
///         }
///         Ok(value)
///     }
/// }
///
/// let value = Pointer(Some(Typed(Event { kind: 1, text: "event".into() })));
/// let bytes = to_vec(&value).unwrap();
/// assert_eq!(from_slice::<Pointer<Typed<Event>>>(&bytes).unwrap(), value);
/// ```
pub trait NativeType: Sized {
    fn type_info(&self) -> Result<&[u8]>;

    fn deserialize_fields<'de, D: ::serde::Deserializer<'de>>(
        type_info: &[u8],
        deserializer: D,
    ) -> std::result::Result<Self, D::Error>;
}

/// A native object whose type information is governed by [`NativeType`].
///
/// Works as a root, nested object, object-array element or pointer target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Typed<T>(pub T);

struct TypeFields<'a, T: ?Sized> {
    type_info: &'a [u8],
    value: &'a T,
}
struct TypeBytes<'a>(&'a [u8]);
impl Serialize for TypeBytes<'_> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(self.0)
    }
}
impl<T: Serialize + ?Sized> Serialize for TypeFields<'_, T> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut fields = s.serialize_tuple_struct(TYPED_ADAPTER, 2)?;
        fields.serialize_field(&TypeBytes(self.type_info))?;
        fields.serialize_field(self.value)?;
        fields.end()
    }
}
impl<T: NativeType + Serialize> Serialize for Typed<T> {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let type_info = self.0.type_info().map_err(::serde::ser::Error::custom)?;
        s.serialize_newtype_struct(
            TYPED_ADAPTER,
            &TypeFields {
                type_info,
                value: &self.0,
            },
        )
    }
}

struct TypeSeed<T> {
    type_info: Vec<u8>,
    marker: PhantomData<T>,
}
impl<'de, T: NativeType> DeserializeSeed<'de> for TypeSeed<T> {
    type Value = T;
    fn deserialize<D: ::serde::Deserializer<'de>>(self, d: D) -> std::result::Result<T, D::Error> {
        T::deserialize_fields(&self.type_info, d)
    }
}
impl<'de, T: NativeType> Deserialize<'de> for Typed<T> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T: NativeType> Visitor<'de> for V<T> {
            type Value = Typed<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("native type information and fields")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let ByteArray(type_info) = a
                    .next_element()?
                    .ok_or_else(|| ::serde::de::Error::custom("missing native type information"))?;
                let value = a
                    .next_element_seed(TypeSeed {
                        type_info,
                        marker: PhantomData,
                    })?
                    .ok_or_else(|| ::serde::de::Error::custom("missing native object fields"))?;
                Ok(Typed(value))
            }
        }
        d.deserialize_newtype_struct(TYPED_ADAPTER, V(PhantomData))
    }
}
