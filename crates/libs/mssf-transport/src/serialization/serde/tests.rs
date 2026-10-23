use super::*;
use crate::serialization::{MAX_COLLECTION_COUNT, MAX_DEPTH, MAX_STRING_UNITS};
use ::serde::ser::SerializeStruct;
use std::{cell::Cell, fmt::Write};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Row {
    text: String,
    number: i64,
    guid: Guid,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Sample {
    small: u16,
    signed: i32,
    unsigned: u32,
    large: u64,
    yes: bool,
    row: Row,
    bytes: ByteArray,
    rows: ObjectArray<Vec<Row>>,
}
fn row() -> Row {
    Row {
        text: "A🦀é".into(),
        number: i64::MIN,
        guid: Guid(std::array::from_fn(|i| (i + 1) as u8)),
    }
}
fn write_row(e: &mut Encoder, value: &Row) -> Result<()> {
    e.string(&value.text)?;
    e.i64(value.number);
    e.guid(&value.guid.0);
    Ok(())
}
#[test]
fn derived_bytes_match_independent_native_schema() {
    for empty in [false, true] {
        let value = Sample {
            small: u16::MAX,
            signed: i32::MIN,
            unsigned: u32::MAX,
            large: u64::MAX,
            yes: !empty,
            row: row(),
            bytes: ByteArray(if empty {
                vec![]
            } else {
                vec![0, 0x1f, 0x2f, 0x3f, 255]
            }),
            rows: ObjectArray(if empty { vec![] } else { vec![row()] }),
        };
        let mut old = Encoder::new();
        old.object(|e| {
            e.u16(value.small);
            e.i32(value.signed);
            e.u32(value.unsigned);
            e.u64(value.large);
            e.boolean(value.yes);
            e.object(|e| write_row(e, &value.row))?;
            e.byte_array(&value.bytes.0)?;
            e.object_array(&value.rows.0, write_row)
        })
        .unwrap();
        let bytes = to_vec(&value).unwrap();
        assert_eq!(bytes, old.finish());
        assert_eq!(from_slice::<Sample>(&bytes).unwrap(), value);
        for end in 0..bytes.len() {
            assert!(matches!(
                from_slice::<Sample>(&bytes[..end]),
                Err(Error::Protocol(_))
            ));
        }
        let mut extra = bytes;
        extra.push(0);
        assert!(from_slice::<Sample>(&extra).is_err());
    }
}

macro_rules! bitflags_case {
    ($test:ident, $bits:ty, $method:ident) => {
        #[test]
        fn $test() {
            bitflags::bitflags! {
                #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
                #[serde(transparent)]
                struct Flags: $bits {
                    const A = 1;
                    const B = 2;
                }
            }
            #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
            struct Payload {
                flags: Flags,
                tail: bool,
            }
            for bits in [0, 1, 3, 8, <$bits>::MIN, <$bits>::MAX] {
                let flags = Flags::from_bits_retain(bits);
                let bytes = to_vec(&flags).unwrap();
                let mut native = Encoder::new();
                native.$method(bits);
                assert_eq!(bytes, native.finish());
                assert_eq!(from_slice::<Flags>(&bytes).unwrap().bits(), bits);

                let payload = Payload { flags, tail: true };
                let bytes = to_vec(&payload).unwrap();
                let mut native = Encoder::new();
                native
                    .object(|e| {
                        e.$method(bits);
                        e.boolean(true);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(bytes, native.finish());
                assert_eq!(from_slice::<Payload>(&bytes).unwrap(), payload);
            }
        }
    };
}

bitflags_case!(bitflags_u16_preserves_native_bits, u16, u16);
bitflags_case!(bitflags_u32_preserves_native_bits, u32, u32);
bitflags_case!(bitflags_u64_preserves_native_bits, u64, u64);
bitflags_case!(bitflags_i32_preserves_native_bits, i32, i32);
bitflags_case!(bitflags_i64_preserves_native_bits, i64, i64);

#[test]
fn bitflags_unsupported_width_fails_explicitly() {
    bitflags::bitflags! {
        #[derive(Serialize, Deserialize)]
        struct Flags: u8 {
            const A = 1;
        }
    }
    assert!(matches!(to_vec(&Flags::A), Err(Error::InvalidInput(_))));
    assert!(matches!(from_slice::<Flags>(&[1]), Err(Error::Protocol(_))));
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Pair(u32, String);
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Newtype(Pair);
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Empty {}
#[test]
fn tuple_struct_newtype_and_explicit_empty_kinds() {
    let value = Newtype(Pair(128, String::new()));
    let mut old = Encoder::new();
    old.object(|e| {
        e.u32(128);
        e.string("")
    })
    .unwrap();
    assert_eq!(to_vec(&value).unwrap(), old.finish());
    assert_eq!(
        from_slice::<Newtype>(&to_vec(&value).unwrap()).unwrap(),
        value
    );
    assert_eq!(to_vec(&ByteArray(&[] as &[u8])).unwrap(), [0xc4]);
    assert_eq!(to_vec(&ObjectArray(Vec::<Empty>::new())).unwrap(), [0xc0]);
    assert_eq!(to_vec(&Guid([0; 16])).unwrap(), [0x4c]);
    assert_ne!(
        to_vec(&ByteArray([1; 16])).unwrap(),
        to_vec(&Guid([1; 16])).unwrap()
    );
    assert!(to_vec(&ObjectArray(vec![1u32])).is_err());
    let mut e = Encoder::new();
    e.object_array(&[()], |_, _| Ok(())).unwrap();
    assert!(from_slice::<ObjectArray<Vec<u32>>>(&e.finish()).is_err());
}

#[test]
fn field_bridges_preserve_pointer_tags_scopes_and_continuation() {
    let mut e = Encoder::new();
    assert!(to_fields(&mut e, &row()).is_err());
    e.object(|e| {
        e.pointer(None, |_| panic!("null callback"))?;
        e.pointer(Some(&1i32.to_le_bytes()), |e| {
            e.i64(1);
            to_fields(e, &row())?;
            e.next_scope()?;
            to_fields(e, &Pair(7, "derived".into()))
        })?;
        e.boolean(true);
        Ok(())
    })
    .unwrap();
    let bytes = e.finish();
    let mut d = Decoder::new(&bytes);
    assert!(from_fields::<Row>(&mut d).is_err());
    d.object(|d| {
        assert!(d.pointer::<()>(|_, _| panic!("null callback"))?.is_none());
        assert!(d
            .pointer(|d, tag| {
                assert_eq!(tag, 1i32.to_le_bytes());
                assert_eq!(d.i64()?, 1);
                assert_eq!(from_fields::<Row>(d)?, row());
                d.next_scope()?;
                assert_eq!(from_fields::<Pair>(d)?, Pair(7, "derived".into()));
                Ok(())
            })?
            .is_some());
        assert!(d.boolean()?);
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
}

#[derive(Serialize)]
struct Bad {
    good: u32,
    bad: f64,
}
#[test]
fn failed_fields_and_nested_objects_roll_back_prior_bytes_and_depth() {
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32(9);
        assert!(matches!(
            to_fields(e, &Bad { good: 1, bad: 3.0 }),
            Err(Error::InvalidInput(_))
        ));
        to_fields(e, &Pair(7, "after".into()))
    })
    .unwrap();
    let mut old = Encoder::new();
    old.object(|e| {
        e.u32(9);
        e.u32(7);
        e.string("after")
    })
    .unwrap();
    assert_eq!(e.finish(), old.finish());
    assert!(to_vec(&Bad { good: 1, bad: 0.0 }).is_err());
}

#[test]
fn generated_display_is_bounded_before_buffering_and_preserves_native_bytes() {
    struct Generated {
        text: &'static str,
        count: usize,
        writes: Cell<usize>,
        swallow: bool,
    }
    impl fmt::Display for Generated {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for _ in 0..self.count {
                self.writes.set(self.writes.get() + 1);
                let result = f.write_str(self.text);
                if !self.swallow {
                    result?;
                }
            }
            Ok(())
        }
    }
    impl Serialize for Generated {
        fn serialize<S: ::serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self)
        }
    }
    for (text, count) in [
        ("a", MAX_STRING_UNITS),
        ("é", MAX_STRING_UNITS),
        ("🦀", MAX_STRING_UNITS / 2),
    ] {
        let value = Generated {
            text,
            count,
            writes: Cell::new(0),
            swallow: false,
        };
        assert_eq!(
            to_vec(&value).unwrap(),
            to_vec(&text.repeat(count)).unwrap()
        );
        let over = Generated {
            count: count + 1,
            ..value
        };
        assert!(matches!(to_vec(&over), Err(Error::InvalidInput(_))));
        assert_eq!(over.writes.get(), count * 2 + 1); // valid call then early rejected call
    }
    let generated = Generated {
        text: "a",
        count: 1_000_000,
        writes: Cell::new(0),
        swallow: false,
    };
    assert!(to_vec(&generated).is_err());
    assert_eq!(generated.writes.get(), MAX_STRING_UNITS + 1);
    // An oversized single write must fail before copying or reserving a buffer.
    let mut bounded = BoundedText::default();
    assert!(bounded
        .write_str(&"a".repeat(MAX_STRING_UNITS * 3 + 1))
        .is_err());
    assert_eq!(bounded.text.capacity(), 0);
    assert!(bounded.text.is_empty());
    #[derive(Serialize)]
    struct Fields<'a> {
        first: u32,
        text: &'a Generated,
    }
    let swallowed = Generated {
        text: "a",
        count: MAX_STRING_UNITS + 1,
        writes: Cell::new(0),
        swallow: true,
    };
    assert!(to_vec(&swallowed).is_err());
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32(5);
        assert!(to_fields(
            e,
            &Fields {
                first: 1,
                text: &swallowed
            }
        )
        .is_err());
        to_fields(e, &Pair(2, "after".into()))
    })
    .unwrap();
    let mut old = Encoder::new();
    old.object(|e| {
        e.u32(5);
        e.u32(2);
        e.string("after")
    })
    .unwrap();
    assert_eq!(e.finish(), old.finish());
}

fn unsupported<T: Serialize + for<'de> Deserialize<'de>>(value: &T) {
    assert!(matches!(to_vec(value), Err(Error::InvalidInput(_))));
    assert!(matches!(from_slice::<T>(&[]), Err(Error::Protocol(_))));
}
#[derive(Serialize, Deserialize)]
enum Enum {
    Unit,
    Newtype(u32),
    Tuple(u32, u32),
    Struct { value: u32 },
}
#[derive(Serialize, Deserialize)]
struct Unit;
#[test]
fn unsupported_standard_serde_shapes_fail_in_both_directions() {
    unsupported(&1u8);
    unsupported(&1i8);
    unsupported(&1i16);
    unsupported(&1u128);
    unsupported(&1i128);
    unsupported(&1.0f32);
    unsupported(&1.0f64);
    unsupported(&'x');
    unsupported(&());
    unsupported(&Unit);
    unsupported(&None::<u32>);
    unsupported(&Some(1u32));
    unsupported(&vec![1u32]);
    unsupported(&Vec::<u32>::new());
    unsupported(&[1u32; 2]);
    unsupported(&(1u32, 2u32));
    unsupported(&std::collections::BTreeMap::<String, u32>::new());
    unsupported(&Enum::Unit);
    unsupported(&Enum::Newtype(1));
    unsupported(&Enum::Tuple(1, 2));
    unsupported(&Enum::Struct { value: 1 });
    struct Any;
    impl<'de> Deserialize<'de> for Any {
        fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Any;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("anything")
                }
            }
            d.deserialize_any(V)
        }
    }
    assert!(matches!(from_slice::<Any>(&[]), Err(Error::Protocol(_))));
    assert!(matches!(
        <Error as ::serde::ser::Error>::custom("custom"),
        Error::InvalidInput(_)
    ));
    assert!(matches!(
        <Error as ::serde::de::Error>::custom("custom"),
        Error::Protocol(_)
    ));
}

#[test]
fn exact_string_byte_array_object_array_and_type_info_limits() {
    let text = "🦀".repeat(MAX_STRING_UNITS / 2);
    assert_eq!(from_slice::<String>(&to_vec(&text).unwrap()).unwrap(), text);
    assert!(to_vec(&(text + "a")).is_err());
    let bytes = ByteArray(vec![1; MAX_COLLECTION_COUNT]);
    assert_eq!(
        from_slice::<ByteArray>(&to_vec(&bytes).unwrap()).unwrap(),
        bytes
    );
    assert!(to_vec(&ByteArray(vec![0; MAX_COLLECTION_COUNT + 1])).is_err());
    let objects = ObjectArray(
        (0..MAX_COLLECTION_COUNT)
            .map(|_| Empty {})
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        from_slice::<ObjectArray<Vec<Empty>>>(&to_vec(&objects).unwrap()).unwrap(),
        objects
    );
    assert!(to_vec(&ObjectArray(
        (0..=MAX_COLLECTION_COUNT)
            .map(|_| Empty {})
            .collect::<Vec<_>>()
    ))
    .is_err());
    let tag = vec![0x1f; MAX_COLLECTION_COUNT];
    let mut e = Encoder::new();
    e.typed_object(&tag, |e| to_fields(e, &row())).unwrap();
    let bytes = e.finish();
    let mut d = Decoder::new(&bytes);
    d.typed_object(|d, t| {
        assert_eq!(t, tag);
        assert_eq!(from_fields::<Row>(d)?, row());
        Ok(())
    })
    .unwrap();
    d.finish().unwrap();
    let mut e = Encoder::new();
    assert!(e
        .typed_object(&vec![0; MAX_COLLECTION_COUNT + 1], |e| to_fields(e, &row()))
        .is_err());
    assert!(e.finish().is_empty());
    // Advertised oversized or truncated counts are rejected before allocation.
    for data in [
        &[0x84, 0xa0, 0x01][..],
        &[0x80, 0xa0, 0x01],
        &[0x84, 0x01],
        &[0x80, 0x01],
    ] {
        assert!(from_slice::<ByteArray>(data).is_err());
        assert!(from_slice::<ObjectArray<Vec<Empty>>>(data).is_err());
    }
}

struct Nested(usize);
impl Serialize for Nested {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut s = s.serialize_struct("Nested", usize::from(self.0 > 1))?;
        if self.0 > 1 {
            s.serialize_field("child", &Nested(self.0 - 1))?;
        }
        s.end()
    }
}
struct ReadNested<const N: usize>;
impl<'de, const N: usize> Deserialize<'de> for ReadNested<N> {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V(usize);
        impl<'de> Visitor<'de> for V {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("nested objects")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<(), A::Error> {
                if self.0 > 1 {
                    a.next_element_seed(Seed(self.0 - 1))?
                        .ok_or_else(|| ::serde::de::Error::custom("missing child"))?;
                }
                Ok(())
            }
        }
        struct Seed(usize);
        impl<'de> DeserializeSeed<'de> for Seed {
            type Value = ();
            fn deserialize<D: ::serde::Deserializer<'de>>(
                self,
                d: D,
            ) -> std::result::Result<(), D::Error> {
                d.deserialize_tuple_struct("Nested", usize::from(self.0 > 1), V(self.0))
            }
        }
        d.deserialize_tuple_struct("Nested", usize::from(N > 1), V(N))?;
        Ok(Self)
    }
}
#[test]
fn depth_exact_and_over_bound_and_skipping() {
    let bytes = to_vec(&Nested(MAX_DEPTH)).unwrap();
    from_slice::<ReadNested<MAX_DEPTH>>(&bytes).unwrap();
    from_slice::<Empty>(&bytes).unwrap(); // bounded unknown-tail skip
    assert!(to_vec(&Nested(MAX_DEPTH + 1)).is_err());
    // Independently wrap the valid depth32 fixture in one extent-correct object.
    let mut too_deep = vec![0; 9];
    too_deep[1..5].copy_from_slice(&((bytes.len() + 11) as u32).to_le_bytes());
    too_deep.push(0x1f);
    too_deep.extend(bytes);
    too_deep.extend([0x2f, 0x3f]);
    assert!(from_slice::<ReadNested<33>>(&too_deep).is_err());
    assert!(from_slice::<Empty>(&too_deep).is_err());
}

struct Fill(usize);
impl Serialize for Fill {
    fn serialize<S: ::serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut s = s.serialize_struct("Fill", self.0)?;
        for _ in 0..self.0 {
            s.serialize_field("zero", &0u32)?;
        }
        s.end()
    }
}
#[test]
fn object_size_accounting_exact_and_over_bound() {
    let exact = to_vec(&Fill(MAX_OBJECT_SIZE - 11)).unwrap();
    assert_eq!(exact.len(), MAX_OBJECT_SIZE + 1);
    // Empty derived schema exercises validated typed skipping of all remaining fields.
    from_slice::<Empty>(&exact).unwrap();
    assert!(to_vec(&Fill(MAX_OBJECT_SIZE - 10)).is_err());
    let mut over = exact;
    over[1..5].copy_from_slice(&((MAX_OBJECT_SIZE + 1) as u32).to_le_bytes());
    assert!(from_slice::<Empty>(&over).is_err());
}

#[test]
fn root_array_has_per_object_not_aggregate_object_size_limit() {
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Large {
        text: String,
    }
    let rows = ObjectArray(
        (0..65)
            .map(|_| Large {
                text: "a".repeat(MAX_STRING_UNITS),
            })
            .collect::<Vec<_>>(),
    );
    let mut old = Encoder::new();
    old.object_array(&rows.0, |e, row| e.string(&row.text))
        .unwrap();
    let bytes = to_vec(&rows).unwrap();
    assert!(bytes.len() > MAX_OBJECT_SIZE);
    assert_eq!(bytes, old.finish());
    assert_eq!(from_slice::<ObjectArray<Vec<Large>>>(&bytes).unwrap(), rows);
    #[derive(Serialize)]
    struct Enclosing<'a> {
        rows: ObjectArray<&'a [Large]>,
    }
    assert!(to_vec(&Enclosing {
        rows: ObjectArray(&rows.0)
    })
    .is_err());
    let mut old = Encoder::new();
    assert!(old
        .object(|e| e.object_array(&rows.0, |e, row| e.string(&row.text)))
        .is_err());
    assert!(old.finish().is_empty());
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Suffix {
    first: u32,
    suffix: String,
}
#[test]
fn missing_truncated_wrong_metadata_and_supported_unknown_tails() {
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32(1);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<Suffix>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32(1);
        e.boolean(true);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<Suffix>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.object(|e| {
        to_fields(e, &Pair(1, "tail".into()))?;
        e.next_scope()?;
        e.byte_array(&[0x1f, 0x2f, 0x3f])
    })
    .unwrap();
    assert_eq!(
        from_slice::<Pair>(&e.finish()).unwrap(),
        Pair(1, "tail".into())
    );
    for data in [
        &[0x8d, 0x01, 0x00, 0xd8][..],
        &[0x08, 0xff, 0xff, 0xff, 0xff, 0x7f],
        &[0x09, 0x80],
        &[0x84, 0x02, 0x01],
    ] {
        assert!(from_slice::<String>(data).is_err());
        assert!(from_slice::<u32>(data).is_err());
        assert!(from_slice::<ByteArray>(data).is_err());
    }
    assert_eq!(from_slice::<i32>(&[0x07, 0xff, 0x7f]).unwrap(), -1);
    assert_eq!(from_slice::<u32>(&[0x08, 0x80, 0x00]).unwrap(), 0);
    assert_eq!(from_slice::<String>(&[0x8d, 0x00]).unwrap(), "");
    assert_eq!(
        from_slice::<ByteArray>(&[0x84, 0x00]).unwrap(),
        ByteArray(vec![])
    );
}
