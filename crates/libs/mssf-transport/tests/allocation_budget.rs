use std::{cell::Cell, mem::size_of};

use mssf_transport::{
    serialization::{
        serde::{
            from_slice, from_slice_with_budget, to_vec, ByteArray, CountedStrings, CountedValues,
            Derived, Guid, Inline, NativeEnum, NumericEnum, ObjectArray, Pointer, RawTagged,
            Trailing,
        },
        Encoder, MAX_COLLECTION_COUNT,
    },
    Error, Result,
};
use serde::{Deserialize, Serialize};

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8], limit: usize) -> Result<(T, usize)> {
    let mut used = 0;
    let value = from_slice_with_budget(bytes, &mut |bytes| {
        if bytes > limit.saturating_sub(used) {
            return Err(Error::ResourceLimit("test allocation budget"));
        }
        used += bytes;
        Ok(())
    })?;
    Ok((value, used))
}

#[test]
fn strings_reserve_exact_utf8_capacity_including_non_ascii_expansion() {
    for text in ["", "ascii", "🦀é", "中文", &"中".repeat(1000)] {
        let mut encoder = Encoder::new();
        encoder.string(text).unwrap();
        let bytes = encoder.finish();
        let (value, used) = decode::<String>(&bytes, text.len()).unwrap();
        assert_eq!(value, text);
        assert_eq!(value.capacity(), used);
        assert_eq!(used, text.len());
        assert_eq!(from_slice::<String>(&bytes).unwrap(), value);
        if !text.is_empty() {
            assert!(matches!(
                decode::<String>(&bytes, text.len() - 1),
                Err(Error::ResourceLimit(_))
            ));
        }
    }
}

#[test]
fn bytes_guids_and_raw_pointer_tags_are_charged() {
    for value in [vec![], vec![1; 16], vec![0; 4096]] {
        let bytes = to_vec(&ByteArray(&value)).unwrap();
        let (decoded, used) = decode::<ByteArray>(&bytes, value.len()).unwrap();
        assert_eq!(decoded.0, value);
        assert_eq!(decoded.0.capacity(), used);
        if !value.is_empty() {
            assert!(decode::<ByteArray>(&bytes, value.len() - 1).is_err());
        }
    }
    for guid in [Guid([0; 16]), Guid([1; 16])] {
        let bytes = to_vec(&guid).unwrap();
        assert_eq!(decode::<Guid>(&bytes, 16).unwrap(), (guid, 16));
        assert!(decode::<Guid>(&bytes, 15).is_err());
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Text {
        value: String,
    }
    let tagged = Pointer(Some(RawTagged {
        type_info: vec![7; 4],
        value: Text {
            value: "中".into()
        },
    }));
    let bytes = to_vec(&tagged).unwrap();
    assert_eq!(
        decode::<Pointer<RawTagged<Text>>>(&bytes, 7).unwrap(),
        (tagged, 7)
    );
    assert!(decode::<Pointer<RawTagged<Text>>>(&bytes, 6).is_err());
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Dense {
    a: u64,
    b: u64,
    c: u64,
    d: u64,
    e: u64,
    f: u64,
    g: u64,
    h: u64,
}

#[test]
fn sequence_storage_uses_rust_element_sizes_not_encoded_lengths() {
    for count in [0, 1, 4, 5, 17, MAX_COLLECTION_COUNT] {
        let values = ObjectArray(vec![
            Dense {
                a: 0,
                b: 0,
                c: 0,
                d: 0,
                e: 0,
                f: 0,
                g: 0,
                h: 0,
            };
            count
        ]);
        let bytes = to_vec(&values).unwrap();
        let (decoded, used) = decode::<ObjectArray<Vec<Dense>>>(&bytes, usize::MAX).unwrap();
        assert_eq!(decoded, values);
        assert!(used >= decoded.0.capacity() * size_of::<Dense>());
        if count != 0 {
            assert!(used > bytes.len());
            assert!(decode::<ObjectArray<Vec<Dense>>>(&bytes, used - 1).is_err());
            assert_eq!(
                decode::<ObjectArray<Vec<Dense>>>(&bytes, used).unwrap().1,
                used
            );
        }
    }
    let values = CountedStrings::<Vec<String>>(vec!["".into(), "中".into(), "🦀".into()]);
    let bytes = to_vec(&values).unwrap();
    let (decoded, used) = decode::<CountedStrings<Vec<String>>>(&bytes, usize::MAX).unwrap();
    assert_eq!(decoded.0, values.0);
    assert!(
        used >= decoded.0.capacity() * size_of::<String>()
            + decoded.0.iter().map(String::capacity).sum::<usize>()
    );
}

thread_local! {
    static ELEMENTS_DECODED: Cell<usize> = const { Cell::new(0) };
}

#[derive(Serialize)]
struct Probe(u64);
impl<'de> Deserialize<'de> for Probe {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        ELEMENTS_DECODED.with(|count| count.set(count.get() + 1));
        u64::deserialize(deserializer).map(Self)
    }
}

#[test]
fn rejected_sequence_storage_never_decodes_the_element() {
    let bytes = to_vec(&CountedValues(vec![Probe(0)])).unwrap();
    ELEMENTS_DECODED.with(|count| count.set(0));
    assert!(decode::<CountedValues<Vec<Probe>>>(&bytes, 0).is_err());
    assert_eq!(ELEMENTS_DECODED.with(Cell::get), 0);
    let (decoded, _) = decode::<CountedValues<Vec<Probe>>>(&bytes, usize::MAX).unwrap();
    assert_eq!(decoded.0[0].0, 0);
    assert_eq!(ELEMENTS_DECODED.with(Cell::get), 1);

    let mut encoder = Encoder::new();
    encoder.u32((MAX_COLLECTION_COUNT + 1) as u32);
    assert!(matches!(
        decode::<CountedValues<Vec<Probe>>>(&encoder.finish(), 0),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn shared_budget_reaches_inline_derived_trailing_and_enum_fields() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Base {
        text: String,
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Choice {
        #[serde(rename = "1")]
        Text { value: String },
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Number {
        #[serde(rename = "-1")]
        Negative,
        #[serde(rename = "0")]
        Zero,
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        base: Inline<Base>,
        choice: Pointer<NativeEnum<Choice>>,
        number: NumericEnum<Number>,
        suffix: Derived<Base>,
        tail: Trailing<String>,
    }
    let payload = Payload {
        base: Inline(Base { text: "a".into() }),
        choice: Pointer(Some(NativeEnum(Choice::Text { value: "b".into() }))),
        number: NumericEnum(Number::Negative),
        suffix: Derived(Base { text: "c".into() }),
        tail: Trailing(Some("d".into())),
    };
    let bytes = to_vec(&payload).unwrap();
    assert_eq!(decode::<Payload>(&bytes, 4).unwrap(), (payload, 4));
    assert!(decode::<Payload>(&bytes, 3).is_err());
    let bytes = to_vec(&NumericEnum(Number::Zero)).unwrap();
    assert_eq!(
        decode::<NumericEnum<Number>>(&bytes, 0).unwrap(),
        (NumericEnum(Number::Zero), 0)
    );
}

#[test]
fn skipped_tails_allocate_nothing_and_malformed_utf16_remains_an_error() {
    #[derive(Debug, PartialEq, Deserialize)]
    struct Known {
        value: u32,
    }
    let mut encoder = Encoder::new();
    encoder
        .object(|encoder| {
            encoder.u32(7);
            encoder.string(&"中".repeat(1000))?;
            encoder.byte_array(&[1; 16])
        })
        .unwrap();
    assert_eq!(
        decode::<Known>(&encoder.finish(), 0).unwrap(),
        (Known { value: 7 }, 0)
    );

    let mut encoder = Encoder::new();
    encoder.string("a").unwrap();
    let mut bytes = encoder.finish();
    let length = bytes.len();
    bytes[length - 2..].copy_from_slice(&0xd800u16.to_le_bytes());
    assert!(matches!(
        decode::<String>(&bytes, 0),
        Err(Error::Protocol(_))
    ));
}
