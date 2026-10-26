use super::*;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PartitionFields {
    id: Guid,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Partition {
    fields: Derived<PartitionFields>,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Poly {
    #[serde(rename = "17")]
    Event { text: String, value: u32 },
    #[serde(rename = "-2")]
    None,
    #[serde(rename = "7")]
    Partition(Partition),
    #[serde(rename = "5")]
    Tuple(u32, String),
    #[serde(rename = "-2147483648")]
    Min,
    #[serde(rename = "2147483647")]
    Max,
    #[serde(rename = "0")]
    Zero,
}
fn cases() -> Vec<Poly> {
    vec![
        Poly::Event {
            text: "event".into(),
            value: 99,
        },
        Poly::None,
        Poly::Partition(Partition {
            fields: Derived(PartitionFields { id: Guid([1; 16]) }),
        }),
        Poly::Tuple(3, "tuple".into()),
        Poly::Min,
        Poly::Max,
        Poly::Zero,
    ]
}
fn code(value: &Poly) -> i32 {
    match value {
        Poly::Event { .. } => 17,
        Poly::None => -2,
        Poly::Partition(_) => 7,
        Poly::Tuple(..) => 5,
        Poly::Min => i32::MIN,
        Poly::Max => i32::MAX,
        Poly::Zero => 0,
    }
}
fn write_poly(e: &mut Encoder, value: &Poly) -> Result<()> {
    e.i64(i64::from(code(value)));
    match value {
        Poly::Event { text, value } => {
            e.string(text)?;
            e.u32(*value);
        }
        Poly::None | Poly::Min | Poly::Max | Poly::Zero => {}
        Poly::Partition(value) => {
            e.next_scope()?;
            e.guid(&value.fields.0.id.0);
        }
        Poly::Tuple(value, text) => {
            e.u32(*value);
            e.string(text)?;
        }
    }
    Ok(())
}

#[test]
fn derived_enum_shapes_match_manual_tags_kinds_scopes_and_pointer_framing() {
    for value in cases() {
        let mut e = Encoder::new();
        e.typed_object(&code(&value).to_le_bytes(), |e| write_poly(e, &value))
            .unwrap();
        let value = NativeEnum(value);
        let bytes = to_vec(&value).unwrap();
        assert_eq!(bytes, e.finish());
        assert_eq!(from_slice::<NativeEnum<Poly>>(&bytes).unwrap(), value);
        for end in 0..bytes.len() {
            assert!(from_slice::<NativeEnum<Poly>>(&bytes[..end]).is_err());
        }
        let mut e = Encoder::new();
        e.pointer(Some(&code(&value.0).to_le_bytes()), |e| {
            write_poly(e, &value.0)
        })
        .unwrap();
        let value = Pointer(Some(value));
        let bytes = to_vec(&value).unwrap();
        assert_eq!(bytes, e.finish());
        assert_eq!(
            from_slice::<Pointer<NativeEnum<Poly>>>(&bytes).unwrap(),
            value
        );
    }
    let values = cases().into_iter().map(NativeEnum).collect::<Vec<_>>();
    let mut e = Encoder::new();
    e.object_array_header(values.len()).unwrap();
    for value in &values {
        e.typed_object(&code(&value.0).to_le_bytes(), |e| write_poly(e, &value.0))
            .unwrap();
    }
    let values = ObjectArray(values);
    assert_eq!(to_vec(&values).unwrap(), e.finish());
    assert_eq!(
        from_slice::<ObjectArray<Vec<NativeEnum<Poly>>>>(&to_vec(&values).unwrap()).unwrap(),
        values
    );
}

#[test]
fn fixed_and_empty_tags_automatically_deserialize_fields_in_every_object_context() {
    let value = FixedTag::<_, 17>(Pair(9, "fixed".into()));
    let mut e = Encoder::new();
    e.typed_object(&17i32.to_le_bytes(), |e| {
        e.u32(9);
        e.string("fixed")
    })
    .unwrap();
    assert_eq!(to_vec(&value).unwrap(), e.finish());
    assert_eq!(
        from_slice::<FixedTag<Pair, 17>>(&to_vec(&value).unwrap()).unwrap(),
        value
    );
    assert!(from_slice::<FixedTag<Pair, 16>>(&to_vec(&value).unwrap()).is_err());
    assert!(from_slice::<Untagged<Pair>>(&to_vec(&value).unwrap()).is_err());
    let untagged = Untagged(Pair(9, "plain".into()));
    assert_eq!(to_vec(&untagged).unwrap(), to_vec(&untagged.0).unwrap());
    assert_eq!(
        from_slice::<Untagged<Pair>>(&to_vec(&untagged).unwrap()).unwrap(),
        untagged
    );
    assert!(from_slice::<FixedTag<Pair, 0>>(&to_vec(&untagged).unwrap()).is_err());
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Container {
        plain: Untagged<Pair>,
        tagged: FixedTag<Pair, 17>,
        pointer: Pointer<FixedTag<Pair, 17>>,
        empty: Pointer<NativeEnum<Poly>>,
        rows: ObjectArray<Vec<FixedTag<Pair, 17>>>,
    }
    let container = Container {
        plain: untagged,
        tagged: value.clone(),
        pointer: Pointer(Some(value.clone())),
        empty: Pointer(None),
        rows: ObjectArray(vec![value]),
    };
    let bytes = to_vec(&container).unwrap();
    assert_eq!(from_slice::<Container>(&bytes).unwrap(), container);
}

#[test]
fn native_enum_rejects_unknown_mismatching_missing_and_malformed_discriminants() {
    for (tag, kind) in [
        (vec![], Some(17)),
        (vec![17], Some(17)),
        (vec![17; 5], Some(17)),
        (88i32.to_le_bytes().to_vec(), Some(88)),
        (17i32.to_le_bytes().to_vec(), Some(7)),
        (17i32.to_le_bytes().to_vec(), Some(i64::MAX)),
        (17i32.to_le_bytes().to_vec(), None),
    ] {
        let mut e = Encoder::new();
        e.typed_object(&tag, |e| {
            if let Some(kind) = kind {
                e.i64(kind);
            }
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            from_slice::<NativeEnum<Poly>>(&e.finish()),
            Err(Error::Protocol(_))
        ));
    }
    let mut e = Encoder::new();
    e.typed_object(&17i32.to_le_bytes(), |e| {
        e.u32(17);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<NativeEnum<Poly>>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.typed_object(&7i32.to_le_bytes(), |e| {
        e.i64(7);
        e.guid(&[1; 16]);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<NativeEnum<Poly>>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.typed_object(&7i32.to_le_bytes(), |e| {
        e.i64(7);
        e.boolean(true);
        e.next_scope()?;
        e.guid(&[1; 16]);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        from_slice::<NativeEnum<Poly>>(&e.finish()).unwrap(),
        NativeEnum(Poly::Partition(Partition {
            fields: Derived(PartitionFields { id: Guid([1; 16]) })
        }))
    );
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Scalar {
    #[serde(rename = "65535")]
    All,
    #[serde(rename = "-9223372036854775808")]
    Min,
    #[serde(rename = "9223372036854775807")]
    Max,
    #[serde(rename = "0")]
    Zero,
}
#[test]
fn numeric_enum_uses_explicit_i64_codes_not_variant_ordinals() {
    for (value, code) in [
        (Scalar::All, 65535),
        (Scalar::Min, i64::MIN),
        (Scalar::Max, i64::MAX),
        (Scalar::Zero, 0),
    ] {
        let mut e = Encoder::new();
        e.i64(code);
        let bytes = e.finish();
        let value = NumericEnum(value);
        assert_eq!(to_vec(&value).unwrap(), bytes);
        assert_eq!(from_slice::<NumericEnum<Scalar>>(&bytes).unwrap(), value);
        assert!(to_vec(&value.0).is_err());
        assert!(from_slice::<Scalar>(&bytes).is_err());
    }
    let mut e = Encoder::new();
    e.i64(1);
    assert!(from_slice::<NumericEnum<Scalar>>(&e.finish()).is_err());
    assert!(to_vec(&NativeEnum(Scalar::Max)).is_err());
    let mut e = Encoder::new();
    e.u32(65535);
    assert!(from_slice::<NumericEnum<Scalar>>(&e.finish()).is_err());
}

#[test]
fn enum_adapters_reject_implicit_codes_and_do_not_accept_unknown_fallbacks() {
    #[derive(Serialize, Deserialize)]
    enum Implicit {
        First,
        Second,
    }
    #[derive(Serialize, Deserialize)]
    enum Noncanonical {
        #[serde(rename = "01")]
        One,
    }
    #[derive(Serialize, Deserialize)]
    enum Fallback {
        #[serde(rename = "17")]
        Known,
        #[serde(other, rename = "-1")]
        Other,
    }
    assert!(to_vec(&NativeEnum(Implicit::First)).is_err());
    assert!(to_vec(&NumericEnum(Implicit::Second)).is_err());
    assert!(to_vec(&NativeEnum(Noncanonical::One)).is_err());
    assert!(to_vec(&NumericEnum(Noncanonical::One)).is_err());
    let mut e = Encoder::new();
    e.typed_object(&88i32.to_le_bytes(), |e| {
        e.i64(88);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<NativeEnum<Fallback>>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.i64(88);
    assert!(from_slice::<NumericEnum<Fallback>>(&e.finish()).is_err());
    assert!(variant_name(1, &["1", "1"], false).is_err());
    let mut e = Encoder::new();
    e.i64(1);
    assert!(from_slice::<NumericEnum<Noncanonical>>(&e.finish()).is_err());
    assert!(to_vec(&NumericEnum(Poly::Tuple(1, "payload".into()))).is_err());
}

#[test]
fn native_enum_encoding_rolls_back_nested_failures() {
    #[derive(Serialize)]
    struct Fields {
        value: Pointer<NativeEnum<Poly>>,
    }
    let mut e = Encoder::new();
    e.object(|e| {
        let invalid = Poly::Event {
            text: "x".repeat(MAX_STRING_UNITS + 1),
            value: 0,
        };
        assert!(to_fields(
            e,
            &Fields {
                value: Pointer(Some(NativeEnum(invalid)))
            }
        )
        .is_err());
        to_fields(e, &Pair(2, "after".into()))
    })
    .unwrap();
    assert_eq!(
        from_slice::<Pair>(&e.finish()).unwrap(),
        Pair(2, "after".into())
    );
}

#[test]
fn native_enum_preserves_exact_object_and_nesting_bounds() {
    #[derive(Serialize)]
    enum Large {
        #[serde(rename = "1")]
        Fill(Fill),
    }
    #[derive(Deserialize)]
    enum ReadLarge {
        #[serde(rename = "1")]
        Fill(Empty),
    }
    let bytes = to_vec(&Pointer(Some(NativeEnum(Large::Fill(Fill(
        MAX_OBJECT_SIZE - 18,
    ))))))
    .unwrap();
    assert_eq!(bytes.len(), MAX_OBJECT_SIZE + 2);
    from_slice::<Pointer<NativeEnum<ReadLarge>>>(&bytes).unwrap();
    assert!(to_vec(&Pointer(Some(NativeEnum(Large::Fill(Fill(
        MAX_OBJECT_SIZE - 17
    ))))))
    .is_err());
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Recursive {
        #[serde(rename = "1")]
        Next {
            next: Pointer<Box<NativeEnum<Recursive>>>,
        },
    }
    let mut value = NativeEnum(Recursive::Next {
        next: Pointer(None),
    });
    for _ in 1..MAX_DEPTH {
        value = NativeEnum(Recursive::Next {
            next: Pointer(Some(Box::new(value))),
        });
    }
    let bytes = to_vec(&value).unwrap();
    assert_eq!(from_slice::<NativeEnum<Recursive>>(&bytes).unwrap(), value);
    assert!(to_vec(&NativeEnum(Recursive::Next {
        next: Pointer(Some(Box::new(value)))
    }))
    .is_err());
}
