use mssf_transport::serialization::{
    serde::{from_slice, to_vec, CountedValues, Pointer},
    Encoder, MAX_COLLECTION_COUNT,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Item {
    value: i64,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Values {
    values: CountedValues<Vec<Pointer<Item>>>,
}

#[test]
fn counted_pointer_vectors_match_native_count_then_pointer_elements_without_slots() {
    let value = Values {
        values: CountedValues(vec![
            Pointer(Some(Item { value: 42 })),
            Pointer(None),
            Pointer(Some(Item { value: -1 })),
        ]),
    };
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32(3);
        e.pointer(Some(&[]), |e| {
            e.i64(42);
            Ok(())
        })?;
        e.pointer(None, |_| unreachable!())?;
        e.pointer(Some(&[]), |e| {
            e.i64(-1);
            Ok(())
        })?;
        Ok(())
    })
    .unwrap();
    let bytes = e.finish();
    assert_eq!(to_vec(&value).unwrap(), bytes);
    assert_eq!(from_slice::<Values>(&bytes).unwrap(), value);
    for length in 0..bytes.len() {
        assert!(from_slice::<Values>(&bytes[..length]).is_err());
    }
}

#[test]
fn counted_values_reject_oversize_counts_and_old_object_array_shape() {
    let mut e = Encoder::new();
    e.object(|e| {
        e.u32((MAX_COLLECTION_COUNT + 1) as u32);
        Ok(())
    })
    .unwrap();
    assert!(from_slice::<Values>(&e.finish()).is_err());
    let mut e = Encoder::new();
    e.object(|e| e.object_array::<i32>(&[], |_, _| unreachable!()))
        .unwrap();
    assert!(from_slice::<Values>(&e.finish()).is_err());
    let values = Values {
        values: CountedValues((0..=MAX_COLLECTION_COUNT).map(|_| Pointer(None)).collect()),
    };
    assert!(to_vec(&values).is_err());
}
