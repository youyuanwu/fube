// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Source: shared/serialization/src/FabricSerializableStream.cpp:2181-2232
// at Service Fabric 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.
use mssf_transport::serialization::{
    serde::{from_slice, from_slice_with_budget, to_vec, UInt32Array},
    Decoder, Encoder, MAX_COLLECTION_COUNT,
};
use serde::{Deserialize, Serialize};

#[test]
fn native_ulong_arrays_use_compressed_payload_without_scalar_tags() {
    let values = [0, 1, 127, 128, u32::MAX];
    let expected = [0x88, 5, 0, 1, 127, 0x81, 0, 0x8f, 0xff, 0xff, 0xff, 0x7f];
    let mut encoder = Encoder::new();
    encoder.u32_array(&values).unwrap();
    assert_eq!(encoder.finish(), expected);
    let mut decoder = Decoder::new(&expected);
    assert_eq!(decoder.u32_array().unwrap(), values);
    decoder.finish().unwrap();
    assert_eq!(to_vec(&UInt32Array(&values[..])).unwrap(), expected);
    assert_eq!(
        from_slice::<UInt32Array<Vec<u32>>>(&expected).unwrap().0,
        values
    );
    assert_eq!(to_vec(&UInt32Array(Vec::<u32>::new())).unwrap(), [0xc8]);
    assert!(from_slice::<UInt32Array<Vec<u64>>>(&expected).is_err());
}

#[test]
fn counts_extents_overflow_truncation_and_allocation_are_checked() {
    for bad in [
        vec![0x88, 2, 1],
        vec![0x88, 1, 0x90, 0x80, 0x80, 0x80, 0],
        vec![0x88, 1, 0x80],
        vec![0x08, 1],
        vec![0x88, 0xa0, 0x01],
    ] {
        assert!(
            from_slice::<UInt32Array<Vec<u32>>>(&bad).is_err(),
            "{bad:?}"
        );
    }
    let mut encoder = Encoder::new();
    assert!(encoder
        .u32_array(&vec![0; MAX_COLLECTION_COUNT + 1])
        .is_err());
    encoder.u32_array(&[0; 8]).unwrap();
    let bytes = encoder.finish();
    assert!(
        from_slice_with_budget::<UInt32Array<Vec<u32>>>(&bytes, &mut |_| {
            Err(mssf_transport::Error::ResourceLimit("test"))
        })
        .is_err()
    );
    for end in 0..bytes.len() {
        assert!(from_slice::<UInt32Array<Vec<u32>>>(&bytes[..end]).is_err());
    }
}

#[test]
fn supported_unknown_array_tail_is_skipped_without_allocation() {
    #[derive(Serialize)]
    struct New {
        value: i64,
        tail: UInt32Array<Vec<u32>>,
    }
    #[derive(Debug, Deserialize)]
    struct Old {
        value: i64,
    }
    let bytes = to_vec(&New {
        value: 7,
        tail: UInt32Array(vec![1, 0, 128]),
    })
    .unwrap();
    let decoded: Old =
        from_slice_with_budget(&bytes, &mut |_| panic!("skipped tail allocated")).unwrap();
    assert_eq!(decoded.value, 7);
}
