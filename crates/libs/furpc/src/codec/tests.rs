use super::*;
use serde::{Deserialize, Serialize};

struct Segmented;
impl Encoder for Segmented {
    type Item = Body;

    fn encode(&mut self, body: Body, dst: &mut EncodeBuf<'_>) -> Result<()> {
        dst.write_all(b"ab")?;
        dst.append_body(body)?;
        dst.write_all(b"ef")
    }
}

#[test]
fn shared_encoding_and_decode_cursors_cross_segment_boundaries() {
    let shared = Body::from_bytes(b"_cd_".to_vec())
        .unwrap()
        .slice(1..3)
        .unwrap();
    let pointer = shared.segments().next().unwrap().as_ptr();
    let body = encode(&mut Segmented, shared.clone(), 6).unwrap();
    assert_eq!(
        body.segments().collect::<Vec<_>>(),
        [b"ab".as_slice(), b"cd", b"ef"]
    );
    assert_eq!(body.segments().nth(1).unwrap().as_ptr(), pointer);
    assert!(matches!(
        encode(&mut Segmented, shared, 5),
        Err(CodecError::ResourceLimit { .. })
    ));

    let mut src = DecodeBuf::new(&body);
    let mut bytes = [0; 3];
    src.copy_to_slice(&mut bytes).unwrap();
    assert_eq!(&bytes, b"abc");
    let view = src.take(2).unwrap();
    assert_eq!(view.segments().collect::<Vec<_>>(), [b"d".as_slice(), b"e"]);
    assert_eq!(
        view.segments().next().unwrap().as_ptr(),
        pointer.wrapping_add(1)
    );
    assert!(src.advance(2).is_err());
    assert!(src.copy_to_slice(&mut bytes).is_err());
    assert_eq!(src.copy_remaining().unwrap(), b"f");
    src.copy_to_slice(&mut []).unwrap();
    src.finish().unwrap();
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Payload {
    text: String,
    count: u64,
}

#[test]
fn fubst_preserves_native_bytes_and_decodes_fragmented_bodies_exactly() {
    let value = Payload {
        text: "native \u{1f980}".into(),
        count: 1000,
    };
    let expected = fubst::serde::to_vec(&value).unwrap();
    let body = encode(
        &mut FubstEncoder::<Payload>::default(),
        value,
        expected.len(),
    )
    .unwrap();
    assert_eq!(body.segments().next().unwrap(), expected);
    let fragmented = Body::from_segments(vec![
        body.slice(0..3).unwrap(),
        body.slice(3..body.len()).unwrap(),
    ])
    .unwrap();
    let result = decode(
        &mut FubstDecoder::<Payload>::default(),
        &fragmented,
        fragmented.len(),
    )
    .unwrap();
    assert_eq!(result.text, "native \u{1f980}");
    assert_eq!(result.count, 1000);
    let trailing =
        Body::from_segments(vec![fragmented, Body::from_bytes(vec![0x42]).unwrap()]).unwrap();
    assert!(matches!(
        decode(
            &mut FubstDecoder::<Payload>::default(),
            &trailing,
            trailing.len()
        ),
        Err(CodecError::InvalidPayload { .. })
    ));
}

#[test]
fn fubst_respects_explicit_byte_and_depth_limits_without_changing_error_category() {
    let body = Body::from_bytes(fubst::serde::to_vec("hello").unwrap()).unwrap();
    assert!(matches!(
        encode(
            &mut FubstEncoder::<String>::default(),
            "hello".into(),
            body.len() - 1
        ),
        Err(CodecError::ResourceLimit { .. })
    ));
    let mut decoder = FubstDecoder::<String>::with_limits(fubst::Limits {
        max_bytes: body.len() - 1,
        ..Default::default()
    });
    for body in [
        body.clone(),
        Body::from_segments(vec![
            body.slice(0..1).unwrap(),
            body.slice(1..body.len()).unwrap(),
        ])
        .unwrap(),
    ] {
        assert!(matches!(
            decode(&mut decoder, &body, body.len()),
            Err(CodecError::ResourceLimit { .. })
        ));
    }
    let mut encoder = FubstEncoder::<Payload>::with_limits(fubst::Limits {
        max_depth: 0,
        ..Default::default()
    });
    assert!(matches!(
        encode(
            &mut encoder,
            Payload {
                text: String::new(),
                count: 0
            },
            MAX_BODY_BYTES
        ),
        Err(CodecError::ResourceLimit { .. })
    ));
}
