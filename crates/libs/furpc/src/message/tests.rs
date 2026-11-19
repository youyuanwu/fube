use super::*;
use crate::{
    body::Segment,
    header,
    wire::headers::{HeaderDecodeLimits, Headers as WireHeaders},
    ActorId, HeaderId,
};
use fubst::Encoder;
use serde::Serialize;

fn object(fields: impl FnOnce(&mut Encoder) -> fubst::Result<()>) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.object(fields).unwrap();
    encoder.finish()
}

fn append_raw(bytes: &mut Vec<u8>, id: u16, value: &[u8]) {
    bytes.extend_from_slice(&id.to_le_bytes());
    bytes.extend_from_slice(&u16::try_from(value.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(value);
}

fn encoded(headers: &Headers) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in headers.raw_records() {
        append_raw(&mut bytes, record.id().get(), record.value());
    }
    bytes
}

#[test]
fn native_helpers_and_generic_headers_match_independent_schemas() {
    let actor = header::actor(ActorId::new(70000).unwrap()).unwrap();
    assert_eq!(actor.value(), object(|encoder| encoder.i64(70000)));
    let action = header::action("A\u{301}").unwrap();
    assert_eq!(action.value(), object(|encoder| encoder.string("A\u{301}")));
    #[derive(Serialize)]
    struct Tag {
        sequence: u32,
    }
    let id = HeaderId::new(0x9001).unwrap();
    let tag = HeaderRecord::native(id, &Tag { sequence: 7 }).unwrap();
    assert_eq!(tag.value(), object(|encoder| encoder.u32(7)));
    assert!(HeaderRecord::native(id, &7u32).is_err());
    assert!(HeaderId::new(0x8000).is_err());
    assert!(ActorId::new(u64::MAX).is_err());
    let guid = std::array::from_fn(|index| index as u8);
    let record = HeaderRecord::native(
        header::MESSAGE_ID,
        &crate::MessageId {
            guid,
            index: u32::MAX,
        },
    )
    .unwrap();
    assert_eq!(
        record.value(),
        object(|encoder| {
            encoder.guid(&guid)?;
            encoder.u32(u32::MAX)
        })
    );
}

#[test]
fn ordered_lookup_and_atomic_edits_preserve_shared_messages() {
    let message = Message::builder()
        .append_header(header::actor(ActorId::new(1).unwrap()).unwrap())
        .append_header(header::action("first").unwrap())
        .append_header(header::actor(ActorId::new(5).unwrap()).unwrap())
        .append_header(header::action("last").unwrap())
        .build()
        .unwrap();
    assert_eq!(message.headers().first_actor().unwrap().get(), 1);
    assert_eq!(message.headers().effective().actor.get(), 5);
    assert_eq!(
        message.headers().first_action().unwrap(),
        "first".encode_utf16().collect::<Vec<_>>()
    );
    assert_eq!(
        message.headers().effective().action(),
        "last".encode_utf16().collect::<Vec<_>>()
    );
    let mut edited = message.headers().clone();
    assert_eq!(edited.remove_all(header::ACTOR).unwrap(), 2);
    assert!(edited.first_actor().is_none());
    assert_eq!(edited.effective().actor.get(), 0);
    assert_eq!(edited.remove_all(header::ACTOR).unwrap(), 0);
    assert_eq!(edited.remove_all(header::ACTION).unwrap(), 2);
    edited
        .append(header::action("replacement").unwrap())
        .unwrap();
    assert_eq!(message.headers().effective().actor.get(), 5);
    let before = encoded(&edited);
    let huge = HeaderRecord::from_encoded(
        HeaderId::new(0x9001).unwrap(),
        vec![0xff; u16::MAX as usize - 4],
    )
    .unwrap();
    assert!(matches!(edited.append(huge), Err(Error::InvalidInput(_))));
    assert_eq!(encoded(&edited), before);
}

#[test]
fn received_utf16_unknown_and_deleted_records_forward_after_source_drop() {
    let mut bytes = Vec::new();
    let mut action = object(|encoder| encoder.string("a"));
    let unit = action.len() - 4;
    action[unit..unit + 2].copy_from_slice(&0xd800u16.to_le_bytes());
    append_raw(&mut bytes, header::ACTION.get(), &action);
    append_raw(&mut bytes, 0x9001, &[0xff]);
    append_raw(&mut bytes, 0x8000, &[0xff]);
    let headers = Headers::from_received(
        Segment::from_bytes(bytes.clone()),
        &HeaderDecodeContext::native(),
    )
    .unwrap();
    assert_eq!(headers.first_action().unwrap(), [0xd800]);
    assert_eq!(headers.effective().action(), [0xd800]);
    assert_eq!(headers.raw_records().count(), 3);
    assert_eq!(headers.records().count(), 2);
    let mut builder = Message::builder();
    for record in headers.raw_records() {
        builder = builder.append_header(record.to_owned());
    }
    let forwarded = builder.build().unwrap();
    drop(headers);
    assert_eq!(encoded(forwarded.headers()), bytes);
}

#[test]
fn builder_stops_at_the_first_native_error() {
    let huge = HeaderRecord::from_encoded(
        HeaderId::new(0x9001).unwrap(),
        vec![0xff; u16::MAX as usize - 4],
    )
    .unwrap();
    let builder = Message::builder()
        .append_header(huge)
        .append_header(header::actor(ActorId::new(1).unwrap()).unwrap());
    let first = builder.error.clone().unwrap();
    let builder = builder
        .append_header(header::action("ignored").unwrap())
        .body(Body::from_bytes(vec![1, 2, 3]).unwrap());
    assert_eq!(builder.headers.raw_records().count(), 1);
    assert!(builder.body.is_empty());
    assert_eq!(builder.build().unwrap_err(), first);
}

#[test]
fn constructors_enforce_native_widths_without_extra_backing_depth_or_work_caps() {
    let id = HeaderId::new(0x9001).unwrap();
    #[derive(Serialize)]
    struct Text<'a> {
        value: &'a str,
    }
    let text = "a".repeat(u16::MAX as usize);
    assert!(HeaderRecord::native(id, &Text { value: &text }).is_err());
    assert!(HeaderRecord::from_encoded(id, Vec::new()).is_err());
    assert!(HeaderRecord::from_encoded(id, vec![0xff; u16::MAX as usize - 3]).is_err());
    let mut bytes = Vec::with_capacity(4 * 1024 * 1024 + 1);
    bytes.push(0xff);
    let mut headers = Headers::new();
    headers
        .append(HeaderRecord::from_encoded(id, bytes).unwrap())
        .unwrap();

    fn nested(encoder: &mut Encoder, depth: usize) -> fubst::Result<()> {
        encoder.object(|encoder| {
            if depth == 0 {
                Ok(())
            } else {
                nested(encoder, depth - 1)
            }
        })
    }
    let actor = object(|encoder| {
        encoder.i64(7)?;
        nested(encoder, 80)
    });
    headers
        .append(HeaderRecord::from_encoded(header::ACTOR, actor).unwrap())
        .unwrap();
    assert_eq!(headers.effective().actor.get(), 7);
    #[derive(Serialize)]
    struct ZeroWidth {
        values: fubst::serde::CountedValues<Vec<fubst::serde::Trailing<u32>>>,
    }
    let value = ZeroWidth {
        values: fubst::serde::CountedValues(
            (0..65_536).map(|_| fubst::serde::Trailing(None)).collect(),
        ),
    };
    headers
        .append(HeaderRecord::native(id, &value).unwrap())
        .unwrap();

    let byte = Body::from_bytes(vec![1]).unwrap();
    let body = Body::from_segments(vec![byte; 1025]).unwrap();
    assert_eq!(body.len(), 1025);
    assert_eq!(body.segments().count(), 1025);
}

#[test]
fn maximum_record_area_includes_physical_record_overhead() {
    let count = u16::MAX as usize / 5;
    let bytes = [1, 0x90, 1, 0, 0xff].repeat(count);
    let mut headers =
        Headers::from_received(Segment::from_bytes(bytes), &HeaderDecodeContext::native()).unwrap();
    assert_eq!(headers.encoded_len(), u16::MAX as usize);
    let record = HeaderRecord::from_encoded(HeaderId::new(0x9001).unwrap(), vec![0xff]).unwrap();
    assert!(matches!(
        headers.append(record),
        Err(Error::InvalidInput(_))
    ));
    assert_eq!(headers.records().count(), count);
    assert_eq!(
        headers.remove_all(HeaderId::new(0x9001).unwrap()).unwrap(),
        count
    );
    assert!(headers.records().next().is_none());
}

#[test]
fn submission_validation_rechecks_the_configured_frame_limit() {
    let message = Message::builder()
        .append_header(header::actor(ActorId::new(70000).unwrap()).unwrap())
        .body(Body::from_bytes(vec![1, 2, 3]).unwrap())
        .build()
        .unwrap();
    assert!(matches!(
        message.validate(&HeaderDecodeContext::native(), 12),
        Err(Error::ResourceLimit(_))
    ));
    message
        .validate(&HeaderDecodeContext::native(), u32::MAX as usize)
        .unwrap();
    let context = HeaderDecodeContext::new(HeaderDecodeLimits {
        max_bytes: message.headers.encoded_len() - 1,
        max_records: 100,
        max_depth: 80,
        max_work_bytes: 1000,
    })
    .unwrap();
    assert!(matches!(
        message.validate(&context, u32::MAX as usize),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn native_envelope_boundaries_need_no_giant_allocations() {
    let headers = u16::MAX as usize;
    let body = u32::MAX as usize - 12 - headers;
    validate_envelope(headers, body, u32::MAX as usize).unwrap();
    for (headers, body) in [(headers, body + 1), (headers + 1, 0), (0, usize::MAX)] {
        assert!(matches!(
            validate_envelope(headers, body, usize::MAX),
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(matches!(
        validate_envelope(0, 1, 12),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn owned_and_borrowed_decoders_validate_all_common_duplicates() {
    let mut bytes = Vec::new();
    append_raw(
        &mut bytes,
        header::ACTOR.get(),
        &object(|encoder| encoder.i64(5)),
    );
    append_raw(&mut bytes, header::ACTOR.get(), &[0xff]);
    assert!(WireHeaders::decode(&bytes, &HeaderDecodeContext::native()).is_err());
    assert!(matches!(
        Headers::from_received(Segment::from_bytes(bytes), &HeaderDecodeContext::native()),
        Err(Error::NativeHeader {
            source: fubst::Error::Protocol(_),
            ..
        })
    ));
    assert!(matches!(
        HeaderRecord::from_encoded(header::MESSAGE_ID, object(|_| Ok(()))),
        Err(Error::UnsupportedSchema { .. })
    ));
}
