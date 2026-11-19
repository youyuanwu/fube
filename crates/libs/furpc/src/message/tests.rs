use super::*;
use crate::{
    body::Segment,
    header,
    resource::{admission_scratch_bytes, ByteBudget},
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

fn scope() -> (ByteBudget, ByteScope) {
    let budget = ByteBudget::new(16 * 1024 * 1024).unwrap();
    let scope = ByteScope::new(budget.clone(), None);
    (budget, scope)
}

#[test]
fn native_helpers_and_generic_headers_match_independent_object_schemas() {
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
    assert!(matches!(
        HeaderRecord::native(id, &7u32),
        Err(Error::InvalidInput(_))
    ));
    assert!(HeaderId::new(0x8000).is_err());
    assert!(ActorId::new(u64::MAX).is_err());
    let guid = std::array::from_fn(|index| index as u8);
    let id = crate::MessageId {
        guid,
        index: u32::MAX,
    };
    let record = HeaderRecord::native(header::MESSAGE_ID, &id).unwrap();
    assert_eq!(
        record.value(),
        object(|encoder| {
            encoder.guid(&guid)?;
            encoder.u32(u32::MAX)
        })
    );
    assert!(matches!(
        HeaderRecord::from_encoded(header::ACTOR, vec![0xff]),
        Err(Error::NativeHeader {
            source: fubst::Error::Protocol(_),
            ..
        })
    ));
}

#[test]
fn ordered_lookup_and_atomic_edits_do_not_mutate_shared_messages() {
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
    assert_eq!(edited.first_actor(), None);
    assert_eq!(edited.effective().actor.get(), 0);
    assert_eq!(edited.remove_all(header::ACTOR).unwrap(), 0);
    assert_eq!(edited.remove_all(header::ACTION).unwrap(), 2);
    edited
        .append(header::action("replacement").unwrap())
        .unwrap();
    assert_eq!(
        edited.effective().action(),
        "replacement".encode_utf16().collect::<Vec<_>>()
    );
    assert_eq!(message.headers().effective().actor.get(), 5);
    assert_eq!(
        message.headers().effective().action(),
        "last".encode_utf16().collect::<Vec<_>>()
    );

    let before = encoded(&edited);
    let huge = HeaderRecord::from_encoded(
        HeaderId::new(0x9001).unwrap(),
        vec![0xff; policy::MAX_HEADER_BYTES - 4],
    )
    .unwrap();
    assert!(matches!(edited.append(huge), Err(Error::ResourceLimit(_))));
    assert_eq!(encoded(&edited), before);
}

#[test]
fn received_utf16_unknown_and_deleted_records_forward_without_losing_leases() {
    let mut bytes = Vec::new();
    let mut action = object(|encoder| encoder.string("a"));
    let unit = action.len() - 4;
    action[unit..unit + 2].copy_from_slice(&0xd800u16.to_le_bytes());
    append_raw(&mut bytes, header::ACTION.get(), &action);
    append_raw(&mut bytes, 0x9001, &[0xff]);
    append_raw(&mut bytes, 0x8000, &[0xff]);
    let area = Segment::from_bytes(bytes.clone(), policy::BODY_LIMITS).unwrap();
    let headers = Headers::from_received(
        area,
        &policy::header_context(),
        policy::MAX_HEADER_RETAINED_BYTES,
        policy::MAX_ALLOCATION_LEDGERS,
    )
    .unwrap();
    assert_eq!(headers.first_action().unwrap(), [0xd800]);
    assert_eq!(headers.effective().action(), [0xd800]);
    assert_eq!(headers.raw_records().count(), 3);
    assert_eq!(headers.records().count(), 2);
    assert_eq!(encoded(&headers), bytes);
    let (budget, scope) = scope();
    admit_allocations(headers.allocations(), &scope).unwrap();
    let mut forwarded = Message::builder();
    for record in headers.raw_records() {
        forwarded = forwarded.append_header(record.to_owned());
    }
    let forwarded = forwarded.build().unwrap();
    drop(headers);
    assert!(budget.used() > 0);
    assert_eq!(encoded(forwarded.headers()), bytes);
    forwarded
        .admit_storage(&policy::header_context(), u32::MAX as usize, &scope)
        .unwrap();
    drop(forwarded);
    assert_eq!(budget.used(), 0);
}

#[test]
fn builder_stops_at_the_first_error_and_retains_no_later_body() {
    let huge = HeaderRecord::from_encoded(
        HeaderId::new(0x9001).unwrap(),
        vec![0xff; policy::MAX_HEADER_BYTES - 4],
    )
    .unwrap();
    let builder = Message::builder()
        .append_header(huge)
        .append_header(header::actor(ActorId::new(1).unwrap()).unwrap());
    let first = builder.error.unwrap();
    let builder = builder
        .append_header(header::action("ignored").unwrap())
        .body(Body::from_bytes(vec![1, 2, 3]).unwrap());
    assert_eq!(builder.headers.raw_records().count(), 1);
    assert!(builder.body.is_empty());
    assert_eq!(builder.build().unwrap_err(), first);
}

#[test]
fn standalone_header_encoding_and_adoption_enforce_construction_policy() {
    #[derive(Serialize)]
    struct Text<'a> {
        value: &'a str,
    }
    let text = "a".repeat(policy::MAX_HEADER_BYTES);
    assert!(matches!(
        HeaderRecord::native(HeaderId::new(0x9001).unwrap(), &Text { value: &text }),
        Err(Error::NativeHeader {
            source: fubst::Error::ResourceLimit(_),
            ..
        })
    ));
    let mut oversized_backing = Vec::with_capacity(policy::MAX_HEADER_RETAINED_BYTES + 1);
    oversized_backing.push(0xff);
    assert!(matches!(
        HeaderRecord::from_encoded(HeaderId::new(0x9001).unwrap(), oversized_backing),
        Err(Error::ResourceLimit(_))
    ));
    #[derive(Serialize)]
    struct Node {
        next: fubst::serde::Pointer<Box<Node>>,
    }
    let mut nested = Node {
        next: fubst::serde::Pointer(None),
    };
    for _ in 0..policy::MAX_NATIVE_DEPTH {
        nested = Node {
            next: fubst::serde::Pointer(Some(Box::new(nested))),
        };
    }
    assert!(matches!(
        HeaderRecord::native(HeaderId::new(0x9001).unwrap(), &nested),
        Err(Error::NativeHeader {
            source: fubst::Error::ResourceLimit(_),
            ..
        })
    ));
    let mut headers = Headers::new();
    for index in 0..2 {
        let mut bytes = Vec::with_capacity(policy::MAX_HEADER_RETAINED_BYTES / 2);
        bytes.push(0xff);
        let record = HeaderRecord::from_encoded(HeaderId::new(0x9001).unwrap(), bytes).unwrap();
        let result = headers.append(record);
        if index == 0 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::ResourceLimit(_))));
        }
    }
    assert_eq!(headers.records().count(), 1);

    #[derive(Serialize)]
    struct ZeroWidth {
        values: fubst::serde::CountedValues<Vec<fubst::serde::Trailing<u32>>>,
    }
    let value = ZeroWidth {
        values: fubst::serde::CountedValues(
            (0..=policy::MAX_NATIVE_FIELDS_AND_ELEMENTS)
                .map(|_| fubst::serde::Trailing(None))
                .collect(),
        ),
    };
    assert!(matches!(
        HeaderRecord::native(HeaderId::new(0x9001).unwrap(), &value),
        Err(Error::NativeHeader {
            source: fubst::Error::ResourceLimit("serialization work"),
            ..
        })
    ));
}

#[test]
fn maximum_record_area_and_record_overhead_are_checked_without_growth() {
    let bytes = [0x01, 0x90, 1, 0, 0xff].repeat(policy::MAX_HEADER_RECORDS);
    let area = Segment::from_bytes(bytes, policy::BODY_LIMITS).unwrap();
    let mut headers = Headers::from_received(
        area,
        &policy::header_context(),
        policy::MAX_HEADER_RETAINED_BYTES,
        policy::MAX_ALLOCATION_LEDGERS,
    )
    .unwrap();
    assert_eq!(headers.encoded_len(), policy::MAX_HEADER_BYTES);
    let record = HeaderRecord::from_encoded(HeaderId::new(0x9001).unwrap(), vec![0xff]).unwrap();
    assert!(matches!(
        headers.append(record),
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(headers.records().count(), policy::MAX_HEADER_RECORDS);
    assert_eq!(
        headers.remove_all(HeaderId::new(0x9001).unwrap()).unwrap(),
        policy::MAX_HEADER_RECORDS
    );
    assert!(headers.records().next().is_none());
}

#[test]
fn locally_built_messages_reject_stricter_scope_policies_before_reserving() {
    let message = Message::builder()
        .append_header(header::actor(ActorId::new(70000).unwrap()).unwrap())
        .body(Body::from_bytes(vec![1, 2, 3]).unwrap())
        .build()
        .unwrap();
    let (budget, scope) = scope();
    let context = HeaderDecodeContext::new(HeaderDecodeLimits {
        max_bytes: message.headers.encoded_len() - 1,
        max_records: policy::MAX_HEADER_RECORDS,
        max_depth: policy::MAX_NATIVE_DEPTH,
        max_work_bytes: 2 * policy::MAX_HEADER_BYTES,
    })
    .unwrap();
    assert!(matches!(
        message.admit_storage(&context, u32::MAX as usize, &scope),
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(budget.used(), 0);
    assert!(matches!(
        message.admit_storage(&policy::header_context(), 12, &scope),
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(budget.used(), 0);
    message
        .admit_storage(&policy::header_context(), u32::MAX as usize, &scope)
        .unwrap();
    let retained = budget.used();
    let alias = message.clone();
    alias
        .admit_storage(&policy::header_context(), u32::MAX as usize, &scope)
        .unwrap();
    assert_eq!(budget.used(), retained);
    drop(alias);
    let body = message.into_body();
    assert!(budget.used() > 0);
    assert!(budget.used() < retained);
    drop(body);
    assert_eq!(budget.used(), 0);
}

#[test]
fn failed_combined_header_body_admission_preserves_existing_leases() {
    let message = Message::builder()
        .append_header(header::action("call").unwrap())
        .body(Body::from_bytes(vec![1; 16]).unwrap())
        .build()
        .unwrap();
    let (original, original_scope) = scope();
    message
        .admit_storage(
            &policy::header_context(),
            u32::MAX as usize,
            &original_scope,
        )
        .unwrap();
    let retained = original.used();
    let count = message
        .headers
        .allocations()
        .chain(message.body.allocations())
        .count();
    let insufficient =
        ByteBudget::new(retained + admission_scratch_bytes(count).unwrap() - 1).unwrap();
    let shared = ByteBudget::new(16 * 1024 * 1024).unwrap();
    let scope = ByteScope::new(insufficient.clone(), Some(shared.clone()));
    assert!(matches!(
        message.admit_storage(&policy::header_context(), u32::MAX as usize, &scope),
        Err(Error::ResourceLimit(_))
    ));
    assert_eq!(insufficient.used(), 0);
    assert_eq!(shared.used(), 0);
    assert_eq!(original.used(), retained);
    drop(message);
    assert_eq!(original.used(), 0);
}

#[test]
fn headers_and_body_sharing_receive_backing_use_one_allocation_charge() {
    let mut bytes = Vec::new();
    append_raw(
        &mut bytes,
        header::ACTION.get(),
        &object(|encoder| encoder.string("a")),
    );
    let header_len = bytes.len();
    bytes.extend_from_slice(b"xyz");
    let source = Segment::from_bytes(bytes, policy::BODY_LIMITS).unwrap();
    let headers = Headers::from_received(
        source.slice(0..header_len).unwrap(),
        &policy::header_context(),
        policy::MAX_HEADER_RETAINED_BYTES,
        policy::MAX_ALLOCATION_LEDGERS,
    )
    .unwrap();
    let body = Body {
        inner: StoredBody::from_segments(
            vec![source.slice(header_len..header_len + 3).unwrap()],
            policy::BODY_LIMITS,
        )
        .unwrap(),
    };
    let message = Message { headers, body };
    let (budget, scope) = scope();
    message
        .admit_storage(&policy::header_context(), u32::MAX as usize, &scope)
        .unwrap();
    let body = message.into_body();
    assert_eq!(budget.used(), body.retained_bytes());
    drop(body);
    assert_eq!(budget.used(), source.retained_bytes());
    drop(source);
    assert_eq!(budget.used(), 0);
}

#[test]
fn native_envelope_boundaries_include_record_bytes_without_giant_allocations() {
    let headers = u16::MAX as usize;
    let body = u32::MAX as usize - 12 - headers;
    validate_envelope(headers, body, u32::MAX as usize).unwrap();
    assert!(matches!(
        validate_envelope(headers, body + 1, usize::MAX),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        validate_envelope(headers + 1, 0, usize::MAX),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        validate_envelope(0, usize::MAX, usize::MAX),
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        validate_envelope(0, 1, 12),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn owned_and_borrowed_decoders_share_duplicate_and_schema_validation() {
    let mut bytes = Vec::new();
    append_raw(
        &mut bytes,
        header::ACTOR.get(),
        &object(|encoder| encoder.i64(5)),
    );
    append_raw(&mut bytes, header::ACTOR.get(), &[0xff]);
    assert!(WireHeaders::decode(&bytes, &policy::header_context()).is_err());
    let area = Segment::from_bytes(bytes, policy::BODY_LIMITS).unwrap();
    assert!(matches!(
        Headers::from_received(
            area,
            &policy::header_context(),
            policy::MAX_HEADER_RETAINED_BYTES,
            policy::MAX_ALLOCATION_LEDGERS
        ),
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
