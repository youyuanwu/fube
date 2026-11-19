use super::*;
use fubst::Encoder;

fn context() -> HeaderDecodeContext {
    HeaderDecodeContext::new(HeaderDecodeLimits {
        max_bytes: MAX_NATIVE_HEADER_BYTES,
        max_records: 16_384,
        max_depth: 32,
        max_work_bytes: 2 * MAX_NATIVE_HEADER_BYTES,
    })
    .unwrap()
}

fn object(fields: impl FnOnce(&mut Encoder) -> fubst::Result<()>) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.object(fields).unwrap();
    encoder.finish()
}

fn append_raw(encoded: &mut Vec<u8>, id: u16, value: &[u8]) {
    encoded.extend_from_slice(&id.to_le_bytes());
    encoded.extend_from_slice(&u16::try_from(value.len()).unwrap().to_le_bytes());
    encoded.extend_from_slice(value);
}

fn append(encoded: &mut Vec<u8>, id: u16, fields: impl FnOnce(&mut Encoder) -> fubst::Result<()>) {
    append_raw(encoded, id, &object(fields));
}

#[test]
fn native_record_layout_preserves_order_unknown_and_deleted_bytes() {
    // Independent native Int64 Actor object, followed by opaque/deleted records.
    let bytes = [
        0x01, 0x80, 14, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0x1f, 0x09, 5, 0x2f, 0x3f, 0xfe, 0x7f, 1, 0,
        0xff, 0x00, 0x80, 1, 0, 0xff,
    ];
    let headers = Headers::decode(&bytes, &context()).unwrap();
    assert_eq!(headers.encoded(), bytes);
    assert_eq!(headers.encoded().as_ptr(), bytes.as_ptr());
    assert_eq!(
        headers.raw_records().map(HeaderRef::id).collect::<Vec<_>>(),
        [ACTOR, 0x7ffe, DELETED]
    );
    assert_eq!(
        headers.records().map(HeaderRef::id).collect::<Vec<_>>(),
        [ACTOR, 0x7ffe]
    );
    assert_eq!(headers.first_raw(0x7ffe).unwrap().value(), [0xff]);
    assert!(headers.first_raw(DELETED).is_none());
    assert_eq!(headers.first_actor(), Some(5));
    assert_eq!(headers.effective().actor, 5);
}

#[test]
fn every_common_property_uses_the_last_visible_occurrence() {
    let mut bytes = Vec::new();
    for (actor, text, guid, index, retry, flag, code) in [
        (1, "first", [0x11; 16], 7, -3, true, -9),
        (5, "last", [0x22; 16], u32::MAX, i32::MAX, false, 0),
    ] {
        append(&mut bytes, ACTOR, |encoder| encoder.i64(actor));
        append(&mut bytes, ACTION, |encoder| encoder.string(text));
        for id in [MESSAGE_ID, RELATES_TO] {
            append(&mut bytes, id, |encoder| {
                encoder.guid(&guid)?;
                encoder.u32(index)
            });
        }
        append(&mut bytes, RETRY, |encoder| encoder.i32(retry));
        for id in [EXPECTS_REPLY, IDEMPOTENT, HIGH_PRIORITY, UNCORRELATED_REPLY] {
            append(&mut bytes, id, |encoder| encoder.boolean(flag));
        }
        append(&mut bytes, FAULT, |encoder| {
            encoder.i64(code)?;
            encoder.boolean(flag)
        });
        append_raw(&mut bytes, DELETED, &[0xff]);
    }
    let headers = Headers::decode(&bytes, &context()).unwrap();
    assert_eq!(headers.first_actor(), Some(1));
    assert_eq!(
        headers.first_action().unwrap().units().collect::<Vec<_>>(),
        "first".encode_utf16().collect::<Vec<_>>()
    );
    let common = headers.effective();
    assert_eq!(common.actor, 5);
    assert_eq!(
        common.action.units().collect::<Vec<_>>(),
        "last".encode_utf16().collect::<Vec<_>>()
    );
    let id = MessageId {
        guid: [0x22; 16],
        index: u32::MAX,
    };
    assert_eq!(common.message_id, id);
    assert_eq!(common.relates_to, id);
    assert_eq!(common.retry_count, i32::MAX);
    assert!(!common.expects_reply);
    assert!(!common.idempotent);
    assert!(!common.high_priority);
    assert!(!common.uncorrelated_reply);
    assert_eq!(common.fault, Fault::default());
}

#[test]
fn malformed_duplicates_fail_before_or_after_valid_neighbors() {
    for id in [
        ACTOR,
        ACTION,
        MESSAGE_ID,
        RELATES_TO,
        RETRY,
        EXPECTS_REPLY,
        IDEMPOTENT,
        HIGH_PRIORITY,
        UNCORRELATED_REPLY,
        FAULT,
    ] {
        let valid = object(|encoder| match id {
            ACTOR => encoder.i64(5),
            ACTION => encoder.string("action"),
            MESSAGE_ID | RELATES_TO => {
                encoder.guid(&[1; 16])?;
                encoder.u32(1)
            }
            RETRY => encoder.i32(1),
            FAULT => {
                encoder.i64(-1)?;
                encoder.boolean(true)
            }
            _ => encoder.boolean(true),
        });
        for malformed_first in [true, false] {
            let mut bytes = Vec::new();
            let values: [&[u8]; 2] = if malformed_first {
                [&[0xff], &valid]
            } else {
                [&valid, &[0xff]]
            };
            for value in values {
                append_raw(&mut bytes, id, value);
            }
            assert!(
                matches!(
                    Headers::decode(&bytes, &context()),
                    Err(HeaderError::Decode {
                        id: actual,
                        source: fubst::Error::Protocol(_),
                    }) if actual == id
                ),
                "id {id:#06x}, malformed_first {malformed_first}"
            );
        }
    }
}

#[test]
fn absent_actor_and_action_are_not_synthesized() {
    let mut bytes = Vec::new();
    append_raw(&mut bytes, 0x8019, &[0xff]);
    let headers = Headers::decode(&bytes, &context()).unwrap();
    assert_eq!(headers.first_actor(), None);
    assert_eq!(headers.first_action(), None);
    assert!(headers.first_raw(ACTOR).is_none());
    assert_eq!(*headers.effective(), CommonHeaders::default());
    assert_eq!(headers.encoded(), bytes);
    assert_eq!(headers.records().count(), 1);
}

#[test]
fn action_borrows_unaligned_raw_utf16_and_does_not_normalize() {
    let mut bytes = Vec::new();
    append_raw(&mut bytes, 0x7ffe, &[0xff]);
    // A, lone high surrogate, NUL, decomposed e + accent; native string metadata.
    let value = [
        0, 23, 0, 0, 0, 0, 0, 0, 0, 0x1f, 0x8d, 5, 0x41, 0, 0, 0xd8, 0, 0, 0x65, 0, 0x01, 0x03,
        0x2f, 0x3f,
    ];
    append_raw(&mut bytes, ACTION, &value);
    let headers = Headers::decode(&bytes, &context()).unwrap();
    let action = headers.first_action().unwrap();
    assert_eq!(
        action.units().collect::<Vec<_>>(),
        [0x41, 0xd800, 0, 0x65, 0x301]
    );
    assert_eq!(action.as_le_bytes().as_ptr(), bytes[21..].as_ptr());
    assert_eq!(headers.effective().action, action);
    assert!(String::from_utf16(&action.units().collect::<Vec<_>>()).is_err());
}

#[test]
fn schema_defaults_are_distinct_from_absent_header_properties() {
    let mut bytes = Vec::new();
    for id in [
        ACTOR,
        ACTION,
        RETRY,
        EXPECTS_REPLY,
        IDEMPOTENT,
        HIGH_PRIORITY,
        UNCORRELATED_REPLY,
    ] {
        append(&mut bytes, id, |_| Ok(()));
    }
    append(&mut bytes, FAULT, |encoder| encoder.i64(-17));
    let headers = Headers::decode(&bytes, &context()).unwrap();
    assert_eq!(headers.first_actor(), Some(0));
    assert_eq!(headers.first_action().unwrap().units().len(), 0);
    let common = headers.effective();
    assert_eq!(common.retry_count, 0);
    assert!(!common.expects_reply);
    assert!(common.idempotent);
    assert!(common.high_priority);
    assert!(common.uncorrelated_reply);
    assert_eq!(
        common.fault,
        Fault {
            code: -17,
            has_body: false
        }
    );
    assert!(
        !Headers::decode(&[], &context())
            .unwrap()
            .effective()
            .idempotent
    );
}

#[test]
fn wrong_present_field_types_are_not_missing_trailing_defaults() {
    for id in [ACTOR, RETRY, EXPECTS_REPLY, MESSAGE_ID, FAULT] {
        let mut bytes = Vec::new();
        append(&mut bytes, id, |encoder| match id {
            ACTOR => encoder.i32(0),
            RETRY => encoder.i64(0),
            EXPECTS_REPLY => encoder.u32(0),
            MESSAGE_ID => {
                encoder.guid(&[1; 16])?;
                encoder.u64(0)
            }
            FAULT => {
                encoder.i64(-1)?;
                encoder.u32(0)
            }
            _ => unreachable!(),
        });
        assert!(matches!(
            Headers::decode(&bytes, &context()),
            Err(HeaderError::Decode {
                id: actual,
                source: fubst::Error::Protocol(_),
            }) if actual == id
        ));
    }
}

#[test]
fn incomplete_native_ids_are_unsupported_not_synthesized_or_malformed() {
    for id in [MESSAGE_ID, RELATES_TO] {
        for guid_present in [false, true] {
            let mut bytes = Vec::new();
            append(&mut bytes, id, |encoder| {
                if guid_present {
                    encoder.guid(&[1; 16])?;
                }
                Ok(())
            });
            assert!(matches!(
                Headers::decode(&bytes, &context()),
                Err(HeaderError::UnsupportedSchema { id: actual, .. }) if actual == id
            ));
        }
    }
}

#[test]
fn known_values_validate_suffixes_and_exact_object_extents() {
    let mut bytes = Vec::new();
    append(&mut bytes, ACTOR, |encoder| {
        encoder.i64(5)?;
        encoder.object(|encoder| encoder.u32(8))?;
        encoder.next_scope()?;
        encoder.boolean(true)
    });
    assert_eq!(
        Headers::decode(&bytes, &context())
            .unwrap()
            .effective()
            .actor,
        5
    );

    for mutation in 0..3 {
        let mut value = object(|encoder| encoder.i64(5));
        match mutation {
            0 => value.push(0xff),
            1 => {
                let last = value.len() - 2;
                value[last] = 0xff;
            }
            2 => value[1] -= 1,
            _ => unreachable!(),
        }
        let mut bytes = Vec::new();
        append_raw(&mut bytes, ACTOR, &value);
        assert!(matches!(
            Headers::decode(&bytes, &context()),
            Err(HeaderError::Decode {
                source: fubst::Error::Protocol(_),
                ..
            })
        ));
    }
}

#[test]
fn native_suffix_values_preserve_common_headers_and_unknown_bytes() {
    for suffix in [&[0x04, 0x2f, 0x44][..], &[0x8d, 1, 0x00, 0xd8]] {
        let mut value = object(|encoder| encoder.string("first"));
        let end = value.len() - 2;
        value.splice(end..end, suffix.iter().copied());
        let extent = u32::try_from(value.len() - 1).unwrap();
        value[1..5].copy_from_slice(&extent.to_le_bytes());
        let mut bytes = Vec::new();
        append_raw(&mut bytes, ACTION, &value);
        append(&mut bytes, ACTION, |encoder| encoder.string("last"));
        let headers = Headers::decode(&bytes, &context()).unwrap();
        assert_eq!(
            headers.first_action().unwrap().units().collect::<Vec<_>>(),
            "first".encode_utf16().collect::<Vec<_>>()
        );
        assert_eq!(
            headers.effective().action.units().collect::<Vec<_>>(),
            "last".encode_utf16().collect::<Vec<_>>()
        );
        assert_eq!(headers.first_raw(ACTION).unwrap().value(), value);
        assert_eq!(headers.encoded(), bytes);
    }
}

#[test]
fn malformed_record_lengths_include_deleted_and_unknown_records() {
    for bytes in [
        vec![0],
        vec![0, 0x80, 0],
        vec![0, 0x80, 0, 0],
        vec![0xfe, 0x7f, 2, 0, 0xff],
        vec![0, 0x80, 2, 0, 0xff],
    ] {
        assert!(matches!(
            Headers::decode(&bytes, &context()),
            Err(HeaderError::Protocol(_))
        ));
    }
}

#[test]
fn record_byte_and_work_limits_remain_resource_policy() {
    let mut bytes = Vec::new();
    append_raw(&mut bytes, DELETED, &[0xff]);
    append_raw(&mut bytes, 0x7ffe, &[0xff]);
    append(&mut bytes, ACTOR, |encoder| encoder.i64(5));
    let value_len = 14;
    let limits = HeaderDecodeLimits {
        max_bytes: bytes.len(),
        max_records: 3,
        max_depth: 1,
        max_work_bytes: bytes.len() + value_len,
    };
    let context = HeaderDecodeContext::new(limits).unwrap();
    assert!(Headers::decode(&bytes, &context).is_ok());
    for (limited, reason) in [
        (
            HeaderDecodeLimits {
                max_bytes: bytes.len() - 1,
                ..limits
            },
            "encoded header bytes",
        ),
        (
            HeaderDecodeLimits {
                max_records: 2,
                ..limits
            },
            "record count",
        ),
        (
            HeaderDecodeLimits {
                max_work_bytes: bytes.len() + value_len - 1,
                ..limits
            },
            "decode work bytes",
        ),
        (
            HeaderDecodeLimits {
                max_work_bytes: bytes.len() - 1,
                ..limits
            },
            "decode work bytes",
        ),
    ] {
        let context = HeaderDecodeContext::new(limited).unwrap();
        assert!(matches!(
            Headers::decode(&bytes, &context),
            Err(HeaderError::ResourceLimit(actual)) if actual == reason
        ));
    }
    let context = HeaderDecodeContext::new(HeaderDecodeLimits {
        max_depth: 0,
        ..limits
    })
    .unwrap();
    assert!(matches!(
        Headers::decode(&bytes, &context),
        Err(HeaderError::Decode {
            id: ACTOR,
            source: fubst::Error::ResourceLimit("object nesting")
        })
    ));
}

#[test]
fn work_and_depth_limits_cover_duplicate_payloads_and_unknown_suffixes() {
    let mut bytes = Vec::new();
    for actor in [1, 5] {
        append(&mut bytes, ACTOR, |encoder| {
            encoder.i64(actor)?;
            encoder.object(|encoder| encoder.u32(8))
        });
    }
    let limits = HeaderDecodeLimits {
        max_bytes: bytes.len(),
        max_records: 2,
        max_depth: 2,
        max_work_bytes: 2 * bytes.len() - 2 * RECORD_PREFIX_LEN,
    };
    assert!(Headers::decode(&bytes, &HeaderDecodeContext::new(limits).unwrap()).is_ok());
    let shallow = HeaderDecodeContext::new(HeaderDecodeLimits {
        max_depth: 1,
        ..limits
    })
    .unwrap();
    assert!(matches!(
        Headers::decode(&bytes, &shallow),
        Err(HeaderError::Decode {
            source: fubst::Error::ResourceLimit("object nesting"),
            ..
        })
    ));
    let less_work = HeaderDecodeContext::new(HeaderDecodeLimits {
        max_work_bytes: limits.max_work_bytes - 1,
        ..limits
    })
    .unwrap();
    assert!(matches!(
        Headers::decode(&bytes, &less_work),
        Err(HeaderError::ResourceLimit("decode work bytes"))
    ));
}

#[test]
fn native_header_bound_is_exact_and_old_record_count_ceiling_is_not_wire_validity() {
    let bytes = [0xfe, 0x7f, 1, 0, 0xff].repeat(MAX_NATIVE_HEADER_BYTES / 5);
    assert_eq!(bytes.len(), MAX_NATIVE_HEADER_BYTES);
    let headers = Headers::decode(&bytes, &context()).unwrap();
    assert_eq!(headers.records().count(), MAX_NATIVE_HEADER_BYTES / 5);
    let mut oversized = bytes;
    oversized.push(0);
    assert!(matches!(
        Headers::decode(&oversized, &context()),
        Err(HeaderError::Protocol("combined header area exceeds u16"))
    ));
    let mut limits = context().limits;
    limits.max_bytes += 1;
    assert!(matches!(
        HeaderDecodeContext::new(limits),
        Err(HeaderError::InvalidInput(_))
    ));
}
