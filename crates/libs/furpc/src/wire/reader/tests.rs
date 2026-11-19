use super::*;

fn wire() -> FrameWireContext {
    FrameWireContext::new(0, true, true).unwrap()
}
fn work(bytes: usize) -> WorkBudget {
    WorkBudget::new(bytes, 2).unwrap()
}
fn input(bytes: &[u8]) -> ReceiveInput {
    ReceiveInput::new(Segment::from_bytes(bytes.to_vec()))
}

fn encoded(headers: &[u8], body: &[u8]) -> Vec<u8> {
    let mut crc = Crc32::new();
    crc.update(headers);
    crc.update(body);
    let header = FrameHeader::for_send(headers.len(), body.len(), crc.value(), &wire()).unwrap();
    [header.encode().as_slice(), headers, body].concat()
}

#[derive(Debug, Default)]
struct Outcome {
    headers: Vec<Vec<u8>>,
    bodies: Vec<Body>,
    discarded: usize,
}

fn drive<'a>(
    chunks: impl IntoIterator<Item = &'a [u8]>,
    retain: bool,
    quantum: usize,
) -> FrameResult<Outcome> {
    let mut reader = FrameReader::new();
    let mut prefix = None;
    let mut outcome = Outcome::default();
    for chunk in chunks {
        let mut input = input(chunk);
        let mut calls = 0;
        loop {
            calls += 1;
            assert!(calls < chunk.len() * 4 + 32);
            let before = input.remaining().len();
            let step = reader.step(&mut input, &wire(), work(quantum))?;
            assert!(before - input.remaining().len() <= quantum);
            match step {
                FrameReadStep::FixedHeader { frame, header } => {
                    reader.admit_headers(frame, HeaderPermit::new(frame, header, 1024 * 1024)?)?;
                }
                FrameReadStep::Headers(headers) => {
                    outcome.headers.push(headers.area.bytes().to_vec());
                    let decision = if retain {
                        BodyDecision::Retain(BodyPermit::new(headers.frame, headers.header)?)
                    } else {
                        BodyDecision::Discard(DiscardReason::NoDestination)
                    };
                    reader.decide_body(headers.frame, decision)?;
                    prefix = Some(headers);
                }
                FrameReadStep::Complete(complete) => {
                    assert_eq!(prefix.take().unwrap().frame, complete.frame);
                    outcome.bodies.push(complete.body);
                }
                FrameReadStep::Discarded(discarded) => {
                    assert_eq!(prefix.take().unwrap().frame, discarded.frame);
                    assert_eq!(discarded.reason, DiscardReason::NoDestination);
                    outcome.discarded += 1;
                }
                FrameReadStep::NeedInput { max_bytes } => {
                    assert!(max_bytes > 0 && max_bytes <= quantum);
                    assert!(input.remaining().is_empty());
                    break;
                }
                FrameReadStep::Yield => {}
            }
        }
    }
    reader.finish_eof(&input(&[]))?;
    Ok(outcome)
}

#[test]
fn every_split_and_one_byte_turns_preserve_shared_headers_and_bodies() {
    let bytes = encoded(b"header area", b"body payload");
    for split in 0..=bytes.len() {
        let outcome = drive([&bytes[..split], &bytes[split..]], true, 1).unwrap();
        assert_eq!(outcome.headers, [b"header area"]);
        let alias = outcome.bodies[0].slice(1..4).unwrap();
        drop(outcome);
        assert_eq!(alias.segments().next().unwrap(), b"ody");
    }
    assert_eq!(drive(bytes.chunks(1), true, 3).unwrap().bodies[0].len(), 12);
}

#[test]
fn coalesced_frames_and_empty_stages_keep_distinct_events() {
    let bytes = [
        encoded(b"", b""),
        encoded(b"hdr", b"first"),
        encoded(b"", b"second"),
        encoded(b"last", b""),
    ]
    .concat();
    let outcome = drive([bytes.as_slice()], true, 1024).unwrap();
    assert_eq!(outcome.headers, [b"".as_slice(), b"hdr", b"", b"last"]);
    assert_eq!(
        outcome.bodies.iter().map(Body::len).collect::<Vec<_>>(),
        [0, 5, 6, 0]
    );
    let outcome = drive([bytes.as_slice()], false, 1).unwrap();
    assert_eq!(outcome.discarded, 4);
    assert!(outcome.bodies.is_empty());
}

#[test]
fn admission_stops_at_exact_boundaries_and_discard_preserves_the_next_frame() {
    let bytes = [encoded(b"hdr", &[7; 8192]), encoded(b"", b"next")].concat();
    let mut input = input(&bytes);
    let mut reader = FrameReader::new();
    let FrameReadStep::FixedHeader { frame, header } =
        reader.step(&mut input, &wire(), work(20_000)).unwrap()
    else {
        panic!("missing header")
    };
    assert_eq!(input.consumed, 12);
    assert!(matches!(
        HeaderPermit::new(frame, header, header.frame_len() as usize - 1),
        Err(FrameError::SizeLimit { .. })
    ));
    reader
        .admit_headers(frame, HeaderPermit::new(frame, header, 20_000).unwrap())
        .unwrap();
    let FrameReadStep::Headers(prefix) = reader.step(&mut input, &wire(), work(20_000)).unwrap()
    else {
        panic!("missing headers")
    };
    assert_eq!(prefix.area.bytes(), b"hdr");
    assert_eq!(input.consumed, 15);
    reader
        .decide_body(frame, BodyDecision::Discard(DiscardReason::PolicyRejected))
        .unwrap();
    assert!(matches!(
        reader.step(&mut input, &wire(), work(20_000)).unwrap(),
        FrameReadStep::Discarded(_)
    ));
    assert_eq!(input.consumed, header.frame_len() as usize);
    assert_eq!(input.remaining(), encoded(b"", b"next"));
}

#[test]
fn retain_and_discard_verify_the_same_bytes_including_the_header_area() {
    let good = encoded(b"hdr", b"body");
    for retain in [false, true] {
        for index in 12..good.len() {
            let mut corrupt = good.clone();
            corrupt[index] ^= 0x80;
            assert!(matches!(
                drive(corrupt.chunks(1), retain, 2),
                Err(FrameError::BodyChecksumMismatch { .. })
            ));
        }
        let mut unchecked = good.clone();
        unchecked[5] = 0;
        unchecked[8..12].fill(0);
        *unchecked.last_mut().unwrap() ^= 0x80;
        assert!(drive(unchecked.chunks(1), retain, 1).is_ok());
    }
}

#[test]
fn eof_reports_every_partial_stage_and_accepts_only_frame_boundaries() {
    let bytes = encoded(b"hdr", b"body");
    for retain in [false, true] {
        for end in 1..bytes.len() {
            let (stage, missing) = if end < 12 {
                ("fixed header", 12 - end)
            } else if end < 15 {
                ("header area", 15 - end)
            } else {
                ("body", bytes.len() - end)
            };
            assert!(matches!(drive([&bytes[..end]], retain, 1),
                Err(FrameError::Truncated { stage: actual, missing: count }) if actual == stage && count == missing));
        }
        assert!(drive([&[][..]], retain, 1).is_ok());
        assert!(drive([bytes.as_slice()], retain, 1).is_ok());
    }
}

#[test]
fn wrong_reader_duplicate_and_unresolved_gate_calls_are_terminal() {
    let bytes = encoded(b"", b"");
    let mut first_input = input(&bytes);
    let mut second_input = input(&bytes);
    let mut first = FrameReader::new();
    let mut second = FrameReader::new();
    let FrameReadStep::FixedHeader {
        frame: first_key,
        header,
    } = first.step(&mut first_input, &wire(), work(64)).unwrap()
    else {
        panic!("missing frame")
    };
    let FrameReadStep::FixedHeader {
        frame: second_key, ..
    } = second.step(&mut second_input, &wire(), work(64)).unwrap()
    else {
        panic!("missing frame")
    };
    assert_ne!(first_key, second_key);
    let error = second
        .admit_headers(
            second_key,
            HeaderPermit::new(first_key, header, 64).unwrap(),
        )
        .unwrap_err();
    assert!(matches!(error, FrameError::InvalidState(_)));
    assert_eq!(
        second
            .step(&mut second_input, &wire(), work(64))
            .unwrap_err(),
        error
    );
    assert!(matches!(
        first.finish_eof(&first_input),
        Err(FrameError::InvalidState(_))
    ));
    let mut reader = FrameReader::new();
    let mut input = input(&bytes);
    let FrameReadStep::FixedHeader { frame, header } =
        reader.step(&mut input, &wire(), work(64)).unwrap()
    else {
        panic!("missing frame")
    };
    reader
        .admit_headers(frame, HeaderPermit::new(frame, header, 64).unwrap())
        .unwrap();
    assert!(matches!(
        reader.admit_headers(frame, HeaderPermit::new(frame, header, 64).unwrap()),
        Err(FrameError::InvalidState(_))
    ));
}

#[test]
fn stale_body_decisions_and_wrong_body_permits_cannot_cross_frames() {
    let bytes = [encoded(b"", b""), encoded(b"", b"body")].concat();
    for wrong_permit in [false, true] {
        let mut input = input(&bytes);
        let mut reader = FrameReader::new();
        let mut previous = None;
        for index in 0..2 {
            let FrameReadStep::FixedHeader { frame, header } =
                reader.step(&mut input, &wire(), work(128)).unwrap()
            else {
                panic!("missing frame")
            };
            reader
                .admit_headers(frame, HeaderPermit::new(frame, header, 128).unwrap())
                .unwrap();
            assert!(matches!(
                reader.step(&mut input, &wire(), work(128)).unwrap(),
                FrameReadStep::Headers(_)
            ));
            if index == 0 {
                previous = Some((frame, header));
                reader
                    .decide_body(frame, BodyDecision::Discard(DiscardReason::NoDestination))
                    .unwrap();
                assert!(matches!(
                    reader.step(&mut input, &wire(), work(128)).unwrap(),
                    FrameReadStep::Discarded(_)
                ));
            } else {
                let (old_frame, old_header) = previous.unwrap();
                let result = if wrong_permit {
                    reader.decide_body(
                        frame,
                        BodyDecision::Retain(BodyPermit::new(old_frame, old_header).unwrap()),
                    )
                } else {
                    reader.decide_body(
                        old_frame,
                        BodyDecision::Discard(DiscardReason::NoDestination),
                    )
                };
                assert!(matches!(result, Err(FrameError::InvalidState(_))));
                assert_eq!(input.consumed, 24);
            }
        }
    }
}

#[test]
fn phase_changes_and_unprocessed_header_events_at_eof_are_internal_errors() {
    let bytes = encoded(b"hdr", b"");
    let mut reader = FrameReader::new();
    let mut partial = input(&bytes[..1]);
    assert!(matches!(
        reader.step(&mut partial, &wire(), work(64)).unwrap(),
        FrameReadStep::NeedInput { .. }
    ));
    let changed = FrameWireContext::new(0, false, false).unwrap();
    assert!(matches!(
        reader.step(&mut input(&bytes[1..]), &changed, work(64)),
        Err(FrameError::InvalidState(_))
    ));
    let mut reader = FrameReader::new();
    let mut input = input(&bytes);
    let FrameReadStep::FixedHeader { frame, header } =
        reader.step(&mut input, &wire(), work(64)).unwrap()
    else {
        panic!("missing frame")
    };
    reader
        .admit_headers(frame, HeaderPermit::new(frame, header, 64).unwrap())
        .unwrap();
    assert!(matches!(
        reader
            .step(&mut input, &wire(), WorkBudget::new(64, 1).unwrap())
            .unwrap(),
        FrameReadStep::Yield
    ));
    assert!(input.remaining().is_empty());
    assert!(matches!(
        reader.finish_eof(&input),
        Err(FrameError::InvalidState(_))
    ));
}

#[test]
fn native_width_body_rejects_before_allocation_and_integrity_errors_do_not_resynchronize() {
    let header = FrameHeader::for_send(0, u32::MAX as usize - 12, 0, &wire()).unwrap();
    let mut reader = FrameReader::new();
    let mut fixed = input(&header.encode());
    let FrameReadStep::FixedHeader { frame, header } =
        reader.step(&mut fixed, &wire(), work(64)).unwrap()
    else {
        panic!("missing frame")
    };
    assert!(matches!(
        HeaderPermit::new(frame, header, 1024),
        Err(FrameError::SizeLimit { .. })
    ));
    for corrupt_fixed in [false, true] {
        let mut bad = encoded(b"", b"bad");
        let index = if corrupt_fixed { 5 } else { bad.len() - 1 };
        bad[index] ^= 0xff;
        let bytes = [bad, encoded(b"", b"good")].concat();
        let mut input = input(&bytes);
        let mut reader = FrameReader::new();
        let error = if corrupt_fixed {
            reader.step(&mut input, &wire(), work(128)).unwrap_err()
        } else {
            let FrameReadStep::FixedHeader { frame, header } =
                reader.step(&mut input, &wire(), work(128)).unwrap()
            else {
                panic!("missing frame")
            };
            reader
                .admit_headers(frame, HeaderPermit::new(frame, header, 128).unwrap())
                .unwrap();
            assert!(matches!(
                reader.step(&mut input, &wire(), work(128)).unwrap(),
                FrameReadStep::Headers(_)
            ));
            reader
                .decide_body(
                    frame,
                    BodyDecision::Retain(BodyPermit::new(frame, header).unwrap()),
                )
                .unwrap();
            reader.step(&mut input, &wire(), work(128)).unwrap_err()
        };
        assert!(matches!(
            error,
            FrameError::HeaderChecksumMismatch { .. } | FrameError::BodyChecksumMismatch { .. }
        ));
        let consumed = input.consumed;
        assert_eq!(
            reader.step(&mut input, &wire(), work(128)).unwrap_err(),
            error
        );
        assert_eq!(input.consumed, consumed);
        assert_eq!(reader.finish_eof(&input), Err(error));
    }
}
