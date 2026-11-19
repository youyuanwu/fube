use super::*;
use crate::{header, ActorId, Error, HeaderId, HeaderRecord};
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
};

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn run<T>(future: impl Future<Output = T> + Send) -> (T, usize) {
    let wake = Arc::new(WakeCount::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    for polls in 1..10_000 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return (value, polls);
        }
        assert_eq!(wake.0.load(Ordering::Relaxed), polls);
    }
    panic!("preparation failed to finish");
}

fn wire() -> FrameWireContext {
    FrameWireContext::new(0, true, true).unwrap()
}
fn limits(quantum: usize) -> FrameWriteLimits {
    FrameWriteLimits {
        headers: HeaderDecodeContext::native(),
        max_frame_bytes: 1024 * 1024,
        work_bytes: quantum,
    }
}

fn message() -> Message {
    Message::builder()
        .append_header(header::actor(ActorId::new(70000).unwrap()).unwrap())
        .append_header(header::action("one").unwrap())
        .append_header(header::action("two").unwrap())
        .append_header(
            HeaderRecord::from_encoded(HeaderId::new(0x9001).unwrap(), vec![0xff]).unwrap(),
        )
        .body(
            Body::from_segments(vec![
                Body::from_bytes(b"first".to_vec()).unwrap(),
                Body::empty(),
                Body::from_bytes(b"second".to_vec()).unwrap(),
            ])
            .unwrap(),
        )
        .build()
        .unwrap()
}

fn payload(message: &Message) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in message.headers().raw_records() {
        bytes.extend_from_slice(&record.id().get().to_le_bytes());
        bytes.extend_from_slice(&(record.value().len() as u16).to_le_bytes());
        bytes.extend_from_slice(record.value());
    }
    for segment in message.body().segments() {
        bytes.extend_from_slice(segment);
    }
    bytes
}

fn collect(mut cursor: FrameCursor, scratch: usize, quantum: usize) -> Vec<u8> {
    let mut output = Vec::new();
    while !cursor.is_complete() {
        let mut slices = vec![IoSlice::new(&[]); scratch];
        let count = cursor.io_slices(&mut slices);
        assert!(count > 0);
        assert!(slices[..count].iter().all(|slice| !slice.is_empty()));
        let available: usize = slices[..count].iter().map(|slice| slice.len()).sum();
        let written = available.min(quantum);
        output.extend(
            slices[..count]
                .iter()
                .flat_map(|slice| slice.iter().copied())
                .take(written),
        );
        drop(slices);
        cursor.advance(written).unwrap();
    }
    assert_eq!(cursor.io_slices(&mut [IoSlice::new(&[])]), 0);
    output
}

#[test]
fn scalar_and_vectored_partial_advancement_preserve_exact_native_output() {
    let message = message();
    let expected = payload(&message);
    let length = 12 + expected.len();
    for scratch in [1, 2, 8] {
        for quantum in 1..=length {
            let frame = run(PreparedFrame::prepare(message.clone(), wire(), limits(7)))
                .0
                .unwrap();
            assert_eq!(frame.wire(), wire());
            assert_eq!(frame.encoded_len(), length);
            let bytes = collect(frame.into_cursor(), scratch, quantum);
            assert_eq!(&bytes[12..], expected);
            let header = FrameHeader::decode(bytes[..12].try_into().unwrap(), &wire()).unwrap();
            let mut crc = Crc32::new();
            crc.update(&bytes[12..]);
            header.verify_body_crc(&crc).unwrap();
            assert_eq!(header.body_len() as usize, message.body().len());
        }
    }
}

#[test]
fn body_segments_stay_shared_and_cursor_overadvancement_is_atomic() {
    let message = message();
    let pointers: Vec<_> = message
        .body()
        .segments()
        .map(|bytes| bytes.as_ptr())
        .collect();
    let frame = run(PreparedFrame::prepare(message, wire(), limits(3)))
        .0
        .unwrap();
    let mut cursor = frame.into_cursor();
    assert_eq!(cursor.io_slices(&mut []), 0);
    assert!(!cursor.is_complete());
    let original = cursor.remaining();
    assert!(cursor.advance(original + 1).is_err());
    assert_eq!(cursor.remaining(), original);
    cursor.advance(0).unwrap();
    cursor
        .advance(12 + cursor.frame.header.headers_len() as usize)
        .unwrap();
    let mut slices = [IoSlice::new(&[]); 8];
    let count = cursor.io_slices(&mut slices);
    assert_eq!(
        slices[..count]
            .iter()
            .map(|bytes| bytes.as_ptr())
            .collect::<Vec<_>>(),
        pointers
    );
    cursor.advance(cursor.remaining()).unwrap();
    assert!(cursor.is_complete());
    assert!(cursor.advance(1).is_err());
}

#[test]
fn preparation_yields_within_large_segments_and_cancellation_preserves_aliases() {
    for polls in [0, 1, 2, 19] {
        let message = Message::builder()
            .body(Body::from_bytes(vec![7; 4096]).unwrap())
            .build()
            .unwrap();
        let alias = message.body().clone();
        let mut future = Box::pin(PreparedFrame::prepare(message, wire(), limits(17)));
        let waker = Waker::from(Arc::new(WakeCount::default()));
        let mut cx = Context::from_waker(&waker);
        for _ in 0..polls {
            assert!(future.as_mut().poll(&mut cx).is_pending());
        }
        drop(future);
        assert_eq!(alias.segments().next().unwrap(), &[7; 4096]);
    }
    let message = Message::builder()
        .body(Body::from_bytes(vec![7; 4096]).unwrap())
        .build()
        .unwrap();
    let (result, polls) = run(PreparedFrame::prepare(message, wire(), limits(17)));
    result.unwrap();
    assert_eq!(polls, 4096usize.div_ceil(17));
}

#[test]
fn configured_frame_and_header_limits_reject_before_output() {
    let message = message();
    let mut policy = limits(64);
    policy.max_frame_bytes = 12 + payload(&message).len() - 1;
    assert!(matches!(
        run(PreparedFrame::prepare(message.clone(), wire(), policy)).0,
        Err(FrameError::SizeLimit { .. })
    ));
    policy = limits(64);
    policy.headers = HeaderDecodeContext::new(crate::wire::headers::HeaderDecodeLimits {
        max_bytes: 0,
        max_records: 0,
        max_depth: 0,
        max_work_bytes: 0,
    })
    .unwrap();
    assert!(matches!(
        run(PreparedFrame::prepare(message, wire(), policy)).0,
        Err(FrameError::Storage(Error::ResourceLimit(_)))
    ));
}

#[test]
fn empty_frames_and_checksum_profiles_preserve_native_encoding() {
    for wire in [wire(), FrameWireContext::new(0, false, false).unwrap()] {
        let (frame, polls) = run(PreparedFrame::prepare(
            Message::builder().build().unwrap(),
            wire,
            limits(1),
        ));
        assert_eq!(polls, 1);
        let bytes = collect(frame.unwrap().into_cursor(), 1, 1);
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes[5] != 0, wire == self::wire());
        assert_eq!(&bytes[8..12], &[0; 4]);
    }
}

#[test]
fn forwarded_physical_records_round_trip_through_writer_and_gated_reader() {
    use crate::wire::reader::{
        BodyDecision, BodyPermit, FrameReadStep, FrameReader, HeaderPermit, ReceiveInput,
        WorkBudget,
    };
    let raw = vec![0, 0x80, 1, 0, 0xff, 1, 0x90, 2, 0, 0xab, 0xcd];
    let headers = crate::Headers::from_received(
        Segment::from_bytes(raw.clone()),
        &HeaderDecodeContext::native(),
    )
    .unwrap();
    let mut builder = Message::builder();
    for record in headers.raw_records() {
        builder = builder.append_header(record.to_owned());
    }
    let message = builder
        .body(Body::from_bytes(b"payload".to_vec()).unwrap())
        .build()
        .unwrap();
    drop(headers);
    let prepared = run(PreparedFrame::prepare(message, wire(), limits(1)))
        .0
        .unwrap();
    let bytes = collect(prepared.into_cursor(), 2, 3);
    assert_eq!(&bytes[12..12 + raw.len()], raw);
    let mut input = ReceiveInput::new(Segment::from_bytes(bytes));
    let mut reader = FrameReader::new();
    let work = WorkBudget::new(1024, 4).unwrap();
    let FrameReadStep::FixedHeader { frame, header } =
        reader.step(&mut input, &wire(), work).unwrap()
    else {
        panic!("missing frame")
    };
    reader
        .admit_headers(frame, HeaderPermit::new(frame, header, 1024).unwrap())
        .unwrap();
    let FrameReadStep::Headers(prefix) = reader.step(&mut input, &wire(), work).unwrap() else {
        panic!("missing headers")
    };
    let decoded =
        crate::wire::headers::Headers::decode(prefix.area.bytes(), &HeaderDecodeContext::native())
            .unwrap();
    assert_eq!(decoded.raw_records().count(), 2);
    assert_eq!(decoded.records().count(), 1);
    reader
        .decide_body(
            frame,
            BodyDecision::Retain(BodyPermit::new(frame, header).unwrap()),
        )
        .unwrap();
    let FrameReadStep::Complete(complete) = reader.step(&mut input, &wire(), work).unwrap() else {
        panic!("missing body")
    };
    assert_eq!(complete.frame, prefix.frame);
    assert_eq!(complete.body.segments().next().unwrap(), b"payload");
    reader.finish_eof(&input).unwrap();
}
