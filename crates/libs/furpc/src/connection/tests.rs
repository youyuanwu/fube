use super::{
    driver::{ConnectionDriver, ConnectionEvent, DropReason},
    *,
};
use crate::{
    header,
    wire::{
        frame::{FrameHeader, FIXED_HEADER_LEN},
        writer::{FrameWriteLimits, PreparedFrame},
    },
    ActorId, Body, HeaderRecord, MessageId,
};
use std::{collections::VecDeque, future::poll_fn, io::IoSlice, task::Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

fn config() -> ConnectionConfig {
    ConnectionConfig {
        wire: FrameWireContext::new(0, true, true).unwrap(),
        headers: HeaderDecodeContext::native(),
        max_receive_frame_bytes: 64 * 1024,
        max_send_frame_bytes: Some(64 * 1024),
        send_queue_bytes: 128 * 1024,
        max_pending_sends: 16,
        work_bytes: 17,
        work_steps: 8,
        frame_timeout: Duration::from_secs(20),
        discard_timeout: Duration::from_secs(5),
        max_discard_bytes: 64 * 1024,
        write_timeout: Duration::from_secs(30),
        drain_timeout: Duration::from_secs(10),
        ingress: Ingress::Raw,
    }
}

fn local() -> LocalInstance {
    LocalInstance {
        address: "127.0.0.1:1234".into(),
        instance: 7,
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

fn message(bytes: &[u8]) -> Message {
    Message::builder()
        .body(Body::from_bytes(bytes.to_vec()).unwrap())
        .build()
        .unwrap()
}

async fn encoded(message: Message, config: ConnectionConfig) -> Vec<u8> {
    let mut cursor = PreparedFrame::prepare(
        message,
        config.wire,
        FrameWriteLimits {
            headers: config.headers,
            max_frame_bytes: u32::MAX as usize,
            work_bytes: config.work_bytes,
        },
    )
    .await
    .unwrap()
    .into_cursor();
    let mut bytes = Vec::new();
    while !cursor.is_complete() {
        let mut slices = [IoSlice::new(&[]); 16];
        let count = cursor.io_slices(&mut slices);
        let mut written = 0;
        for slice in &slices[..count] {
            bytes.extend_from_slice(slice);
            written += slice.len();
        }
        cursor.advance(written).unwrap();
    }
    bytes
}

#[derive(Clone, Copy)]
enum WriteAction {
    Pending,
    Interrupted,
    Error(io::ErrorKind),
    Overreport,
    Zero,
}

struct IoState {
    input: VecDeque<u8>,
    output: Vec<u8>,
    read_limit: usize,
    write_limit: usize,
    read_interrupt: bool,
    read_error: Option<io::ErrorKind>,
    write_actions: VecDeque<WriteAction>,
    eof: bool,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    shutdown_pending: bool,
    shutdown_error: Option<io::ErrorKind>,
    shutdowns: usize,
    dropped: bool,
    bytes_read: usize,
    max_offered: usize,
}

#[derive(Clone)]
struct Probe(Arc<Mutex<IoState>>);

impl Probe {
    fn feed(&self, bytes: &[u8]) {
        let mut state = self.0.lock().unwrap();
        state.input.extend(bytes);
        let wake = state.read_waker.take();
        drop(state);
        wake_if_some(wake);
    }

    fn write_limit(&self, limit: usize) {
        let mut state = self.0.lock().unwrap();
        state.write_limit = limit;
        let wake = state.write_waker.take();
        drop(state);
        wake_if_some(wake);
    }
}

struct TestIo {
    probe: Probe,
    vectored: bool,
}

impl TestIo {
    fn new(vectored: bool) -> (Self, Probe) {
        let probe = Probe(Arc::new(Mutex::new(IoState {
            input: VecDeque::new(),
            output: Vec::new(),
            read_limit: usize::MAX,
            write_limit: usize::MAX,
            read_interrupt: false,
            read_error: None,
            write_actions: VecDeque::new(),
            eof: false,
            read_waker: None,
            write_waker: None,
            shutdown_pending: false,
            shutdown_error: None,
            shutdowns: 0,
            dropped: false,
            bytes_read: 0,
            max_offered: 0,
        })));
        (
            Self {
                probe: probe.clone(),
                vectored,
            },
            probe,
        )
    }

    fn write(&self, cx: &mut Context<'_>, slices: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let mut state = self.probe.0.lock().unwrap();
        let offered: usize = slices.iter().map(|slice| slice.len()).sum();
        state.max_offered = state.max_offered.max(offered);
        match state.write_actions.pop_front() {
            Some(WriteAction::Pending) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Some(WriteAction::Interrupted) => {
                return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
            }
            Some(WriteAction::Error(kind)) => return Poll::Ready(Err(kind.into())),
            Some(WriteAction::Overreport) => return Poll::Ready(Ok(offered + 1)),
            Some(WriteAction::Zero) => return Poll::Ready(Ok(0)),
            None => {}
        }
        if state.write_limit == 0 {
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let count = offered.min(state.write_limit);
        state.output.extend(
            slices
                .iter()
                .flat_map(|slice| slice.iter().copied())
                .take(count),
        );
        Poll::Ready(Ok(count))
    }
}

impl Drop for TestIo {
    fn drop(&mut self) {
        self.probe.0.lock().unwrap().dropped = true;
    }
}

impl AsyncRead for TestIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.probe.0.lock().unwrap();
        if std::mem::take(&mut state.read_interrupt) {
            return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
        }
        if let Some(kind) = state.read_error.take() {
            return Poll::Ready(Err(kind.into()));
        }
        if state.input.is_empty() && !state.eof {
            state.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let count = buffer
            .remaining()
            .min(state.input.len())
            .min(state.read_limit);
        for _ in 0..count {
            buffer.put_slice(&[state.input.pop_front().unwrap()]);
        }
        state.bytes_read += count;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TestIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write(cx, &[IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write(cx, slices)
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.probe.0.lock().unwrap();
        if let Some(kind) = state.shutdown_error {
            Poll::Ready(Err(kind.into()))
        } else if state.shutdown_pending {
            state.write_waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            state.shutdowns += 1;
            Poll::Ready(Ok(()))
        }
    }
}

fn poll<I: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    driver: &mut ConnectionDriver<I>,
) -> Poll<ConnectionResult<Option<ConnectionEvent>>> {
    driver.poll_event(&mut Context::from_waker(Waker::noop()))
}

fn ready<I: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    driver: &mut ConnectionDriver<I>,
) -> ConnectionResult<Option<ConnectionEvent>> {
    for _ in 0..10_000 {
        if let Poll::Ready(result) = poll(driver) {
            return result;
        }
    }
    panic!("driver did not produce an event");
}

fn pump(driver: &mut ConnectionDriver<TestIo>) {
    for _ in 0..128 {
        assert!(poll(driver).is_pending(), "unexpected driver event");
    }
}

fn receipt(receipt: &mut SendReceipt) -> ConnectionResult<()> {
    match Pin::new(receipt).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("unsettled write receipt"),
    }
}

async fn opened(
    config: ConnectionConfig,
    vectored: bool,
) -> (ConnectionHandle, ConnectionDriver<TestIo>, Probe) {
    let (io, probe) = TestIo::new(vectored);
    let hello = native::hello(local(), Role::Accepted, &config).unwrap();
    probe.feed(&encoded(hello, config).await);
    let (handle, mut driver) =
        ConnectionDriver::new(io, Role::Initiating, local(), config, deadline()).unwrap();
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Opened(_))
    ));
    {
        let mut state = probe.0.lock().unwrap();
        state.output.clear();
        state.bytes_read = 0;
        state.max_offered = 0;
    }
    assert_eq!(handle.phase(), Phase::Open);
    assert_eq!(handle.queued_bytes(), 0);
    (handle, driver, probe)
}

#[tokio::test(start_paused = true)]
async fn scalar_and_vectored_io_preserve_segmented_frames_and_frame_queue_accounting() {
    for vectored in [false, true] {
        for quantum in [1, 3, 17, usize::MAX] {
            let config = config();
            let (handle, mut driver, probe) = opened(config, vectored).await;
            probe.write_limit(quantum);
            probe
                .0
                .lock()
                .unwrap()
                .write_actions
                .extend([WriteAction::Pending, WriteAction::Interrupted]);
            let body = Body::from_segments(vec![
                Body::from_bytes(b"first".to_vec()).unwrap(),
                Body::from_bytes(b"second".to_vec()).unwrap(),
            ])
            .unwrap();
            let alias = body.clone();
            let first = Message::builder().body(body).build().unwrap();
            let second = message(b"next");
            let mut expected = encoded(first.clone(), config).await;
            expected.extend(encoded(second.clone(), config).await);
            let mut one = handle.send(first, deadline()).unwrap();
            let mut two = handle.send(second, deadline()).unwrap();
            assert_eq!(handle.queued_bytes(), expected.len());
            pump(&mut driver);
            receipt(&mut one).unwrap();
            receipt(&mut two).unwrap();
            assert_eq!(handle.queued_bytes(), 0);
            assert_eq!(alias.len(), 11);
            let state = probe.0.lock().unwrap();
            assert_eq!(state.output, expected);
            assert!(state.max_offered <= config.work_bytes);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_commit_retracts_but_observer_drop_after_commit_keeps_suffix() {
    let mut config = config();
    config.work_steps = 1;
    let (handle, mut driver, probe) = opened(config, true).await;
    probe.write_limit(0);
    let first = message(&[7; 4096]);
    let alias = first.body().clone();
    let mut cancelled = handle.send(first, deadline()).unwrap();
    for _ in 0..3 {
        assert!(poll(&mut driver).is_pending());
    }
    assert!(cancelled.cancel());
    pump(&mut driver);
    assert!(matches!(
        &*receipt(&mut cancelled).unwrap_err(),
        ConnectionError::Cancelled
    ));
    assert!(probe.0.lock().unwrap().output.is_empty());
    assert_eq!(alias.len(), 4096);
    assert_eq!(handle.queued_bytes(), 0);

    let mut pending_write = handle.send(message(b"pending"), deadline()).unwrap();
    pump(&mut driver);
    assert!(pending_write.cancel());
    pump(&mut driver);
    assert!(matches!(
        &*receipt(&mut pending_write).unwrap_err(),
        ConnectionError::Cancelled
    ));

    probe.write_limit(1);
    let first = message(b"must retain this suffix");
    let second = message(b"cannot interleave");
    let mut expected = encoded(first.clone(), config).await;
    let first_len = expected.len();
    expected.extend(encoded(second.clone(), config).await);
    let committed = handle.send(first, deadline()).unwrap();
    while !committed.operation.lock().unwrap().committed {
        assert!(poll(&mut driver).is_pending());
    }
    assert_eq!(handle.queued_bytes(), first_len);
    assert!(!committed.cancel());
    drop(committed);
    let mut following = handle.send(second, deadline()).unwrap();
    pump(&mut driver);
    receipt(&mut following).unwrap();
    assert_eq!(probe.0.lock().unwrap().output, expected);
    assert_eq!(handle.queued_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn native_queue_limit_zero_empty_queue_exception_and_operation_count_are_distinct() {
    for disabled in [false, true] {
        let mut config = config();
        config.send_queue_bytes = 16;
        config.max_send_frame_bytes = (!disabled).then_some(64 * 1024);
        let (handle, mut driver, _) = opened(config, true).await;
        let result = handle.send(message(&[1; 32]), deadline());
        if disabled {
            let mut accepted = result.unwrap();
            assert_eq!(handle.queued_bytes(), 44);
            assert!(handle.send(message(b""), deadline()).is_err());
            pump(&mut driver);
            receipt(&mut accepted).unwrap();
            assert_eq!(handle.queued_bytes(), 0);
        } else {
            assert!(result.is_err());
            assert_eq!(handle.queued_bytes(), 0);
        }
    }
    let mut config = config();
    config.send_queue_bytes = 0;
    config.max_pending_sends = 2;
    let (handle, mut driver, _) = opened(config, true).await;
    let shared = message(&[9; 100]);
    let alias = shared.body().clone();
    let mut one = handle.send(shared.clone(), deadline()).unwrap();
    let mut two = handle.clone().send(shared, deadline()).unwrap();
    assert_eq!(handle.queued_bytes(), 224);
    assert!(handle.send(message(b""), deadline()).is_err());
    assert!(two.cancel());
    pump(&mut driver);
    receipt(&mut one).unwrap();
    assert!(matches!(
        &*receipt(&mut two).unwrap_err(),
        ConnectionError::Cancelled
    ));
    assert_eq!(handle.queued_bytes(), 0);
    assert_eq!(alias.len(), 100);
}

#[tokio::test(start_paused = true)]
async fn header_classification_precedes_body_and_publishes_only_after_integrity() {
    let config = config();
    let (_handle, mut driver, probe) = opened(config, true).await;
    let frame = encoded(message(b"verified only at the end"), config).await;
    probe.0.lock().unwrap().read_limit = 1;
    probe.0.lock().unwrap().read_interrupt = true;
    probe.feed(&frame[..frame.len() - 1]);
    pump(&mut driver);
    probe.feed(&frame[frame.len() - 1..]);
    let Some(ConnectionEvent::Message(received)) = ready(&mut driver).unwrap() else {
        panic!("missing verified application message");
    };
    assert!(received.headers().first_actor().is_none());
    assert_eq!(
        received
            .body()
            .segments()
            .flatten()
            .copied()
            .collect::<Vec<_>>(),
        b"verified only at the end"
    );
}

#[tokio::test(start_paused = true)]
async fn unmatched_and_special_replies_never_fall_through_raw_ingress() {
    let config = config();
    let (_handle, mut driver, probe) = opened(config, true).await;
    #[derive(serde::Serialize)]
    struct Flag {
        value: bool,
    }
    for (record, expected) in [
        (
            HeaderRecord::native(
                header::RELATES_TO,
                &MessageId {
                    guid: [1; 16],
                    index: 1,
                },
            )
            .unwrap(),
            DropReason::UnmatchedReply,
        ),
        (
            HeaderRecord::native(header::UNCORRELATED_REPLY, &Flag { value: true }).unwrap(),
            DropReason::SpecialDuplexUnavailable,
        ),
    ] {
        let reply = Message::builder()
            .append_header(record)
            .body(Body::from_bytes(vec![1; 50]).unwrap())
            .build()
            .unwrap();
        probe.feed(&encoded(reply, config).await);
        assert!(matches!(ready(&mut driver).unwrap(),
            Some(ConnectionEvent::Discarded(reason)) if reason == expected));
    }
    probe.feed(&encoded(message(b"ordinary"), config).await);
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Message(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn discard_is_diagnosed_checks_crc_and_does_not_consume_the_following_frame() {
    let mut config = config();
    config.ingress = Ingress::None;
    let (_handle, mut driver, probe) = opened(config, true).await;
    let frame = encoded(message(b"discard"), config).await;
    probe.feed(&frame);
    probe.feed(&frame);
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Discarded(DropReason::NoDestination))
    ));
    assert_eq!(probe.0.lock().unwrap().bytes_read, frame.len());
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Discarded(DropReason::NoDestination))
    ));
    let mut corrupt = frame;
    *corrupt.last_mut().unwrap() ^= 1;
    probe.feed(&corrupt);
    let error = ready(&mut driver).unwrap_err();
    assert!(matches!(
        &*error,
        ConnectionError::Frame(FrameError::BodyChecksumMismatch { .. })
    ));
    assert!(probe.0.lock().unwrap().dropped);
    assert!(Arc::ptr_eq(&error, &ready(&mut driver).unwrap_err()));
}

#[tokio::test(start_paused = true)]
async fn size_and_discard_admission_reject_before_reading_declared_body() {
    for discard in [false, true] {
        let mut config = config();
        config.max_receive_frame_bytes = 256;
        config.max_discard_bytes = 4;
        config.ingress = if discard { Ingress::None } else { Ingress::Raw };
        let (handle, mut driver, probe) = opened(config, true).await;
        let body_len = if discard { 100 } else { 1000 };
        let header = FrameHeader::for_send(0, body_len, 0, &config.wire).unwrap();
        probe.feed(&header.encode());
        let error = ready(&mut driver).unwrap_err();
        assert!(matches!(
            &*error,
            ConnectionError::Frame(FrameError::SizeLimit { .. })
                | ConnectionError::Native(Error::ResourceLimit("discard body bytes"))
        ));
        assert_eq!(probe.0.lock().unwrap().bytes_read, FIXED_HEADER_LEN);
        assert_eq!(handle.phase(), Phase::Closed);
    }
}

#[tokio::test(start_paused = true)]
async fn partial_frame_and_discard_deadlines_are_not_renewed_by_progress() {
    for discard in [false, true] {
        let mut config = config();
        config.ingress = if discard { Ingress::None } else { Ingress::Raw };
        let (_handle, mut driver, probe) = opened(config, true).await;
        let frame = encoded(message(&[1; 30]), config).await;
        let initial = if discard { 13 } else { 1 };
        probe.feed(&frame[..initial]);
        pump(&mut driver);
        let duration = if discard {
            config.discard_timeout
        } else {
            config.frame_timeout
        };
        tokio::time::advance(duration - Duration::from_secs(1)).await;
        probe.feed(&frame[initial..initial + 1]);
        pump(&mut driver);
        tokio::time::advance(Duration::from_secs(2)).await;
        let error = ready(&mut driver).unwrap_err();
        assert!(matches!(&*error, ConnectionError::Timeout(phase)
            if *phase == if discard { "discard" } else { "partial frame" }));
    }
}

#[tokio::test(start_paused = true)]
async fn precommit_expiry_behind_committed_output_does_not_cancel_its_suffix() {
    let config = config();
    let (handle, mut driver, probe) = opened(config, true).await;
    let mut first = handle
        .send(message(&[1; 50]), Instant::now() + Duration::from_secs(1))
        .unwrap();
    probe.write_limit(1);
    while !first.operation.lock().unwrap().committed {
        assert!(poll(&mut driver).is_pending());
    }
    probe.write_limit(0);
    let mut queued = handle
        .send(message(b"expire"), Instant::now() + Duration::from_secs(2))
        .unwrap();
    pump(&mut driver);
    tokio::time::advance(Duration::from_secs(3)).await;
    pump(&mut driver);
    assert!(matches!(
        &*receipt(&mut queued).unwrap_err(),
        ConnectionError::Timeout("send admission")
    ));
    assert_eq!(handle.queued_bytes(), 62);
    probe.write_limit(usize::MAX);
    pump(&mut driver);
    receipt(&mut first).unwrap();
    assert_eq!(handle.queued_bytes(), 0);
}

#[tokio::test(start_paused = true)]
async fn committed_write_timeout_preserves_first_cause_and_settles_all_receipts() {
    let mut config = config();
    config.work_steps = 1;
    let (handle, mut driver, probe) = opened(config, true).await;
    probe.write_limit(1);
    let mut first = handle.send(message(b"committed"), deadline()).unwrap();
    let mut second = handle.send(message(b"queued"), deadline()).unwrap();
    while !first.operation.lock().unwrap().committed {
        assert!(poll(&mut driver).is_pending());
    }
    probe.write_limit(0);
    pump(&mut driver);
    tokio::time::advance(config.write_timeout).await;
    let error = ready(&mut driver).unwrap_err();
    assert!(matches!(
        &*error,
        ConnectionError::Timeout("committed write")
    ));
    assert!(Arc::ptr_eq(&error, &receipt(&mut first).unwrap_err()));
    assert!(Arc::ptr_eq(&error, &receipt(&mut second).unwrap_err()));
    handle.abort(Arc::new(ConnectionError::Closed));
    assert!(Arc::ptr_eq(&error, &ready(&mut driver).unwrap_err()));
    assert_eq!(handle.queued_bytes(), 0);
    assert!(probe.0.lock().unwrap().dropped);
}

#[tokio::test(start_paused = true)]
async fn drain_flushes_admitted_frames_rejects_new_sends_and_has_one_deadline() {
    for expires in [false, true] {
        let config = config();
        let (handle, mut driver, probe) = opened(config, true).await;
        let mut accepted = handle.send(message(b"admitted"), deadline()).unwrap();
        handle.drain().unwrap();
        assert!(handle.send(message(b"late"), deadline()).is_err());
        if expires {
            probe.write_limit(0);
            pump(&mut driver);
            tokio::time::advance(config.drain_timeout - Duration::from_secs(1)).await;
            handle.drain().unwrap();
            tokio::time::advance(Duration::from_secs(2)).await;
            let error = ready(&mut driver).unwrap_err();
            assert!(matches!(&*error, ConnectionError::Timeout("drain")));
            assert!(Arc::ptr_eq(&error, &receipt(&mut accepted).unwrap_err()));
        } else {
            assert!(ready(&mut driver).unwrap().is_none());
            receipt(&mut accepted).unwrap();
            assert_eq!(probe.0.lock().unwrap().shutdowns, 1);
        }
        assert_eq!(handle.phase(), Phase::Closed);
        assert!(probe.0.lock().unwrap().dropped);
    }
}

#[tokio::test(start_paused = true)]
async fn owner_drop_driver_drop_abort_and_io_errors_release_owned_work() {
    for mode in 0..8 {
        let config = config();
        let (handle, mut driver, probe) = opened(config, true).await;
        let mut pending = handle.send(message(b"owned"), deadline()).unwrap();
        match mode {
            0 => {
                drop(driver);
                drop(handle);
            }
            1 => {
                drop(handle);
                assert!(ready(&mut driver).is_err());
            }
            2 => {
                let first = Arc::new(ConnectionError::Unsupported("test abort"));
                handle.abort(first.clone());
                handle.abort(Arc::new(ConnectionError::Closed));
                assert!(Arc::ptr_eq(&first, &ready(&mut driver).unwrap_err()));
            }
            3 | 4 => {
                probe
                    .0
                    .lock()
                    .unwrap()
                    .write_actions
                    .push_back(if mode == 3 {
                        WriteAction::Error(io::ErrorKind::BrokenPipe)
                    } else {
                        WriteAction::Overreport
                    });
                let error = ready(&mut driver).unwrap_err();
                if mode == 3 {
                    assert!(
                        matches!(&*error, ConnectionError::Io { phase: "write", source }
                        if source.kind() == io::ErrorKind::BrokenPipe)
                    );
                } else {
                    assert!(matches!(
                        &*error,
                        ConnectionError::Native(Error::Internal(_))
                    ));
                }
            }
            5 => {
                probe.0.lock().unwrap().read_error = Some(io::ErrorKind::ConnectionReset);
                let error = ready(&mut driver).unwrap_err();
                assert!(
                    matches!(&*error, ConnectionError::Io { phase: "read", source }
                    if source.kind() == io::ErrorKind::ConnectionReset)
                );
            }
            6 => {
                probe.write_limit(0);
                handle.drain().unwrap();
                drop(pending);
                probe.0.lock().unwrap().shutdown_error = Some(io::ErrorKind::ConnectionReset);
                assert!(matches!(
                    &*ready(&mut driver).unwrap_err(),
                    ConnectionError::Io {
                        phase: "shutdown",
                        ..
                    }
                ));
                assert!(probe.0.lock().unwrap().dropped);
                continue;
            }
            7 => {
                probe
                    .0
                    .lock()
                    .unwrap()
                    .write_actions
                    .push_back(WriteAction::Zero);
                assert!(matches!(&*ready(&mut driver).unwrap_err(),
                    ConnectionError::Io { phase: "write", source }
                    if source.kind() == io::ErrorKind::WriteZero));
            }
            _ => unreachable!(),
        }
        assert!(receipt(&mut pending).is_err());
        assert!(probe.0.lock().unwrap().dropped);
    }
}

#[tokio::test(start_paused = true)]
async fn eof_is_clean_only_at_a_frame_boundary_and_does_not_hide_ready_messages() {
    let config = config();
    let frame = encoded(
        Message::builder()
            .append_header(header::action("header").unwrap())
            .body(Body::from_bytes(b"body".to_vec()).unwrap())
            .build()
            .unwrap(),
        config,
    )
    .await;
    for end in [0, 1, 11, 12, 13, frame.len() - 1, frame.len()] {
        let (_handle, mut driver, probe) = opened(config, true).await;
        probe.feed(&frame[..end]);
        probe.0.lock().unwrap().eof = true;
        if end == frame.len() {
            assert!(matches!(
                ready(&mut driver).unwrap(),
                Some(ConnectionEvent::Message(_))
            ));
        }
        if end == 0 || end == frame.len() {
            assert!(ready(&mut driver).unwrap().is_none());
        } else {
            assert!(matches!(
                &*ready(&mut driver).unwrap_err(),
                ConnectionError::Frame(FrameError::Truncated { .. })
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn establishment_release_requires_both_verified_hello_and_complete_local_write() {
    let config = config();
    let (io, probe) = TestIo::new(true);
    probe.write_limit(0);
    probe.feed(
        &encoded(
            native::hello(local(), Role::Accepted, &config).unwrap(),
            config,
        )
        .await,
    );
    let (handle, mut driver) =
        ConnectionDriver::new(io, Role::Initiating, local(), config, deadline()).unwrap();
    pump(&mut driver);
    assert_eq!(handle.phase(), Phase::Opening);
    assert!(handle.send(message(b"too early"), deadline()).is_err());
    probe.write_limit(usize::MAX);
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Opened(_))
    ));

    let (io, probe) = TestIo::new(true);
    let until = Instant::now() + Duration::from_secs(2);
    let (_handle, mut driver) =
        ConnectionDriver::new(io, Role::Accepted, local(), config, until).unwrap();
    pump(&mut driver);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        &*ready(&mut driver).unwrap_err(),
        ConnectionError::Timeout("establishment")
    ));
    assert!(probe.0.lock().unwrap().dropped);
}

#[tokio::test(start_paused = true)]
async fn reads_progress_while_a_large_send_is_preparing_or_backpressured() {
    let config = config();
    let (handle, mut driver, probe) = opened(config, true).await;
    probe.write_limit(0);
    let mut outgoing = handle.send(message(&[1; 16 * 1024]), deadline()).unwrap();
    probe.feed(&encoded(message(b"incoming"), config).await);
    assert!(matches!(
        ready(&mut driver).unwrap(),
        Some(ConnectionEvent::Message(_))
    ));
    assert!(!outgoing.operation.lock().unwrap().committed);
    assert!(probe.0.lock().unwrap().output.is_empty());
    assert!(outgoing.cancel());
    pump(&mut driver);
    assert!(receipt(&mut outgoing).is_err());
}

#[tokio::test(start_paused = true)]
async fn controls_and_malformed_headers_cannot_enter_raw_ingress() {
    let config = config();
    for actor in [native::SEND_TARGET, native::SECURITY, native::TRANSPORT] {
        let (handle, mut driver, probe) = opened(config, true).await;
        let control = Message::builder()
            .append_header(header::actor(ActorId::new(actor).unwrap()).unwrap())
            .append_header(header::action("unsupported").unwrap())
            .append_header(
                HeaderRecord::native(
                    header::RELATES_TO,
                    &MessageId {
                        guid: [1; 16],
                        index: 1,
                    },
                )
                .unwrap(),
            )
            .body(Body::from_bytes(vec![1; 10]).unwrap())
            .build()
            .unwrap();
        assert!(handle.send(control.clone(), deadline()).is_err());
        probe.feed(&encoded(control, config).await);
        assert!(matches!(
            &*ready(&mut driver).unwrap_err(),
            ConnectionError::Unsupported(_)
        ));
        assert!(probe.0.lock().unwrap().input.len() >= 10);
    }

    let (_handle, mut driver, probe) = opened(config, true).await;
    let header = FrameHeader::for_send(5, 1000, 0, &config.wire).unwrap();
    probe.feed(&header.encode());
    probe.feed(&[0x01, 0x80, 1, 0, 0xff]);
    let error = ready(&mut driver).unwrap_err();
    assert!(matches!(
        &*error,
        ConnectionError::Native(Error::NativeHeader { id: 0x8001, .. })
    ));
    assert_eq!(probe.0.lock().unwrap().bytes_read, 17);
}

#[tokio::test(start_paused = true)]
async fn invalid_hello_and_wrong_provider_never_publish_an_open_connection() {
    let config = config();
    for case in 0..4 {
        let (io, probe) = TestIo::new(true);
        let mut frame = match case {
            0 => {
                encoded(
                    native::hello(local(), Role::Initiating, &config).unwrap(),
                    config,
                )
                .await
            }
            1 | 3 => {
                encoded(
                    native::hello(local(), Role::Accepted, &config).unwrap(),
                    config,
                )
                .await
            }
            _ => encoded(message(b"not a hello"), config).await,
        };
        if case == 1 {
            *frame.last_mut().unwrap() ^= 1;
        }
        if case == 3 {
            frame[4] = 1;
            frame[5] = 0;
        }
        probe.feed(&frame);
        let (handle, mut driver) =
            ConnectionDriver::new(io, Role::Initiating, local(), config, deadline()).unwrap();
        let error = ready(&mut driver).unwrap_err();
        match case {
            0 | 2 => assert!(matches!(
                &*error,
                ConnectionError::Native(Error::Protocol(_))
            )),
            1 => assert!(matches!(
                &*error,
                ConnectionError::Frame(FrameError::BodyChecksumMismatch { .. })
            )),
            3 => assert!(matches!(
                &*error,
                ConnectionError::Frame(FrameError::ProviderMismatch { .. })
            )),
            _ => unreachable!(),
        }
        assert_eq!(handle.phase(), Phase::Closed);
        assert!(probe.0.lock().unwrap().dropped);
    }
}

#[tokio::test(start_paused = true)]
async fn verified_listen_instance_updates_preserve_native_utf16_metadata() {
    let config = config();
    let (_handle, mut driver, probe) = opened(config, true).await;
    let hello = native::hello(
        LocalInstance {
            address: "x".into(),
            instance: 9,
        },
        Role::Accepted,
        &config,
    )
    .unwrap();
    let mut bytes = hello
        .body()
        .segments()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let offset = bytes.windows(2).position(|pair| pair == b"x\0").unwrap();
    bytes[offset..offset + 2].copy_from_slice(&0xd800u16.to_le_bytes());
    let builder = hello
        .headers()
        .raw_records()
        .fold(Message::builder(), |builder, record| {
            builder.append_header(record.to_owned())
        });
    let update = builder
        .body(Body::from_bytes(bytes).unwrap())
        .build()
        .unwrap();
    probe.feed(&encoded(update, config).await);
    assert!(matches!(ready(&mut driver).unwrap(),
        Some(ConnectionEvent::InstanceUpdated(instance))
        if instance.address == [0xd800] && instance.instance == 9));
}

#[tokio::test(start_paused = true)]
async fn pending_shutdown_and_partial_frame_waits_are_woken_by_driver_owned_timers() {
    let config = config();
    let (handle, mut driver, probe) = opened(config, true).await;
    handle.drain().unwrap();
    probe.0.lock().unwrap().shutdown_pending = true;
    let error = poll_fn(|cx| driver.poll_event(cx)).await.unwrap_err();
    assert!(matches!(&*error, ConnectionError::Timeout("drain")));
    assert!(probe.0.lock().unwrap().dropped);

    let (_handle, mut driver, probe) = opened(config, true).await;
    probe.feed(&[12]);
    let error = poll_fn(|cx| driver.poll_event(cx)).await.unwrap_err();
    assert!(matches!(&*error, ConnectionError::Timeout("partial frame")));
}

async fn exchange<I, J>(a: I, b: J)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    J: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let config = config();
    let (a_handle, mut a) =
        ConnectionDriver::new(a, Role::Initiating, local(), config, deadline()).unwrap();
    let (b_handle, mut b) =
        ConnectionDriver::new(b, Role::Accepted, local(), config, deadline()).unwrap();
    let (a_open, b_open) = tokio::join!(
        poll_fn(|cx| a.poll_event(cx)),
        poll_fn(|cx| b.poll_event(cx)),
    );
    assert!(
        matches!(a_open.unwrap(), Some(ConnectionEvent::Opened(instance)) if instance.nonce != [0;16])
    );
    assert!(
        matches!(b_open.unwrap(), Some(ConnectionEvent::Opened(instance)) if instance.nonce == [0;16])
    );
    let one = a_handle
        .send(message(b"initiating -> accepted"), deadline())
        .unwrap();
    let two = b_handle
        .send(message(b"accepted -> initiating"), deadline())
        .unwrap();
    let (a_message, b_message) = tokio::join!(
        poll_fn(|cx| a.poll_event(cx)),
        poll_fn(|cx| b.poll_event(cx)),
    );
    for (event, expected) in [
        (a_message, b"accepted -> initiating"),
        (b_message, b"initiating -> accepted"),
    ] {
        let Some(ConnectionEvent::Message(message)) = event.unwrap() else {
            panic!("no message")
        };
        assert_eq!(
            message
                .body()
                .segments()
                .flatten()
                .copied()
                .collect::<Vec<_>>(),
            expected
        );
    }
    one.await.unwrap();
    two.await.unwrap();
    a_handle.drain().unwrap();
    b_handle.drain().unwrap();
    let (a_end, b_end) = tokio::join!(
        poll_fn(|cx| a.poll_event(cx)),
        poll_fn(|cx| b.poll_event(cx)),
    );
    assert!(a_end.unwrap().is_none());
    assert!(b_end.unwrap().is_none());
}

#[tokio::test]
async fn one_engine_exchanges_native_frames_over_backpressured_duplex_io() {
    let (a, b) = tokio::io::duplex(7);
    tokio::time::timeout(Duration::from_secs(5), exchange(a, b))
        .await
        .unwrap();
}

#[tokio::test]
async fn one_engine_exchanges_native_frames_over_clear_loopback_tcp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (a, b) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    tokio::time::timeout(Duration::from_secs(5), exchange(a.unwrap(), b.unwrap().0))
        .await
        .unwrap();
}
