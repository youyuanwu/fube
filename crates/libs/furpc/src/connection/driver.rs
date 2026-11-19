use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{sleep_until, Instant, Sleep},
};

use super::{
    native, wake_if_some, ConnectionConfig, ConnectionError, ConnectionHandle, ConnectionResult,
    Ingress, ListenInstance, LocalInstance, Operation, Phase, Role, SendJob, Shared, State,
};
use crate::{
    body::Segment,
    wire::{
        frame::FrameWireContext,
        reader::{
            BodyDecision, BodyPermit, DiscardReason, FrameKey, FrameReadStep, FrameReader,
            HeaderPermit, ReceiveInput, WorkBudget,
        },
        writer::{FrameCursor, FrameWriteLimits, PreparedFrame},
    },
    Error, Headers, Message, MessageId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropReason {
    NoDestination,
    UnknownActor,
    RouteMiss,
    UnmatchedReply,
    SpecialDuplexUnavailable,
    Draining,
    PolicyRejected,
    Capacity,
}

#[derive(Debug)]
pub(crate) enum ConnectionEvent {
    Opened(ListenInstance),
    IngressAdmitted,
    Reply(Message),
    ReadClosed,
    InstanceUpdated(ListenInstance),
    Message(Message),
    Discarded(DropReason),
}

type Preparation =
    Pin<Box<dyn Future<Output = crate::wire::frame::FrameResult<PreparedFrame>> + Send>>;

struct ActiveSend {
    job: SendJob,
    preparation: Option<Preparation>,
    cursor: Option<FrameCursor>,
    write_deadline: Option<Instant>,
}

impl ActiveSend {
    fn new(mut job: SendJob, wire: FrameWireContext, config: &ConnectionConfig) -> Self {
        let message = job
            .message
            .take()
            .expect("unprepared send owns its message");
        let preparation = Box::pin(PreparedFrame::prepare(
            message,
            wire,
            FrameWriteLimits {
                headers: config.headers,
                max_frame_bytes: config.send_frame_limit(),
                work_bytes: config.work_bytes,
            },
        ));
        Self {
            job,
            preparation: Some(preparation),
            cursor: None,
            write_deadline: None,
        }
    }
}

enum ReceiveKind {
    Hello,
    Application,
    Reply,
    Discard(DropReason),
}

struct Provisional {
    frame: FrameKey,
    headers: Option<Headers>,
    kind: ReceiveKind,
}

/// Owned byte I/O and resumable progress. The scope must keep polling this
/// driver, including between events; no internal task or receive queue exists.
/// Cancelling a poll does not discard offsets. Dropping the driver aborts I/O.
pub(crate) struct ConnectionDriver<I> {
    io: Option<I>,
    shared: Arc<Shared>,
    config: ConnectionConfig,
    role: Role,
    tx_wire: FrameWireContext,
    rx_wire: FrameWireContext,
    establishment_deadline: Instant,
    reader: FrameReader,
    input: ReceiveInput,
    read_scratch: [u8; 8192],
    provisional: Option<Provisional>,
    frame_deadline: Option<Instant>,
    discard_deadline: Option<Instant>,
    active: Option<ActiveSend>,
    hello_written: bool,
    remote_received: bool,
    remote: Option<ListenInstance>,
    read_eof: bool,
    timer: Pin<Box<Sleep>>,
    rpc: bool,
}

impl<I> ConnectionDriver<I>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(crate) fn new(
        io: I,
        role: Role,
        local: LocalInstance,
        config: ConnectionConfig,
        establishment_deadline: Instant,
    ) -> ConnectionResult<(ConnectionHandle, Self)> {
        config.validate()?;
        if establishment_deadline <= Instant::now() {
            return Err(Arc::new(ConnectionError::Timeout("establishment")));
        }
        let hello = native::hello(local, role, &config)?;
        let bytes = 12 + hello.headers().encoded_len() + hello.body().len();
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                phase: Phase::Opening,
                terminal: None,
                queue: Default::default(),
                queued_bytes: bytes,
                pending_sends: 0,
                handles: 1,
                drain_deadline: None,
                waker: None,
                drain_holds: 0,
            }),
            config,
        });
        let active = ActiveSend::new(
            SendJob {
                message: Some(hello),
                bytes,
                deadline: establishment_deadline,
                operation: Operation::new(),
                hello: true,
                authorization: None,
            },
            config.wire,
            &config,
        );
        Ok((
            ConnectionHandle {
                shared: shared.clone(),
            },
            Self {
                io: Some(io),
                shared,
                config,
                role,
                tx_wire: config.wire,
                rx_wire: config.wire,
                establishment_deadline,
                reader: FrameReader::new(),
                input: ReceiveInput::new(Segment::from_bytes(Vec::new())),
                read_scratch: [0; 8192],
                provisional: None,
                frame_deadline: None,
                discard_deadline: None,
                active: Some(active),
                hello_written: false,
                remote_received: false,
                remote: None,
                read_eof: false,
                timer: Box::pin(sleep_until(establishment_deadline)),
                rpc: false,
            },
        ))
    }

    pub(crate) fn enable_rpc(&mut self) {
        self.rpc = true;
    }

    pub(crate) fn poll_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ConnectionResult<Option<ConnectionEvent>>> {
        self.poll_event_inner(cx, &mut |_, _| Ok(()), false)
    }

    pub(crate) fn poll_event_with_admission(
        &mut self,
        cx: &mut Context<'_>,
        admit: &mut impl FnMut(&Headers, usize) -> Result<(), DropReason>,
    ) -> Poll<ConnectionResult<Option<ConnectionEvent>>> {
        self.poll_event_inner(cx, admit, true)
    }

    fn poll_event_inner(
        &mut self,
        cx: &mut Context<'_>,
        admit: &mut impl FnMut(&Headers, usize) -> Result<(), DropReason>,
        notify_admission: bool,
    ) -> Poll<ConnectionResult<Option<ConnectionEvent>>> {
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.phase == Phase::Closed {
                return Poll::Ready(match &state.terminal {
                    Some(error) => Err(error.clone()),
                    None => Ok(None),
                });
            }
            state.waker = Some(cx.waker().clone());
        }
        match self.poll_running(cx, admit, notify_admission) {
            Ok(Poll::Ready(None)) => Poll::Ready(match self.retire(None) {
                Some(error) => Err(error),
                None => Ok(None),
            }),
            Ok(poll) => poll.map(Ok),
            Err(error) => {
                let first = self
                    .retire(Some(error))
                    .expect("failed driver retains its cause");
                Poll::Ready(Err(first))
            }
        }
    }

    fn poll_running(
        &mut self,
        cx: &mut Context<'_>,
        admit: &mut impl FnMut(&Headers, usize) -> Result<(), DropReason>,
        notify_admission: bool,
    ) -> ConnectionResult<Poll<Option<ConnectionEvent>>> {
        for _ in 0..self.config.work_steps {
            self.check_watchdogs()?;
            let mut progress = self.prune_one();
            progress |= self.poll_send(cx)?;

            {
                let mut state = self.shared.state.lock().unwrap();
                if let Some(error) = &state.terminal {
                    return Err(error.clone());
                }
                if state.phase == Phase::Opening && self.hello_written && self.remote_received {
                    // This is the clear profile's release gate, not authentication.
                    state.phase = Phase::Open;
                    return Ok(Poll::Ready(Some(ConnectionEvent::Opened(
                        self.remote.take().expect("verified remote hello"),
                    ))));
                }
                if state.phase == Phase::Draining
                    && state.drain_holds == 0
                    && self.active.is_none()
                    && state.queue.is_empty()
                {
                    drop(state);
                    return match Pin::new(self.io.as_mut().expect("live driver owns I/O"))
                        .poll_shutdown(cx)
                    {
                        Poll::Ready(Ok(())) => Ok(Poll::Ready(None)),
                        Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {
                            cx.waker().wake_by_ref();
                            Ok(Poll::Pending)
                        }
                        Poll::Ready(Err(source)) => Err(Arc::new(ConnectionError::Io {
                            phase: "shutdown",
                            source,
                        })),
                        Poll::Pending => {
                            self.arm_timer(cx);
                            Ok(Poll::Pending)
                        }
                    };
                }
            }

            if !self.read_eof && (!self.remote_received || self.hello_written) {
                match self.poll_receive(cx, admit, notify_admission)? {
                    ReceiveProgress::Event(event) => return Ok(Poll::Ready(Some(event))),
                    ReceiveProgress::Progress => progress = true,
                    ReceiveProgress::Pending => {}
                }
            }
            if !progress {
                self.arm_timer(cx);
                return Ok(Poll::Pending);
            }
        }
        cx.waker().wake_by_ref();
        Ok(Poll::Pending)
    }

    fn poll_send(&mut self, cx: &mut Context<'_>) -> ConnectionResult<bool> {
        if self.active.is_none() {
            let next = self.shared.state.lock().unwrap().queue.pop_front();
            if let Some(job) = next {
                self.active = Some(ActiveSend::new(job, self.tx_wire, &self.config));
            } else {
                return Ok(false);
            }
        }

        let rejection = {
            let active = self.active.as_ref().unwrap();
            let state = self.shared.state.lock().unwrap();
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            let operation = active.job.operation.lock().unwrap();
            uncommitted_rejection(&operation, active.job.deadline)
        };
        if let Some(error) = rejection {
            if self.active.as_ref().unwrap().job.hello {
                return Err(error);
            }
            self.finish_send(Err(error));
            return Ok(true);
        }

        let active = self.active.as_mut().unwrap();
        if let Some(preparation) = &mut active.preparation {
            return match preparation.as_mut().poll(cx) {
                Poll::Pending => Ok(false),
                Poll::Ready(Ok(frame)) => {
                    active.cursor = Some(frame.into_cursor());
                    active.preparation = None;
                    Ok(true)
                }
                Poll::Ready(Err(error)) => {
                    if active.job.hello {
                        return Err(error.into());
                    }
                    self.finish_send(Err(error.into()));
                    Ok(true)
                }
            };
        }

        // Cancellation/abort and first positive output are one transaction.
        // Trusted nonblocking socket polls cannot await or reenter this gate.
        let state = self.shared.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        let mut operation = active.job.operation.lock().unwrap();
        if let Some(error) = uncommitted_rejection(&operation, active.job.deadline) {
            drop(operation);
            drop(state);
            self.finish_send(Err(error));
            return Ok(true);
        }
        if !operation.committed {
            if let Some(authorization) = &active.job.authorization {
                if let Err(error) = authorization.check_commit() {
                    drop(operation);
                    drop(state);
                    self.finish_send(Err(error.into()));
                    return Ok(true);
                }
            }
        }
        let cursor = active.cursor.as_mut().expect("prepared send owns a cursor");
        if cursor.wire() != self.tx_wire {
            return Err(Error::Internal("prepared frame has obsolete transmit phase").into());
        }
        let mut slices = [IoSlice::new(&[]); 16];
        let count = cursor.io_slices_bounded(&mut slices, self.config.work_bytes);
        if count == 0 {
            return Err(Error::Internal("nonempty output has no write slices").into());
        }
        let io = self.io.as_mut().expect("live driver owns I/O");
        let vectored = io.is_write_vectored();
        let offered = if vectored {
            slices[..count].iter().map(|slice| slice.len()).sum()
        } else {
            slices[0].len()
        };
        let result = if vectored {
            Pin::new(io).poll_write_vectored(cx, &slices[..count])
        } else {
            Pin::new(io).poll_write(cx, &slices[0])
        };
        match result {
            Poll::Pending => Ok(false),
            Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => Ok(true),
            Poll::Ready(Err(source)) => Err(Arc::new(ConnectionError::Io {
                phase: "write",
                source,
            })),
            Poll::Ready(Ok(0)) => Err(Arc::new(ConnectionError::Io {
                phase: "write",
                source: io::Error::new(io::ErrorKind::WriteZero, "nonempty native frame"),
            })),
            Poll::Ready(Ok(written)) => {
                if written > offered {
                    return Err(Error::Internal("I/O reported more bytes than offered").into());
                }
                operation.committed = true;
                active
                    .write_deadline
                    .get_or_insert_with(|| Instant::now() + self.config.write_timeout);
                cursor.advance(written)?;
                let complete = cursor.is_complete();
                drop(operation);
                drop(state);
                if complete {
                    self.finish_send(Ok(()));
                }
                Ok(true)
            }
        }
    }

    fn finish_send(&mut self, result: ConnectionResult<()>) {
        let active = self.active.take().expect("finishing active send");
        let mut state = self.shared.state.lock().unwrap();
        state.queued_bytes -= active.job.bytes;
        if active.job.hello {
            self.hello_written = result.is_ok();
        } else {
            state.pending_sends -= 1;
        }
        let wake = active.job.operation.lock().unwrap().settle(result);
        drop(state);
        drop(active);
        wake_if_some(wake);
    }

    /// Pending count is explicit runtime policy. Scan that bounded set, removing
    /// at most one expired/cancelled queued operation per scheduling turn.
    fn prune_one(&mut self) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        let rejected = state.queue.iter().enumerate().find_map(|(index, job)| {
            uncommitted_rejection(&job.operation.lock().unwrap(), job.deadline)
                .map(|error| (index, error))
        });
        let Some((index, error)) = rejected else {
            return false;
        };
        let job = state.queue.remove(index).expect("located queued send");
        state.queued_bytes -= job.bytes;
        state.pending_sends -= 1;
        let wake = job.operation.lock().unwrap().settle(Err(error));
        drop(state);
        drop(job);
        wake_if_some(wake);
        true
    }

    fn poll_receive(
        &mut self,
        cx: &mut Context<'_>,
        admit: &mut impl FnMut(&Headers, usize) -> Result<(), DropReason>,
        notify_admission: bool,
    ) -> ConnectionResult<ReceiveProgress> {
        let work = WorkBudget::new(self.config.work_bytes, self.config.work_steps)?;
        match self.reader.step(&mut self.input, &self.rx_wire, work)? {
            FrameReadStep::NeedInput { max_bytes } => {
                let len = max_bytes.min(self.read_scratch.len());
                let mut buffer = ReadBuf::new(&mut self.read_scratch[..len]);
                match Pin::new(self.io.as_mut().expect("live driver owns I/O"))
                    .poll_read(cx, &mut buffer)
                {
                    Poll::Pending => Ok(ReceiveProgress::Pending),
                    Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {
                        Ok(ReceiveProgress::Progress)
                    }
                    Poll::Ready(Err(source)) => Err(Arc::new(ConnectionError::Io {
                        phase: "read",
                        source,
                    })),
                    Poll::Ready(Ok(())) if buffer.filled().is_empty() => {
                        self.reader.finish_eof(&self.input)?;
                        self.read_eof = true;
                        let mut state = self.shared.state.lock().unwrap();
                        match state.phase {
                            Phase::Opening => {
                                return Err(
                                    Error::Protocol("EOF before native establishment").into()
                                );
                            }
                            Phase::Open => {
                                state.phase = Phase::Draining;
                                state.drain_deadline =
                                    Some(Instant::now() + self.config.drain_timeout);
                            }
                            _ => {}
                        }
                        if notify_admission {
                            Ok(ReceiveProgress::Event(ConnectionEvent::ReadClosed))
                        } else {
                            Ok(ReceiveProgress::Progress)
                        }
                    }
                    Poll::Ready(Ok(())) => {
                        self.frame_deadline
                            .get_or_insert_with(|| Instant::now() + self.config.frame_timeout);
                        let mut bytes = Vec::new();
                        bytes
                            .try_reserve_exact(buffer.filled().len())
                            .map_err(|_| Error::ResourceLimit("read storage allocation"))?;
                        bytes.extend_from_slice(buffer.filled());
                        self.input = ReceiveInput::new(Segment::from_bytes(bytes));
                        Ok(ReceiveProgress::Progress)
                    }
                }
            }
            FrameReadStep::FixedHeader { frame, header } => {
                self.config
                    .headers
                    .validate_area(header.headers_len() as usize)
                    .map_err(Error::from)?;
                let permit = HeaderPermit::new(frame, header, self.config.max_receive_frame_bytes)?;
                self.reader.admit_headers(frame, permit)?;
                Ok(ReceiveProgress::Progress)
            }
            FrameReadStep::Headers(prefix) => {
                let headers = Headers::from_received(prefix.area, &self.config.headers)?;
                let mut kind = self.classify(&headers)?;
                if matches!(kind, ReceiveKind::Application | ReceiveKind::Reply) {
                    if let Err(reason) = admit(&headers, prefix.header.body_len() as usize) {
                        kind = ReceiveKind::Discard(reason);
                    }
                }
                let retained = !matches!(kind, ReceiveKind::Discard(_));
                let decision = if retained {
                    BodyDecision::Retain(BodyPermit::new(prefix.frame, prefix.header)?)
                } else {
                    if prefix.header.body_len() as usize > self.config.max_discard_bytes {
                        return Err(Error::ResourceLimit("discard body bytes").into());
                    }
                    self.discard_deadline = Some(Instant::now() + self.config.discard_timeout);
                    BodyDecision::Discard(DiscardReason::PolicyRejected)
                };
                self.reader.decide_body(prefix.frame, decision)?;
                self.provisional = Some(Provisional {
                    frame: prefix.frame,
                    headers: retained.then_some(headers),
                    kind,
                });
                if notify_admission
                    && matches!(
                        self.provisional.as_ref().unwrap().kind,
                        ReceiveKind::Application | ReceiveKind::Reply
                    )
                {
                    Ok(ReceiveProgress::Event(ConnectionEvent::IngressAdmitted))
                } else {
                    Ok(ReceiveProgress::Progress)
                }
            }
            FrameReadStep::Complete(verified) => {
                let provisional = self.take_provisional(verified.frame)?;
                let headers = provisional
                    .headers
                    .ok_or(Error::Internal("retained frame lost decoded headers"))?;
                match provisional.kind {
                    ReceiveKind::Hello => {
                        let remote = native::decode_hello(&verified.body, self.role, &self.config)?;
                        if self.remote_received {
                            Ok(ReceiveProgress::Event(ConnectionEvent::InstanceUpdated(
                                remote,
                            )))
                        } else {
                            self.remote_received = true;
                            self.remote = Some(remote);
                            Ok(ReceiveProgress::Progress)
                        }
                    }
                    ReceiveKind::Application => {
                        let state = self.shared.state.lock().unwrap();
                        if let Some(error) = &state.terminal {
                            return Err(error.clone());
                        }
                        if state.phase == Phase::Draining {
                            Ok(ReceiveProgress::Event(ConnectionEvent::Discarded(
                                DropReason::Draining,
                            )))
                        } else {
                            Ok(ReceiveProgress::Event(ConnectionEvent::Message(
                                Message::from_received(headers, verified.body),
                            )))
                        }
                    }
                    ReceiveKind::Reply => Ok(ReceiveProgress::Event(ConnectionEvent::Reply(
                        Message::from_received(headers, verified.body),
                    ))),
                    ReceiveKind::Discard(_) => {
                        Err(Error::Internal("discarded frame produced retained body").into())
                    }
                }
            }
            FrameReadStep::Discarded(discarded) => {
                let provisional = self.take_provisional(discarded.frame)?;
                if discarded.reason != DiscardReason::PolicyRejected {
                    return Err(Error::Internal("unexpected parser discard reason").into());
                }
                match provisional.kind {
                    ReceiveKind::Discard(reason) => {
                        Ok(ReceiveProgress::Event(ConnectionEvent::Discarded(reason)))
                    }
                    _ => Err(Error::Internal("retained frame produced discard event").into()),
                }
            }
            FrameReadStep::Yield => {
                cx.waker().wake_by_ref();
                Ok(ReceiveProgress::Pending)
            }
        }
    }

    fn classify(&self, headers: &Headers) -> ConnectionResult<ReceiveKind> {
        let state = self.shared.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        let common = headers.effective();
        if common.actor.get() == native::TRANSPORT && !common.action().is_empty() {
            return Err(Arc::new(ConnectionError::Unsupported("transport action")));
        }
        if state.phase == Phase::Opening || common.actor.get() == native::TRANSPORT {
            native::check_hello_headers(headers)?;
            return Ok(ReceiveKind::Hello);
        }
        if native::is_control_actor(common.actor.get()) {
            return Err(Arc::new(ConnectionError::Unsupported(
                "security/send-target control",
            )));
        }
        let reason = if common.uncorrelated_reply {
            Some(DropReason::SpecialDuplexUnavailable)
        } else if common.relates_to != MessageId::default() && !self.rpc {
            Some(DropReason::UnmatchedReply)
        } else if common.relates_to != MessageId::default() {
            return Ok(ReceiveKind::Reply);
        } else if state.phase == Phase::Draining {
            Some(DropReason::Draining)
        } else if matches!(self.config.ingress, Ingress::None) {
            Some(DropReason::NoDestination)
        } else {
            None
        };
        Ok(match reason {
            Some(reason) => ReceiveKind::Discard(reason),
            None => ReceiveKind::Application,
        })
    }

    fn take_provisional(&mut self, frame: FrameKey) -> ConnectionResult<Provisional> {
        let provisional = self
            .provisional
            .take()
            .ok_or(Error::Internal("complete frame lacks classification"))?;
        if provisional.frame != frame {
            return Err(Error::Internal("classification belongs to another frame").into());
        }
        self.frame_deadline = None;
        self.discard_deadline = None;
        Ok(provisional)
    }

    fn active_deadline(&self) -> Option<(Instant, &'static str)> {
        let state = self.shared.state.lock().unwrap();
        [
            (state.phase == Phase::Opening)
                .then_some((self.establishment_deadline, "establishment")),
            self.frame_deadline
                .map(|deadline| (deadline, "partial frame")),
            self.discard_deadline.map(|deadline| (deadline, "discard")),
            self.active
                .as_ref()
                .and_then(|active| active.write_deadline)
                .map(|deadline| (deadline, "committed write")),
            state.drain_deadline.map(|deadline| (deadline, "drain")),
        ]
        .into_iter()
        .flatten()
        .min_by_key(|(deadline, _)| *deadline)
    }

    fn check_watchdogs(&self) -> ConnectionResult<()> {
        if let Some(error) = &self.shared.state.lock().unwrap().terminal {
            return Err(error.clone());
        }
        if let Some((deadline, phase)) = self.active_deadline() {
            if Instant::now() >= deadline {
                return Err(Arc::new(ConnectionError::Timeout(phase)));
            }
        }
        Ok(())
    }

    fn arm_timer(&mut self, cx: &mut Context<'_>) {
        let mut deadline = self.active_deadline().map(|(deadline, _)| deadline);
        let state = self.shared.state.lock().unwrap();
        for job in state
            .queue
            .iter()
            .chain(self.active.iter().map(|active| &active.job))
        {
            if !job.operation.lock().unwrap().committed {
                deadline = Some(deadline.map_or(job.deadline, |current| current.min(job.deadline)));
            }
        }
        drop(state);
        if let Some(deadline) = deadline {
            self.timer.as_mut().reset(deadline);
            if self.timer.as_mut().poll(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
        }
    }
}

impl<I> ConnectionDriver<I> {
    /// Settles each receipt once with the first connection cause and drops I/O
    /// even when the driver object itself remains retained after termination.
    fn retire(&mut self, error: Option<Arc<ConnectionError>>) -> Option<Arc<ConnectionError>> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(error) = error {
            state.terminal.get_or_insert(error);
        }
        let first = state.terminal.clone();
        let settlement = first
            .clone()
            .unwrap_or_else(|| Arc::new(ConnectionError::Closed));
        state.phase = Phase::Closed;
        state.queued_bytes = 0;
        state.pending_sends = 0;
        let queue = std::mem::take(&mut state.queue);
        state.waker = None;
        drop(state);
        for job in queue
            .into_iter()
            .chain(self.active.take().map(|active| active.job))
        {
            let wake = job
                .operation
                .lock()
                .unwrap()
                .settle(Err(settlement.clone()));
            wake_if_some(wake);
        }
        self.provisional = None;
        self.remote = None;
        self.reader = FrameReader::new();
        self.input = ReceiveInput::new(Segment::from_bytes(Vec::new()));
        self.io = None;
        first
    }
}

impl<I> Drop for ConnectionDriver<I> {
    fn drop(&mut self) {
        if self.io.is_some() {
            self.retire(Some(Arc::new(ConnectionError::Closed)));
        }
    }
}

enum ReceiveProgress {
    Pending,
    Progress,
    Event(ConnectionEvent),
}

fn uncommitted_rejection(operation: &Operation, deadline: Instant) -> Option<Arc<ConnectionError>> {
    if operation.committed {
        None
    } else if operation.cancelled {
        Some(Arc::new(ConnectionError::Cancelled))
    } else if Instant::now() >= deadline {
        Some(Arc::new(ConnectionError::Timeout("send admission")))
    } else {
        None
    }
}
