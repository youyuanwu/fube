//! Socket-free, admission-gated clear-frame parsing. Driver read-ahead, header
//! classification, destination admission, deadlines and delivery remain outside
//! this module. A retained body currently uses one shared allocation.

#![allow(
    dead_code,
    reason = "Private transport awaits public connection integration"
)]

use std::sync::atomic::{AtomicU64, Ordering};

use super::frame::{
    Crc32, FrameError, FrameHeader, FrameResult, FrameWireContext, FIXED_HEADER_LEN,
};
use crate::{
    body::{Body as StoredBody, Segment, SegmentBuffer},
    Body,
};

/// Unique even across readers, so another connection cannot reuse a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameKey(u64);

impl FrameKey {
    fn next() -> FrameResult<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.try_update(Ordering::Relaxed, Ordering::Relaxed, |key| {
            key.checked_add(1)
        })
        .map(Self)
        .map_err(|_| FrameError::InvalidState("frame identity exhausted"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameInfo {
    key: FrameKey,
    header: FrameHeader,
}

/// A consumable view of one driver-owned read allocation, not an accumulator.
#[derive(Debug)]
pub(crate) struct ReceiveInput {
    segment: Segment,
    consumed: usize,
}

impl ReceiveInput {
    pub(crate) fn new(segment: Segment) -> Self {
        Self {
            segment,
            consumed: 0,
        }
    }

    pub(crate) fn remaining(&self) -> &[u8] {
        &self.segment.bytes()[self.consumed..]
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkBudget {
    bytes: usize,
    transitions: usize,
}

impl WorkBudget {
    pub(crate) fn new(bytes: usize, transitions: usize) -> FrameResult<Self> {
        if bytes == 0 || transitions == 0 || bytes == usize::MAX || transitions == usize::MAX {
            return Err(FrameError::InvalidState(
                "work budgets must be positive and finite",
            ));
        }
        Ok(Self { bytes, transitions })
    }
}

#[derive(Debug)]
pub(crate) struct HeaderPermit {
    frame: FrameInfo,
    buffer: SegmentBuffer,
}

impl HeaderPermit {
    pub(crate) fn new(
        frame: FrameKey,
        header: FrameHeader,
        max_frame_bytes: usize,
    ) -> FrameResult<Self> {
        if header.frame_len() as usize > max_frame_bytes {
            return Err(FrameError::SizeLimit {
                frame_len: header.frame_len(),
                limit: max_frame_bytes,
            });
        }
        let len = usize::from(header.headers_len());
        let buffer = SegmentBuffer::new(len)?;
        Ok(Self {
            frame: FrameInfo { key: frame, header },
            buffer,
        })
    }
}

/// The driver retains these headers while classifying and awaiting verification.
#[derive(Debug)]
pub(crate) struct HeaderPrefix {
    pub(crate) frame: FrameKey,
    pub(crate) header: FrameHeader,
    pub(crate) area: Segment,
}

#[derive(Debug)]
pub(crate) struct BodyPermit {
    frame: FrameInfo,
    buffer: Option<SegmentBuffer>,
}

impl BodyPermit {
    /// Issued after header classification chooses to retain this body.
    pub(crate) fn new(frame: FrameKey, header: FrameHeader) -> FrameResult<Self> {
        let len = header.body_len() as usize;
        let buffer = if len == 0 {
            None
        } else {
            Some(SegmentBuffer::new(len)?)
        };
        Ok(Self {
            frame: FrameInfo { key: frame, header },
            buffer,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiscardReason {
    NoDestination,
    PolicyRejected,
    ResourceRejected,
}

#[derive(Debug)]
pub(crate) enum BodyDecision {
    Retain(BodyPermit),
    /// Byte/time drain limits are enforced by the driver, without renewing them
    /// on input or stage progress. Discard still verifies the complete CRC.
    Discard(DiscardReason),
}

#[derive(Debug)]
pub(crate) struct VerifiedBody {
    pub(crate) frame: FrameKey,
    pub(crate) body: Body,
}

#[derive(Debug)]
pub(crate) struct DiscardedFrame {
    pub(crate) frame: FrameKey,
    pub(crate) reason: DiscardReason,
}

#[derive(Debug)]
pub(crate) enum FrameReadStep {
    NeedInput {
        max_bytes: usize,
    },
    FixedHeader {
        frame: FrameKey,
        header: FrameHeader,
    },
    Headers(HeaderPrefix),
    Complete(VerifiedBody),
    Discarded(DiscardedFrame),
    Yield,
}

#[derive(Debug, Clone, Copy)]
enum Disposition {
    Retain,
    Discard(DiscardReason),
}

#[derive(Debug, Clone, Copy)]
enum State {
    Fixed {
        bytes: [u8; FIXED_HEADER_LEN],
        used: usize,
        wire: Option<FrameWireContext>,
    },
    HeaderGate(FrameInfo),
    Headers(FrameInfo),
    BodyGate(FrameInfo),
    Body {
        frame: FrameInfo,
        remaining: usize,
        disposition: Disposition,
    },
    Failed(FrameError),
}

impl State {
    fn initial() -> Self {
        Self::Fixed {
            bytes: [0; FIXED_HEADER_LEN],
            used: 0,
            wire: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FrameReader {
    state: State,
    headers: Option<HeaderPermit>,
    body: Option<BodyPermit>,
    crc: Crc32,
}

impl FrameReader {
    pub(crate) fn new() -> Self {
        Self {
            state: State::initial(),
            headers: None,
            body: None,
            crc: Crc32::new(),
        }
    }

    fn fail(&mut self, error: FrameError) -> FrameError {
        let error = match self.state {
            State::Failed(first) => first,
            _ => error,
        };
        self.state = State::Failed(error);
        self.headers = None;
        self.body = None;
        error
    }

    pub(crate) fn admit_headers(
        &mut self,
        frame: FrameKey,
        permit: HeaderPermit,
    ) -> FrameResult<()> {
        match self.state {
            State::HeaderGate(expected) if expected.key == frame && expected == permit.frame => {
                self.headers = Some(permit);
                self.state = State::Headers(expected);
                Ok(())
            }
            _ => Err(self.fail(FrameError::InvalidState("wrong header admission"))),
        }
    }

    pub(crate) fn decide_body(
        &mut self,
        frame: FrameKey,
        decision: BodyDecision,
    ) -> FrameResult<()> {
        let State::BodyGate(expected) = self.state else {
            return Err(self.fail(FrameError::InvalidState("body decision outside gate")));
        };
        if frame != expected.key {
            return Err(self.fail(FrameError::InvalidState("wrong body frame identity")));
        }
        let disposition = match decision {
            BodyDecision::Retain(permit) => {
                if expected != permit.frame {
                    return Err(self.fail(FrameError::InvalidState("wrong body permit")));
                }
                self.body = Some(permit);
                Disposition::Retain
            }
            BodyDecision::Discard(reason) => Disposition::Discard(reason),
        };
        self.state = State::Body {
            frame: expected,
            remaining: expected.header.body_len() as usize,
            disposition,
        };
        Ok(())
    }

    pub(crate) fn step(
        &mut self,
        input: &mut ReceiveInput,
        wire: &FrameWireContext,
        work: WorkBudget,
    ) -> FrameResult<FrameReadStep> {
        self.step_inner(input, wire, work)
            .map_err(|error| self.fail(error))
    }

    fn step_inner(
        &mut self,
        input: &mut ReceiveInput,
        wire: &FrameWireContext,
        mut work: WorkBudget,
    ) -> FrameResult<FrameReadStep> {
        loop {
            if work.transitions == 0 {
                return Ok(FrameReadStep::Yield);
            }
            work.transitions -= 1;
            match self.state {
                State::Fixed {
                    mut bytes,
                    mut used,
                    wire: snapshot,
                } => {
                    if snapshot.is_some_and(|snapshot| snapshot != *wire) {
                        return Err(FrameError::InvalidState("wire phase changed inside header"));
                    }
                    let count = Self::input_count(input, FIXED_HEADER_LEN - used, &work);
                    if count == 0 {
                        return Ok(Self::need_input(input, FIXED_HEADER_LEN - used, &work));
                    }
                    bytes[used..used + count].copy_from_slice(&input.remaining()[..count]);
                    input.consumed += count;
                    work.bytes -= count;
                    used += count;
                    self.state = State::Fixed {
                        bytes,
                        used,
                        wire: Some(*wire),
                    };
                    if used == FIXED_HEADER_LEN {
                        let header = FrameHeader::decode(&bytes, wire)?;
                        let frame = FrameKey::next()?;
                        self.state = State::HeaderGate(FrameInfo { key: frame, header });
                        return Ok(FrameReadStep::FixedHeader { frame, header });
                    }
                }
                State::HeaderGate(_) | State::BodyGate(_) => {
                    return Err(FrameError::InvalidState(
                        "step at unresolved admission gate",
                    ));
                }
                State::Headers(frame) => {
                    let permit = self
                        .headers
                        .as_mut()
                        .ok_or(FrameError::InvalidState("missing header permit"))?;
                    let remaining = permit.buffer.remaining();
                    if remaining == 0 {
                        let permit = self.headers.take().expect("checked header permit");
                        let area = permit.buffer.finish()?;
                        self.state = State::BodyGate(frame);
                        return Ok(FrameReadStep::Headers(HeaderPrefix {
                            frame: frame.key,
                            header: frame.header,
                            area,
                        }));
                    }
                    let count = Self::input_count(input, remaining, &work);
                    if count == 0 {
                        return Ok(Self::need_input(input, remaining, &work));
                    }
                    let bytes = &input.remaining()[..count];
                    permit.buffer.extend(bytes)?;
                    self.crc.update(bytes);
                    input.consumed += count;
                    work.bytes -= count;
                }
                State::Body {
                    frame,
                    remaining,
                    disposition,
                } => {
                    if remaining == 0 {
                        frame.header.verify_body_crc(&self.crc)?;
                        let event = match disposition {
                            Disposition::Retain => {
                                let permit = self
                                    .body
                                    .take()
                                    .ok_or(FrameError::InvalidState("missing body permit"))?;
                                let body = match permit.buffer {
                                    Some(buffer) => Body::from_stored(StoredBody::from_segment(
                                        buffer.finish()?,
                                    )?),
                                    None => Body::empty(),
                                };
                                FrameReadStep::Complete(VerifiedBody {
                                    frame: frame.key,
                                    body,
                                })
                            }
                            Disposition::Discard(reason) => {
                                FrameReadStep::Discarded(DiscardedFrame {
                                    frame: frame.key,
                                    reason,
                                })
                            }
                        };
                        self.state = State::initial();
                        self.crc = Crc32::new();
                        return Ok(event);
                    }
                    let count = Self::input_count(input, remaining, &work);
                    if count == 0 {
                        return Ok(Self::need_input(input, remaining, &work));
                    }
                    let bytes = &input.remaining()[..count];
                    if matches!(disposition, Disposition::Retain) {
                        self.body
                            .as_mut()
                            .and_then(|permit| permit.buffer.as_mut())
                            .ok_or(FrameError::InvalidState("missing retained body storage"))?
                            .extend(bytes)?;
                    }
                    self.crc.update(bytes);
                    input.consumed += count;
                    work.bytes -= count;
                    self.state = State::Body {
                        frame,
                        remaining: remaining - count,
                        disposition,
                    };
                }
                State::Failed(error) => return Err(error),
            }
        }
    }

    fn input_count(input: &ReceiveInput, missing: usize, work: &WorkBudget) -> usize {
        input.remaining().len().min(missing).min(work.bytes)
    }

    fn need_input(input: &ReceiveInput, missing: usize, work: &WorkBudget) -> FrameReadStep {
        if work.bytes == 0 {
            FrameReadStep::Yield
        } else {
            debug_assert!(input.remaining().is_empty() && missing > 0);
            FrameReadStep::NeedInput {
                max_bytes: missing.min(work.bytes),
            }
        }
    }

    /// Call only after buffered bytes/events and admission gates have been
    /// processed. Checking input explicitly prevents a framing-clean false EOF.
    pub(crate) fn finish_eof(&mut self, input: &ReceiveInput) -> FrameResult<()> {
        let result = match self.state {
            State::Failed(error) => Err(error),
            _ if !input.remaining().is_empty() => Err(FrameError::InvalidState(
                "EOF before processing buffered input",
            )),
            State::Fixed { used: 0, .. } => Ok(()),
            State::Fixed { used, .. } => Err(FrameError::Truncated {
                stage: "fixed header",
                missing: FIXED_HEADER_LEN - used,
            }),
            State::Headers(_) => match self.headers.as_ref() {
                Some(permit) if permit.buffer.remaining() > 0 => Err(FrameError::Truncated {
                    stage: "header area",
                    missing: permit.buffer.remaining(),
                }),
                _ => Err(FrameError::InvalidState(
                    "EOF before processing ready header event",
                )),
            },
            State::Body { remaining, .. } if remaining > 0 => Err(FrameError::Truncated {
                stage: "body",
                missing: remaining,
            }),
            _ => Err(FrameError::InvalidState(
                "EOF before processing gate or ready event",
            )),
        };
        result.map_err(|error| self.fail(error))
    }
}

#[cfg(test)]
mod tests;
