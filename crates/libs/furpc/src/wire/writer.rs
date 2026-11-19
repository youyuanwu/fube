//! Immutable segmented output. Preparation is owned work, not send
//! authority or commitment; the future neither spawns nor polls any I/O.

#![allow(
    dead_code,
    reason = "Private transport awaits public connection integration"
)]

use std::{future::poll_fn, io::IoSlice, task::Poll};

use super::{
    frame::{Crc32, FrameError, FrameHeader, FrameResult, FrameWireContext, FIXED_HEADER_LEN},
    headers::HeaderDecodeContext,
};
use crate::{
    body::{Segment, SegmentBuffer},
    Body, Message,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameWriteLimits {
    pub(crate) headers: HeaderDecodeContext,
    pub(crate) max_frame_bytes: usize,
    pub(crate) work_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct PreparedFrame {
    header: FrameHeader,
    fixed: [u8; FIXED_HEADER_LEN],
    headers: Segment,
    body: Body,
    wire: FrameWireContext,
}

impl PreparedFrame {
    pub(crate) async fn prepare(
        message: Message,
        wire: FrameWireContext,
        limits: FrameWriteLimits,
    ) -> FrameResult<Self> {
        if limits.work_bytes == 0 || limits.work_bytes == usize::MAX {
            return Err(FrameError::InvalidState(
                "write work quantum must be positive and finite",
            ));
        }
        let initial = FrameHeader::for_send(
            message.headers().encoded_len(),
            message.body().len(),
            0,
            &wire,
        )?;
        if initial.frame_len() as usize > limits.max_frame_bytes {
            return Err(FrameError::SizeLimit {
                frame_len: initial.frame_len(),
                limit: limits.max_frame_bytes,
            });
        }
        message.validate(&limits.headers, limits.max_frame_bytes)?;
        let mut buffer = SegmentBuffer::new(message.headers().encoded_len())?;
        let mut work = PreparationWork {
            remaining: limits.work_bytes,
            quantum: limits.work_bytes,
        };
        let mut crc = Crc32::new();
        for record in message.headers().raw_records() {
            let id = record.id().get().to_le_bytes();
            let len = u16::try_from(record.value().len())
                .map_err(|_| FrameError::HeaderLengthOverflow {
                    len: record.value().len(),
                })?
                .to_le_bytes();
            work.header(&id, &mut buffer, &mut crc).await?;
            work.header(&len, &mut buffer, &mut crc).await?;
            work.header(record.value(), &mut buffer, &mut crc).await?;
        }
        for segment in message.body().segments() {
            work.checksum(segment, &mut crc).await;
        }
        let header = FrameHeader::for_send(
            message.headers().encoded_len(),
            message.body().len(),
            crc.value(),
            &wire,
        )?;
        Ok(Self {
            header,
            fixed: header.encode(),
            headers: buffer.finish()?,
            body: message.into_body(),
            wire,
        })
    }

    pub(crate) fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.header.frame_len() as usize
    }

    /// The driver must compare this snapshot at the synchronized commitment gate.
    pub(crate) fn wire(&self) -> FrameWireContext {
        self.wire
    }

    pub(crate) fn into_cursor(self) -> FrameCursor {
        FrameCursor {
            frame: self,
            written: 0,
        }
    }
}

struct PreparationWork {
    remaining: usize,
    quantum: usize,
}

impl PreparationWork {
    async fn allowance(&mut self, len: usize) -> usize {
        if self.remaining == 0 {
            let mut yielded = false;
            poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
            self.remaining = self.quantum;
        }
        let count = self.remaining.min(len);
        self.remaining -= count;
        count
    }

    async fn header(
        &mut self,
        mut bytes: &[u8],
        buffer: &mut SegmentBuffer,
        crc: &mut Crc32,
    ) -> FrameResult<()> {
        while !bytes.is_empty() {
            let count = self.allowance(bytes.len()).await;
            buffer.extend(&bytes[..count])?;
            crc.update(&bytes[..count]);
            bytes = &bytes[count..];
        }
        Ok(())
    }

    async fn checksum(&mut self, mut bytes: &[u8], crc: &mut Crc32) {
        while !bytes.is_empty() {
            let count = self.allowance(bytes.len()).await;
            crc.update(&bytes[..count]);
            bytes = &bytes[count..];
        }
    }
}

#[derive(Debug)]
pub(crate) struct FrameCursor {
    frame: PreparedFrame,
    written: usize,
}

impl FrameCursor {
    pub(crate) fn wire(&self) -> FrameWireContext {
        self.frame.wire()
    }

    pub(crate) fn remaining(&self) -> usize {
        self.frame.encoded_len() - self.written
    }

    /// Caller-owned scratch bounds the number of segments offered. The driver
    /// also validates write counts against the bytes actually offered to I/O.
    pub(crate) fn io_slices<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        self.io_slices_bounded(dst, usize::MAX)
    }

    pub(crate) fn io_slices_bounded<'a>(
        &'a self,
        dst: &mut [IoSlice<'a>],
        mut max_bytes: usize,
    ) -> usize {
        let segments = std::iter::once(self.frame.fixed.as_slice())
            .chain(std::iter::once(self.frame.headers.bytes()))
            .chain(self.frame.body.segments());
        let mut skip = self.written;
        let mut count = 0;
        for segment in segments {
            if count == dst.len() || max_bytes == 0 {
                break;
            }
            if skip >= segment.len() {
                skip -= segment.len();
                continue;
            }
            let len = (segment.len() - skip).min(max_bytes);
            dst[count] = IoSlice::new(&segment[skip..skip + len]);
            max_bytes -= len;
            count += 1;
            skip = 0;
        }
        count
    }

    pub(crate) fn advance(&mut self, written: usize) -> FrameResult<()> {
        if written > self.remaining() {
            return Err(FrameError::InvalidState(
                "write advancement exceeds remaining frame",
            ));
        }
        self.written += written;
        Ok(())
    }

    /// Acceptance by the immediate next layer, not remote receipt or settlement.
    pub(crate) fn is_complete(&self) -> bool {
        self.remaining() == 0
    }
}

#[cfg(test)]
mod tests;
