//! Shared immutable bytes and segmented bodies. Ownership follows references,
//! independently of connection send-queue accounting.

#![allow(dead_code, reason = "Transport storage awaits the connection driver")]

use std::{ops::Range, sync::Arc};

use crate::{wire::frame::FIXED_HEADER_LEN, Error, Result};

pub(crate) const MAX_BODY_BYTES: usize = u32::MAX as usize - FIXED_HEADER_LEN;

#[derive(Debug, Clone)]
pub(crate) struct Segment {
    owner: Arc<Vec<u8>>,
    range: Range<usize>,
}

impl Segment {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        let range = 0..bytes.len();
        Self {
            owner: Arc::new(bytes),
            range,
        }
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> Result<Self> {
        check_range(&range, self.range.len())?;
        Ok(Self {
            owner: self.owner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
}

/// An unshared assembly buffer which never grows beyond its declared length.
#[derive(Debug)]
pub(crate) struct SegmentBuffer {
    bytes: Vec<u8>,
    expected: usize,
}

impl SegmentBuffer {
    pub(crate) fn new(len: usize) -> Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| Error::ResourceLimit("byte buffer allocation"))?;
        Ok(Self {
            bytes,
            expected: len,
        })
    }

    pub(crate) fn remaining(&self) -> usize {
        self.expected - self.bytes.len()
    }

    pub(crate) fn extend(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.remaining() {
            return Err(Error::Internal("assembly buffer overflow"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Segment> {
        if self.remaining() != 0 {
            return Err(Error::Internal("incomplete assembly buffer"));
        }
        Ok(Segment::from_bytes(self.bytes))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Body {
    owner: Option<Arc<Vec<Segment>>>,
    range: Range<usize>,
}

impl Body {
    pub(crate) const fn empty() -> Self {
        Self {
            owner: None,
            range: 0..0,
        }
    }

    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() > MAX_BODY_BYTES {
            return Err(Error::InvalidInput(
                "native body length exceeds frame capacity",
            ));
        }
        if bytes.is_empty() {
            return Ok(Self::empty());
        }
        Self::from_segment(Segment::from_bytes(bytes))
    }

    pub(crate) fn from_segment(segment: Segment) -> Result<Self> {
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(1)
            .map_err(|_| Error::ResourceLimit("body segment allocation"))?;
        segments.push(segment);
        Self::from_segments(segments)
    }

    pub(crate) fn from_segments(segments: Vec<Segment>) -> Result<Self> {
        let len = segments.iter().try_fold(0usize, |total, segment| {
            total
                .checked_add(segment.bytes().len())
                .filter(|len| *len <= MAX_BODY_BYTES)
                .ok_or(Error::InvalidInput(
                    "native body length exceeds frame capacity",
                ))
        })?;
        if len == 0 {
            return Ok(Self::empty());
        }
        Ok(Self {
            owner: Some(Arc::new(segments)),
            range: 0..len,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.range.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> Result<Self> {
        check_range(&range, self.len())?;
        Ok(Self {
            owner: self.owner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = &[u8]> {
        self.visible_segments()
            .map(|(segment, range)| &segment.bytes()[range])
    }

    pub(crate) fn owned_segments(&self) -> impl Iterator<Item = Segment> + '_ {
        self.visible_segments().map(|(segment, range)| Segment {
            owner: segment.owner.clone(),
            range: segment.range.start + range.start..segment.range.start + range.end,
        })
    }

    fn visible_segments(&self) -> impl Iterator<Item = (&Segment, Range<usize>)> {
        let mut position = 0;
        self.owner
            .iter()
            .flat_map(|owner| owner.iter())
            .filter_map(move |segment| {
                let start = position;
                position += segment.bytes().len();
                let left = self.range.start.max(start);
                let right = self.range.end.min(position);
                (left < right).then(|| (segment, left - start..right - start))
            })
    }
}

fn check_range(range: &Range<usize>, len: usize) -> Result<()> {
    if range.start > range.end || range.end > len {
        return Err(Error::InvalidInput("body slice range out of bounds"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
