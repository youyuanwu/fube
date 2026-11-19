//! Application-body codecs, independent of native framing and correlation.
//!
//! Codecs are trusted, cooperative application code. Buffer limits restrict
//! encoded bytes, not arbitrary codec allocations or execution time.

use std::{fmt, marker::PhantomData};

use serde::{de::DeserializeOwned, Serialize};

use crate::{body::MAX_BODY_BYTES, Body, BoxError, Error, ErrorSource};

type Result<T> = std::result::Result<T, CodecError>;

/// A call-local body failure. The enclosing [`Error::Codec`] records its phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    InvalidPayload { source: ErrorSource },
    ResourceLimit { source: ErrorSource },
}

impl CodecError {
    pub fn invalid_payload(source: impl Into<BoxError>) -> Self {
        Self::InvalidPayload {
            source: ErrorSource::new(source),
        }
    }

    pub fn resource_limit(source: impl Into<BoxError>) -> Self {
        Self::ResourceLimit {
            source: ErrorSource::new(source),
        }
    }

    fn storage(error: Error) -> Self {
        match error {
            Error::ResourceLimit(_) => Self::resource_limit(error),
            _ => Self::invalid_payload(error),
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayload { source } => {
                write!(f, "invalid application payload: {}", source.as_error())
            }
            Self::ResourceLimit { source } => {
                write!(f, "application codec resource limit: {}", source.as_error())
            }
        }
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidPayload { source } | Self::ResourceLimit { source } => {
                Some(source.as_error())
            }
        }
    }
}

impl From<fubst::Error> for CodecError {
    fn from(error: fubst::Error) -> Self {
        match error {
            fubst::Error::ResourceLimit(_) => Self::resource_limit(error),
            _ => Self::invalid_payload(error),
        }
    }
}

/// Locally sent and received types may differ. No Serde bounds are required.
pub trait Codec {
    type Encode;
    type Decode;
    type Encoder: Encoder<Item = Self::Encode>;
    type Decoder: Decoder<Item = Self::Decode>;

    fn encoder(&mut self) -> Self::Encoder;
    fn decoder(&mut self) -> Self::Decoder;
}

pub trait Encoder {
    type Item;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<()>;
}

pub trait Decoder {
    type Item;

    /// Decode one whole body, consuming or explicitly skipping all its bytes.
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Self::Item>;
}

/// Fallible byte writes and shared-body composition for one encoded value.
pub struct EncodeBuf<'a> {
    segments: &'a mut Vec<Body>,
    pending: Vec<u8>,
    len: usize,
    max_bytes: usize,
}

impl EncodeBuf<'_> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn remaining(&self) -> usize {
        self.max_bytes - self.len
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.check_additional(bytes.len())?;
        self.pending
            .try_reserve(bytes.len())
            .map_err(CodecError::resource_limit)?;
        self.pending.extend_from_slice(bytes);
        self.len += bytes.len();
        Ok(())
    }

    /// Adopt shared visible segments without copying their payload.
    pub fn append_body(&mut self, body: Body) -> Result<()> {
        self.check_additional(body.len())?;
        if body.is_empty() {
            return Ok(());
        }
        self.flush()?;
        self.segments
            .try_reserve(1)
            .map_err(CodecError::resource_limit)?;
        self.len += body.len();
        self.segments.push(body);
        Ok(())
    }

    fn check_additional(&self, len: usize) -> Result<()> {
        if len > self.remaining() {
            return Err(CodecError::resource_limit("encoded body bytes"));
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.pending.is_empty() {
            self.segments
                .try_reserve(1)
                .map_err(CodecError::resource_limit)?;
            let body =
                Body::from_bytes(std::mem::take(&mut self.pending)).map_err(CodecError::storage)?;
            self.segments.push(body);
        }
        Ok(())
    }
}

pub(crate) fn encode<E: Encoder>(encoder: &mut E, item: E::Item, max_bytes: usize) -> Result<Body> {
    let mut segments = Vec::new();
    let mut buf = EncodeBuf {
        segments: &mut segments,
        pending: Vec::new(),
        len: 0,
        max_bytes: max_bytes.min(MAX_BODY_BYTES),
    };
    encoder.encode(item, &mut buf)?;
    buf.flush()?;
    Body::from_segments(segments).map_err(CodecError::storage)
}

/// A cursor over exactly one complete body, with zero-copy shared views.
pub struct DecodeBuf<'a> {
    body: &'a Body,
    position: usize,
}

impl<'a> DecodeBuf<'a> {
    pub fn new(body: &'a Body) -> Self {
        Self { body, position: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.body.len() - self.position
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Borrow the next contiguous portion, which may be shorter than remaining.
    pub fn chunk(&self) -> &'a [u8] {
        self.segments().next().unwrap_or_default()
    }

    pub fn advance(&mut self, count: usize) -> Result<()> {
        if count > self.remaining() {
            return Err(CodecError::invalid_payload("incomplete application body"));
        }
        self.position += count;
        Ok(())
    }

    pub fn take(&mut self, count: usize) -> Result<Body> {
        if count > self.remaining() {
            return Err(CodecError::invalid_payload("incomplete application body"));
        }
        let body = self
            .body
            .slice(self.position..self.position + count)
            .map_err(CodecError::storage)?;
        self.position += count;
        Ok(body)
    }

    pub fn copy_to_slice(&mut self, dst: &mut [u8]) -> Result<()> {
        if dst.len() > self.remaining() {
            return Err(CodecError::invalid_payload("incomplete application body"));
        }
        let mut copied = 0;
        for bytes in self.segments() {
            let count = bytes.len().min(dst.len() - copied);
            dst[copied..copied + count].copy_from_slice(&bytes[..count]);
            copied += count;
            if copied == dst.len() {
                break;
            }
        }
        self.position += copied;
        Ok(())
    }

    /// Fallibly copy and consume the remaining input. No allocation charge follows it.
    pub fn copy_remaining(&mut self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.remaining())
            .map_err(CodecError::resource_limit)?;
        for segment in self.segments() {
            bytes.extend_from_slice(segment);
        }
        self.position = self.body.len();
        Ok(bytes)
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if !self.is_empty() {
            return Err(CodecError::invalid_payload(
                "unconsumed application body bytes",
            ));
        }
        Ok(())
    }

    fn segments(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        let mut skip = self.position;
        self.body.segments().filter_map(move |bytes| {
            let count = skip.min(bytes.len());
            skip -= count;
            (count < bytes.len()).then(|| &bytes[count..])
        })
    }
}

pub(crate) fn decode<D: Decoder>(decoder: &mut D, body: &Body) -> Result<D::Item> {
    let mut buf = DecodeBuf::new(body);
    let value = decoder.decode(&mut buf)?;
    buf.finish()?;
    Ok(value)
}

/// Native Fabric body format with the schemas/adapters supported by `fubst`.
///
/// Defaults add no depth or work ceiling. Optional codec-local limits do not
/// affect native headers, connection controls, or transport Fault decoding.
pub struct FubstCodec<Out, In> {
    limits: fubst::Limits,
    marker: PhantomData<fn(Out) -> In>,
}

impl<Out, In> FubstCodec<Out, In> {
    pub fn with_limits(limits: fubst::Limits) -> Self {
        Self {
            limits,
            marker: PhantomData,
        }
    }
}

impl<Out, In> Default for FubstCodec<Out, In> {
    fn default() -> Self {
        Self::with_limits(fubst::Limits::default())
    }
}

pub struct FubstEncoder<T> {
    limits: fubst::Limits,
    marker: PhantomData<fn(T)>,
}

pub struct FubstDecoder<T> {
    limits: fubst::Limits,
    marker: PhantomData<fn() -> T>,
}

impl<Out: Serialize, In: DeserializeOwned> Codec for FubstCodec<Out, In> {
    type Encode = Out;
    type Decode = In;
    type Encoder = FubstEncoder<Out>;
    type Decoder = FubstDecoder<In>;

    fn encoder(&mut self) -> Self::Encoder {
        FubstEncoder {
            limits: self.limits,
            marker: PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        FubstDecoder {
            limits: self.limits,
            marker: PhantomData,
        }
    }
}

impl<T: Serialize> Encoder for FubstEncoder<T> {
    type Item = T;

    fn encode(&mut self, item: T, dst: &mut EncodeBuf<'_>) -> Result<()> {
        let limits = fubst::Limits {
            max_bytes: self.limits.max_bytes.min(dst.remaining()),
            ..self.limits
        };
        let bytes = fubst::serde::to_vec_with_limits(&item, limits)?;
        dst.append_body(Body::from_bytes(bytes).map_err(CodecError::storage)?)
    }
}

impl<T: DeserializeOwned> Decoder for FubstDecoder<T> {
    type Item = T;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<T> {
        let len = src.remaining();
        if len > self.limits.max_bytes {
            return Err(fubst::Error::ResourceLimit("encoded size").into());
        }
        let chunk = src.chunk();
        if chunk.len() == len {
            let value = fubst::serde::from_slice_with_limits(chunk, self.limits)?;
            src.advance(len)?;
            Ok(value)
        } else {
            fubst::serde::from_slice_with_limits(&src.copy_remaining()?, self.limits)
                .map_err(Into::into)
        }
    }
}

#[cfg(test)]
mod tests;
