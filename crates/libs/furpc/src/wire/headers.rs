//! Ordered header rules follow Service Fabric revision
//! `51a7703a1f83749a9234d3ceb4296d8622e42cca`: `Transport/MessageHeaders.{h,cpp}`,
//! `MessageHeaderId.h`, the common `*Header.h` schemas, and
//! `Common/FabricSerializationHelper.h`.
//!
//! Input storage is borrowed, not copied or admitted. All common occurrences
//! are eagerly validated; unknown/deleted payloads stay opaque. Native strings
//! remain borrowed UTF-16 bytes. Only fubst's bounded traversal scratch may
//! allocate; shared message ownership is implemented above this borrowed parser.
//!
//! Missing MessageId/RelatesTo components are explicitly unsupported for now:
//! native initializes them with process-local generated IDs, not zero values.

#![allow(
    dead_code,
    reason = "Private transport awaits public connection integration"
)]

use std::fmt;

use fubst::{Decoder, Limits};

pub(crate) const DELETED: u16 = 0x8000;
pub(crate) const ACTOR: u16 = 0x8001;
pub(crate) const ACTION: u16 = 0x8002;
pub(crate) const MESSAGE_ID: u16 = 0x8003;
pub(crate) const RELATES_TO: u16 = 0x8004;
pub(crate) const EXPECTS_REPLY: u16 = 0x8005;
pub(crate) const RETRY: u16 = 0x8006;
pub(crate) const FAULT: u16 = 0x8007;
pub(crate) const IDEMPOTENT: u16 = 0x8008;
pub(crate) const HIGH_PRIORITY: u16 = 0x8009;
pub(crate) const UNCORRELATED_REPLY: u16 = 0x803e;

const MAX_NATIVE_HEADER_BYTES: usize = u16::MAX as usize;
const RECORD_PREFIX_LEN: usize = 4;

pub(crate) type HeaderResult<T> = Result<T, HeaderError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeaderError {
    Protocol(&'static str),
    InvalidInput(&'static str),
    ResourceLimit(&'static str),
    Decode { id: u16, source: fubst::Error },
    UnsupportedSchema { id: u16, reason: &'static str },
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(message) => write!(f, "native header protocol: {message}"),
            Self::InvalidInput(message) => write!(f, "invalid header limits: {message}"),
            Self::ResourceLimit(resource) => write!(f, "native header resource limit: {resource}"),
            Self::Decode { id, source } => write!(f, "native header {id:#06x}: {source}"),
            Self::UnsupportedSchema { id, reason } => {
                write!(f, "unsupported native header {id:#06x} schema: {reason}")
            }
        }
    }
}

impl std::error::Error for HeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Per-validation policy, to be derived from the scope's TransportLimits.
/// These are not additional native wire rules or application-body settings.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeaderDecodeLimits {
    pub(crate) max_bytes: usize,
    pub(crate) max_records: usize,
    pub(crate) max_depth: usize,
    /// Charge the complete record area once, then each interpreted common
    /// payload once, including duplicate payloads and their unknown suffixes.
    /// This bounds input work, not individual CPU instructions or later view use.
    pub(crate) max_work_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HeaderDecodeContext {
    limits: HeaderDecodeLimits,
}

impl HeaderDecodeContext {
    pub(crate) fn native() -> Self {
        Self {
            limits: HeaderDecodeLimits {
                max_bytes: MAX_NATIVE_HEADER_BYTES,
                max_records: usize::MAX,
                max_depth: usize::MAX,
                max_work_bytes: usize::MAX,
            },
        }
    }

    pub(crate) fn new(limits: HeaderDecodeLimits) -> HeaderResult<Self> {
        if limits.max_bytes > MAX_NATIVE_HEADER_BYTES {
            return Err(HeaderError::InvalidInput("header byte limit exceeds u16"));
        }
        if [limits.max_records, limits.max_depth, limits.max_work_bytes].contains(&usize::MAX) {
            return Err(HeaderError::InvalidInput("decode limits must be finite"));
        }
        Ok(Self { limits })
    }

    pub(crate) fn max_records(&self) -> usize {
        self.limits.max_records
    }

    pub(crate) fn control_limits(&self, max_bytes: usize) -> Limits {
        Limits {
            max_bytes,
            max_depth: self.limits.max_depth,
        }
    }

    pub(crate) fn validate_area(&self, len: usize) -> HeaderResult<()> {
        if len > MAX_NATIVE_HEADER_BYTES {
            return Err(HeaderError::Protocol("combined header area exceeds u16"));
        }
        if len > self.limits.max_bytes {
            return Err(HeaderError::ResourceLimit("encoded header bytes"));
        }
        if len > self.limits.max_work_bytes {
            return Err(HeaderError::ResourceLimit("decode work bytes"));
        }
        Ok(())
    }
}

/// An unaligned little-endian view; no normalization or surrogate validation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeUtf16<'a>(&'a [u8]);

impl<'a> NativeUtf16<'a> {
    pub(crate) fn as_le_bytes(self) -> &'a [u8] {
        self.0
    }

    pub(crate) fn units(self) -> impl ExactSizeIterator<Item = u16> + Clone + 'a {
        self.0
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_le_bytes(*pair))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub struct MessageId {
    /// Native GUID field order, not UUID network order.
    #[serde(with = "fubst::serde::guid")]
    pub guid: [u8; 16],
    pub index: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct Fault {
    pub code: i64,
    pub has_body: bool,
}

/// Effective received values after a complete ordered scan, not first matches.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommonHeaders<'a> {
    pub(crate) actor: i64,
    pub(crate) action: NativeUtf16<'a>,
    pub(crate) message_id: MessageId,
    pub(crate) relates_to: MessageId,
    pub(crate) retry_count: i32,
    pub(crate) expects_reply: bool,
    pub(crate) idempotent: bool,
    pub(crate) high_priority: bool,
    pub(crate) fault: Fault,
    pub(crate) uncorrelated_reply: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeaderRef<'a> {
    id: u16,
    value: &'a [u8],
}

impl<'a> HeaderRef<'a> {
    pub(crate) fn new(id: u16, value: &'a [u8]) -> Self {
        Self { id, value }
    }

    pub(crate) fn id(self) -> u16 {
        self.id
    }

    pub(crate) fn value(self) -> &'a [u8] {
        self.value
    }

    pub(crate) fn is_deleted(self) -> bool {
        self.id == DELETED
    }

    fn read<T>(
        self,
        context: &HeaderDecodeContext,
        remaining_work: &mut usize,
        fields: impl FnOnce(&mut Decoder<'a>) -> fubst::Result<T>,
    ) -> HeaderResult<T> {
        *remaining_work = remaining_work
            .checked_sub(self.value.len())
            .ok_or(HeaderError::ResourceLimit("decode work bytes"))?;
        let decode = || {
            let mut decoder = Decoder::with_limits(
                self.value,
                Limits {
                    max_bytes: context.limits.max_bytes,
                    max_depth: context.limits.max_depth,
                },
            )?;
            let value = decoder.object(fields)?;
            decoder.finish()?;
            Ok(value)
        };
        decode().map_err(|source| HeaderError::Decode {
            id: self.id,
            source,
        })
    }

    fn read_message_id(
        self,
        context: &HeaderDecodeContext,
        remaining_work: &mut usize,
    ) -> HeaderResult<MessageId> {
        let (guid, index) = self.read(context, remaining_work, |decoder| {
            Ok((
                trailing(decoder, Decoder::guid)?,
                trailing(decoder, Decoder::u32)?,
            ))
        })?;
        match (guid, index) {
            (Some(guid), Some(index)) => Ok(MessageId { guid, index }),
            _ => Err(HeaderError::UnsupportedSchema {
                id: self.id,
                reason: "missing ID components require native generated defaults",
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct HeaderProperties<'a> {
    pub(crate) effective: CommonHeaders<'a>,
    pub(crate) first_actor: Option<i64>,
    pub(crate) first_action: Option<NativeUtf16<'a>>,
}

impl<'a> HeaderProperties<'a> {
    pub(crate) fn decode(
        encoded_len: usize,
        records: impl Iterator<Item = HeaderResult<HeaderRef<'a>>>,
        context: &HeaderDecodeContext,
    ) -> HeaderResult<Self> {
        if encoded_len > MAX_NATIVE_HEADER_BYTES {
            return Err(HeaderError::Protocol("combined header area exceeds u16"));
        }
        if encoded_len > context.limits.max_bytes {
            return Err(HeaderError::ResourceLimit("encoded header bytes"));
        }
        let mut remaining_work = context
            .limits
            .max_work_bytes
            .checked_sub(encoded_len)
            .ok_or(HeaderError::ResourceLimit("decode work bytes"))?;
        let mut headers = Self {
            effective: CommonHeaders::default(),
            first_actor: None,
            first_action: None,
        };
        let mut actual_len = 0usize;
        for (count, record) in records.enumerate() {
            let record = record?;
            if record.value.is_empty() || record.value.len() > MAX_NATIVE_HEADER_BYTES {
                return Err(HeaderError::Protocol("invalid record length"));
            }
            actual_len = actual_len
                .checked_add(RECORD_PREFIX_LEN + record.value.len())
                .filter(|len| *len <= encoded_len)
                .ok_or(HeaderError::InvalidInput("record area length mismatch"))?;
            if count == context.limits.max_records {
                return Err(HeaderError::ResourceLimit("record count"));
            }
            match record.id {
                ACTOR => {
                    let value = record.read(context, &mut remaining_work, |decoder| {
                        Ok(trailing(decoder, Decoder::i64)?.unwrap_or(0))
                    })?;
                    headers.first_actor.get_or_insert(value);
                    headers.effective.actor = value;
                }
                ACTION => {
                    let value = record.read(context, &mut remaining_work, |decoder| {
                        Ok(NativeUtf16(
                            trailing(decoder, Decoder::string_utf16_bytes)?.unwrap_or(&[]),
                        ))
                    })?;
                    headers.first_action.get_or_insert(value);
                    headers.effective.action = value;
                }
                MESSAGE_ID => {
                    headers.effective.message_id =
                        record.read_message_id(context, &mut remaining_work)?;
                }
                RELATES_TO => {
                    headers.effective.relates_to =
                        record.read_message_id(context, &mut remaining_work)?;
                }
                RETRY => {
                    headers.effective.retry_count =
                        record.read(context, &mut remaining_work, |decoder| {
                            Ok(trailing(decoder, Decoder::i32)?.unwrap_or(0))
                        })?;
                }
                EXPECTS_REPLY | IDEMPOTENT | HIGH_PRIORITY | UNCORRELATED_REPLY => {
                    // These three native header constructors default to true,
                    // unlike the absent-header effective properties.
                    let value = record.read(context, &mut remaining_work, |decoder| {
                        Ok(trailing(decoder, Decoder::boolean)?
                            .unwrap_or(record.id != EXPECTS_REPLY))
                    })?;
                    match record.id {
                        EXPECTS_REPLY => headers.effective.expects_reply = value,
                        IDEMPOTENT => headers.effective.idempotent = value,
                        HIGH_PRIORITY => headers.effective.high_priority = value,
                        UNCORRELATED_REPLY => headers.effective.uncorrelated_reply = value,
                        _ => unreachable!(),
                    }
                }
                FAULT => {
                    headers.effective.fault =
                        record.read(context, &mut remaining_work, |decoder| {
                            Ok(Fault {
                                code: trailing(decoder, Decoder::i64)?.unwrap_or(0),
                                has_body: trailing(decoder, Decoder::boolean)?.unwrap_or(false),
                            })
                        })?;
                }
                _ => {}
            }
        }
        if actual_len != encoded_len {
            return Err(HeaderError::InvalidInput("record area length mismatch"));
        }
        Ok(headers)
    }
}

#[derive(Debug)]
pub(crate) struct Headers<'a> {
    encoded: &'a [u8],
    properties: HeaderProperties<'a>,
}

impl<'a> Headers<'a> {
    pub(crate) fn decode(encoded: &'a [u8], context: &HeaderDecodeContext) -> HeaderResult<Self> {
        Ok(Self {
            encoded,
            properties: HeaderProperties::decode(
                encoded.len(),
                Self::encoded_records(encoded),
                context,
            )?,
        })
    }

    pub(crate) fn encoded_records(
        mut encoded: &[u8],
    ) -> impl Iterator<Item = HeaderResult<HeaderRef<'_>>> {
        std::iter::from_fn(move || match take_record(&mut encoded) {
            Ok(record) => record.map(Ok),
            Err(error) => {
                encoded = &[];
                Some(Err(error))
            }
        })
    }

    pub(crate) fn encoded(&self) -> &'a [u8] {
        self.encoded
    }

    /// Physical records, including deletion markers and opaque unknown values.
    pub(crate) fn raw_records(&self) -> impl Iterator<Item = HeaderRef<'a>> + 'a {
        HeaderRecords {
            remaining: self.encoded,
        }
    }

    /// Visible records in native iteration order; no compaction of encoded bytes.
    pub(crate) fn records(&self) -> impl Iterator<Item = HeaderRef<'a>> + 'a {
        self.raw_records().filter(|record| !record.is_deleted())
    }

    pub(crate) fn first_raw(&self, id: u16) -> Option<HeaderRef<'a>> {
        self.records().find(|record| record.id == id)
    }

    pub(crate) fn first_actor(&self) -> Option<i64> {
        self.properties.first_actor
    }

    pub(crate) fn first_action(&self) -> Option<NativeUtf16<'a>> {
        self.properties.first_action
    }

    pub(crate) fn effective(&self) -> &CommonHeaders<'a> {
        &self.properties.effective
    }
}

fn trailing<'a, T>(
    decoder: &mut Decoder<'a>,
    read: impl FnOnce(&mut Decoder<'a>) -> fubst::Result<T>,
) -> fubst::Result<Option<T>> {
    if decoder.is_scope_end()? {
        Ok(None)
    } else {
        read(decoder).map(Some)
    }
}

fn take_record<'a>(input: &mut &'a [u8]) -> HeaderResult<Option<HeaderRef<'a>>> {
    if input.is_empty() {
        return Ok(None);
    }
    let prefix = input
        .get(..RECORD_PREFIX_LEN)
        .ok_or(HeaderError::Protocol("truncated record prefix"))?;
    let id = u16::from_le_bytes([prefix[0], prefix[1]]);
    let size = usize::from(u16::from_le_bytes([prefix[2], prefix[3]]));
    let payload = &input[RECORD_PREFIX_LEN..];
    if size == 0 || size > payload.len() {
        return Err(HeaderError::Protocol("invalid record length"));
    }
    let (value, remaining) = payload.split_at(size);
    *input = remaining;
    Ok(Some(HeaderRef { id, value }))
}

struct HeaderRecords<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for HeaderRecords<'a> {
    type Item = HeaderRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        take_record(&mut self.remaining).expect("immutable records were validated during decoding")
    }
}

#[cfg(test)]
mod tests;
