//! Ordered native headers and standalone constructors. Header data grants no
//! permission to dispatch, send, correlate, or reply.

use std::sync::Arc;

use serde::Serialize;

use crate::{
    body::Segment,
    wire::headers::{self as wire, HeaderDecodeContext, HeaderProperties},
    Error, Fault, MessageId, Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeaderId(u16);

impl HeaderId {
    pub const fn new(value: u16) -> Result<Self> {
        if value == wire::DELETED {
            return Err(Error::InvalidInput("deletion marker is reserved"));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A native numeric Actor code; unknown representable values are preserved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ActorId(i64);

impl ActorId {
    pub fn new(value: impl TryInto<i64>) -> Result<Self> {
        value
            .try_into()
            .map(Self)
            .map_err(|_| Error::InvalidInput("Actor code exceeds native Int64"))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

pub const ACTOR: HeaderId = HeaderId(wire::ACTOR);
pub const ACTION: HeaderId = HeaderId(wire::ACTION);
pub const MESSAGE_ID: HeaderId = HeaderId(wire::MESSAGE_ID);
pub const RELATES_TO: HeaderId = HeaderId(wire::RELATES_TO);
pub const EXPECTS_REPLY: HeaderId = HeaderId(wire::EXPECTS_REPLY);
pub const RETRY: HeaderId = HeaderId(wire::RETRY);
pub const FAULT: HeaderId = HeaderId(wire::FAULT);
pub const IDEMPOTENT: HeaderId = HeaderId(wire::IDEMPOTENT);
pub const HIGH_PRIORITY: HeaderId = HeaderId(wire::HIGH_PRIORITY);
pub const UNCORRELATED_REPLY: HeaderId = HeaderId(wire::UNCORRELATED_REPLY);

#[derive(Serialize)]
struct Scalar<T> {
    value: T,
}

pub fn actor(value: ActorId) -> Result<HeaderRecord> {
    HeaderRecord::native(ACTOR, &Scalar { value: value.get() })
}

pub fn action(value: &str) -> Result<HeaderRecord> {
    HeaderRecord::native(ACTION, &Scalar { value })
}

/// One immutable record. Forwarded records share their original byte storage.
#[derive(Debug, Clone)]
pub struct HeaderRecord {
    id: HeaderId,
    value: Segment,
}

impl HeaderRecord {
    pub fn native<T: Serialize + ?Sized>(id: HeaderId, value: &T) -> Result<Self> {
        if id.0 == wire::DELETED {
            return Err(Error::InvalidInput("deletion marker is reserved"));
        }
        let encoded = fubst::serde::to_vec_with_limits(
            value,
            fubst::Limits {
                max_bytes: u16::MAX as usize - 4,
                ..fubst::Limits::default()
            },
        )
        .map_err(|source| Error::NativeHeader { id: id.0, source })?;
        if encoded.first() != Some(&0) {
            return Err(Error::InvalidInput(
                "native header requires an object schema",
            ));
        }
        Self::from_encoded(id, encoded)
    }

    /// Validate recognized schemas; unknown payloads remain opaque.
    pub fn from_encoded(id: HeaderId, encoded_native_object: Vec<u8>) -> Result<Self> {
        if id.0 == wire::DELETED {
            return Err(Error::InvalidInput("deletion marker is reserved"));
        }
        if encoded_native_object.is_empty() || encoded_native_object.len() > u16::MAX as usize - 4 {
            return Err(Error::InvalidInput("native header record length"));
        }
        HeaderProperties::decode(
            encoded_native_object.len() + 4,
            std::iter::once(Ok(wire::HeaderRef::new(id.0, &encoded_native_object))),
            &HeaderDecodeContext::native(),
        )?;
        Ok(Self {
            id,
            value: Segment::from_bytes(encoded_native_object),
        })
    }

    pub fn id(&self) -> HeaderId {
        self.id
    }
    pub fn value(&self) -> &[u8] {
        self.value.bytes()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeaderRef<'a>(&'a HeaderRecord);

impl<'a> HeaderRef<'a> {
    pub fn id(self) -> HeaderId {
        self.0.id
    }
    pub fn value(self) -> &'a [u8] {
        self.0.value()
    }
    pub fn is_deleted(self) -> bool {
        self.0.id.0 == wire::DELETED
    }
    pub fn to_owned(self) -> HeaderRecord {
        self.0.clone()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CommonHeaders {
    pub actor: ActorId,
    pub message_id: MessageId,
    pub relates_to: MessageId,
    pub retry_count: i32,
    pub expects_reply: bool,
    pub idempotent: bool,
    pub high_priority: bool,
    pub fault: Fault,
    pub uncorrelated_reply: bool,
    action: Vec<u16>,
}

impl CommonHeaders {
    pub fn action(&self) -> &[u16] {
        &self.action
    }
}

static EMPTY_COMMON: CommonHeaders = CommonHeaders {
    actor: ActorId(0),
    message_id: MessageId {
        guid: [0; 16],
        index: 0,
    },
    relates_to: MessageId {
        guid: [0; 16],
        index: 0,
    },
    retry_count: 0,
    expects_reply: false,
    idempotent: false,
    high_priority: false,
    fault: Fault {
        code: 0,
        has_body: false,
    },
    uncorrelated_reply: false,
    action: Vec::new(),
};

#[derive(Debug)]
struct HeaderData {
    records: Vec<HeaderRecord>,
    encoded_len: usize,
    common: CommonHeaders,
    first_actor: Option<ActorId>,
    first_action: Option<Vec<u16>>,
}

/// Ordered shared records with atomic, fallible copy-on-write edits.
#[derive(Debug, Default, Clone)]
pub struct Headers {
    owner: Option<Arc<HeaderData>>,
}

impl Headers {
    pub const fn new() -> Self {
        Self { owner: None }
    }

    pub fn records(&self) -> impl Iterator<Item = HeaderRef<'_>> {
        self.raw_records().filter(|record| !record.is_deleted())
    }

    pub fn raw_records(&self) -> impl Iterator<Item = HeaderRef<'_>> {
        self.owner
            .iter()
            .flat_map(|owner| owner.records.iter())
            .map(HeaderRef)
    }

    pub fn first_raw(&self, id: HeaderId) -> Option<HeaderRef<'_>> {
        self.records().find(|record| record.id() == id)
    }

    pub fn first_actor(&self) -> Option<ActorId> {
        self.owner.as_ref().and_then(|owner| owner.first_actor)
    }

    pub fn first_action(&self) -> Option<&[u16]> {
        self.owner
            .as_ref()
            .and_then(|owner| owner.first_action.as_deref())
    }

    pub fn effective(&self) -> &CommonHeaders {
        self.owner
            .as_ref()
            .map_or(&EMPTY_COMMON, |owner| &owner.common)
    }

    pub fn append(&mut self, record: HeaderRecord) -> Result<()> {
        self.encoded_len()
            .checked_add(4 + record.value().len())
            .filter(|len| *len <= u16::MAX as usize)
            .ok_or(Error::InvalidInput("native header area exceeds u16"))?;
        let mut records = Vec::new();
        reserve_records(&mut records, self.raw_records().count() + 1)?;
        records.extend(self.raw_records().map(HeaderRef::to_owned));
        records.push(record);
        *self = Self::from_records(records, &HeaderDecodeContext::native())?;
        Ok(())
    }

    pub fn remove_all(&mut self, id: HeaderId) -> Result<usize> {
        let removed = self.records().filter(|record| record.id() == id).count();
        if removed == 0 {
            return Ok(0);
        }
        let mut records = Vec::new();
        reserve_records(&mut records, self.raw_records().count() - removed)?;
        records.extend(
            self.raw_records()
                .filter(|record| record.is_deleted() || record.id() != id)
                .map(HeaderRef::to_owned),
        );
        *self = Self::from_records(records, &HeaderDecodeContext::native())?;
        Ok(removed)
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.owner.as_ref().map_or(0, |owner| owner.encoded_len)
    }

    pub(crate) fn validate(&self, context: &HeaderDecodeContext) -> Result<()> {
        HeaderProperties::decode(
            self.encoded_len(),
            self.raw_records()
                .map(|record| Ok(wire::HeaderRef::new(record.id().0, record.value()))),
            context,
        )?;
        Ok(())
    }

    #[allow(dead_code, reason = "Receive ownership awaits the connection driver")]
    pub(crate) fn from_received(area: Segment, context: &HeaderDecodeContext) -> Result<Self> {
        context.validate_area(area.bytes().len())?;
        let mut count = 0usize;
        for record in wire::Headers::encoded_records(area.bytes()) {
            record?;
            count += 1;
            if count > context.max_records() {
                return Err(Error::ResourceLimit("record count"));
            }
        }
        let mut records = Vec::new();
        reserve_records(&mut records, count)?;
        let mut offset = 0;
        for record in wire::Headers::encoded_records(area.bytes()) {
            let record = record?;
            let end = offset + 4 + record.value().len();
            records.push(HeaderRecord {
                id: HeaderId(record.id()),
                value: area.slice(offset + 4..end)?,
            });
            offset = end;
        }
        Self::from_records(records, context)
    }

    fn from_records(records: Vec<HeaderRecord>, context: &HeaderDecodeContext) -> Result<Self> {
        if records.is_empty() {
            return Ok(Self::new());
        }
        let encoded_len = records.iter().try_fold(0usize, |total, record| {
            total
                .checked_add(4 + record.value().len())
                .ok_or(Error::InvalidInput("native header size overflow"))
        })?;
        let properties = HeaderProperties::decode(
            encoded_len,
            records
                .iter()
                .map(|record| Ok(wire::HeaderRef::new(record.id.0, record.value()))),
            context,
        )?;
        let common = &properties.effective;
        let first_action = properties
            .first_action
            .map(|action| copy_units(action.units()))
            .transpose()?;
        let common = CommonHeaders {
            actor: ActorId(common.actor),
            action: copy_units(common.action.units())?,
            message_id: common.message_id,
            relates_to: common.relates_to,
            retry_count: common.retry_count,
            expects_reply: common.expects_reply,
            idempotent: common.idempotent,
            high_priority: common.high_priority,
            fault: common.fault,
            uncorrelated_reply: common.uncorrelated_reply,
        };
        let first_actor = properties.first_actor.map(ActorId);
        Ok(Self {
            owner: Some(Arc::new(HeaderData {
                records,
                encoded_len,
                common,
                first_actor,
                first_action,
            })),
        })
    }
}

fn reserve_records(records: &mut Vec<HeaderRecord>, count: usize) -> Result<()> {
    records
        .try_reserve_exact(count)
        .map_err(|_| Error::ResourceLimit("header record allocation"))
}

fn copy_units(units: impl ExactSizeIterator<Item = u16>) -> Result<Vec<u16>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(units.len())
        .map_err(|_| Error::ResourceLimit("header UTF-16 allocation"))?;
    result.extend(units);
    Ok(result)
}
