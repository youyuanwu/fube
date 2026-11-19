//! Ordered native headers and standalone constructors. Headers carry data, not
//! permission to dispatch, send, correlate, or reply. See [`crate::construction`]
//! for finite construction policy; submission must apply its own scope policy.

use std::{mem::size_of, sync::Arc};

use serde::Serialize;

use crate::{
    body::Segment,
    construction as policy,
    resource::{AllocationRef, SharedAllocation},
    wire::headers::{self as wire, HeaderDecodeContext, HeaderProperties},
    Error, Fault, MessageId, Result,
};

/// A native 16-bit record identifier, not a routing or authority capability.
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

/// A native numeric Actor code. Unknown codes are preserved, not whitelisted.
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

/// Encode Rust text as native UTF-16 without normalization.
pub fn action(value: &str) -> Result<HeaderRecord> {
    HeaderRecord::native(ACTION, &Scalar { value })
}

/// One immutable record. Clones and forwarded received records keep leases.
#[derive(Debug, Clone)]
pub struct HeaderRecord {
    id: HeaderId,
    value: Segment,
}

impl HeaderRecord {
    /// Encode a native object schema, not a scalar or an application-body codec.
    pub fn native<T: Serialize + ?Sized>(id: HeaderId, value: &T) -> Result<Self> {
        if id.0 == wire::DELETED {
            return Err(Error::InvalidInput("deletion marker is reserved"));
        }
        let encoded = fubst::serde::to_vec_with_limits_and_work(
            value,
            fubst::Limits {
                max_bytes: policy::MAX_HEADER_BYTES - 4,
                max_depth: policy::MAX_NATIVE_DEPTH,
            },
            policy::MAX_NATIVE_FIELDS_AND_ELEMENTS,
        )
        .map_err(|source| Error::NativeHeader { id: id.0, source })?;
        if encoded.first() != Some(&0) {
            return Err(Error::InvalidInput(
                "native header requires an object schema",
            ));
        }
        Self::from_encoded(id, encoded)
    }

    /// Adopt native-schema bytes without certifying an unknown schema.
    /// Recognized common headers are validated; unknown payloads stay opaque.
    pub fn from_encoded(id: HeaderId, encoded_native_object: Vec<u8>) -> Result<Self> {
        if id.0 == wire::DELETED {
            return Err(Error::InvalidInput("deletion marker is reserved"));
        }
        if encoded_native_object.is_empty()
            || encoded_native_object.len() > policy::MAX_HEADER_BYTES - 4
        {
            return Err(Error::InvalidInput("native header record length"));
        }
        if SharedAllocation::<Vec<u8>>::minimum_bytes(
            encoded_native_object.capacity(),
            policy::MAX_ALLOCATION_LEDGERS,
        )? > policy::MAX_HEADER_RETAINED_BYTES
        {
            return Err(Error::ResourceLimit("retained header bytes"));
        }
        HeaderProperties::decode(
            encoded_native_object.len() + 4,
            std::iter::once(Ok(wire::HeaderRef::new(id.0, &encoded_native_object))),
            &policy::header_context(),
        )?;
        Ok(Self {
            id,
            value: Segment::from_bytes(encoded_native_object, policy::HEADER_STORAGE_LIMITS)?,
        })
    }

    pub fn id(&self) -> HeaderId {
        self.id
    }

    pub fn value(&self) -> &[u8] {
        self.value.bytes()
    }
}

/// Borrowed record inspection. Owned forwarding retains the original leases.
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

/// Native effective properties, distinct from first-visible record lookup.
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
    unique: Vec<usize>,
    encoded_len: usize,
    backing_bytes: usize,
    common: CommonHeaders,
    first_actor: Option<ActorId>,
    first_action: Option<Vec<u16>>,
}

/// Ordered immutable record storage with fallible copy-on-write edits.
/// A raw [`crate::Message`] exposes only a shared reference to this collection.
#[derive(Debug, Default, Clone)]
pub struct Headers {
    owner: Option<Arc<SharedAllocation<HeaderData>>>,
}

impl Headers {
    pub const fn new() -> Self {
        Self { owner: None }
    }

    pub fn records(&self) -> impl Iterator<Item = HeaderRef<'_>> {
        self.raw_records().filter(|record| !record.is_deleted())
    }

    /// Include preserved physical deletion markers as well as visible records.
    pub fn raw_records(&self) -> impl Iterator<Item = HeaderRef<'_>> {
        self.owner
            .iter()
            .flat_map(|owner| owner.data().records.iter())
            .map(HeaderRef)
    }

    pub fn first_raw(&self, id: HeaderId) -> Option<HeaderRef<'_>> {
        self.records().find(|record| record.id() == id)
    }

    pub fn first_actor(&self) -> Option<ActorId> {
        self.owner
            .as_ref()
            .and_then(|owner| owner.data().first_actor)
    }

    pub fn first_action(&self) -> Option<&[u16]> {
        self.owner
            .as_ref()
            .and_then(|owner| owner.data().first_action.as_deref())
    }

    pub fn effective(&self) -> &CommonHeaders {
        self.owner
            .as_ref()
            .map_or(&EMPTY_COMMON, |owner| &owner.data().common)
    }

    pub fn append(&mut self, record: HeaderRecord) -> Result<()> {
        let count = self.raw_records().count();
        let next_count = count
            .checked_add(1)
            .filter(|count| *count <= policy::MAX_HEADER_RECORDS)
            .ok_or(Error::ResourceLimit("constructed header record count"))?;
        let encoded_len = self
            .encoded_len()
            .checked_add(4 + record.value().len())
            .filter(|len| *len <= policy::MAX_HEADER_BYTES)
            .ok_or(Error::ResourceLimit("constructed header bytes"))?;
        let old_backing = self
            .owner
            .as_ref()
            .map_or(0, |owner| owner.data().backing_bytes);
        let already_retained = self
            .raw_records()
            .any(|existing| existing.0.value.same_allocation(&record.value));
        let backing = old_backing
            .checked_add(if already_retained {
                0
            } else {
                record.value.retained_bytes()
            })
            .ok_or(Error::ResourceLimit("header backing size"))?;
        let allowance = policy::MAX_HEADER_RETAINED_BYTES
            .checked_sub(backing)
            .ok_or(Error::ResourceLimit("retained header bytes"))?;
        let action_units = if record.id == ACTION {
            HeaderProperties::decode(
                record.value().len() + 4,
                std::iter::once(Ok(wire::HeaderRef::new(record.id.0, record.value()))),
                &policy::header_context(),
            )?
            .effective
            .action
            .units()
            .len()
        } else {
            self.effective().action.len()
        };
        let first_units = self.first_action().map_or(
            if record.id == ACTION { action_units } else { 0 },
            <[u16]>::len,
        );
        let metadata = checked_bytes::<HeaderRecord>(next_count)?
            .checked_add(checked_bytes::<usize>(next_count)?)
            .and_then(|bytes| bytes.checked_add((first_units + action_units) * size_of::<u16>()))
            .ok_or(Error::ResourceLimit("header metadata size"))?;
        if SharedAllocation::<HeaderData>::minimum_bytes(metadata, policy::MAX_ALLOCATION_LEDGERS)?
            > allowance
        {
            return Err(Error::ResourceLimit("retained header bytes"));
        }
        let mut records = Vec::new();
        reserve_records(&mut records, next_count)?;
        records.extend(self.raw_records().map(HeaderRef::to_owned));
        records.push(record);
        let replacement = Self::from_records(
            records,
            &policy::header_context(),
            policy::MAX_HEADER_RETAINED_BYTES,
            policy::MAX_ALLOCATION_LEDGERS,
        )?;
        debug_assert_eq!(replacement.encoded_len(), encoded_len);
        *self = replacement;
        Ok(())
    }

    /// Remove every visible occurrence, preserving all other record order.
    /// Failure leaves this collection unchanged; absent IDs return zero.
    pub fn remove_all(&mut self, id: HeaderId) -> Result<usize> {
        let removed = self.records().filter(|record| record.id() == id).count();
        if removed == 0 {
            return Ok(0);
        }
        let count = self.raw_records().count() - removed;
        let mut records = Vec::new();
        reserve_records(&mut records, count)?;
        records.extend(
            self.raw_records()
                .filter(|record| record.is_deleted() || record.id() != id)
                .map(HeaderRef::to_owned),
        );
        let replacement = Self::from_records(
            records,
            &policy::header_context(),
            policy::MAX_HEADER_RETAINED_BYTES,
            policy::MAX_ALLOCATION_LEDGERS,
        )?;
        *self = replacement;
        Ok(removed)
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.owner
            .as_ref()
            .map_or(0, |owner| owner.data().encoded_len)
    }

    pub(crate) fn allocations(&self) -> impl Iterator<Item = AllocationRef<'_>> + Clone {
        self.owner.iter().flat_map(|owner| {
            std::iter::once(owner.allocation()).chain(
                owner
                    .data()
                    .unique
                    .iter()
                    .map(move |index| owner.data().records[*index].value.allocation()),
            )
        })
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

    #[allow(
        dead_code,
        reason = "Receive ownership seam for the upcoming frame reader"
    )]
    pub(crate) fn from_received(
        area: Segment,
        context: &HeaderDecodeContext,
        max_retained_bytes: usize,
        max_ledgers: usize,
    ) -> Result<Self> {
        context.validate_area(area.bytes().len())?;
        if area.retained_bytes() > max_retained_bytes {
            return Err(Error::ResourceLimit("retained header bytes"));
        }
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
        Self::from_records(records, context, max_retained_bytes, max_ledgers)
    }

    fn from_records(
        records: Vec<HeaderRecord>,
        context: &HeaderDecodeContext,
        max_retained_bytes: usize,
        max_ledgers: usize,
    ) -> Result<Self> {
        if records.is_empty() {
            return Ok(Self::new());
        }
        let encoded_len = records.iter().try_fold(0usize, |total, record| {
            total
                .checked_add(4 + record.value().len())
                .ok_or(Error::ResourceLimit("header size overflow"))
        })?;
        context.validate_area(encoded_len)?;
        if records.len() > context.max_records() {
            return Err(Error::ResourceLimit("record count"));
        }
        let record_bytes = checked_bytes::<HeaderRecord>(records.capacity())?;
        let index_bytes = checked_bytes::<usize>(records.len())?;
        let minimum = SharedAllocation::<HeaderData>::minimum_bytes(
            record_bytes
                .checked_add(index_bytes)
                .ok_or(Error::ResourceLimit("header metadata size"))?,
            max_ledgers,
        )?;
        if minimum > max_retained_bytes {
            return Err(Error::ResourceLimit("retained header bytes"));
        }
        let mut unique = Vec::new();
        unique
            .try_reserve_exact(records.len())
            .map_err(|_| Error::ResourceLimit("header index allocation"))?;
        unique.extend(0..records.len());
        unique.sort_unstable_by_key(|index| records[*index].value.allocation().identity());
        unique.dedup_by_key(|index| records[*index].value.allocation().identity());
        let backing_bytes = unique.iter().try_fold(0usize, |total, index| {
            total
                .checked_add(records[*index].value.retained_bytes())
                .ok_or(Error::ResourceLimit("header backing size"))
        })?;
        let allowance = max_retained_bytes
            .checked_sub(backing_bytes)
            .ok_or(Error::ResourceLimit("retained header bytes"))?;
        let properties = HeaderProperties::decode(
            encoded_len,
            records
                .iter()
                .map(|record| Ok(wire::HeaderRef::new(record.id.0, record.value()))),
            context,
        )?;
        let common = &properties.effective;
        let action_units = common.action.units().len();
        let first_units = properties
            .first_action
            .map_or(0, |action| action.units().len());
        let metadata = record_bytes
            .checked_add(checked_bytes::<usize>(unique.capacity())?)
            .and_then(|bytes| bytes.checked_add((action_units + first_units) * size_of::<u16>()))
            .ok_or(Error::ResourceLimit("header metadata size"))?;
        if SharedAllocation::<HeaderData>::minimum_bytes(metadata, max_ledgers)? > allowance {
            return Err(Error::ResourceLimit("retained header bytes"));
        }
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
        let metadata = record_bytes
            .checked_add(checked_bytes::<usize>(unique.capacity())?)
            .and_then(|bytes| bytes.checked_add(common.action.capacity() * size_of::<u16>()))
            .and_then(|bytes| {
                bytes.checked_add(
                    first_action
                        .as_ref()
                        .map_or(0, |action| action.capacity() * size_of::<u16>()),
                )
            })
            .ok_or(Error::ResourceLimit("header metadata size"))?;
        let owner = SharedAllocation::new(
            HeaderData {
                records,
                unique,
                encoded_len,
                backing_bytes,
                common,
                first_actor,
                first_action,
            },
            metadata,
            max_ledgers,
            allowance,
        )?;
        Ok(Self { owner: Some(owner) })
    }
}

fn reserve_records(records: &mut Vec<HeaderRecord>, count: usize) -> Result<()> {
    checked_bytes::<HeaderRecord>(count)?;
    records
        .try_reserve_exact(count)
        .map_err(|_| Error::ResourceLimit("header record allocation"))
}

fn checked_bytes<T>(count: usize) -> Result<usize> {
    count
        .checked_mul(size_of::<T>())
        .ok_or(Error::ResourceLimit("header capacity overflow"))
}

fn copy_units(units: impl ExactSizeIterator<Item = u16>) -> Result<Vec<u16>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(units.len())
        .map_err(|_| Error::ResourceLimit("header UTF-16 allocation"))?;
    result.extend(units);
    Ok(result)
}
