//! Immutable byte/segment storage, private until the owned message API exists.
//! Construction has explicit finite limits and performs no scope admission.
//! Admission attaches storage-owned leases, not view-owned permits. Clones and
//! slices share those owners; a body slice retains the entire segment table
//! and its backing allocations, even when only a small range is visible.

#![allow(dead_code, reason = "Private body foundation awaits owned messages")]

use std::{mem::size_of, ops::Range, sync::Arc};

use crate::resource::{AllocationAdmission, ByteScope, Error, Result, SharedAllocation};

#[derive(Debug, Clone, Copy)]
pub(crate) struct BodyLimits {
    pub(crate) max_body_bytes: usize,
    pub(crate) max_segments: usize,
    pub(crate) max_retained_bytes: usize,
    /// Bounds distinct local/shared ledgers attached to each allocation.
    /// Slots are allocated at construction, not grown during admission.
    pub(crate) max_ledgers: usize,
}

impl BodyLimits {
    fn validate(self) -> Result<()> {
        if [
            self.max_body_bytes,
            self.max_segments,
            self.max_retained_bytes,
            self.max_ledgers,
        ]
        .contains(&usize::MAX)
        {
            return Err(Error::InvalidInput(
                "body construction limits must be finite",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Segment {
    owner: Arc<SharedAllocation<Vec<u8>>>,
    range: Range<usize>,
}

impl Segment {
    pub(crate) fn from_bytes(bytes: Vec<u8>, limits: BodyLimits) -> Result<Self> {
        limits.validate()?;
        if bytes.len() > limits.max_body_bytes {
            return Err(Error::ResourceLimit("constructed body bytes"));
        }
        let range = 0..bytes.len();
        let capacity = bytes.capacity();
        Ok(Self {
            owner: SharedAllocation::new(
                bytes,
                capacity,
                limits.max_ledgers,
                limits.max_retained_bytes,
            )?,
            range,
        })
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> Result<Self> {
        check_range(&range, self.range.len())?;
        Ok(Self {
            owner: self.owner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    fn bytes(&self) -> &[u8] {
        &self.owner.data()[self.range.clone()]
    }
}

#[derive(Debug)]
struct BodyData {
    segments: Vec<Segment>,
    // Sorted allocation identity order is also the lock order for admission.
    unique: Vec<usize>,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Body {
    owner: Option<Arc<SharedAllocation<BodyData>>>,
    range: Range<usize>,
}

impl Body {
    pub(crate) const fn empty() -> Self {
        Self {
            owner: None,
            range: 0..0,
        }
    }

    pub(crate) fn from_bytes(bytes: Vec<u8>, limits: BodyLimits) -> Result<Self> {
        limits.validate()?;
        if bytes.is_empty() {
            return Ok(Self::empty());
        }
        if limits.max_segments == 0 {
            return Err(Error::ResourceLimit("body segment count"));
        }
        let minimum_metadata = SharedAllocation::<BodyData>::minimum_bytes(
            size_of::<Segment>() + size_of::<usize>(),
            limits.max_ledgers,
        )?;
        let minimum =
            SharedAllocation::<Vec<u8>>::minimum_bytes(bytes.capacity(), limits.max_ledgers)?
                .checked_add(minimum_metadata)
                .ok_or(Error::ResourceLimit("body backing size"))?;
        if minimum > limits.max_retained_bytes {
            return Err(Error::ResourceLimit("retained construction bytes"));
        }
        let segment = Segment::from_bytes(bytes, limits)?;
        if segment
            .owner
            .retained_bytes()
            .checked_add(minimum_metadata)
            .ok_or(Error::ResourceLimit("body backing size"))?
            > limits.max_retained_bytes
        {
            return Err(Error::ResourceLimit("retained construction bytes"));
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(1)
            .map_err(|_| Error::ResourceLimit("body segment allocation"))?;
        segments.push(segment);
        Self::from_segments(segments, limits)
    }

    pub(crate) fn from_segments(segments: Vec<Segment>, limits: BodyLimits) -> Result<Self> {
        limits.validate()?;
        if segments.len() > limits.max_segments {
            return Err(Error::ResourceLimit("body segment count"));
        }
        if segments.is_empty() {
            return Ok(Self::empty());
        }
        let len = segments.iter().try_fold(0usize, |total, segment| {
            total
                .checked_add(segment.range.len())
                .filter(|total| *total <= limits.max_body_bytes)
                .ok_or(Error::ResourceLimit("constructed body bytes"))
        })?;
        // Check the distinct backing total before allocating index/lease metadata.
        // max_segments bounds this allocation-free duplicate scan.
        let mut retained_bytes = 0usize;
        for (index, segment) in segments.iter().enumerate() {
            if !segments[..index]
                .iter()
                .any(|earlier| Arc::ptr_eq(&earlier.owner, &segment.owner))
            {
                retained_bytes = retained_bytes
                    .checked_add(segment.owner.retained_bytes())
                    .ok_or(Error::ResourceLimit("body backing size"))?;
            }
        }
        let metadata_allowance = limits
            .max_retained_bytes
            .checked_sub(retained_bytes)
            .ok_or(Error::ResourceLimit("retained construction bytes"))?;
        let segment_bytes = capacity_bytes::<Segment>(segments.capacity())?;
        let index_bytes = capacity_bytes::<usize>(segments.len())?;
        let minimum = SharedAllocation::<BodyData>::minimum_bytes(
            segment_bytes
                .checked_add(index_bytes)
                .ok_or(Error::ResourceLimit("body metadata size"))?,
            limits.max_ledgers,
        )?;
        if minimum > metadata_allowance {
            return Err(Error::ResourceLimit("retained construction bytes"));
        }
        let mut unique = Vec::new();
        unique
            .try_reserve_exact(segments.len())
            .map_err(|_| Error::ResourceLimit("body index allocation"))?;
        unique.extend(0..segments.len());
        unique.sort_unstable_by_key(|index| Arc::as_ptr(&segments[*index].owner).addr());
        unique.dedup_by_key(|index| Arc::as_ptr(&segments[*index].owner));
        let backing_bytes = segment_bytes
            .checked_add(capacity_bytes::<usize>(unique.capacity())?)
            .ok_or(Error::ResourceLimit("body metadata size"))?;
        let owner = SharedAllocation::new(
            BodyData {
                segments,
                unique,
                retained_bytes,
            },
            backing_bytes,
            limits.max_ledgers,
            metadata_allowance,
        )?;
        Ok(Self {
            owner: Some(owner),
            range: 0..len,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.range.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.owner.as_ref().map_or(0, |owner| {
            owner.retained_bytes() + owner.data().retained_bytes
        })
    }

    pub(crate) fn slice(&self, range: Range<usize>) -> Result<Self> {
        check_range(&range, self.len())?;
        Ok(Self {
            owner: self.owner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = &[u8]> {
        let mut position = 0;
        self.owner
            .iter()
            .flat_map(|owner| &owner.data().segments)
            .filter_map(move |segment| {
                let start = position;
                position += segment.range.len();
                let left = self.range.start.max(start);
                let right = self.range.end.min(position);
                (left < right).then(|| &segment.bytes()[left - start..right - start])
            })
    }

    /// Charge backing/metadata once per ledger, retaining all inherited leases.
    /// Temporary lock-table storage also needs local/shared byte headroom.
    /// No quota waiters, operation authority, connection state, or scope owner.
    pub(crate) fn admit(&self, scope: &ByteScope) -> Result<()> {
        let Some(owner) = &self.owner else {
            return Ok(());
        };
        let data = owner.data();
        let mut already_admitted = owner.is_admitted(scope)?;
        for index in &data.unique {
            if !already_admitted {
                break;
            }
            already_admitted = data.segments[*index].owner.is_admitted(scope)?;
        }
        if already_admitted {
            return Ok(());
        }
        let count = data
            .unique
            .len()
            .checked_add(1)
            .ok_or(Error::ResourceLimit("admission metadata size"))?;
        let scratch_bytes = capacity_bytes::<Option<AllocationAdmission<'_>>>(count)?;
        let _scratch = scope.try_reserve(scratch_bytes)?;
        let mut admissions = Vec::new();
        admissions
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceLimit("admission metadata allocation"))?;
        admissions.resize_with(count, || None);
        let mut admissions = admissions.into_boxed_slice();
        admissions[0] = Some(owner.begin_admission()?);
        for (slot, index) in admissions[1..].iter_mut().zip(&data.unique) {
            *slot = Some(data.segments[*index].owner.begin_admission()?);
        }
        for admission in admissions.iter_mut().flatten() {
            admission.reserve(scope)?;
        }
        for admission in admissions.iter_mut().flatten() {
            admission.commit();
        }
        Ok(())
    }
}

fn check_range(range: &Range<usize>, len: usize) -> Result<()> {
    if range.start > range.end || range.end > len {
        return Err(Error::InvalidInput("body slice range out of bounds"));
    }
    Ok(())
}

fn capacity_bytes<T>(capacity: usize) -> Result<usize> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or(Error::ResourceLimit("storage capacity overflow"))
}

#[cfg(test)]
mod tests;
