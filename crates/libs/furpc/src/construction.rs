//! Finite standalone construction policy, not transport defaults or native wire
//! validity rules. Submission separately applies the actual scope policy.
//! These bounds cover crate-owned storage/work, not arbitrary application
//! allocations or work performed inside a custom Serialize implementation.

use crate::{
    body::BodyLimits,
    wire::headers::{HeaderDecodeContext, HeaderDecodeLimits},
};

pub const MAX_HEADER_BYTES: usize = u16::MAX as usize;
pub const MAX_HEADER_RECORDS: usize = MAX_HEADER_BYTES / 5;
pub const MAX_NATIVE_DEPTH: usize = 64;
/// Aggregate declared Serde fields/sequence elements in one native header encode.
pub const MAX_NATIVE_FIELDS_AND_ELEMENTS: usize = MAX_HEADER_BYTES;
/// Includes adopted backing capacity and owned header/lookup bookkeeping.
pub const MAX_HEADER_RETAINED_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = u32::MAX as usize - 12;
/// Approximately 4 GiB; also leaves a finite representable bound on 32-bit hosts.
pub const MAX_BODY_RETAINED_BYTES: usize = u32::MAX as usize - (usize::BITS == 32) as usize;
pub const MAX_BODY_SEGMENTS: usize = 1024;
/// Distinct local/shared ledgers that newly constructed allocations can retain.
pub const MAX_ALLOCATION_LEDGERS: usize = 8;

pub(crate) const BODY_LIMITS: BodyLimits = BodyLimits {
    max_body_bytes: MAX_BODY_BYTES,
    max_segments: MAX_BODY_SEGMENTS,
    max_retained_bytes: MAX_BODY_RETAINED_BYTES,
    max_ledgers: MAX_ALLOCATION_LEDGERS,
};

pub(crate) const HEADER_STORAGE_LIMITS: BodyLimits = BodyLimits {
    max_body_bytes: MAX_HEADER_BYTES - 4,
    max_segments: 1,
    max_retained_bytes: MAX_HEADER_RETAINED_BYTES,
    max_ledgers: MAX_ALLOCATION_LEDGERS,
};

pub(crate) fn header_context() -> HeaderDecodeContext {
    HeaderDecodeContext::new(HeaderDecodeLimits {
        max_bytes: MAX_HEADER_BYTES,
        max_records: MAX_HEADER_RECORDS,
        max_depth: MAX_NATIVE_DEPTH,
        max_work_bytes: 2 * MAX_HEADER_BYTES,
    })
    .expect("finite native-width construction profile")
}
