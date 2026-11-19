//! Bounded Service Fabric native serialization, independent of transport.
//!
//! [`Encoder`] and [`Decoder`] implement the supported positional wire format.
//! The [`mod@serde`] module provides Serde mappings and explicit native adapters.
//! Native field widths and enclosing extents are always checked. [`Limits`]
//! optionally adds caller-selected byte and nesting policies; there are no
//! additional default codec ceilings. Transport consumers own message-size
//! policy and can bound encoding before buffer growth. Local policy exhaustion
//! is [`Error::ResourceLimit`], not malformed wire input.

mod error;
pub mod serde;

pub use error::{Error, Result};

/// Legacy string bound retained for existing consumers' domain policies.
/// The codec itself uses [`Limits`], not this ceiling.
pub const MAX_STRING_UNITS: usize = 32_768;
/// Legacy collection bound retained for existing consumers' domain policies.
/// The codec itself uses [`Limits`], not this ceiling.
pub const MAX_COLLECTION_COUNT: usize = 4_096;
/// Legacy nesting bound; not a native wire limit or a default in [`Limits`].
pub const MAX_DEPTH: usize = 32;
/// Legacy object bound; the codec now uses native extents and [`Limits::max_bytes`].
pub const MAX_OBJECT_SIZE: usize = 4 * 1024 * 1024;

/// Local resource policy for one encoder or decoder, not native wire limits.
///
/// Defaults impose no additional byte or nesting ceiling beyond native field
/// widths, enclosing extents, and platform representability. Transports should
/// supply their own operation bounds. Allocation accounting is separate;
/// Serde's allocation-budget callback can be combined with these limits.
///
/// Unknown-field traversal is iterative, but application callbacks and typed
/// Serde implementations may recurse. Callers handling recursive schemas must
/// select a suitable `max_depth` or otherwise manage their stack. This optional
/// guard is Rust execution policy, not a native validity rule, and cannot bound
/// recursion performed entirely inside custom application code.
///
/// ```
/// use fubst::{serde::{from_slice_with_limits, to_vec_with_limits}, Limits};
///
/// let limits = Limits { max_bytes: 1024, max_depth: 8 };
/// let bytes = to_vec_with_limits("hello", limits)?;
/// assert_eq!(from_slice_with_limits::<String>(&bytes, limits)?, "hello");
/// # Ok::<(), fubst::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Aggregate encoded bytes, including framing, across all root values.
    /// Defaults to `usize::MAX` (no additional policy ceiling).
    /// Zero permits no encoded values.
    pub max_bytes: usize,
    /// Optional execution-policy bound on simultaneously open native objects.
    /// Defaults to `usize::MAX` (no additional policy ceiling).
    /// Zero permits scalar values but no objects.
    /// Also bounds traversal-stack growth when skipping unknown objects.
    pub max_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: usize::MAX,
            max_depth: usize::MAX,
        }
    }
}

const OBJECT: u8 = 0x00;
const SCOPE_BEGIN: u8 = 0x1f;
const SCOPE_END: u8 = 0x2f;
const OBJECT_END: u8 = 0x3f;
const EMPTY: u8 = 0x40;
const POINTER: u8 = 0x01;
const OBJECT_ARRAY: u8 = 0x80;
const UCHAR: u8 = 0x04;
const UINT16: u8 = 0x06;
const INT32: u8 = 0x07;
const UINT32: u8 = 0x08;
const INT64: u8 = 0x09;
const UINT64: u8 = 0x0a;
const DOUBLE: u8 = 0x0b;
const GUID: u8 = 0x0c;
const STRING: u8 = 0x8d;
const BYTE_ARRAY: u8 = 0x84;
const UINT32_ARRAY: u8 = 0x88;
const TRUE: u8 = 0x42;
const FALSE: u8 = 0x72;

/// The supported native serializer with per-operation resource limits.
#[derive(Default)]
pub struct Encoder {
    bytes: Vec<u8>,
    objects: Vec<ObjectState>,
    limits: Limits,
    remaining_work: Option<usize>,
}

struct ObjectState {
    start: usize,
    // Reserve closing delimiters for this object and every open ancestor.
    limit: usize,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_limits(limits: Limits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn reserve_work(&mut self, units: usize) -> Result<()> {
        if let Some(remaining) = &mut self.remaining_work {
            *remaining = remaining
                .checked_sub(units)
                .ok_or(Error::ResourceLimit("serialization work"))?;
        }
        Ok(())
    }

    pub fn object(&mut self, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        self.typed_object(&[], f)
    }

    /// Encode an object with optional raw native polymorphic type information.
    pub fn typed_object(
        &mut self,
        type_info: &[u8],
        f: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        let depth = self.objects.len();
        let start = self.begin_object(type_info)?;
        let result = f(self);
        if let Err(error) = result {
            self.rollback(start, depth);
            return Err(error);
        }
        self.end_object(start)
    }

    fn begin_object(&mut self, type_info: &[u8]) -> Result<usize> {
        if self.objects.len() >= self.limits.max_depth {
            return Err(Error::ResourceLimit("object nesting"));
        }
        self.check_count(type_info.len())?;
        let tag_len = if type_info.is_empty() {
            0
        } else {
            type_info
                .len()
                .checked_add(unsigned_payload_len(type_info.len() as u64))
                .ok_or(Error::ResourceLimit("object size"))?
        };
        let prefix_len = 10usize
            .checked_add(tag_len)
            .ok_or(Error::ResourceLimit("object size"))?;
        let max_size = u32::MAX as usize;
        if prefix_len > max_size - 1 {
            return Err(Error::InvalidInput("native object extent exceeds UInt32"));
        }
        let start = self.bytes.len();
        self.reserve(
            prefix_len
                .checked_add(2)
                .ok_or(Error::ResourceLimit("encoded size"))?,
        )?;
        let limit = start
            .saturating_add(max_size)
            .saturating_add(1)
            .min(self.write_limit())
            - 2;
        self.objects
            .try_reserve(1)
            .map_err(|_| Error::ResourceLimit("object stack allocation"))?;
        // Marker, little-endian size, flags, and three deterministic pad bytes.
        self.bytes.extend_from_slice(&[0; 9]);
        if !type_info.is_empty() {
            self.bytes[start + 5] = 1;
            self.write_unsigned_payload(type_info.len() as u64)?;
            self.bytes.extend_from_slice(type_info);
        }
        self.bytes.push(SCOPE_BEGIN);
        self.objects.push(ObjectState { start, limit });
        Ok(start)
    }

    fn end_object(&mut self, start: usize) -> Result<()> {
        let object = self.objects.pop().expect("an object is open");
        debug_assert_eq!(object.start, start);
        if self.bytes.len() > object.limit {
            self.bytes.truncate(start);
            return Err(Error::ResourceLimit("object size"));
        }
        if let Err(error) = self.write(&[SCOPE_END, OBJECT_END]) {
            self.bytes.truncate(start);
            return Err(error);
        }
        let size = self.bytes.len() - start - 1;
        self.bytes[start + 1..start + 5].copy_from_slice(&(size as u32).to_le_bytes());
        Ok(())
    }

    fn rollback(&mut self, start: usize, depth: usize) {
        self.bytes.truncate(start);
        self.objects.truncate(depth);
    }

    fn write_limit(&self) -> usize {
        self.objects
            .last()
            .map_or(self.limits.max_bytes, |object| object.limit)
    }

    fn reserve(&mut self, additional: usize) -> Result<()> {
        let limit = self.write_limit();
        let end = self
            .bytes
            .len()
            .checked_add(additional)
            .filter(|&end| end <= limit)
            .ok_or(Error::ResourceLimit("encoded size"))?;
        if end > self.bytes.capacity() {
            let capacity = end.max(self.bytes.capacity().saturating_mul(2)).min(limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| Error::ResourceLimit("encoded buffer allocation"))?;
        }
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.reserve(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn check_count(&self, count: usize) -> Result<()> {
        if count > u32::MAX as usize {
            return Err(Error::InvalidInput("native count exceeds UInt32"));
        }
        Ok(())
    }

    /// End the current base scope and begin a derived scope.
    pub fn next_scope(&mut self) -> Result<()> {
        if self.objects.is_empty() {
            return Err(Error::InvalidInput("scope requires an object"));
        }
        self.write(&[SCOPE_END, SCOPE_BEGIN])
    }

    /// A nullable object pointer; `None` emits the native empty-pointer marker.
    pub fn pointer(
        &mut self,
        type_info: Option<&[u8]>,
        f: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        let start = self.bytes.len();
        if let Some(type_info) = type_info {
            self.write(&[POINTER])?;
            if let Err(error) = self.typed_object(type_info, f) {
                self.bytes.truncate(start);
                return Err(error);
            }
        } else {
            self.write(&[POINTER | EMPTY])?;
        }
        Ok(())
    }

    /// Serializable objects, not pointers or a scalar count followed by fields.
    pub fn object_array<T>(
        &mut self,
        values: &[T],
        mut f: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        let start = self.bytes.len();
        self.object_array_header(values.len())?;
        for value in values {
            if let Err(error) = self.object(|encoder| f(encoder, value)) {
                self.bytes.truncate(start);
                return Err(error);
            }
        }
        Ok(())
    }

    fn object_array_header(&mut self, count: usize) -> Result<()> {
        self.check_count(count)?;
        if count == 0 {
            self.write(&[OBJECT_ARRAY | EMPTY])
        } else {
            self.unsigned_with_metadata(Some(OBJECT_ARRAY), count as u64)
        }
    }

    /// Encode a native compressed UInt16, including its scalar metadata.
    pub fn u16(&mut self, value: u16) -> Result<()> {
        self.unsigned(UINT16, u64::from(value))
    }

    pub fn i32(&mut self, value: i32) -> Result<()> {
        self.signed(INT32, i64::from(value))
    }

    pub fn u32(&mut self, value: u32) -> Result<()> {
        self.unsigned(UINT32, u64::from(value))
    }

    /// Native ULONG vector: array metadata, compressed count, then compressed
    /// payloads without scalar metadata (including a byte for zero elements).
    pub fn u32_array(&mut self, values: &[u32]) -> Result<()> {
        self.check_count(values.len())?;
        if !values.is_empty()
            && values.len()
                > self
                    .write_limit()
                    .saturating_sub(self.bytes.len())
                    .saturating_sub(1 + unsigned_payload_len(values.len() as u64))
        {
            return Err(Error::ResourceLimit("encoded size"));
        }
        let size = if values.is_empty() {
            1
        } else {
            values.iter().try_fold(
                1 + unsigned_payload_len(values.len() as u64),
                |size, &value| {
                    size.checked_add(unsigned_payload_len(u64::from(value)))
                        .ok_or(Error::ResourceLimit("encoded size"))
                },
            )?
        };
        self.reserve(size)?;
        self.u32_array_header(values.len())?;
        for &value in values {
            self.write_unsigned_payload(u64::from(value))?;
        }
        Ok(())
    }

    fn u32_array_header(&mut self, count: usize) -> Result<()> {
        self.check_count(count)?;
        if count == 0 {
            self.write(&[UINT32_ARRAY | EMPTY])
        } else {
            self.unsigned_with_metadata(Some(UINT32_ARRAY), count as u64)
        }
    }

    pub fn i64(&mut self, value: i64) -> Result<()> {
        self.signed(INT64, value)
    }

    pub fn u64(&mut self, value: u64) -> Result<()> {
        self.unsigned(UINT64, value)
    }

    /// Native x86-64 IEEE Double; both signed zeros use the empty marker.
    pub fn f64(&mut self, value: f64) -> Result<()> {
        if value == 0.0 {
            self.write(&[DOUBLE | EMPTY])
        } else {
            let mut bytes = [DOUBLE; 9];
            bytes[1..].copy_from_slice(&value.to_le_bytes());
            self.write(&bytes)
        }
    }

    pub fn boolean(&mut self, value: bool) -> Result<()> {
        self.write(&[if value { TRUE } else { FALSE }])
    }

    /// Accepts the native Windows-order GUID bytes without reordering them.
    pub fn guid(&mut self, value: &[u8; 16]) -> Result<()> {
        if *value == [0; 16] {
            self.write(&[GUID | EMPTY])
        } else {
            let mut bytes = [GUID; 17];
            bytes[1..].copy_from_slice(value);
            self.write(&bytes)
        }
    }

    pub fn string(&mut self, value: &str) -> Result<()> {
        // At most three UTF-8 bytes per UTF-16 code unit. Reject oversized
        // caller buffers before scanning them; encoding work stays bounded too.
        let maximum =
            (self.write_limit().saturating_sub(self.bytes.len()) / 2).min(u32::MAX as usize);
        if value.len() > maximum.saturating_mul(3) {
            return Err(Error::ResourceLimit("string code units"));
        }
        let count = value.encode_utf16().count();
        if count > maximum {
            return Err(Error::ResourceLimit("string code units"));
        }
        if count == 0 {
            self.write(&[STRING | EMPTY])?;
        } else {
            let size = count
                .checked_mul(2)
                .and_then(|size| size.checked_add(1 + unsigned_payload_len(count as u64)))
                .ok_or(Error::ResourceLimit("encoded size"))?;
            self.reserve(size)?;
            self.bytes.push(STRING);
            self.write_unsigned_payload(count as u64)?;
            for unit in value.encode_utf16() {
                self.bytes.extend_from_slice(&unit.to_le_bytes());
            }
        }
        Ok(())
    }

    pub fn byte_array(&mut self, value: &[u8]) -> Result<()> {
        self.check_count(value.len())?;
        if value.is_empty() {
            self.write(&[BYTE_ARRAY | EMPTY])?;
        } else {
            let size = value
                .len()
                .checked_add(1 + unsigned_payload_len(value.len() as u64))
                .ok_or(Error::ResourceLimit("encoded size"))?;
            self.reserve(size)?;
            self.bytes.push(BYTE_ARRAY);
            self.write_unsigned_payload(value.len() as u64)?;
            self.bytes.extend_from_slice(value);
        }
        Ok(())
    }

    fn signed(&mut self, metadata: u8, mut value: i64) -> Result<()> {
        if value == 0 {
            return self.write(&[metadata | EMPTY]);
        }
        let mut payload = [0; 11];
        let mut start = payload.len();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            start -= 1;
            payload[start] = byte;
            if (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0) {
                break;
            }
        }
        for byte in &mut payload[start..10] {
            *byte |= 0x80;
        }
        start -= 1;
        payload[start] = metadata;
        self.write(&payload[start..])
    }

    fn unsigned(&mut self, metadata: u8, value: u64) -> Result<()> {
        if value == 0 {
            self.write(&[metadata | EMPTY])
        } else {
            self.unsigned_with_metadata(Some(metadata), value)
        }
    }

    fn write_unsigned_payload(&mut self, value: u64) -> Result<()> {
        self.unsigned_with_metadata(None, value)
    }

    fn unsigned_with_metadata(&mut self, metadata: Option<u8>, mut value: u64) -> Result<()> {
        let mut payload = [0; 11];
        let mut start = payload.len();
        loop {
            start -= 1;
            payload[start] = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        for byte in &mut payload[start..10] {
            *byte |= 0x80;
        }
        if let Some(metadata) = metadata {
            start -= 1;
            payload[start] = metadata;
        }
        self.write(&payload[start..])
    }
}

fn unsigned_payload_len(value: u64) -> usize {
    (u64::BITS - value.leading_zeros()).max(1).div_ceil(7) as usize
}

/// A bounded reader for supported positional native values, not a general
/// forward-compatible reader for every Fabric metadata or extension type.
pub struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    limit: usize,
    depth: usize,
    limits: Limits,
}

#[derive(Clone, Copy)]
enum SkipFrame {
    Scope,
    Object { end: usize, outer_limit: usize },
    Array { remaining: usize },
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            limit: bytes.len(),
            depth: 0,
            limits: Limits::default(),
        }
    }

    /// Reject oversized input before any value is decoded or allocated.
    pub fn with_limits(bytes: &'a [u8], limits: Limits) -> Result<Self> {
        if bytes.len() > limits.max_bytes {
            return Err(Error::ResourceLimit("encoded input size"));
        }
        Ok(Self {
            limits,
            ..Self::new(bytes)
        })
    }

    pub fn finish(self) -> Result<()> {
        if self.position != self.bytes.len() {
            return Err(Error::Protocol("trailing serialized data"));
        }
        Ok(())
    }

    pub fn object<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.typed_object(|decoder, _| f(decoder))
    }

    /// Read an object while exposing its bounded raw native type information.
    pub fn typed_object<T>(
        &mut self,
        f: impl FnOnce(&mut Self, &'a [u8]) -> Result<T>,
    ) -> Result<T> {
        let outer_limit = self.limit;
        let outer_depth = self.depth;
        let result = self.read_object(f);
        self.limit = outer_limit;
        self.depth = outer_depth;
        result
    }

    /// Consume remaining supported base fields, then enter the derived scope.
    pub fn next_scope(&mut self) -> Result<()> {
        if self.depth == 0 {
            return Err(Error::Protocol("scope requires an object"));
        }
        self.finish_scope()?;
        self.expect(SCOPE_BEGIN)
    }

    /// Whether the current object scope ends at this position, without consuming
    /// it. Only a valid delimiter denotes an absent optional trailing field;
    /// missing bytes and truncated values still fail decoding.
    pub fn is_scope_end(&self) -> Result<bool> {
        if self.depth == 0 {
            return Err(Error::Protocol("scope requires an object"));
        }
        Ok(self.peek()? == SCOPE_END)
    }

    /// Read a nullable native object pointer, exposing nonnull type information.
    /// The callback is not invoked for a null pointer.
    pub fn pointer<T>(
        &mut self,
        f: impl FnOnce(&mut Self, &'a [u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        match self.byte()? {
            metadata if metadata == POINTER | EMPTY => Ok(None),
            POINTER => self.typed_object(f).map(Some),
            _ => Err(Error::Protocol("expected object pointer metadata")),
        }
    }

    /// Read complete serializable objects using the callback for each first scope.
    /// Validate the count and minimum enclosing extent before allocating values.
    pub fn object_array<T>(&mut self, mut f: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let count = self.object_array_count()?;
        let mut values = Vec::new();
        for _ in 0..count {
            values.push(self.object(&mut f)?);
        }
        Ok(values)
    }

    fn object_array_count(&mut self) -> Result<usize> {
        match self.byte()? {
            metadata if metadata == OBJECT_ARRAY | EMPTY => Ok(0),
            OBJECT_ARRAY => {
                let count = self.count()?;
                // Even a fieldless object occupies marker + header + delimiters.
                if count > (self.limit - self.position) / 12 {
                    return Err(Error::Protocol("object-array exceeds enclosing extent"));
                }
                Ok(count)
            }
            _ => Err(Error::Protocol("expected object-array metadata")),
        }
    }

    /// Read a native compressed UInt16, rejecting wrong metadata and overflow.
    pub fn u16(&mut self) -> Result<u16> {
        self.unsigned(UINT16, 16).map(|value| value as u16)
    }

    pub fn i32(&mut self) -> Result<i32> {
        self.signed(INT32, 32).map(|value| value as i32)
    }

    pub fn u32(&mut self) -> Result<u32> {
        self.unsigned(UINT32, 32).map(|value| value as u32)
    }

    /// Decode a bounded native ULONG vector, not a counted list of scalars.
    pub fn u32_array(&mut self) -> Result<Vec<u32>> {
        let count = self.u32_array_count()?;
        (0..count)
            .map(|_| self.read_unsigned_payload(32).map(|v| v as u32))
            .collect()
    }

    fn u32_array_count(&mut self) -> Result<usize> {
        match self.byte()? {
            metadata if metadata == UINT32_ARRAY | EMPTY => Ok(0),
            UINT32_ARRAY => {
                let count = self.count()?;
                if count > self.limit - self.position {
                    return Err(Error::Protocol("UInt32-array exceeds enclosing extent"));
                }
                Ok(count)
            }
            _ => Err(Error::Protocol("expected UInt32-array metadata")),
        }
    }

    pub fn i64(&mut self) -> Result<i64> {
        self.signed(INT64, 64)
    }

    pub fn u64(&mut self) -> Result<u64> {
        self.unsigned(UINT64, 64)
    }

    pub fn f64(&mut self) -> Result<f64> {
        match self.byte()? {
            0x4b => Ok(0.0),
            DOUBLE => Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            _ => Err(Error::Protocol("expected Double metadata")),
        }
    }

    pub fn boolean(&mut self) -> Result<bool> {
        match self.byte()? {
            TRUE => Ok(true),
            FALSE => Ok(false),
            _ => Err(Error::Protocol("expected boolean metadata")),
        }
    }

    pub fn guid(&mut self) -> Result<[u8; 16]> {
        match self.byte()? {
            metadata if metadata == GUID | EMPTY => Ok([0; 16]),
            GUID => {
                let mut value = [0; 16];
                value.copy_from_slice(self.take(16)?);
                Ok(value)
            }
            _ => Err(Error::Protocol("expected GUID metadata")),
        }
    }

    pub fn string(&mut self) -> Result<String> {
        self.string_with_budget(&mut |_| Ok(()))
    }

    /// Borrow a native string's little-endian UTF-16 code-unit bytes.
    ///
    /// Checks string metadata, count, input limits, and enclosing extents, but
    /// does not require Unicode scalar validity. Unpaired surrogates, embedded
    /// NULs, and original code units are preserved. No allocation or alignment
    /// to `u16` is required; each pair of bytes represents one code unit.
    /// Use [`Self::string`] when a validated Rust string is required instead.
    pub fn string_utf16_bytes(&mut self) -> Result<&'a [u8]> {
        self.string_bytes()
    }

    fn string_with_budget(
        &mut self,
        reserve: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<String> {
        let bytes = self.string_bytes()?;
        let length = Self::characters(bytes).try_fold(0usize, |length, character| {
            let character = character.map_err(|_| Error::Protocol("invalid UTF-16 string"))?;
            length
                .checked_add(character.len_utf8())
                .ok_or(Error::ResourceLimit("decoded string length"))
        })?;
        reserve(length)?;
        let mut value = String::with_capacity(length);
        for character in Self::characters(bytes) {
            value.push(character.map_err(|_| Error::Protocol("invalid UTF-16 string"))?);
        }
        Ok(value)
    }

    pub fn byte_array(&mut self) -> Result<Vec<u8>> {
        Ok(self.byte_array_bytes()?.to_vec())
    }

    fn begin_object(&mut self) -> Result<&'a [u8]> {
        if self.depth >= self.limits.max_depth {
            return Err(Error::ResourceLimit("object nesting"));
        }
        self.expect(OBJECT)?;
        let extent_start = self.position;
        let header = self.take(8)?;
        let size = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if size < 11 {
            return Err(Error::Protocol("invalid object size"));
        }
        let end = extent_start
            .checked_add(size)
            .ok_or(Error::Protocol("object extent overflow"))?;
        if end > self.limit {
            return Err(Error::Protocol("object exceeds enclosing extent"));
        }
        let flags = header[4];
        if flags & !1 != 0 {
            return Err(Error::Protocol("unsupported object extension flags"));
        }
        self.limit = end;
        self.depth += 1;
        let type_info = if flags == 1 {
            let count = self.count()?;
            if count == 0 {
                return Err(Error::Protocol("empty object type information"));
            }
            self.take(count)?
        } else {
            &[]
        };
        self.expect(SCOPE_BEGIN)?;
        Ok(type_info)
    }

    fn read_object<T>(&mut self, f: impl FnOnce(&mut Self, &'a [u8]) -> Result<T>) -> Result<T> {
        let type_info = self.begin_object()?;
        let end = self.limit;
        let value = f(self, type_info)?;
        self.finish_scope()?;
        while self.peek()? == SCOPE_BEGIN {
            self.byte()?;
            self.finish_scope()?;
        }
        self.end_object(end)?;
        Ok(value)
    }

    fn end_object(&mut self, end: usize) -> Result<()> {
        self.expect(OBJECT_END)?;
        if self.position != end {
            return Err(Error::Protocol(
                "object size does not match consumed extent",
            ));
        }
        Ok(())
    }

    fn finish_scope(&mut self) -> Result<()> {
        let outer_limit = self.limit;
        let outer_depth = self.depth;
        let result = self.skip_scope();
        self.limit = outer_limit;
        self.depth = outer_depth;
        result
    }

    fn skip_scope(&mut self) -> Result<()> {
        let mut stack = Vec::new();
        loop {
            match stack.last().copied().unwrap_or(SkipFrame::Scope) {
                SkipFrame::Scope => match self.peek()? {
                    SCOPE_END => {
                        self.byte()?;
                        if stack.pop().is_none() {
                            return Ok(());
                        }
                    }
                    SCOPE_BEGIN | OBJECT_END => {
                        return Err(Error::Protocol("unexpected scope or object delimiter"));
                    }
                    OBJECT => self.begin_skipped_object(&mut stack)?,
                    POINTER | 0x41 => {
                        if self.byte()? == POINTER {
                            self.begin_skipped_object(&mut stack)?;
                        }
                    }
                    OBJECT_ARRAY | 0xc0 => {
                        let remaining = self.object_array_count()?;
                        if remaining != 0 {
                            stack
                                .try_reserve(1)
                                .map_err(|_| Error::ResourceLimit("skip stack allocation"))?;
                            stack.push(SkipFrame::Array { remaining });
                        }
                    }
                    _ => self.skip_scalar_value()?,
                },
                SkipFrame::Object { end, outer_limit } => {
                    if self.peek()? == SCOPE_BEGIN {
                        self.byte()?;
                        stack
                            .try_reserve(1)
                            .map_err(|_| Error::ResourceLimit("skip stack allocation"))?;
                        stack.push(SkipFrame::Scope);
                    } else {
                        self.end_object(end)?;
                        self.limit = outer_limit;
                        self.depth -= 1;
                        stack.pop();
                    }
                }
                SkipFrame::Array { remaining } => {
                    if remaining == 0 {
                        stack.pop();
                    } else {
                        *stack.last_mut().expect("an array is open") = SkipFrame::Array {
                            remaining: remaining - 1,
                        };
                        self.begin_skipped_object(&mut stack)?;
                    }
                }
            }
        }
    }

    fn begin_skipped_object(&mut self, stack: &mut Vec<SkipFrame>) -> Result<()> {
        let outer_limit = self.limit;
        self.begin_object()?;
        stack
            .try_reserve(2)
            .map_err(|_| Error::ResourceLimit("skip stack allocation"))?;
        stack.push(SkipFrame::Object {
            end: self.limit,
            outer_limit,
        });
        stack.push(SkipFrame::Scope);
        Ok(())
    }

    fn skip_scalar_value(&mut self) -> Result<()> {
        match self.peek()? {
            UCHAR | 0x44 => {
                if self.byte()? == UCHAR {
                    self.byte()?;
                }
                Ok(())
            }
            UINT16 | 0x46 => self.u16().map(|_| ()),
            INT32 | 0x47 => self.i32().map(|_| ()),
            UINT32 | 0x48 => self.u32().map(|_| ()),
            INT64 | 0x49 => self.i64().map(|_| ()),
            UINT64 | 0x4a => self.u64().map(|_| ()),
            DOUBLE | 0x4b => self.f64().map(|_| ()),
            GUID | 0x4c => self.guid().map(|_| ()),
            TRUE | FALSE => self.boolean().map(|_| ()),
            // Skipping native code units does not require Rust Unicode validity.
            STRING | 0xcd => self.string_bytes().map(|_| ()),
            BYTE_ARRAY | 0xc4 => self.byte_array_bytes().map(|_| ()),
            UINT32_ARRAY | 0xc8 => {
                let count = self.u32_array_count()?;
                for _ in 0..count {
                    self.read_unsigned_payload(32)?;
                }
                Ok(())
            }
            _ => Err(Error::Protocol("unsupported trailing value metadata")),
        }
    }

    fn signed(&mut self, metadata: u8, width: u32) -> Result<i64> {
        let actual = self.byte()?;
        if actual == metadata | EMPTY {
            return Ok(0);
        }
        if actual != metadata {
            return Err(Error::Protocol("expected signed integer metadata"));
        }
        // An i128 accumulator permits checking the entire final group before
        // narrowing, including the sign-extension bits of 32/64-bit values.
        let mut value = if self.peek()? & 0x40 != 0 { -1i128 } else { 0 };
        for _ in 0..width.div_ceil(7) {
            let byte = self.byte()?;
            value = (value << 7) | i128::from(byte & 0x7f);
            if byte & 0x80 == 0 {
                let sign_bit = 1i128 << (width - 1);
                if value < -sign_bit || value >= sign_bit {
                    return Err(Error::Protocol("signed integer overflow"));
                }
                return Ok(value as i64);
            }
        }
        Err(Error::Protocol("too many signed integer groups"))
    }

    fn unsigned(&mut self, metadata: u8, width: u32) -> Result<u64> {
        let actual = self.byte()?;
        if actual == metadata | EMPTY {
            return Ok(0);
        }
        if actual != metadata {
            return Err(Error::Protocol("expected unsigned integer metadata"));
        }
        self.read_unsigned_payload(width)
    }

    fn read_unsigned_payload(&mut self, width: u32) -> Result<u64> {
        let mut value = 0u128;
        for _ in 0..width.div_ceil(7) {
            let byte = self.byte()?;
            value = (value << 7) | u128::from(byte & 0x7f);
            if byte & 0x80 == 0 {
                if value >= 1u128 << width {
                    return Err(Error::Protocol("unsigned integer overflow"));
                }
                return Ok(value as u64);
            }
        }
        Err(Error::Protocol("too many unsigned integer groups"))
    }

    fn count(&mut self) -> Result<usize> {
        Ok(self.read_unsigned_payload(32)? as usize)
    }

    fn string_bytes(&mut self) -> Result<&'a [u8]> {
        match self.byte()? {
            metadata if metadata == STRING | EMPTY => Ok(&[]),
            STRING => {
                let count = self.count()?;
                let length = count
                    .checked_mul(2)
                    .ok_or(Error::Protocol("string length overflow"))?;
                self.take(length)
            }
            _ => Err(Error::Protocol("expected string metadata")),
        }
    }

    fn characters(
        bytes: &[u8],
    ) -> impl Iterator<Item = std::result::Result<char, std::char::DecodeUtf16Error>> + '_ {
        char::decode_utf16(
            bytes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|unit| u16::from_le_bytes([unit[0], unit[1]])),
        )
    }

    fn byte_array_bytes(&mut self) -> Result<&'a [u8]> {
        match self.byte()? {
            metadata if metadata == BYTE_ARRAY | EMPTY => Ok(&[]),
            BYTE_ARRAY => {
                let count = self.count()?;
                self.take(count)
            }
            _ => Err(Error::Protocol("expected byte-array metadata")),
        }
    }

    fn expect(&mut self, metadata: u8) -> Result<()> {
        if self.byte()? != metadata {
            return Err(Error::Protocol("unexpected serialization delimiter"));
        }
        Ok(())
    }

    fn peek(&self) -> Result<u8> {
        if self.bytes.len() > self.limits.max_bytes {
            return Err(Error::ResourceLimit("encoded input size"));
        }
        if self.position >= self.limit {
            return Err(Error::Protocol("truncated serialized value"));
        }
        Ok(self.bytes[self.position])
    }

    fn byte(&mut self) -> Result<u8> {
        let value = self.peek()?;
        self.position += 1;
        Ok(value)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        if self.bytes.len() > self.limits.max_bytes {
            return Err(Error::ResourceLimit("encoded input size"));
        }
        let end = self
            .position
            .checked_add(length)
            .ok_or(Error::Protocol("serialized length overflow"))?;
        if end > self.limit {
            return Err(Error::Protocol("truncated serialized value"));
        }
        let bytes = &self.bytes[self.position..end];
        self.position = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_utf16_preserves_native_units_without_rust_string_validation() {
        let bytes = [0x8d, 5, 0x41, 0, 0, 0xd8, 0, 0, 0x34, 0xd8, 0x1e, 0xdd];
        let mut decoder = Decoder::new(&bytes);
        let raw = decoder.string_utf16_bytes().unwrap();
        assert_eq!(raw.as_ptr(), bytes[2..].as_ptr());
        assert_eq!(
            raw.as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u16::from_le_bytes(*pair))
                .collect::<Vec<_>>(),
            [0x41, 0xd800, 0, 0xd834, 0xdd1e]
        );
        decoder.finish().unwrap();
        assert_eq!(
            Decoder::new(&bytes).string(),
            Err(Error::Protocol("invalid UTF-16 string"))
        );
        let mut decoder = Decoder::new(&[0xcd]);
        assert_eq!(decoder.string_utf16_bytes().unwrap(), &[]);
        decoder.finish().unwrap();
    }

    #[test]
    fn borrowed_utf16_keeps_metadata_extent_and_input_checks() {
        let bytes = [0x8d, 2, b'a', 0, b'b', 0];
        for split in 0..bytes.len() {
            assert!(Decoder::new(&bytes[..split]).string_utf16_bytes().is_err());
        }
        assert!(Decoder::new(&[0xc4]).string_utf16_bytes().is_err());
        assert!(matches!(
            Decoder::with_limits(
                &bytes,
                Limits {
                    max_bytes: bytes.len() - 1,
                    max_depth: 1,
                }
            ),
            Err(Error::ResourceLimit("encoded input size"))
        ));
        let mut encoder = Encoder::new();
        encoder.object(|encoder| encoder.string("ab")).unwrap();
        let mut bytes = encoder.finish();
        // Keep the backing bytes, but end the enclosing object inside the string.
        bytes[1..5].copy_from_slice(&12u32.to_le_bytes());
        let mut decoder = Decoder::new(&bytes);
        assert!(decoder
            .object(|decoder| decoder.string_utf16_bytes())
            .is_err());
    }

    #[test]
    fn scope_end_probe_does_not_consume_or_forgive_truncated_values() {
        assert!(Decoder::new(&[]).is_scope_end().is_err());
        let mut e = Encoder::new();
        e.object(|e| e.string("value")).unwrap();
        let bytes = e.finish();
        let mut d = Decoder::new(&bytes);
        d.object(|d| {
            assert!(!d.is_scope_end()?);
            assert_eq!(d.string()?, "value");
            assert!(d.is_scope_end()?);
            assert!(d.is_scope_end()?);
            Ok(())
        })
        .unwrap();
        d.finish().unwrap();
        let mut truncated = bytes.clone();
        // Keep the outer extent valid, but shorten the string payload so the
        // delimiter becomes part of the field rather than an absent suffix.
        truncated.drain(14..22);
        let extent = (truncated.len() - 1) as u32;
        truncated[1..5].copy_from_slice(&extent.to_le_bytes());
        let mut d = Decoder::new(&truncated);
        assert!(d
            .object(|d| {
                assert!(!d.is_scope_end()?);
                d.string()
            })
            .is_err());
    }

    #[test]
    fn pointers_arrays_type_info_and_derived_scopes_are_bounded() {
        let mut encoder = Encoder::new();
        encoder
            .object(|encoder| {
                encoder.pointer(None, |_| panic!("null pointer callback"))?;
                encoder.pointer(Some(&[0x1f, 0x2f, 0x3f]), |encoder| {
                    encoder.u16(u16::MAX).unwrap();
                    encoder.next_scope()?;
                    encoder.object_array(&[0u16, 127, 128, 32768], |encoder, value| {
                        encoder.u16(*value).unwrap();
                        Ok(())
                    })
                })
            })
            .unwrap();
        let bytes = encoder.finish();
        let mut decoder = Decoder::new(&bytes);
        decoder
            .object(|decoder| {
                assert_eq!(decoder.pointer(|_, _| Ok(1))?, None);
                assert_eq!(
                    decoder.pointer(|decoder, tag| {
                        assert_eq!(tag, [0x1f, 0x2f, 0x3f]);
                        assert_eq!(decoder.u16()?, u16::MAX);
                        decoder.next_scope()?;
                        decoder.object_array(Decoder::u16)
                    })?,
                    Some(vec![0, 127, 128, 32768])
                );
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
        let mut decoder = Decoder::new(&bytes);
        decoder.object(|_| Ok(())).unwrap(); // bounded typed skip, including derived scope
        decoder.finish().unwrap();
        for length in 0..bytes.len() {
            assert!(Decoder::new(&bytes[..length]).object(|_| Ok(())).is_err());
        }
        for count in [1, MAX_COLLECTION_COUNT as u64 + 1, u32::MAX as u64] {
            let array = counted(OBJECT_ARRAY, count, &[]);
            assert!(Decoder::new(&array).object_array(|_| Ok(())).is_err());
            assert!(Decoder::new(&object_bytes(&array))
                .object(|_| Ok(()))
                .is_err());
        }
        for bytes in [
            &[POINTER][..],
            &[POINTER, 0x41],
            &[UINT16],
            &[UINT16, 0x84, 0x80, 0], // 65536 overflow
            &[UINT16, 0x80, 0x80, 0x80, 0],
        ] {
            assert!(Decoder::new(&object_bytes(bytes))
                .object(|_| Ok(()))
                .is_err());
        }
        assert!(Decoder::new(&[]).next_scope().is_err());
        assert!(Encoder::new().next_scope().is_err());
        let mut encoder = Encoder::new();
        encoder.u16(42).unwrap();
        assert!(encoder
            .pointer(Some(&[]), |_| Err(Error::InvalidInput("model failure")))
            .is_err());
        assert_eq!(Decoder::new(&encoder.finish()).u16().unwrap(), 42);
        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: MAX_COLLECTION_COUNT,
            ..Limits::default()
        });
        assert!(encoder
            .object_array(&vec![0; MAX_COLLECTION_COUNT + 1], |_, _| Ok(()))
            .is_err());
        assert!(encoder.finish().is_empty());
    }

    fn encode(f: impl FnOnce(&mut Encoder)) -> Vec<u8> {
        let mut encoder = Encoder::new();
        f(&mut encoder);
        encoder.finish()
    }

    fn object_bytes(fields: &[u8]) -> Vec<u8> {
        let mut bytes = vec![OBJECT];
        bytes.extend_from_slice(&((11 + fields.len()) as u32).to_le_bytes());
        bytes.extend_from_slice(&[0, 0, 0, 0, SCOPE_BEGIN]);
        bytes.extend_from_slice(fields);
        bytes.extend_from_slice(&[SCOPE_END, OBJECT_END]);
        bytes
    }

    fn set_size(bytes: &mut [u8], size: usize) {
        bytes[1..5].copy_from_slice(&(size as u32).to_le_bytes());
    }

    fn counted(metadata: u8, count: u64, payload: &[u8]) -> Vec<u8> {
        encode(|encoder| {
            encoder.bytes.push(metadata);
            encoder.write_unsigned_payload(count).unwrap();
            encoder.bytes.extend_from_slice(payload);
        })
    }

    #[test]
    fn all_integer_group_boundaries_round_trip() {
        let mut signed = vec![i64::MIN, i64::MAX, -65, -64, -1, 0, 63, 64];
        for shift in 0..63 {
            let boundary = 1i64 << shift;
            signed.extend([boundary - 1, boundary, -boundary, -boundary - 1]);
        }
        for value in signed {
            let bytes = encode(|encoder| encoder.i64(value).unwrap());
            let mut decoder = Decoder::new(&bytes);
            assert_eq!(decoder.i64().unwrap(), value);
            decoder.finish().unwrap();
            if let Ok(value) = i32::try_from(value) {
                let bytes = encode(|encoder| encoder.i32(value).unwrap());
                let mut decoder = Decoder::new(&bytes);
                assert_eq!(decoder.i32().unwrap(), value);
                decoder.finish().unwrap();
            }
        }
        let mut unsigned = vec![0, u64::MAX];
        for shift in 0..64 {
            let boundary = 1u64 << shift;
            unsigned.extend([boundary - 1, boundary]);
        }
        for value in unsigned {
            let bytes = encode(|encoder| encoder.u64(value).unwrap());
            let mut decoder = Decoder::new(&bytes);
            assert_eq!(decoder.u64().unwrap(), value);
            decoder.finish().unwrap();
            if let Ok(value) = u32::try_from(value) {
                let bytes = encode(|encoder| encoder.u32(value).unwrap());
                let mut decoder = Decoder::new(&bytes);
                assert_eq!(decoder.u32().unwrap(), value);
                decoder.finish().unwrap();
            }
        }
    }

    #[test]
    fn empty_scalars_booleans_and_guids_round_trip() {
        let guid = std::array::from_fn(|index| index as u8);
        let bytes = encode(|encoder| {
            encoder.i32(0).unwrap();
            encoder.u32(0).unwrap();
            encoder.i64(0).unwrap();
            encoder.u64(0).unwrap();
            encoder.boolean(true).unwrap();
            encoder.boolean(false).unwrap();
            encoder.guid(&[0; 16]).unwrap();
            encoder.guid(&guid).unwrap();
            encoder.string("").unwrap();
            encoder.byte_array(&[]).unwrap();
        });
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.i32().unwrap(), 0);
        assert_eq!(decoder.u32().unwrap(), 0);
        assert_eq!(decoder.i64().unwrap(), 0);
        assert_eq!(decoder.u64().unwrap(), 0);
        assert!(decoder.boolean().unwrap());
        assert!(!decoder.boolean().unwrap());
        assert_eq!(decoder.guid().unwrap(), [0; 16]);
        assert_eq!(decoder.guid().unwrap(), guid);
        assert_eq!(decoder.string().unwrap(), "");
        assert_eq!(decoder.byte_array().unwrap(), []);
        decoder.finish().unwrap();
    }

    #[test]
    fn utf16_code_units_and_surrogate_pairs() {
        let bytes = encode(|encoder| encoder.string("A🦀é").unwrap());
        // Pinned FabricSerializationTypes.h: Array (0x80) | WString (0x0d).
        assert_eq!(bytes[0], 0x8d);
        assert_eq!(Decoder::new(&[0x8d, 1, b'A', 0]).string().unwrap(), "A");
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.string().unwrap(), "A🦀é");
        decoder.finish().unwrap();
        for bytes in [
            &[0x8d, 1, 0, 0xd8][..],
            &[0x8d, 1, 0, 0xdc],
            &[0x8d, 2, 0, 0xd8, 0x41, 0],
        ] {
            assert!(matches!(
                Decoder::new(bytes).string(),
                Err(Error::Protocol(_))
            ));
            let object = object_bytes(bytes);
            assert!(Decoder::new(&object).object(Decoder::string).is_err());
            let mut decoder = Decoder::new(&object);
            decoder.object(|_| Ok(())).unwrap();
            decoder.finish().unwrap();
        }
    }

    #[test]
    fn object_extents_and_ignored_padding() {
        let bytes = encode(|encoder| encoder.object(|_| Ok(())).unwrap());
        let mut padded = bytes;
        padded[6..9].copy_from_slice(&[0x1f, 0x2f, 0x3f]);
        let mut decoder = Decoder::new(&padded);
        decoder.object(|_| Ok(())).unwrap();
        decoder.finish().unwrap();

        let bytes = encode(|encoder| {
            encoder
                .object(|encoder| {
                    encoder.object(|encoder| {
                        encoder.u32(128).unwrap();
                        Ok(())
                    })?;
                    encoder.i32(-65).unwrap();
                    Ok(())
                })
                .unwrap();
        });
        assert_eq!(bytes[0], OBJECT);
        assert_eq!(bytes[10], OBJECT);
        assert_eq!(
            u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize,
            bytes.len() - 1
        );
        let mut decoder = Decoder::new(&bytes);
        decoder
            .object(|decoder| {
                assert_eq!(decoder.object(Decoder::u32)?, 128);
                assert_eq!(decoder.i32()?, -65);
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
    }

    #[test]
    fn nested_depth_limit_is_checked_for_readers_and_writers() {
        let limits = Limits {
            max_depth: MAX_DEPTH,
            ..Limits::default()
        };
        fn write_nested(encoder: &mut Encoder, count: usize) -> Result<()> {
            encoder.object(|encoder| {
                if count > 1 {
                    write_nested(encoder, count - 1)?;
                }
                Ok(())
            })
        }
        fn read_nested(decoder: &mut Decoder<'_>, count: usize) -> Result<()> {
            decoder.object(|decoder| {
                if count > 1 {
                    read_nested(decoder, count - 1)?;
                }
                Ok(())
            })
        }
        let mut encoder = Encoder::with_limits(limits);
        write_nested(&mut encoder, MAX_DEPTH).unwrap();
        let bytes = encoder.finish();
        let mut decoder = Decoder::with_limits(&bytes, limits).unwrap();
        read_nested(&mut decoder, MAX_DEPTH).unwrap();
        decoder.finish().unwrap();
        let mut decoder = Decoder::with_limits(&bytes, limits).unwrap();
        decoder.object(|_| Ok(())).unwrap();
        decoder.finish().unwrap();

        let mut encoder = Encoder::with_limits(limits);
        assert!(matches!(
            write_nested(&mut encoder, MAX_DEPTH + 1),
            Err(Error::ResourceLimit(_))
        ));
        assert!(encoder.finish().is_empty());
        let too_deep = object_bytes(&bytes);
        assert!(read_nested(
            &mut Decoder::with_limits(&too_deep, limits).unwrap(),
            MAX_DEPTH + 1
        )
        .is_err());
        assert!(Decoder::with_limits(&too_deep, limits)
            .unwrap()
            .object(|_| Ok(()))
            .is_err());
    }

    #[test]
    fn every_truncated_prefix_fails() {
        let bytes = encode(|encoder| {
            encoder
                .object(|encoder| {
                    encoder.i32(i32::MIN).unwrap();
                    encoder.u32(u32::MAX).unwrap();
                    encoder.i64(i64::MIN).unwrap();
                    encoder.u64(u64::MAX).unwrap();
                    encoder.boolean(true).unwrap();
                    encoder.guid(&[0x3f; 16]).unwrap();
                    encoder.string("x🦀").unwrap();
                    encoder.byte_array(&[0x1f, 0x2f, 0x3f]).unwrap();
                    encoder.object(|encoder| {
                        encoder.i64(-65).unwrap();
                        Ok(())
                    })
                })
                .unwrap();
        });
        for length in 0..bytes.len() {
            let mut visited = false;
            assert!(
                Decoder::new(&bytes[..length])
                    .object(|_| {
                        visited = true;
                        Ok(())
                    })
                    .is_err(),
                "accepted truncated object of {length} bytes"
            );
            assert!(!visited, "invalid extent reached the field callback");
        }
        let mut visited = false;
        Decoder::new(&bytes)
            .object(|_| {
                visited = true;
                Ok(())
            })
            .unwrap();
        assert!(visited);
        let values = [
            encode(|encoder| encoder.i32(i32::MIN).unwrap()),
            encode(|encoder| encoder.u32(u32::MAX).unwrap()),
            encode(|encoder| encoder.i64(i64::MIN).unwrap()),
            encode(|encoder| encoder.u64(u64::MAX).unwrap()),
            encode(|encoder| encoder.guid(&[1; 16]).unwrap()),
            encode(|encoder| encoder.string("🦀").unwrap()),
            encode(|encoder| encoder.byte_array(&[1, 2, 3]).unwrap()),
        ];
        for value in values {
            for length in 0..value.len() {
                let object = object_bytes(&value[..length]);
                // A zero-byte field list is a valid empty object.
                if length != 0 {
                    assert!(Decoder::new(&object).object(|_| Ok(())).is_err());
                }
            }
        }
    }

    #[test]
    fn malformed_integer_payloads_are_rejected() {
        for bytes in [
            &[0x07, 0x88, 0x80, 0x80, 0x80, 0][..],
            &[0x07, 0xf7, 0xff, 0xff, 0xff, 0x7f],
            &[0x07, 0x80, 0x80, 0x80, 0x80, 0x80, 0],
            &[0x07],
        ] {
            assert!(Decoder::new(bytes).i32().is_err());
        }
        for bytes in [
            &[0x08, 0x90, 0x80, 0x80, 0x80, 0][..],
            &[0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0],
            &[0x08],
        ] {
            assert!(Decoder::new(bytes).u32().is_err());
        }
        for bytes in [
            &[
                0x09, 0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ][..],
            &[
                0x09, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f,
            ],
            &[
                0x09, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ],
        ] {
            assert!(Decoder::new(bytes).i64().is_err());
        }
        for bytes in [
            &[
                0x0a, 0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ][..],
            &[
                0x0a, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ],
        ] {
            assert!(Decoder::new(bytes).u64().is_err());
        }
    }

    #[test]
    fn redundant_groups_and_zero_counts_are_not_mistaken_for_delimiters() {
        assert_eq!(Decoder::new(&[0x07, 0xff, 0x7f]).i32().unwrap(), -1);
        assert_eq!(Decoder::new(&[0x08, 0x80, 0]).u32().unwrap(), 0);
        assert_eq!(Decoder::new(&[0x8d, 0]).string().unwrap(), "");
        assert_eq!(Decoder::new(&[0x84, 0]).byte_array().unwrap(), []);
    }

    #[test]
    fn strings_and_byte_arrays_are_bounded_before_allocation() {
        let string = "🦀".repeat(MAX_STRING_UNITS / 2);
        let bytes = encode(|encoder| encoder.string(&string).unwrap());
        assert_eq!(Decoder::new(&bytes).string().unwrap(), string);
        let too_long = format!("{string}x");
        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: bytes.len(),
            ..Limits::default()
        });
        assert!(matches!(
            encoder.string(&too_long),
            Err(Error::ResourceLimit(_))
        ));
        assert!(encoder.finish().is_empty());
        let array = vec![0x3f; MAX_COLLECTION_COUNT];
        let bytes = encode(|encoder| encoder.byte_array(&array).unwrap());
        assert_eq!(Decoder::new(&bytes).byte_array().unwrap(), array);
        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: bytes.len(),
            ..Limits::default()
        });
        assert!(matches!(
            encoder.byte_array(&vec![0; MAX_COLLECTION_COUNT + 1]),
            Err(Error::ResourceLimit(_))
        ));
        assert!(encoder.finish().is_empty());

        for count in [MAX_STRING_UNITS as u64 + 1, u64::from(u32::MAX), 1u64 << 32] {
            let bytes = counted(STRING, count, &[]);
            assert!(Decoder::new(&bytes).string().is_err());
            assert!(Decoder::new(&object_bytes(&bytes))
                .object(|_| Ok(()))
                .is_err());
        }
        for count in [
            MAX_COLLECTION_COUNT as u64 + 1,
            u64::from(u32::MAX),
            1u64 << 32,
        ] {
            let bytes = counted(BYTE_ARRAY, count, &[]);
            assert!(Decoder::new(&bytes).byte_array().is_err());
            assert!(Decoder::new(&object_bytes(&bytes))
                .object(|_| Ok(()))
                .is_err());
        }
        assert!(Decoder::new(&counted(STRING, 2, &[0, 0])).string().is_err());
        assert!(Decoder::new(&counted(BYTE_ARRAY, 2, &[0]))
            .byte_array()
            .is_err());
        for metadata in [STRING, BYTE_ARRAY] {
            let bytes = [metadata, 0x80, 0x80, 0x80, 0x80, 0x80, 0];
            assert!(Decoder::new(&object_bytes(&bytes))
                .object(|_| Ok(()))
                .is_err());
        }
    }

    #[test]
    fn byte_limit_rejects_writes_before_growth_without_changing_prior_bytes() {
        let limits = Limits {
            max_bytes: 4,
            max_depth: 0,
        };
        let writes: [fn(&mut Encoder) -> Result<()>; 7] = [
            |e| e.u32(u32::MAX),
            |e| e.i64(i64::MIN),
            |e| e.f64(1.0),
            |e| e.guid(&[1; 16]),
            |e| e.string("abc"),
            |e| e.byte_array(&[0; 4]),
            |e| e.u32_array(&[u32::MAX]),
        ];
        let mut encoder = Encoder::with_limits(limits);
        for write in writes {
            assert!(matches!(write(&mut encoder), Err(Error::ResourceLimit(_))));
            assert!(encoder.bytes.is_empty());
            assert_eq!(encoder.bytes.capacity(), 0);
        }
        encoder.u16(128).unwrap();
        let previous = encoder.bytes.clone();
        let capacity = encoder.bytes.capacity();
        assert!(encoder.u32(128).is_err());
        assert_eq!(encoder.bytes, previous);
        assert_eq!(encoder.bytes.capacity(), capacity);
        encoder.boolean(true).unwrap();
        assert!(encoder.boolean(false).is_err());
        let bytes = encoder.finish();
        let mut decoder = Decoder::with_limits(&bytes, limits).unwrap();
        assert_eq!(decoder.u16().unwrap(), 128);
        assert!(decoder.boolean().unwrap());
        decoder.finish().unwrap();

        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: 0,
            max_depth: 0,
        });
        assert!(encoder.boolean(false).is_err());
        assert!(encoder.string("").is_err());
        assert!(encoder.byte_array(&[]).is_err());
        assert!(encoder.u32_array(&[]).is_err());
        assert!(encoder.pointer(None, |_| unreachable!()).is_err());
        assert_eq!(encoder.bytes.capacity(), 0);
        assert!(Decoder::with_limits(
            &[],
            Limits {
                max_bytes: 0,
                max_depth: 0
            }
        )
        .unwrap()
        .finish()
        .is_ok());
    }

    #[test]
    fn nested_and_root_objects_share_the_byte_budget_and_restore_state_on_error() {
        let limits = Limits {
            max_bytes: 25,
            max_depth: 2,
        };
        let write = |e: &mut Encoder| e.object(|e| e.pointer(Some(&[]), |_| Ok(())));
        let mut encoder = Encoder::with_limits(limits);
        write(&mut encoder).unwrap();
        let bytes = encoder.finish();
        assert_eq!(bytes.len(), limits.max_bytes);
        Decoder::with_limits(&bytes, limits)
            .unwrap()
            .object(|_| Ok(()))
            .unwrap();

        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: 24,
            ..limits
        });
        assert!(write(&mut encoder).is_err());
        assert!(encoder.bytes.is_empty());
        assert!(encoder.objects.is_empty());
        assert!(encoder.bytes.capacity() <= 24);
        encoder.object(|_| Ok(())).unwrap();

        let mut decoder = Decoder::with_limits(
            &bytes,
            Limits {
                max_depth: 1,
                ..limits
            },
        )
        .unwrap();
        assert!(matches!(
            decoder.object(|_| Ok(())),
            Err(Error::ResourceLimit(_))
        ));
        assert_eq!(decoder.depth, 0);
        assert_eq!(decoder.limit, bytes.len());

        let mut encoder = Encoder::with_limits(limits);
        assert!(encoder.object_array(&[(), ()], |_, _| Ok(())).is_err());
        assert!(encoder.bytes.is_empty());
        assert!(encoder.objects.is_empty());
        encoder.object(|_| Ok(())).unwrap();
        encoder.object(|_| Ok(())).unwrap();
        encoder.boolean(false).unwrap();
        assert!(encoder.boolean(false).is_err());
        assert_eq!(encoder.finish().len(), limits.max_bytes);
    }

    #[test]
    fn object_size_boundaries() {
        let fields = vec![0x48; MAX_OBJECT_SIZE - 11];
        let bytes = object_bytes(&fields);
        let mut decoder = Decoder::new(&bytes);
        decoder.object(|_| Ok(())).unwrap();
        decoder.finish().unwrap();
        for size in [0, 8, 10, MAX_OBJECT_SIZE + 1, u32::MAX as usize] {
            let mut bytes = object_bytes(&[]);
            set_size(&mut bytes, size);
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
        let mut encoder = Encoder::new();
        encoder
            .object(|encoder| {
                encoder.write(&fields)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(encoder.finish().len(), MAX_OBJECT_SIZE + 1);
        let mut encoder = Encoder::with_limits(Limits {
            max_bytes: MAX_OBJECT_SIZE + 1,
            ..Limits::default()
        });
        assert!(matches!(
            encoder.object(|encoder| {
                encoder.write(&fields)?;
                encoder.u32(0)?;
                Ok(())
            }),
            Err(Error::ResourceLimit(_))
        ));
        assert!(encoder.finish().is_empty());
    }

    #[test]
    fn malformed_object_extents_and_delimiters() {
        let valid = object_bytes(&[0x47]);
        for (index, replacement) in [(0, 1), (9, SCOPE_END), (11, OBJECT_END), (12, SCOPE_END)] {
            let mut bytes = valid.clone();
            bytes[index] = replacement;
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
        for delta in [-1isize, 1] {
            let mut bytes = valid.clone();
            let size = (bytes.len() as isize - 1 + delta) as usize;
            set_size(&mut bytes, size);
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
        let mut bytes = valid.clone();
        bytes.push(0x48);
        let size = bytes.len() - 1;
        set_size(&mut bytes, size);
        assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());

        let child = object_bytes(&[0x48]);
        let mut bytes = object_bytes(&child);
        let outer_size = bytes.len() - 1;
        // The child cannot borrow the parent's delimiters or following bytes.
        bytes[11..15].copy_from_slice(&(outer_size as u32).to_le_bytes());
        bytes.extend_from_slice(&[SCOPE_END, OBJECT_END]);
        assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        for flags in [2, 3, 4, 0xff] {
            let mut bytes = valid.clone();
            bytes[5] = flags;
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
    }

    #[test]
    fn type_information_is_bounded_and_skipped_as_opaque_bytes() {
        let mut bytes = object_bytes(&[0x07, 0x7f]);
        bytes[5] = 1;
        bytes.splice(9..9, [3, SCOPE_BEGIN, SCOPE_END, OBJECT_END]);
        let size = bytes.len() - 1;
        set_size(&mut bytes, size);
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.object(Decoder::i32).unwrap(), -1);
        decoder.finish().unwrap();
        for count in [0, MAX_COLLECTION_COUNT as u64 + 1, 1u64 << 32] {
            let mut bytes = object_bytes(&[]);
            bytes[5] = 1;
            let mut count_bytes = Encoder::new();
            count_bytes.write_unsigned_payload(count).unwrap();
            bytes.splice(9..9, count_bytes.finish());
            let size = bytes.len() - 1;
            set_size(&mut bytes, size);
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
        let mut bytes = object_bytes(&[]);
        bytes[5] = 1;
        bytes.splice(9..9, [100]);
        let size = bytes.len() - 1;
        set_size(&mut bytes, size);
        assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
    }

    #[test]
    fn supported_unknown_tail_values_and_extra_scopes_are_typed_skipped() {
        let fields = encode(|encoder| {
            encoder.i32(-1).unwrap();
            encoder.i32(47).unwrap();
            encoder.u32(63).unwrap();
            encoder.i64(i64::MIN).unwrap();
            encoder.u64(u64::MAX).unwrap();
            encoder.boolean(true).unwrap();
            encoder.boolean(false).unwrap();
            encoder.guid(&[0x2f; 16]).unwrap();
            encoder.guid(&[0; 16]).unwrap();
            encoder.string("/?\u{1f}\0🦀").unwrap();
            encoder.string("").unwrap();
            encoder.byte_array(&[0x1f, 0x2f, 0x3f, 0]).unwrap();
            encoder.byte_array(&[]).unwrap();
            encoder
                .object(|encoder| {
                    encoder.string("nested").unwrap();
                    Ok(())
                })
                .unwrap();
        });
        let mut bytes = object_bytes(&fields);
        let end = bytes.len() - 1;
        bytes.splice(
            end..end,
            [SCOPE_BEGIN, 0x48, SCOPE_END, SCOPE_BEGIN, SCOPE_END],
        );
        let size = bytes.len() - 1;
        set_size(&mut bytes, size);
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.object(Decoder::i32).unwrap(), -1);
        decoder.finish().unwrap();
        for tail in [&[0xee][..], &[SCOPE_BEGIN], &[OBJECT_END], &[0x07, 0x80]] {
            let bytes = object_bytes(tail);
            assert!(Decoder::new(&bytes).object(|_| Ok(())).is_err());
        }
    }

    #[test]
    fn native_suffix_uchar_values_include_empty_and_delimiter_payloads() {
        let fields = [
            0x48,
            0x04,
            0x00,
            0x04,
            SCOPE_BEGIN,
            0x04,
            SCOPE_END,
            0x04,
            OBJECT_END,
            0x44,
        ];
        let mut bytes = object_bytes(&fields);
        let end = bytes.len() - 1;
        bytes.splice(end..end, [SCOPE_BEGIN, 0x04, 0xff, 0x44, SCOPE_END]);
        let size = bytes.len() - 1;
        set_size(&mut bytes, size);
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.object(Decoder::u32).unwrap(), 0);
        decoder.finish().unwrap();

        for suffix in [&[0x04][..], &[0x24]] {
            let mut bytes = object_bytes(suffix);
            bytes.extend_from_slice(&object_bytes(&[]));
            assert!(matches!(
                Decoder::new(&bytes).object(|_| Ok(())),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn native_suffix_strings_keep_raw_utf16_but_still_check_extents() {
        for suffix in [&[0x8d, 1, 0x00, 0xd8][..], &[0x8d, 1, 0x00, 0xdc], &[0xcd]] {
            let nested = object_bytes(suffix);
            let bytes = object_bytes(&nested);
            let mut decoder = Decoder::new(&bytes);
            decoder.object(|_| Ok(())).unwrap();
            decoder.finish().unwrap();
        }
        for suffix in [
            &[0x8d][..],
            &[0x8d, 0x80],
            &[0x8d, 3, 0x00, 0xd8],
            &[0x8d, 0x90, 0x80, 0x80, 0x80, 0x00],
        ] {
            let mut bytes = object_bytes(suffix);
            bytes.extend_from_slice(&object_bytes(&[]));
            assert!(matches!(
                Decoder::new(&bytes).object(|_| Ok(())),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn deep_unknown_objects_pointers_and_arrays_use_a_growable_traversal_stack() {
        let depth = 4_096;
        let mut bytes = Vec::new();
        let mut objects = Vec::new();
        for level in 0..depth {
            let array_item = level != 0 && level % 3 == 0;
            if array_item {
                bytes.extend_from_slice(&[OBJECT_ARRAY, 2]);
            } else if level % 3 == 1 {
                bytes.push(POINTER);
            }
            let start = bytes.len();
            bytes.extend_from_slice(&[0; 9]);
            if level % 5 == 0 {
                bytes[start + 5] = 1;
                bytes.extend_from_slice(&[1, 7]);
            }
            bytes.push(SCOPE_BEGIN);
            objects.push((start, array_item));
        }
        let leaf_end = bytes.len() + 4;
        for (start, array_item) in objects.into_iter().rev() {
            bytes.extend_from_slice(&[SCOPE_END, SCOPE_BEGIN, TRUE, SCOPE_END, OBJECT_END]);
            let size = (bytes.len() - start - 1) as u32;
            bytes[start + 1..start + 5].copy_from_slice(&size.to_le_bytes());
            if array_item {
                bytes.extend_from_slice(&object_bytes(&[]));
            }
        }
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                #[derive(::serde::Deserialize)]
                struct Empty {}

                let mut decoder = Decoder::new(&bytes);
                decoder.object(|_| Ok(())).unwrap();
                decoder.finish().unwrap();
                crate::serde::from_slice::<Empty>(&bytes).unwrap();
                let limits = Limits {
                    max_depth: 101,
                    ..Limits::default()
                };
                assert!(matches!(
                    Decoder::with_limits(&bytes, limits)
                        .unwrap()
                        .object(|_| Ok(())),
                    Err(Error::ResourceLimit(_))
                ));
                let mut malformed = bytes;
                malformed[leaf_end] = SCOPE_END;
                let mut decoder = Decoder::new(&malformed);
                assert!(matches!(
                    decoder.object(|d| d.next_scope()),
                    Err(Error::Protocol(_))
                ));
                assert_eq!(decoder.depth, 0);
                assert_eq!(decoder.limit, malformed.len());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn failed_iterative_scope_skip_restores_the_original_object_context() {
        let child = object_bytes(&[]);
        let mut bytes = object_bytes(&child);
        bytes[10 + child.len() - 1] = SCOPE_BEGIN;
        let mut decoder = Decoder::new(&bytes);
        assert!(decoder
            .object(|d| {
                let limit = d.limit;
                assert!(matches!(d.next_scope(), Err(Error::Protocol(_))));
                assert_eq!(d.limit, limit);
                assert_eq!(d.depth, 1);
                Err::<(), _>(Error::Protocol("invalid fixture"))
            })
            .is_err());
        assert_eq!(decoder.depth, 0);
        assert_eq!(decoder.limit, bytes.len());
    }

    #[test]
    fn missing_required_fields_and_wrong_metadata_are_errors() {
        let empty = object_bytes(&[]);
        assert!(Decoder::new(&empty).object(Decoder::boolean).is_err());
        assert!(Decoder::new(&[0x48]).i32().is_err());
        assert!(Decoder::new(&[0x47]).u32().is_err());
        assert!(Decoder::new(&[0x4a]).i64().is_err());
        assert!(Decoder::new(&[0x49]).u64().is_err());
        assert!(Decoder::new(&[0x02]).boolean().is_err());
        assert!(Decoder::new(&[0x43]).boolean().is_err());
        assert!(Decoder::new(&[0x4a]).guid().is_err());
        assert!(Decoder::new(&[0xc4]).string().is_err());
        assert!(Decoder::new(&[0xcd]).byte_array().is_err());
    }

    #[test]
    fn outer_finish_requires_all_input_and_objects_do_not_consume_siblings() {
        let bytes = encode(|encoder| {
            encoder.object(|_| Ok(())).unwrap();
            encoder.object(|_| Ok(())).unwrap();
            encoder.i32(7).unwrap();
        });
        let mut decoder = Decoder::new(&bytes);
        decoder.object(|_| Ok(())).unwrap();
        assert_eq!(decoder.position, 12);
        decoder.object(|_| Ok(())).unwrap();
        assert_eq!(decoder.i32().unwrap(), 7);
        decoder.finish().unwrap();
        let mut decoder = Decoder::new(&bytes);
        decoder.object(|_| Ok(())).unwrap();
        assert!(decoder.finish().is_err());
    }

    #[test]
    fn failed_encoder_objects_roll_back_without_losing_prior_values() {
        let mut encoder = Encoder::new();
        encoder.i32(7).unwrap();
        assert!(encoder
            .object(|encoder| {
                encoder.i64(4).unwrap();
                Err(Error::InvalidInput("test failure"))
            })
            .is_err());
        encoder.object(|_| Ok(())).unwrap();
        let bytes = encoder.finish();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.i32().unwrap(), 7);
        decoder.object(|_| Ok(())).unwrap();
        decoder.finish().unwrap();
    }
}
