use crate::{Error, Result};

pub mod serde;

pub const MAX_STRING_UNITS: usize = 32_768;
pub const MAX_COLLECTION_COUNT: usize = 4_096;
pub const MAX_DEPTH: usize = 32;
pub const MAX_OBJECT_SIZE: usize = 4 * 1024 * 1024;

const OBJECT: u8 = 0x00;
const SCOPE_BEGIN: u8 = 0x1f;
const SCOPE_END: u8 = 0x2f;
const OBJECT_END: u8 = 0x3f;
const EMPTY: u8 = 0x40;
const POINTER: u8 = 0x01;
const OBJECT_ARRAY: u8 = 0x80;
const UINT16: u8 = 0x06;
const INT32: u8 = 0x07;
const UINT32: u8 = 0x08;
const INT64: u8 = 0x09;
const UINT64: u8 = 0x0a;
const GUID: u8 = 0x0c;
const STRING: u8 = 0x8d;
const BYTE_ARRAY: u8 = 0x84;
const TRUE: u8 = 0x42;
const FALSE: u8 = 0x72;

/// The explicit native serializer subset used by the clear-TCP POC.
#[derive(Default)]
pub struct Encoder {
    bytes: Vec<u8>,
    depth: usize,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn finish(self) -> Vec<u8> {
        self.bytes
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
        let start = self.begin_object(type_info)?;
        let result = f(self);
        if let Err(error) = result {
            self.bytes.truncate(start);
            self.depth -= 1;
            return Err(error);
        }
        self.end_object(start)
    }

    fn begin_object(&mut self, type_info: &[u8]) -> Result<usize> {
        if self.depth >= MAX_DEPTH {
            return Err(Error::InvalidInput("object nesting limit exceeded"));
        }
        if type_info.len() > MAX_COLLECTION_COUNT {
            return Err(Error::InvalidInput("type-information limit exceeded"));
        }
        let start = self.bytes.len();
        // Marker, little-endian size, flags, and three deterministic pad bytes.
        self.bytes.extend_from_slice(&[0; 9]);
        if !type_info.is_empty() {
            self.bytes[start + 5] = 1;
            self.write_unsigned_payload(type_info.len() as u64);
            self.bytes.extend_from_slice(type_info);
        }
        self.bytes.push(SCOPE_BEGIN);
        self.depth += 1;
        Ok(start)
    }

    fn end_object(&mut self, start: usize) -> Result<()> {
        self.depth -= 1;
        self.bytes.extend_from_slice(&[SCOPE_END, OBJECT_END]);
        let size = self.bytes.len() - start - 1;
        if size > MAX_OBJECT_SIZE {
            self.bytes.truncate(start);
            return Err(Error::InvalidInput("object size limit exceeded"));
        }
        self.bytes[start + 1..start + 5].copy_from_slice(&(size as u32).to_le_bytes());
        Ok(())
    }

    /// End the current base scope and begin a derived scope.
    pub fn next_scope(&mut self) -> Result<()> {
        if self.depth == 0 {
            return Err(Error::InvalidInput("scope requires an object"));
        }
        self.bytes.extend_from_slice(&[SCOPE_END, SCOPE_BEGIN]);
        Ok(())
    }

    /// A nullable object pointer; `None` emits the native empty-pointer marker.
    pub fn pointer(
        &mut self,
        type_info: Option<&[u8]>,
        f: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        let start = self.bytes.len();
        if let Some(type_info) = type_info {
            self.bytes.push(POINTER);
            if let Err(error) = self.typed_object(type_info, f) {
                self.bytes.truncate(start);
                return Err(error);
            }
        } else {
            self.bytes.push(POINTER | EMPTY);
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
        if count > MAX_COLLECTION_COUNT {
            return Err(Error::InvalidInput("object-array count limit exceeded"));
        }
        if count == 0 {
            self.bytes.push(OBJECT_ARRAY | EMPTY);
        } else {
            self.bytes.push(OBJECT_ARRAY);
            self.write_unsigned_payload(count as u64);
        }
        Ok(())
    }

    /// Encode a native compressed UInt16, including its scalar metadata.
    pub fn u16(&mut self, value: u16) {
        self.unsigned(UINT16, u64::from(value));
    }

    pub fn i32(&mut self, value: i32) {
        self.signed(INT32, i64::from(value));
    }

    pub fn u32(&mut self, value: u32) {
        self.unsigned(UINT32, u64::from(value));
    }

    pub fn i64(&mut self, value: i64) {
        self.signed(INT64, value);
    }

    pub fn u64(&mut self, value: u64) {
        self.unsigned(UINT64, value);
    }

    pub fn boolean(&mut self, value: bool) {
        self.bytes.push(if value { TRUE } else { FALSE });
    }

    /// Accepts the native Windows-order GUID bytes without reordering them.
    pub fn guid(&mut self, value: &[u8; 16]) {
        if *value == [0; 16] {
            self.bytes.push(GUID | EMPTY);
        } else {
            self.bytes.push(GUID);
            self.bytes.extend_from_slice(value);
        }
    }

    pub fn string(&mut self, value: &str) -> Result<()> {
        // At most three UTF-8 bytes per UTF-16 code unit. Reject oversized
        // caller buffers before scanning them; encoding work stays bounded too.
        if value.len() > MAX_STRING_UNITS * 3 {
            return Err(Error::InvalidInput("string code-unit limit exceeded"));
        }
        let count = value.encode_utf16().count();
        if count > MAX_STRING_UNITS {
            return Err(Error::InvalidInput("string code-unit limit exceeded"));
        }
        if count == 0 {
            self.bytes.push(STRING | EMPTY);
        } else {
            self.bytes.push(STRING);
            self.write_unsigned_payload(count as u64);
            for unit in value.encode_utf16() {
                self.bytes.extend_from_slice(&unit.to_le_bytes());
            }
        }
        Ok(())
    }

    pub fn byte_array(&mut self, value: &[u8]) -> Result<()> {
        if value.len() > MAX_COLLECTION_COUNT {
            return Err(Error::InvalidInput("byte-array count limit exceeded"));
        }
        if value.is_empty() {
            self.bytes.push(BYTE_ARRAY | EMPTY);
        } else {
            self.bytes.push(BYTE_ARRAY);
            self.write_unsigned_payload(value.len() as u64);
            self.bytes.extend_from_slice(value);
        }
        Ok(())
    }

    fn signed(&mut self, metadata: u8, mut value: i64) {
        if value == 0 {
            self.bytes.push(metadata | EMPTY);
            return;
        }
        self.bytes.push(metadata);
        let mut payload = [0; 10];
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
        self.write_groups(&payload[start..]);
    }

    fn unsigned(&mut self, metadata: u8, value: u64) {
        if value == 0 {
            self.bytes.push(metadata | EMPTY);
        } else {
            self.bytes.push(metadata);
            self.write_unsigned_payload(value);
        }
    }

    fn write_unsigned_payload(&mut self, mut value: u64) {
        let mut payload = [0; 10];
        let mut start = payload.len();
        loop {
            start -= 1;
            payload[start] = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        self.write_groups(&payload[start..]);
    }

    fn write_groups(&mut self, payload: &[u8]) {
        for (index, &byte) in payload.iter().enumerate() {
            self.bytes
                .push(byte | if index + 1 < payload.len() { 0x80 } else { 0 });
        }
    }
}

/// A bounded reader for supported positional native values, not a general
/// forward-compatible reader for every Fabric metadata or extension type.
pub struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
    limit: usize,
    depth: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            limit: bytes.len(),
            depth: 0,
        }
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
                let count = self.count(MAX_COLLECTION_COUNT)?;
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

    pub fn i64(&mut self) -> Result<i64> {
        self.signed(INT64, 64)
    }

    pub fn u64(&mut self) -> Result<u64> {
        self.unsigned(UINT64, 64)
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

    fn string_with_budget(
        &mut self,
        reserve: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<String> {
        let bytes = self.string_bytes()?;
        let length = Self::characters(bytes).try_fold(0, |length, character| {
            character
                .map(|character| length + character.len_utf8())
                .map_err(|_| Error::Protocol("invalid UTF-16 string"))
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

    fn read_object<T>(&mut self, f: impl FnOnce(&mut Self, &'a [u8]) -> Result<T>) -> Result<T> {
        if self.depth >= MAX_DEPTH {
            return Err(Error::Protocol("object nesting limit exceeded"));
        }
        self.expect(OBJECT)?;
        let extent_start = self.position;
        let header = self.take(8)?;
        let size = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if !(11..=MAX_OBJECT_SIZE).contains(&size) {
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
            let count = self.count(MAX_COLLECTION_COUNT)?;
            if count == 0 {
                return Err(Error::Protocol("empty object type information"));
            }
            self.take(count)?
        } else {
            &[]
        };
        self.expect(SCOPE_BEGIN)?;
        let value = f(self, type_info)?;
        self.finish_scope()?;
        while self.peek()? == SCOPE_BEGIN {
            self.byte()?;
            self.finish_scope()?;
        }
        self.expect(OBJECT_END)?;
        if self.position != end {
            return Err(Error::Protocol(
                "object size does not match consumed extent",
            ));
        }
        Ok(value)
    }

    fn finish_scope(&mut self) -> Result<()> {
        loop {
            match self.peek()? {
                SCOPE_END => {
                    self.byte()?;
                    return Ok(());
                }
                SCOPE_BEGIN | OBJECT_END => {
                    return Err(Error::Protocol("unexpected scope or object delimiter"));
                }
                _ => self.skip_value()?,
            }
        }
    }

    fn skip_value(&mut self) -> Result<()> {
        match self.peek()? {
            OBJECT => self.object(|_| Ok(())),
            POINTER | 0x41 => self.pointer(|_, _| Ok(())).map(|_| ()),
            OBJECT_ARRAY | 0xc0 => {
                let count = self.object_array_count()?;
                for _ in 0..count {
                    self.object(|_| Ok(()))?;
                }
                Ok(())
            }
            UINT16 | 0x46 => self.u16().map(|_| ()),
            INT32 | 0x47 => self.i32().map(|_| ()),
            UINT32 | 0x48 => self.u32().map(|_| ()),
            INT64 | 0x49 => self.i64().map(|_| ()),
            UINT64 | 0x4a => self.u64().map(|_| ()),
            GUID | 0x4c => self.guid().map(|_| ()),
            TRUE | FALSE => self.boolean().map(|_| ()),
            STRING | 0xcd => {
                let bytes = self.string_bytes()?;
                for character in Self::characters(bytes) {
                    character.map_err(|_| Error::Protocol("invalid UTF-16 string"))?;
                }
                Ok(())
            }
            BYTE_ARRAY | 0xc4 => self.byte_array_bytes().map(|_| ()),
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

    fn count(&mut self, maximum: usize) -> Result<usize> {
        let count = self.read_unsigned_payload(32)? as usize;
        if count > maximum {
            return Err(Error::Protocol("collection count limit exceeded"));
        }
        Ok(count)
    }

    fn string_bytes(&mut self) -> Result<&'a [u8]> {
        match self.byte()? {
            metadata if metadata == STRING | EMPTY => Ok(&[]),
            STRING => {
                let count = self.count(MAX_STRING_UNITS)?;
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
                let count = self.count(MAX_COLLECTION_COUNT)?;
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
                    encoder.u16(u16::MAX);
                    encoder.next_scope()?;
                    encoder.object_array(&[0u16, 127, 128, 32768], |encoder, value| {
                        encoder.u16(*value);
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
        encoder.u16(42);
        assert!(encoder
            .pointer(Some(&[]), |_| Err(Error::InvalidInput("model failure")))
            .is_err());
        assert_eq!(Decoder::new(&encoder.finish()).u16().unwrap(), 42);
        let mut encoder = Encoder::new();
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
            encoder.write_unsigned_payload(count);
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
            let bytes = encode(|encoder| encoder.i64(value));
            let mut decoder = Decoder::new(&bytes);
            assert_eq!(decoder.i64().unwrap(), value);
            decoder.finish().unwrap();
            if let Ok(value) = i32::try_from(value) {
                let bytes = encode(|encoder| encoder.i32(value));
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
            let bytes = encode(|encoder| encoder.u64(value));
            let mut decoder = Decoder::new(&bytes);
            assert_eq!(decoder.u64().unwrap(), value);
            decoder.finish().unwrap();
            if let Ok(value) = u32::try_from(value) {
                let bytes = encode(|encoder| encoder.u32(value));
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
            encoder.i32(0);
            encoder.u32(0);
            encoder.i64(0);
            encoder.u64(0);
            encoder.boolean(true);
            encoder.boolean(false);
            encoder.guid(&[0; 16]);
            encoder.guid(&guid);
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
            assert!(Decoder::new(&object).object(|_| Ok(())).is_err());
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
                        encoder.u32(128);
                        Ok(())
                    })?;
                    encoder.i32(-65);
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
        let mut encoder = Encoder::new();
        write_nested(&mut encoder, MAX_DEPTH).unwrap();
        let bytes = encoder.finish();
        let mut decoder = Decoder::new(&bytes);
        read_nested(&mut decoder, MAX_DEPTH).unwrap();
        decoder.finish().unwrap();
        let mut decoder = Decoder::new(&bytes);
        decoder.object(|_| Ok(())).unwrap();
        decoder.finish().unwrap();

        let mut encoder = Encoder::new();
        assert!(matches!(
            write_nested(&mut encoder, MAX_DEPTH + 1),
            Err(Error::InvalidInput(_))
        ));
        assert!(encoder.finish().is_empty());
        let too_deep = object_bytes(&bytes);
        assert!(read_nested(&mut Decoder::new(&too_deep), MAX_DEPTH + 1).is_err());
        assert!(Decoder::new(&too_deep).object(|_| Ok(())).is_err());
    }

    #[test]
    fn every_truncated_prefix_fails() {
        let bytes = encode(|encoder| {
            encoder
                .object(|encoder| {
                    encoder.i32(i32::MIN);
                    encoder.u32(u32::MAX);
                    encoder.i64(i64::MIN);
                    encoder.u64(u64::MAX);
                    encoder.boolean(true);
                    encoder.guid(&[0x3f; 16]);
                    encoder.string("x🦀").unwrap();
                    encoder.byte_array(&[0x1f, 0x2f, 0x3f]).unwrap();
                    encoder.object(|encoder| {
                        encoder.i64(-65);
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
            encode(|encoder| encoder.i32(i32::MIN)),
            encode(|encoder| encoder.u32(u32::MAX)),
            encode(|encoder| encoder.i64(i64::MIN)),
            encode(|encoder| encoder.u64(u64::MAX)),
            encode(|encoder| encoder.guid(&[1; 16])),
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
        let mut encoder = Encoder::new();
        assert!(matches!(
            encoder.string(&too_long),
            Err(Error::InvalidInput(_))
        ));
        assert!(encoder.finish().is_empty());
        let array = vec![0x3f; MAX_COLLECTION_COUNT];
        let bytes = encode(|encoder| encoder.byte_array(&array).unwrap());
        assert_eq!(Decoder::new(&bytes).byte_array().unwrap(), array);
        let mut encoder = Encoder::new();
        assert!(matches!(
            encoder.byte_array(&vec![0; MAX_COLLECTION_COUNT + 1]),
            Err(Error::InvalidInput(_))
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
                encoder.bytes.extend_from_slice(&fields);
                Ok(())
            })
            .unwrap();
        assert_eq!(encoder.finish().len(), MAX_OBJECT_SIZE + 1);
        let mut encoder = Encoder::new();
        assert!(matches!(
            encoder.object(|encoder| {
                encoder.bytes.extend_from_slice(&fields);
                encoder.u32(0);
                Ok(())
            }),
            Err(Error::InvalidInput(_))
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
            count_bytes.write_unsigned_payload(count);
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
            encoder.i32(-1);
            encoder.i32(47);
            encoder.u32(63);
            encoder.i64(i64::MIN);
            encoder.u64(u64::MAX);
            encoder.boolean(true);
            encoder.boolean(false);
            encoder.guid(&[0x2f; 16]);
            encoder.guid(&[0; 16]);
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
            encoder.i32(7);
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
        encoder.i32(7);
        assert!(encoder
            .object(|encoder| {
                encoder.i64(4);
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
