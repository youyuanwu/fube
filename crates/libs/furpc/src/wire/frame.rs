//! Native layout and checksum rules follow Service Fabric revision
//! `51a7703a1f83749a9234d3ceb4296d8622e42cca`, specifically
//! `Transport/TcpFrameHeader.{h,cpp}`, `Transport/Tcp{Send,Receive}Buffer.cpp`,
//! and `Common/crc.{h,cpp}`.
//!
//! Unlike native first-frame provider capture, the selected clear-only profile
//! rejects nonzero masks on every frame. Size/resource policy belongs above
//! this allocation-free codec, separately from native representation bounds.

#![allow(
    dead_code,
    reason = "Private framing foundation awaits the connection driver"
)]

use std::fmt;

pub(crate) const FIXED_HEADER_LEN: usize = 12;

pub(crate) type FrameResult<T> = Result<T, FrameError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameError {
    InvalidFrameLength { frame_len: u32, headers_len: u16 },
    HeaderLengthOverflow { len: usize },
    FrameLengthOverflow,
    HeaderChecksumMismatch { expected: u8, actual: u8 },
    BodyChecksumMismatch { expected: u32, actual: u32 },
    UnsupportedProvider { mask: u8 },
    ProviderMismatch { expected: u8, actual: u8 },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrameLength {
                frame_len,
                headers_len,
            } => write!(
                f,
                "invalid native frame length {frame_len} for {headers_len} header bytes"
            ),
            Self::HeaderLengthOverflow { len } => {
                write!(f, "native header length {len} exceeds u16")
            }
            Self::FrameLengthOverflow => f.write_str("native frame length exceeds u32"),
            Self::HeaderChecksumMismatch { expected, actual } => write!(
                f,
                "native header CRC mismatch: expected {expected:#04x}, computed {actual:#04x}"
            ),
            Self::BodyChecksumMismatch { expected, actual } => write!(
                f,
                "native body CRC mismatch: expected {expected:#010x}, computed {actual:#010x}"
            ),
            Self::UnsupportedProvider { mask } => {
                write!(f, "unsupported native security provider mask {mask:#04x}")
            }
            Self::ProviderMismatch { expected, actual } => write!(
                f,
                "native security provider mismatch: expected {expected:#04x}, received {actual:#04x}"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

/// Validated local profile; checksum settings control sending, not receiving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameWireContext {
    provider_mask: u8,
    header_crc: bool,
    body_crc: bool,
}

impl FrameWireContext {
    pub(crate) fn new(provider_mask: u8, header_crc: bool, body_crc: bool) -> FrameResult<Self> {
        if provider_mask != 0 {
            return Err(FrameError::UnsupportedProvider {
                mask: provider_mask,
            });
        }
        Ok(Self {
            provider_mask,
            header_crc,
            body_crc,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    frame_len: u32,
    provider_mask: u8,
    header_crc: u8,
    headers_len: u16,
    body_crc: u32,
}

impl FrameHeader {
    /// `body_crc` covers encoded native headers followed by body, not this header.
    /// The caller computes it over immutable segments before preparing output.
    pub(crate) fn for_send(
        headers_len: usize,
        body_len: usize,
        body_crc: u32,
        wire: &FrameWireContext,
    ) -> FrameResult<Self> {
        let headers_len = u16::try_from(headers_len)
            .map_err(|_| FrameError::HeaderLengthOverflow { len: headers_len })?;
        let frame_len = FIXED_HEADER_LEN
            .checked_add(usize::from(headers_len))
            .and_then(|len| len.checked_add(body_len))
            .and_then(|len| u32::try_from(len).ok())
            .ok_or(FrameError::FrameLengthOverflow)?;
        let mut header = Self {
            frame_len,
            provider_mask: wire.provider_mask,
            header_crc: 0,
            headers_len,
            body_crc: if wire.body_crc { body_crc } else { 0 },
        };
        if wire.header_crc {
            header.header_crc = fixed_header_crc(header.encode());
        }
        Ok(header)
    }

    pub(crate) fn decode(
        bytes: &[u8; FIXED_HEADER_LEN],
        wire: &FrameWireContext,
    ) -> FrameResult<Self> {
        if bytes[5] != 0 {
            let actual = fixed_header_crc(*bytes);
            if bytes[5] != actual {
                return Err(FrameError::HeaderChecksumMismatch {
                    expected: bytes[5],
                    actual,
                });
            }
        }
        let frame_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let headers_len = u16::from_le_bytes([bytes[6], bytes[7]]);
        if frame_len
            .checked_sub(FIXED_HEADER_LEN as u32)
            .is_none_or(|payload_len| u32::from(headers_len) > payload_len)
        {
            return Err(FrameError::InvalidFrameLength {
                frame_len,
                headers_len,
            });
        }
        if bytes[4] != wire.provider_mask {
            return Err(FrameError::ProviderMismatch {
                expected: wire.provider_mask,
                actual: bytes[4],
            });
        }
        Ok(Self {
            frame_len,
            provider_mask: bytes[4],
            header_crc: bytes[5],
            headers_len,
            body_crc: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        })
    }

    pub(crate) fn encode(&self) -> [u8; FIXED_HEADER_LEN] {
        let mut bytes = [0; FIXED_HEADER_LEN];
        bytes[..4].copy_from_slice(&self.frame_len.to_le_bytes());
        bytes[4] = self.provider_mask;
        bytes[5] = self.header_crc;
        bytes[6..8].copy_from_slice(&self.headers_len.to_le_bytes());
        bytes[8..].copy_from_slice(&self.body_crc.to_le_bytes());
        bytes
    }

    pub(crate) fn frame_len(&self) -> u32 {
        self.frame_len
    }

    pub(crate) fn headers_len(&self) -> u16 {
        self.headers_len
    }

    pub(crate) fn body_len(&self) -> u32 {
        self.frame_len - FIXED_HEADER_LEN as u32 - u32::from(self.headers_len)
    }

    /// Call only after all encoded native-header and body bytes have been added.
    pub(crate) fn verify_body_crc(&self, crc: &Crc32) -> FrameResult<()> {
        let actual = crc.value();
        if self.body_crc != 0 && self.body_crc != actual {
            return Err(FrameError::BodyChecksumMismatch {
                expected: self.body_crc,
                actual,
            });
        }
        Ok(())
    }
}

fn fixed_header_crc(mut bytes: [u8; FIXED_HEADER_LEN]) -> u8 {
    bytes[5] = 0;
    bytes[8..].fill(0);
    crc8(&bytes)
}

fn crc8(bytes: &[u8]) -> u8 {
    let mut crc = 0;
    for byte in bytes {
        crc ^= byte;
        for _ in 0..8 {
            crc = if crc & 0x80 != 0 {
                (crc << 1) ^ 0x07
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Incremental native CRC-32 over segments; observing the value does not reset it.
#[derive(Debug)]
pub(crate) struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub(crate) fn new() -> Self {
        Self { state: u32::MAX }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u32::from(*byte);
            for _ in 0..8 {
                self.state = (self.state >> 1) ^ if self.state & 1 != 0 { 0xedb8_8320 } else { 0 };
            }
        }
    }

    pub(crate) fn value(&self) -> u32 {
        !self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_wire() -> FrameWireContext {
        FrameWireContext::new(0, true, false).unwrap()
    }

    fn unchecked_lengths(frame_len: u32, headers_len: u16) -> [u8; FIXED_HEADER_LEN] {
        let mut bytes = [0; FIXED_HEADER_LEN];
        bytes[..4].copy_from_slice(&frame_len.to_le_bytes());
        bytes[6..8].copy_from_slice(&headers_len.to_le_bytes());
        bytes
    }

    #[test]
    fn native_checksum_vectors_and_incremental_updates() {
        // Independent expected values from Common/crc.test.cpp at the pinned revision.
        for (bytes, expected8, expected32) in [
            (&b"this is a test string"[..], 0x17, 0xfc86_d6b5),
            (&b"this is a test strinf"[..], 0x10, 0x8b81_e623),
            (&b"Garbled data@34@!@%@#"[..], 0x9f, 0x582d_102a),
            (&b"123456789"[..], 0xf4, 0xcbf4_3926),
            (&b""[..], 0, 0),
        ] {
            assert_eq!(crc8(bytes), expected8);
            for split in 0..=bytes.len() {
                let mut crc = Crc32::new();
                crc.update(&bytes[..split]);
                let _ = crc.value();
                crc.update(&[]);
                crc.update(&bytes[split..]);
                assert_eq!(crc.value(), expected32, "split {split}");
            }
            let mut crc = Crc32::new();
            for byte in bytes {
                crc.update(std::slice::from_ref(byte));
            }
            assert_eq!(crc.value(), expected32);
        }
        let mut crc = Crc32::new();
        crc.update(b"this is a test string");
        assert_eq!(crc.value(), 0xfc86_d6b5);
        crc.update(b"this is a part 2 of s1");
        assert_eq!(crc.value(), 0x36af_107a);
    }

    #[test]
    fn fixed_header_matches_independent_native_layout_vectors() {
        // Header CRC values were derived using the native Common/crc.cpp table.
        let wire = FrameWireContext::new(0, true, true).unwrap();
        let bytes = [0x15, 0, 0, 0, 0, 0x9e, 3, 0, 0x26, 0x39, 0xf4, 0xcb];
        let header = FrameHeader::decode(&bytes, &wire).unwrap();
        assert_eq!(header.frame_len(), 21);
        assert_eq!(header.headers_len(), 3);
        assert_eq!(header.body_len(), 6);
        assert_eq!(header.encode(), bytes);
        assert_eq!(
            FrameHeader::for_send(3, 6, 0xcbf4_3926, &wire)
                .unwrap()
                .encode(),
            bytes
        );
        let mut crc = Crc32::new();
        crc.update(b"123");
        crc.update(b"456789");
        header.verify_body_crc(&crc).unwrap();

        assert_eq!(
            FrameHeader::for_send(0, 0, 0, &wire).unwrap().encode(),
            [12, 0, 0, 0, 0, 0x95, 0, 0, 0, 0, 0, 0]
        );
        let bytes = [0x78, 0x56, 0x34, 0x12, 0, 0x8c, 0xcd, 0xab, 0, 0, 0, 0];
        let header = FrameHeader::decode(&bytes, &wire).unwrap();
        assert_eq!(header.frame_len(), 0x1234_5678);
        assert_eq!(header.headers_len(), 0xabcd);
        assert_eq!(
            FrameHeader::for_send(0xabcd, 0x1234_5678 - 12 - 0xabcd, 0, &wire)
                .unwrap()
                .encode(),
            bytes
        );
    }

    #[test]
    fn checksum_generation_policy_does_not_require_received_checksums() {
        let mut crc = Crc32::new();
        crc.update(b"123456789");
        for header_crc in [false, true] {
            for body_crc in [false, true] {
                let wire = FrameWireContext::new(0, header_crc, body_crc).unwrap();
                let bytes = FrameHeader::for_send(3, 6, crc.value(), &wire)
                    .unwrap()
                    .encode();
                assert_eq!(bytes[5], if header_crc { 0x9e } else { 0 });
                assert_eq!(
                    &bytes[8..],
                    if body_crc {
                        &[0x26, 0x39, 0xf4, 0xcb]
                    } else {
                        &[0; 4]
                    }
                );
                let receiver = FrameWireContext::new(0, !header_crc, !body_crc).unwrap();
                let header = FrameHeader::decode(&bytes, &receiver).unwrap();
                header.verify_body_crc(&crc).unwrap();
                if !body_crc {
                    header.verify_body_crc(&Crc32::new()).unwrap();
                }
                let unchecked = unchecked_lengths(21, 3);
                FrameHeader::decode(&unchecked, &wire)
                    .unwrap()
                    .verify_body_crc(&crc)
                    .unwrap();
            }
        }
    }

    #[test]
    fn header_crc_excludes_both_crc_fields_and_detects_fixed_field_corruption() {
        let wire = FrameWireContext::new(0, true, true).unwrap();
        let receiver = FrameWireContext::new(0, false, false).unwrap();
        let bytes = FrameHeader::for_send(3, 6, 0xcbf4_3926, &wire)
            .unwrap()
            .encode();
        for index in 0..8 {
            let mut corrupted = bytes;
            corrupted[index] ^= 1;
            assert!(matches!(
                FrameHeader::decode(&corrupted, &receiver),
                Err(FrameError::HeaderChecksumMismatch { .. })
            ));
        }
        for index in 8..12 {
            let mut corrupted = bytes;
            corrupted[index] ^= 1;
            let header = FrameHeader::decode(&corrupted, &receiver).unwrap();
            let mut crc = Crc32::new();
            crc.update(b"123456789");
            assert!(matches!(
                header.verify_body_crc(&crc),
                Err(FrameError::BodyChecksumMismatch { .. })
            ));
        }
    }

    #[test]
    fn body_crc_covers_native_headers_and_body_but_not_fixed_header() {
        let wire = FrameWireContext::new(0, true, true).unwrap();
        let header = FrameHeader::for_send(3, 6, 0xcbf4_3926, &wire).unwrap();
        for index in 0..9 {
            let mut payload = *b"123456789";
            payload[index] ^= 1;
            let mut crc = Crc32::new();
            crc.update(&payload[..3]);
            crc.update(&payload[3..]);
            assert!(matches!(
                header.verify_body_crc(&crc),
                Err(FrameError::BodyChecksumMismatch { .. })
            ));
        }
        let mut crc = Crc32::new();
        crc.update(&header.encode());
        crc.update(b"123456789");
        assert!(matches!(
            header.verify_body_crc(&crc),
            Err(FrameError::BodyChecksumMismatch { .. })
        ));
    }

    #[test]
    fn nonclear_providers_cannot_be_configured_or_accepted() {
        let wire = clear_wire();
        for mask in 1..=u8::MAX {
            assert_eq!(
                FrameWireContext::new(mask, true, false),
                Err(FrameError::UnsupportedProvider { mask })
            );
            let mut bytes = unchecked_lengths(12, 0);
            bytes[4] = mask;
            bytes[5] = fixed_header_crc(bytes);
            assert_eq!(
                FrameHeader::decode(&bytes, &wire),
                Err(FrameError::ProviderMismatch {
                    expected: 0,
                    actual: mask,
                })
            );
        }
    }

    #[test]
    fn invalid_lengths_are_rejected_without_underflow() {
        let wire = clear_wire();
        for frame_len in 0..12 {
            assert_eq!(
                FrameHeader::decode(&unchecked_lengths(frame_len, 0), &wire),
                Err(FrameError::InvalidFrameLength {
                    frame_len,
                    headers_len: 0,
                })
            );
        }
        for (frame_len, headers_len) in [(12, 1), (20, 9), (65546, u16::MAX)] {
            assert_eq!(
                FrameHeader::decode(&unchecked_lengths(frame_len, headers_len), &wire),
                Err(FrameError::InvalidFrameLength {
                    frame_len,
                    headers_len,
                })
            );
        }
    }

    #[test]
    fn native_length_boundaries_need_no_body_allocations_or_local_size_cap() {
        let wire = clear_wire();
        for headers_len in [0, usize::from(u16::MAX)] {
            for frame_len in [FIXED_HEADER_LEN + headers_len, u32::MAX as usize] {
                let body_len = frame_len - FIXED_HEADER_LEN - headers_len;
                let header = FrameHeader::for_send(headers_len, body_len, 0, &wire).unwrap();
                let decoded = FrameHeader::decode(&header.encode(), &wire).unwrap();
                assert_eq!(decoded.frame_len() as usize, frame_len);
                assert_eq!(usize::from(decoded.headers_len()), headers_len);
                assert_eq!(decoded.body_len() as usize, body_len);
            }
            let body_len = u32::MAX as usize - FIXED_HEADER_LEN - headers_len + 1;
            assert_eq!(
                FrameHeader::for_send(headers_len, body_len, 0, &wire),
                Err(FrameError::FrameLengthOverflow)
            );
            assert_eq!(
                FrameHeader::for_send(headers_len, usize::MAX, 0, &wire),
                Err(FrameError::FrameLengthOverflow)
            );
        }
        let len = usize::from(u16::MAX) + 1;
        assert_eq!(
            FrameHeader::for_send(len, 0, 0, &wire),
            Err(FrameError::HeaderLengthOverflow { len })
        );
        assert_eq!(
            FrameHeader::for_send(usize::MAX, 0, 0, &wire),
            Err(FrameError::HeaderLengthOverflow { len: usize::MAX })
        );
    }
}
