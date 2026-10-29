use crate::{Error, Result};

pub const FIXED_HEADER_SIZE: usize = 12;
pub const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;
pub const MAX_HEADER_SIZE: usize = u16::MAX as usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub headers: Vec<u8>,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.encode_with_body_crc(false)
    }

    pub fn encode_with_body_crc(&self, body_crc: bool) -> Result<Vec<u8>> {
        let size = FIXED_HEADER_SIZE
            .checked_add(self.headers.len())
            .and_then(|n| n.checked_add(self.body.len()))
            .ok_or(Error::Protocol("frame size overflow"))?;
        if size > MAX_FRAME_SIZE || self.headers.len() > MAX_HEADER_SIZE {
            return Err(Error::Protocol("frame or header area exceeds limit"));
        }
        let mut bytes = vec![0; FIXED_HEADER_SIZE];
        bytes[..4].copy_from_slice(&(size as u32).to_le_bytes());
        bytes[6..8].copy_from_slice(&(self.headers.len() as u16).to_le_bytes());
        bytes[5] = crc8(&bytes);
        bytes.extend_from_slice(&self.headers);
        bytes.extend_from_slice(&self.body);
        if body_crc {
            let checksum = crc32(&bytes[FIXED_HEADER_SIZE..]);
            bytes[8..12].copy_from_slice(&checksum.to_le_bytes());
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let fixed: &[u8; FIXED_HEADER_SIZE] = bytes
            .get(..FIXED_HEADER_SIZE)
            .ok_or(Error::Protocol("truncated fixed frame header"))?
            .try_into()
            .map_err(|_| Error::Protocol("invalid fixed frame header"))?;
        let (size, header_size) = validate_fixed(fixed)?;
        if bytes.len() != size {
            return Err(Error::Protocol("frame length does not match input"));
        }
        let checksum = u32::from_le_bytes(fixed[8..12].try_into().unwrap());
        if checksum != 0 && checksum != crc32(&bytes[FIXED_HEADER_SIZE..]) {
            return Err(Error::Protocol("frame body CRC mismatch"));
        }
        Ok(Self {
            headers: bytes[FIXED_HEADER_SIZE..FIXED_HEADER_SIZE + header_size].to_vec(),
            body: bytes[FIXED_HEADER_SIZE + header_size..].to_vec(),
        })
    }
}

pub fn validate_fixed(fixed: &[u8; FIXED_HEADER_SIZE]) -> Result<(usize, usize)> {
    let size = u32::from_le_bytes(fixed[..4].try_into().unwrap()) as usize;
    let header_size = u16::from_le_bytes(fixed[6..8].try_into().unwrap()) as usize;
    if !(FIXED_HEADER_SIZE..=MAX_FRAME_SIZE).contains(&size)
        || header_size > size - FIXED_HEADER_SIZE
    {
        return Err(Error::Protocol("invalid or oversized frame length"));
    }
    if fixed[4] != 0 {
        return Err(Error::Protocol("only clear-TCP frames are supported"));
    }
    if fixed[5] != 0 {
        let mut checked = *fixed;
        checked[5] = 0;
        checked[8..12].fill(0);
        if fixed[5] != crc8(&checked) {
            return Err(Error::Protocol("frame header CRC mismatch"));
        }
    }
    Ok((size, header_size))
}

pub fn crc8(bytes: &[u8]) -> u8 {
    let mut crc = 0u8;
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

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xedb8_8320 } else { 0 };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_crc_vectors() {
        assert_eq!(crc8(b"123456789"), 0xf4);
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn crc_fields_and_truncation() {
        let frame = Frame {
            headers: vec![1, 2, 3],
            body: vec![4, 5, 6],
        };
        for body_crc in [false, true] {
            let bytes = frame.encode_with_body_crc(body_crc).unwrap();
            assert_eq!(Frame::decode(&bytes).unwrap(), frame);
            for split in 0..bytes.len() {
                assert!(Frame::decode(&bytes[..split]).is_err());
            }
        }
        let mut bytes = frame.encode_with_body_crc(true).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(Frame::decode(&bytes).is_err());
        let mut bytes = frame.encode().unwrap();
        bytes[6] ^= 1;
        assert!(Frame::decode(&bytes).is_err());
    }

    #[test]
    fn absent_crc_and_invalid_lengths() {
        let frame = Frame {
            headers: vec![],
            body: vec![0; 10],
        };
        let mut bytes = frame.encode().unwrap();
        bytes[5] = 0;
        assert_eq!(Frame::decode(&bytes).unwrap(), frame);
        bytes[4] = 1;
        assert!(Frame::decode(&bytes).is_err());
        bytes[4] = 0;
        bytes[..4].copy_from_slice(&((MAX_FRAME_SIZE + 1) as u32).to_le_bytes());
        assert!(Frame::decode(&bytes).is_err());
        bytes[..4].copy_from_slice(&11u32.to_le_bytes());
        assert!(Frame::decode(&bytes).is_err());
    }

    #[test]
    fn header_limit_is_exact() {
        let mut frame = Frame {
            headers: vec![0; MAX_HEADER_SIZE],
            body: vec![],
        };
        assert!(frame.encode().is_ok());
        frame.headers.push(0);
        assert!(frame.encode().is_err());
        frame.headers.clear();
        frame.body.resize(MAX_FRAME_SIZE, 0);
        assert!(frame.encode().is_err());
    }
}
