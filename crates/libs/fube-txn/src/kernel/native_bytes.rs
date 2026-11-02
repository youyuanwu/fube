//! Bounded native little-endian/UTF-8-varint primitives, no extra envelope.
use super::record::check_size;
use crate::{Epoch, Error, OperationData, Result};
pub(crate) fn invalid() -> Error {
    Error::InvalidInput("invalid native codec input")
}
pub(crate) fn unsupported(message: &'static str) -> Error {
    Error::new(fube_runtime::ErrorKind::Unsupported, message)
}
pub(crate) fn single(data: &OperationData) -> Result<&[u8]> {
    check_size(data.segments().len(), data.bytes())?;
    if data.segments().len() != 1 {
        return Err(invalid());
    }
    Ok(&data.segments()[0])
}
pub(crate) struct Reader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) position: usize,
}
impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(count).ok_or_else(invalid)?;
        let result = self.bytes.get(self.position..end).ok_or_else(invalid)?;
        self.position = end;
        Ok(result)
    }
    pub(crate) fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn i32(&mut self) -> Result<i32> {
        Ok(self.u32()? as i32)
    }
    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn epoch(&mut self) -> Result<Epoch> {
        Ok(Epoch {
            data_loss_number: self.i64()?,
            configuration_number: self.i64()?,
        })
    }
    pub(crate) fn string(&mut self) -> Result<String> {
        let mut length = 0u32;
        let mut terminated = false;
        for shift in [0, 7, 14, 21, 28] {
            let byte = self.byte()?;
            if shift == 28 && byte > 15 {
                return Err(invalid());
            }
            length |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                terminated = true;
                break;
            }
        }
        if !terminated || length > 4096 {
            return Err(invalid());
        }
        let value = std::str::from_utf8(self.take(length as usize)?).map_err(|_| invalid())?;
        if value.contains('\0') {
            return Err(invalid());
        }
        Ok(value.to_owned())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
pub(crate) fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.len() > 4096 || value.contains('\0') {
        return Err(invalid());
    }
    let mut length = value.len() as u32;
    loop {
        let byte = (length & 127) as u8;
        length >>= 7;
        bytes.push(byte | if length == 0 { 0 } else { 128 });
        if length == 0 {
            break;
        }
    }
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}
pub(crate) fn put_epoch(bytes: &mut Vec<u8>, epoch: Epoch) {
    bytes.extend(epoch.data_loss_number.to_le_bytes());
    bytes.extend(epoch.configuration_number.to_le_bytes());
}
