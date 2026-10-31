//! Native registry copy and transaction metadata, SF 51a7703 CheckpointFile/
//! MetadataOperationData/RedoOperationData. No snapshot/root envelope.
use super::{
    native_bytes::{invalid, put_string, single, unsupported, Reader},
    provider::ProviderMode,
    record::check_size,
};
use crate::{OperationData, Result};
#[derive(Clone)]
pub(crate) struct Metadata {
    pub(crate) id: i64,
    pub(crate) parent: i64,
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) create_lsn: i64,
    pub(crate) delete_lsn: i64,
    pub(crate) initialization: Option<OperationData>,
    pub(crate) mode: ProviderMode,
}
impl Metadata {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.id == 0
            || self.id == super::provider::REGISTRY_ID
            || self.parent == self.id
            || self.parent == super::provider::REGISTRY_ID
            || self.name.is_empty()
            || self.type_name.is_empty()
            || self.create_lsn < -1
            || self.delete_lsn < -1
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        if self.mode == ProviderMode::Stale {
            return Err(unsupported("stale metadata is not a copy snapshot"));
        }
        let mut bytes = vec![0; 4];
        put_string(&mut bytes, &self.name)?;
        put_string(&mut bytes, &self.type_name)?;
        bytes.extend(self.create_lsn.to_le_bytes());
        bytes.extend(self.delete_lsn.to_le_bytes());
        match &self.initialization {
            None => bytes.extend((-1i32).to_le_bytes()),
            Some(data) => {
                let count = i32::try_from(data.segments().len()).map_err(|_| invalid())?;
                bytes.extend(if count == 0 { 0 } else { !count }.to_le_bytes());
                for segment in data.segments() {
                    bytes.extend((segment.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(segment);
                }
            }
        }
        bytes.extend(
            (if self.mode == ProviderMode::Active {
                0i32
            } else {
                1
            })
            .to_le_bytes(),
        );
        bytes.extend(self.id.to_le_bytes());
        bytes.extend(self.parent.to_le_bytes());
        check_size(1, bytes.len() + 17)?;
        let size = i32::try_from(bytes.len()).map_err(|_| invalid())?;
        bytes[..4].copy_from_slice(&size.to_le_bytes());
        Ok(bytes)
    }
    fn decode(reader: &mut Reader<'_>) -> Result<Self> {
        let size = reader.i32()?;
        if size <= 44 {
            return Err(invalid());
        }
        let mut item = Reader::new(reader.take(size as usize - 4)?);
        let name = item.string()?;
        let type_name = item.string()?;
        let create_lsn = item.i64()?;
        let delete_lsn = item.i64()?;
        let initialization = match item.i32()? {
            -1 => None,
            0 => Some(OperationData::new(Vec::new())?),
            value if value < -1 => {
                let count = (!value) as usize;
                check_size(count, 0)?;
                let mut segments = Vec::with_capacity(count);
                for _ in 0..count {
                    let length = item.u32()? as usize;
                    segments.push(item.take(length)?.to_vec());
                }
                Some(OperationData::new(segments)?)
            }
            _ => return Err(unsupported("managed registry-copy initialization")),
        };
        let mode = match item.i32()? {
            0 => ProviderMode::Active,
            1 => ProviderMode::DelayDelete,
            2 => return Err(unsupported("stale metadata in native copy snapshot")),
            _ => return Err(invalid()),
        };
        let result = Self {
            name,
            type_name,
            create_lsn,
            delete_lsn,
            initialization,
            mode,
            id: item.i64()?,
            parent: item.i64()?,
        };
        item.finish()?;
        result.validate()?;
        Ok(result)
    }
}
pub(crate) enum RegistryPacket {
    Version,
    Metadata(Vec<Metadata>),
}
pub(crate) fn decode_copy(data: &OperationData) -> Result<RegistryPacket> {
    let bytes = single(data)?;
    let (&opcode, body) = bytes.split_last().ok_or_else(invalid)?;
    let mut reader = Reader::new(body);
    match opcode {
        0 => {
            if reader.u32()? != 1 {
                return Err(unsupported("native registry-copy version"));
            }
            reader.finish()?;
            Ok(RegistryPacket::Version)
        }
        1 => {
            let mut result = Vec::new();
            while reader.remaining() != 0 {
                if result.len() == 64 {
                    return Err(invalid());
                }
                result.push(Metadata::decode(&mut reader)?);
            }
            if result.is_empty() {
                return Err(invalid());
            }
            Ok(RegistryPacket::Metadata(result))
        }
        _ => Err(unsupported("native registry-copy opcode")),
    }
}
pub(crate) fn encode_copy(mut rows: Vec<Metadata>) -> Result<Vec<OperationData>> {
    if rows.len() > 64 {
        return Err(invalid());
    }
    rows.sort_by(|left, right| right.name.encode_utf16().cmp(left.name.encode_utf16()));
    let mut result = vec![OperationData::new(vec![vec![1, 0, 0, 0, 0]])?];
    let mut chunk = Vec::new();
    for row in rows {
        chunk.extend(row.encode()?);
        if chunk.len() >= 32 * 1024 {
            chunk.push(1);
            result.push(OperationData::new(vec![std::mem::take(&mut chunk)])?);
        }
    }
    if !chunk.is_empty() {
        chunk.push(1);
        result.push(OperationData::new(vec![chunk])?);
    }
    Ok(result)
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyType {
    Insert,
    Delete,
}
pub(crate) struct RegistryOperation {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) kind: ApplyType,
}
impl RegistryOperation {
    pub(crate) fn encode(&self) -> Result<OperationData> {
        if self.id == 0 || self.id == super::provider::REGISTRY_ID || self.name.is_empty() {
            return Err(invalid());
        }
        let mut bytes = 1u32.to_le_bytes().to_vec();
        bytes.extend(self.id.to_le_bytes());
        put_string(&mut bytes, &self.name)?;
        bytes.push(if self.kind == ApplyType::Insert { 1 } else { 2 });
        OperationData::new(vec![bytes])
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        check_size(data.segments().len(), data.bytes())?;
        let mut reader = Reader::new(data.segments().first().ok_or_else(invalid)?);
        if reader.u32()? as usize != data.segments().len() {
            return Err(invalid());
        }
        let id = reader.i64()?;
        let name = reader.string()?;
        let kind = match reader.byte()? {
            1 => ApplyType::Insert,
            2 => ApplyType::Delete,
            _ => return Err(unsupported("native registry ApplyType")),
        };
        reader.finish()?;
        if id == 0 || id == super::provider::REGISTRY_ID || name.is_empty() {
            return Err(invalid());
        }
        Ok(Self { id, name, kind })
    }
}
pub(crate) struct InsertRedo {
    pub(crate) type_name: String,
    pub(crate) parent: i64,
    pub(crate) initialization: Option<OperationData>,
}
impl InsertRedo {
    pub(crate) fn encode(&self) -> Result<OperationData> {
        if self.type_name.is_empty() {
            return Err(invalid());
        }
        let mut header = Vec::new();
        put_string(&mut header, &self.type_name)?;
        let tag = match &self.initialization {
            None => -1,
            Some(data) => !i32::try_from(data.segments().len() + 1).map_err(|_| invalid())?,
        };
        header.extend(tag.to_le_bytes());
        header.extend(self.parent.to_le_bytes());
        let mut segments = vec![header];
        if let Some(data) = &self.initialization {
            segments.extend(data.to_segments());
        }
        OperationData::new(segments)
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        check_size(data.segments().len(), data.bytes())?;
        let mut reader = Reader::new(data.segments().first().ok_or_else(invalid)?);
        let type_name = reader.string()?;
        let tag = reader.i32()?;
        let parent = reader.i64()?;
        reader.finish()?;
        let initialization = match tag {
            -1 if data.segments().len() == 1 => None,
            value if value < -1 && (!value) as usize == data.segments().len() => {
                Some(OperationData::new(
                    data.segments()[1..]
                        .iter()
                        .map(|segment| segment.to_vec())
                        .collect(),
                )?)
            }
            value if value >= 0 => return Err(unsupported("managed registry insertion redo")),
            _ => return Err(invalid()),
        };
        if type_name.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            type_name,
            parent,
            initialization,
        })
    }
}
