// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native CopyContext/Header/Metadata and ProgressVector selection, pin51a7703.
//! Source: logrecordlib/ProgressVector.cpp:130–257,304–481; CopyStream.cpp:167–351.
use super::{
    memory_log::{vector_bytes, ProgressEntry},
    native_bytes::{invalid, put_epoch, single, unsupported, Reader},
    status::Status,
};
use crate::{Epoch, Error, OperationData, Result};
use std::sync::Arc;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum Stage {
    None = 2,
    State = 3,
    Progress = 4,
    FalseProgress = 5,
    Log = 7,
}
impl Stage {
    pub(crate) fn read(value: i32) -> Result<Self> {
        match value {
            2 => Ok(Self::None),
            3 => Ok(Self::State),
            4 => Ok(Self::Progress),
            5 => Ok(Self::FalseProgress),
            7 => Ok(Self::Log),
            _ => Err(unsupported("native copy stage")),
        }
    }
}
pub(crate) fn epoch_key(epoch: Epoch) -> (i64, i64) {
    (epoch.data_loss_number, epoch.configuration_number)
}
pub(crate) fn equal(a: &ProgressEntry, b: &ProgressEntry) -> bool {
    a.epoch == b.epoch && a.lsn == b.lsn
}
pub(crate) fn valid_vector(vector: &[ProgressEntry]) -> bool {
    !vector.is_empty()
        && vector.len() <= 128
        && vector.iter().all(|v| {
            v.lsn >= 0 && v.epoch.data_loss_number >= 0 && v.epoch.configuration_number >= 0
        })
        && vector
            .windows(2)
            .all(|w| epoch_key(w[0].epoch) < epoch_key(w[1].epoch) && w[0].lsn <= w[1].lsn)
}
pub(crate) fn read_vector(reader: &mut Reader<'_>) -> Result<Vec<ProgressEntry>> {
    let count = reader.u32()? as usize;
    if count == 0 || count > 128 {
        return Err(invalid());
    }
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push(ProgressEntry {
            epoch: reader.epoch()?,
            lsn: reader.i64()?,
            primary: reader.i64()?,
            ticks: reader.i64()?,
        });
    }
    if !valid_vector(&entries) {
        return Err(invalid());
    }
    Ok(entries)
}
#[derive(Clone)]
pub(crate) struct CopyContext {
    pub(crate) replica: i64,
    pub(crate) vector: Vec<ProgressEntry>,
    pub(crate) head_epoch: Epoch,
    pub(crate) head: i64,
    pub(crate) tail: i64,
    pub(crate) atomic_redo: i64,
}
impl CopyContext {
    pub(crate) fn volatile_null(ticks: i64) -> Self {
        let epoch = Epoch {
            data_loss_number: 0,
            configuration_number: 0,
        };
        Self {
            replica: 999,
            vector: vec![ProgressEntry {
                epoch,
                lsn: 1,
                primary: 1,
                ticks,
            }],
            head_epoch: epoch,
            head: 1,
            tail: 1,
            atomic_redo: 0,
        }
    }
    pub(crate) fn encode(&self) -> Result<OperationData> {
        self.validate()?;
        let mut bytes = self.replica.to_le_bytes().to_vec();
        bytes.extend(vector_bytes(&self.vector));
        put_epoch(&mut bytes, self.head_epoch);
        for value in [self.head, self.tail, self.atomic_redo] {
            bytes.extend(value.to_le_bytes());
        }
        OperationData::new(vec![bytes])
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        let mut r = Reader::new(single(data)?);
        let result = Self {
            replica: r.i64()?,
            vector: read_vector(&mut r)?,
            head_epoch: r.epoch()?,
            head: r.i64()?,
            tail: r.i64()?,
            atomic_redo: r.i64()?,
        };
        r.finish()?;
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<()> {
        if self.atomic_redo != 0 {
            return Err(unsupported("atomic-redo recovery copy context"));
        }
        if !valid_vector(&self.vector) || self.head < 0 || self.tail < self.head {
            return Err(invalid());
        }
        Ok(())
    }
}
pub(crate) struct CopyHeader {
    pub(crate) stage: Stage,
    pub(crate) primary: i64,
}
impl CopyHeader {
    pub(crate) fn encode(&self) -> Result<OperationData> {
        if !matches!(self.stage, Stage::None | Stage::State | Stage::Log) {
            return Err(unsupported("unsupported initial copy stage"));
        }
        let mut b = 1u32.to_le_bytes().to_vec();
        b.extend((self.stage as i32).to_le_bytes());
        b.extend(self.primary.to_le_bytes());
        OperationData::new(vec![b])
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        let mut r = Reader::new(single(data)?);
        if r.u32()? != 1 {
            return Err(unsupported("copy header version"));
        }
        let stage = Stage::read(r.i32()?)?;
        let primary = r.i64()?;
        r.finish()?;
        if !matches!(stage, Stage::None | Stage::State | Stage::Log) {
            return Err(unsupported("copy initial stage"));
        }
        Ok(Self { stage, primary })
    }
    pub(crate) fn decode_operation(data: &OperationData) -> Result<Self> {
        if data.segments().len() != 1 || data.bytes() != 16 {
            return Err(Error::InvalidInput("native copy header length"));
        }
        Self::decode(data).map_err(|_| Status::NotImplemented.error())
    }
}

/// Segmented native payloads, not FabricSerializable envelopes or ACK owners.
/// The state-stage router borrows segments until it can return the exact transition.
pub(crate) enum CopyMessage<P = OperationData> {
    None(P),
    State(P),
    Progress(P),
    FalseProgress(P),
    Log(P),
}
pub(crate) struct CopyPayload<'a>(&'a [Arc<[u8]>]);
impl CopyPayload<'_> {
    pub(crate) fn into_owned(self) -> Result<OperationData> {
        OperationData::new(self.0.iter().map(|segment| segment.to_vec()).collect())
    }
}
impl<P> CopyMessage<P> {
    fn new(stage: Stage, payload: P) -> Self {
        match stage {
            Stage::None => Self::None(payload),
            Stage::State => Self::State(payload),
            Stage::Progress => Self::Progress(payload),
            Stage::FalseProgress => Self::FalseProgress(payload),
            Stage::Log => Self::Log(payload),
        }
    }
    fn stage(&self) -> Stage {
        match self {
            Self::None(_) => Stage::None,
            Self::State(_) => Stage::State,
            Self::Progress(_) => Stage::Progress,
            Self::FalseProgress(_) => Stage::FalseProgress,
            Self::Log(_) => Stage::Log,
        }
    }
    fn into_payload(self) -> P {
        match self {
            Self::None(payload)
            | Self::State(payload)
            | Self::Progress(payload)
            | Self::FalseProgress(payload)
            | Self::Log(payload) => payload,
        }
    }
}
impl CopyMessage<Vec<Vec<u8>>> {
    pub(crate) fn encode(self) -> Result<OperationData> {
        let stage = self.stage();
        let mut segments = self.into_payload();
        segments.push((stage as i32).to_le_bytes().to_vec());
        OperationData::new(segments)
    }
}
impl CopyMessage {
    pub(crate) fn encode(self) -> Result<OperationData> {
        CopyMessage::new(self.stage(), self.into_payload().to_segments()).encode()
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        CopyMessage::<CopyPayload<'_>>::decode_borrowed(data)?.into_owned()
    }
    pub(crate) fn decode_log(data: &OperationData) -> Result<OperationData> {
        match CopyMessage::<CopyPayload<'_>>::decode_borrowed(data)
            .map_err(|_| Status::NotImplemented.error())?
        {
            CopyMessage::Log(payload) => payload.into_owned(),
            _ => Err(Status::NotImplemented.error()),
        }
    }
}
impl<'a> CopyMessage<CopyPayload<'a>> {
    fn decode_borrowed(data: &'a OperationData) -> Result<Self> {
        let (last, payload) = data.segments().split_last().ok_or_else(invalid)?;
        let bytes = last.as_ref().try_into().map_err(|_| invalid())?;
        Ok(Self::new(
            Stage::read(i32::from_le_bytes(bytes))?,
            CopyPayload(payload),
        ))
    }
    pub(crate) fn decode_state(data: &'a OperationData) -> Result<Self> {
        let (last, payload) = data
            .segments()
            .split_last()
            .ok_or(Error::InvalidInput("missing native copy stage"))?;
        let bytes = last
            .get(..4)
            .ok_or(Error::InvalidInput("truncated native copy stage"))?;
        let stage = Stage::read(i32::from_le_bytes(bytes.try_into().unwrap()))
            .map_err(|_| Status::NotImplemented.error())?;
        Ok(Self::new(stage, CopyPayload(payload)))
    }
    pub(crate) fn into_owned(self) -> Result<CopyMessage> {
        Ok(CopyMessage::new(
            self.stage(),
            self.into_payload().into_owned()?,
        ))
    }
}

#[derive(Clone)]
pub(crate) struct CopyMetadata {
    pub(crate) vector: Vec<ProgressEntry>,
    pub(crate) starting_epoch: Epoch,
    pub(crate) starting: i64,
    pub(crate) checkpoint: i64,
    pub(crate) upto: i64,
    pub(crate) highest_provider: i64,
}
impl CopyMetadata {
    pub(crate) fn encode(&self) -> Result<OperationData> {
        self.validate()?;
        let mut b = 1u32.to_le_bytes().to_vec();
        b.extend(vector_bytes(&self.vector));
        put_epoch(&mut b, self.starting_epoch);
        for value in [
            self.starting,
            self.checkpoint,
            self.upto,
            self.highest_provider,
        ] {
            b.extend(value.to_le_bytes());
        }
        CopyMessage::Progress(vec![b]).encode()
    }
    pub(crate) fn decode_payload(data: &OperationData) -> Result<Self> {
        let mut r = Reader::new(single(data)?);
        if r.u32()? != 1 {
            return Err(unsupported("copy metadata version"));
        }
        let result = Self {
            vector: read_vector(&mut r)?,
            starting_epoch: r.epoch()?,
            starting: r.i64()?,
            checkpoint: r.i64()?,
            upto: r.i64()?,
            highest_provider: r.i64()?,
        };
        r.finish()?;
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<()> {
        if !valid_vector(&self.vector)
            || self.starting < 0
            || self.starting > self.checkpoint
            || self.checkpoint > self.upto
            || self.highest_provider < self.checkpoint
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FullReason {
    Trimmed,
    Validation,
    DataLoss,
    InsufficientLogs,
    Other,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CopyMode {
    None,
    Full(FullReason),
    Partial { source: i64, target: i64 },
    FalseProgress { source: i64, target: i64 },
}
fn le(a: &ProgressEntry, b: &ProgressEntry) -> bool {
    epoch_key(a.epoch) < epoch_key(b.epoch) || (a.epoch == b.epoch && a.lsn <= b.lsn)
}
pub(crate) fn find_copy_mode(source: &CopyContext, target: &CopyContext) -> Result<CopyMode> {
    source.validate()?;
    target.validate()?;
    let s = &source.vector;
    let t = &target.vector;
    if t.last().unwrap().epoch.data_loss_number > s.last().unwrap().epoch.data_loss_number {
        return Err(invalid());
    }
    let (mut i, mut j) = (s.len() - 1, t.len() - 1);
    loop {
        while !le(&t[j], &s[i]) {
            if j == 0 {
                return Ok(CopyMode::Full(FullReason::Trimmed));
            }
            j -= 1;
        }
        while !le(&s[i], &t[j]) {
            if i == 0 {
                return Ok(CopyMode::Full(FullReason::Trimmed));
            }
            i -= 1;
        }
        if equal(&s[i], &t[j]) {
            break;
        }
    }
    // Native shared-prefix validation groups repeated-LSN epoch attempts.
    if t.last().unwrap().epoch.data_loss_number == s.last().unwrap().epoch.data_loss_number
        && j + 1 < t.len()
        && t[j].epoch.data_loss_number == t.last().unwrap().epoch.data_loss_number
    {
        let mut changes = 0;
        let mut lsn = t[j + 1].lsn;
        for entry in &t[j + 2..] {
            if entry.epoch.data_loss_number != t[j].epoch.data_loss_number {
                break;
            }
            if entry.lsn != lsn {
                changes += 1;
                lsn = entry.lsn;
            }
        }
        if changes > 1 {
            return Ok(CopyMode::Full(FullReason::Validation));
        }
    }
    // Native helper returns the preceding different-LSN entry WITHOUT
    // decrementing its index; the next iteration consumes that LSN group.
    fn preceding(index: &mut usize, vector: &[ProgressEntry], lsn: i64) -> Option<ProgressEntry> {
        while *index > 0 {
            let previous = vector[*index - 1].clone();
            if previous.lsn != lsn {
                return Some(previous);
            }
            *index -= 1;
        }
        None
    }
    let (mut a, mut b) = (i, j);
    let (mut se, mut te) = (s[i].clone(), t[j].clone());
    while let Some(previous) = preceding(&mut a, s, se.lsn) {
        se = previous;
        let Some(previous) = preceding(&mut b, t, te.lsn) else {
            break;
        };
        te = previous;
        if !equal(&se, &te) {
            return Ok(CopyMode::Full(FullReason::Validation));
        }
    }
    if equal(s.last().unwrap(), t.last().unwrap()) && source.tail == target.tail {
        return Ok(CopyMode::None);
    }
    let brand_new =
        t.len() == 1 && t[0].epoch.data_loss_number == 0 && t[0].lsn == 0 && target.tail == 1;
    if !brand_new
        && (s[i].epoch.data_loss_number != s.last().unwrap().epoch.data_loss_number
            || t[j].epoch.data_loss_number != t.last().unwrap().epoch.data_loss_number
            || source.head_epoch.data_loss_number > t[j].epoch.data_loss_number
            || target.head_epoch.data_loss_number > s[i].epoch.data_loss_number)
    {
        return Ok(CopyMode::Full(FullReason::DataLoss));
    }
    let source_start = if i + 1 == s.len() {
        source.tail
    } else {
        s[i + 1].lsn
    };
    let target_start = if j + 1 == t.len() {
        target.tail
    } else {
        t[j + 1].lsn
    };
    if source.head > target.tail || source_start < source.head || target.head > source_start {
        return Ok(CopyMode::Full(FullReason::InsufficientLogs));
    }
    if source_start < target_start {
        return Ok(CopyMode::FalseProgress {
            source: source_start,
            target: target_start,
        });
    }
    if target_start != target.tail {
        return Ok(CopyMode::FalseProgress {
            source: target_start,
            target: target_start,
        });
    }
    if epoch_key(s[i].epoch) < epoch_key(t.last().unwrap().epoch) {
        return Ok(CopyMode::Full(FullReason::Other));
    }
    Ok(CopyMode::Partial {
        source: source_start,
        target: target_start,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_copy_messages_keep_segment_boundaries_native_bytes_and_borrowed_views() {
        let payload = OperationData::new(vec![vec![1, 2, 3], Vec::new(), vec![4, 5]]).unwrap();
        for stage in [
            Stage::None,
            Stage::State,
            Stage::Progress,
            Stage::FalseProgress,
            Stage::Log,
        ] {
            let encoded = CopyMessage::new(stage, payload.clone()).encode().unwrap();
            let mut expected = payload.to_segments();
            expected.push((stage as i32).to_le_bytes().to_vec());
            assert_eq!(encoded.to_segments(), expected);
            let decoded = CopyMessage::decode(&encoded).unwrap();
            assert_eq!(decoded.stage(), stage);
            assert_eq!(decoded.into_payload(), payload);

            let borrowed = CopyMessage::decode_state(&encoded).unwrap();
            assert_eq!(borrowed.stage(), stage);
            let view = borrowed.into_payload();
            assert!(std::ptr::eq(view.0.as_ptr(), encoded.segments().as_ptr()));
            assert_eq!(view.into_owned().unwrap(), payload);
        }
        assert_eq!(
            CopyMessage::decode_log(&CopyMessage::Log(payload.clone()).encode().unwrap()).unwrap(),
            payload
        );
    }

    #[test]
    fn typed_copy_header_and_metadata_keep_their_distinct_native_framing() {
        for stage in [Stage::None, Stage::State, Stage::Log] {
            let header = CopyHeader {
                stage,
                primary: -17,
            };
            let data = header.encode().unwrap();
            let mut expected = 1u32.to_le_bytes().to_vec();
            expected.extend((stage as i32).to_le_bytes());
            expected.extend((-17i64).to_le_bytes());
            assert_eq!(data.to_segments(), [expected]);
            let decoded = CopyHeader::decode_operation(&data).unwrap();
            assert_eq!((decoded.stage, decoded.primary), (stage, -17));
        }
        let epoch = Epoch {
            data_loss_number: 1,
            configuration_number: 2,
        };
        let metadata = CopyMetadata {
            vector: vec![ProgressEntry {
                epoch,
                lsn: 1,
                primary: -17,
                ticks: 42,
            }],
            starting_epoch: epoch,
            starting: 1,
            checkpoint: 2,
            upto: 3,
            highest_provider: 4,
        };
        let data = metadata.encode().unwrap();
        let CopyMessage::Progress(payload) = CopyMessage::decode(&data).unwrap() else {
            panic!("expected copy metadata");
        };
        let decoded = CopyMetadata::decode_payload(&payload).unwrap();
        assert_eq!(decoded.encode().unwrap(), data);
        assert!(CopyMetadata::decode_payload(&data).is_err());
    }

    #[test]
    fn typed_copy_messages_count_stage_against_limits_and_keep_profile_errors() {
        let limit = fube_data::Settings::default().operation_segments;
        let payload = OperationData::new(vec![vec![]; limit]).unwrap();
        assert!(matches!(
            CopyMessage::State(payload).encode(),
            Err(error) if error.kind() == fube_runtime::ErrorKind::ReplicationOperationTooLarge
        ));
        let data = CopyMessage::Log(vec![vec![]; limit - 1]).encode().unwrap();
        assert_eq!(
            CopyMessage::decode_log(&data).unwrap().segments().len(),
            limit - 1
        );
        for code in [2i32, 3, 5, 99] {
            let data = OperationData::new(vec![code.to_le_bytes().to_vec()]).unwrap();
            if code == 99 {
                assert!(matches!(
                    CopyMessage::decode(&data),
                    Err(error) if error.kind() == fube_runtime::ErrorKind::Unsupported && error.native_code().is_none()
                ));
            }
            assert_eq!(
                CopyMessage::decode_log(&data).unwrap_err().native_code(),
                Some(Status::NotImplemented.code())
            );
        }
        let data = OperationData::new(vec![vec![3, 0, 0, 0, 9]]).unwrap();
        assert!(matches!(
            CopyMessage::decode(&data),
            Err(Error::InvalidInput(_))
        ));
        assert!(matches!(
            CopyMessage::decode_state(&data),
            Ok(CopyMessage::State(_))
        ));
    }
}
