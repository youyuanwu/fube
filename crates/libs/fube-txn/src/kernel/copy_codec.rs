// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native CopyContext/Header/Metadata and ProgressVector selection, pin51a7703.
//! Source: logrecordlib/ProgressVector.cpp:130–257,304–481; CopyStream.cpp:167–351.
use super::{
    memory_log::{vector_bytes, ProgressEntry},
    native_bytes::{invalid, put_epoch, single, unsupported, Reader},
};
use crate::{Epoch, OperationData, Result};
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
        add_stage(OperationData::new(vec![b])?, Stage::Progress)
    }
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        let (stage, payload) = remove_stage(data)?;
        if stage != Stage::Progress {
            return Err(invalid());
        }
        let mut r = Reader::new(single(&payload)?);
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
pub(crate) fn add_stage(data: OperationData, stage: Stage) -> Result<OperationData> {
    let mut segments = data.to_segments();
    segments.push((stage as i32).to_le_bytes().to_vec());
    OperationData::new(segments)
}
pub(crate) fn remove_stage(data: &OperationData) -> Result<(Stage, OperationData)> {
    let segments = data.segments();
    let last = segments.last().ok_or_else(invalid)?;
    if last.len() != 4 {
        return Err(invalid());
    }
    let stage = Stage::read(i32::from_le_bytes(last[..].try_into().unwrap()))?;
    Ok((
        stage,
        OperationData::new(
            segments[..segments.len() - 1]
                .iter()
                .map(|s| s.to_vec())
                .collect(),
        )?,
    ))
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
