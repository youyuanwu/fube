// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native little-endian logical records, without the physical record envelope.
//!
//! Layout authority: SF 51a7703 `logrecordlib/LogRecord.cpp:486–595,845–873`,
//! the five concrete logical record writers, and
//! `utilities/OperationData.cpp:121–294`. Each inherited section and each
//! payload count/length occupies its own segment. Section sizes permit unknown
//! trailing fields, not a universal schema-version field.
//!
//! A0 rejects impossible sizes, noncanonical bools, descriptor/content mismatch,
//! unexplained trailing bytes/segments and quota violations. These checked-input
//! exclusions are not native assertion/status guarantees. Fixtures are derived
//! from pinned source, not produced by a native binary or interoperability test.
//! Atomic identities and redo-only layout are recognized, not execution support.

#![allow(dead_code)]

use crate::{Epoch, Error, OperationData, Result};
use fube_data::Settings;
use fube_runtime::ErrorKind;
use std::sync::Arc;

/// Supported native logical fields; transaction execution has a narrower gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecordKind {
    /// Native type 1; a single begin is itself the commit record.
    Begin {
        /// Signed native identity, including recognition-only atomic identities.
        transaction_id: i64,
        /// Native single-operation flag.
        single: bool,
        /// Nullable provider metadata, serialized before redo and undo.
        metadata: Option<OperationData>,
        /// Nullable segmented redo.
        redo: Option<OperationData>,
        /// Nullable segmented undo.
        undo: Option<OperationData>,
    },
    /// Native type 2; redo-only omits the entire undo frame.
    Operation {
        /// Signed native identity.
        transaction_id: i64,
        /// Native atomic-redo discriminator; requires a negative identity.
        redo_only: bool,
        /// Nullable provider metadata.
        metadata: Option<OperationData>,
        /// Nullable segmented redo.
        redo: Option<OperationData>,
        /// Must be absent for redo-only records, rather than silently discarded.
        undo: Option<OperationData>,
    },
    /// Native type 3.
    End {
        /// Signed native identity.
        transaction_id: i64,
        /// Native committed-versus-aborted flag.
        committed: bool,
    },
    /// Native type 4.
    Barrier {
        /// Advertised stable frontier, not necessarily this record's LSN.
        last_stable_lsn: i64,
    },
    /// Native type 5; the timestamp is native DateTime ticks, not Unix time.
    UpdateEpoch {
        /// Data-loss version precedes configuration version on the wire.
        epoch: Epoch,
        /// Native signed primary replica identity.
        primary_replica_id: i64,
        /// Raw native DateTime ticks.
        timestamp_ticks: i64,
    },
}

/// Logical record identity and fields, independent of physical log framing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Record {
    /// Embedded LSN; lower replication may subsequently assign a different LSN.
    pub(crate) lsn: i64,
    /// Supported native logical kind.
    pub(crate) kind: RecordKind,
}

impl Record {
    /// Serialize native section buffers and nullable metadata/redo/undo framing.
    pub(crate) fn encode(&self) -> Result<OperationData> {
        self.validate()?;
        let native_type: i32 = match &self.kind {
            RecordKind::Begin { .. } => 1,
            RecordKind::Operation { .. } => 2,
            RecordKind::End { .. } => 3,
            RecordKind::Barrier { .. } => 4,
            RecordKind::UpdateEpoch { .. } => 5,
        };
        let mut wire = Wire::default();
        wire.section(&[&native_type.to_le_bytes(), &self.lsn.to_le_bytes()])?;
        wire.section(&[])?;
        if let Some(id) = self.transaction_id() {
            wire.section(&[&id.to_le_bytes()])?;
        }
        match &self.kind {
            RecordKind::Begin {
                single,
                metadata,
                redo,
                undo,
                ..
            } => {
                wire.section(&[&[u8::from(*single)]])?;
                wire.payload(metadata.as_ref())?;
                wire.payload(redo.as_ref())?;
                wire.payload(undo.as_ref())?;
            }
            RecordKind::Operation {
                redo_only,
                metadata,
                redo,
                undo,
                ..
            } => {
                wire.section(&[&[u8::from(*redo_only)]])?;
                wire.payload(metadata.as_ref())?;
                wire.payload(redo.as_ref())?;
                if !*redo_only {
                    wire.payload(undo.as_ref())?;
                }
            }
            RecordKind::End { committed, .. } => {
                wire.section(&[&[u8::from(*committed)]])?;
            }
            RecordKind::Barrier { last_stable_lsn } => {
                wire.section(&[&last_stable_lsn.to_le_bytes()])?;
            }
            RecordKind::UpdateEpoch {
                epoch,
                primary_replica_id,
                timestamp_ticks,
            } => {
                wire.section(&[
                    &epoch.data_loss_number.to_le_bytes(),
                    &epoch.configuration_number.to_le_bytes(),
                    &primary_replica_id.to_le_bytes(),
                    &timestamp_ticks.to_le_bytes(),
                ])?;
            }
        }
        OperationData::new(wire.segments)
    }

    /// Decode one complete segmented logical record, skipping section extensions.
    pub(crate) fn decode(data: &OperationData) -> Result<Self> {
        check_size(data.segments().len(), data.bytes())?;
        let mut reader = Segments {
            segments: data.segments(),
            next: 0,
        };
        let common = reader.section(16)?;
        let native_type = read_u32(&common[..4])? as i32;
        let lsn = read_i64(&common[4..12])?;
        match native_type {
            1..=5 => {}
            0 => return Err(Error::InvalidInput("invalid native record sentinel")),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "unsupported native logical record kind",
                ));
            }
        }
        reader.section(4)?;
        let transaction_id = if native_type <= 3 {
            read_i64(&reader.section(12)?[..8])?
        } else {
            0
        };
        if native_type <= 3 {
            // Native TransactionLogRecord::ReadLogical creates the sign-derived
            // transaction here, advancing the shared generator during decoding.
            super::identity::replay(transaction_id)?;
        }
        let kind = match native_type {
            1 => RecordKind::Begin {
                transaction_id,
                single: reader.flag()?,
                metadata: reader.payload()?,
                redo: reader.payload()?,
                undo: reader.payload()?,
            },
            2 => {
                let redo_only = reader.flag()?;
                RecordKind::Operation {
                    transaction_id,
                    redo_only,
                    metadata: reader.payload()?,
                    redo: reader.payload()?,
                    undo: if redo_only { None } else { reader.payload()? },
                }
            }
            3 => RecordKind::End {
                transaction_id,
                committed: reader.flag()?,
            },
            4 => RecordKind::Barrier {
                last_stable_lsn: read_i64(&reader.section(12)?[..8])?,
            },
            5 => {
                let fields = reader.section(36)?;
                RecordKind::UpdateEpoch {
                    epoch: Epoch {
                        data_loss_number: read_i64(&fields[..8])?,
                        configuration_number: read_i64(&fields[8..16])?,
                    },
                    primary_replica_id: read_i64(&fields[16..24])?,
                    timestamp_ticks: read_i64(&fields[24..32])?,
                }
            }
            _ => unreachable!(),
        };
        if reader.next != reader.segments.len() {
            return Err(Error::InvalidInput("trailing native record segments"));
        }
        let record = Self { lsn, kind };
        record.validate()?;
        Ok(record)
    }

    /// Return native transaction identity only for begin, operation and end.
    pub(crate) fn transaction_id(&self) -> Option<i64> {
        match &self.kind {
            RecordKind::Begin { transaction_id, .. }
            | RecordKind::Operation { transaction_id, .. }
            | RecordKind::End { transaction_id, .. } => Some(*transaction_id),
            _ => None,
        }
    }

    /// Native `LogRecord::IsCommitLogRecord`, including atomic recognition.
    pub(crate) fn is_commit(&self) -> bool {
        match &self.kind {
            RecordKind::Begin { single, .. } => *single,
            RecordKind::End { committed, .. } => *committed,
            RecordKind::Operation {
                transaction_id,
                redo_only,
                ..
            } => *transaction_id < 0 || *redo_only,
            _ => false,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.transaction_id() == Some(0) {
            return Err(Error::InvalidInput("zero native transaction identity"));
        }
        if let RecordKind::Operation {
            transaction_id,
            redo_only: true,
            undo,
            ..
        } = &self.kind
        {
            if *transaction_id >= 0 || undo.is_some() {
                return Err(Error::InvalidInput(
                    "native redo-only requires negative identity and omitted undo",
                ));
            }
        }
        Ok(())
    }
}

/// Apply the existing OperationData budgets, including native framing segments.
pub(super) fn check_size(segments: usize, bytes: usize) -> Result<()> {
    let limits = Settings::default();
    if segments > limits.operation_segments || bytes > limits.operation_bytes {
        return Err(too_large());
    }
    Ok(())
}

fn too_large() -> Error {
    Error::new(
        ErrorKind::ReplicationOperationTooLarge,
        "native operation framing exceeds OperationData limits",
    )
}

/// Read an exact native little-endian descriptor.
pub(super) fn read_u32(bytes: &[u8]) -> Result<u32> {
    let value = bytes
        .try_into()
        .map_err(|_| Error::InvalidInput("native u32 field size"))?;
    Ok(u32::from_le_bytes(value))
}

/// Read an exact native little-endian signed field.
pub(super) fn read_i64(bytes: &[u8]) -> Result<i64> {
    let value = bytes
        .try_into()
        .map_err(|_| Error::InvalidInput("native i64 field size"))?;
    Ok(i64::from_le_bytes(value))
}

#[derive(Default)]
struct Wire {
    segments: Vec<Vec<u8>>,
    bytes: usize,
}

impl Wire {
    fn reserve(&mut self, bytes: usize) -> Result<()> {
        let total = self.bytes.checked_add(bytes).ok_or_else(too_large)?;
        let count = self.segments.len().checked_add(1).ok_or_else(too_large)?;
        check_size(count, total)?;
        self.bytes = total;
        Ok(())
    }

    fn push(&mut self, bytes: &[u8]) -> Result<()> {
        self.reserve(bytes.len())?;
        self.segments.push(bytes.to_vec());
        Ok(())
    }

    fn section(&mut self, fields: &[&[u8]]) -> Result<()> {
        let size = fields.iter().try_fold(4usize, |size, field| {
            size.checked_add(field.len()).ok_or_else(too_large)
        })?;
        let size_u32 = u32::try_from(size).map_err(|_| too_large())?;
        self.reserve(size)?;
        let mut section = Vec::with_capacity(size);
        section.extend_from_slice(&size_u32.to_le_bytes());
        for field in fields {
            section.extend_from_slice(field);
        }
        self.segments.push(section);
        Ok(())
    }

    fn payload(&mut self, data: Option<&OperationData>) -> Result<()> {
        let Some(data) = data else {
            return self.push(&u32::MAX.to_le_bytes());
        };
        let count = u32::try_from(data.segments().len()).map_err(|_| too_large())?;
        self.push(&count.to_le_bytes())?;
        for segment in data.segments() {
            let size = u32::try_from(segment.len()).map_err(|_| too_large())?;
            self.push(&size.to_le_bytes())?;
            if !segment.is_empty() {
                self.push(segment)?;
            }
        }
        Ok(())
    }
}

struct Segments<'a> {
    segments: &'a [Arc<[u8]>],
    next: usize,
}

impl<'a> Segments<'a> {
    fn take(&mut self) -> Result<&'a [u8]> {
        let segment = self
            .segments
            .get(self.next)
            .ok_or(Error::InvalidInput("truncated native record"))?;
        self.next += 1;
        Ok(segment)
    }

    fn section(&mut self, minimum: usize) -> Result<&'a [u8]> {
        let segment = self.take()?;
        let prefix = segment
            .get(..4)
            .ok_or(Error::InvalidInput("truncated native section size"))?;
        let size = usize::try_from(read_u32(prefix)?)
            .map_err(|_| Error::InvalidInput("native section size overflow"))?;
        if size < minimum || size != segment.len() {
            return Err(Error::InvalidInput("native section size mismatch"));
        }
        Ok(&segment[4..size])
    }

    fn flag(&mut self) -> Result<bool> {
        match self.section(5)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::InvalidInput("noncanonical native bool")),
        }
    }

    fn payload(&mut self) -> Result<Option<OperationData>> {
        let count = read_u32(self.take()?)?;
        if count == u32::MAX {
            return Ok(None);
        }
        let count = usize::try_from(count).map_err(|_| too_large())?;
        check_size(count, 0)?;
        if count > self.segments.len() - self.next {
            return Err(Error::InvalidInput("truncated native payload descriptors"));
        }
        let mut segments = Vec::with_capacity(count);
        let mut bytes = 0usize;
        for _ in 0..count {
            let size = usize::try_from(read_u32(self.take()?)?).map_err(|_| too_large())?;
            bytes = bytes.checked_add(size).ok_or_else(too_large)?;
            check_size(count, bytes)?;
            if size == 0 {
                segments.push(Vec::new());
            } else {
                let content = self.take()?;
                if content.len() != size {
                    return Err(Error::InvalidInput("native payload length mismatch"));
                }
                segments.push(content.to_vec());
            }
        }
        Ok(Some(OperationData::new(segments)?))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn decode_advances_the_shared_native_identity_generator() {
        let before = super::super::identity::generate().unwrap();
        let future_id = before + 100_000_000;
        let record = super::Record {
            lsn: 2,
            kind: super::RecordKind::End {
                transaction_id: -future_id,
                committed: false,
            },
        };
        super::Record::decode(&record.encode().unwrap()).unwrap();
        assert!(super::super::identity::generate().unwrap() > future_id);
    }
    use super::*;

    fn fixture(segments: &[&str]) -> OperationData {
        OperationData::new(
            segments
                .iter()
                .map(|segment| {
                    segment
                        .split_ascii_whitespace()
                        .map(|byte| u8::from_str_radix(byte, 16).unwrap())
                        .collect()
                })
                .collect(),
        )
        .unwrap()
    }

    fn begin_fixture() -> OperationData {
        fixture(&[
            "10 00 00 00 01 00 00 00 ff ff ff ff ff ff ff ff",
            "04 00 00 00",
            "0c 00 00 00 2a 00 00 00 00 00 00 00",
            "05 00 00 00 01",
            "ff ff ff ff",
            "02 00 00 00",
            "00 00 00 00",
            "02 00 00 00",
            "ab cd",
            "00 00 00 00",
        ])
    }

    fn begin_record() -> Record {
        Record {
            lsn: -1,
            kind: RecordKind::Begin {
                transaction_id: 42,
                single: true,
                metadata: None,
                redo: Some(OperationData::new(vec![vec![], vec![0xab, 0xcd]]).unwrap()),
                undo: Some(OperationData::new(vec![]).unwrap()),
            },
        }
    }

    #[test]
    fn source_derived_begin_golden_59_bytes() {
        let bytes = begin_fixture();
        assert_eq!(bytes.bytes(), 59);
        assert_eq!(
            bytes.segments().iter().map(|s| s.len()).collect::<Vec<_>>(),
            [16, 4, 12, 5, 4, 4, 4, 4, 2, 4]
        );
        let record = begin_record();
        assert_eq!(record.encode().unwrap(), bytes);
        assert_eq!(Record::decode(&bytes).unwrap(), record);
        assert!(record.is_commit());
    }

    #[test]
    fn source_derived_operation_metadata_redo_undo_order() {
        let bytes = fixture(&[
            "10 00 00 00 02 00 00 00 08 07 06 05 04 03 02 01",
            "04 00 00 00",
            "0c 00 00 00 7b 00 00 00 00 00 00 00",
            "05 00 00 00 00",
            "01 00 00 00",
            "01 00 00 00",
            "11",
            "01 00 00 00",
            "02 00 00 00",
            "22 23",
            "01 00 00 00",
            "01 00 00 00",
            "33",
        ]);
        let record = Record {
            lsn: 0x0102_0304_0506_0708,
            kind: RecordKind::Operation {
                transaction_id: 123,
                redo_only: false,
                metadata: Some(fixture(&["11"])),
                redo: Some(fixture(&["22 23"])),
                undo: Some(fixture(&["33"])),
            },
        };
        assert_eq!(bytes.bytes(), 65);
        assert_eq!(record.encode().unwrap(), bytes);
        assert_eq!(Record::decode(&bytes).unwrap(), record);
        assert!(!record.is_commit());
    }

    #[test]
    fn source_derived_end_commit_and_abort_fixtures() {
        for committed in [false, true] {
            let mut segments = fixture(&[
                "10 00 00 00 03 00 00 00 63 00 00 00 00 00 00 00",
                "04 00 00 00",
                "0c 00 00 00 d6 ff ff ff ff ff ff ff",
                "05 00 00 00 00",
            ])
            .to_segments();
            segments[3][4] = u8::from(committed);
            let bytes = OperationData::new(segments).unwrap();
            let record = Record {
                lsn: 99,
                kind: RecordKind::End {
                    transaction_id: -42,
                    committed,
                },
            };
            assert_eq!(bytes.bytes(), 37);
            assert_eq!(record.encode().unwrap(), bytes);
            assert_eq!(Record::decode(&bytes).unwrap(), record);
            assert_eq!(record.is_commit(), committed);
        }
    }

    #[test]
    fn source_derived_barrier_and_epoch_fixtures() {
        let barrier = fixture(&[
            "10 00 00 00 04 00 00 00 01 01 00 00 00 00 00 00",
            "04 00 00 00",
            "0c 00 00 00 00 01 00 00 00 00 00 00",
        ]);
        let epoch = fixture(&[
            "10 00 00 00 05 00 00 00 63 00 00 00 00 00 00 00",
            "04 00 00 00",
            "24 00 00 00 01 00 00 00 00 00 00 00 02 01 00 00 00 00 00 00 fe ff ff ff ff ff ff ff 08 07 06 05 04 03 02 01",
        ]);
        let cases = [
            (
                barrier,
                Record {
                    lsn: 257,
                    kind: RecordKind::Barrier {
                        last_stable_lsn: 256,
                    },
                },
                32,
            ),
            (
                epoch,
                Record {
                    lsn: 99,
                    kind: RecordKind::UpdateEpoch {
                        epoch: Epoch {
                            data_loss_number: 1,
                            configuration_number: 258,
                        },
                        primary_replica_id: -2,
                        timestamp_ticks: 0x0102_0304_0506_0708,
                    },
                },
                56,
            ),
        ];
        for (bytes, record, size) in cases {
            assert_eq!(bytes.bytes(), size);
            assert_eq!(record.encode().unwrap(), bytes);
            assert_eq!(Record::decode(&bytes).unwrap(), record);
            assert_eq!(record.transaction_id(), None);
            assert!(!record.is_commit());
        }
    }

    #[test]
    fn native_redo_only_omits_undo_and_recognizes_atomic_commit() {
        let bytes = fixture(&[
            "10 00 00 00 02 00 00 00 4d 00 00 00 00 00 00 00",
            "04 00 00 00",
            "0c 00 00 00 d6 ff ff ff ff ff ff ff",
            "05 00 00 00 01",
            "ff ff ff ff",
            "00 00 00 00",
        ]);
        let record = Record {
            lsn: 77,
            kind: RecordKind::Operation {
                transaction_id: -42,
                redo_only: true,
                metadata: None,
                redo: Some(fixture(&[])),
                undo: None,
            },
        };
        assert_eq!(record.encode().unwrap(), bytes);
        assert_eq!(Record::decode(&bytes).unwrap(), record);
        assert!(record.is_commit());
        let mut extra_undo = bytes.to_segments();
        extra_undo.push(u32::MAX.to_le_bytes().to_vec());
        assert!(Record::decode(&OperationData::new(extra_undo).unwrap()).is_err());

        let mut positive = bytes.to_segments();
        positive[2][4..12].copy_from_slice(&42i64.to_le_bytes());
        assert!(Record::decode(&OperationData::new(positive).unwrap()).is_err());
        for (id, redo_only, undo, valid, commit) in [
            (-42, true, None, true, true),
            (42, true, None, false, true),
            (-42, true, Some(fixture(&[])), false, true),
            (-42, false, None, true, true),
            (42, false, None, true, false),
        ] {
            let record = Record {
                lsn: -1,
                kind: RecordKind::Operation {
                    transaction_id: id,
                    redo_only,
                    metadata: None,
                    redo: None,
                    undo,
                },
            };
            assert_eq!(record.encode().is_ok(), valid);
            assert_eq!(record.is_commit(), commit);
        }
    }

    #[test]
    fn native_null_empty_data_and_empty_segment_are_distinct() {
        let bytes = fixture(&[
            "10 00 00 00 01 00 00 00 ff ff ff ff ff ff ff ff",
            "04 00 00 00",
            "0c 00 00 00 2a 00 00 00 00 00 00 00",
            "05 00 00 00 00",
            "ff ff ff ff",
            "00 00 00 00",
            "01 00 00 00",
            "00 00 00 00",
        ]);
        let expected = Record {
            lsn: -1,
            kind: RecordKind::Begin {
                transaction_id: 42,
                single: false,
                metadata: None,
                redo: Some(fixture(&[])),
                undo: Some(fixture(&[""])),
            },
        };
        assert_eq!(Record::decode(&bytes).unwrap(), expected);
        assert_eq!(expected.encode().unwrap(), bytes);
        assert!(!expected.is_commit());
        assert!(bytes.segments().iter().all(|s| !s.is_empty()));
    }

    #[test]
    fn native_section_extensions_are_skipped_independently() {
        let expected = begin_record();
        let mut segments = begin_fixture().to_segments();
        for segment in &mut segments[..4] {
            segment.extend_from_slice(&[0xa5, 0x5a]);
            let size = segment.len() as u32;
            segment[..4].copy_from_slice(&size.to_le_bytes());
        }
        assert_eq!(
            Record::decode(&OperationData::new(segments).unwrap()).unwrap(),
            expected
        );
        for kind in [
            RecordKind::Barrier {
                last_stable_lsn: -1,
            },
            RecordKind::UpdateEpoch {
                epoch: Epoch::INVALID,
                primary_replica_id: -1,
                timestamp_ticks: 7,
            },
        ] {
            let record = Record { lsn: -1, kind };
            let mut segments = record.encode().unwrap().to_segments();
            segments[2].extend_from_slice(&[0x91, 0x92, 0x93]);
            let size = segments[2].len() as u32;
            segments[2][..4].copy_from_slice(&size.to_le_bytes());
            assert_eq!(
                Record::decode(&OperationData::new(segments).unwrap()).unwrap(),
                record
            );
        }
    }

    #[test]
    fn malformed_sections_descriptors_flags_and_truncations_reject() {
        let original = begin_fixture().to_segments();
        for end in 0..original.len() {
            assert!(
                Record::decode(&OperationData::new(original[..end].to_vec()).unwrap()).is_err()
            );
        }
        for index in 0..4 {
            for size in [0, (original[index].len() - 1) as u32, u32::MAX] {
                let mut segments = original.clone();
                segments[index][..4].copy_from_slice(&size.to_le_bytes());
                assert!(Record::decode(&OperationData::new(segments).unwrap()).is_err());
            }
        }
        for (index, replacement) in [
            (0, vec![0, 0, 0]),
            (3, vec![5, 0, 0, 0, 2]),
            (4, vec![255, 255, 255, 255, 0]),
            (7, vec![1, 0, 0, 0]),
            (8, vec![]),
        ] {
            let mut segments = original.clone();
            segments[index] = replacement;
            assert!(Record::decode(&OperationData::new(segments).unwrap()).is_err());
        }
        let mut trailing = original.clone();
        trailing.push(vec![0, 0, 0, 0]);
        assert!(Record::decode(&OperationData::new(trailing).unwrap()).is_err());
        let mut outside_section = original.clone();
        outside_section[1].push(0);
        assert!(Record::decode(&OperationData::new(outside_section).unwrap()).is_err());
        let flattened = OperationData::new(vec![original.concat()]).unwrap();
        assert!(Record::decode(&flattened).is_err());
        let mut zero_id = original;
        zero_id[2][4..12].fill(0);
        assert!(Record::decode(&OperationData::new(zero_id).unwrap()).is_err());
        let zero_record = Record {
            lsn: -1,
            kind: RecordKind::End {
                transaction_id: 0,
                committed: true,
            },
        };
        assert!(zero_record.encode().is_err());
    }

    #[test]
    fn unsupported_logical_and_physical_kinds_reject_before_body_decode() {
        for kind in [6i32, 7, 8, 9, 10, 11, 12, 13, 14, -1] {
            let mut header = begin_fixture().to_segments().remove(0);
            header[4..8].copy_from_slice(&kind.to_le_bytes());
            let error = Record::decode(&OperationData::new(vec![header]).unwrap()).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
        }
        let bytes = fixture(&["10 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff"]);
        assert_eq!(
            Record::decode(&bytes).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn encoded_limits_include_all_framing_and_empty_segment_descriptors() {
        let limits = Settings::default();
        let make = |redo| Record {
            lsn: -1,
            kind: RecordKind::Begin {
                transaction_id: 42,
                single: true,
                metadata: None,
                redo: Some(redo),
                undo: None,
            },
        };
        for (size, valid) in [
            (limits.operation_bytes - 53, true),
            (limits.operation_bytes - 52, false),
        ] {
            let record = make(OperationData::new(vec![vec![0x71; size]]).unwrap());
            let encoded = record.encode();
            if valid {
                let encoded = encoded.unwrap();
                assert_eq!(encoded.bytes(), limits.operation_bytes);
                assert_eq!(Record::decode(&encoded).unwrap(), record);
            } else {
                assert_eq!(
                    encoded.unwrap_err().kind(),
                    ErrorKind::ReplicationOperationTooLarge
                );
            }
        }
        for (count, valid) in [
            (limits.operation_segments - 7, true),
            (limits.operation_segments - 6, false),
        ] {
            let record = make(OperationData::new(vec![vec![]; count]).unwrap());
            let encoded = record.encode();
            if valid {
                let encoded = encoded.unwrap();
                assert_eq!(encoded.segments().len(), limits.operation_segments);
                assert_eq!(Record::decode(&encoded).unwrap(), record);
            } else {
                assert_eq!(
                    encoded.unwrap_err().kind(),
                    ErrorKind::ReplicationOperationTooLarge
                );
            }
        }
    }

    #[test]
    fn hostile_payload_counts_and_lengths_reject_without_unbounded_allocation() {
        for (index, descriptor) in [
            (5, u32::MAX - 1),
            (5, Settings::default().operation_segments as u32 + 1),
            (7, u32::MAX),
        ] {
            let mut segments = begin_fixture().to_segments();
            segments[index] = descriptor.to_le_bytes().to_vec();
            let error = Record::decode(&OperationData::new(segments).unwrap()).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ReplicationOperationTooLarge);
        }
    }
}
