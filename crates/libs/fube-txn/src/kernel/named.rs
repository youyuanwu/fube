// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

//! Native state-provider routing, distinct from logical-record framing.
//!
//! SF 51a7703 `statemanager/NamedOperationData.cpp:97–374` appends a 16-byte
//! metadata header; `CopyNamedOperationData.cpp:110–357` prepends a 12-byte
//! copy header. These are exact-size format discriminators, not sized sections.
//! Native metadata counts skip additional metadata segments as the source
//! readers do. Writers emit the current one-header native format.
//!
//! Managed formats explicitly reject, regardless of their version field.
//! Impossible counts and quota failures are checked A0 input exclusions;
//! source-derived fixtures do not establish native binary interoperability.

#![allow(dead_code)]

use super::record::{check_size, read_i64, read_u32};
use crate::{Error, OperationData, Result};
use fube_runtime::ErrorKind;

/// Append native provider routing to nullable user metadata; redo/undo are separate.
pub(crate) fn encode(provider_id: i64, data: Option<&OperationData>) -> Result<OperationData> {
    let count = data.map_or(0, |data| data.segments().len());
    check_wrapped_size(data, 16)?;
    let user_count = if data.is_some() {
        u32::try_from(count).map_err(|_| Error::InvalidInput("native metadata count overflow"))?
    } else {
        u32::MAX
    };
    let mut header = Vec::with_capacity(16);
    header.extend_from_slice(&1u32.to_le_bytes());
    header.extend_from_slice(&user_count.to_le_bytes());
    header.extend_from_slice(&provider_id.to_le_bytes());
    let mut segments = data.map_or_else(Vec::new, OperationData::to_segments);
    segments.push(header);
    OperationData::new(segments)
}

/// Decode native header-last metadata, preserving null, empty and segmentation.
pub(crate) fn decode(data: &OperationData) -> Result<(i64, Option<OperationData>)> {
    check_size(data.segments().len(), data.bytes())?;
    let header = data
        .segments()
        .last()
        .ok_or(Error::InvalidInput("missing native named metadata header"))?;
    if header.len() == 13 {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "managed named metadata format",
        ));
    }
    if header.len() != 16 {
        return Err(Error::InvalidInput("native named metadata header size"));
    }
    let metadata_count = usize::try_from(read_u32(&header[..4])?)
        .map_err(|_| Error::InvalidInput("native metadata count overflow"))?;
    let user_count = read_u32(&header[4..8])?;
    let provider_id = read_i64(&header[8..16])?;
    if metadata_count == 0 || metadata_count > data.segments().len() {
        return Err(Error::InvalidInput("native named metadata count"));
    }
    if user_count == u32::MAX {
        if metadata_count != data.segments().len() {
            return Err(Error::InvalidInput("null native metadata segment count"));
        }
        return Ok((provider_id, None));
    }
    let user_count = usize::try_from(user_count)
        .map_err(|_| Error::InvalidInput("native user metadata count overflow"))?;
    if metadata_count.checked_add(user_count) != Some(data.segments().len()) {
        return Err(Error::InvalidInput("native named user segment count"));
    }
    let user = OperationData::new(
        data.segments()[..user_count]
            .iter()
            .map(|segment| segment.to_vec())
            .collect(),
    )?;
    Ok((provider_id, Some(user)))
}

/// Prepend native provider routing to nonnull copy data; empty data is not EOS.
pub(crate) fn encode_copy(provider_id: i64, data: &OperationData) -> Result<OperationData> {
    check_wrapped_size(Some(data), 12)?;
    let mut header = Vec::with_capacity(12);
    header.extend_from_slice(&1u32.to_le_bytes());
    header.extend_from_slice(&provider_id.to_le_bytes());
    let mut segments = Vec::with_capacity(data.segments().len() + 1);
    segments.push(header);
    segments.extend(data.to_segments());
    OperationData::new(segments)
}

/// Decode native header-first copy routing, always returning nonnull user data.
pub(crate) fn decode_copy(data: &OperationData) -> Result<(i64, OperationData)> {
    check_size(data.segments().len(), data.bytes())?;
    let header = data
        .segments()
        .first()
        .ok_or(Error::InvalidInput("missing native named copy header"))?;
    if header.len() == 4 {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "managed named copy format",
        ));
    }
    if header.len() != 12 {
        return Err(Error::InvalidInput("native named copy header size"));
    }
    let metadata_count = usize::try_from(read_u32(&header[..4])?)
        .map_err(|_| Error::InvalidInput("native copy metadata count overflow"))?;
    let provider_id = read_i64(&header[4..12])?;
    if metadata_count == 0 || metadata_count > data.segments().len() {
        return Err(Error::InvalidInput("native named copy metadata count"));
    }
    let user = OperationData::new(
        data.segments()[metadata_count..]
            .iter()
            .map(|segment| segment.to_vec())
            .collect(),
    )?;
    Ok((provider_id, user))
}

fn check_wrapped_size(data: Option<&OperationData>, header_bytes: usize) -> Result<()> {
    let count = data.map_or(0, |data| data.segments().len());
    let bytes = data.map_or(0, OperationData::bytes);
    let count = count
        .checked_add(1)
        .ok_or(Error::InvalidInput("native named segment count overflow"))?;
    let bytes = bytes
        .checked_add(header_bytes)
        .ok_or(Error::InvalidInput("native named byte count overflow"))?;
    check_size(count, bytes)
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn source_derived_metadata_header_last_null_empty_and_segments() {
        let cases = [
            (
                fixture(&["01 00 00 00 ff ff ff ff 07 00 00 00 00 00 00 00"]),
                None,
            ),
            (
                fixture(&["01 00 00 00 00 00 00 00 07 00 00 00 00 00 00 00"]),
                Some(fixture(&[])),
            ),
            (
                fixture(&[
                    "",
                    "ab cd",
                    "",
                    "01 00 00 00 03 00 00 00 07 00 00 00 00 00 00 00",
                ]),
                Some(fixture(&["", "ab cd", ""])),
            ),
        ];
        for (bytes, user) in cases {
            assert_eq!(encode(7, user.as_ref()).unwrap(), bytes);
            assert_eq!(decode(&bytes).unwrap(), (7, user));
        }
    }

    #[test]
    fn source_derived_copy_header_first_and_nonnull_header_only() {
        let cases = [
            (
                fixture(&["01 00 00 00 fe ff ff ff ff ff ff ff"]),
                fixture(&[]),
            ),
            (
                fixture(&["01 00 00 00 fe ff ff ff ff ff ff ff", "", "ab cd", ""]),
                fixture(&["", "ab cd", ""]),
            ),
        ];
        for (bytes, user) in cases {
            assert_eq!(encode_copy(-2, &user).unwrap(), bytes);
            assert_eq!(decode_copy(&bytes).unwrap(), (-2, user));
        }
    }

    #[test]
    fn native_metadata_count_skips_only_metadata_segments() {
        let metadata = fixture(&[
            "aa",
            "a5 5a",
            "02 00 00 00 01 00 00 00 07 00 00 00 00 00 00 00",
        ]);
        assert_eq!(decode(&metadata).unwrap(), (7, Some(fixture(&["aa"]))));
        let null_metadata = fixture(&["a5 5a", "02 00 00 00 ff ff ff ff 07 00 00 00 00 00 00 00"]);
        assert_eq!(decode(&null_metadata).unwrap(), (7, None));
        let copy = fixture(&["02 00 00 00 07 00 00 00 00 00 00 00", "a5 5a", "aa", ""]);
        assert_eq!(decode_copy(&copy).unwrap(), (7, fixture(&["aa", ""])));
    }

    #[test]
    fn managed_named_and_copy_versions_are_explicitly_unsupported() {
        for version in [0i32, 1, 2, -1] {
            let mut metadata = version.to_le_bytes().to_vec();
            metadata.extend_from_slice(&7i64.to_le_bytes());
            metadata.push(0);
            let bytes = OperationData::new(vec![metadata]).unwrap();
            assert_eq!(decode(&bytes).unwrap_err().kind(), ErrorKind::Unsupported);
            let bytes = OperationData::new(vec![
                version.to_le_bytes().to_vec(),
                7i64.to_le_bytes().to_vec(),
                vec![],
            ])
            .unwrap();
            assert_eq!(
                decode_copy(&bytes).unwrap_err().kind(),
                ErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn malformed_named_counts_header_sizes_and_wrong_positions_reject() {
        assert!(decode(&fixture(&[])).is_err());
        assert!(decode_copy(&fixture(&[])).is_err());
        for count in [0u32, 2, u32::MAX] {
            let mut metadata =
                fixture(&["01 00 00 00 00 00 00 00 07 00 00 00 00 00 00 00"]).to_segments();
            metadata[0][..4].copy_from_slice(&count.to_le_bytes());
            assert!(decode(&OperationData::new(metadata).unwrap()).is_err());
            let mut copy = fixture(&["01 00 00 00 07 00 00 00 00 00 00 00"]).to_segments();
            copy[0][..4].copy_from_slice(&count.to_le_bytes());
            assert!(decode_copy(&OperationData::new(copy).unwrap()).is_err());
        }
        for count in [1u32, u32::MAX - 1] {
            let mut metadata =
                fixture(&["01 00 00 00 00 00 00 00 07 00 00 00 00 00 00 00"]).to_segments();
            metadata[0][4..8].copy_from_slice(&count.to_le_bytes());
            assert!(decode(&OperationData::new(metadata).unwrap()).is_err());
        }
        let null_with_user = fixture(&["aa", "01 00 00 00 ff ff ff ff 07 00 00 00 00 00 00 00"]);
        assert!(decode(&null_with_user).is_err());
        let reversed_metadata = fixture(&["01 00 00 00 01 00 00 00 07 00 00 00 00 00 00 00", "aa"]);
        let reversed_copy = fixture(&["aa", "01 00 00 00 07 00 00 00 00 00 00 00"]);
        assert!(decode(&reversed_metadata).is_err());
        assert!(decode_copy(&reversed_copy).is_err());
        for size in [0usize, 3, 15, 17] {
            assert!(decode(&OperationData::new(vec![vec![0; size]]).unwrap()).is_err());
        }
        for size in [0usize, 3, 11, 13] {
            assert!(decode_copy(&OperationData::new(vec![vec![0; size]]).unwrap()).is_err());
        }
    }

    #[test]
    fn named_wire_limits_include_header_bytes_and_segments() {
        let limits = fube_data::Settings::default();
        for is_copy in [false, true] {
            let header = if is_copy { 12 } else { 16 };
            for (size, valid) in [
                (limits.operation_bytes - header, true),
                (limits.operation_bytes - header + 1, false),
            ] {
                let user = OperationData::new(vec![vec![0x91; size]]).unwrap();
                let encoded = if is_copy {
                    encode_copy(7, &user)
                } else {
                    encode(7, Some(&user))
                };
                if valid {
                    let encoded = encoded.unwrap();
                    assert_eq!(encoded.bytes(), limits.operation_bytes);
                    if is_copy {
                        assert_eq!(decode_copy(&encoded).unwrap(), (7, user));
                    } else {
                        assert_eq!(decode(&encoded).unwrap(), (7, Some(user)));
                    }
                } else {
                    assert_eq!(
                        encoded.unwrap_err().kind(),
                        ErrorKind::ReplicationOperationTooLarge
                    );
                }
            }
            for (count, valid) in [
                (limits.operation_segments - 1, true),
                (limits.operation_segments, false),
            ] {
                let user = OperationData::new(vec![vec![]; count]).unwrap();
                let encoded = if is_copy {
                    encode_copy(7, &user)
                } else {
                    encode(7, Some(&user))
                };
                if valid {
                    let encoded = encoded.unwrap();
                    assert_eq!(encoded.segments().len(), limits.operation_segments);
                    if is_copy {
                        assert_eq!(decode_copy(&encoded).unwrap(), (7, user));
                    } else {
                        assert_eq!(decode(&encoded).unwrap(), (7, Some(user)));
                    }
                } else {
                    assert_eq!(
                        encoded.unwrap_err().kind(),
                        ErrorKind::ReplicationOperationTooLarge
                    );
                }
            }
        }
    }
}
