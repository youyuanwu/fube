// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repository root.
//
// Source-guided port of the classic replication wire contracts in Microsoft
// service-fabric, revision 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d:
// src/prod/src/Reliability/Replication/:
//   ReplicationTransport.cpp:25-33,168-868,920-930;
//   ReplicationEndpointId.h:75-80; ReplicationEndpointId.cpp:50-97;
//   ReplicationFromHeader.h; ReplicationActorHeader.h;
//   ReplicationOperationHeader.h:90-99; CopyOperationHeader.h:65;
//   CopyContextOperationHeader.h:57; ReplicationOperationBodyHeader.h;
//   StartCopyMessageBody.h; ReplicationAckMessageBody.h; AckMessageBody.h;
//   InduceFaultMessageBody.h; OperationErrorHeader.h (FABRIC_FIELDS contracts).
// src/prod/src/Common/{RetailSerialization.h,FabricSerializationHelper.h};
// src/prod/src/prebuilt/sdk/FabricTypes.h (FABRIC_EPOCH/OPERATION_METADATA).

//! Native classic replication schemas for the Linux x86-64 clear-TCP profile.
//!
//! These are payload/header codecs, not a connection, security handshake, or
//! replication engine. Framing and security-provider rejection belong to
//! `fube_transport::frame::Frame`. Native interoperability requires execution
//! against an independently built native peer; these codecs alone do not prove it.
//!
//! Bounds are 1 MiB and 1024 buffers per operation, 1024 operations per batch,
//! and the shared transport's complete 4 MiB frame limit. The shared serializer
//! additionally caps each vector at 4096 elements, including the batch's
//! flattened segment-size vector.

use std::{fmt, net::Ipv6Addr, str::FromStr};

use fube_runtime::Epoch;
use fube_transport::{
    frame::MAX_FRAME_SIZE,
    message::{Header, Message, MessageId, ACTION, MESSAGE_ID},
    serialization::{Decoder, Encoder, MAX_COLLECTION_COUNT, MAX_STRING_UNITS},
    Error, Result,
};
use uuid::Uuid;

const RE_FROM: u16 = 0x800a;
const RE_ACTOR: u16 = 0x8019;
const RE_OPERATION: u16 = 0x801a;
const COPY_OPERATION: u16 = 0x801b;
const COPY_CONTEXT: u16 = 0x801d;
const OPERATION_ERROR: u16 = 0x801f;
const OPERATION_BODY: u16 = 0x804a;
const MESSAGE_SECURITY: u16 = 0x8029;
const SECURITY_NEGOTIATION: u16 = 0x8048;

const MAX_OPERATION_BYTES: usize = 1024 * 1024;
const MAX_SEGMENTS: usize = 1024;
const MAX_BATCH_OPERATIONS: usize = 1024;
const NORMAL: u32 = 1;
const END_OF_STREAM: u32 = 2;
const INVALID_ATOMIC_GROUP: i64 = -1;

/// Full native demultiplexing identity, including the replica incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Actor {
    pub partition: Uuid,
    pub replica: i64,
    pub incarnation: Uuid,
}

impl Actor {
    fn write(self, encoder: &mut Encoder) {
        // ReplicationEndpointId uses FABRIC_PRIMITIVE_FIELDS_03, not an object.
        encoder.guid(&self.partition.to_bytes_le());
        encoder.i64(self.replica);
        encoder.guid(&self.incarnation.to_bytes_le());
    }

    fn read(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition: Uuid::from_bytes_le(decoder.guid()?),
            replica: decoder.i64()?,
            incarnation: Uuid::from_bytes_le(decoder.guid()?),
        })
    }

    fn validate(self) -> Result<()> {
        if self.partition.is_nil() && self.replica == 0 && self.incarnation.is_nil() {
            return Err(Error::Protocol("empty replication actor"));
        }
        Ok(())
    }
}

/// Published host/port, native replica identity, and optional routing suffix.
///
/// The transport ID is part of the public replicator endpoint, but is not a
/// field of native `REFrom`. Packet encoding does not transmit that suffix;
/// packet decoding always returns `transport_id: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub address: String,
    pub actor: Actor,
    pub transport_id: Option<String>,
}

impl Endpoint {
    fn validate(&self) -> Result<()> {
        validate_address(&self.address)?;
        self.actor.validate()?;
        if let Some(id) = &self.transport_id {
            if id.len() > MAX_STRING_UNITS
                || id
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace() || c == ';' || c == '/')
            {
                return Err(Error::Protocol("invalid replication transport ID"));
            }
        }
        Ok(())
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}-{};{}",
            self.address, self.actor.partition, self.actor.replica, self.actor.incarnation
        )?;
        if let Some(id) = &self.transport_id {
            write!(f, ";{id}")?;
        }
        Ok(())
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() > MAX_STRING_UNITS {
            return Err(Error::Protocol("replication endpoint exceeds limit"));
        }
        let (address, identity) = value
            .split_once('/')
            .ok_or(Error::Protocol("replication endpoint lacks actor"))?;
        let mut parts = identity.split(';');
        let partition_replica = parts.next().unwrap_or_default();
        let incarnation = parts
            .next()
            .ok_or(Error::Protocol("replication endpoint lacks incarnation"))?;
        let transport_id = parts.next().map(str::to_owned);
        if parts.next().is_some() {
            return Err(Error::Protocol("extra replication endpoint suffix"));
        }
        // A fixed GUID boundary also handles negative signed replica IDs.
        let (partition, replica) = partition_replica
            .get(..36)
            .zip(partition_replica.get(36..))
            .ok_or(Error::Protocol("invalid replication partition ID"))?;
        let replica = replica
            .strip_prefix('-')
            .ok_or(Error::Protocol("replication endpoint lacks replica ID"))?;
        if replica.is_empty()
            || replica.starts_with('+')
            || !replica
                .strip_prefix('-')
                .unwrap_or(replica)
                .bytes()
                .all(|b| b.is_ascii_digit())
        {
            return Err(Error::Protocol("invalid replication replica ID"));
        }
        let endpoint = Self {
            address: address.to_owned(),
            actor: Actor {
                partition: Uuid::parse_str(partition)
                    .map_err(|_| Error::Protocol("invalid replication partition ID"))?,
                replica: replica
                    .parse()
                    .map_err(|_| Error::Protocol("replication replica ID overflow"))?,
                incarnation: Uuid::parse_str(incarnation)
                    .map_err(|_| Error::Protocol("invalid replication incarnation ID"))?,
            },
            transport_id,
        };
        endpoint.validate()?;
        Ok(endpoint)
    }
}

fn validate_address(address: &str) -> Result<()> {
    if address.len() > MAX_STRING_UNITS
        || address.chars().any(|c| {
            c.is_control() || c.is_whitespace() || matches!(c, '/' | ';' | '\\' | '?' | '#')
        })
    {
        return Err(Error::Protocol("invalid replication address"));
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or(Error::Protocol("replication address lacks port"))?;
    let valid_host = if let Some(host) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        host.parse::<Ipv6Addr>().is_ok()
    } else {
        !host.is_empty() && !host.contains([':', '[', ']'])
    };
    if !valid_host
        || port.is_empty()
        || !port.bytes().all(|b| b.is_ascii_digit())
        || port.parse::<u16>().ok().filter(|p| *p != 0).is_none()
    {
        return Err(Error::Protocol("invalid replication host or port"));
    }
    Ok(())
}

/// An operation's native metadata and provider-defined buffer segmentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireOperation {
    pub sequence: i64,
    pub kind: u32,
    pub atomic_group: i64,
    pub data: Vec<Vec<u8>>,
}

/// The eight native classic actions. ACKs intentionally contain no epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    StartCopy {
        epoch: Epoch,
        target: i64,
        start: i64,
    },
    Replication {
        primary_epoch: Epoch,
        operation_epoch: Epoch,
        operations: Vec<WireOperation>,
        batch_end: i64,
        completed: i64,
    },
    Copy {
        epoch: Epoch,
        target: i64,
        sequence: i64,
        data: Vec<Vec<u8>>,
        last: bool,
    },
    Ack {
        received: i64,
        applied: i64,
        copy_received: i64,
        copy_applied: i64,
        error: i32,
    },
    RequestAck,
    CopyContext {
        sequence: i64,
        data: Vec<Vec<u8>>,
        last: bool,
    },
    CopyContextAck {
        sequence: i64,
        error: i32,
    },
    InduceFault {
        target: i64,
        incarnation: Uuid,
        reason: String,
    },
}

impl Body {
    fn action(&self) -> &'static str {
        match self {
            Self::StartCopy { .. } => "StartCopy",
            Self::Replication { .. } => "ReplicationOperation",
            Self::Copy { .. } => "CopyOperation",
            Self::Ack { .. } => "ReplicationAck",
            Self::RequestAck => "RequestAck",
            Self::CopyContext { .. } => "CopyContextOperation",
            Self::CopyContextAck { .. } => "CopyContextAckAction",
            Self::InduceFault { .. } => "InduceFault",
        }
    }
}

/// Addressed native classic message, independent of Fabric TCP framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub from: Endpoint,
    pub to: Actor,
    pub body: Body,
}

impl Packet {
    /// Encode using native objects/headers and raw segmented operation payloads.
    ///
    /// `header_in_body` applies to replication and copy, just as in the native
    /// transport. A batch has one shared kind/atomic-group metadata value and
    /// consecutive sequence numbers; heterogeneous batches must be split.
    pub fn encode(&self, header_in_body: bool) -> Result<Message> {
        self.from.validate()?;
        self.to.validate()?;
        let mut message = Message {
            headers: vec![
                Header::object(RE_FROM, |e| {
                    e.string(&self.from.address)?;
                    self.from.actor.write(e);
                    Ok(())
                })?,
                Header::object(RE_ACTOR, |e| {
                    self.to.write(e);
                    Ok(())
                })?,
                Header::action(self.body.action())?,
                Header::message_id(MessageId {
                    guid: Uuid::new_v4().to_bytes_le(),
                    index: 1,
                })?,
            ],
            body: Vec::new(),
        };
        match &self.body {
            Body::StartCopy {
                epoch,
                target,
                start,
            } => {
                positive_sequence(*start)?;
                message.body = object(|e| {
                    write_epoch(e, *epoch)?;
                    e.i64(*target);
                    e.i64(*start);
                    Ok(())
                })?;
            }
            Body::Replication {
                primary_epoch,
                operation_epoch,
                operations,
                batch_end,
                completed,
            } => {
                let first = operations
                    .first()
                    .ok_or(Error::Protocol("empty replication batch"))?;
                let last = operations.last().unwrap();
                validate_batch(
                    first.sequence,
                    last.sequence,
                    *batch_end,
                    *completed,
                    operations.len(),
                )?;
                let mut sizes = Vec::new();
                let mut counts = Vec::with_capacity(operations.len());
                let mut total = 0usize;
                for (index, operation) in operations.iter().enumerate() {
                    if first.sequence.checked_add(index as i64) != Some(operation.sequence)
                        || operation.kind != first.kind
                        || operation.atomic_group != first.atomic_group
                    {
                        return Err(Error::Protocol(
                            "noncontiguous or heterogeneous replication batch",
                        ));
                    }
                    validate_metadata(operation.kind, operation.atomic_group)?;
                    let operation_sizes = segment_sizes(&operation.data)?;
                    sizes.extend_from_slice(&operation_sizes);
                    if sizes.len() > MAX_COLLECTION_COUNT {
                        return Err(Error::Protocol(
                            "aggregate segment count exceeds codec limit",
                        ));
                    }
                    counts.push(operation_sizes.len() as u32);
                    total = total
                        .checked_add(operation.data.iter().map(Vec::len).sum::<usize>())
                        .ok_or(Error::Protocol("replication payload size overflow"))?;
                    if total > MAX_FRAME_SIZE {
                        return Err(Error::Protocol(
                            "replication payload exceeds aggregate limit",
                        ));
                    }
                }
                let header = object(|e| {
                    write_metadata(e, first.kind, first.sequence, first.atomic_group)?;
                    write_epoch(e, *primary_epoch)?;
                    write_epoch(e, *operation_epoch)?;
                    e.u32_array(&sizes)?;
                    e.i64(first.sequence);
                    e.i64(last.sequence);
                    e.i64(*batch_end);
                    e.u32_array(&counts)?;
                    e.i64(*completed);
                    Ok(())
                })?;
                add_operation_header(&mut message, RE_OPERATION, header, header_in_body)?;
                for operation in operations {
                    append_segments(&mut message.body, &operation.data)?;
                }
            }
            Body::Copy {
                epoch,
                target,
                sequence,
                data,
                last,
            } => {
                positive_sequence(*sequence)?;
                let sizes = segment_sizes(data)?;
                let header = object(|e| {
                    e.i64(*target);
                    write_epoch(e, *epoch)?;
                    write_metadata(e, copy_kind(*last), *sequence, INVALID_ATOMIC_GROUP)?;
                    e.u32_array(&sizes)?;
                    e.boolean(*last);
                    Ok(())
                })?;
                add_operation_header(&mut message, COPY_OPERATION, header, header_in_body)?;
                append_segments(&mut message.body, data)?;
            }
            Body::Ack {
                received,
                applied,
                copy_received,
                copy_applied,
                error,
            } => {
                validate_ack(*received, *applied, *copy_received, *copy_applied)?;
                message.body = object(|e| {
                    e.i64(*received);
                    e.i64(*applied);
                    e.i64(*copy_received);
                    e.i64(*copy_applied);
                    Ok(())
                })?;
                add_error(&mut message, *error)?;
            }
            Body::RequestAck => {}
            Body::CopyContext {
                sequence,
                data,
                last,
            } => {
                positive_sequence(*sequence)?;
                let sizes = segment_sizes(data)?;
                message.headers.push(Header::object(COPY_CONTEXT, |e| {
                    write_metadata(e, copy_kind(*last), *sequence, INVALID_ATOMIC_GROUP)?;
                    e.u32_array(&sizes)?;
                    e.boolean(*last);
                    Ok(())
                })?);
                append_segments(&mut message.body, data)?;
            }
            Body::CopyContextAck { sequence, error } => {
                if *sequence < 0 {
                    return Err(Error::Protocol("invalid copy-context ACK sequence"));
                }
                message.body = object(|e| {
                    e.i64(*sequence);
                    Ok(())
                })?;
                add_error(&mut message, *error)?;
            }
            Body::InduceFault {
                target,
                incarnation,
                reason,
            } => {
                message.body = object(|e| {
                    e.i64(*target);
                    e.guid(&incarnation.to_bytes_le());
                    e.string(reason)
                })?;
            }
        }
        validate_message(&message)?;
        Ok(message)
    }

    /// Decode both native operation-header locations, rejecting ambiguous
    /// identities, impossible batches, malformed native ABI values and bounds.
    pub fn decode(message: &Message) -> Result<Self> {
        validate_message(message)?;
        for id in [
            RE_FROM,
            RE_ACTOR,
            ACTION,
            MESSAGE_ID,
            RE_OPERATION,
            COPY_OPERATION,
            COPY_CONTEXT,
            OPERATION_BODY,
            OPERATION_ERROR,
        ] {
            unique_header(message, id)?;
        }
        if message
            .headers
            .iter()
            .any(|h| matches!(h.id, MESSAGE_SECURITY | SECURITY_NEGOTIATION))
        {
            return Err(Error::Protocol(
                "replication security headers outside clear-TCP profile",
            ));
        }
        required_header(message, MESSAGE_ID)?.read(MessageId::decode)?;
        let action = required_header(message, ACTION)?.read(|d| d.string())?;
        let from = required_header(message, RE_FROM)?.read(|d| {
            Ok(Endpoint {
                address: d.string()?,
                actor: Actor::read(d)?,
                transport_id: None,
            })
        })?;
        let to = required_header(message, RE_ACTOR)?.read(Actor::read)?;
        from.validate()?;
        to.validate()?;
        let allowed: &[u16] = match action.as_str() {
            "ReplicationOperation" => &[RE_OPERATION, OPERATION_BODY],
            "CopyOperation" => &[COPY_OPERATION, OPERATION_BODY],
            "CopyContextOperation" => &[COPY_CONTEXT],
            "ReplicationAck" | "CopyContextAckAction" => &[OPERATION_ERROR],
            "StartCopy" | "RequestAck" | "InduceFault" => &[],
            _ => return Err(Error::Protocol("unsupported classic replication action")),
        };
        for header in &message.headers {
            if matches!(
                header.id,
                RE_OPERATION | COPY_OPERATION | COPY_CONTEXT | OPERATION_BODY | OPERATION_ERROR
            ) && !allowed.contains(&header.id)
            {
                return Err(Error::Protocol("operation header does not match action"));
            }
        }
        let body = match action.as_str() {
            "StartCopy" => read_object(&message.body, |d| {
                let epoch = read_epoch(d)?;
                let target = d.i64()?;
                let start = d.i64()?;
                positive_sequence(start)?;
                Ok(Body::StartCopy {
                    epoch,
                    target,
                    start,
                })
            })?,
            "ReplicationOperation" => decode_replication(message)?,
            "CopyOperation" => {
                let (header, payload) = operation_parts(message, COPY_OPERATION)?;
                let (target, epoch, sequence, sizes, last) = read_object(header, |d| {
                    let target = d.i64()?;
                    let epoch = read_epoch(d)?;
                    let metadata = read_metadata(d)?;
                    let sizes = d.u32_array()?;
                    let last = d.boolean()?;
                    validate_copy_metadata(metadata, last)?;
                    Ok((target, epoch, metadata.1, sizes, last))
                })?;
                Body::Copy {
                    epoch,
                    target,
                    sequence,
                    data: split_segments(&sizes, payload)?,
                    last,
                }
            }
            "CopyContextOperation" => {
                let (sequence, sizes, last) =
                    required_header(message, COPY_CONTEXT)?.read(|d| {
                        let metadata = read_metadata(d)?;
                        let sizes = d.u32_array()?;
                        let last = d.boolean()?;
                        validate_copy_metadata(metadata, last)?;
                        Ok((metadata.1, sizes, last))
                    })?;
                Body::CopyContext {
                    sequence,
                    data: split_segments(&sizes, &message.body)?,
                    last,
                }
            }
            "ReplicationAck" => read_object(&message.body, |d| {
                let received = d.i64()?;
                let applied = d.i64()?;
                let copy_received = d.i64()?;
                let copy_applied = d.i64()?;
                validate_ack(received, applied, copy_received, copy_applied)?;
                Ok(Body::Ack {
                    received,
                    applied,
                    copy_received,
                    copy_applied,
                    error: read_error(message)?,
                })
            })?,
            "RequestAck" => {
                if !message.body.is_empty() {
                    return Err(Error::Protocol("RequestAck body must be empty"));
                }
                Body::RequestAck
            }
            "CopyContextAckAction" => read_object(&message.body, |d| {
                let sequence = d.i64()?;
                if sequence < 0 {
                    return Err(Error::Protocol("invalid copy-context ACK sequence"));
                }
                Ok(Body::CopyContextAck {
                    sequence,
                    error: read_error(message)?,
                })
            })?,
            "InduceFault" => read_object(&message.body, |d| {
                Ok(Body::InduceFault {
                    target: d.i64()?,
                    incarnation: Uuid::from_bytes_le(d.guid()?),
                    reason: d.string()?,
                })
            })?,
            _ => unreachable!(),
        };
        Ok(Self { from, to, body })
    }
}

fn object(f: impl FnOnce(&mut Encoder) -> Result<()>) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder.object(f)?;
    Ok(encoder.finish())
}

fn read_object<T>(bytes: &[u8], f: impl FnOnce(&mut Decoder<'_>) -> Result<T>) -> Result<T> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.object(f)?;
    decoder.finish()?;
    Ok(value)
}

fn validate_message(message: &Message) -> Result<()> {
    if message.body.len() > MAX_FRAME_SIZE {
        return Err(Error::Protocol("replication body exceeds aggregate limit"));
    }
    // Keep frame/header limits owned by the shared transport, including the
    // complete 4 MiB extent rather than just the provider payload length.
    message.to_frame()?.encode()?;
    Ok(())
}

fn unique_header(message: &Message, id: u16) -> Result<Option<&Header>> {
    let mut matches = message.headers.iter().filter(|header| header.id == id);
    let first = matches.next();
    if matches.next().is_some() {
        return Err(Error::Protocol("duplicate critical replication header"));
    }
    Ok(first)
}

fn required_header(message: &Message, id: u16) -> Result<&Header> {
    unique_header(message, id)?.ok_or(Error::Protocol("missing critical replication header"))
}

fn add_error(message: &mut Message, error: i32) -> Result<()> {
    if error != 0 {
        message.headers.push(Header::object(OPERATION_ERROR, |e| {
            e.i32(error);
            Ok(())
        })?);
    }
    Ok(())
}

fn read_error(message: &Message) -> Result<i32> {
    unique_header(message, OPERATION_ERROR)?
        .map(|header| header.read(|d| d.i32()))
        .transpose()
        .map(|error| error.unwrap_or_default())
}

fn add_operation_header(
    message: &mut Message,
    id: u16,
    header: Vec<u8>,
    in_body: bool,
) -> Result<()> {
    if in_body {
        message.headers.push(Header::object(OPERATION_BODY, |e| {
            e.u32(header.len() as u32);
            Ok(())
        })?);
        message.body = header;
    } else {
        message.headers.push(Header { id, value: header });
    }
    Ok(())
}

fn operation_parts(message: &Message, id: u16) -> Result<(&[u8], &[u8])> {
    match (
        unique_header(message, id)?,
        unique_header(message, OPERATION_BODY)?,
    ) {
        (Some(header), None) => Ok((&header.value, &message.body)),
        (None, Some(header)) => {
            let size = header.read(|d| d.u32())? as usize;
            if size == 0 || size > message.body.len() {
                return Err(Error::Protocol("invalid in-body operation header length"));
            }
            Ok(message.body.split_at(size))
        }
        _ => Err(Error::Protocol("missing or ambiguous operation header")),
    }
}

fn write_epoch(encoder: &mut Encoder, epoch: Epoch) -> Result<()> {
    validate_epoch(epoch)?;
    let mut bytes = [0; 24];
    bytes[..8].copy_from_slice(&epoch.data_loss.to_le_bytes());
    bytes[8..16].copy_from_slice(&epoch.configuration.to_le_bytes());
    encoder.byte_array(&bytes)
}

fn read_epoch(decoder: &mut Decoder<'_>) -> Result<Epoch> {
    let bytes = decoder.byte_array()?;
    if bytes.len() != 24 || bytes[16..].iter().any(|b| *b != 0) {
        return Err(Error::Protocol("invalid Linux FABRIC_EPOCH ABI"));
    }
    let epoch = Epoch {
        data_loss: i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        configuration: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    };
    validate_epoch(epoch)?;
    Ok(epoch)
}

fn validate_epoch(epoch: Epoch) -> Result<()> {
    if epoch.data_loss < 0 || epoch.configuration < 0 {
        return Err(Error::Protocol("negative replication epoch"));
    }
    Ok(())
}

fn write_metadata(encoder: &mut Encoder, kind: u32, sequence: i64, group: i64) -> Result<()> {
    validate_metadata(kind, group)?;
    positive_sequence(sequence)?;
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&kind.to_le_bytes());
    bytes[8..16].copy_from_slice(&sequence.to_le_bytes());
    bytes[16..24].copy_from_slice(&group.to_le_bytes());
    encoder.byte_array(&bytes)
}

fn read_metadata(decoder: &mut Decoder<'_>) -> Result<(u32, i64, i64)> {
    let bytes = decoder.byte_array()?;
    if bytes.len() != 32 || bytes[24..].iter().any(|b| *b != 0) {
        return Err(Error::Protocol(
            "invalid Linux FABRIC_OPERATION_METADATA ABI",
        ));
    }
    // Bytes 4..8 are native ABI padding; native stack objects need not zero it.
    let kind = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    let sequence = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let group = i64::from_le_bytes(bytes[16..24].try_into().unwrap());
    validate_metadata(kind, group)?;
    positive_sequence(sequence)?;
    Ok((kind, sequence, group))
}

fn validate_metadata(kind: u32, group: i64) -> Result<()> {
    if !matches!(kind, NORMAL | END_OF_STREAM | 0x10 | 0x20 | 0x40 | 0x80) || group < -1 {
        return Err(Error::Protocol("invalid operation type or atomic group"));
    }
    Ok(())
}

fn copy_kind(last: bool) -> u32 {
    if last {
        END_OF_STREAM
    } else {
        NORMAL
    }
}

fn validate_copy_metadata((kind, _sequence, group): (u32, i64, i64), last: bool) -> Result<()> {
    // The native receiver explicitly overwrites Type with EOS when isLast.
    if group != INVALID_ATOMIC_GROUP || !(kind == NORMAL || (last && kind == END_OF_STREAM)) {
        return Err(Error::Protocol("invalid copy operation metadata"));
    }
    Ok(())
}

fn positive_sequence(sequence: i64) -> Result<()> {
    if sequence <= 0 {
        return Err(Error::Protocol("nonpositive operation sequence"));
    }
    Ok(())
}

fn validate_batch(
    first: i64,
    last: i64,
    batch_end: i64,
    completed: i64,
    count: usize,
) -> Result<()> {
    positive_sequence(first)?;
    let range = last.checked_sub(first).and_then(|n| n.checked_add(1));
    if count == 0
        || count > MAX_BATCH_OPERATIONS
        || range != Some(count as i64)
        || batch_end < last
        || completed < 0
    {
        return Err(Error::Protocol(
            "invalid replication batch range or completion",
        ));
    }
    Ok(())
}

fn validate_ack(received: i64, applied: i64, copy_received: i64, copy_applied: i64) -> Result<()> {
    if received < 0
        || applied < 0
        || applied > received
        || copy_received < -1
        || copy_applied < -1
        || (copy_applied == -1 && copy_received != -1)
        || (copy_received >= 0 && copy_applied > copy_received)
    {
        return Err(Error::Protocol("invalid cumulative replication/copy ACK"));
    }
    // -1 for received means all copy was received, not a smaller watermark.
    Ok(())
}

fn segment_sizes(data: &[Vec<u8>]) -> Result<Vec<u32>> {
    if data.len() > MAX_SEGMENTS {
        return Err(Error::Protocol("operation segment count exceeds limit"));
    }
    let mut total = 0usize;
    let mut sizes = Vec::with_capacity(data.len());
    for segment in data {
        total = total
            .checked_add(segment.len())
            .ok_or(Error::Protocol("operation byte size overflow"))?;
        if total > MAX_OPERATION_BYTES {
            return Err(Error::Protocol("operation byte size exceeds limit"));
        }
        sizes.push(segment.len() as u32);
    }
    Ok(sizes)
}

fn append_segments(body: &mut Vec<u8>, data: &[Vec<u8>]) -> Result<()> {
    for segment in data {
        if body
            .len()
            .checked_add(segment.len())
            .is_none_or(|n| n > MAX_FRAME_SIZE)
        {
            return Err(Error::Protocol("operation body exceeds aggregate limit"));
        }
        body.extend_from_slice(segment);
    }
    Ok(())
}

fn payload_size(sizes: &[u32]) -> Result<usize> {
    if sizes.len() > MAX_SEGMENTS {
        return Err(Error::Protocol("operation segment count exceeds limit"));
    }
    let size = sizes
        .iter()
        .try_fold(0usize, |n, &size| n.checked_add(size as usize))
        .ok_or(Error::Protocol("operation byte size overflow"))?;
    if size > MAX_OPERATION_BYTES {
        return Err(Error::Protocol("operation byte size exceeds limit"));
    }
    Ok(size)
}

fn split_segments(sizes: &[u32], payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    if payload_size(sizes)? != payload.len() {
        return Err(Error::Protocol("operation segments do not match payload"));
    }
    let mut offset = 0;
    Ok(sizes
        .iter()
        .map(|&size| {
            let end = offset + size as usize;
            let segment = payload[offset..end].to_vec();
            offset = end;
            segment
        })
        .collect())
}

fn decode_replication(message: &Message) -> Result<Body> {
    let (header, payload) = operation_parts(message, RE_OPERATION)?;
    let (
        metadata,
        primary_epoch,
        operation_epoch,
        sizes,
        first,
        last,
        batch_end,
        counts,
        completed,
    ) = read_object(header, |d| {
        let metadata = read_metadata(d)?;
        let primary_epoch = read_epoch(d)?;
        let operation_epoch = read_epoch(d)?;
        let sizes = d.u32_array()?;
        if d.is_scope_end()? {
            let count = sizes.len() as u32;
            return Ok((
                metadata,
                primary_epoch,
                operation_epoch,
                sizes,
                metadata.1,
                metadata.1,
                metadata.1,
                vec![count],
                0,
            ));
        }
        let first = d.i64()?;
        let last = d.i64()?;
        let batch_end = d.i64()?;
        let counts = d.u32_array()?;
        let completed = if d.is_scope_end()? { 0 } else { d.i64()? };
        if first == 0 {
            if last != 0 || batch_end != 0 || !counts.is_empty() {
                return Err(Error::Protocol("inconsistent legacy replication batch"));
            }
            let count = sizes.len() as u32;
            return Ok((
                metadata,
                primary_epoch,
                operation_epoch,
                sizes,
                metadata.1,
                metadata.1,
                metadata.1,
                vec![count],
                completed,
            ));
        }
        Ok((
            metadata,
            primary_epoch,
            operation_epoch,
            sizes,
            first,
            last,
            batch_end,
            counts,
            completed,
        ))
    })?;
    validate_batch(first, last, batch_end, completed, counts.len())?;
    if metadata.1 != first {
        return Err(Error::Protocol("operation metadata does not match batch"));
    }
    let count = counts
        .iter()
        .try_fold(0usize, |sum, &n| sum.checked_add(n as usize))
        .ok_or(Error::Protocol("batch buffer-count overflow"))?;
    if count != sizes.len() {
        return Err(Error::Protocol("batch buffer counts do not match segments"));
    }
    // Validate every operation extent before allocating any provider buffers.
    let mut segment_offset = 0;
    let mut total = 0usize;
    for &count in &counts {
        let end = segment_offset + count as usize;
        total = total
            .checked_add(payload_size(&sizes[segment_offset..end])?)
            .ok_or(Error::Protocol("batch payload size overflow"))?;
        segment_offset = end;
    }
    if total != payload.len() {
        return Err(Error::Protocol("batch segments do not match payload"));
    }
    let mut operations = Vec::with_capacity(counts.len());
    let mut segment_offset = 0;
    let mut payload_offset = 0;
    for (index, &count) in counts.iter().enumerate() {
        let end = segment_offset + count as usize;
        let operation_sizes = &sizes[segment_offset..end];
        let payload_end = payload_offset + payload_size(operation_sizes)?;
        operations.push(WireOperation {
            sequence: first
                .checked_add(index as i64)
                .ok_or(Error::Protocol("batch sequence overflow"))?,
            kind: metadata.0,
            atomic_group: metadata.2,
            data: split_segments(operation_sizes, &payload[payload_offset..payload_end])?,
        });
        segment_offset = end;
        payload_offset = payload_end;
    }
    Ok(Body::Replication {
        primary_epoch,
        operation_epoch,
        operations,
        batch_end,
        completed,
    })
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
