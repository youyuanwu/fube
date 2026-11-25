use fubst::serde::{from_slice, Derived, Guid, Inline};
use furpc::{
    header, ActorId, DecodeBuf, Decoder, FubstDecoder, HeaderId, HeaderRecord, Headers, Message,
    MessageId,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::Instant;

use crate::{DomainFailure, FurpcClientError, ImageStoreError, ImageStoreResult as Result};

pub(crate) const SERVICE: &str = "fabric:/System/ImageStoreService";
pub(crate) const CREATE: u16 = 0x804f;
pub(crate) const FILE_NOT_FOUND: i64 = -2147017728;
pub(crate) const OPERATIONS_PENDING: i64 = -2147017570;
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 3 * 1024 * 1024;
const FILE_SEQUENCE: u16 = 0x803c;
const ACTIVITY: u16 = 0x8020;
const CLIENT_VERSION: u16 = 0x8025;
const IPC: u16 = 0x800b;

pub(crate) fn remaining_ticks(deadline: Instant) -> Result<i64> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(furpc::Error::Timeout("ImageStore"))?;
    let ticks = i64::try_from(remaining.as_nanos() / 100)
        .map_err(|_| furpc::Error::InvalidInput("timeout exceeds native TimeSpan"))?;
    if ticks == 0 {
        return Err(furpc::Error::Timeout("ImageStore").into());
    }
    Ok(ticks)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct FileSequence {
    pub number: u64,
    last: bool,
    pub buffer_size: u64,
}

#[derive(Debug)]
pub(crate) struct FileTransferReply {
    pub activity: [u8; 16],
    pub actor: i64,
    pub sequence: Option<FileSequence>,
    pub result: std::result::Result<(), DomainFailure>,
}

#[derive(Serialize, Deserialize)]
struct ActivityId(Guid, u64);

#[derive(Serialize, Deserialize)]
struct Activity {
    activity: ActivityId,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Version(i64, i64);

#[derive(Serialize)]
struct Timeout {
    ticks: i64,
}

#[derive(Deserialize)]
struct Failure {
    code: i64,
    diagnostic: String,
}

fn read_header<T: DeserializeOwned>(headers: &Headers, id: u16) -> Result<T> {
    let record = headers
        .records()
        .find(|record| record.id().get() == id)
        .ok_or(FurpcClientError::Protocol(
            "missing native file-transfer header",
        ))?;
    Ok(from_slice(record.value())?)
}

fn failure(src: &mut DecodeBuf<'_>) -> Result<DomainFailure> {
    let failure = FubstDecoder::<Failure>::default()
        .decode(src)
        .map_err(|source| {
            ImageStoreError::from(furpc::Error::Codec {
                phase: "decode",
                source,
            })
        })?;
    if failure.code == 0 || i32::try_from(failure.code).is_err() {
        return Err(FurpcClientError::Protocol("invalid native upload failure code").into());
    }
    Ok(DomainFailure {
        code: failure.code,
        message: failure.diagnostic,
    })
}

/// Interpret every semantic ACK before applying obsolete-Activity policy.
pub(crate) fn decode_reply(
    headers: &Headers,
    body: &mut DecodeBuf<'_>,
) -> Result<FileTransferReply> {
    let common = headers.effective();
    let success = common
        .action()
        .iter()
        .copied()
        .eq("ClientOperationSuccess".encode_utf16());
    let failed = common
        .action()
        .iter()
        .copied()
        .eq("ClientOperationFailure".encode_utf16());
    if !matches!(common.actor.get(), 25 | 27)
        || !(success || failed)
        || common.relates_to != MessageId::default()
        || common.expects_reply
        || common.uncorrelated_reply
        || headers.first_raw(header::FAULT).is_some()
    {
        return Err(FurpcClientError::Protocol("invalid native file-transfer envelope").into());
    }
    let mut seen = std::collections::HashSet::new();
    for record in headers.records() {
        if record.id().get() == IPC || !seen.insert(record.id()) {
            return Err(
                FurpcClientError::Protocol("duplicate or IPC native file-transfer header").into(),
            );
        }
    }
    if read_header::<Version>(headers, CLIENT_VERSION)? != Version(1, 2) {
        return Err(
            FurpcClientError::Protocol("native upload requires protocol version 1.2").into(),
        );
    }
    let Activity {
        activity: ActivityId(Guid(activity), index),
    } = read_header(headers, ACTIVITY)?;
    if activity == [0; 16] || index != 0 {
        return Err(FurpcClientError::Protocol("invalid native upload Activity").into());
    }
    let sequence = if common.actor.get() == 25 {
        let sequence: FileSequence = read_header(headers, FILE_SEQUENCE)?;
        if sequence.last {
            return Err(
                FurpcClientError::Protocol("v1.2 acknowledgement has last-chunk flag").into(),
            );
        }
        Some(sequence)
    } else {
        if seen.contains(&HeaderId::new(FILE_SEQUENCE)?) {
            return Err(FurpcClientError::Protocol("commit reply has a sequence header").into());
        }
        None
    };
    let result = if success {
        if !body.is_empty() {
            return Err(FurpcClientError::Protocol("native upload success has a body").into());
        }
        Ok(())
    } else {
        Err(failure(body)?)
    };
    Ok(FileTransferReply {
        activity,
        actor: common.actor.get(),
        sequence,
        result,
    })
}

#[derive(Serialize)]
struct Upload<'a> {
    service: &'a str,
    path: &'a str,
    overwrite: bool,
    size: u64,
}

pub(crate) fn create(path: &str, size: u64) -> Result<HeaderRecord> {
    Ok(HeaderRecord::native(
        HeaderId::new(CREATE)?,
        &Upload {
            service: SERVICE,
            path,
            overwrite: true,
            size,
        },
    )?)
}

pub(crate) fn headers(activity: [u8; 16]) -> Result<Vec<HeaderRecord>> {
    Ok(vec![
        HeaderRecord::native(
            HeaderId::new(ACTIVITY)?,
            &Activity {
                activity: ActivityId(Guid(activity), 0),
            },
        )?,
        HeaderRecord::native(HeaderId::new(CLIENT_VERSION)?, &Version(1, 2))?,
    ])
}

pub(crate) fn timeout(deadline: Instant) -> Result<HeaderRecord> {
    Ok(HeaderRecord::native(
        HeaderId::new(0x8023)?,
        &Timeout {
            ticks: remaining_ticks(deadline)?,
        },
    )?)
}

pub(crate) fn sequence(number: u64, size: u64) -> Result<HeaderRecord> {
    Ok(HeaderRecord::native(
        HeaderId::new(FILE_SEQUENCE)?,
        &FileSequence {
            number,
            last: false,
            buffer_size: size,
        },
    )?)
}

fn widest_message_id() -> furpc::Result<HeaderRecord> {
    HeaderRecord::native(
        header::MESSAGE_ID,
        &MessageId {
            guid: [255; 16],
            index: u32::MAX,
        },
    )
}

/// Native full-frame capacity, including the managed ID. Endpoint policy still applies.
pub(crate) fn chunk_capacity() -> Result<u64> {
    let mut headers = headers([255; 16])?;
    headers.extend([
        header::actor(ActorId::new(26)?)?,
        header::action("FileContent")?,
        widest_message_id()?,
        HeaderRecord::native(HeaderId::new(0x8023)?, &Timeout { ticks: i64::MAX })?,
        sequence(u64::MAX, u64::MAX)?,
    ]);
    let overhead = 12
        + headers
            .iter()
            .map(|header| 4 + header.value().len())
            .sum::<usize>();
    Ok((u32::MAX as usize - overhead) as u64)
}

#[derive(Serialize)]
struct Forward<'a> {
    actor: i64,
    action: &'a str,
}
#[derive(Serialize)]
struct Application<'a> {
    name: &'a str,
    number: u32,
}
#[derive(Serialize)]
struct Package<'a> {
    application: Application<'a>,
    name: &'a str,
}
#[derive(Serialize)]
struct ServiceType<'a> {
    package: Package<'a>,
    name: &'a str,
}
#[derive(Serialize)]
struct TypeScope<'a> {
    service_type: ServiceType<'a>,
}
#[derive(Serialize)]
struct Route<'a> {
    base: Inline<Forward<'a>>,
    derived: Derived<TypeScope<'a>>,
}
#[derive(Serialize)]
pub(crate) struct Delete<'a> {
    path: &'a str,
}

pub(crate) fn delete(path: &str, activity: [u8; 16]) -> Result<(Vec<HeaderRecord>, Delete<'_>)> {
    #[derive(Serialize)]
    struct Partition {
        id: Guid,
    }

    let mut headers = headers(activity)?;
    headers.extend([
        HeaderRecord::native(
            HeaderId::new(0x8028)?,
            &Forward {
                actor: 17,
                action: "ServiceRouteRequest",
            },
        )?,
        HeaderRecord::native(
            HeaderId::new(0x802c)?,
            &Route {
                base: Inline(Forward {
                    actor: 22,
                    action: "DeleteAction",
                }),
                derived: Derived(TypeScope {
                    service_type: ServiceType {
                        package: Package {
                            application: Application {
                                name: "__FabricSystem",
                                number: u32::MAX,
                            },
                            name: "FileStoreService",
                        },
                        name: "FileStoreServiceType",
                    },
                }),
            },
        )?,
        HeaderRecord::native(
            HeaderId::new(0x8034)?,
            &Partition {
                id: Guid(uuid::Uuid::from_u128(0x3000).to_bytes_le()),
            },
        )?,
    ]);
    Ok((headers, Delete { path }))
}

pub(crate) fn decode_delete(
    headers: &Headers,
    body: &mut DecodeBuf<'_>,
) -> Result<Option<[u8; 16]>> {
    let common = headers.effective();
    if headers.first_raw(header::FAULT).is_some()
        || common.expects_reply
        || common.uncorrelated_reply
    {
        return Err(FurpcClientError::Protocol("invalid routed ImageStore Delete reply").into());
    }
    if common.actor.get() == 5
        && common
            .action()
            .iter()
            .copied()
            .eq("ClientOperationFailure".encode_utf16())
    {
        let failure = failure(body)?;
        return if failure.code == FILE_NOT_FOUND {
            Ok(None)
        } else {
            Err(FurpcClientError::Domain(failure).into())
        };
    }
    if common.actor.get() != 22
        || !common
            .action()
            .iter()
            .copied()
            .eq("OperationSuccessAction".encode_utf16())
        || !body.is_empty()
    {
        return Err(
            FurpcClientError::Protocol("unexpected routed ImageStore Delete result").into(),
        );
    }
    if headers
        .records()
        .filter(|record| record.id().get() == ACTIVITY)
        .count()
        != 1
    {
        return Err(FurpcClientError::Protocol("Delete success requires one Activity").into());
    }
    let Activity {
        activity: ActivityId(Guid(activity), index),
    } = read_header(headers, ACTIVITY)?;
    if index != 0 {
        return Err(FurpcClientError::Protocol("invalid Delete Activity index").into());
    }
    Ok(Some(activity))
}

pub(crate) fn preflight_root_marker(destination: &str) -> Result<()> {
    let mut headers = headers([255; 16])?;
    headers.extend([
        header::actor(ActorId::new(26)?)?,
        header::action("FileCreate")?,
        widest_message_id()?,
        create(&format!("{destination}/_.dir"), 0)?,
        HeaderRecord::native(HeaderId::new(0x8023)?, &Timeout { ticks: i64::MAX })?,
    ]);
    let mut message = Message::builder();
    for header in headers {
        message = message.append_header(header);
    }
    message.build()?;
    Ok(())
}
