use fube_transport::{
    frame::{FIXED_HEADER_SIZE, MAX_FRAME_SIZE},
    message::{Header, Message, MessageId, ACTIVITY},
    serialization::{
        serde::{to_fields, to_vec, Derived, Guid, Inline},
        Decoder,
    },
};
use serde::Serialize;

use crate::{Error, Result};

pub(crate) const SERVICE: &str = "fabric:/System/ImageStoreService";
pub(crate) const CREATE: u16 = 0x804f;
pub(crate) const FILE_NOT_FOUND: i64 = -2147017728;
pub(crate) const OPERATIONS_PENDING: i64 = -2147017570;
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 3 * 1024 * 1024;

#[derive(Serialize)]
struct Upload<'a> {
    service: &'a str,
    path: &'a str,
    overwrite: bool,
    size: u64,
}

pub(crate) fn create(path: &str, size: u64) -> Result<Header> {
    Header::object(CREATE, |e| {
        to_fields(
            e,
            &Upload {
                service: SERVICE,
                path,
                overwrite: true,
                size,
            },
        )
    })
}

pub(crate) fn headers(activity: [u8; 16]) -> Result<Vec<Header>> {
    Ok(vec![Header::activity(activity)?, Header::version(1, 2)?])
}

pub(crate) fn sequence(number: u64, size: u64) -> Result<Header> {
    Header::object(fube_transport::file_transfer::FILE_SEQUENCE, |e| {
        e.u64(number);
        e.boolean(false);
        e.u64(size);
        Ok(())
    })
}

/// Reserve the largest encoding of every content header, not just raw payload.
pub(crate) fn chunk_capacity() -> Result<u64> {
    let mut headers = headers([255; 16])?;
    headers.extend([
        Header::actor(26)?,
        Header::action("FileContent")?,
        Header::message_id(MessageId {
            guid: [255; 16],
            index: u32::MAX,
        })?,
        Header::timeout(i64::MAX)?,
        sequence(u64::MAX, u64::MAX)?,
    ]);
    let overhead = FIXED_HEADER_SIZE + headers.iter().map(|h| 4 + h.value.len()).sum::<usize>();
    Ok((MAX_FRAME_SIZE - overhead) as u64)
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
struct Delete<'a> {
    path: &'a str,
}

pub(crate) fn delete(path: &str, activity: [u8; 16]) -> Result<(Vec<Header>, Vec<u8>)> {
    let mut headers = headers(activity)?;
    headers.extend([
        Header::object(0x8028, |e| {
            to_fields(
                e,
                &Forward {
                    actor: 17,
                    action: "ServiceRouteRequest",
                },
            )
        })?,
        Header::object(0x802c, |e| {
            to_fields(
                e,
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
            )
        })?,
        Header::object(0x8034, |e| {
            e.guid(&uuid::Uuid::from_u128(0x3000).to_bytes_le());
            Ok(())
        })?,
    ]);
    Ok((headers, to_vec(&Delete { path })?))
}

pub(crate) fn decode_delete(message: Message) -> Result<Option<[u8; 16]>> {
    let p = message.properties()?;
    if p.fault.is_some() || p.expects_reply || p.uncorrelated_reply {
        return Err(Error::Protocol("invalid routed ImageStore Delete reply"));
    }
    if p.actor == 5 && p.action == "ClientOperationFailure" {
        let mut d = Decoder::new(&message.body);
        let (code, diagnostic) = d.object(|d| Ok((d.i64()?, d.string()?)))?;
        d.finish()?;
        if code == 0 || i32::try_from(code).is_err() {
            return Err(Error::Protocol("invalid routed Delete failure code"));
        }
        return if code == FILE_NOT_FOUND {
            Ok(None)
        } else {
            Err(Error::Remote {
                code,
                message: diagnostic,
            })
        };
    }
    if p.actor != 22 || p.action != "OperationSuccessAction" || !message.body.is_empty() {
        return Err(Error::Protocol(
            "unexpected routed ImageStore Delete result",
        ));
    }
    let mut activities = message.headers.iter().filter(|h| h.id == ACTIVITY);
    let activity = activities
        .next()
        .ok_or(Error::Protocol("Delete success lacks Activity"))?;
    if activities.next().is_some() {
        return Err(Error::Protocol("duplicate Delete success Activity"));
    }
    let guid = activity.read(|d| {
        d.object(|d| {
            let guid = d.guid()?;
            if d.u64()? != 0 {
                return Err(Error::Protocol("invalid Delete Activity index"));
            }
            Ok(Guid(guid))
        })
    })?;
    Ok(Some(guid.0))
}

pub(crate) fn preflight_root_marker(destination: &str) -> Result<()> {
    let preflight = || {
        // Match the transport-added FileCreate envelope at maximum native
        // MessageId/TimeSpan widths, then reuse the actual upload headers.
        let mut complete = vec![
            Header::actor(26)?,
            Header::action("FileCreate")?,
            Header::message_id(MessageId {
                guid: [255; 16],
                index: u32::MAX,
            })?,
        ];
        complete.extend(headers([255; 16])?);
        complete.push(create(&format!("{destination}/_.dir"), 0)?);
        complete.push(Header::timeout(i64::MAX)?);
        Message {
            headers: complete,
            body: vec![],
        }
        .to_frame()?
        .encode()?;
        Ok::<_, Error>(())
    };
    preflight().map_err(|_| {
        Error::InvalidInput("ImageStore destination exceeds complete root marker frame limits")
    })
}
