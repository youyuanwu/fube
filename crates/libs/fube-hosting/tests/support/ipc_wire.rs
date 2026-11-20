//! Independent adversarial wire fixture. Public furpc headers are used for
//! schemas/inspection; injected clear frames deliberately disable checksums.
//! This is not a production codec or a managed model peer.
use fube_rap_test_support::transport::relay::Frame;
use fubst::serde::Guid;
use furpc::{header, ActorId, BoxError, HeaderId, HeaderRecord, Headers, MessageId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub const EXPECTS_REPLY: u16 = header::EXPECTS_REPLY.get();
pub const IPC: u16 = 0x800b;
pub const TIMEOUT: u16 = 0x8023;
pub const FAULT: u16 = header::FAULT.get();
pub const UNCORRELATED_REPLY: u16 = header::UNCORRELATED_REPLY.get();
pub type Result<T> = std::result::Result<T, BoxError>;

#[derive(Serialize)]
pub struct Identity<'a> {
    pub host: &'a str,
    pub pid: u32,
}
#[derive(Serialize)]
pub struct Fault {
    pub error: i64,
    pub has_body: bool,
}
#[derive(Serialize)]
struct Scalar<T> {
    value: T,
}

#[derive(Clone)]
pub struct Header {
    pub id: u16,
    pub value: Vec<u8>,
}
impl Header {
    fn record(record: HeaderRecord) -> Self {
        Self {
            id: record.id().get(),
            value: record.value().to_vec(),
        }
    }
    pub fn native<T: Serialize>(id: u16, value: &T) -> furpc::Result<Self> {
        Ok(Self::record(HeaderRecord::native(
            HeaderId::new(id)?,
            value,
        )?))
    }
    pub fn actor(value: i64) -> furpc::Result<Self> {
        Ok(Self::record(header::actor(ActorId::new(value)?)?))
    }
    pub fn action(value: &str) -> furpc::Result<Self> {
        Ok(Self::record(header::action(value)?))
    }
    pub fn message_id(id: MessageId) -> furpc::Result<Self> {
        Self::native(header::MESSAGE_ID.get(), &id)
    }
    pub fn timeout(value: i64) -> furpc::Result<Self> {
        Self::native(TIMEOUT, &Scalar { value })
    }
    pub fn boolean(id: u16, value: bool) -> furpc::Result<Self> {
        Self::native(id, &Scalar { value })
    }
}

#[derive(Clone)]
pub struct Message {
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}
pub struct Properties {
    pub actor: i64,
    pub action: String,
    pub id: MessageId,
    pub relates_to: MessageId,
    pub expects_reply: bool,
}
impl Message {
    pub fn properties(&self) -> Result<Properties> {
        let mut headers = Headers::new();
        for header in &self.headers {
            headers.append(HeaderRecord::from_encoded(
                HeaderId::new(header.id)?,
                header.value.clone(),
            )?)?;
        }
        let common = headers.effective();
        Ok(Properties {
            actor: common.actor.get(),
            action: String::from_utf16(common.action())?,
            id: common.message_id,
            relates_to: common.relates_to,
            expects_reply: common.expects_reply,
        })
    }
    pub fn listen_instance(id: MessageId, value: &ListenInstance) -> Result<Self> {
        Ok(Self {
            headers: vec![Header::actor(1)?, Header::message_id(id)?],
            body: fubst::serde::to_vec(value)?,
        })
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct ListenInstance {
    pub address: String,
    pub instance: u64,
    pub nonce: Guid,
}
impl ListenInstance {
    pub fn decode(body: &[u8]) -> Result<Self> {
        Ok(fubst::serde::from_slice(body)?)
    }
}

pub async fn read_message(reader: &mut (impl AsyncRead + Unpin)) -> Result<Message> {
    let frame = Frame::read(reader).await?.ok_or(furpc::Error::Closed)?;
    Ok(Message {
        headers: frame
            .headers
            .records()
            .map(|header| Header {
                id: header.id().get(),
                value: header.value().to_vec(),
            })
            .collect(),
        body: frame.body().to_vec(),
    })
}

pub async fn write_message(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &Message,
) -> Result<()> {
    let header_bytes: usize = message
        .headers
        .iter()
        .map(|header| 4 + header.value.len())
        .sum();
    let size = u32::try_from(12 + header_bytes + message.body.len())?;
    let mut bytes = Vec::with_capacity(size as usize);
    bytes.extend_from_slice(&size.to_le_bytes());
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend_from_slice(&u16::try_from(header_bytes)?.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    for header in &message.headers {
        bytes.extend_from_slice(&header.id.to_le_bytes());
        bytes.extend_from_slice(&u16::try_from(header.value.len())?.to_le_bytes());
        bytes.extend_from_slice(&header.value);
    }
    bytes.extend_from_slice(&message.body);
    writer.write_all(&bytes).await?;
    Ok(())
}
