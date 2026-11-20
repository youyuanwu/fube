//! Independent clear-frame fixture. Injection disables checksums; reception
//! validates actual frames with the shared public-header observer.
use fube_rap_test_support::transport::relay::Frame;
use furpc::{header, ActorId, Body, BoxError, HeaderRecord, Headers, Message, MessageId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub const FIXED_HEADER_SIZE: usize = 12;
pub type Result<T> = std::result::Result<T, BoxError>;

#[derive(Default, Serialize, Deserialize)]
pub struct ListenInstance {
    pub address: String,
    pub instance: u64,
    #[serde(with = "fubst::serde::guid")]
    pub nonce: [u8; 16],
}

impl ListenInstance {
    pub fn decode(body: &Body) -> Result<Self> {
        let bytes: Vec<_> = body.segments().flatten().copied().collect();
        Ok(fubst::serde::from_slice(&bytes)?)
    }
}

pub struct Properties {
    pub actor: i64,
    pub action: String,
    pub id: MessageId,
}

pub struct Injection(Vec<u8>);
impl Injection {
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.clone())
    }
}

pub trait MessageExt {
    fn properties(&self) -> Result<Properties>;
    fn to_frame(&self) -> Result<Injection>;
    fn listen_instance(id: MessageId, value: &ListenInstance) -> Result<Message>;
}

impl MessageExt for Message {
    fn properties(&self) -> Result<Properties> {
        let common = self.headers().effective();
        Ok(Properties {
            actor: common.actor.get(),
            action: String::from_utf16(common.action())?,
            id: common.message_id,
        })
    }
    fn to_frame(&self) -> Result<Injection> {
        let header_bytes: usize = self
            .headers()
            .records()
            .map(|header| 4 + header.value().len())
            .sum();
        let size = u32::try_from(12 + header_bytes + self.body().len())?;
        let mut bytes = Vec::with_capacity(size as usize);
        bytes.extend_from_slice(&size.to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&u16::try_from(header_bytes)?.to_le_bytes());
        bytes.extend_from_slice(&[0; 4]);
        for header in self.headers().records() {
            bytes.extend_from_slice(&header.id().get().to_le_bytes());
            bytes.extend_from_slice(&u16::try_from(header.value().len())?.to_le_bytes());
            bytes.extend_from_slice(header.value());
        }
        for segment in self.body().segments() {
            bytes.extend_from_slice(segment);
        }
        Ok(Injection(bytes))
    }
    fn listen_instance(id: MessageId, value: &ListenInstance) -> Result<Message> {
        Ok(Message::builder()
            .append_header(header::actor(ActorId::new(1)?)?)
            .append_header(HeaderRecord::native(header::MESSAGE_ID, &id)?)
            .body(Body::from_bytes(fubst::serde::to_vec(value)?)?)
            .build()?)
    }
}

fn message(headers: Headers, body: Vec<u8>) -> Result<Message> {
    let mut builder = Message::builder();
    for header in headers.records() {
        builder = builder.append_header(header.to_owned());
    }
    Ok(builder.body(Body::from_bytes(body)?).build()?)
}

pub async fn read_message(reader: &mut (impl AsyncRead + Unpin)) -> Result<Message> {
    let frame = Frame::read(reader).await?.ok_or(furpc::Error::Closed)?;
    message(frame.headers.clone(), frame.body().to_vec())
}

pub async fn decode_message(bytes: &[u8]) -> Result<Message> {
    read_message(&mut &bytes[..]).await
}

pub async fn write_message(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &Message,
) -> Result<()> {
    writer.write_all(&message.to_frame()?.0).await?;
    Ok(())
}

pub fn validate_fixed(fixed: &[u8; FIXED_HEADER_SIZE]) -> Result<(usize, usize)> {
    let size = u32::from_le_bytes(fixed[..4].try_into()?) as usize;
    let headers = usize::from(u16::from_le_bytes([fixed[6], fixed[7]]));
    if !(12..=4 * 1024 * 1024).contains(&size) || 12 + headers > size || fixed[4] != 0 {
        return Err(furpc::Error::Protocol("invalid observed clear frame").into());
    }
    Ok((size, headers))
}
