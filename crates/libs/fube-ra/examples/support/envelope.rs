//! Mutable schema-fixture scratch. Native construction still uses public furpc.
#![allow(dead_code)]
use furpc::{Body, HeaderId, HeaderRecord, Message};
use serde::{Deserialize, Serialize};

pub const ACTOR: u16 = furpc::header::ACTOR.get();
pub const ACTION: u16 = furpc::header::ACTION.get();
pub const HIGH_PRIORITY: u16 = furpc::header::HIGH_PRIORITY.get();

#[derive(Clone, Debug)]
pub struct Header {
    pub id: u16,
    pub value: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
struct Scalar<T> {
    value: T,
}
impl From<HeaderRecord> for Header {
    fn from(value: HeaderRecord) -> Self {
        Self {
            id: value.id().get(),
            value: value.value().to_vec(),
        }
    }
}
impl Header {
    pub fn actor(value: i64) -> furpc::Result<Self> {
        furpc::header::actor(furpc::ActorId::new(value)?).map(Into::into)
    }
    pub fn action(value: &str) -> furpc::Result<Self> {
        furpc::header::action(value).map(Into::into)
    }
    pub fn boolean(id: u16, value: bool) -> furpc::Result<Self> {
        HeaderRecord::native(HeaderId::new(id)?, &Scalar { value }).map(Into::into)
    }
}
pub fn native(headers: Vec<Header>, body: Vec<u8>) -> furpc::Result<Message> {
    let mut builder = Message::builder().body(Body::from_bytes(body)?);
    for header in headers {
        builder = builder.append_header(HeaderRecord::from_encoded(
            HeaderId::new(header.id)?,
            header.value,
        )?);
    }
    builder.build()
}
pub fn headers(message: &Message) -> Vec<Header> {
    message
        .headers()
        .records()
        .map(|h| h.to_owned().into())
        .collect()
}
pub fn bytes(message: &Message) -> Vec<u8> {
    message.body().segments().flatten().copied().collect()
}
pub fn edit_headers(
    message: &mut Message,
    edit: impl FnOnce(&mut Vec<Header>),
) -> furpc::Result<()> {
    let mut values = headers(message);
    edit(&mut values);
    *message = native(values, bytes(message))?;
    Ok(())
}
pub fn edit_body(message: &mut Message, edit: impl FnOnce(&mut Vec<u8>)) {
    let mut body = bytes(message);
    edit(&mut body);
    *message = native(headers(message), body).unwrap();
}
pub struct Properties {
    pub actor: i64,
    pub action: String,
    pub high_priority: bool,
}
pub trait MessageExt {
    fn properties(&self) -> furpc::Result<Properties>;
}
impl MessageExt for Message {
    fn properties(&self) -> furpc::Result<Properties> {
        let value = self.headers().effective();
        Ok(Properties {
            actor: value.actor.get(),
            action: String::from_utf16(value.action())
                .map_err(|_| furpc::Error::Protocol("fixture action"))?,
            high_priority: value.high_priority,
        })
    }
}
