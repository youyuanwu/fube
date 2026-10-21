use crate::{
    frame::{Frame, FIXED_HEADER_SIZE, MAX_FRAME_SIZE, MAX_HEADER_SIZE},
    serialization::{Decoder, Encoder},
    Error, Result,
};

pub const ACTOR: u16 = 0x8001;
pub const ACTION: u16 = 0x8002;
pub const MESSAGE_ID: u16 = 0x8003;
pub const RELATES_TO: u16 = 0x8004;
pub const EXPECTS_REPLY: u16 = 0x8005;
pub const FAULT: u16 = 0x8007;
pub const HIGH_PRIORITY: u16 = 0x8009;
pub const ACTIVITY: u16 = 0x8020;
pub const TIMEOUT: u16 = 0x8023;
pub const CLIENT_VERSION: u16 = 0x8025;
pub const UNCORRELATED_REPLY: u16 = 0x803e;
pub const MAX_HEADER_COUNT: usize = 4096;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessageId {
    /// Native GUID field order, not a UUID's network byte order.
    pub guid: [u8; 16],
    pub index: u32,
}

impl MessageId {
    pub fn encode(&self, encoder: &mut Encoder) {
        encoder.guid(&self.guid);
        encoder.u32(self.index);
    }

    pub fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            guid: decoder.guid()?,
            index: decoder.u32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub id: u16,
    pub value: Vec<u8>,
}

impl Header {
    pub fn object(id: u16, fields: impl FnOnce(&mut Encoder) -> Result<()>) -> Result<Self> {
        let mut encoder = Encoder::new();
        encoder.object(fields)?;
        let value = encoder.finish();
        if value.len() > u16::MAX as usize {
            return Err(Error::Protocol("serialized header exceeds limit"));
        }
        Ok(Self { id, value })
    }

    pub fn actor(actor: i64) -> Result<Self> {
        Self::object(ACTOR, |encoder| {
            encoder.i64(actor);
            Ok(())
        })
    }

    pub fn action(action: &str) -> Result<Self> {
        Self::object(ACTION, |encoder| encoder.string(action))
    }

    pub fn message_id(id: MessageId) -> Result<Self> {
        Self::id_header(MESSAGE_ID, id)
    }

    pub fn relates_to(id: MessageId) -> Result<Self> {
        Self::id_header(RELATES_TO, id)
    }

    fn id_header(kind: u16, id: MessageId) -> Result<Self> {
        Self::object(kind, |encoder| {
            id.encode(encoder);
            Ok(())
        })
    }

    pub fn boolean(kind: u16, value: bool) -> Result<Self> {
        Self::object(kind, |encoder| {
            encoder.boolean(value);
            Ok(())
        })
    }

    pub fn timeout(ticks: i64) -> Result<Self> {
        Self::object(TIMEOUT, |encoder| {
            encoder.i64(ticks);
            Ok(())
        })
    }

    pub fn activity(guid: [u8; 16]) -> Result<Self> {
        Self::object(ACTIVITY, |encoder| {
            encoder.object(|encoder| {
                encoder.guid(&guid);
                encoder.u64(0);
                Ok(())
            })
        })
    }

    pub fn version(major: i64, minor: i64) -> Result<Self> {
        Self::object(CLIENT_VERSION, |encoder| {
            encoder.i64(major);
            encoder.i64(minor);
            Ok(())
        })
    }

    pub fn read<T>(&self, fields: impl FnOnce(&mut Decoder<'_>) -> Result<T>) -> Result<T> {
        let mut decoder = Decoder::new(&self.value);
        let result = decoder.object(fields)?;
        decoder.finish()?;
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fault {
    pub code: i64,
    pub has_body: bool,
}

#[derive(Debug, Default)]
pub struct Properties {
    pub actor: i64,
    pub action: String,
    pub id: MessageId,
    pub relates_to: MessageId,
    pub expects_reply: bool,
    pub high_priority: bool,
    pub uncorrelated_reply: bool,
    pub fault: Option<Fault>,
}

impl Message {
    pub fn from_frame(frame: Frame) -> Result<Self> {
        if frame.headers.len() > MAX_HEADER_SIZE
            || frame
                .headers
                .len()
                .checked_add(frame.body.len())
                .is_none_or(|size| size > MAX_FRAME_SIZE - FIXED_HEADER_SIZE)
        {
            return Err(Error::Protocol("frame or header area exceeds limit"));
        }
        let mut headers = Vec::new();
        let mut input = frame.headers.as_slice();
        let mut count = 0;
        while !input.is_empty() {
            if input.len() < 4 || count == MAX_HEADER_COUNT {
                return Err(Error::Protocol("invalid header records or count limit"));
            }
            count += 1;
            let id = u16::from_le_bytes(input[..2].try_into().unwrap());
            let size = u16::from_le_bytes(input[2..4].try_into().unwrap()) as usize;
            if size == 0 || size > input.len() - 4 {
                return Err(Error::Protocol("invalid header record length"));
            }
            if id != 0x8000 {
                headers.push(Header {
                    id,
                    value: input[4..4 + size].to_vec(),
                });
            }
            input = &input[4 + size..];
        }
        let message = Self {
            headers,
            body: frame.body,
        };
        message.properties()?;
        Ok(message)
    }

    pub fn to_frame(&self) -> Result<Frame> {
        if self.headers.len() > MAX_HEADER_COUNT {
            return Err(Error::Protocol("header count exceeds limit"));
        }
        let mut headers = Vec::new();
        for header in &self.headers {
            let size = header.value.len();
            if size == 0 || size > u16::MAX as usize || headers.len() + 4 + size > MAX_HEADER_SIZE {
                return Err(Error::Protocol("header size exceeds limit"));
            }
            headers.extend_from_slice(&header.id.to_le_bytes());
            headers.extend_from_slice(&(size as u16).to_le_bytes());
            headers.extend_from_slice(&header.value);
        }
        Ok(Frame {
            headers,
            body: self.body.clone(),
        })
    }

    pub fn properties(&self) -> Result<Properties> {
        let mut properties = Properties::default();
        for header in &self.headers {
            match header.id {
                ACTOR => properties.actor = header.read(|decoder| decoder.i64())?,
                ACTION => properties.action = header.read(|decoder| decoder.string())?,
                MESSAGE_ID => properties.id = header.read(MessageId::decode)?,
                RELATES_TO => properties.relates_to = header.read(MessageId::decode)?,
                EXPECTS_REPLY => {
                    properties.expects_reply = header.read(|decoder| decoder.boolean())?
                }
                HIGH_PRIORITY => {
                    properties.high_priority = header.read(|decoder| decoder.boolean())?
                }
                UNCORRELATED_REPLY => {
                    properties.uncorrelated_reply = header.read(|decoder| decoder.boolean())?
                }
                FAULT => {
                    properties.fault = Some(header.read(|decoder| {
                        Ok(Fault {
                            code: decoder.i64()?,
                            has_body: decoder.boolean()?,
                        })
                    })?)
                }
                _ => {}
            }
        }
        Ok(properties)
    }

    pub fn protocol_version(&self) -> Result<Option<(i64, i64)>> {
        self.headers
            .iter()
            .find(|header| header.id == CLIENT_VERSION)
            .map(|header| header.read(|decoder| Ok((decoder.i64()?, decoder.i64()?))))
            .transpose()
    }

    pub fn listen_instance(id: MessageId, instance: &ListenInstance) -> Result<Self> {
        Ok(Self {
            headers: vec![
                Header::message_id(id)?,
                Header::actor(1)?,
                Header::boolean(HIGH_PRIORITY, true)?,
            ],
            body: instance.encode()?,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ListenInstance {
    pub address: String,
    pub instance: u64,
    pub nonce: [u8; 16],
}

impl ListenInstance {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        encoder.object(|encoder| {
            encoder.string(&self.address)?;
            encoder.u64(self.instance);
            encoder.guid(&self.nonce);
            Ok(())
        })?;
        Ok(encoder.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        let instance = decoder.object(|decoder| {
            Ok(Self {
                address: decoder.string()?,
                instance: decoder.u64()?,
                nonce: decoder.guid()?,
            })
        })?;
        decoder.finish()?;
        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_control_round_trip_preserves_fields() {
        let message = Message::listen_instance(
            MessageId {
                guid: [1; 16],
                index: 1,
            },
            &ListenInstance::default(),
        )
        .unwrap();
        let bytes = message.to_frame().unwrap().encode().unwrap();
        let decoded = Message::from_frame(Frame::decode(&bytes).unwrap()).unwrap();
        assert_eq!(decoded, message);
        let properties = decoded.properties().unwrap();
        assert_eq!(properties.actor, 1);
        assert!(properties.high_priority);
        assert!(properties.action.is_empty());
        assert_eq!(
            ListenInstance::decode(&decoded.body).unwrap(),
            ListenInstance::default()
        );
    }

    #[test]
    fn common_duplicates_are_last_visible_typed_lookup_is_first() {
        let message = Message {
            headers: vec![
                Header::actor(1).unwrap(),
                Header {
                    id: 0x8000,
                    value: vec![0xff],
                },
                Header {
                    id: 0x7ffe,
                    value: vec![0xff],
                },
                Header::actor(5).unwrap(),
                Header::version(1, 1).unwrap(),
                Header::version(1, 2).unwrap(),
            ],
            body: vec![],
        };
        let parsed = Message::from_frame(message.to_frame().unwrap()).unwrap();
        assert_eq!(parsed.properties().unwrap().actor, 5);
        assert_eq!(parsed.protocol_version().unwrap(), Some((1, 1)));
        assert!(!parsed.headers.iter().any(|header| header.id == 0x8000));
        let mut malformed = parsed;
        malformed.headers.push(Header {
            id: ACTOR,
            value: vec![0xff],
        });
        assert!(malformed.properties().is_err());
    }

    #[test]
    fn malformed_record_lengths_fail() {
        for headers in [vec![0], vec![0, 0x80, 0, 0], vec![1, 0x80, 10, 0, 0]] {
            assert!(Message::from_frame(Frame {
                headers,
                body: vec![]
            })
            .is_err());
        }
    }

    #[test]
    fn header_count_limit_includes_deleted_records() {
        let frame = Frame {
            headers: [0x00, 0x80, 0x01, 0x00, 0xff].repeat(MAX_HEADER_COUNT),
            body: vec![],
        };
        assert!(Message::from_frame(frame.clone()).is_ok());
        let mut oversized = frame;
        oversized.headers.extend_from_slice(&[0, 0x80, 1, 0, 0xff]);
        assert!(Message::from_frame(oversized).is_err());
    }
}
