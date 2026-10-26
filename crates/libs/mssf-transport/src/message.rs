use crate::{
    frame::{Frame, FIXED_HEADER_SIZE, MAX_FRAME_SIZE, MAX_HEADER_SIZE},
    serialization::{
        serde::{from_slice, to_fields, to_vec, Guid},
        Decoder, Encoder,
    },
    Error, Result,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Scalar<T> {
    value: T,
}
#[derive(Serialize)]
struct Fields2<A, B>(A, B);

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    /// Native GUID field order, not a UUID's network byte order.
    #[serde(with = "crate::serialization::serde::guid")]
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
        Self::serialized(ACTOR, &Scalar { value: actor })
    }

    pub fn action(action: &str) -> Result<Self> {
        Self::serialized(ACTION, &Scalar { value: action })
    }

    pub fn message_id(id: MessageId) -> Result<Self> {
        Self::id_header(MESSAGE_ID, id)
    }

    pub fn relates_to(id: MessageId) -> Result<Self> {
        Self::id_header(RELATES_TO, id)
    }

    fn id_header(kind: u16, id: MessageId) -> Result<Self> {
        Self::serialized(kind, &id)
    }

    pub fn boolean(kind: u16, value: bool) -> Result<Self> {
        Self::serialized(kind, &Scalar { value })
    }

    pub fn timeout(ticks: i64) -> Result<Self> {
        Self::serialized(TIMEOUT, &Scalar { value: ticks })
    }

    pub fn activity(guid: [u8; 16]) -> Result<Self> {
        Self::serialized(
            ACTIVITY,
            &Scalar {
                value: Fields2(Guid(guid), 0u64),
            },
        )
    }

    pub fn version(major: i64, minor: i64) -> Result<Self> {
        Self::serialized(CLIENT_VERSION, &Fields2(major, minor))
    }

    fn serialized<T: Serialize>(id: u16, value: &T) -> Result<Self> {
        Self::object(id, |encoder| to_fields(encoder, value))
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenInstance {
    pub address: String,
    pub instance: u64,
    #[serde(with = "crate::serialization::serde::guid")]
    pub nonce: [u8; 16],
}

impl ListenInstance {
    pub fn encode(&self) -> Result<Vec<u8>> {
        to_vec(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrated_payloads_match_independent_pre_serde_schemas() {
        fn compare(header: Header, fields: impl FnOnce(&mut Encoder) -> Result<()>) {
            assert_eq!(header, Header::object(header.id, fields).unwrap());
        }
        for guid in [[0; 16], std::array::from_fn(|i| (i + 1) as u8)] {
            for index in [0, u32::MAX] {
                let id = MessageId { guid, index };
                compare(Header::message_id(id).unwrap(), |e| {
                    e.guid(&guid);
                    e.u32(index);
                    Ok(())
                });
                compare(Header::relates_to(id).unwrap(), |e| {
                    e.guid(&guid);
                    e.u32(index);
                    Ok(())
                });
            }
            compare(Header::activity(guid).unwrap(), |e| {
                e.object(|e| {
                    e.guid(&guid);
                    e.u64(0);
                    Ok(())
                })
            });
            for address in ["", "node🦀é"] {
                for instance in [0, u64::MAX] {
                    let value = ListenInstance {
                        address: address.into(),
                        instance,
                        nonce: guid,
                    };
                    let mut old = Encoder::new();
                    old.object(|e| {
                        e.string(address)?;
                        e.u64(instance);
                        e.guid(&guid);
                        Ok(())
                    })
                    .unwrap();
                    let bytes = value.encode().unwrap();
                    assert_eq!(bytes, old.finish());
                    assert_eq!(ListenInstance::decode(&bytes).unwrap(), value);
                }
            }
        }
        for value in [0, -1, i64::MIN, i64::MAX] {
            compare(Header::actor(value).unwrap(), |e| {
                e.i64(value);
                Ok(())
            });
            compare(Header::timeout(value).unwrap(), |e| {
                e.i64(value);
                Ok(())
            });
            compare(Header::version(value, 1).unwrap(), |e| {
                e.i64(value);
                e.i64(1);
                Ok(())
            });
        }
        let largest_action = "a".repeat(32759);
        for value in ["", "Action🦀é", largest_action.as_str()] {
            compare(Header::action(value).unwrap(), |e| e.string(value));
        }
        for kind in [EXPECTS_REPLY, HIGH_PRIORITY, UNCORRELATED_REPLY, 0x8033] {
            for value in [false, true] {
                compare(Header::boolean(kind, value).unwrap(), |e| {
                    e.boolean(value);
                    Ok(())
                });
            }
        }
        // The header-record size bound still applies after successful native string encoding.
        for length in [32760, 32768] {
            assert!(matches!(
                Header::action(&"a".repeat(length)),
                Err(Error::Protocol(_))
            ));
        }
    }

    #[test]
    fn public_records_serialize_directly_with_native_object_framing() {
        for guid in [[0; 16], std::array::from_fn(|i| (i + 1) as u8)] {
            for index in [0, u32::MAX] {
                let id = MessageId { guid, index };
                let bytes = to_vec(&id).unwrap();
                let mut native = Encoder::new();
                native
                    .object(|e| {
                        id.encode(e);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(bytes, native.finish());
                assert_eq!(from_slice::<MessageId>(&bytes).unwrap(), id);
                assert_eq!(Header::message_id(id).unwrap().value, bytes);
                assert_eq!(Header::relates_to(id).unwrap().value, bytes);

                let mut inline = Encoder::new();
                id.encode(&mut inline);
                let inline = inline.finish();
                let mut decoder = Decoder::new(&inline);
                assert_eq!(MessageId::decode(&mut decoder).unwrap(), id);
                decoder.finish().unwrap();
                for end in 0..bytes.len() {
                    assert!(from_slice::<MessageId>(&bytes[..end]).is_err());
                }
                let mut extra = bytes;
                extra.push(0);
                assert!(from_slice::<MessageId>(&extra).is_err());
            }
            for address in ["", "node🦀é"] {
                for instance in [0, u64::MAX] {
                    let value = ListenInstance {
                        address: address.into(),
                        instance,
                        nonce: guid,
                    };
                    let bytes = to_vec(&value).unwrap();
                    assert_eq!(bytes, value.encode().unwrap());
                    assert_eq!(from_slice::<ListenInstance>(&bytes).unwrap(), value);
                    assert_eq!(ListenInstance::decode(&bytes).unwrap(), value);
                    for end in 0..bytes.len() {
                        assert!(from_slice::<ListenInstance>(&bytes[..end]).is_err());
                    }
                    let mut extra = bytes;
                    extra.push(0);
                    assert!(from_slice::<ListenInstance>(&extra).is_err());
                }
            }
        }
    }

    #[test]
    fn header_record_and_area_bounds_are_exact() {
        let mut message = Message {
            headers: vec![Header {
                id: 0x7ffe,
                value: vec![0xff; MAX_HEADER_SIZE - 4],
            }],
            body: vec![],
        };
        let frame = message.to_frame().unwrap();
        assert_eq!(frame.headers.len(), MAX_HEADER_SIZE);
        assert_eq!(Message::from_frame(frame.clone()).unwrap(), message);
        let mut oversized = frame;
        oversized.headers.push(0);
        assert!(Message::from_frame(oversized).is_err());
        message.headers[0].value.push(0);
        assert!(message.to_frame().is_err());
        for size in [0, u16::MAX as usize, u16::MAX as usize + 1] {
            message.headers[0].value = vec![0xff; size];
            assert!(message.to_frame().is_err());
        }
        message.headers = vec![
            Header {
                id: 0x7ffe,
                value: vec![0xff],
            };
            MAX_HEADER_COUNT
        ];
        assert!(message.to_frame().is_ok());
        message.headers.push(message.headers[0].clone());
        assert!(message.to_frame().is_err());
    }

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
