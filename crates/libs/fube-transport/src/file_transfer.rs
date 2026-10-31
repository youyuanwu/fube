//! Native v1.2 file-transfer semantic replies, distinct from transport replies.
use crate::{
    message::{Message, MessageId, ACTION, ACTIVITY, ACTOR, CLIENT_VERSION},
    serialization::Decoder,
    Error, Result,
};

pub const FILE_SEQUENCE: u16 = 0x803c;
pub const MAX_QUEUED_FILE_REPLIES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSequence {
    pub number: u64,
    pub buffer_size: u64,
}

#[derive(Debug)]
pub struct FileTransferReply {
    pub activity: [u8; 16],
    pub actor: i64,
    pub sequence: Option<FileSequence>,
    pub result: Result<()>,
}

/// Validate before handing off, including replies for obsolete upload GUIDs.
pub fn decode_reply(message: &Message) -> Result<FileTransferReply> {
    let properties = message.properties()?;
    if !matches!(properties.actor, 25 | 27)
        || !matches!(
            properties.action.as_str(),
            "ClientOperationSuccess" | "ClientOperationFailure"
        )
        || properties.relates_to != MessageId::default()
        || properties.expects_reply
        || properties.uncorrelated_reply
        || properties.fault.is_some()
    {
        return Err(Error::Protocol("invalid native file-transfer envelope"));
    }
    // A semantic reply may have neither MessageId nor RelatesTo.
    let mut seen = std::collections::HashSet::new();
    for header in &message.headers {
        if !seen.insert(header.id) {
            return Err(Error::Protocol("duplicate native file-transfer header"));
        }
    }
    let required = |id| {
        message
            .headers
            .iter()
            .find(|header| header.id == id)
            .ok_or(Error::Protocol("missing native file-transfer header"))
    };
    required(ACTOR)?;
    required(ACTION)?;
    if required(CLIENT_VERSION)?.read(|d| Ok((d.i64()?, d.i64()?)))? != (1, 2) {
        return Err(Error::Protocol(
            "native upload requires protocol version 1.2",
        ));
    }
    let activity = required(ACTIVITY)?.read(|d| {
        d.object(|d| {
            let guid = d.guid()?;
            if guid == [0; 16] || d.u64()? != 0 {
                return Err(Error::Protocol("invalid native upload Activity"));
            }
            Ok(guid)
        })
    })?;
    let sequence = if properties.actor == 25 {
        Some(required(FILE_SEQUENCE)?.read(|d| {
            let number = d.u64()?;
            if d.boolean()? {
                return Err(Error::Protocol("v1.2 acknowledgement has last-chunk flag"));
            }
            Ok(FileSequence {
                number,
                buffer_size: d.u64()?,
            })
        })?)
    } else {
        if seen.contains(&FILE_SEQUENCE) {
            return Err(Error::Protocol("commit reply has a sequence header"));
        }
        None
    };
    let result = if properties.action == "ClientOperationSuccess" {
        if !message.body.is_empty() {
            return Err(Error::Protocol("native upload success has a body"));
        }
        Ok(())
    } else {
        let mut decoder = Decoder::new(&message.body);
        let (code, diagnostic) = decoder.object(|d| Ok((d.i64()?, d.string()?)))?;
        decoder.finish()?;
        if code == 0 || i32::try_from(code).is_err() {
            return Err(Error::Protocol("invalid native upload failure code"));
        }
        Err(Error::Remote {
            code,
            message: diagnostic,
        })
    };
    Ok(FileTransferReply {
        activity,
        actor: properties.actor,
        sequence,
        result,
    })
}
