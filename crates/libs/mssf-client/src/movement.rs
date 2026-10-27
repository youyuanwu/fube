//! Constraint-respecting native FM control used to qualify reconfiguration.
//! Source: client/FabricClientImpl.cpp:1354-1393 and FM QueryHelper.cpp:16-60.
use crate::{
    query_arguments::validate_node_name,
    query_protocol::{Query, Reply},
    Error, Result,
};
use mssf_transport::{
    message::{Message, ACTION, ACTOR},
    serialization::serde::{from_slice, Pointer, RawTagged},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize)]
pub(crate) struct MovePrimary {
    #[serde(rename = "NodeName")]
    pub(crate) target: String,
    #[serde(rename = "PartitionId")]
    pub(crate) partition: Uuid,
}
impl Query for MovePrimary {
    type Output = ();
    type Reply = EmptyReply;
    const NAME: &'static str = "MovePrimary";
    const SUPPORTS_MAX_RESULTS: bool = false;
    fn validate_parameters(&self) -> Result<()> {
        validate_node_name(
            &self.target,
            "primary target must be an explicit native node",
        )?;
        if self.partition.is_nil() {
            return Err(Error::InvalidInput("primary move needs a partition"));
        }
        Ok(())
    }
}
pub(crate) struct EmptyReply;
#[derive(Deserialize)]
struct Empty {}
#[derive(Deserialize)]
struct Envelope {
    kind: i64,
    payload: Pointer<RawTagged<Empty>>,
    error: i64,
    paging: Pointer<RawTagged<Empty>>,
}
impl Reply for EmptyReply {
    type Record = ();
    type Output = ();
    fn decode(message: &Message, _: fn(()) -> Result<()>) -> Result<()> {
        crate::protocol::check_failure(message)?;
        let properties = message.properties()?;
        if (message.headers.iter().any(|h| h.id == ACTOR) && properties.actor != 5)
            || (message.headers.iter().any(|h| h.id == ACTION) && properties.action != "QueryReply")
        {
            return Err(Error::Protocol(
                "invalid native movement query reply envelope",
            ));
        }
        let result: Envelope = from_slice(&message.body)?;
        if result.kind != 0 || result.payload.0.is_some() || result.paging.0.is_some() {
            return Err(Error::Protocol(
                "movement reply is not an empty QueryResult",
            ));
        }
        if result.error != 0 {
            return Err(Error::Remote {
                code: result.error,
                message: "native primary movement".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_protocol::{decode_reply, tests::assert_request};
    use mssf_transport::serialization::Encoder;
    fn reply(kind: i64, error: i64, payload: bool, paging: bool) -> Message {
        let mut e = Encoder::new();
        e.object(|e| {
            e.i64(kind);
            e.pointer(payload.then_some(&[][..]), |_| Ok(()))?;
            e.i64(error);
            e.pointer(paging.then_some(&[][..]), |_| Ok(()))
        })
        .unwrap();
        Message {
            headers: vec![],
            body: e.finish(),
        }
    }
    #[test]
    fn movement_rejects_nonempty_wrong_header_and_native_errors() {
        for (kind, payload, paging) in [(1, false, false), (0, true, false), (0, false, true)] {
            assert!(decode_reply::<MovePrimary>(&reply(kind, 0, payload, paging)).is_err());
        }
        assert!(matches!(
            decode_reply::<MovePrimary>(&reply(0, -123, false, false)),
            Err(Error::Remote { code: -123, .. })
        ));
        for header in [
            mssf_transport::message::Header::actor(12).unwrap(),
            mssf_transport::message::Header::action("WrongAction").unwrap(),
        ] {
            let mut value = reply(0, 0, false, false);
            value.headers.push(header);
            assert!(decode_reply::<MovePrimary>(&value).is_err());
        }
    }
    #[test]
    fn movement_never_exposes_force_or_ignore_constraints_and_checks_empty_reply() {
        let query = MovePrimary {
            target: "N0020".into(),
            partition: "00112233-4455-6677-8899-aabbccddeeff".parse().unwrap(),
        };
        assert_request(
            &query,
            None,
            "MovePrimary",
            &[
                ("NodeName", "N0020"),
                ("PartitionId", "00112233-4455-6677-8899-aabbccddeeff"),
            ],
        );
        let mut encoder = Encoder::new();
        encoder
            .object(|e| {
                e.i64(0);
                e.pointer(None, |_| Ok(()))?;
                e.i64(0);
                e.pointer(None, |_| Ok(()))
            })
            .unwrap();
        let message = Message {
            headers: vec![],
            body: encoder.finish(),
        };
        decode_reply::<MovePrimary>(&message).unwrap();
        for cut in 0..message.body.len() {
            assert!(decode_reply::<MovePrimary>(&Message {
                headers: vec![],
                body: message.body[..cut].to_vec()
            })
            .is_err());
        }
        assert!(MovePrimary {
            target: "".into(),
            partition: query.partition
        }
        .validate_parameters()
        .is_err());
        assert!(MovePrimary {
            target: "N0020".into(),
            partition: Uuid::nil()
        }
        .validate_parameters()
        .is_err());
    }
}
