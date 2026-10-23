use mssf_transport::serialization::serde::{
    from_fields, from_slice, to_vec, ByteArray, CountedStrings, Untagged,
};
use mssf_transport::{message::Message, serialization::Decoder};
use serde::{Deserialize, Serialize};

use crate::{Error, GatewayDescription, NodeId, Result};

// ClientServerTransport::DeleteNameHeader: public deletion is explicit.
pub(crate) const DELETE_NAME_HEADER: u16 = 0x8033;

pub(crate) fn encode_name(name: &str) -> Result<Vec<u8>> {
    validate_name(name)?;
    to_vec(&Untagged(NamingUriWire {
        uri_type: 1,
        scheme: "fabric".into(),
        authority: String::new(),
        host_type: 0,
        host: String::new(),
        port: -1,
        path: name["fabric:".len()..].into(),
        query: String::new(),
        fragment: String::new(),
        segments: CountedStrings(
            name["fabric:/".len()..]
                .split('/')
                .map(str::to_owned)
                .collect(),
        ),
    }))
}

#[derive(Serialize, Deserialize)]
pub(crate) struct NamingUriWire {
    uri_type: i64,
    scheme: String,
    authority: String,
    host_type: i64,
    host: String,
    port: i32,
    path: String,
    query: String,
    fragment: String,
    segments: CountedStrings<Vec<String>>,
}
impl NamingUriWire {
    pub(crate) fn into_name(self) -> Result<String> {
        if self.uri_type != 1
            || self.scheme != "fabric"
            || !self.authority.is_empty()
            || self.host_type != 0
            || !self.host.is_empty()
            || self.port != -1
        {
            return Err(Error::Protocol("unsupported native naming URI profile"));
        }
        if !self.query.is_empty() || !self.fragment.is_empty() {
            return Err(Error::Protocol("naming URI has query or fragment"));
        }
        let value = format!("fabric:{}", self.path);
        validate_name(&value)
            .map_err(|_| Error::Protocol("unsupported native naming URI value"))?;
        if !self.path[1..]
            .split('/')
            .eq(self.segments.0.iter().map(String::as_str))
        {
            return Err(Error::Protocol("naming URI segments disagree with path"));
        }
        Ok(value)
    }
}

pub(crate) fn decode_node_id(ByteArray(bytes): ByteArray) -> Result<NodeId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Protocol("node ID must contain exactly 16 bytes"))?;
    Ok(NodeId {
        high: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        low: u64::from_le_bytes(bytes[8..].try_into().unwrap()),
    })
}

pub(crate) fn validate_name(name: &str) -> Result<()> {
    if name.len() > 4096 {
        return Err(Error::InvalidInput("naming URI exceeds 4096 bytes"));
    }
    let suffix = name
        .strip_prefix("fabric:/")
        .ok_or(Error::InvalidInput("naming URI must start with fabric:/"))?;
    if suffix.split('/').any(|segment| {
        segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    }) {
        return Err(Error::InvalidInput(
            "naming URI requires nonempty ASCII alphanumeric, underscore or hyphen segments",
        ));
    }
    Ok(())
}

pub(crate) fn check_failure(message: &Message) -> Result<()> {
    let properties = message.properties()?;
    if let Some(fault) = properties.fault.filter(|fault| fault.code != 0) {
        let mut code = fault.code;
        let mut diagnostic = String::new();
        if fault.has_body {
            #[derive(Deserialize)]
            struct CodeText(i64, String);
            #[derive(Deserialize)]
            struct Details {
                details: CodeText,
            }
            match from_slice::<Details>(&message.body) {
                Ok(Details {
                    details: CodeText(detail_code, text),
                }) if !text.is_empty() => {
                    // Native ErrorCodeDetails replaces the header only for a
                    // successfully decoded, nonempty diagnostic. Never erase
                    // a nonzero failure with a success code.
                    if detail_code != 0 {
                        code = detail_code;
                    }
                    diagnostic = text;
                }
                Ok(_) => {}
                Err(_) => diagnostic = "malformed optional fault details".into(),
            }
        }
        return Err(Error::Remote {
            code,
            message: diagnostic,
        });
    }
    if properties.action == "ClientOperationFailure" {
        let mut code = None;
        let mut decoder = Decoder::new(&message.body);
        #[derive(Deserialize)]
        struct FailureCode {
            code: i64,
        }
        #[derive(Deserialize)]
        struct Diagnostic {
            text: String,
        }
        let text = decoder.object(|decoder| {
            code = Some(from_fields::<FailureCode>(decoder)?.code);
            Ok(from_fields::<Diagnostic>(decoder)?.text)
        });
        let text = text.and_then(|text| decoder.finish().map(|()| text));
        return match code {
            Some(code) if code != 0 => Err(Error::Remote {
                code,
                message: text.unwrap_or_else(|_| "missing or malformed failure diagnostic".into()),
            }),
            _ => Err(Error::Protocol(
                "operation failure has no nonzero error code",
            )),
        };
    }
    Ok(())
}

fn check_reply(message: &Message, actions: &[&str]) -> Result<()> {
    check_failure(message)?;
    let properties = message.properties()?;
    if properties.actor != 5 {
        return Err(Error::Protocol("operation reply is not from NamingGateway"));
    }
    if !actions.contains(&properties.action.as_str()) {
        return Err(Error::Protocol("unexpected operation reply action"));
    }
    Ok(())
}

pub(crate) fn decode_ping(message: &Message) -> Result<GatewayDescription> {
    // Native NamingTcpMessage::GetPingReply uses PingRequest, not PingReply.
    check_reply(message, &["PingRequest"])?;
    match message.protocol_version()? {
        Some((1, minor)) if minor >= 1 => {}
        _ => {
            return Err(Error::Protocol(
                "ping reply requires protocol version 1.1 or newer 1.x",
            ))
        }
    }
    #[derive(Deserialize)]
    struct NodeInstance(ByteArray, u64);
    #[derive(Deserialize)]
    struct Gateway(String, NodeInstance, String);
    #[derive(Deserialize)]
    struct Reply {
        gateway: Gateway,
    }
    let Reply {
        gateway: Gateway(address, NodeInstance(ByteArray(bytes), node_instance), node_name),
    } = from_slice(&message.body)?;
    let NodeId { high, low } = decode_node_id(ByteArray(bytes))?;
    if address.is_empty() || node_name.is_empty() {
        return Err(Error::Protocol(
            "gateway address and node name must be nonempty",
        ));
    }
    Ok(GatewayDescription {
        address,
        node_id: (u128::from(high) << 64) | u128::from(low),
        node_instance,
        node_name,
    })
}

pub(crate) fn decode_name_exists(message: &Message) -> Result<bool> {
    check_reply(message, &["NameOperationReply"])?;
    #[derive(Deserialize)]
    struct Reply(bool, i64);
    let Reply(exists, _user_service_state) = from_slice(&message.body)?;
    Ok(exists)
}

pub(crate) fn decode_name_write(message: &Message) -> Result<()> {
    check_reply(message, &["NameOperationReply"])?;
    if !message.body.is_empty() {
        return Err(Error::Protocol("name write success body must be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssf_transport::serialization::Encoder;

    #[test]
    fn naming_uri_serde_bytes_match_the_manual_native_schema() {
        for name in [
            "fabric:/App",
            "fabric:/App_1/Service-2",
            &format!("fabric:/{}", "a".repeat(4088)),
        ] {
            let mut encoder = Encoder::new();
            encoder
                .object(|e| {
                    e.i64(1);
                    e.string("fabric")?;
                    e.string("")?;
                    e.i64(0);
                    e.string("")?;
                    e.i32(-1);
                    e.string(&name["fabric:".len()..])?;
                    e.string("")?;
                    e.string("")?;
                    let segments: Vec<_> = name["fabric:/".len()..].split('/').collect();
                    e.u32(segments.len() as u32);
                    for segment in segments {
                        e.string(segment)?;
                    }
                    Ok(())
                })
                .unwrap();
            let bytes = encode_name(name).unwrap();
            assert_eq!(bytes, encoder.finish());
            assert_eq!(
                from_slice::<Untagged<NamingUriWire>>(&bytes)
                    .unwrap()
                    .0
                    .into_name()
                    .unwrap(),
                name
            );
        }
    }

    #[test]
    fn uri_rejects_every_unsupported_grammar_class() {
        for input in [
            "",
            "fabric:",
            "fabric:/",
            "Fabric:/app",
            "http:/app",
            "fabric://app",
            "fabric:/app/",
            "fabric:/app//child",
            "fabric:/.",
            "fabric:/..",
            "fabric:/app.child",
            "fabric:/a%20b",
            "fabric:/a?query",
            "fabric:/a#f",
            " fabric:/app",
            "fabric:/a b",
            "fabric:/a\n",
            "fabric:/é",
            "fabric:/a+b",
            "fabric:/a$b",
            "fabric:/a|b",
            "fabric:/a~b",
            "fabric:/a:b",
            "fabric:/a\\b",
        ] {
            assert!(
                matches!(encode_name(input), Err(Error::InvalidInput(_))),
                "{input:?}"
            );
        }
        assert!(encode_name(&format!("fabric:/{}", "a".repeat(4088))).is_ok());
        assert!(matches!(
            encode_name(&format!("fabric:/{}", "a".repeat(4089))),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn uri_preserves_path_case_and_segment_spelling() {
        let bytes = encode_name("fabric:/App_1/Service-2").unwrap();
        let mut decoder = Decoder::new(&bytes);
        decoder
            .object(|decoder| {
                assert_eq!(decoder.i64()?, 1);
                assert_eq!(decoder.string()?, "fabric");
                assert_eq!(decoder.string()?, "");
                assert_eq!(decoder.i64()?, 0);
                assert_eq!(decoder.string()?, "");
                assert_eq!(decoder.i32()?, -1);
                assert_eq!(decoder.string()?, "/App_1/Service-2");
                assert_eq!(decoder.string()?, "");
                assert_eq!(decoder.string()?, "");
                assert_eq!(decoder.u32()?, 2);
                assert_eq!(decoder.string()?, "App_1");
                assert_eq!(decoder.string()?, "Service-2");
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
    }
}
