use mssf_transport::{
    message::Message,
    serialization::{Decoder, Encoder},
};

use crate::{Error, GatewayDescription, Result};

// ClientServerTransport::DeleteNameHeader: public deletion is explicit.
pub(crate) const DELETE_NAME_HEADER: u16 = 0x8033;

pub(crate) fn encode_name(name: &str) -> Result<Vec<u8>> {
    if name.len() > 4096 {
        return Err(Error::InvalidInput("naming URI exceeds 4096 bytes"));
    }
    let suffix = name
        .strip_prefix("fabric:/")
        .ok_or(Error::InvalidInput("naming URI must start with fabric:/"))?;
    let segments: Vec<_> = suffix.split('/').collect();
    if segments.iter().any(|segment| {
        segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    }) {
        return Err(Error::InvalidInput(
            "naming URI requires nonempty ASCII alphanumeric, underscore or hyphen segments",
        ));
    }
    let mut encoder = Encoder::new();
    encoder.object(|encoder| {
        encoder.i64(1);
        encoder.string("fabric")?;
        encoder.string("")?;
        encoder.i64(0);
        encoder.string("")?;
        encoder.i32(-1);
        encoder.string(&name["fabric:".len()..])?;
        encoder.string("")?;
        encoder.string("")?;
        encoder.u32(segments.len() as u32);
        for segment in segments {
            encoder.string(segment)?;
        }
        Ok(())
    })?;
    Ok(encoder.finish())
}

pub(crate) fn check_failure(message: &Message) -> Result<()> {
    let properties = message.properties()?;
    if let Some(fault) = properties.fault.filter(|fault| fault.code != 0) {
        let mut code = fault.code;
        let mut diagnostic = String::new();
        if fault.has_body {
            let mut decoder = Decoder::new(&message.body);
            let details = decoder.object(|decoder| {
                decoder.object(|decoder| Ok((decoder.i64()?, decoder.string()?)))
            });
            match details.and_then(|details| decoder.finish().map(|()| details)) {
                Ok((detail_code, text)) if !text.is_empty() => {
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
        let text = decoder.object(|decoder| {
            code = Some(decoder.i64()?);
            decoder.string()
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
    let mut decoder = Decoder::new(&message.body);
    let gateway = decoder.object(|decoder| {
        decoder.object(|decoder| {
            let address = decoder.string()?;
            let (node_id, node_instance) = decoder.object(|decoder| {
                let bytes = decoder.byte_array()?;
                if bytes.len() != 16 {
                    return Err(Error::Protocol("node ID must contain exactly 16 bytes"));
                }
                let high = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                let low = u64::from_le_bytes(bytes[8..].try_into().unwrap());
                Ok(((u128::from(high) << 64) | u128::from(low), decoder.u64()?))
            })?;
            let node_name = decoder.string()?;
            if address.is_empty() || node_name.is_empty() {
                return Err(Error::Protocol(
                    "gateway address and node name must be nonempty",
                ));
            }
            Ok(GatewayDescription {
                address,
                node_id,
                node_instance,
                node_name,
            })
        })
    })?;
    decoder.finish()?;
    Ok(gateway)
}

pub(crate) fn decode_name_exists(message: &Message) -> Result<bool> {
    check_reply(message, &["NameOperationReply"])?;
    let mut decoder = Decoder::new(&message.body);
    let exists = decoder.object(|decoder| {
        let exists = decoder.boolean()?;
        let _user_service_state = decoder.i64()?;
        Ok(exists)
    })?;
    decoder.finish()?;
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
