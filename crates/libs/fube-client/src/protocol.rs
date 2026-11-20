use fube_transport::message::Message;
use fubst::serde::{from_fields, from_slice, to_vec};
use fubst::Decoder;
use serde::Deserialize;

use crate::{Error, GatewayDescription, NamingUri, NodeId, Result};

// ClientServerTransport::DeleteNameHeader: public deletion is explicit.
pub(crate) const DELETE_NAME_HEADER: u16 = 0x8033;

pub(crate) fn encode_name(name: &str) -> Result<Vec<u8>> {
    Ok(to_vec(&NamingUri::parse(name)?)?)
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

pub(crate) fn check_reply(message: &Message, actions: &[&str]) -> Result<()> {
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

#[derive(Deserialize)]
struct PingNodeInstance(NodeId, u64);
#[derive(Deserialize)]
struct PingGateway(String, PingNodeInstance, String);
#[derive(Deserialize)]
pub(crate) struct PingReplyBody {
    gateway: PingGateway,
}
impl PingReplyBody {
    pub(crate) fn into_gateway(self) -> std::result::Result<GatewayDescription, &'static str> {
        let PingGateway(address, PingNodeInstance(NodeId { high, low }, node_instance), node_name) =
            self.gateway;
        if address.is_empty() || node_name.is_empty() {
            return Err("gateway address and node name must be nonempty");
        }
        Ok(GatewayDescription {
            address,
            node_id: (u128::from(high) << 64) | u128::from(low),
            node_instance,
            node_name,
        })
    }
}

#[derive(Deserialize)]
pub(crate) struct GatewayFailureBody {
    pub(crate) code: i64,
    #[serde(deserialize_with = "crate::native::trailing_default::deserialize")]
    pub(crate) diagnostic: String,
}

#[derive(Deserialize)]
pub(crate) struct NameExistsReplyBody {
    pub exists: bool,
    _user_service_state: i64,
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
                matches!(NamingUri::parse(input), Err(Error::InvalidInput(_))),
                "{input:?}"
            );
        }
        assert!(NamingUri::parse(&format!("fabric:/{}", "a".repeat(4088))).is_ok());
        assert!(matches!(
            NamingUri::parse(&format!("fabric:/{}", "a".repeat(4089))),
            Err(Error::InvalidInput(_))
        ));
    }
}
