use serde::Deserialize;

use crate::{GatewayDescription, NodeId};

// ClientServerTransport::DeleteNameHeader: public deletion is explicit.
pub(crate) const DELETE_NAME_HEADER: u16 = 0x8033;

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

#[cfg(test)]
mod tests {
    use crate::{Error, NamingUri};

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
