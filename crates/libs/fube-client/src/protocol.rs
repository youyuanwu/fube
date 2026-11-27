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

crate::native::lossless_enum! {
    /// Naming::UserServiceState at native pin 3988b451.
    enum NativeUserServiceState {
        Invalid = 0,
        None = 1,
        Creating = 2,
        Deleting = 3,
        Created = 4,
        Updating = 5,
        ForceDeleting = 6,
    }
}

#[derive(Deserialize)]
pub(crate) struct NameExistsReplyBody {
    pub exists: bool,
    _user_service_state: NativeUserServiceState,
}

#[cfg(test)]
mod tests {
    use super::{NameExistsReplyBody, NativeUserServiceState};
    use crate::{Error, NamingUri};

    #[test]
    fn name_exists_metadata_retains_known_and_unknown_signed_states() {
        use NativeUserServiceState::*;

        for (code, state) in [
            (0, Invalid),
            (1, None),
            (2, Creating),
            (3, Deleting),
            (4, Created),
            (5, Updating),
            (6, ForceDeleting),
            (7, Unrecognized(7)),
            (-1, Unrecognized(-1)),
            (i64::MIN, Unrecognized(i64::MIN)),
            (i64::MAX, Unrecognized(i64::MAX)),
        ] {
            for exists in [false, true] {
                let reply: NameExistsReplyBody = serde_json::from_value(serde_json::json!({
                    "exists": exists,
                    "_user_service_state": code,
                }))
                .unwrap();
                assert_eq!(reply.exists, exists);
                assert_eq!(reply._user_service_state, state);
                assert_eq!(
                    serde_json::to_value(reply._user_service_state).unwrap(),
                    serde_json::json!(code)
                );
            }
        }
    }

    #[test]
    fn name_exists_metadata_still_requires_a_signed_integer_state() {
        for state in [
            serde_json::Value::Null,
            serde_json::json!("Created"),
            serde_json::json!(true),
            serde_json::json!(4.5),
            serde_json::json!(u64::MAX),
        ] {
            assert!(
                serde_json::from_value::<NameExistsReplyBody>(serde_json::json!({
                    "exists": true,
                    "_user_service_state": state,
                }))
                .is_err()
            );
        }
        assert!(
            serde_json::from_value::<NameExistsReplyBody>(serde_json::json!({
                "exists": true,
            }))
            .is_err()
        );
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
