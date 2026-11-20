//! Source-derived notification schemas. No endpoint description enrichment.
use fubst::serde::{CountedValues, Guid, ObjectArray, Pointer};
use furpc::{Body, Response};
use serde::{Deserialize, Serialize};

use crate::{
    furpc_client::{action_is, check_gateway_actor, check_gateway_failure},
    resolution::Generation,
    resolution_protocol::TableEntry,
    FurpcClientError, GatewayDescription, NamingUri, NodeId,
};

type Result<T> = std::result::Result<T, FurpcClientError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Interval {
    pub start: i64,
    pub end: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Versions {
    pub ranges: ObjectArray<Vec<Interval>>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Flags {
    pub bits: u32,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Filter {
    pub id: u64,
    pub name: NamingUri,
    pub flags: Flags,
}
#[derive(Serialize)]
pub(crate) struct Connect<'a> {
    pub client: &'a str,
    pub generation: Generation,
    pub versions: Pointer<Versions>,
    pub filters: CountedValues<Vec<Pointer<Filter>>>,
}
#[derive(Serialize)]
pub(crate) struct Register<'a> {
    pub client: &'a str,
    pub filter: Pointer<Filter>,
}
#[derive(Serialize)]
pub(crate) struct Unregister<'a> {
    pub client: &'a str,
    pub id: u64,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct NodeInstance {
    pub id: NodeId,
    pub instance: u64,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Gateway {
    pub address: String,
    pub node: NodeInstance,
    pub name: String,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Connected {
    pub generation: Generation,
    pub last_deleted: i64,
    pub gateway: Gateway,
}
impl Connected {
    pub fn validate(&self) -> Result<()> {
        // Native notification gateway descriptions intentionally omit the address.
        if self.gateway.name.is_empty() {
            return Err(FurpcClientError::Protocol(
                "empty notification gateway identity",
            ));
        }
        Ok(())
    }

    pub fn description(&self) -> GatewayDescription {
        GatewayDescription {
            address: self.gateway.address.clone(),
            node_id: (u128::from(self.gateway.node.id.high) << 64)
                | u128::from(self.gateway.node.id.low),
            node_instance: self.gateway.node.instance,
            node_name: self.gateway.name.clone(),
        }
    }
}
pub(crate) fn connected(response: &Response<Body>) -> Result<()> {
    check_reply(response, "NotificationClientConnectionReply")
}
pub(crate) fn registered(response: &Response<Body>) -> Result<()> {
    check_reply(response, "RegisterServiceNotificationFilterReply")
}
pub(crate) fn unregistered(response: &Response<Body>) -> Result<()> {
    check_reply(response, "UnregisterServiceNotificationFilterReply")
}
fn check_reply(response: &Response<Body>, action: &str) -> Result<()> {
    check_gateway_actor(response)?;
    check_gateway_failure(response)?;
    if !action_is(response, action) {
        return Err(FurpcClientError::Protocol(
            "unexpected notification session reply action",
        ));
    }
    Ok(())
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Activity {
    pub guid: Guid,
    pub index: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PageId {
    pub activity: Activity,
    pub index: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub entry: Pointer<TableEntry>,
    pub primary_only: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Page {
    pub id: PageId,
    pub generation: Generation,
    pub versions: Pointer<Versions>,
    pub entries: CountedValues<Vec<Pointer<Entry>>>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct PageBody {
    pub page: Pointer<Page>,
}
#[derive(Serialize)]
pub(crate) struct Status {
    pub error: i64,
}
