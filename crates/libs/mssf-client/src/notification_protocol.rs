//! Source-derived notification schemas. No endpoint description enrichment.
use mssf_transport::{
    message::Message,
    serialization::serde::{
        from_slice, from_slice_with_budget, to_vec, CountedValues, Guid, ObjectArray, Pointer,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    resolution::Generation, resolution_protocol::TableEntry, Error, GatewayDescription, NamingUri,
    NodeId, NotificationResult, Result,
};

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
pub(crate) fn decode_connected(message: Message) -> Result<Connected> {
    crate::protocol::check_reply(&message, &["NotificationClientConnectionReply"])?;
    let body: Connected = from_slice(&message.body)?;
    // ServiceNotificationManager::CreateGatewayDescription intentionally emits
    // an empty address: node identity/name identify the actual notification gateway.
    if body.gateway.name.is_empty() {
        return Err(crate::Error::Protocol(
            "empty notification gateway identity",
        ));
    }
    Ok(body)
}
pub(crate) fn decode_registered(message: Message) -> Result<()> {
    crate::protocol::check_reply(&message, &["RegisterServiceNotificationFilterReply"])?;
    empty(&message)
}
pub(crate) fn decode_unregistered(message: Message) -> Result<()> {
    crate::protocol::check_reply(&message, &["UnregisterServiceNotificationFilterReply"])?;
    empty(&message)
}
fn empty(message: &Message) -> Result<()> {
    if !message.body.is_empty() {
        return Err(crate::Error::Protocol("filter reply has a body"));
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
pub(crate) fn decode_page(
    body: &[u8],
    reserve: &mut impl FnMut(usize) -> NotificationResult<()>,
) -> NotificationResult<Page> {
    // Serde's format error stays shared; retain the original reservation cause
    // separately rather than interpreting a transport diagnostic string.
    let mut allocation_error = None;
    let decoded = from_slice_with_budget::<PageBody>(body, &mut |bytes| {
        reserve(bytes).map_err(|error| {
            allocation_error = Some(error);
            Error::ResourceLimit("decoded allocation reservation")
        })
    });
    if let Some(error) = allocation_error {
        return Err(error);
    }
    decoded?
        .page
        .0
        .ok_or_else(|| Error::Protocol("null notification page").into())
}
pub(crate) fn status(success: bool) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct Status {
        error: i64,
    }
    // ErrorCodeValue::OperationFailed (HRESULT E_FAIL), not a Result payload.
    to_vec(&Status {
        error: if success { 0 } else { -2147467259 },
    })
}
