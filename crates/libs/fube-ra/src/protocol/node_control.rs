//! Pinned node-control/load codecs, not handler or endpoint-publication completion.
//!
//! Empty native constructors leave some scalar members unspecified. Rust defaults
//! are deterministic initialized values, not guarantees about those constructors.
use super::*;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeActivationRequest {
    pub sequence: i64,
    pub from_fmm: bool,
}
pub type NodeActivateRequest = NodeActivationRequest;
pub type NodeDeactivateRequest = NodeActivationRequest;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeActivationReply {
    pub sequence: i64,
}
pub type NodeActivateReply = NodeActivationReply;
pub type NodeDeactivateReply = NodeActivationReply;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeUpdateServiceRequest {
    pub service: Service,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeUpdateServiceReply {
    pub service: String,
    pub instance: u64,
    pub version: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTypeUpdate {
    pub types: ObjectArray<Vec<ServiceType>>,
    pub sequence: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTypeUpdateReply {
    pub sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadMetricValue {
    pub name: String,
    pub value: u32,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyLoadReport {
    pub unit: PartitionId,
    pub role: i64,
    // ReportLoadInfo deliberately does not serialize its isStateful_ member.
    pub metrics: ObjectArray<Vec<LoadMetricValue>>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadMetricStats {
    pub name: String,
    pub value: u32,
    pub count: u32,
    pub weighted_sum: f64,
    pub weight: f64,
    pub time: i64,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeLoads {
    // Native std::map uses an ArrayPair scope, with raw NodeId and object-array value.
    #[serde(with = "node_id")]
    pub node: [u8; 16],
    pub loads: ObjectArray<Vec<LoadMetricStats>>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadDescription {
    pub partition: Guid,
    pub service: String,
    pub stateful: bool,
    pub primary: ObjectArray<Vec<LoadMetricStats>>,
    pub secondary: ObjectArray<Vec<LoadMetricStats>>,
    pub nodes: ObjectArray<Vec<NodeLoads>>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReportLoad {
    pub legacy: ObjectArray<Vec<LegacyLoadReport>>,
    pub reports: ObjectArray<Vec<LoadDescription>>,
    pub time: i64,
}
pub fn report_load_message(body: &ReportLoad) -> Result<Message> {
    reliability_message(body, "ReportLoad", FM_ACTOR, false, None)
}
pub fn decode_report_load(message: &Message) -> Result<ReportLoad> {
    check_factory(message, "ReportLoad", &[FM_ACTOR])?;
    from_slice(&message.body)
}

fn request<T: Serialize>(body: &T, action: &str, generation: &GenerationHeader) -> Result<Message> {
    reliability_message(body, action, RA_ACTOR, false, Some(generation))
}
fn decode_request<T: serde::de::DeserializeOwned>(
    message: &Message,
    action: &str,
) -> Result<(T, GenerationHeader)> {
    check_factory(message, action, &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
fn decode_reply<T: serde::de::DeserializeOwned>(
    message: &Message,
    action: &str,
) -> Result<(T, Owner)> {
    let actor = check_factory(message, action, &[FM_ACTOR, FMM_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        if actor == FM_ACTOR {
            Owner::Fm
        } else {
            Owner::Fmm
        },
    ))
}

pub fn node_activate_request_message(
    body: &NodeActivateRequest,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "NodeActivateRequest", generation)
}
pub fn decode_node_activate_request(
    message: &Message,
) -> Result<(NodeActivateRequest, GenerationHeader)> {
    decode_request(message, "NodeActivateRequest")
}
pub fn node_activate_reply_message(body: &NodeActivateReply, owner: Owner) -> Result<Message> {
    reliability_message(body, "NodeActivateReply", owner_actor(owner), false, None)
}
pub fn decode_node_activate_reply(message: &Message) -> Result<(NodeActivateReply, Owner)> {
    decode_reply(message, "NodeActivateReply")
}
pub fn node_deactivate_request_message(
    body: &NodeDeactivateRequest,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "NodeDeactivateRequest", generation)
}
pub fn decode_node_deactivate_request(
    message: &Message,
) -> Result<(NodeDeactivateRequest, GenerationHeader)> {
    decode_request(message, "NodeDeactivateRequest")
}
pub fn node_deactivate_reply_message(body: &NodeDeactivateReply, owner: Owner) -> Result<Message> {
    reliability_message(body, "NodeDeactivateReply", owner_actor(owner), false, None)
}
pub fn decode_node_deactivate_reply(message: &Message) -> Result<(NodeDeactivateReply, Owner)> {
    decode_reply(message, "NodeDeactivateReply")
}
pub fn node_update_service_request_message(
    body: &NodeUpdateServiceRequest,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "NodeUpdateServiceRequest", generation)
}
pub fn decode_node_update_service_request(
    message: &Message,
) -> Result<(NodeUpdateServiceRequest, GenerationHeader)> {
    decode_request(message, "NodeUpdateServiceRequest")
}
pub fn node_update_service_reply_message(
    body: &NodeUpdateServiceReply,
    owner: Owner,
) -> Result<Message> {
    reliability_message(
        body,
        "NodeUpdateServiceReply",
        owner_actor(owner),
        false,
        None,
    )
}
pub fn decode_node_update_service_reply(
    message: &Message,
) -> Result<(NodeUpdateServiceReply, Owner)> {
    decode_reply(message, "NodeUpdateServiceReply")
}
pub fn service_type_enabled_message(body: &ServiceTypeUpdate) -> Result<Message> {
    reliability_message(body, "ServiceTypeEnabled", FM_ACTOR, false, None)
}
pub fn service_type_disabled_message(body: &ServiceTypeUpdate) -> Result<Message> {
    reliability_message(body, "ServiceTypeDisabled", FM_ACTOR, false, None)
}
pub fn decode_service_type_enabled(message: &Message) -> Result<ServiceTypeUpdate> {
    check_factory(message, "ServiceTypeEnabled", &[FM_ACTOR])?;
    from_slice(&message.body)
}
pub fn decode_service_type_disabled(message: &Message) -> Result<ServiceTypeUpdate> {
    check_factory(message, "ServiceTypeDisabled", &[FM_ACTOR])?;
    from_slice(&message.body)
}
pub fn service_type_enabled_reply_message(
    body: &ServiceTypeUpdateReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "ServiceTypeEnabledReply", generation)
}
pub fn service_type_disabled_reply_message(
    body: &ServiceTypeUpdateReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "ServiceTypeDisabledReply", generation)
}
pub fn decode_service_type_enabled_reply(
    message: &Message,
) -> Result<(ServiceTypeUpdateReply, GenerationHeader)> {
    decode_request(message, "ServiceTypeEnabledReply")
}
pub fn decode_service_type_disabled_reply(
    message: &Message,
) -> Result<(ServiceTypeUpdateReply, GenerationHeader)> {
    decode_request(message, "ServiceTypeDisabledReply")
}
pub fn replica_endpoint_updated_message(body: &AddInstance, owner: Owner) -> Result<Message> {
    reliability_message(
        body,
        "ReplicaEndpointUpdated",
        owner_actor(owner),
        false,
        None,
    )
}
pub fn decode_replica_endpoint_updated(message: &Message) -> Result<(AddInstance, Owner)> {
    decode_reply(message, "ReplicaEndpointUpdated")
}
pub fn replica_endpoint_updated_reply_message(
    body: &AddInstanceReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    request(body, "ReplicaEndpointUpdatedReply", generation)
}
pub fn decode_replica_endpoint_updated_reply(
    message: &Message,
) -> Result<(AddInstanceReply, GenerationHeader)> {
    decode_request(message, "ReplicaEndpointUpdatedReply")
}
