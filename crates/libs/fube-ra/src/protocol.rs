//! Selected reliability schemas over the existing bounded Fabric codec.
//!
//! Field order follows pinned native FABRIC_FIELDS, not declaration order.
//! Scalar enums and ErrorCode values are LONG64; unknown values are retained.
//! ErrorCode diagnostic strings are not serialized. No Federation routing.
use fube_transport::{
    message::{Header, Message, ACTION, ACTOR, HIGH_PRIORITY},
    serialization::serde::{
        from_slice, to_vec, ByteArray, Derived, Guid, Inline, NativeEnum, ObjectArray, Pointer,
    },
    Error, Result,
};
use serde::{Deserialize, Serialize};

pub const GENERATION_HEADER: u16 = 0x8018;
pub const RA_ACTOR: i64 = 12;
pub const FM_ACTOR: i64 = 11;
pub const FMM_ACTOR: i64 = 10;

/// Native NodeId is raw LargeInteger layout: high u64 then low u64, little endian.
/// It is not a GUID object or UUID network order.
mod node_id {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &[u8; 16],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        ByteArray(value).serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<[u8; 16], D::Error> {
        ByteArray::deserialize(deserializer)?
            .0
            .try_into()
            .map_err(|_| serde::de::Error::custom("NodeId must contain 16 bytes"))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionId {
    pub guid: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsistencyUnit {
    pub id: PartitionId,
    pub low: i64,
    pub high: i64,
    pub name: String,
    pub kind: i64,
}
impl Default for ConsistencyUnit {
    fn default() -> Self {
        Self {
            id: PartitionId::default(),
            low: 0,
            high: 0,
            name: String::new(),
            kind: 1,
        }
    }
}

/// Wire order is configuration then data loss, opposite the native constructor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epoch {
    pub configuration: i64,
    pub data_loss: i64,
}
impl Epoch {
    pub fn older_than(self, other: Self) -> bool {
        (self.data_loss, self.configuration) < (other.data_loss, other.configuration)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverUnit {
    pub id: PartitionId,
    pub consistency: ConsistencyUnit,
    pub current: Epoch,
    pub previous: Epoch,
    pub target: i32,
    pub minimum: i32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInstance {
    #[serde(with = "node_id")]
    pub id: [u8; 16],
    pub instance: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationId {
    pub type_name: String,
    pub number: u32,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageId {
    pub application: ApplicationId,
    pub name: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceType {
    pub package: PackageId,
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rollout {
    pub major: u32,
    pub minor: u32,
}
impl Default for Rollout {
    fn default() -> Self {
        Self { major: 1, minor: 0 }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationVersion {
    pub value: Rollout,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageVersion {
    pub application: ApplicationVersion,
    pub value: Rollout,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionInstance {
    pub version: PackageVersion,
    pub instance: u64,
}
impl VersionInstance {
    pub fn invalid() -> Self {
        Self {
            version: PackageVersion {
                application: ApplicationVersion {
                    value: Rollout { major: 0, minor: 0 },
                },
                value: Rollout { major: 0, minor: 0 },
            },
            instance: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedServiceType {
    pub id: ServiceType,
    pub version: VersionInstance,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTypeInfo {
    pub versioned: VersionedServiceType,
    pub code_package: String,
}
/// Native request and reply bodies both contain a vector of ServiceTypeInfo.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceTypeNotification {
    pub infos: ObjectArray<Vec<ServiceTypeInfo>>,
}
pub type ServiceTypeNotificationReply = ServiceTypeNotification;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    pub node: NodeInstance,
    pub id: i64,
    pub instance: i64,
    pub current_role: i64,
    pub previous_role: i64,
    pub up: bool,
    pub last_lsn: i64,
    pub first_lsn: i64,
    pub state: i64,
    pub version: VersionInstance,
    pub service_endpoint: String,
    pub replication_endpoint: String,
}
impl Default for Replica {
    fn default() -> Self {
        Self {
            node: NodeInstance::default(),
            id: -1,
            instance: -1,
            current_role: 1,
            previous_role: 1,
            up: true,
            last_lsn: -1,
            first_lsn: -1,
            state: 2,
            version: VersionInstance::invalid(),
            service_endpoint: String::new(),
            replication_endpoint: String::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Correlation {
    pub service: String,
    pub scheme: i64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub weight: i64,
    pub primary: u32,
    pub secondary: u32,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub domain: String,
    pub kind: i64,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PartitionLoad {
    pub metric: String,
    pub lower: f64,
    pub upper: f64,
    pub interval: u32,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceLoad {
    pub metric: String,
    pub lower: f64,
    pub upper: f64,
    pub interval: u32,
    pub primary_only: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalingTrigger {
    #[serde(rename = "0")]
    Invalid,
    #[serde(rename = "1")]
    Partition { fields: Derived<PartitionLoad> },
    #[serde(rename = "2")]
    Service { fields: Derived<ServiceLoad> },
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScaleCount {
    pub maximum: i32,
    pub minimum: i32,
    pub increment: i32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalingMechanism {
    #[serde(rename = "0")]
    Invalid,
    #[serde(rename = "1")]
    Instance { fields: Derived<ScaleCount> },
    #[serde(rename = "2")]
    Partition { fields: Derived<ScaleCount> },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalingPolicy {
    pub trigger: Pointer<NativeEnum<ScalingTrigger>>,
    pub mechanism: Pointer<NativeEnum<ScalingMechanism>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Service {
    pub name: String,
    pub instance: u64,
    pub service_type: ServiceType,
    pub application: String,
    pub version: VersionInstance,
    pub partitions: i32,
    pub target: i32,
    pub minimum: i32,
    pub stateful: bool,
    pub persisted: bool,
    pub initialization: ByteArray,
    pub group: bool,
    pub correlations: ObjectArray<Vec<Correlation>>,
    pub constraints: String,
    pub scaleout: i32,
    pub metrics: ObjectArray<Vec<Metric>>,
    pub move_cost: u32,
    pub restart_wait: i64,
    pub quorum_wait: i64,
    pub update_version: u64,
    pub placement: ObjectArray<Vec<Placement>>,
    pub standby_keep: i64,
    pub activation_mode: i64,
    pub dns: String,
    pub scaling: ObjectArray<Vec<ScalingPolicy>>,
}
impl Default for Service {
    fn default() -> Self {
        Self {
            name: String::new(),
            instance: 0,
            service_type: ServiceType::default(),
            application: String::new(),
            version: VersionInstance::default(),
            partitions: 0,
            target: 0,
            minimum: 0,
            stateful: false,
            persisted: false,
            initialization: ByteArray::default(),
            group: false,
            correlations: ObjectArray::default(),
            constraints: String::new(),
            scaleout: 0,
            metrics: ObjectArray::default(),
            move_cost: 1,
            restart_wait: i64::MIN,
            quorum_wait: i64::MIN,
            update_version: 0,
            placement: ObjectArray::default(),
            standby_keep: i64::MIN,
            activation_mode: 0,
            dns: String::new(),
            scaling: ObjectArray::default(),
        }
    }
}

/// Native ReplicaMessageBody, used by AddInstance.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AddInstance {
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub service: Service,
}
/// Native ReplicaReplyMessageBody. ErrorCode is only its primitive signed value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddInstanceReply {
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub error: i64,
}

/// DeleteReplicaMessageBody inherits ReplicaMessageBody's scope, then adds force.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveInstance {
    pub replica: Inline<AddInstance>,
    pub deletion: Derived<DeleteOptions>,
}
impl Default for RemoveInstance {
    fn default() -> Self {
        Self::new(AddInstance::default(), false)
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteOptions {
    pub force: bool,
}
impl RemoveInstance {
    pub fn new(body: AddInstance, force: bool) -> Self {
        Self {
            replica: Inline(body),
            deletion: Derived(DeleteOptions { force }),
        }
    }
}
pub type RemoveInstanceReply = AddInstanceReply;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub description: Replica,
    pub intermediate_role: i64,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FailoverUnitInfo {
    pub service: Service,
    pub unit: FailoverUnit,
    pub intermediate: Epoch,
    pub report_from_primary: bool,
    pub replicas: ObjectArray<Vec<ReplicaInfo>>,
    pub local_deleted: bool,
}
/// Native ReplicaUpMessageBody, also used by ReplicaUpReply.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplicaUp {
    pub replicas: ObjectArray<Vec<FailoverUnitInfo>>,
    pub dropped: ObjectArray<Vec<FailoverUnitInfo>>,
    pub last: bool,
    pub from_fmm: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationNumber {
    pub generation: i64,
    #[serde(with = "node_id")]
    pub owner: [u8; 16],
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationHeader {
    pub number: GenerationNumber,
    pub for_fm_replica: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    Fm,
    Fmm,
}

impl GenerationHeader {
    pub fn header(&self) -> Result<Header> {
        Ok(Header {
            id: GENERATION_HEADER,
            value: to_vec(self)?,
        })
    }
}

pub fn add_instance_message(body: &AddInstance, generation: &GenerationHeader) -> Result<Message> {
    Ok(Message {
        headers: vec![
            Header::actor(RA_ACTOR)?,
            Header::action("AddInstance")?,
            Header::boolean(HIGH_PRIORITY, true)?,
            generation.header()?,
        ],
        body: to_vec(body)?,
    })
}

pub fn add_instance_reply_message(body: &AddInstanceReply, owner: Owner) -> Result<Message> {
    Ok(Message {
        headers: vec![
            Header::actor(match owner {
                Owner::Fm => FM_ACTOR,
                Owner::Fmm => FMM_ACTOR,
            })?,
            Header::action("AddInstanceReply")?,
            Header::boolean(HIGH_PRIORITY, true)?,
        ],
        body: to_vec(body)?,
    })
}

fn reliability_message<T: Serialize>(
    body: &T,
    action: &str,
    actor: i64,
    priority: bool,
    generation: Option<&GenerationHeader>,
) -> Result<Message> {
    let mut headers = vec![
        Header::actor(actor)?,
        Header::action(action)?,
        Header::boolean(HIGH_PRIORITY, priority)?,
    ];
    if let Some(generation) = generation {
        headers.push(generation.header()?);
    }
    Ok(Message {
        headers,
        body: to_vec(body)?,
    })
}
pub fn remove_instance_message(
    body: &RemoveInstance,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "RemoveInstance", RA_ACTOR, true, Some(generation))
}
pub fn remove_instance_reply_message(body: &RemoveInstanceReply, owner: Owner) -> Result<Message> {
    reliability_message(body, "RemoveInstanceReply", owner_actor(owner), true, None)
}
pub fn replica_up_message(body: &ReplicaUp, owner: Owner) -> Result<Message> {
    reliability_message(body, "ReplicaUp", owner_actor(owner), true, None)
}
pub fn replica_up_reply_message(
    body: &ReplicaUp,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "ReplicaUpReply", RA_ACTOR, true, Some(generation))
}
pub fn service_type_notification_message(body: &ServiceTypeNotification) -> Result<Message> {
    reliability_message(body, "ServiceTypeNotification", FM_ACTOR, false, None)
}
pub fn service_type_notification_reply_message(
    body: &ServiceTypeNotificationReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(
        body,
        "ServiceTypeNotificationReply",
        RA_ACTOR,
        false,
        Some(generation),
    )
}
fn owner_actor(owner: Owner) -> i64 {
    match owner {
        Owner::Fm => FM_ACTOR,
        Owner::Fmm => FMM_ACTOR,
    }
}

fn unique(message: &Message, id: u16) -> Result<&Header> {
    let mut headers = message.headers.iter().filter(|h| h.id == id);
    let header = headers
        .next()
        .ok_or(Error::Protocol("missing reliability header"))?;
    if headers.next().is_some() {
        return Err(Error::Protocol("duplicate reliability header"));
    }
    Ok(header)
}

fn check_factory(message: &Message, action: &str, actors: &[i64]) -> Result<i64> {
    let actor = unique(message, ACTOR)?.read(|d| d.i64())?;
    if !actors.contains(&actor) || unique(message, ACTION)?.read(|d| d.string())? != action {
        return Err(Error::Protocol("wrong reliability actor/action"));
    }
    // Priority is a native factory default, not semantic admission/acknowledgment.
    unique(message, HIGH_PRIORITY)?.read(|d| d.boolean())?;
    Ok(actor)
}

pub fn decode_add_instance(message: &Message) -> Result<(AddInstance, GenerationHeader)> {
    check_factory(message, "AddInstance", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}

pub fn decode_add_instance_reply(message: &Message) -> Result<(AddInstanceReply, Owner)> {
    let actor = check_factory(message, "AddInstanceReply", &[FM_ACTOR, FMM_ACTOR])?;
    // The actual FMTransport reply helper does not require GenerationHeader.
    Ok((
        from_slice(&message.body)?,
        if actor == FM_ACTOR {
            Owner::Fm
        } else {
            Owner::Fmm
        },
    ))
}

pub fn decode_remove_instance(message: &Message) -> Result<(RemoveInstance, GenerationHeader)> {
    check_factory(message, "RemoveInstance", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
pub fn decode_remove_instance_reply(message: &Message) -> Result<(RemoveInstanceReply, Owner)> {
    let actor = check_factory(message, "RemoveInstanceReply", &[FM_ACTOR, FMM_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        if actor == FM_ACTOR {
            Owner::Fm
        } else {
            Owner::Fmm
        },
    ))
}
pub fn decode_replica_up(message: &Message) -> Result<(ReplicaUp, Owner)> {
    let actor = check_factory(message, "ReplicaUp", &[FM_ACTOR, FMM_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        if actor == FM_ACTOR {
            Owner::Fm
        } else {
            Owner::Fmm
        },
    ))
}
pub fn decode_replica_up_reply(message: &Message) -> Result<(ReplicaUp, GenerationHeader)> {
    check_factory(message, "ReplicaUpReply", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
pub fn decode_service_type_notification(message: &Message) -> Result<ServiceTypeNotification> {
    check_factory(message, "ServiceTypeNotification", &[FM_ACTOR])?;
    from_slice(&message.body)
}
pub fn decode_service_type_notification_reply(
    message: &Message,
) -> Result<(ServiceTypeNotificationReply, GenerationHeader)> {
    check_factory(message, "ServiceTypeNotificationReply", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
