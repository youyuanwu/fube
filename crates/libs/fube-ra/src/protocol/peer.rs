//! Selected peer creation codecs, not build/copy or reconfiguration admission.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaDeactivationInfo {
    pub epoch: Epoch,
    pub catchup_lsn: i64,
}

impl Default for ReplicaDeactivationInfo {
    fn default() -> Self {
        Self {
            epoch: Epoch::default(),
            catchup_lsn: -1,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct CreateReplicaOptions<T> {
    info: T,
}

#[derive(Serialize, Deserialize)]
struct CreateReplicaWire<T, I> {
    replica: Inline<T>,
    deactivation: Derived<CreateReplicaOptions<I>>,
}

/// Native RAReplicaMessageBody, retaining the complete base and deactivation info.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateReplica {
    pub replica: Derived<AddInstance>,
    pub deactivation: ReplicaDeactivationInfo,
}

impl Default for CreateReplica {
    fn default() -> Self {
        Self::new(AddInstance::default(), ReplicaDeactivationInfo::default())
    }
}

impl CreateReplica {
    pub fn new(replica: AddInstance, info: ReplicaDeactivationInfo) -> Self {
        Self {
            replica: Derived(replica),
            deactivation: info,
        }
    }
}

// Native places the base in the first scope, then one nested info object in
// the next scope. Keep that layout separate from the public inherited-body API.
impl Serialize for CreateReplica {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        CreateReplicaWire {
            replica: Inline(&self.replica.0),
            deactivation: Derived(CreateReplicaOptions {
                info: &self.deactivation,
            }),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CreateReplica {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let body =
            CreateReplicaWire::<AddInstance, ReplicaDeactivationInfo>::deserialize(deserializer)?;
        Ok(Self::new(body.replica.0, body.deactivation.0.info))
    }
}

pub type CreateReplicaReply = AddInstanceReply;

pub fn create_replica_message(
    body: &CreateReplica,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "CreateReplica", RA_ACTOR, true, Some(generation))
}

pub fn create_replica_reply_message(
    body: &CreateReplicaReply,
    generation: &GenerationHeader,
) -> Result<Message> {
    reliability_message(body, "CreateReplicaReply", RA_ACTOR, true, Some(generation))
}

pub fn decode_create_replica(message: &Message) -> Result<(CreateReplica, GenerationHeader)> {
    check_factory(message, "CreateReplica", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}

pub fn decode_create_replica_reply(
    message: &Message,
) -> Result<(CreateReplicaReply, GenerationHeader)> {
    check_factory(message, "CreateReplicaReply", &[RA_ACTOR])?;
    Ok((
        from_slice(&message.body)?,
        from_slice(&unique(message, GENERATION_HEADER)?.value)?,
    ))
}
