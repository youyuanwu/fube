//! Native FM/RA and RA/RA channel policy, bounded decode and exact source binding.
use super::{err, Result, MAX_BODY_BYTES as LIMIT};
use fube_ra::protocol::{
    GenerationHeader, NodeInstance, FM_ACTOR, GENERATION_HEADER, RA_ACTOR, RS_ACTOR,
};
use fube_transport::{
    channel::{IncomingMessage, MessageReceiver},
    message::{Header, Message, Properties},
    native_channel::GatewayTarget,
    native_demux::{
        ActorDemux, ActorRegistration, ChannelComposition, OutboundKind, RequestPolicy,
    },
    serialization::serde::{from_slice_with_budget, to_vec},
    Channel, ChannelLimits,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

// Native pin 3988b451: Federation/PToPHeader.h FABRIC_FIELDS_06;
// PToPActor.h Direct=0; Transport/MessageHeaderId.h PToP=0x8013.
// Native NodeUp contains NodeDescription, NOT a made-up incarnation field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PToP {
    from: NodeInstance,
    to: NodeInstance,
    actor: i64,
    from_ring: String,
    to_ring: String,
    exact_instance: bool,
}
const PTOP: u16 = 0x8013;

fn bounded(message: &Message) -> fube_transport::Result<()> {
    if message.body.len() > LIMIT
        || message.headers.len() > 16
        || message
            .headers
            .iter()
            .map(|header| header.value.len())
            .sum::<usize>()
            > 4096
    {
        return Err(fube_transport::Error::ResourceLimit("cluster envelope"));
    }
    Ok(())
}
fn fm_ingress(message: &Message, props: &Properties) -> fube_transport::Result<()> {
    bounded(message)?;
    if props.actor != FM_ACTOR
        || props.uncorrelated_reply
        || !matches!(
            props.action.as_str(),
            "NodeUp"
                | "LFUMUpload"
                | "ReplicaUp"
                | "ServiceTypeNotification"
                | "AddInstanceReply"
                | "AddPrimaryReply"
                | "AddReplicaReply"
                | "DoReconfigurationReply"
                | "ChangeConfiguration"
                | "ReplicaEndpointUpdated"
                | "RemoveReplicaReply"
                | "RemoveInstanceReply"
                | "DeleteReplicaReply"
                | "NodeActivateReply"
                | "NodeDeactivateReply"
                | "NodeUpdateServiceReply"
                | "ServiceTypeEnabled"
                | "ServiceTypeDisabled"
                | "ReportLoad"
                | "GenerationUpdateReject"
        )
        || props.expects_reply != matches!(props.action.as_str(), "NodeUp" | "LFUMUpload")
        || props.high_priority
            != matches!(
                props.action.as_str(),
                "NodeUp"
                    | "LFUMUpload"
                    | "ReplicaUp"
                    | "AddInstanceReply"
                    | "AddPrimaryReply"
                    | "AddReplicaReply"
                    | "DoReconfigurationReply"
                    | "ChangeConfiguration"
                    | "RemoveReplicaReply"
                    | "RemoveInstanceReply"
                    | "DeleteReplicaReply"
                    | "GenerationUpdateReject"
            )
    {
        return Err(fube_transport::Error::Protocol("unsupported FM ingress"));
    }
    Ok(())
}
fn node_ingress(message: &Message, props: &Properties) -> fube_transport::Result<()> {
    bounded(message)?;
    if props.actor != RA_ACTOR
        || props.expects_reply != (props.action == "GenerationProposal")
        || props.uncorrelated_reply
        || props.high_priority
            != matches!(
                props.action.as_str(),
                "AddInstance"
                    | "AddPrimary"
                    | "AddReplica"
                    | "DoReconfiguration"
                    | "ChangeConfigurationReply"
                    | "RemoveReplica"
                    | "RemoveInstance"
                    | "DeleteReplica"
                    | "ReplicaUpReply"
                    | "GenerationProposal"
                    | "GenerationUpdate"
            )
        || !matches!(
            props.action.as_str(),
            "AddInstance"
                | "AddPrimary"
                | "AddReplica"
                | "DoReconfiguration"
                | "ChangeConfigurationReply"
                | "RemoveReplica"
                | "RemoveInstance"
                | "DeleteReplica"
                | "ReplicaUpReply"
                | "ServiceTypeNotificationReply"
                | "ReplicaEndpointUpdatedReply"
                | "NodeActivateRequest"
                | "NodeDeactivateRequest"
                | "NodeUpdateServiceRequest"
                | "ServiceTypeEnabledReply"
                | "ServiceTypeDisabledReply"
                | "GenerationProposal"
                | "GenerationUpdate"
        )
    {
        return Err(fube_transport::Error::Protocol("unsupported node ingress"));
    }
    Ok(())
}
fn ack(message: &Message, props: &Properties) -> fube_transport::Result<()> {
    bounded(message)?;
    if !matches!(
        (props.actor, props.action.as_str()),
        (RS_ACTOR, "NodeUpAck") | (RA_ACTOR, "LFUMUploadReply")
    ) || props.expects_reply
        || props.uncorrelated_reply
        || !props.high_priority
    {
        return Err(fube_transport::Error::Protocol(
            "unsupported bootstrap reply",
        ));
    }
    Ok(())
}
fn proposal_ack(message: &Message, props: &Properties) -> fube_transport::Result<()> {
    bounded(message)?;
    if props.actor != FM_ACTOR
        || props.action != "GenerationProposalReply"
        || props.expects_reply
        || props.uncorrelated_reply
        || !props.high_priority
    {
        return Err(fube_transport::Error::Protocol(
            "unsupported generation proposal reply",
        ));
    }
    Ok(())
}
fn fm_out(message: &Message, props: &Properties, kind: OutboundKind) -> fube_transport::Result<()> {
    if (kind == OutboundKind::Rpc) != (props.action == "GenerationProposal") {
        return Err(fube_transport::Error::Protocol("wrong FM send mechanism"));
    }
    let mut canonical = message.properties()?;
    canonical.expects_reply = canonical.action == "GenerationProposal";
    node_ingress(message, &canonical)
}
fn node_out(
    message: &Message,
    props: &Properties,
    kind: OutboundKind,
) -> fube_transport::Result<()> {
    if (kind == OutboundKind::Rpc) != matches!(props.action.as_str(), "NodeUp" | "LFUMUpload") {
        return Err(fube_transport::Error::Protocol("wrong node send mechanism"));
    }
    // RPC adds ExpectsReply below outbound policy.
    let mut canonical = message.properties()?;
    canonical.expects_reply = matches!(canonical.action.as_str(), "NodeUp" | "LFUMUpload");
    fm_ingress(message, &canonical)
}
fn peer_ingress(message: &Message, props: &Properties) -> fube_transport::Result<()> {
    bounded(message)?;
    if props.actor != RA_ACTOR
        || !props.high_priority
        || props.expects_reply
        || props.uncorrelated_reply
        || !matches!(
            props.action.as_str(),
            "CreateReplica"
                | "CreateReplicaReply"
                | "Activate"
                | "ActivateReply"
                | "Deactivate"
                | "DeactivateReply"
                | "GetLSN"
                | "GetLSNReply"
                | "DoReconfiguration"
        )
        || message
            .headers
            .iter()
            .filter(|header| header.id == GENERATION_HEADER)
            .count()
            != 1
    {
        return Err(fube_transport::Error::Protocol("unsupported peer ingress"));
    }
    Ok(())
}
fn peer_out(
    message: &Message,
    props: &Properties,
    kind: OutboundKind,
) -> fube_transport::Result<()> {
    if kind == OutboundKind::Rpc {
        return Err(fube_transport::Error::Protocol("peer traffic is one-way"));
    }
    peer_ingress(message, props)
}
pub fn peer_composition() -> fube_transport::Result<ChannelComposition> {
    Ok(ChannelComposition {
        outbound: peer_out,
        ordinary_reply: peer_ingress,
        actors: ActorDemux::new(vec![ActorRegistration {
            actor: RA_ACTOR,
            authorize: peer_ingress,
        }])?,
        requests: None,
        duplex: None,
        ordered_ingress: true,
    })
}
pub fn composition(fm: bool) -> fube_transport::Result<ChannelComposition> {
    Ok(ChannelComposition {
        outbound: if fm { fm_out } else { node_out },
        ordinary_reply: if fm { proposal_ack } else { ack },
        actors: ActorDemux::new(vec![ActorRegistration {
            actor: if fm { FM_ACTOR } else { RA_ACTOR },
            authorize: if fm { fm_ingress } else { node_ingress },
        }])?,
        requests: if fm {
            Some(RequestPolicy {
                incoming: fm_ingress,
                reply: ack,
            })
        } else {
            Some(RequestPolicy {
                incoming: node_ingress,
                reply: proposal_ack,
            })
        },
        duplex: None,
        ordered_ingress: true,
    })
}
pub fn limits() -> ChannelLimits {
    ChannelLimits {
        max_operations: 8,
        max_replies: 4,
        max_ingress: 8,
        max_frame_bytes: LIMIT,
        max_retained_bytes: 32 * 1024 * 1024,
        max_history: 4096,
    }
}
pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let mut budget = 4 * LIMIT;
    from_slice_with_budget(bytes, &mut |count| {
        budget = budget
            .checked_sub(count)
            .ok_or(fube_transport::Error::ResourceLimit(
                "cluster native decode",
            ))?;
        Ok(())
    })
    .map_err(err)
}

// Bound allocations before the existing action/header/body factory decoders
// run. Their second decode is then bounded by this validated full schema.
pub fn predecode(message: &Message) -> Result<()> {
    use fube_ra::protocol::*;
    match message.properties().map_err(err)?.action.as_str() {
        "GetLSN" => {
            decode::<GetLsn>(&message.body)?;
        }
        "GetLSNReply" => {
            decode::<GetLsnReply>(&message.body)?;
        }
        "ReplicaEndpointUpdated" => {
            decode::<AddInstance>(&message.body)?;
        }
        "ReplicaEndpointUpdatedReply" => {
            decode::<AddInstanceReply>(&message.body)?;
        }
        "DoReconfiguration" => {
            decode::<DoReconfiguration>(&message.body)?;
        }
        "DoReconfigurationReply" => {
            decode::<ConfigurationReply>(&message.body)?;
        }
        "ChangeConfiguration" => {
            decode::<Configuration>(&message.body)?;
        }
        "ChangeConfigurationReply" => {
            decode::<FailoverUnitReply>(&message.body)?;
        }
        "Activate" => {
            decode::<Activate>(&message.body)?;
        }
        "ActivateReply" => {
            decode::<ActivateReply>(&message.body)?;
        }
        "Deactivate" => {
            decode::<Deactivate>(&message.body)?;
        }
        "DeactivateReply" => {
            decode::<DeactivateReply>(&message.body)?;
        }
        "GenerationProposal" => {
            if !message.body.is_empty() {
                return Err("GenerationProposal must be bodyless".into());
            }
        }
        "LFUMUpload" => {
            decode::<LfumUpload>(&message.body)?;
        }
        "LFUMUploadReply" => {
            if !message.body.is_empty() {
                return Err("LFUMUploadReply must be bodyless".into());
            }
        }
        "GenerationProposalReply" => {
            decode::<fube_ra::protocol::GenerationProposalReply>(&message.body)?;
        }
        "GenerationUpdate" => {
            decode::<GenerationUpdate>(&message.body)?;
        }
        "GenerationUpdateReject" => {
            decode::<GenerationNumber>(&message.body)?;
        }
        "NodeUp" => {
            decode::<NodeUp>(&message.body)?;
        }
        "NodeUpAck" => {
            decode::<NodeUpAck>(&message.body)?;
        }
        "AddInstance" | "AddPrimary" | "AddReplica" | "RemoveReplica" => {
            decode::<AddInstance>(&message.body)?;
        }
        "CreateReplica" => {
            decode::<CreateReplica>(&message.body)?;
        }
        "CreateReplicaReply" => {
            decode::<CreateReplicaReply>(&message.body)?;
        }
        "RemoveInstance" | "DeleteReplica" => {
            decode::<RemoveInstance>(&message.body)?;
        }
        "AddInstanceReply"
        | "AddPrimaryReply"
        | "AddReplicaReply"
        | "RemoveReplicaReply"
        | "RemoveInstanceReply"
        | "DeleteReplicaReply" => {
            decode::<AddInstanceReply>(&message.body)?;
        }
        "ReplicaUp" | "ReplicaUpReply" => {
            decode::<ReplicaUp>(&message.body)?;
        }
        "ServiceTypeNotification" | "ServiceTypeNotificationReply" => {
            decode::<ServiceTypeNotification>(&message.body)?;
        }
        "NodeActivateRequest" | "NodeDeactivateRequest" => {
            decode::<NodeActivationRequest>(&message.body)?;
        }
        "NodeActivateReply" | "NodeDeactivateReply" => {
            decode::<NodeActivationReply>(&message.body)?;
        }
        "NodeUpdateServiceRequest" => {
            decode::<NodeUpdateServiceRequest>(&message.body)?;
        }
        "NodeUpdateServiceReply" => {
            decode::<NodeUpdateServiceReply>(&message.body)?;
        }
        "ServiceTypeEnabled" | "ServiceTypeDisabled" => {
            decode::<ServiceTypeUpdate>(&message.body)?;
        }
        "ServiceTypeEnabledReply" | "ServiceTypeDisabledReply" => {
            decode::<ServiceTypeUpdateReply>(&message.body)?;
        }
        "ReportLoad" => {
            decode::<ReportLoad>(&message.body)?;
        }
        _ => return Err("unsupported native action retained at boundary".into()),
    }
    Ok(())
}

#[derive(Clone)]
pub struct Link {
    pub channel: Channel,
    target: Arc<GatewayTarget>,
    local: NodeInstance,
    remote: NodeInstance,
    generation: Arc<Mutex<GenerationHeader>>,
}
impl Link {
    pub fn generation(&self) -> GenerationHeader {
        self.generation.lock().unwrap().clone()
    }
    pub fn advance_generation(&self, generation: &GenerationHeader) {
        let mut current = self.generation.lock().unwrap();
        if !generation.number.older_than(&current.number) {
            *current = generation.clone();
        }
    }
    // Only the composition owner constructs a Link, from its original one-use
    // admitted Channel. Deployment/session binding is local capability state,
    // not a fabricated universal native header or a reconnect credential.
    pub fn new(
        channel: Channel,
        local: NodeInstance,
        remote: NodeInstance,
        generation: GenerationHeader,
    ) -> Result<Self> {
        let target = channel.target().map_err(err)?;
        Ok(Self {
            channel,
            target,
            local,
            remote,
            generation: Arc::new(Mutex::new(generation)),
        })
    }
    pub fn seal(&self, mut message: Message) -> Result<Message> {
        if message.headers.iter().any(|header| header.id == PTOP) {
            return Err("duplicate source header".into());
        }
        message.headers.push(Header {
            id: PTOP,
            value: to_vec(&PToP {
                from: self.local.clone(),
                to: self.remote.clone(),
                actor: 0,
                from_ring: String::new(),
                to_ring: String::new(),
                exact_instance: true,
            })
            .map_err(err)?,
        });
        bounded(&message).map_err(err)?;
        Ok(message)
    }
    pub fn validate(&self, message: &Message) -> Result<()> {
        validate_source(message, &self.local, &self.remote)?;
        let mut headers = message
            .headers
            .iter()
            .filter(|header| header.id == GENERATION_HEADER);
        if let Some(header) = headers.next() {
            let generation: GenerationHeader = decode(&header.value)?;
            let negotiation = matches!(
                message.properties().map_err(err)?.action.as_str(),
                "GenerationProposal" | "GenerationUpdate" | "LFUMUploadReply"
            );
            if headers.next().is_some()
                || generation.for_fm_replica
                || (!negotiation && generation.number.older_than(&self.generation().number))
            {
                return Err("foreign/stale/duplicate native FM generation".into());
            }
        }
        Ok(())
    }
    pub async fn send(&self, message: Message) -> Result<()> {
        self.channel
            .send_reserved(&self.target, self.seal(message)?, deadline(), None)
            .await
            .map_err(err)
    }
    pub async fn request(&self, message: Message) -> Result<Message> {
        self.request_optional(message)
            .await?
            .ok_or("native request reply timeout".into())
    }
    pub async fn request_optional(&self, message: Message) -> Result<Option<Message>> {
        let (future, _) =
            self.channel
                .request_tracked(&self.target, self.seal(message)?, deadline());
        let reply = match future.await {
            Ok(reply) => reply,
            Err(fube_transport::Error::Timeout) => return Ok(None),
            Err(error) => return Err(err(error)),
        };
        self.validate(&reply)?;
        Ok(Some(reply))
    }
    pub async fn receive(&self, until: Instant) -> Result<Option<IncomingMessage<GatewayTarget>>> {
        self.receive_checked(until)
            .await
            .map_err(|error| error.to_string())
    }
    /// Preserve transport causes separately from native-envelope validation failures.
    pub async fn receive_checked(
        &self,
        until: Instant,
    ) -> std::result::Result<Option<IncomingMessage<GatewayTarget>>, ReceiveError> {
        match self
            .channel
            .receiver()
            .map_err(|error| {
                ReceiveError::Transport(self.channel.terminal_error().unwrap_or(error))
            })?
            .receive(until)
            .await
        {
            Ok(incoming) => {
                if !Arc::ptr_eq(&incoming.source, &self.target) {
                    self.channel.dispose();
                    return Err(ReceiveError::Invalid(
                        "foreign Channel source capability".into(),
                    ));
                }
                if let Err(error) = self.validate(&incoming.message) {
                    self.channel.dispose();
                    return Err(ReceiveError::Invalid(error));
                }
                Ok(Some(incoming))
            }
            Err(fube_transport::Error::Timeout) => Ok(None),
            Err(error) => Err(ReceiveError::Transport(
                self.channel.terminal_error().unwrap_or(error),
            )),
        }
    }
    pub async fn close(&self) -> Result<()> {
        self.channel.dispose();
        self.channel.closed(deadline()).await.map_err(err)
    }
}
#[derive(Debug)]
pub enum ReceiveError {
    Transport(fube_transport::Error),
    Invalid(String),
}
impl std::fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error:?}"),
            Self::Invalid(error) => f.write_str(error),
        }
    }
}
fn validate_source(message: &Message, local: &NodeInstance, remote: &NodeInstance) -> Result<()> {
    bounded(message).map_err(err)?;
    let mut headers = message.headers.iter().filter(|header| header.id == PTOP);
    let header = headers.next().ok_or("missing native source identity")?;
    if headers.next().is_some() {
        return Err("duplicate native source identity".into());
    }
    let mut budget = 8192usize;
    let value: PToP = from_slice_with_budget(&header.value, &mut |bytes| {
        budget = budget
            .checked_sub(bytes)
            .ok_or(fube_transport::Error::ResourceLimit("PToP decode"))?;
        Ok(())
    })
    .map_err(err)?;
    if value
        != (PToP {
            from: remote.clone(),
            to: local.clone(),
            actor: 0,
            from_ring: String::new(),
            to_ring: String::new(),
            exact_instance: true,
        })
    {
        return Err("foreign/stale native sender or recipient incarnation".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_idle_channel_admits_only_generation_bound_high_priority_one_way_ra_actions() {
        use fube_ra::protocol::*;
        let generation = GenerationHeader {
            number: GenerationNumber {
                owner: [9; 16],
                generation: 7,
            },
            for_fm_replica: false,
        };
        let body = CreateReplica::new(AddInstance::default(), ReplicaDeactivationInfo::default());
        for message in [
            create_replica_message(&body, &generation).unwrap(),
            create_replica_reply_message(&CreateReplicaReply::default(), &generation).unwrap(),
        ] {
            let properties = message.properties().unwrap();
            peer_ingress(&message, &properties).unwrap();
            predecode(&message).unwrap();
            assert!(peer_out(&message, &properties, OutboundKind::Rpc).is_err());
            for invalid in [
                Properties {
                    actor: FM_ACTOR,
                    ..message.properties().unwrap()
                },
                Properties {
                    high_priority: false,
                    ..message.properties().unwrap()
                },
                Properties {
                    expects_reply: true,
                    ..message.properties().unwrap()
                },
                Properties {
                    uncorrelated_reply: true,
                    ..message.properties().unwrap()
                },
                Properties {
                    action: "AddPrimary".into(),
                    ..message.properties().unwrap()
                },
            ] {
                assert!(peer_ingress(&message, &invalid).is_err());
            }
            let mut missing = message.clone();
            missing
                .headers
                .retain(|header| header.id != GENERATION_HEADER);
            assert!(peer_ingress(&missing, &properties).is_err());
            let mut duplicate = message.clone();
            duplicate.headers.push(
                message
                    .headers
                    .iter()
                    .find(|header| header.id == GENERATION_HEADER)
                    .unwrap()
                    .clone(),
            );
            assert!(peer_ingress(&duplicate, &properties).is_err());
        }
    }

    #[test]
    fn native_sender_incarnations_are_not_taken_from_node_description_or_actor() {
        let local = NodeInstance {
            id: [1; 16],
            instance: 10,
        };
        let remote = NodeInstance {
            id: [2; 16],
            instance: 20,
        };
        let identity = PToP {
            from: remote.clone(),
            to: local.clone(),
            actor: 0,
            from_ring: String::new(),
            to_ring: String::new(),
            exact_instance: true,
        };
        let message = |identity: &PToP| Message {
            headers: vec![Header {
                id: PTOP,
                value: to_vec(identity).unwrap(),
            }],
            body: vec![],
        };
        validate_source(&message(&identity), &local, &remote).unwrap();
        let mut stale = identity.clone();
        stale.from.instance -= 1;
        assert!(validate_source(&message(&stale), &local, &remote).is_err());
        stale = identity.clone();
        stale.to.instance -= 1;
        assert!(validate_source(&message(&stale), &local, &remote).is_err());
        stale = identity.clone();
        stale.from.id = [3; 16];
        assert!(validate_source(&message(&stale), &local, &remote).is_err());
        stale = identity.clone();
        stale.exact_instance = false;
        assert!(validate_source(&message(&stale), &local, &remote).is_err());
        let mut duplicate = message(&identity);
        duplicate.headers.push(duplicate.headers[0].clone());
        assert!(validate_source(&duplicate, &local, &remote).is_err());
    }
}
