//! Shared capability admission and native FM/peer dispatch for an owned Node.
use super::{err, wire, Result, SemanticAck};
use crate::local::{Error, FmWork, Node};
use fube_ra::{entity as ra, protocol::*};
use fube_transport::{channel::IncomingMessage, message::Message, native_channel::GatewayTarget};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Stateless,
    StatefulSingleton,
    StatefulPeerIdle,
    StatefulThree,
    Stateful { target: i32, minimum: i32 },
}

impl Profile {
    pub fn stateful(self) -> bool {
        self != Self::Stateless
    }

    pub fn peer_replicas(self) -> bool {
        matches!(self, Self::StatefulPeerIdle | Self::StatefulThree)
            || matches!(self, Self::Stateful { target, .. } if target > 1)
    }

    pub fn configured_sizes(self) -> Option<(i32, i32)> {
        match self {
            Self::Stateful { target, minimum } => Some((target, minimum)),
            _ => None,
        }
    }

    pub fn mesh_peers(self) -> bool {
        matches!(self, Self::StatefulThree | Self::Stateful { .. })
    }

    pub fn validate(self) -> Result<()> {
        if self.configured_sizes().is_some_and(|(target, minimum)| {
            minimum < 1 || target < minimum || target as usize > ra::MAX_CONFIGURATION_REPLICAS
        }) {
            return Err("stateful sizes require 1 <= minimum <= target <= 64".into());
        }
        Ok(())
    }

    pub fn node_count(self) -> usize {
        match self {
            Self::Stateful { target, .. } => target as usize,
            Self::StatefulThree => 3,
            _ => 2,
        }
    }

    pub fn check_service_sizes(self, unit: &FailoverUnit, service: &Service) -> Result<()> {
        if self.configured_sizes().is_some_and(|(target, minimum)| {
            unit.target != target
                || unit.minimum != minimum
                || service.target != target
                || service.minimum != minimum
        }) {
            return Err("replica-set settings are immutable for this service".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Placement {
    pub node: NodeInstance,
    pub replica: i64,
    pub incarnation: i64,
}

pub enum Admission {
    /// Keep the original incoming frame and its unacknowledged handoff.
    Retained,
    Submitted(Option<(u64, SemanticAck)>),
}

pub struct Ingress {
    profile: Profile,
    coordinating: bool,
    partition: PartitionId,
    fm: NodeInstance,
    local: Placement,
    peers: Vec<Placement>,
}

impl Ingress {
    pub fn new(
        profile: Profile,
        coordinating: bool,
        partition: PartitionId,
        fm: NodeInstance,
        local: Placement,
        peers: Vec<Placement>,
    ) -> Result<Self> {
        profile.validate()?;
        let expected = if profile.peer_replicas() {
            profile.node_count() - 1
        } else {
            0
        };
        if peers.len() != expected
            || fm.instance == 0
            || std::iter::once(&local)
                .chain(&peers)
                .enumerate()
                .any(|(index, placement)| {
                    placement.node.instance == 0
                        || placement.node.id == fm.id
                        || placement.replica < 0
                        || placement.incarnation <= 0
                        || std::iter::once(&local)
                            .chain(&peers)
                            .take(index)
                            .any(|prior| {
                                prior.node.id == placement.node.id
                                    || prior.replica == placement.replica
                            })
                })
        {
            return Err("invalid original Node control topology".into());
        }

        Ok(Self {
            profile,
            coordinating,
            partition,
            fm,
            local,
            peers,
        })
    }

    fn successor(&self) -> Result<(i64, i64)> {
        if self.profile != Profile::StatefulPeerIdle {
            return Err("replacement outside the original two-node profile is gated".into());
        }
        let peer = self.peers.first().ok_or("missing original peer")?;
        let destination_instance = if self.coordinating {
            peer.incarnation
        } else {
            self.local.incarnation
        };
        let next = self
            .local
            .replica
            .max(peer.replica)
            .max(destination_instance)
            .checked_add(1)
            .ok_or("Idle replica identity exhausted")?;
        Ok((next, next))
    }

    fn description(&self, unit: &FailoverUnit, service: &Service) -> Result<()> {
        if unit.id != self.partition {
            return Err("partition is outside the original Node control topology".into());
        }
        self.profile.check_service_sizes(unit, service)
    }

    fn initial_target(&self, node: &Node, body: &AddInstance, deleting: bool) -> Result<()> {
        self.description(&body.unit, &body.service)?;
        if node.snapshot(body.unit.id.guid.0).committed.is_none()
            && (body.replica.node != self.local.node
                || body.replica.id != self.local.replica
                || if deleting {
                    !(0..=self.local.incarnation).contains(&body.replica.instance)
                } else {
                    body.replica.instance != self.local.incarnation
                })
        {
            return Err("initial replica is outside the original Node control topology".into());
        }
        Ok(())
    }

    fn fixed_target(
        &self,
        partition: &PartitionId,
        replica: &Replica,
        placement: &Placement,
        identity: (i64, i64),
    ) -> Result<()> {
        if partition != &self.partition
            || replica.node != placement.node
            || (replica.id, replica.instance) != identity
        {
            return Err(
                "peer body is outside the original fixed partition/replica topology".into(),
            );
        }
        Ok(())
    }

    fn idle_target(
        &self,
        partition: &PartitionId,
        replica: &Replica,
        placement: &Placement,
    ) -> Result<()> {
        let original = (placement.replica, placement.incarnation);
        let incoming = (replica.id, replica.instance);
        let expected = if incoming == original
            || (self.profile.mesh_peers() && incoming.0 > original.0 && incoming.1 > original.1)
        {
            // The Node/RA validates the exact current or freshly admitted lifetime.
            incoming
        } else {
            self.successor()?
        };
        self.fixed_target(partition, replica, placement, expected)
    }

    pub fn fm(&self, node: &mut Node, message: &Message, stopping: bool) -> Result<Admission> {
        wire::predecode(message)?;
        let mut headers = message
            .headers
            .iter()
            .filter(|header| header.id == GENERATION_HEADER);
        let generation: GenerationHeader =
            wire::decode(&headers.next().ok_or("missing FM generation")?.value)?;
        if headers.next().is_some()
            || generation.for_fm_replica
            || generation.number != node.generation_state().receive
        {
            return Err("FM generation must match the Node's admitted receive generation".into());
        }
        let action = message.properties().map_err(err)?.action;
        if stopping
            && !matches!(
                action.as_str(),
                "DeleteReplica"
                    | "RemoveInstance"
                    | "ReplicaUpReply"
                    | "ServiceTypeNotificationReply"
                    | "ServiceTypeEnabledReply"
                    | "ServiceTypeDisabledReply"
                    | "ReplicaEndpointUpdatedReply"
            )
        {
            return Err("new lifecycle intent is not admitted during shutdown".into());
        }
        let mut acknowledgement = None;
        let result = match action.as_str() {
            "ServiceTypeEnabledReply" | "ServiceTypeDisabledReply" => {
                let enabled = action == "ServiceTypeEnabledReply";
                let (body, generation) = if enabled {
                    decode_service_type_enabled_reply(message)
                } else {
                    decode_service_type_disabled_reply(message)
                }
                .map_err(err)?;
                node.fm_work(FmWork::TypeStatusReply(
                    enabled,
                    body,
                    self.fm.clone(),
                    generation,
                ))
            }
            "NodeActivateRequest" => {
                let (body, generation) = decode_node_activate_request(message).map_err(err)?;
                node.fm_work(FmWork::NodeActivate(body, self.fm.clone(), generation))
            }
            "NodeDeactivateRequest" => {
                let (body, generation) = decode_node_deactivate_request(message).map_err(err)?;
                node.fm_work(FmWork::NodeDeactivate(body, self.fm.clone(), generation))
            }
            "NodeUpdateServiceRequest" => {
                let (body, generation) =
                    decode_node_update_service_request(message).map_err(err)?;
                node.fm_work(FmWork::NodeUpdateService(
                    Box::new(body),
                    self.fm.clone(),
                    generation,
                ))
            }
            "AddInstance" => {
                let (body, generation) = decode_add_instance(message).map_err(err)?;
                self.initial_target(node, &body, false)?;
                node.add_from_fm(body, &self.fm, &generation)
            }
            "AddPrimary" => {
                let (body, generation) = decode_add_primary(message).map_err(err)?;
                self.initial_target(node, &body, false)?;
                node.add_primary_from_fm(body, &self.fm, &generation)
            }
            "DoReconfiguration" => {
                if !self.profile.peer_replicas()
                    || (!self.coordinating && !self.profile.mesh_peers())
                {
                    return Err(
                        "DoReconfiguration requires the original coordinating Primary".into(),
                    );
                }
                let (body, generation) = decode_do_reconfiguration(message).map_err(err)?;
                self.description(&body.configuration.unit, &body.configuration.service)?;
                node.reconfigure_from_fm(body, &self.fm, &generation)
            }
            "ReplicaEndpointUpdatedReply" => {
                let (body, generation) =
                    decode_replica_endpoint_updated_reply(message).map_err(err)?;
                node.endpoint_reply_from_fm(body, &self.fm, &generation)
            }
            "AddReplica" | "RemoveReplica" => {
                let removing = action == "RemoveReplica";
                let (body, generation) = if removing {
                    decode_remove_replica(message)
                } else {
                    decode_add_replica(message)
                }
                .map_err(err)?;
                self.description(&body.unit, &body.service)?;
                let peer = self
                    .peers
                    .iter()
                    .find(|peer| peer.node == body.replica.node)
                    .filter(|_| self.coordinating || self.profile.mesh_peers())
                    .ok_or("replica control requires the original coordinating Primary")?;
                self.idle_target(&body.unit.id, &body.replica, peer)?;
                if removing {
                    node.remove_replica_from_fm(body, &self.fm, &generation)
                } else {
                    node.add_replica_from_fm(body, &self.fm, &generation)
                }
            }
            "RemoveInstance" => {
                let (body, generation) = decode_remove_instance(message).map_err(err)?;
                self.description(&body.replica.0.unit, &body.replica.0.service)?;
                node.fm_work(FmWork::Remove(Box::new(body), self.fm.clone(), generation))
            }
            "DeleteReplica" => {
                let (body, generation) = decode_delete_replica(message).map_err(err)?;
                self.initial_target(node, &body.replica.0, true)?;
                if self.profile == Profile::StatefulPeerIdle && !self.coordinating {
                    let incoming = &body.replica.0;
                    let (id, expected_instance) = if incoming.replica.id == self.local.replica {
                        (self.local.replica, self.local.incarnation)
                    } else {
                        self.successor()?
                    };
                    let instance = if incoming.replica.instance < expected_instance
                        && node
                            .snapshot(incoming.unit.id.guid.0)
                            .committed
                            .is_some_and(|state| state.peer_creation.is_some())
                    {
                        incoming.replica.instance
                    } else {
                        expected_instance
                    };
                    self.fixed_target(
                        &incoming.unit.id,
                        &incoming.replica,
                        &self.local,
                        (id, instance),
                    )?;
                }
                node.fm_work(FmWork::DeleteReplica(
                    Box::new(body),
                    self.fm.clone(),
                    generation,
                ))
            }
            "ReplicaUpReply" => {
                let (body, generation) = decode_replica_up_reply(message).map_err(err)?;
                acknowledgement = Some(SemanticAck::Report(body.clone()));
                node.fm_work(FmWork::ReplicaUpReply(body, self.fm.clone(), generation))
            }
            "ServiceTypeNotificationReply" => {
                let (body, generation) =
                    decode_service_type_notification_reply(message).map_err(err)?;
                acknowledgement = Some(SemanticAck::Notification(body.clone()));
                node.fm_work(FmWork::ServiceTypeReply(body, self.fm.clone(), generation))
            }
            _ => return Err(format!("unsupported FM command: {action}")),
        };
        match result {
            Ok(id) => Ok(Admission::Submitted(acknowledgement.map(|ack| (id, ack)))),
            Err(Error::Capacity) => Ok(Admission::Retained),
            Err(error) => Err(err(error)),
        }
    }

    pub fn peer(
        &self,
        node: &mut Node,
        source: &NodeInstance,
        message: &Message,
        stopping: bool,
    ) -> Result<Option<u64>> {
        wire::predecode(message)?;
        let peer = self
            .peers
            .iter()
            .find(|peer| &peer.node == source)
            .ok_or("unbound peer ingress")?;
        let action = message.properties().map_err(err)?.action;
        if stopping
            && !matches!(
                action.as_str(),
                "CreateReplicaReply" | "GetLSNReply" | "DeactivateReply" | "ActivateReply"
            )
        {
            return Err("new peer lifecycle intent is not admitted during shutdown".into());
        }
        let result = match action.as_str() {
            "DoReconfiguration" if !self.coordinating && !self.profile.mesh_peers() => {
                let (body, generation) = decode_do_reconfiguration(message).map_err(err)?;
                node.continue_swap_from_peer(body, source, &generation)
            }
            "GetLSN" => {
                let (body, generation) = decode_get_lsn(message).map_err(err)?;
                node.get_lsn_from_peer(body, source, &generation)
            }
            "GetLSNReply" => {
                let (body, generation) = decode_get_lsn_reply(message).map_err(err)?;
                node.get_lsn_reply_from_peer(body, source, &generation)
            }
            "Deactivate" if !self.coordinating || self.profile.mesh_peers() => {
                let (body, generation) = decode_deactivate(message).map_err(err)?;
                self.description(&body.configuration.unit, &body.configuration.service)?;
                node.deactivate_replica_from_peer(body, source, &generation)
            }
            "DeactivateReply" if self.coordinating || self.profile.mesh_peers() => {
                let (body, generation) = decode_deactivate_reply(message).map_err(err)?;
                self.idle_target(&body.unit.id, &body.replica, peer)?;
                node.deactivate_reply_from_peer(body, source, &generation)
            }
            "Activate" => {
                let (body, generation) = decode_activate(message).map_err(err)?;
                self.description(&body.configuration.unit, &body.configuration.service)?;
                node.activate_replica_from_peer(body, source, &generation)
            }
            "ActivateReply" => {
                let (body, generation) = decode_activate_reply(message).map_err(err)?;
                self.idle_target(&body.unit.id, &body.replica, peer)?;
                node.activate_reply_from_peer(body, source, &generation)
            }
            "CreateReplica" if !self.coordinating || self.profile.mesh_peers() => {
                let (body, generation) = decode_create_replica(message).map_err(err)?;
                self.description(&body.replica.0.unit, &body.replica.0.service)?;
                self.idle_target(
                    &body.replica.0.unit.id,
                    &body.replica.0.replica,
                    &self.local,
                )?;
                node.create_replica_from_peer(body, source, &generation)
            }
            "CreateReplicaReply" if self.coordinating || self.profile.mesh_peers() => {
                let (body, generation) = decode_create_replica_reply(message).map_err(err)?;
                self.idle_target(&body.unit.id, &body.replica, peer)?;
                node.create_replica_reply_from_peer(body, source, &generation)
            }
            _ => {
                return Err(format!(
                    "unsupported peer action for original placement: {action}"
                ))
            }
        };
        match result {
            Ok(id) => Ok(Some(id)),
            Err(Error::Capacity) => Ok(None),
            Err(Error::Busy) if action == "CreateReplica" => Ok(None),
            Err(error) => Err(err(error)),
        }
    }

    pub async fn generation(
        &self,
        node: &Node,
        link: &wire::Link,
        incoming: &mut IncomingMessage<GatewayTarget>,
    ) -> Result<bool> {
        let message = &incoming.message;
        match message.properties().map_err(err)?.action.as_str() {
            "GenerationProposal" => {
                wire::predecode(message)?;
                let header = decode_generation_proposal(message).map_err(err)?;
                let body = node
                    .process_generation_proposal(&self.fm, &header)
                    .map_err(err)?;
                let reply = generation_proposal_reply_message(&body).map_err(err)?;
                incoming
                    .reply
                    .take()
                    .ok_or("GenerationProposal missing consuming request context")?
                    .reply(link.seal(reply)?, wire::deadline())
                    .await
                    .map_err(err)?;
            }
            "GenerationUpdate" => {
                wire::predecode(message)?;
                let (body, header) = decode_generation_update(message).map_err(err)?;
                match node
                    .process_generation_update(&self.fm, &body, &header)
                    .map_err(err)?
                {
                    ra::GenerationUpdateOutcome::Rejected(proposed) => {
                        link.send(generation_update_reject_message(&proposed).map_err(err)?)
                            .await?;
                    }
                    ra::GenerationUpdateOutcome::UploadRequired => link.advance_generation(&header),
                    _ => {}
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
