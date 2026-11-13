use crate::{config::Placement, err, Config, Result};
use fube_node::{
    control::{wire, SemanticAck},
    local::{FmWork, Node},
};
use fube_ra::protocol::*;
use fube_transport::message::Message;

pub(crate) enum Admission {
    Retained,
    Submitted(Option<(u64, SemanticAck)>),
}

enum Creation {
    Primary(AddPrimary),
    Idle(NodeInstance, CreateReplica),
}

#[derive(Default)]
pub(crate) struct Policy {
    creation: Option<Creation>,
    builds: [Option<AddReplica>; 2],
    configuration: Option<DoReconfiguration>,
    activation: Option<Activate>,
}

fn generation(config: &Config, value: &GenerationHeader) -> Result<()> {
    if value != &config.generation()? {
        return Err("standalone profile requires its exact original FM generation".into());
    }
    Ok(())
}

fn replica(config: &Config, value: &Replica, placement: &Placement, deleting: bool) -> Result<()> {
    if value.node != placement.identity.native()?
        || value.id != placement.replica_id
        || if deleting {
            value.instance > placement.replica_instance || value.instance < 0
        } else {
            value.instance != placement.replica_instance
        }
    {
        return Err(format!(
            "foreign or replacement replica for {}",
            config.name
        ));
    }
    Ok(())
}

fn unit(config: &Config, value: &FailoverUnit) -> Result<()> {
    if value.id != config.partition_id()? || value.target != 3 || value.minimum != 2 {
        return Err(
            "standalone profile requires the original partition and immutable 3/2 sizes".into(),
        );
    }
    Ok(())
}

fn service(value: &Service) -> Result<()> {
    if !value.stateful || value.persisted || value.target != 3 || value.minimum != 2 {
        return Err("standalone profile supports only the original volatile 3/2 service".into());
    }
    Ok(())
}

fn membership(config: &Config, value: &Configuration, primary: &NodeInstance) -> Result<()> {
    unit(config, &value.unit)?;
    service(&value.service)?;
    if value.replicas.0.len() != 3 {
        return Err("initial admission requires all three original placements".into());
    }
    for placement in std::iter::once(&config.node).chain(&config.peers) {
        let expected = placement.identity.native()?;
        let members: Vec<_> = value
            .replicas
            .0
            .iter()
            .filter(|member| member.node == expected)
            .collect();
        if members.len() != 1 {
            return Err("missing or duplicate original configuration member".into());
        }
        let member = members[0];
        replica(config, member, placement, false)?;
        let (previous, current) = if &expected == primary { (4, 4) } else { (2, 3) };
        if (member.previous_role, member.current_role) != (previous, current)
            || !member.up
            || member.state != 2
        {
            return Err("only healthy initial unchanged-Primary admission is supported".into());
        }
    }
    Ok(())
}

fn admitted(result: fube_node::local::Result<u64>) -> Result<Option<u64>> {
    match result {
        Ok(id) => Ok(Some(id)),
        Err(fube_node::local::Error::Capacity) => Ok(None),
        Err(error) => Err(err(error)),
    }
}

impl Policy {
    pub fn fm(
        &mut self,
        config: &Config,
        node: &mut Node,
        message: &Message,
        stopping: bool,
    ) -> Result<Admission> {
        wire::predecode(message)?;
        let action = message.properties().map_err(err)?.action;
        if stopping
            && !matches!(
                action.as_str(),
                "DeleteReplica"
                    | "ReplicaUpReply"
                    | "ServiceTypeNotificationReply"
                    | "ServiceTypeEnabledReply"
                    | "ServiceTypeDisabledReply"
                    | "ReplicaEndpointUpdatedReply"
            )
        {
            return Err("new lifecycle intent is not admitted during shutdown".into());
        }

        let fm = config.fm.identity.native()?;
        let mut acknowledgement = None;
        let id = match action.as_str() {
            "AddPrimary" => {
                let (body, header) = decode_add_primary(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.unit)?;
                service(&body.service)?;
                replica(config, &body.replica, &config.node, false)?;
                if self.creation.as_ref().is_some_and(|creation| {
                    !matches!(creation, Creation::Primary(original) if original == &body)
                }) {
                    return Err("original creation cannot change role, identity or body".into());
                }
                let id = admitted(node.add_primary_from_fm(body.clone(), &fm, &header))
                    .map_err(|error| format!(
                        "AddPrimary admission: {error}; expected version={:?}, received version={:?}, service/application match={}",
                        node.activation().premise().map(|package| &package.version),
                        body.service.version,
                        node.activation().premise().is_some_and(|package|
                            package.service_name == body.service.name && package.application_name == body.service.application)))?;
                if id.is_some() && self.creation.is_none() {
                    self.creation = Some(Creation::Primary(body));
                }
                id
            }
            "AddReplica" => {
                let (body, header) = decode_add_replica(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.unit)?;
                service(&body.service)?;
                if !matches!(self.creation, Some(Creation::Primary(_))) {
                    return Err("only the original Primary can accept FM build intent".into());
                }
                let index = config
                    .peers
                    .iter()
                    .position(|peer| {
                        peer.identity
                            .native()
                            .is_ok_and(|identity| identity == body.replica.node)
                    })
                    .ok_or("build destination is not an original peer")?;
                replica(config, &body.replica, &config.peers[index], false)?;
                if self.builds[index]
                    .as_ref()
                    .is_some_and(|original| original != &body)
                {
                    return Err("initial build intent cannot be replaced".into());
                }
                let id = admitted(node.add_replica_from_fm(body.clone(), &fm, &header))?;
                if id.is_some() {
                    self.builds[index] = Some(body);
                }
                id
            }
            "DoReconfiguration" => {
                let (body, header) = decode_do_reconfiguration(message).map_err(err)?;
                generation(config, &header)?;
                if !matches!(self.creation, Some(Creation::Primary(_))) {
                    return Err(
                        "only the original Primary accepts initial FM reconfiguration".into(),
                    );
                }
                if matches!(&self.creation, Some(Creation::Primary(original))
                    if original.unit.current.data_loss != body.configuration.unit.current.data_loss)
                {
                    return Err("data-loss epoch changes are not supported".into());
                }
                membership(config, &body.configuration, &config.node.identity.native()?)?;
                if self
                    .configuration
                    .as_ref()
                    .is_some_and(|original| original != &body)
                {
                    return Err(
                        "only exact retries of the initial configuration are supported".into(),
                    );
                }
                let id = admitted(node.reconfigure_from_fm(body.clone(), &fm, &header))?;
                if id.is_some() {
                    self.configuration = Some(body);
                }
                id
            }
            "DeleteReplica" => {
                let (body, header) = decode_delete_replica(message).map_err(err)?;
                generation(config, &header)?;
                if body.deletion.0.force {
                    return Err("force deletion is outside the healthy-startup profile".into());
                }
                unit(config, &body.replica.0.unit)?;
                replica(config, &body.replica.0.replica, &config.node, true)?;
                admitted(node.fm_work(FmWork::DeleteReplica(Box::new(body), fm, header)))?
            }
            "ServiceTypeEnabledReply" | "ServiceTypeDisabledReply" => {
                let enabled = action == "ServiceTypeEnabledReply";
                let (body, header) = if enabled {
                    decode_service_type_enabled_reply(message)
                } else {
                    decode_service_type_disabled_reply(message)
                }
                .map_err(err)?;
                generation(config, &header)?;
                admitted(node.fm_work(FmWork::TypeStatusReply(enabled, body, fm, header)))?
            }
            "ReplicaUpReply" => {
                let (body, header) = decode_replica_up_reply(message).map_err(err)?;
                generation(config, &header)?;
                let id = admitted(node.fm_work(FmWork::ReplicaUpReply(body.clone(), fm, header)))?;
                acknowledgement = Some(SemanticAck::Report(body));
                id
            }
            "ServiceTypeNotificationReply" => {
                let (body, header) =
                    decode_service_type_notification_reply(message).map_err(err)?;
                generation(config, &header)?;
                let id =
                    admitted(node.fm_work(FmWork::ServiceTypeReply(body.clone(), fm, header)))?;
                acknowledgement = Some(SemanticAck::Notification(body));
                id
            }
            "ReplicaEndpointUpdatedReply" => {
                let (body, header) = decode_replica_endpoint_updated_reply(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.unit)?;
                replica(config, &body.replica, &config.node, false)?;
                admitted(node.endpoint_reply_from_fm(body, &fm, &header))?
            }
            _ => return Err(format!("unsupported healthy-startup FM action: {action}")),
        };
        Ok(match id {
            Some(id) => Admission::Submitted(acknowledgement.map(|ack| (id, ack))),
            None => Admission::Retained,
        })
    }

    pub fn peer(
        &mut self,
        config: &Config,
        placement: &Placement,
        node: &mut Node,
        message: &Message,
        stopping: bool,
    ) -> Result<Option<u64>> {
        wire::predecode(message)?;
        let action = message.properties().map_err(err)?.action;
        let source = placement.identity.native()?;
        if stopping && !matches!(action.as_str(), "CreateReplicaReply" | "ActivateReply") {
            return Err("new peer lifecycle intent is not admitted during shutdown".into());
        }
        match action.as_str() {
            "CreateReplica" => {
                let (body, header) = decode_create_replica(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.replica.0.unit)?;
                service(&body.replica.0.service)?;
                replica(config, &body.replica.0.replica, &config.node, false)?;
                if self.creation.as_ref().is_some_and(|creation| {
                    !matches!(creation, Creation::Idle(peer, original) if peer == &source && original == &body)
                }) {
                    return Err("original peer creation cannot be superseded".into());
                }
                let id = match node.create_replica_from_peer(body.clone(), &source, &header) {
                    Err(fube_node::local::Error::Busy) => None,
                    result => admitted(result)?,
                };
                if id.is_some() && self.creation.is_none() {
                    self.creation = Some(Creation::Idle(source, body));
                }
                Ok(id)
            }
            "CreateReplicaReply" => {
                let (body, header) = decode_create_replica_reply(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.unit)?;
                replica(config, &body.replica, placement, false)?;
                admitted(node.create_replica_reply_from_peer(body, &source, &header))
            }
            "Activate" => {
                let (body, header) = decode_activate(message).map_err(err)?;
                generation(config, &header)?;
                if !matches!(&self.creation, Some(Creation::Idle(peer, _)) if peer == &source) {
                    return Err("Activate must come from the original creating Primary".into());
                }
                membership(config, &body.configuration, &source)?;
                if self
                    .activation
                    .as_ref()
                    .is_some_and(|original| original != &body)
                {
                    return Err("only the original peer activation can retry".into());
                }
                let id = admitted(node.activate_replica_from_peer(body.clone(), &source, &header))?;
                if id.is_some() {
                    self.activation = Some(body);
                }
                Ok(id)
            }
            "ActivateReply" => {
                let (body, header) = decode_activate_reply(message).map_err(err)?;
                generation(config, &header)?;
                unit(config, &body.unit)?;
                replica(config, &body.replica, placement, false)?;
                admitted(node.activate_reply_from_peer(body, &source, &header))
            }
            _ => Err(format!("unsupported healthy-startup peer action: {action}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initial() -> (Config, Configuration) {
        let config: Config = serde_json::from_value(serde_json::json!({
            "schema_version": 1, "deployment": uuid::Uuid::new_v4().to_string(), "name": "node2",
            "node": {"identity": {"id": "2", "instance": 1}, "address": "127.0.0.2:19002",
                "replica_id": 20, "replica_instance": 1},
            "fm": {"identity": {"id": "f", "instance": 1}, "address": "127.0.0.1:19001", "generation": 1},
            "peers": [
                {"identity": {"id": "1", "instance": 1}, "address": "127.0.0.1:19002", "replica_id": 10, "replica_instance": 1},
                {"identity": {"id": "3", "instance": 1}, "address": "127.0.0.3:19002", "replica_id": 30, "replica_instance": 1}
            ],
            "partition": uuid::Uuid::new_v4().to_string(), "package": "/pkg",
            "application": "fabric:/External", "package_version_instance": 1, "run_directory": "/run/new-node"
        })).unwrap();
        config.validate().unwrap();
        let id = config.partition_id().unwrap();
        let replicas = std::iter::once(&config.node)
            .chain(&config.peers)
            .map(|placement| {
                let primary = placement.identity.id == config.node.identity.id;
                Replica {
                    node: placement.identity.native().unwrap(),
                    id: placement.replica_id,
                    instance: placement.replica_instance,
                    previous_role: if primary { 4 } else { 2 },
                    current_role: if primary { 4 } else { 3 },
                    state: 2,
                    ..Replica::default()
                }
            })
            .collect();
        let body = Configuration {
            unit: FailoverUnit {
                id: id.clone(),
                consistency: ConsistencyUnit {
                    id,
                    ..Default::default()
                },
                target: 3,
                minimum: 2,
                ..Default::default()
            },
            service: Service {
                stateful: true,
                target: 3,
                minimum: 2,
                ..Default::default()
            },
            replicas: fube_transport::serialization::serde::ObjectArray(replicas),
        };
        (config, body)
    }

    #[test]
    fn initial_membership_rejects_resize_replacement_duplicate_and_non_idle_admission() {
        let (config, body) = initial();
        let primary = config.node.identity.native().unwrap();
        membership(&config, &body, &primary).unwrap();
        for case in 0..5 {
            let mut invalid = body.clone();
            match case {
                0 => invalid.service.target = 4,
                1 => invalid.replicas.0[1].instance += 1,
                2 => invalid.replicas.0[2] = invalid.replicas.0[1].clone(),
                3 => invalid.replicas.0[1].previous_role = 3,
                _ => invalid.replicas.0[0].current_role = 3,
            }
            assert!(membership(&config, &invalid, &primary).is_err());
        }
    }

    #[test]
    fn ordinary_fm_generation_must_match_owner_number_and_scope_exactly() {
        let (config, _) = initial();
        let header = config.generation().unwrap();
        generation(&config, &header).unwrap();
        for case in 0..3 {
            let mut invalid = header.clone();
            match case {
                0 => invalid.number.generation += 1,
                1 => invalid.number.owner = [99; 16],
                _ => invalid.for_fm_replica = true,
            }
            assert!(generation(&config, &invalid).is_err());
        }
    }
}
