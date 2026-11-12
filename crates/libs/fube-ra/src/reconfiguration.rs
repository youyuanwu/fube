use super::*;
use fube_transport::serialization::serde::ObjectArray;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationStage {
    Catchup,
    Deactivate,
    Deactivated,
    Retiring,
    Activate,
    End,
    Secondary,
    Complete,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Reconfiguration {
    pub request: Configuration,
    pub secondary_loss: bool,
    pub stage: ConfigurationStage,
    pub error: i64,
    pub history: Vec<Configuration>,
    pub deactivated: Vec<NodeInstance>,
    pub activated: Vec<NodeInstance>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationCompletion {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub replicas: ObjectArray<Vec<Replica>>,
    pub flags: i64,
    pub error: i64,
}

pub fn secondary_service(old: &Service, new: &Service) -> bool {
    let mut expected = old.clone();
    expected.target = if old.target == 1 { 2 } else { 1 };
    let Some(version) = old.update_version.checked_add(1) else {
        return false;
    };
    expected.update_version = version;
    if matches!(old.target, 1 | 2) && old.minimum == 1 && *new == expected {
        return true;
    }
    expected.target = 3;
    expected.minimum = 2;
    old.target == 1 && old.minimum == 1 && *new == expected
}
pub fn retired_configuration(state: &EntityState) -> bool {
    state.reconfiguration.as_ref().is_some_and(|config| {
        config.stage == ConfigurationStage::Complete && config.request.unit.target == 1
    })
}
pub(super) fn secondary_removal(body: &Configuration) -> bool {
    body.replicas
        .0
        .iter()
        .filter(|r| r.previous_role == 3 && r.current_role == 1 && !r.up && r.state == 3)
        .count()
        == 1
}
pub fn completed_secondary_removal(state: &EntityState) -> bool {
    state.reconfiguration.as_ref().is_some_and(|c| {
        c.secondary_loss && c.stage == ConfigurationStage::Complete && secondary_removal(&c.request)
    })
}
pub fn secondary_removal_in_progress(state: &EntityState) -> bool {
    state.reconfiguration.as_ref().is_some_and(|c| {
        c.secondary_loss
            && secondary_removal(&c.request)
            && matches!(
                c.stage,
                ConfigurationStage::Deactivated | ConfigurationStage::Secondary
            )
    })
}
fn secondary_transition(body: &Configuration, old: &EntityState) -> bool {
    secondary_removal(body)
        || completed_secondary_removal(old)
        || old
            .reconfiguration
            .as_ref()
            .is_some_and(|c| c.secondary_loss && c.request == *body)
        || (body.service == old.body.service
            && old.body.replica.current_role == 2
            && old.peer_creation.as_ref().is_some_and(|p| {
                p.deactivation.epoch != old.body.unit.current
                    && p.deactivation.epoch.data_loss == old.body.unit.current.data_loss
                    && p.deactivation.epoch.configuration & !0xffff_ffff
                        == old.body.unit.current.configuration & !0xffff_ffff
            }))
}
fn stable(request: &Configuration) -> Configuration {
    let mut body = request.clone();
    body.unit.previous = Epoch::default();
    for replica in &mut body.replicas.0 {
        replica.previous_role = 1;
    }
    body
}
fn local<'a>(body: &'a Configuration, node: &NodeInstance) -> Result<&'a Replica> {
    body.replicas
        .0
        .iter()
        .find(|replica| &replica.node == node)
        .ok_or(Failure::Admission("configuration omitted exact local node"))
}
fn initial_configuration(body: &Configuration, old: &EntityState) -> bool {
    old.reconfiguration.is_none()
        && old.body.replica.current_role == if old.fm_primary { 4 } else { 2 }
        && body.service == old.body.service
        && valid_stateful_sizes(&body.service)
        && body.replicas.0.len() == body.service.target as usize
        && body.replicas.0.len() >= 2
        && body
            .replicas
            .0
            .iter()
            .all(|r| matches!((r.previous_role, r.current_role), (4, 4) | (2, 3)))
}
fn shape(body: &Configuration, old: &EntityState, primary: &NodeInstance) -> Result<()> {
    let old_epoch = old.body.unit.current;
    if secondary_removal(body) {
        let prior = old
            .reconfiguration
            .as_ref()
            .filter(|c| c.stage == ConfigurationStage::Complete)
            .ok_or(Failure::Unsupported(
                "Secondary removal requires completed membership",
            ))?;
        let members: Vec<_> = prior
            .request
            .replicas
            .0
            .iter()
            .filter(|r| r.current_role >= 3)
            .collect();
        if body.service != old.body.service
            || !valid_stateful_sizes(&body.service)
            || body.unit.target != body.service.target
            || body.unit.minimum != body.service.minimum
            || body.unit.id != old.body.unit.id
            || body.unit.consistency != old.body.unit.consistency
            || body.unit.previous != old_epoch
            || body.unit.current.data_loss != old_epoch.data_loss
            || Some(body.unit.current.configuration) != old_epoch.configuration.checked_add(1)
            || body.unit.current.configuration & !0xffff_ffff
                != old_epoch.configuration & !0xffff_ffff
            || body.replicas.0.len() != members.len()
            || members.len() != body.service.target as usize
            || members.len() <= body.service.minimum as usize
            || local(&prior.request, primary)?.current_role != 4
        {
            return Err(Failure::Unsupported(
                "Secondary removal must preserve sizes, Primary and minimum",
            ));
        }
        for expected in members {
            let actual = local(body, &expected.node)?;
            let mut normalized = actual.clone();
            normalized.previous_role = expected.previous_role;
            normalized.current_role = expected.current_role;
            normalized.up = expected.up;
            normalized.state = expected.state;
            normalized.first_lsn = expected.first_lsn;
            normalized.last_lsn = expected.last_lsn;
            if normalized != *expected
                || actual.previous_role != expected.current_role
                || body
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.node.id == expected.node.id)
                    .count()
                    != 1
                || if actual.current_role == 1 {
                    expected.current_role != 3 || actual.up || actual.state != 3
                } else {
                    actual.current_role != expected.current_role || !actual.up || actual.state != 2
                }
            {
                return Err(Failure::Admission(
                    "Secondary removal changed a replica owner or live role",
                ));
            }
        }
        return Ok(());
    }
    let initial = initial_configuration(body, old);
    let repair = !initial
        && valid_stateful_sizes(&body.service)
        && body.unit.target >= 2
        && body.service == old.body.service;
    let three = body.unit.target == 3 && old.body.service.target == 1;
    if (!initial
        && body.replicas.0.len()
            != if repair {
                body.service.target as usize
            } else if three {
                3
            } else {
                2
            })
        || body.unit.id != old.body.unit.id
        || body.unit.consistency != old.body.unit.consistency
        || body.unit.previous != old_epoch
        || body.unit.current.data_loss != old_epoch.data_loss
        || body.unit.current.configuration
            != old_epoch
                .configuration
                .checked_add(1)
                .ok_or(Failure::Capacity)?
        || body.unit.current.configuration & !0xffff_ffff != old_epoch.configuration & !0xffff_ffff
        || body.unit.target != body.service.target
        || (!initial && !repair && body.unit.minimum != if three { 2 } else { 1 })
        || body.unit.minimum != body.service.minimum
        || (!initial && !repair && !secondary_service(&old.body.service, &body.service))
    {
        return Err(Failure::Unsupported(
            "only initial configured admission, unchanged-size repair or qualified legacy configuration is enabled",
        ));
    }
    let p = local(body, primary)?;
    if p.current_role != 4
        || p.previous_role != 4
        || body.replicas.0.iter().enumerate().any(|(i, r)| {
            !r.up
                || r.state != 2
                || r.id < 0
                || r.instance < 0
                || r.version != body.service.version
                || r.replication_endpoint.is_empty()
                || body.replicas.0[..i]
                    .iter()
                    .any(|old| old.id == r.id || old.node.id == r.node.id)
                || (r.node != *primary
                    && if body.unit.target >= 2 {
                        r.current_role != 3
                            || !(r.previous_role == 2 || (repair && r.previous_role == 3))
                    } else {
                        r.current_role != 1 || r.previous_role != 3
                    })
        })
    {
        return Err(Failure::Unsupported(
            "not the exact ready P/P and I/S configuration",
        ));
    }
    if repair {
        if body
            .replicas
            .0
            .iter()
            .filter(|r| r.previous_role == 2)
            .count()
            != 1
            || body
                .replicas
                .0
                .iter()
                .filter(|r| r.previous_role == 3)
                .count()
                != body.replicas.0.len() - 2
        {
            return Err(Failure::Admission(
                "repair must retain P/P and every S/S and admit one I/S",
            ));
        }
        if completed_secondary_removal(old) {
            let prior = &old
                .reconfiguration
                .as_ref()
                .expect("completed removal")
                .request;
            for expected in prior.replicas.0.iter().filter(|r| r.current_role >= 3) {
                let mut actual = local(body, &expected.node)?.clone();
                if actual.previous_role != expected.current_role {
                    return Err(Failure::Admission(
                        "repair changed the surviving previous configuration",
                    ));
                }
                actual.previous_role = expected.previous_role;
                actual.first_lsn = expected.first_lsn;
                actual.last_lsn = expected.last_lsn;
                if actual != *expected {
                    return Err(Failure::Admission(
                        "repair changed a surviving replica owner",
                    ));
                }
            }
            let lost = prior
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 1)
                .expect("removed Secondary");
            let fresh = local(body, &lost.node)?;
            if fresh.previous_role != 2 || fresh.id <= lost.id || fresh.instance <= lost.instance {
                return Err(Failure::Admission(
                    "Secondary repair requires a fresh incarnation",
                ));
            }
        } else if let Some(swap) = old
            .swap
            .as_ref()
            .filter(|_| completed_primary_failover(old))
        {
            if old
                .reconfiguration
                .as_ref()
                .is_some_and(|c| swap.body.unit.current.older_than(c.request.unit.current))
            {
                return Err(Failure::Unsupported(
                    "a second post-failover repair is gated",
                ));
            }
            for expected in &swap.body.replicas.0 {
                let mut actual = local(body, &expected.node)?.clone();
                if actual.previous_role != expected.current_role {
                    return Err(Failure::Admission(
                        "repair changed the surviving previous configuration",
                    ));
                }
                actual.previous_role = expected.previous_role;
                actual.first_lsn = expected.first_lsn;
                actual.last_lsn = expected.last_lsn;
                if actual != *expected {
                    return Err(Failure::Admission(
                        "repair changed a surviving replica owner",
                    ));
                }
            }
        } else if old.body.replica.current_role != 2 {
            return Err(Failure::Unsupported(
                "repair requires completed failover or fresh Idle",
            ));
        }
    }
    Ok(())
}
fn apply_body(state: &mut EntityState, body: &Configuration, node: &NodeInstance) -> Result<()> {
    state.body = Arc::new(AddInstance {
        unit: body.unit.clone(),
        replica: local(body, node)?.clone(),
        service: body.service.clone(),
    });
    Ok(())
}
fn configuration_effect(state: &EntityState, flags: i64) -> Effect {
    Effect::UpdateConfiguration {
        body: Box::new(
            state
                .reconfiguration
                .as_ref()
                .expect("owned configuration")
                .request
                .clone(),
        ),
        local: Box::new(state.body.replica.clone()),
        flags,
        registration: state.runtime.clone().expect("open runtime"),
    }
}
fn reply(state: &EntityState) -> Effect {
    let mut body = stable(
        &state
            .reconfiguration
            .as_ref()
            .expect("owned configuration")
            .request,
    );
    if state.fm_primary {
        body.replicas.0.retain(|r| r.current_role >= 3);
        Effect::DoReconfigurationReply(ConfigurationReply {
            unit: body.unit,
            replicas: body.replicas,
            error: 0,
        })
    } else {
        Effect::PeerActivateReply {
            target: state
                .peer_creation
                .as_ref()
                .expect("peer origin")
                .source
                .clone(),
            body: Box::new(ActivateReply {
                unit: body.unit,
                replica: state.body.replica.clone(),
                error: 0,
            }),
        }
    }
}
pub(super) fn deactivate_reply(state: &EntityState) -> Effect {
    Effect::PeerDeactivateReply {
        target: state
            .peer_creation
            .as_ref()
            .expect("peer origin")
            .source
            .clone(),
        body: Box::new(DeactivateReply {
            unit: state.body.unit.clone(),
            replica: Replica {
                node: state.body.replica.node.clone(),
                id: state.body.replica.id,
                instance: state.body.replica.instance,
                up: false,
                state: 3,
                ..Replica::default()
            },
            error: 0,
        }),
    }
}
fn pending_effects(state: &EntityState) -> Vec<Effect> {
    let config = state.reconfiguration.as_ref().expect("owned configuration");
    let effect = match config.stage {
        ConfigurationStage::Catchup => configuration_effect(state, 2),
        ConfigurationStage::End => configuration_effect(state, 1),
        ConfigurationStage::Secondary => configuration_effect(state, 0),
        ConfigurationStage::Retiring => {
            if state.phase == Phase::Closed {
                deactivate_reply(state)
            } else {
                unreachable!("retiring Close uses the lifecycle retry owner")
            }
        }
        ConfigurationStage::Deactivated => deactivate_reply(state),
        ConfigurationStage::Deactivate => {
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("Primary deactivation");
            return config
                .request
                .replicas
                .0
                .iter()
                .filter(|r| r.previous_role == 3 && r.up && !config.deactivated.contains(&r.node))
                .map(|target| Effect::PeerDeactivate {
                    target: target.node.clone(),
                    body: Box::new(Deactivate {
                        configuration: config.request.clone(),
                        force: false,
                        deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                    }),
                })
                .collect();
        }
        ConfigurationStage::Activate => {
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("Primary deactivation");
            return config
                .request
                .replicas
                .0
                .iter()
                .filter(|target| {
                    target.current_role == 3 && !config.activated.contains(&target.node)
                })
                .map(|target| Effect::PeerActivate {
                    target: target.node.clone(),
                    body: Box::new(Activate {
                        configuration: config.request.clone(),
                        deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                    }),
                })
                .collect();
        }
        ConfigurationStage::Complete => reply(state),
    };
    vec![effect]
}
fn admitted(
    source: &NodeInstance,
    generation: &GenerationHeader,
    expected: &NodeInstance,
    admission: &Admission,
) -> Result<bool> {
    if source != expected || generation.for_fm_replica {
        return Err(Failure::Admission("configuration source/owner mismatch"));
    }
    if generation.number.older_than(&admission.generation) {
        return Ok(false);
    }
    if generation.number != admission.generation {
        return Err(Failure::Unsupported(
            "configuration requires the original FM generation",
        ));
    }
    Ok(true)
}
pub(super) fn transition(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
    peers: &[NodeInstance],
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let Some(current) = current else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if !matches!(work.kind, Kind::Deactivate { .. })
        && (current.phase != Phase::Ready || current.runtime.is_none())
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    match &work.kind {
        Kind::DoReconfiguration { source, generation } => {
            if !admitted(source, generation, &admission.fm_source, admission)? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: DoReconfiguration = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid DoReconfiguration"))?;
            let secondary_loss = secondary_transition(&body.configuration, current);
            if !current.fm_primary || body.phase0_duration != i64::MAX {
                return Err(Failure::Unsupported("primary transfer is gated"));
            }
            if let Some(config) = &current.reconfiguration {
                if body.configuration != config.request {
                    if body
                        .configuration
                        .unit
                        .current
                        .older_than(config.request.unit.current)
                    {
                        return Ok(Transition::ignored(Disposition::IgnoredStale));
                    }
                    if config.stage != ConfigurationStage::Complete
                        || (!secondary_loss && config.history.len() >= 2)
                        || (body.configuration.unit.target == 1 && !config.history.is_empty())
                    {
                        return Err(Failure::Unsupported("configuration supersession is gated"));
                    }
                } else {
                    return Ok(Transition {
                        update: Update::None,
                        effects: pending_effects(current),
                        disposition: Disposition::Applied,
                    });
                }
            }
            if !admission.owner_ready || !admission.node_open || !admission.node_activated {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            shape(&body.configuration, current, &admission.local_node)?;
            if secondary_loss {
                if local(&body.configuration, &admission.local_node)?.id != current.body.replica.id
                    || local(&body.configuration, &admission.local_node)?.instance
                        != current.body.replica.instance
                {
                    return Err(Failure::Admission(
                        "Secondary repair changed the Primary owner",
                    ));
                }
                if !secondary_removal(&body.configuration) {
                    let fresh: Vec<_> = current
                        .remote_idles
                        .iter()
                        .filter(|r| {
                            r.ready
                                && r.removal.is_none()
                                && r.fm_request.is_some()
                                && r.body.replica.0.unit == current.body.unit
                        })
                        .collect();
                    let [fresh] = fresh.as_slice() else {
                        return Err(Failure::Unsupported(
                            "Secondary repair requires one fresh Ready Idle",
                        ));
                    };
                    let actual = local(&body.configuration, &fresh.target)?;
                    let mut expected = fresh.reply.as_ref().expect("Ready Idle").replica.clone();
                    expected.previous_role = 2;
                    expected.current_role = 3;
                    expected.state = 2;
                    if actual != &expected {
                        return Err(Failure::Admission(
                            "Secondary repair changed its fresh build owner",
                        ));
                    }
                }
                let mut state = current.clone();
                apply_body(&mut state, &body.configuration, &admission.local_node)?;
                state.reconfiguration = Some(Reconfiguration {
                    request: body.configuration,
                    secondary_loss: true,
                    stage: ConfigurationStage::Catchup,
                    error: 0,
                    history: current
                        .reconfiguration
                        .as_ref()
                        .map_or_else(Vec::new, |c| vec![c.request.clone()]),
                    deactivated: vec![],
                    activated: vec![],
                });
                let effects = pending_effects(&state);
                return Ok(lifecycle::changed(state, effects, true));
            }
            let initial = initial_configuration(&body.configuration, current);
            let repair = completed_primary_failover(current)
                && body.configuration.service == current.body.service
                && valid_stateful_sizes(&body.configuration.service);
            if repair
                && (peers.len() + 1 < body.configuration.replicas.0.len()
                    || current.remote_idles.len() != 1)
            {
                return Err(Failure::Unsupported(
                    "repair requires exactly one freshly built Idle and every surviving peer",
                ));
            }
            if !initial
                && !repair
                && body.configuration.unit.target == 3
                && (peers.len() != 2
                    || current.reconfiguration.is_some()
                    || current.remote_idles.len() != 2)
            {
                return Err(Failure::Unsupported(
                    "three-member admission requires both original built Idles",
                ));
            }
            for remote in &current.remote_idles {
                if !remote.ready || remote.removal.is_some() {
                    return Err(Failure::Unsupported(
                        "configuration requires a successfully built FM Idle",
                    ));
                }
                let opened = remote
                    .reply
                    .as_ref()
                    .ok_or(Failure::Unsupported("missing peer Open"))?;
                let secondary = local(&body.configuration, &remote.target)?;
                let expected_remote = if body.configuration.unit.target == 1 {
                    current
                        .reconfiguration
                        .as_ref()
                        .filter(|c| {
                            c.stage == ConfigurationStage::Complete && c.request.unit.target == 2
                        })
                        .and_then(|c| {
                            c.request
                                .replicas
                                .0
                                .iter()
                                .find(|r| r.node == remote.target)
                        })
                        .ok_or(Failure::Unsupported(
                            "retirement requires completed active membership",
                        ))?
                } else {
                    &opened.replica
                };
                if !peers.contains(&remote.target)
                    || remote.fm_request.is_none()
                    || secondary.id != expected_remote.id
                    || secondary.instance != expected_remote.instance
                    || secondary.replication_endpoint != expected_remote.replication_endpoint
                    || secondary.service_endpoint != expected_remote.service_endpoint
                    || local(&body.configuration, &admission.local_node)?.id
                        != current.body.replica.id
                    || local(&body.configuration, &admission.local_node)?.instance
                        != current.body.replica.instance
                    || local(&body.configuration, &admission.local_node)?.replication_endpoint
                        != current.body.replica.replication_endpoint
                    || local(&body.configuration, &admission.local_node)?.service_endpoint
                        != current.body.replica.service_endpoint
                {
                    return Err(Failure::Admission(
                        "configuration changed original replica ownership",
                    ));
                }
            }
            if !repair && current.remote_idles.len() != body.configuration.replicas.0.len() - 1 {
                return Err(Failure::Admission(
                    "configuration omitted a remote build owner",
                ));
            }
            let mut state = current.clone();
            let mut effects = vec![];
            description::update(&mut state, &body.configuration.service, &mut effects);
            apply_body(&mut state, &body.configuration, &admission.local_node)?;
            state.reconfiguration = Some(Reconfiguration {
                request: body.configuration,
                secondary_loss: false,
                stage: ConfigurationStage::Catchup,
                error: 0,
                history: current.reconfiguration.as_ref().map_or_else(Vec::new, |c| {
                    let mut history = c.history.clone();
                    history.push(c.request.clone());
                    history
                }),
                deactivated: vec![],
                activated: vec![],
            });
            effects.extend(pending_effects(&state));
            Ok(lifecycle::changed(state, effects, true))
        }
        Kind::Activate { source, generation } => {
            if !admitted(
                source,
                generation,
                peer.ok_or(Failure::Admission("no configured peer"))?,
                admission,
            )? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: Activate =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid Activate"))?;
            let secondary_loss = secondary_transition(&body.configuration, current);
            let repair = body.configuration.unit.target >= 2
                && valid_stateful_sizes(&body.configuration.service)
                && body.configuration.service == current.body.service
                && (completed_primary_failover(current) || current.body.replica.current_role == 2)
                && body.deactivation.epoch == body.configuration.unit.current
                && body.deactivation.catchup_lsn >= 0;
            if current.fm_primary
                || (!repair
                    && current.peer_creation.as_ref().is_none_or(|origin| {
                        &origin.source != source || origin.deactivation != body.deactivation
                    }))
            {
                return Err(Failure::Unsupported(
                    "Activate requires the original built Idle source",
                ));
            }
            let deactivated = current.reconfiguration.as_ref().is_some_and(|c| {
                c.stage == ConfigurationStage::Deactivated
                    && c.request == body.configuration
                    && current
                        .peer_creation
                        .as_ref()
                        .is_some_and(|p| p.source == *source && p.deactivation == body.deactivation)
            });
            if let Some(config) = &current.reconfiguration {
                if config.request != body.configuration {
                    if body
                        .configuration
                        .unit
                        .current
                        .older_than(config.request.unit.current)
                    {
                        return Ok(Transition::ignored(Disposition::IgnoredStale));
                    }
                    if (!repair && !secondary_loss) || config.stage != ConfigurationStage::Complete
                    {
                        return Err(Failure::Unsupported(
                            "secondary configuration supersession is gated",
                        ));
                    }
                } else if !deactivated {
                    return Ok(Transition {
                        update: Update::None,
                        effects: pending_effects(current),
                        disposition: Disposition::Applied,
                    });
                }
            }
            if !admission.owner_ready || !admission.node_open || !admission.node_activated {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            if !deactivated {
                shape(&body.configuration, current, source)?;
            }
            let expected = local(&body.configuration, &admission.local_node)?;
            if (!repair && !secondary_loss && current.body.replica.current_role != 2)
                || expected.id != current.body.replica.id
                || expected.instance != current.body.replica.instance
                || expected.replication_endpoint != current.body.replica.replication_endpoint
                || expected.service_endpoint != current.body.replica.service_endpoint
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            if repair {
                if !peers.contains(source)
                    || local(&body.configuration, source)?.current_role != 4
                    || (!completed_primary_failover(current)
                        && current
                            .peer_creation
                            .as_ref()
                            .is_none_or(|p| p.source != *source))
                    || (completed_primary_failover(current)
                        && current.swap.as_ref().is_none_or(|s| {
                            !s.body
                                .replicas
                                .0
                                .iter()
                                .any(|r| r.node == *source && r.current_role == 4)
                        }))
                {
                    return Err(Failure::Admission(
                        "repair Activate requires the surviving Primary",
                    ));
                }
                let origin = state
                    .peer_creation
                    .as_mut()
                    .ok_or(Failure::Admission("missing peer origin"))?;
                origin.source = source.clone();
                origin.deactivation = body.deactivation.clone();
                state.primary_deactivation =
                    Some((body.deactivation.epoch, body.deactivation.catchup_lsn));
            }
            let mut effects = vec![];
            description::update(&mut state, &body.configuration.service, &mut effects);
            apply_body(&mut state, &body.configuration, &admission.local_node)?;
            state.reconfiguration = Some(Reconfiguration {
                request: body.configuration,
                secondary_loss,
                stage: ConfigurationStage::Secondary,
                error: 0,
                history: vec![],
                deactivated: vec![],
                activated: vec![],
            });
            effects.extend(pending_effects(&state));
            Ok(lifecycle::changed(state, effects, true))
        }
        Kind::Deactivate { source, generation } => {
            if !admitted(
                source,
                generation,
                peer.ok_or(Failure::Admission("no configured peer"))?,
                admission,
            )? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: Deactivate =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid Deactivate"))?;
            let secondary_loss = secondary_transition(&body.configuration, current);
            if (completed_primary_failover(current) || secondary_loss)
                && valid_stateful_sizes(&body.configuration.service)
                && body.configuration.service == current.body.service
                && body.configuration.unit.current.data_loss == current.body.unit.current.data_loss
                && body.configuration.unit.current.configuration & !0xffff_ffff
                    == current.body.unit.current.configuration & !0xffff_ffff
            {
                if current.reconfiguration.as_ref().is_some_and(|c| {
                    c.stage == ConfigurationStage::Deactivated && c.request == body.configuration
                }) && !body.force
                    && current
                        .peer_creation
                        .as_ref()
                        .is_some_and(|p| p.source == *source && p.deactivation == body.deactivation)
                {
                    return Ok(Transition {
                        update: Update::None,
                        effects: vec![deactivate_reply(current)],
                        disposition: Disposition::Applied,
                    });
                }
                if current.reconfiguration.as_ref().is_some_and(|c| {
                    c.request == body.configuration
                        && matches!(
                            c.stage,
                            ConfigurationStage::Secondary | ConfigurationStage::Complete
                        )
                }) {
                    return Ok(Transition::ignored(Disposition::IgnoredStale));
                }
                if current.phase != Phase::Ready
                    || current.runtime.is_none()
                    || !admission.owner_ready
                    || !admission.node_open
                    || !admission.node_activated
                {
                    return Ok(Transition::ignored(Disposition::IgnoredNotReady));
                }
                shape(&body.configuration, current, source)?;
                let expected = local(&body.configuration, &admission.local_node)?;
                if body.force
                    || current.fm_primary
                    || expected.previous_role != 3
                    || expected.current_role != 3
                    || expected.id != current.body.replica.id
                    || expected.instance != current.body.replica.instance
                    || (!secondary_loss
                        && body.deactivation.epoch != body.configuration.unit.current)
                    || (secondary_loss
                        && current.peer_creation.as_ref().is_none_or(|p| {
                            p.source != *source || p.deactivation != body.deactivation
                        }))
                    || current
                        .peer_creation
                        .as_ref()
                        .is_none_or(|p| body.deactivation.catchup_lsn < p.deactivation.catchup_lsn)
                    || (!secondary_loss
                        && current.swap.as_ref().is_none_or(|s| {
                            !s.body
                                .replicas
                                .0
                                .iter()
                                .any(|r| r.node == *source && r.current_role == 4)
                        }))
                {
                    return Err(Failure::Admission(
                        "repair Deactivate requires the exact surviving S/S and history",
                    ));
                }
                let mut state = current.clone();
                let origin = state.peer_creation.as_mut().expect("surviving peer");
                origin.source = source.clone();
                origin.deactivation = body.deactivation.clone();
                state.primary_deactivation =
                    Some((body.deactivation.epoch, body.deactivation.catchup_lsn));
                apply_body(&mut state, &body.configuration, &admission.local_node)?;
                state.reconfiguration = Some(Reconfiguration {
                    request: body.configuration,
                    secondary_loss,
                    stage: ConfigurationStage::Deactivated,
                    error: 0,
                    history: vec![],
                    deactivated: vec![],
                    activated: vec![],
                });
                let reply = deactivate_reply(&state);
                return Ok(lifecycle::changed(state, vec![reply], true));
            }
            if body
                .configuration
                .unit
                .current
                .older_than(current.body.unit.current)
                || local(&body.configuration, &admission.local_node)?.instance
                    < current.body.replica.instance
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            if current.reconfiguration.as_ref().is_some_and(|c| {
                c.request == body.configuration && c.stage == ConfigurationStage::Retiring
            }) && !body.force
                && current
                    .peer_creation
                    .as_ref()
                    .is_some_and(|p| p.source == *source && p.deactivation == body.deactivation)
            {
                let mut effects = vec![];
                if current.phase == Phase::Closed
                    && current
                        .peer_creation
                        .as_ref()
                        .is_some_and(|p| p.close_completed && !p.host_down)
                {
                    effects.push(deactivate_reply(current));
                } else {
                    lifecycle::close_retry(current, &mut effects);
                }
                return Ok(Transition {
                    update: Update::None,
                    effects,
                    disposition: Disposition::Applied,
                });
            }
            if !admission.owner_ready || !admission.node_open || !admission.node_activated {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            if body.force
                || current.fm_primary
                || current.phase != Phase::Ready
                || current.runtime.is_none()
                || current.peer_creation.as_ref().is_none_or(|p| {
                    p.source != *source || p.deactivation != body.deactivation || p.host_down
                })
                || current.reconfiguration.as_ref().is_none_or(|c| {
                    c.stage != ConfigurationStage::Complete || c.request.unit.target != 2
                })
            {
                return Err(Failure::Unsupported(
                    "only healthy original S/N retirement is enabled",
                ));
            }
            shape(&body.configuration, current, source)?;
            let replica = local(&body.configuration, &admission.local_node)?;
            let mut expected = current.body.replica.clone();
            expected.previous_role = 3;
            expected.current_role = 1;
            if replica != &expected {
                return Err(Failure::Admission(
                    "retirement changed Secondary identity/endpoints",
                ));
            }
            let prior = current
                .reconfiguration
                .as_ref()
                .expect("active configuration");
            if local(&body.configuration, source)? != local(&prior.request, source)? {
                return Err(Failure::Admission(
                    "retirement changed Primary identity/endpoints",
                ));
            }
            let mut state = current.clone();
            let mut effects = vec![];
            description::update(&mut state, &body.configuration.service, &mut effects);
            apply_body(&mut state, &body.configuration, &admission.local_node)?;
            state.reconfiguration = Some(Reconfiguration {
                request: body.configuration,
                secondary_loss: false,
                stage: ConfigurationStage::Retiring,
                error: 0,
                history: vec![prior.request.clone()],
                deactivated: vec![],
                activated: vec![],
            });
            lifecycle::start_close(&mut state, CloseMode::Deactivate, work.id, &mut effects);
            Ok(lifecycle::changed(state, effects, true))
        }
        Kind::DeactivateReply { source, generation } => {
            if !admitted(
                source,
                generation,
                peer.ok_or(Failure::Admission("no configured peer"))?,
                admission,
            )? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: DeactivateReply = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid DeactivateReply"))?;
            let Some(config) = current
                .reconfiguration
                .as_ref()
                .filter(|c| c.stage == ConfigurationStage::Deactivate)
            else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            let target = local(&config.request, source)?;
            let expected = Replica {
                node: target.node.clone(),
                id: target.id,
                instance: target.instance,
                up: false,
                state: 3,
                ..Replica::default()
            };
            if !current.fm_primary
                || body.unit != config.request.unit
                || body.replica != expected
                || body.error != 0
                || target.previous_role != 3
                || !target.up
                || config.deactivated.contains(source)
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            let config = state.reconfiguration.as_mut().expect("owned configuration");
            config.deactivated.push(source.clone());
            if config
                .request
                .replicas
                .0
                .iter()
                .filter(|r| r.previous_role == 3 && r.up)
                .all(|r| config.deactivated.contains(&r.node))
            {
                config.stage = if config.request.unit.target == 1 {
                    ConfigurationStage::End
                } else if config
                    .request
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.up && r.current_role == 3)
                {
                    ConfigurationStage::Activate
                } else {
                    ConfigurationStage::End
                };
            }
            Ok(lifecycle::changed(
                state.clone(),
                pending_effects(&state),
                true,
            ))
        }
        Kind::ActivateReply { source, generation } => {
            if !admitted(
                source,
                generation,
                peer.ok_or(Failure::Admission("no configured peer"))?,
                admission,
            )? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: ActivateReply =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid ActivateReply"))?;
            let Some(config) = &current.reconfiguration else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            if !current.fm_primary
                || config.stage != ConfigurationStage::Activate
                || body.error != 0
                || body.unit != stable(&config.request).unit
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let expected = local(&stable(&config.request), source)?.clone();
            if body.replica != expected {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            let config = state.reconfiguration.as_mut().expect("owned configuration");
            if config.activated.contains(source) {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            config.activated.push(source.clone());
            if config
                .request
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role == 3)
                .all(|r| config.activated.contains(&r.node))
            {
                config.stage = ConfigurationStage::End;
            }
            Ok(lifecycle::changed(
                state.clone(),
                pending_effects(&state),
                true,
            ))
        }
        Kind::CompleteConfiguration => {
            let body: ConfigurationCompletion = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid RAP configuration completion"))?;
            let Some(config) = &current.reconfiguration else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            let flags = match config.stage {
                ConfigurationStage::Catchup => 2,
                ConfigurationStage::End => 1,
                ConfigurationStage::Secondary => 0,
                _ => return Ok(Transition::ignored(Disposition::IgnoredStale)),
            };
            let mut local = body.local.clone();
            local.first_lsn = current.body.replica.first_lsn;
            local.last_lsn = current.body.replica.last_lsn;
            if body.unit != config.request.unit
                || body.flags != flags
                || local != current.body.replica
                || body.replicas != config.request.replicas
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            state
                .reconfiguration
                .as_mut()
                .expect("owned configuration")
                .error = body.error;
            if body.error != 0 {
                return Ok(lifecycle::changed(state, vec![], false));
            }
            if config.stage == ConfigurationStage::Catchup {
                let repair = completed_primary_failover(current) && !config.secondary_loss;
                if repair {
                    if body.local.last_lsn < 0 {
                        return Err(Failure::Admission(
                            "repair catch-up requires actual initialized progress",
                        ));
                    }
                    state.primary_deactivation = Some((body.unit.current, body.local.last_lsn));
                }
                state
                    .reconfiguration
                    .as_mut()
                    .expect("owned configuration")
                    .stage = if config.request.unit.target == 1
                    || ((repair || config.secondary_loss)
                        && config
                            .request
                            .replicas
                            .0
                            .iter()
                            .any(|r| r.previous_role == 3 && r.up))
                {
                    ConfigurationStage::Deactivate
                } else if config
                    .request
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.up && r.current_role == 3)
                {
                    ConfigurationStage::Activate
                } else {
                    ConfigurationStage::End
                };
            } else {
                let stable = stable(&config.request);
                apply_body(&mut state, &stable, &admission.local_node)?;
                if state.fm_primary && config.request.unit.target == 1 {
                    state.remote_idles.clear();
                }
                if state.fm_primary && config.secondary_loss && secondary_removal(&config.request) {
                    state.remote_idles.retain(|idle| {
                        config
                            .request
                            .replicas
                            .0
                            .iter()
                            .any(|r| r.node == idle.target && r.current_role >= 3)
                    });
                }
                state
                    .reconfiguration
                    .as_mut()
                    .expect("owned configuration")
                    .stage = ConfigurationStage::Complete;
            }
            Ok(lifecycle::changed(
                state.clone(),
                pending_effects(&state),
                true,
            ))
        }
        Kind::RetryConfiguration => {
            if current
                .reconfiguration
                .as_ref()
                .is_some_and(|c| c.stage != ConfigurationStage::Complete)
            {
                Ok(Transition {
                    update: Update::None,
                    effects: pending_effects(current),
                    disposition: Disposition::Applied,
                })
            } else {
                Ok(Transition::ignored(Disposition::IgnoredStale))
            }
        }
        _ => unreachable!(),
    }
}
