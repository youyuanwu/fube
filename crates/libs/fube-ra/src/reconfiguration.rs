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

pub fn initial_membership_configuration(state: &EntityState) -> Option<&Reconfiguration> {
    let config = state.reconfiguration.as_ref()?;
    (state.phase == Phase::Ready
        && state.swap.is_none()
        && state.progress_query.is_none()
        && !config.secondary_loss
        && config.history.is_empty()
        && config.error == 0
        && valid_stateful_sizes(&state.body.service)
        && config.request.unit.current == state.body.unit.current
        && config.request.unit.target == state.body.service.target
        && config.request.unit.minimum == state.body.service.minimum
        && config.request.replicas.0.len() == state.body.service.target as usize
        && config.request.replicas.0.len() >= 2
        && config.request.replicas.0.iter().all(|r| {
            r.up && r.state == 2
                && if r.node == state.body.replica.node {
                    if state.fm_primary {
                        (r.previous_role, r.current_role) == (4, 4)
                    } else {
                        (r.previous_role, r.current_role) == (2, 3)
                    }
                } else {
                    matches!((r.previous_role, r.current_role), (4, 4) | (2, 3))
                }
        })
        && config
            .request
            .replicas
            .0
            .iter()
            .filter(|r| r.current_role == 4)
            .count()
            == 1
        && state.remote_idles.iter().all(|remote| {
            remote.ready
                && remote.fm_request.is_some()
                && remote.removal.is_none()
                && config.request.replicas.0.iter().any(|r| {
                    r.node == remote.target
                        && r.id == remote.body.replica.0.replica.id
                        && r.instance == remote.body.replica.0.replica.instance
                })
        })
        && if state.fm_primary {
            matches!(
                config.stage,
                ConfigurationStage::Catchup
                    | ConfigurationStage::Activate
                    | ConfigurationStage::End
                    | ConfigurationStage::Complete
            )
        } else {
            matches!(
                config.stage,
                ConfigurationStage::Secondary | ConfigurationStage::Complete
            )
        })
    .then_some(config)
}

pub(super) fn continues_initial_membership(
    work: &Submission,
    current: Option<&EntityState>,
) -> bool {
    let Some(config) = current.and_then(initial_membership_configuration) else {
        return false;
    };
    match work.kind {
        Kind::DoReconfiguration { .. } => from_slice::<DoReconfiguration>(&work.bytes)
            .is_ok_and(|body| body.configuration == config.request),
        Kind::Activate { .. } => from_slice::<Activate>(&work.bytes)
            .is_ok_and(|body| body.configuration == config.request),
        Kind::ActivateReply { .. } | Kind::RetryConfiguration => true,
        _ => false,
    }
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
pub fn replica_replacement_metadata(state: &EntityState) -> bool {
    state.reconfiguration.as_ref().is_some_and(|c| {
        valid_stateful_sizes(&c.request.service)
            && c.request.unit.target >= 2
            && (c.stage == ConfigurationStage::Complete
                || (state.fm_primary
                    && c.secondary_loss
                    && secondary_removal(&c.request)
                    && c.request.replicas.0.iter().any(|r| {
                        r.node == state.body.replica.node
                            && r.previous_role == 4
                            && r.current_role == 4
                    })))
    })
}
pub(super) fn completed_configuration(state: &EntityState) -> Option<&Configuration> {
    let configuration = state
        .reconfiguration
        .as_ref()
        .filter(|c| c.stage == ConfigurationStage::Complete);
    let swap = state
        .swap
        .as_ref()
        .filter(|s| s.stage == SwapStage::Complete);
    match (configuration, swap) {
        (Some(c), Some(s)) if c.request.unit.current.older_than(s.body.unit.current) => {
            Some(&s.body)
        }
        (Some(c), _) => Some(&c.request),
        (None, Some(s)) => Some(&s.body),
        _ => None,
    }
}
fn secondary_transition(body: &Configuration, old: &EntityState) -> bool {
    secondary_removal(body)
        || completed_secondary_removal(old)
        || old.reconfiguration.as_ref().is_some_and(|c| {
            c.secondary_loss
                && (c.request == *body
                    || (c.stage == ConfigurationStage::Complete
                        && c.request.unit.current == old.body.unit.current
                        && body.service == old.body.service
                        && c.request
                            .replicas
                            .0
                            .iter()
                            .filter(|r| r.current_role >= 3)
                            .count()
                            < body.service.target as usize))
        })
        || (body.service == old.body.service
            && old.body.replica.current_role == 2
            && old.peer_creation.as_ref().is_some_and(|p| {
                p.deactivation.epoch != old.body.unit.current
                    && p.deactivation.epoch.data_loss == old.body.unit.current.data_loss
                    && p.deactivation.epoch.configuration & !0xffff_ffff
                        == old.body.unit.current.configuration & !0xffff_ffff
            }))
}
pub(super) fn stable(request: &Configuration) -> Configuration {
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
        let fresh = body
            .replicas
            .0
            .iter()
            .filter(|r| r.previous_role == 2 && r.current_role == 3)
            .count();
        let prior = completed_configuration(old);
        if prior.is_none() && !(fresh == 1 && old.body.replica.current_role == 2) {
            return Err(Failure::Unsupported(
                "Secondary removal requires completed membership or the fresh Idle",
            ));
        }
        let members: Vec<_> = prior
            .unwrap_or(body)
            .replicas
            .0
            .iter()
            .filter(|r| {
                if prior.is_some() {
                    r.current_role >= 3
                } else {
                    r.previous_role >= 3
                }
            })
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
            || fresh > 1
            || body.replicas.0.len() != members.len() + fresh
            || members.len() > body.service.target as usize
            || members.len() + fresh <= body.service.minimum as usize
            || members.len() + fresh - 1 > body.service.target as usize
            || local(body, primary)?.previous_role != 4
            || local(body, primary)?.current_role != 4
            || body.replicas.0.iter().enumerate().any(|(i, r)| {
                r.id < 0
                    || r.instance < 0
                    || r.version != body.service.version
                    || (r.current_role == 4 && r.node != *primary)
                    || body.replicas.0[..i]
                        .iter()
                        .any(|old| old.node.id == r.node.id || old.id == r.id)
                    || if r.current_role == 1 {
                        r.previous_role != 3 || r.up || r.state != 3
                    } else {
                        !r.up
                            || r.state != 2
                            || r.replication_endpoint.is_empty()
                            || !matches!(
                                (r.previous_role, r.current_role),
                                (4, 4) | (3, 3) | (2, 3)
                            )
                    }
            })
        {
            return Err(Failure::Unsupported(
                "Secondary removal must preserve sizes, Primary and minimum",
            ));
        }
        if prior.is_none() {
            return Ok(());
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
    let prior = completed_configuration(old);
    let repair_count = prior.map_or(body.replicas.0.len(), |c| {
        c.replicas.0.iter().filter(|r| r.current_role >= 3).count() + 1
    });
    let three = body.unit.target == 3 && old.body.service.target == 1;
    if (!initial
        && body.replicas.0.len()
            != if repair {
                repair_count
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
        || (repair
            && (body.replicas.0.len() > body.service.target as usize
                || body.replicas.0.len() < body.service.minimum as usize))
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
        if let Some(prior) = prior {
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
            if let Some(lost) = prior.replicas.0.iter().find(|r| r.current_role == 1) {
                let fresh = local(body, &lost.node)?;
                if fresh.previous_role != 2
                    || fresh.id <= lost.id
                    || fresh.instance <= lost.instance
                {
                    return Err(Failure::Admission(
                        "Secondary repair requires a fresh incarnation",
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
                    if (config.stage != ConfigurationStage::Complete
                        && !current.swap.as_ref().is_some_and(|s| {
                            s.stage == SwapStage::Complete
                                && config.request.unit.current.older_than(s.body.unit.current)
                        }))
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
                if body
                    .configuration
                    .replicas
                    .0
                    .iter()
                    .any(|r| r.previous_role == 2 && r.current_role == 3)
                {
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
                        && (completed_configuration(current).is_none_or(|c| {
                            !c.replicas
                                .0
                                .iter()
                                .any(|r| r.node == *source && r.current_role == 4)
                        }) || current.primary_deactivation.map_or_else(
                            || {
                                current
                                    .peer_creation
                                    .as_ref()
                                    .is_none_or(|p| p.deactivation != body.deactivation)
                            },
                            |(epoch, catchup_lsn)| {
                                body.deactivation != ReplicaDeactivationInfo { epoch, catchup_lsn }
                            },
                        )))
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
