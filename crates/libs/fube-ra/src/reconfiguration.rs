use super::*;
use fube_transport::serialization::serde::ObjectArray;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigurationStage {
    Catchup,
    Activate,
    End,
    Secondary,
    Complete,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Reconfiguration {
    pub request: Configuration,
    pub stage: ConfigurationStage,
    pub error: i64,
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
    expected.target = 2;
    let Some(version) = old.update_version.checked_add(1) else {
        return false;
    };
    expected.update_version = version;
    old.target == 1 && old.minimum == 1 && *new == expected
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
fn shape(body: &Configuration, old: &EntityState, primary: &NodeInstance) -> Result<()> {
    let old_epoch = old.body.unit.current;
    if body.replicas.0.len() != 2
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
        || body.unit.target != 2
        || body.unit.minimum != 1
        || !secondary_service(&old.body.service, &body.service)
    {
        return Err(Failure::Unsupported(
            "only one same-Primary 1-to-2 configuration is enabled",
        ));
    }
    let p = local(body, primary)?;
    let s = body
        .replicas
        .0
        .iter()
        .find(|r| r.node != *primary)
        .ok_or(Failure::Admission("missing Secondary"))?;
    if p.current_role != 4
        || p.previous_role != 4
        || s.current_role != 3
        || s.previous_role != 2
        || p.id == s.id
        || p.node.id == s.node.id
        || body.replicas.0.iter().any(|r| {
            !r.up
                || r.state != 2
                || r.id < 0
                || r.instance < 0
                || r.version != body.service.version
                || r.replication_endpoint.is_empty()
        })
    {
        return Err(Failure::Unsupported(
            "not the exact ready P/P and I/S configuration",
        ));
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
    let body = stable(
        &state
            .reconfiguration
            .as_ref()
            .expect("owned configuration")
            .request,
    );
    if state.fm_primary {
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
fn pending_effect(state: &EntityState) -> Effect {
    match state
        .reconfiguration
        .as_ref()
        .expect("owned configuration")
        .stage
    {
        ConfigurationStage::Catchup => configuration_effect(state, 2),
        ConfigurationStage::End => configuration_effect(state, 1),
        ConfigurationStage::Secondary => configuration_effect(state, 0),
        ConfigurationStage::Activate => {
            let config = state.reconfiguration.as_ref().expect("owned configuration");
            let target = config
                .request
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 3)
                .expect("Secondary");
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("Primary deactivation");
            Effect::PeerActivate {
                target: target.node.clone(),
                body: Box::new(Activate {
                    configuration: config.request.clone(),
                    deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                }),
            }
        }
        ConfigurationStage::Complete => reply(state),
    }
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
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let Some(current) = current.filter(|s| s.phase == Phase::Ready && s.runtime.is_some()) else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    match &work.kind {
        Kind::DoReconfiguration { source, generation } => {
            if !admitted(source, generation, &admission.fm_source, admission)? {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let body: DoReconfiguration = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid DoReconfiguration"))?;
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
                    return Err(Failure::Unsupported("configuration supersession is gated"));
                }
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![pending_effect(current)],
                    disposition: Disposition::Applied,
                });
            }
            if !admission.owner_ready || !admission.node_open || !admission.node_activated {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            shape(&body.configuration, current, &admission.local_node)?;
            let remote = current
                .remote_idle
                .as_ref()
                .filter(|remote| remote.ready && remote.removal.is_none())
                .ok_or(Failure::Unsupported(
                    "configuration requires a successfully built FM Idle",
                ))?;
            let opened = remote
                .reply
                .as_ref()
                .ok_or(Failure::Unsupported("missing peer Open"))?;
            let secondary = local(&body.configuration, &remote.target)?;
            if peer != Some(&remote.target)
                || remote.fm_request.is_none()
                || secondary.id != opened.replica.id
                || secondary.instance != opened.replica.instance
                || secondary.replication_endpoint != opened.replica.replication_endpoint
                || secondary.service_endpoint != opened.replica.service_endpoint
                || local(&body.configuration, &admission.local_node)?.id != current.body.replica.id
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
            let mut state = current.clone();
            let mut effects = vec![];
            description::update(&mut state, &body.configuration.service, &mut effects);
            apply_body(&mut state, &body.configuration, &admission.local_node)?;
            state.reconfiguration = Some(Reconfiguration {
                request: body.configuration,
                stage: ConfigurationStage::Catchup,
                error: 0,
            });
            effects.push(pending_effect(&state));
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
            if current.fm_primary
                || current.peer_creation.as_ref().is_none_or(|origin| {
                    &origin.source != source || origin.deactivation != body.deactivation
                })
            {
                return Err(Failure::Unsupported(
                    "Activate requires the original built Idle source",
                ));
            }
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
                    return Err(Failure::Unsupported(
                        "secondary configuration supersession is gated",
                    ));
                }
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![pending_effect(current)],
                    disposition: Disposition::Applied,
                });
            }
            if !admission.owner_ready || !admission.node_open || !admission.node_activated {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            shape(&body.configuration, current, source)?;
            let expected = local(&body.configuration, &admission.local_node)?;
            if current.body.replica.current_role != 2
                || expected.id != current.body.replica.id
                || expected.instance != current.body.replica.instance
                || expected.replication_endpoint != current.body.replica.replication_endpoint
                || expected.service_endpoint != current.body.replica.service_endpoint
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            let mut effects = vec![];
            description::update(&mut state, &body.configuration.service, &mut effects);
            apply_body(&mut state, &body.configuration, &admission.local_node)?;
            state.reconfiguration = Some(Reconfiguration {
                request: body.configuration,
                stage: ConfigurationStage::Secondary,
                error: 0,
            });
            effects.push(pending_effect(&state));
            Ok(lifecycle::changed(state, effects, true))
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
            state
                .reconfiguration
                .as_mut()
                .expect("owned configuration")
                .stage = ConfigurationStage::End;
            Ok(lifecycle::changed(
                state.clone(),
                vec![pending_effect(&state)],
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
                // PC contains only the unchanged local Primary: phase 3 has no peer work.
                state
                    .reconfiguration
                    .as_mut()
                    .expect("owned configuration")
                    .stage = ConfigurationStage::Activate;
            } else {
                let stable = stable(&config.request);
                apply_body(&mut state, &stable, &admission.local_node)?;
                state
                    .reconfiguration
                    .as_mut()
                    .expect("owned configuration")
                    .stage = ConfigurationStage::Complete;
            }
            Ok(lifecycle::changed(
                state.clone(),
                vec![pending_effect(&state)],
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
                    effects: vec![pending_effect(current)],
                    disposition: Disposition::Applied,
                })
            } else {
                Ok(Transition::ignored(Disposition::IgnoredStale))
            }
        }
        _ => unreachable!(),
    }
}
