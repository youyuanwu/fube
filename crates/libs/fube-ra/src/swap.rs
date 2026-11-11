use super::*;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapStage {
    Demote,
    Continue,
    GetLsn,
    Catchup,
    Activate,
    Secondary,
    End,
    Complete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrimarySwap {
    pub intent: Configuration,
    pub body: Configuration,
    pub stage: SwapStage,
    pub error: i64,
    pub forwarded: Option<DoReconfiguration>,
    pub local_progress: Option<Replica>,
    pub remote_progress: Option<GetLsnReply>,
    pub lsn_reply: Option<GetLsnReply>,
    pub endpoint: EndpointPublishState,
    pub endpoint_available: bool,
    started: Instant,
    rap: Configuration,
}
fn elapsed(swap: &PrimarySwap) -> i64 {
    i64::try_from(swap.started.elapsed().as_nanos() / 100).unwrap_or(i64::MAX)
}

pub(super) fn handles(work: &Submission, state: Option<&EntityState>) -> Result<bool> {
    Ok(match &work.kind {
        Kind::GetLsn { .. }
        | Kind::GetLsnReply { .. }
        | Kind::CompleteStatus
        | Kind::ProxyEndpoint
        | Kind::EndpointReply { .. } => true,
        Kind::DoReconfiguration { .. } => {
            let body: DoReconfiguration = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid swap configuration"))?;
            state.is_some_and(|s| {
                s.swap.is_some()
                    || body.configuration.unit.current.configuration & !0xffff_ffff
                        != s.body.unit.current.configuration & !0xffff_ffff
            })
        }
        Kind::Activate { .. }
        | Kind::ActivateReply { .. }
        | Kind::CompleteConfiguration
        | Kind::RetryConfiguration => state.is_some_and(|s| s.swap.is_some()),
        _ => false,
    })
}
fn replica<'a>(body: &'a Configuration, node: &NodeInstance) -> Result<&'a Replica> {
    body.replicas
        .0
        .iter()
        .find(|r| &r.node == node)
        .ok_or(Failure::Admission("swap omitted exact replica owner"))
}
fn replace(body: &mut Configuration, value: Replica) -> Result<()> {
    let target = body
        .replicas
        .0
        .iter_mut()
        .find(|r| r.node == value.node)
        .ok_or(Failure::Admission("swap changed replica owner"))?;
    *target = value;
    Ok(())
}
fn apply(state: &mut EntityState, node: &NodeInstance) -> Result<()> {
    let body = &state.swap.as_ref().expect("swap").body;
    state.body = Arc::new(AddInstance {
        unit: body.unit.clone(),
        service: body.service.clone(),
        replica: replica(body, node)?.clone(),
    });
    Ok(())
}
fn generation(
    source: &NodeInstance,
    expected: &NodeInstance,
    header: &GenerationHeader,
    admission: &Admission,
) -> Result<bool> {
    if source != expected || header.for_fm_replica {
        return Err(Failure::Admission("swap source/owner mismatch"));
    }
    if header.number.older_than(&admission.generation) {
        return Ok(false);
    }
    if header.number != admission.generation {
        return Err(Failure::Unsupported(
            "swap requires the original FM generation",
        ));
    }
    Ok(true)
}
fn same_replica(actual: &Replica, expected: &Replica) -> bool {
    let mut actual = actual.clone();
    actual.first_lsn = expected.first_lsn;
    actual.last_lsn = expected.last_lsn;
    actual.service_endpoint = expected.service_endpoint.clone();
    actual == *expected
}
fn ready(state: &EntityState, admission: &Admission) -> bool {
    state.phase == Phase::Ready
        && state.runtime.is_some()
        && admission.owner_ready
        && admission.node_open
        && admission.node_activated
}
fn validate(body: &Configuration, state: &EntityState) -> Result<()> {
    let active = state
        .reconfiguration
        .as_ref()
        .filter(|c| c.stage == ConfigurationStage::Complete && c.request.unit.target == 2)
        .ok_or(Failure::Unsupported(
            "swap requires completed active membership",
        ))?;
    let old = &active.request;
    if body.replicas.0.len() != 2
        || body.service != state.body.service
        || body.unit.target != 2
        || body.unit.minimum != 1
        || body.unit.id != state.body.unit.id
        || body.unit.consistency != state.body.unit.consistency
        || body.unit.previous != state.body.unit.current
        || body.unit.current.data_loss != state.body.unit.current.data_loss
        || body.unit.current.configuration
            != state
                .body
                .unit
                .current
                .configuration
                .checked_add(0x1_0000_0001)
                .ok_or(Failure::Capacity)?
    {
        return Err(Failure::Unsupported(
            "only one healthy two-member planned swap is enabled",
        ));
    }
    for r in &body.replicas.0 {
        let expected = replica(old, &r.node)?;
        let mut normalized = r.clone();
        normalized.previous_role = expected.previous_role;
        normalized.current_role = expected.current_role;
        normalized.first_lsn = expected.first_lsn;
        normalized.last_lsn = expected.last_lsn;
        if r.service_endpoint == expected.service_endpoint
            || (expected.current_role == 4 && r.current_role == 3 && r.service_endpoint.is_empty())
        {
            normalized.service_endpoint = expected.service_endpoint.clone();
        }
        if normalized != *expected
            || r.previous_role != expected.current_role
            || r.current_role != if expected.current_role == 4 { 3 } else { 4 }
            || !r.up
            || r.state != 2
        {
            return Err(Failure::Admission(
                "swap changed original membership/identity/endpoints",
            ));
        }
    }
    Ok(())
}
fn configuration_effect(state: &EntityState, flags: i64) -> Effect {
    let body = &state.swap.as_ref().expect("swap").rap;
    Effect::UpdateConfiguration {
        body: Box::new(body.clone()),
        local: Box::new(
            replica(body, &state.body.replica.node)
                .expect("local")
                .clone(),
        ),
        flags,
        registration: state.runtime.clone().expect("runtime"),
    }
}
fn query(state: &EntityState) -> Effect {
    Effect::QueryReplicaStatus {
        body: state.body.clone(),
        registration: state.runtime.clone().expect("runtime"),
    }
}
fn stable(body: &Configuration) -> Configuration {
    let mut result = body.clone();
    result.unit.previous = Epoch::default();
    for r in &mut result.replicas.0 {
        r.previous_role = 1;
    }
    result
}
fn get_lsn_request(state: &EntityState) -> Effect {
    let swap = state.swap.as_ref().expect("swap");
    let old = swap
        .body
        .replicas
        .0
        .iter()
        .find(|r| r.previous_role == 4)
        .expect("old Primary");
    Effect::PeerGetLsn {
        target: old.node.clone(),
        body: Box::new(AddInstance {
            unit: swap.body.unit.clone(),
            service: swap.body.service.clone(),
            replica: Replica {
                node: old.node.clone(),
                id: old.id,
                instance: old.instance,
                ..Replica::default()
            },
        }),
    }
}
fn effects(state: &EntityState) -> Vec<Effect> {
    let swap = state.swap.as_ref().expect("swap");
    let mut result = match swap.stage {
        SwapStage::Demote | SwapStage::Catchup => vec![configuration_effect(state, 2)],
        SwapStage::Continue => vec![Effect::PeerContinueSwap {
            target: swap
                .body
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .expect("new Primary")
                .node
                .clone(),
            body: Box::new(swap.forwarded.clone().expect("demote completion")),
        }],
        SwapStage::GetLsn => {
            let mut work = vec![];
            if swap.local_progress.is_none() {
                work.push(query(state));
            }
            if swap.remote_progress.is_none() {
                work.push(get_lsn_request(state));
            }
            work
        }
        SwapStage::Activate => {
            let target = swap
                .body
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 3)
                .expect("Secondary");
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("new Primary history");
            vec![Effect::PeerActivate {
                target: target.node.clone(),
                body: Box::new(Activate {
                    configuration: swap.body.clone(),
                    deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                }),
            }]
        }
        SwapStage::Secondary => vec![configuration_effect(state, 0)],
        SwapStage::End => vec![configuration_effect(state, 1)],
        SwapStage::Complete if state.body.replica.current_role == 4 => {
            vec![Effect::DoReconfigurationReply(ConfigurationReply {
                unit: state.body.unit.clone(),
                replicas: swap.body.replicas.clone(),
                error: 0,
            })]
        }
        SwapStage::Complete => vec![Effect::PeerActivateReply {
            target: swap
                .body
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .expect("Primary")
                .node
                .clone(),
            body: Box::new(ActivateReply {
                unit: state.body.unit.clone(),
                replica: state.body.replica.clone(),
                error: 0,
            }),
        }],
    };
    if swap.endpoint_available {
        result.push(Effect::ReplicaEndpointUpdated(Box::new(
            (*state.body).clone(),
        )));
    }
    result
}
fn advance_get_lsn(state: &mut EntityState) -> Result<()> {
    let swap = state.swap.as_mut().expect("swap");
    if let (Some(local), Some(remote)) = (&swap.local_progress, &swap.remote_progress) {
        if local.last_lsn != remote.replica.replica.last_lsn
            || local.first_lsn < 0
            || local.last_lsn < 0
            || remote.replica.replica.first_lsn < 0
        {
            return Err(Failure::Unsupported(
                "planned swap requires matched initialized progress; repair is gated",
            ));
        }
        replace(&mut swap.body, local.clone())?;
        replace(&mut swap.body, remote.replica.replica.clone())?;
        swap.rap = swap.body.clone();
        swap.stage = SwapStage::Catchup;
    }
    Ok(())
}

pub(super) fn transition(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
    config: &NativeConfig,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let Some(current) = current.filter(|s| ready(s, admission)) else {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    };
    let peer = peer.ok_or(Failure::Admission("swap requires original peer"))?;
    let stale = || Ok(Transition::ignored(Disposition::IgnoredStale));
    let mut state = current.clone();
    match &work.kind {
        Kind::DoReconfiguration {
            source,
            generation: header,
        } => {
            let body: DoReconfiguration =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid swap"))?;
            let from_fm = source == &admission.fm_source;
            if !generation(
                source,
                if from_fm { &admission.fm_source } else { peer },
                header,
                admission,
            )? {
                return stale();
            }
            if body
                .configuration
                .unit
                .current
                .older_than(current.body.unit.current)
            {
                return stale();
            }
            if let Some(swap) = &current.swap {
                let expected = if from_fm {
                    &swap.intent
                } else {
                    &swap
                        .forwarded
                        .as_ref()
                        .ok_or(Failure::Unsupported("no continuation"))?
                        .configuration
                };
                if &body.configuration != expected {
                    return Err(Failure::Unsupported("swap supersession is gated"));
                }
                let replay = if from_fm && swap.stage != SwapStage::Demote {
                    let mut copy = current.clone();
                    copy.swap.as_mut().expect("swap").stage = SwapStage::Continue;
                    effects(&copy)
                } else {
                    effects(current)
                };
                return Ok(Transition {
                    update: Update::None,
                    effects: replay,
                    disposition: Disposition::Applied,
                });
            }
            validate(&body.configuration, current)?;
            let local = replica(&body.configuration, &admission.local_node)?;
            if (from_fm && (local.previous_role != 4 || body.phase0_duration != i64::MAX))
                || (!from_fm
                    && (local.current_role != 4
                        || body.phase0_duration <= 0
                        || body.phase0_duration == i64::MAX))
            {
                return Err(Failure::Unsupported(
                    "swap requires old Primary demotion before peer continuation",
                ));
            }
            state.swap = Some(PrimarySwap {
                intent: body.configuration.clone(),
                body: body.configuration.clone(),
                rap: body.configuration.clone(),
                stage: if from_fm {
                    SwapStage::Demote
                } else {
                    SwapStage::GetLsn
                },
                forwarded: (!from_fm).then_some(body),
                error: 0,
                local_progress: None,
                remote_progress: None,
                lsn_reply: None,
                endpoint: EndpointPublishState::default(),
                endpoint_available: false,
                started: Instant::now(),
            });
            apply(&mut state, &admission.local_node)?;
        }
        Kind::GetLsn {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: GetLsn =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid GetLSN"))?;
            let Some(swap) = &current.swap else {
                return stale();
            };
            if body.unit != swap.intent.unit
                || body.service != swap.intent.service
                || body.replica.node != admission.local_node
                || body.replica.id != current.body.replica.id
                || body.replica.instance != current.body.replica.instance
                || !matches!(
                    swap.stage,
                    SwapStage::Continue | SwapStage::Secondary | SwapStage::Complete
                )
            {
                return stale();
            }
            let replies = if let Some(reply) = &swap.lsn_reply {
                vec![Effect::PeerGetLsnReply {
                    target: peer.clone(),
                    body: Box::new(reply.clone()),
                }]
            } else {
                vec![query(current)]
            };
            return Ok(Transition {
                update: Update::None,
                effects: replies,
                disposition: Disposition::Applied,
            });
        }
        Kind::CompleteStatus => {
            let body: ConfigurationCompletion = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid status completion"))?;
            let Some(swap) = &current.swap else {
                return stale();
            };
            if !matches!(swap.stage, SwapStage::GetLsn | SwapStage::Continue)
                || body.unit != current.body.unit
                || body.flags != 0
                || !body.replicas.0.is_empty()
                || !same_replica(&body.local, &current.body.replica)
            {
                return stale();
            }
            state.swap.as_mut().expect("swap").error = body.error;
            if body.error != 0 {
                return Ok(lifecycle::changed(state, vec![], false));
            }
            if body.local.first_lsn < 0 || body.local.last_lsn < 0 {
                return Err(Failure::Admission("uninitialized swap progress"));
            }
            if swap.stage == SwapStage::Continue {
                let (epoch, catchup_lsn) = current
                    .primary_deactivation
                    .ok_or(Failure::Admission("missing old Primary history"))?;
                let reply = GetLsnReply {
                    replica: AddInstanceReply {
                        unit: body.unit,
                        replica: body.local,
                        error: 0,
                    },
                    deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                };
                state.swap.as_mut().expect("swap").lsn_reply = Some(reply.clone());
                return Ok(lifecycle::changed(
                    state,
                    vec![Effect::PeerGetLsnReply {
                        target: peer.clone(),
                        body: Box::new(reply),
                    }],
                    true,
                ));
            }
            replace(
                &mut state.swap.as_mut().expect("swap").body,
                body.local.clone(),
            )?;
            state.swap.as_mut().expect("swap").local_progress = Some(body.local);
            apply(&mut state, &admission.local_node)?;
            advance_get_lsn(&mut state)?;
        }
        Kind::GetLsnReply {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: GetLsnReply =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid GetLSNReply"))?;
            let Some(swap) = current
                .swap
                .as_ref()
                .filter(|s| s.stage == SwapStage::GetLsn)
            else {
                return stale();
            };
            let expected = replica(&swap.body, peer)?;
            if body.replica.unit != swap.body.unit
                || body.replica.error != 0
                || !same_replica(&body.replica.replica, expected)
                || body.deactivation.epoch.data_loss != swap.body.unit.previous.data_loss
                || body.deactivation.epoch.configuration & !0xffff_ffff
                    != swap.body.unit.previous.configuration & !0xffff_ffff
                || body.deactivation.catchup_lsn < 0
            {
                return stale();
            }
            state.swap.as_mut().expect("swap").remote_progress = Some(body);
            advance_get_lsn(&mut state)?;
        }
        Kind::CompleteConfiguration => {
            let body: ConfigurationCompletion = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid swap completion"))?;
            let Some(swap) = &current.swap else {
                return stale();
            };
            let flags = match swap.stage {
                SwapStage::Demote | SwapStage::Catchup => 2,
                SwapStage::Secondary => 0,
                SwapStage::End => 1,
                _ => return stale(),
            };
            if body.unit != swap.rap.unit
                || body.flags != flags
                || body.replicas != swap.rap.replicas
                || !same_replica(&body.local, replica(&swap.rap, &admission.local_node)?)
            {
                return stale();
            }
            state.swap.as_mut().expect("swap").error = body.error;
            if body.error != 0 {
                return Ok(lifecycle::changed(state, vec![], false));
            }
            if (body.local.current_role == 4 && body.local.service_endpoint.is_empty())
                || (body.local.current_role == 3 && !body.local.service_endpoint.is_empty())
                || body.local.first_lsn < 0
                || body.local.last_lsn < 0
            {
                return Err(Failure::Admission(
                    "swap RAP completion omitted role endpoint/progress",
                ));
            }
            replace(
                &mut state.swap.as_mut().expect("swap").body,
                body.local.clone(),
            )?;
            match swap.stage {
                SwapStage::Demote => {
                    let s = state.swap.as_mut().expect("swap");
                    s.forwarded = Some(DoReconfiguration {
                        configuration: s.body.clone(),
                        phase0_duration: i64::try_from(s.started.elapsed().as_nanos() / 100)
                            .unwrap_or(i64::MAX - 1)
                            .clamp(1, i64::MAX - 1),
                    });
                    s.stage = SwapStage::Continue;
                }
                SwapStage::Catchup => {
                    state.primary_deactivation = Some((body.unit.current, body.local.last_lsn));
                    state.swap.as_mut().expect("swap").stage = SwapStage::Activate;
                }
                _ => {
                    let s = state.swap.as_mut().expect("swap");
                    s.body = stable(&s.body);
                    s.endpoint.clear(&mut s.endpoint_available);
                    s.stage = SwapStage::Complete;
                }
            }
            apply(&mut state, &admission.local_node)?;
        }
        Kind::Activate {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: Activate =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid swap Activate"))?;
            let Some(swap) = &current.swap else {
                return stale();
            };
            if body.configuration.unit != swap.intent.unit
                || body.configuration.service != swap.intent.service
                || body.configuration.replicas.0.len() != 2
                || body.deactivation.epoch != swap.intent.unit.current
                || body.deactivation.catchup_lsn < 0
            {
                return stale();
            }
            for r in &body.configuration.replicas.0 {
                if !same_replica(r, replica(&swap.body, &r.node)?) {
                    return stale();
                }
            }
            if swap.stage == SwapStage::Complete || swap.stage == SwapStage::Secondary {
                return Ok(Transition {
                    update: Update::None,
                    effects: effects(current),
                    disposition: Disposition::Applied,
                });
            }
            if swap.stage != SwapStage::Continue {
                return stale();
            }
            let s = state.swap.as_mut().expect("swap");
            s.body = body.configuration.clone();
            s.rap = body.configuration;
            s.stage = SwapStage::Secondary;
            state.primary_deactivation =
                Some((body.deactivation.epoch, body.deactivation.catchup_lsn));
            apply(&mut state, &admission.local_node)?;
        }
        Kind::ActivateReply {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: ActivateReply = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid swap ActivateReply"))?;
            let Some(swap) = current
                .swap
                .as_ref()
                .filter(|s| s.stage == SwapStage::Activate)
            else {
                return stale();
            };
            let expected = stable(&swap.body);
            if body.unit != expected.unit
                || body.error != 0
                || body.replica != *replica(&expected, peer)?
            {
                return stale();
            }
            let s = state.swap.as_mut().expect("swap");
            s.stage = SwapStage::End;
            s.rap = s.body.clone();
        }
        Kind::ProxyEndpoint => {
            let body: AddInstance = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid proxy endpoint"))?;
            let ack = Effect::AcknowledgeEndpoint {
                body: Arc::new(body.clone()),
                registration: current.runtime.clone().expect("runtime"),
            };
            let Some(swap) = current.swap.as_ref().filter(|s| {
                s.stage == SwapStage::Catchup && current.body.replica.current_role == 4
            }) else {
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![ack],
                    disposition: Disposition::IgnoredStale,
                });
            };
            if body.unit.current != swap.body.unit.current
                || !same_replica(&body.replica, &current.body.replica)
                || body.replica.service_endpoint.is_empty()
            {
                return Ok(Transition {
                    update: Update::None,
                    effects: vec![ack],
                    disposition: Disposition::IgnoredStale,
                });
            }
            let s = state.swap.as_mut().expect("swap");
            let mut local = current.body.replica.clone();
            local.service_endpoint = body.replica.service_endpoint;
            replace(&mut s.body, local)?;
            s.endpoint.endpoint_updated(
                elapsed(s),
                config.node(NodeProperty::EndpointPublishTimeoutTicks),
                &mut s.endpoint_available,
            );
            apply(&mut state, &admission.local_node)?;
            let mut out = vec![ack];
            if state.swap.as_ref().expect("swap").endpoint_available {
                out.push(Effect::ReplicaEndpointUpdated(Box::new(
                    (*state.body).clone(),
                )));
            }
            return Ok(lifecycle::changed(state, out, true));
        }
        Kind::EndpointReply {
            source,
            generation: header,
        } => {
            if !generation(source, &admission.fm_source, header, admission)? {
                return stale();
            }
            let body: AddInstanceReply =
                from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid endpoint ACK"))?;
            if body.unit != current.body.unit
                || body.replica != current.body.replica
                || body.error != 0
            {
                return stale();
            }
            let s = state
                .swap
                .as_mut()
                .ok_or(Failure::Unsupported("no endpoint publication"))?;
            s.endpoint.acknowledge(&mut s.endpoint_available);
            return Ok(lifecycle::changed(state, vec![], true));
        }
        Kind::RetryConfiguration => {
            let Some(s) = &mut state.swap else {
                return stale();
            };
            if s.stage == SwapStage::Complete {
                return stale();
            }
            s.endpoint.timer(
                elapsed(s),
                config.node(NodeProperty::EndpointPublishTimeoutTicks),
                &mut s.endpoint_available,
            );
            if state.swap == current.swap {
                return Ok(Transition {
                    update: Update::None,
                    effects: effects(current),
                    disposition: Disposition::Applied,
                });
            }
        }
        _ => unreachable!(),
    }
    let out = effects(&state);
    Ok(lifecycle::changed(state, out, true))
}
