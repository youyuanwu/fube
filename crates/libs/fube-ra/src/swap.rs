use super::*;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapStage {
    Demote,
    Continue,
    GetLsn,
    ChangeConfiguration,
    Catchup,
    Deactivate,
    Deactivated,
    Activate,
    Secondary,
    End,
    Complete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrimarySwap {
    pub failover: bool,
    pub intent: Configuration,
    pub body: Configuration,
    pub stage: SwapStage,
    pub error: i64,
    pub forwarded: Option<DoReconfiguration>,
    pub local_progress: Option<Replica>,
    pub remote_progress: Vec<GetLsnReply>,
    pub deactivated: Vec<NodeInstance>,
    pub activated: Vec<NodeInstance>,
    pub change: Option<Configuration>,
    pub lsn_reply: Option<GetLsnReply>,
    pub endpoint: EndpointPublishState,
    pub endpoint_available: bool,
    started: Instant,
    rap: Configuration,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressQuery {
    pub source: NodeInstance,
    pub request: AddInstance,
    pub reply: Option<GetLsnReply>,
    pub error: i64,
}

fn deactivation(state: &EntityState) -> Result<ReplicaDeactivationInfo> {
    state
        .primary_deactivation
        .map(|(epoch, catchup_lsn)| ReplicaDeactivationInfo { epoch, catchup_lsn })
        .or_else(|| state.peer_creation.as_ref().map(|p| p.deactivation.clone()))
        .ok_or(Failure::Admission("missing original replica history"))
}

fn failover_membership(state: &EntityState) -> Result<&Reconfiguration> {
    state
        .reconfiguration
        .as_ref()
        .filter(|c| {
            c.request.replicas.0.len() >= 2
                && (c.stage == ConfigurationStage::Complete
                    || reconfiguration::secondary_removal_in_progress(state))
        })
        .ok_or(Failure::Unsupported(
            "failover requires completed membership or exact Secondary removal",
        ))
}

fn failover_epoch(unit: &FailoverUnit, active: &Reconfiguration, interrupted: bool) -> bool {
    let old = &active.request.unit;
    let removal = active.secondary_loss && reconfiguration::secondary_removal(&active.request);
    unit.previous == if removal { old.previous } else { old.current }
        && unit.current.data_loss == old.current.data_loss
        && unit
            .current
            .configuration
            .checked_sub(old.current.configuration)
            .and_then(|d| d.checked_sub(i64::from(interrupted && !removal)))
            .is_some_and(|d| d > 0 && d % 0x1_0000_0001 == 0)
}

fn validate_failover(body: &Configuration, state: &EntityState) -> Result<()> {
    let active = failover_membership(state)?;
    let old = &active.request;
    let interrupted = body
        .replicas
        .0
        .iter()
        .any(|r| r.previous_role == 4 && r.current_role == 3 && !r.up && r.state == 3);
    let removal = active.secondary_loss && reconfiguration::secondary_removal(old);
    if body.replicas.0.len() != old.replicas.0.len()
        || body.service != old.service
        || !valid_stateful_sizes(&body.service)
        || body.unit.target != old.unit.target
        || body.unit.minimum != old.unit.minimum
        || body.unit.id != old.unit.id
        || body.unit.consistency != old.unit.consistency
        || !failover_epoch(&body.unit, active, interrupted)
        || (removal && !interrupted)
        || (interrupted
            && (body.replicas.0.len() != body.service.target as usize
                || !reconfiguration::secondary_removal(body)
                || body
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.previous_role >= 3)
                    .count()
                    != body.replicas.0.len()
                || body
                    .replicas
                    .0
                    .iter()
                    .filter(|r| r.current_role >= 3)
                    .count()
                    != body.replicas.0.len() - 1
                || body.replicas.0.iter().filter(|r| !r.up).count() != 2
                || body.service.minimum >= body.service.target))
    {
        return Err(Failure::Unsupported(
            "only unchanged-service no-data-loss Primary failover is enabled",
        ));
    }
    let mut primaries = 0;
    for (i, r) in body.replicas.0.iter().enumerate() {
        let expected = replica(old, &r.node)?;
        let mut normalized = r.clone();
        normalized.previous_role = expected.previous_role;
        normalized.current_role = expected.current_role;
        normalized.up = expected.up;
        normalized.state = expected.state;
        if !same_replica(&normalized, expected)
            || r.previous_role
                != if removal {
                    expected.previous_role
                } else {
                    expected.current_role
                }
            || body.replicas.0[..i]
                .iter()
                .any(|other| other.node.id == r.node.id || other.id == r.id)
            || if expected.current_role == 4 {
                r.up || r.state != 3 || r.current_role != if interrupted { 3 } else { 1 }
            } else if removal && expected.current_role == 1 {
                r.up || r.state != 3 || r.current_role != 1
            } else if interrupted {
                if r.up {
                    r.state != 2 || !matches!(r.current_role, 3 | 4)
                } else {
                    removal || r.state != 3 || r.current_role != 1
                }
            } else {
                if r.up {
                    r.state != 2 || !matches!(r.current_role, 3 | 4)
                } else {
                    !matches!(r.state, 2 | 3) || !matches!(r.current_role, 1 | 3)
                }
            }
        {
            return Err(Failure::Admission(
                "failover changed original membership or surviving replica identity",
            ));
        }
        primaries += usize::from(r.current_role == 4);
    }
    if primaries != 1 {
        return Err(Failure::Admission(
            "failover must have exactly one surviving candidate",
        ));
    }
    Ok(())
}

fn progress_query(
    work: &Submission,
    current: &EntityState,
    admission: &Admission,
    peers: &[NodeInstance],
) -> Result<Option<Transition>> {
    if current.swap.as_ref().is_some_and(|s| {
        !matches!(
            s.stage,
            SwapStage::Complete | SwapStage::ChangeConfiguration
        )
    }) {
        return Ok(None);
    }
    match &work.kind {
        Kind::GetLsn {
            source,
            generation: header,
        } => {
            if !peers.contains(source) {
                return Err(Failure::Admission("unbound survivor progress source"));
            }
            if !generation(source, source, header, admission)? {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            let request: GetLsn = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid failover progress query"))?;
            if current
                .swap
                .as_ref()
                .is_some_and(|s| !s.body.unit.current.older_than(request.unit.current))
            {
                return Ok(None);
            }
            let active = failover_membership(current)?;
            if request.unit.current.older_than(current.body.unit.current) {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            if replica(&active.request, source)?.current_role != 3
                || current.body.replica.current_role != 3
                || request.replica.node != admission.local_node
                || request.replica.id != current.body.replica.id
                || request.replica.instance != current.body.replica.instance
                || request.service != active.request.service
                || request.unit.id != active.request.unit.id
                || request.unit.consistency != active.request.unit.consistency
                || request.unit.target != active.request.unit.target
                || request.unit.minimum != active.request.unit.minimum
                || !(failover_epoch(&request.unit, active, false)
                    || (active.request.replicas.0.len() == active.request.service.target as usize
                        && active.request.service.minimum < active.request.service.target
                        && failover_epoch(&request.unit, active, true)))
            {
                return Err(Failure::Unsupported(
                    "not the exact original survivor progress query",
                ));
            }
            if let Some(query) = current
                .progress_query
                .as_ref()
                .filter(|q| !q.request.unit.current.older_than(request.unit.current))
            {
                if query.source != *source || query.request != request {
                    return Err(Failure::Unsupported("progress-query supersession is gated"));
                }
                return Ok(Some(Transition {
                    update: Update::None,
                    effects: query.reply.as_ref().map_or_else(
                        || vec![query_effect(current)],
                        |reply| {
                            vec![Effect::PeerGetLsnReply {
                                target: source.clone(),
                                body: Box::new(reply.clone()),
                            }]
                        },
                    ),
                    disposition: Disposition::Applied,
                }));
            }
            let mut state = current.clone();
            state.swap = None;
            let mut body = (*state.body).clone();
            body.unit = request.unit.clone();
            body.replica.previous_role = body.replica.current_role;
            state.body = Arc::new(body);
            state.progress_query = Some(ProgressQuery {
                source: source.clone(),
                request,
                reply: None,
                error: 0,
            });
            let effect = query_effect(&state);
            Ok(Some(lifecycle::changed(state, vec![effect], true)))
        }
        Kind::CompleteStatus if current.progress_query.is_some() => {
            let event: ConfigurationCompletion = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid progress completion"))?;
            let query = current.progress_query.as_ref().expect("query");
            if query.reply.is_some()
                || event.unit != current.body.unit
                || event.flags != 0
                || !event.replicas.0.is_empty()
                || !same_replica(&event.local, &current.body.replica)
            {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            if event.error != 0 {
                let mut state = current.clone();
                state.progress_query.as_mut().expect("query").error = event.error;
                return Ok(Some(lifecycle::changed(state, vec![], false)));
            }
            if event.local.first_lsn < 0 || event.local.last_lsn < 0 {
                return Err(Failure::Admission("uninitialized survivor progress"));
            }
            let reply = GetLsnReply {
                replica: AddInstanceReply {
                    unit: event.unit,
                    replica: event.local,
                    error: 0,
                },
                deactivation: deactivation(current)?,
            };
            let mut state = current.clone();
            state.progress_query.as_mut().expect("query").reply = Some(reply.clone());
            state.progress_query.as_mut().expect("query").error = 0;
            Ok(Some(lifecycle::changed(
                state,
                vec![Effect::PeerGetLsnReply {
                    target: query.source.clone(),
                    body: Box::new(reply),
                }],
                true,
            )))
        }
        Kind::Deactivate {
            source,
            generation: header,
        } if current.progress_query.is_some() => {
            if !peers.contains(source) {
                return Err(Failure::Admission("unbound survivor Deactivate source"));
            }
            if !generation(source, source, header, admission)? {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            let body: Deactivate = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid survivor Deactivate"))?;
            let query = current.progress_query.as_ref().expect("query");
            if body
                .configuration
                .unit
                .current
                .older_than(query.request.unit.current)
            {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            validate_failover(&body.configuration, current)?;
            if body.force
                || body.configuration.unit != query.request.unit
                || *source != query.source
                || query.reply.is_none()
                || replica(&body.configuration, source)?.current_role != 4
                || replica(&body.configuration, &admission.local_node)?.current_role != 3
                || body.deactivation.epoch != body.configuration.unit.current
                || body.deactivation.catchup_lsn
                    < query
                        .reply
                        .as_ref()
                        .expect("progress")
                        .replica
                        .replica
                        .last_lsn
            {
                return Err(Failure::Unsupported(
                    "Deactivate requires the exact queried survivor and nondecreasing history",
                ));
            }
            let mut state = current.clone();
            state.swap = Some(new_transition(
                body.configuration,
                true,
                SwapStage::Deactivated,
            ));
            state.primary_deactivation =
                Some((body.deactivation.epoch, body.deactivation.catchup_lsn));
            state.progress_query = None;
            apply(&mut state, &admission.local_node)?;
            let out = effects(&state);
            Ok(Some(lifecycle::changed(state, out, true)))
        }
        _ => Ok(None),
    }
}

fn new_transition(body: Configuration, failover: bool, stage: SwapStage) -> PrimarySwap {
    PrimarySwap {
        failover,
        intent: body.clone(),
        rap: body.clone(),
        body,
        stage,
        error: 0,
        forwarded: None,
        local_progress: None,
        remote_progress: vec![],
        deactivated: vec![],
        activated: vec![],
        change: None,
        lsn_reply: None,
        endpoint: EndpointPublishState::default(),
        endpoint_available: false,
        started: Instant::now(),
    }
}
fn elapsed(swap: &PrimarySwap) -> i64 {
    i64::try_from(swap.started.elapsed().as_nanos() / 100).unwrap_or(i64::MAX)
}

pub(super) fn handles(work: &Submission, state: Option<&EntityState>) -> Result<bool> {
    if state.is_some_and(completed_primary_failover) {
        let s = state.expect("completed failover");
        let epoch = s.swap.as_ref().expect("swap").body.unit.current;
        let newer = match work.kind {
            Kind::DoReconfiguration { .. } => {
                from_slice::<DoReconfiguration>(&work.bytes)
                    .map_err(|_| Failure::Admission("invalid repair configuration"))?
                    .configuration
                    .unit
                    .current
            }
            Kind::Activate { .. } => {
                from_slice::<Activate>(&work.bytes)
                    .map_err(|_| Failure::Admission("invalid repair activation"))?
                    .configuration
                    .unit
                    .current
            }
            Kind::Deactivate { .. } => {
                from_slice::<Deactivate>(&work.bytes)
                    .map_err(|_| Failure::Admission("invalid repair deactivation"))?
                    .configuration
                    .unit
                    .current
            }
            Kind::ActivateReply { .. }
            | Kind::DeactivateReply { .. }
            | Kind::CompleteConfiguration
            | Kind::RetryConfiguration => s
                .reconfiguration
                .as_ref()
                .map_or(epoch, |c| c.request.unit.current),
            _ => epoch,
        };
        if epoch.older_than(newer)
            && epoch.data_loss == newer.data_loss
            && epoch.configuration & !0xffff_ffff == newer.configuration & !0xffff_ffff
        {
            return Ok(false);
        }
    }
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
        | Kind::Deactivate { .. }
        | Kind::DeactivateReply { .. }
        | Kind::ActivateReply { .. }
        | Kind::CompleteConfiguration
        | Kind::RetryConfiguration => {
            state.is_some_and(|s| s.swap.is_some() || s.progress_query.is_some())
        }
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
fn query_effect(state: &EntityState) -> Effect {
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
fn get_lsn_request(state: &EntityState, remote: &Replica) -> Effect {
    let swap = state.swap.as_ref().expect("swap");
    Effect::PeerGetLsn {
        target: remote.node.clone(),
        body: Box::new(AddInstance {
            unit: swap.body.unit.clone(),
            service: swap.body.service.clone(),
            replica: Replica {
                node: remote.node.clone(),
                id: remote.id,
                instance: remote.instance,
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
                work.push(query_effect(state));
            }
            for remote in swap.body.replicas.0.iter().filter(|r| {
                r.node != state.body.replica.node
                    && r.up
                    && r.current_role >= 3
                    && !swap
                        .remote_progress
                        .iter()
                        .any(|p| p.replica.replica.node == r.node)
            }) {
                work.push(get_lsn_request(state, remote));
            }
            work
        }
        SwapStage::ChangeConfiguration => {
            vec![Effect::ChangeConfiguration(Box::new(
                swap.change.clone().expect("selection"),
            ))]
        }
        SwapStage::Activate => {
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("new Primary history");
            swap.body
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role == 3 && r.up && !swap.activated.contains(&r.node))
                .map(|target| Effect::PeerActivate {
                    target: target.node.clone(),
                    body: Box::new(Activate {
                        configuration: swap.body.clone(),
                        deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                    }),
                })
                .collect()
        }
        SwapStage::Deactivate => {
            let (epoch, catchup_lsn) = state.primary_deactivation.expect("new Primary history");
            swap.body
                .replicas
                .0
                .iter()
                .filter(|r| r.current_role == 3 && r.up && !swap.deactivated.contains(&r.node))
                .map(|target| Effect::PeerDeactivate {
                    target: target.node.clone(),
                    body: Box::new(Deactivate {
                        configuration: swap.body.clone(),
                        force: false,
                        deactivation: ReplicaDeactivationInfo { epoch, catchup_lsn },
                    }),
                })
                .collect()
        }
        SwapStage::Deactivated => {
            let target = swap
                .body
                .replicas
                .0
                .iter()
                .find(|r| r.current_role == 4)
                .expect("new Primary");
            vec![Effect::PeerDeactivateReply {
                target: target.node.clone(),
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
fn advance_get_lsn(state: &mut EntityState, config: &NativeConfig) -> Result<()> {
    let history = deactivation(state)?;
    let swap = state.swap.as_mut().expect("swap");
    if swap.failover {
        let Some(local) = &swap.local_progress else {
            return Ok(());
        };
        let progress: Vec<_> = std::iter::once((local.clone(), history))
            .chain(
                swap.remote_progress
                    .iter()
                    .map(|p| (p.replica.replica.clone(), p.deactivation.clone())),
            )
            .collect();
        if progress.iter().any(|(r, h)| {
            r.first_lsn < 0
                || r.last_lsn < 0
                || h.catchup_lsn < 0
                || h.epoch.data_loss != swap.body.unit.previous.data_loss
                || swap.body.unit.previous.older_than(h.epoch)
        }) {
            return Err(Failure::Admission("invalid no-data-loss progress/history"));
        }
        for previous in [true, false] {
            let members: Vec<_> = swap
                .body
                .replicas
                .0
                .iter()
                .filter(|r| {
                    if previous {
                        r.previous_role >= 3
                    } else {
                        r.current_role >= 3
                    }
                })
                .collect();
            if progress
                .iter()
                .filter(|(p, _)| members.iter().any(|r| r.node == p.node))
                .count()
                < members.len().div_ceil(2)
            {
                return Ok(());
            }
        }
        if elapsed(swap) < config.node(NodeProperty::RemoteReplicaProgressQueryWaitTicks)
            && swap.body.replicas.0.iter().any(|r| {
                r.up && r.current_role >= 3 && !progress.iter().any(|(p, _)| p.node == r.node)
            })
        {
            return Ok(());
        }
        let Some(latest) = progress
            .iter()
            .filter(|(r, h)| r.last_lsn >= h.catchup_lsn)
            .map(|(_, h)| (h.epoch.data_loss, h.epoch.configuration & !0xffff_ffff))
            .max()
        else {
            return Ok(());
        };
        let eligible: Vec<_> = progress
            .iter()
            .filter(|(_, h)| (h.epoch.data_loss, h.epoch.configuration & !0xffff_ffff) >= latest)
            .collect();
        let highest = eligible
            .iter()
            .map(|(r, _)| r.last_lsn)
            .max()
            .expect("eligible history");
        let mut candidate = eligible
            .iter()
            .find(|(r, _)| r.last_lsn == highest)
            .expect("highest");
        if candidate.0.node == local.node {
            let lowest = eligible
                .iter()
                .map(|(r, _)| r.last_lsn)
                .min()
                .expect("eligible");
            if lowest < highest && (local.first_lsn == 0 || local.first_lsn > lowest + 1) {
                for other in &eligible {
                    if other.0.last_lsn == highest
                        && other.0.first_lsn > 0
                        && (candidate.0.first_lsn == 0 || other.0.first_lsn < candidate.0.first_lsn)
                    {
                        candidate = other;
                    }
                }
            }
        }
        let selected = candidate.0.clone();
        let local_node = local.node.clone();
        for (r, _) in progress {
            replace(&mut swap.body, r.clone())?;
        }
        if selected.node != local_node {
            let mut change = swap.body.clone();
            for r in &mut change.replicas.0 {
                if r.node != selected.node {
                    r.first_lsn = -1;
                    r.last_lsn = -1;
                }
            }
            swap.change = Some(change);
            swap.stage = SwapStage::ChangeConfiguration;
            let active = state
                .reconfiguration
                .as_ref()
                .expect("completed membership");
            let original = replica(&active.request, &state.body.replica.node)?.clone();
            state.body = Arc::new(AddInstance {
                unit: swap.intent.unit.clone(),
                replica: Replica {
                    previous_role: original.current_role,
                    first_lsn: -1,
                    last_lsn: -1,
                    ..original
                },
                service: active.request.service.clone(),
            });
        } else {
            swap.rap = swap.body.clone();
            swap.stage = SwapStage::Catchup;
        }
        return Ok(());
    }
    if let (Some(local), Some(remote)) = (&swap.local_progress, swap.remote_progress.first()) {
        if local.last_lsn != remote.replica.replica.last_lsn
            || local.first_lsn < 0
            || local.last_lsn < 0
            || remote.replica.replica.first_lsn < 0
        {
            return Err(Failure::Unsupported(
                "Primary transition requires matched initialized progress/history; repair is gated",
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
    peers: &[NodeInstance],
) -> Result<Transition> {
    let Some(current) = current.filter(|s| ready(s, admission)) else {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    };
    if let Some(result) = progress_query(work, current, admission, peers)? {
        return Ok(result);
    }
    let decoded = if matches!(work.kind, Kind::DoReconfiguration { .. }) {
        Some(
            from_slice::<DoReconfiguration>(&work.bytes)
                .map_err(|_| Failure::Admission("invalid Primary transition"))?,
        )
    } else {
        None
    };
    let transition_body = decoded
        .as_ref()
        .map(|b| &b.configuration)
        .or_else(|| current.swap.as_ref().map(|s| &s.intent));
    let incoming_peer = match &work.kind {
        Kind::GetLsn { source, .. }
        | Kind::GetLsnReply { source, .. }
        | Kind::Deactivate { source, .. }
        | Kind::DeactivateReply { source, .. }
        | Kind::Activate { source, .. }
        | Kind::ActivateReply { source, .. } => Some(source),
        _ => None,
    };
    let peer = if let Some(source) = incoming_peer {
        peers.iter().find(|p| *p == source)
    } else if peers.len() == 1 {
        peers.first()
    } else {
        transition_body.and_then(|body| {
            body.replicas
                .0
                .iter()
                .find(|r| r.up && r.current_role >= 3 && r.node != admission.local_node)
                .and_then(|r| peers.iter().find(|p| **p == r.node))
        })
    }
    .or_else(|| {
        if incoming_peer.is_none() {
            peers.first()
        } else {
            None
        }
    })
    .ok_or(Failure::Admission(
        "Primary transition requires an original surviving peer",
    ))?;
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
            if let Some(swap) = current.swap.as_ref().filter(|s| {
                !matches!(
                    s.stage,
                    SwapStage::Complete | SwapStage::ChangeConfiguration
                ) || !s
                    .body
                    .unit
                    .current
                    .older_than(body.configuration.unit.current)
            }) {
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
                let replay = if from_fm && !swap.failover && swap.stage != SwapStage::Demote {
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
            let failover = body
                .configuration
                .replicas
                .0
                .iter()
                .any(|r| r.previous_role == 4 && matches!(r.current_role, 1 | 3) && !r.up);
            if failover {
                validate_failover(&body.configuration, current)?;
            } else {
                validate(&body.configuration, current)?;
            }
            let local = replica(&body.configuration, &admission.local_node)?;
            if failover {
                if !from_fm
                    || local.previous_role != 3
                    || local.current_role != 4
                    || body.phase0_duration != i64::MAX
                {
                    return Err(Failure::Unsupported(
                        "failover requires FM promotion of an original ActiveSecondary",
                    ));
                }
            } else if (from_fm && (local.previous_role != 4 || body.phase0_duration != i64::MAX))
                || (!from_fm
                    && (local.current_role != 4
                        || body.phase0_duration <= 0
                        || body.phase0_duration == i64::MAX))
            {
                return Err(Failure::Unsupported(
                    "swap requires old Primary demotion before peer continuation",
                ));
            }
            state.swap = Some(new_transition(
                body.configuration.clone(),
                failover,
                if from_fm && !failover {
                    SwapStage::Demote
                } else {
                    SwapStage::GetLsn
                },
            ));
            state.swap.as_mut().expect("swap").forwarded = (!from_fm).then_some(body);
            if failover {
                state.remote_idles.clear();
                state.progress_query = None;
            }
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
                vec![query_effect(current)]
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
            advance_get_lsn(&mut state, config)?;
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
                || !expected.up
                || expected.current_role != 3
                || !same_replica(&body.replica.replica, expected)
                || body.replica.replica.first_lsn < 0
                || body.replica.replica.last_lsn < 0
                || body.deactivation.epoch.data_loss != swap.body.unit.previous.data_loss
                || swap.body.unit.previous.older_than(body.deactivation.epoch)
                || body.deactivation.catchup_lsn < 0
            {
                return stale();
            }
            if swap
                .remote_progress
                .iter()
                .any(|r| r.replica.replica.node == *peer)
            {
                return stale();
            }
            state
                .swap
                .as_mut()
                .expect("swap")
                .remote_progress
                .push(body);
            advance_get_lsn(&mut state, config)?;
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
            if swap.failover
                && swap.stage == SwapStage::Catchup
                && swap
                    .local_progress
                    .as_ref()
                    .is_none_or(|p| p.last_lsn != body.local.last_lsn)
            {
                return Err(Failure::Admission(
                    "failover catch-up changed progress without a data-loss exchange",
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
                    let has_remote = swap
                        .body
                        .replicas
                        .0
                        .iter()
                        .any(|r| r.up && r.current_role == 3);
                    let s = state.swap.as_mut().expect("swap");
                    s.stage = if !has_remote {
                        s.rap = s.body.clone();
                        SwapStage::End
                    } else if swap.failover {
                        SwapStage::Deactivate
                    } else {
                        SwapStage::Activate
                    };
                }
                _ => {
                    let s = state.swap.as_mut().expect("swap");
                    s.body = stable(&s.body);
                    if s.failover {
                        s.body.replicas.0.retain(|r| r.current_role >= 3);
                    }
                    s.endpoint.clear(&mut s.endpoint_available);
                    s.stage = SwapStage::Complete;
                }
            }
            apply(&mut state, &admission.local_node)?;
            if state
                .swap
                .as_ref()
                .is_some_and(|s| s.failover && s.stage == SwapStage::Complete)
                && state.body.replica.current_role == 4
            {
                state.fm_primary = true;
            }
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
            if replica(&swap.intent, source)?.current_role != 4
                || body.configuration.unit != swap.intent.unit
                || body.configuration.service != swap.intent.service
                || body.configuration.replicas.0.len() != swap.intent.replicas.0.len()
                || body.deactivation.epoch != swap.intent.unit.current
                || body.deactivation.catchup_lsn < 0
                || (swap.failover && body.deactivation != deactivation(current)?)
            {
                return stale();
            }
            for r in &body.configuration.replicas.0 {
                if !same_replica(r, replica(&swap.rap, &r.node)?) {
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
            if swap.stage
                != if swap.failover {
                    SwapStage::Deactivated
                } else {
                    SwapStage::Continue
                }
            {
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
        Kind::Deactivate {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: Deactivate = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid Deactivate retry"))?;
            let Some(swap) = current
                .swap
                .as_ref()
                .filter(|s| s.failover && s.stage == SwapStage::Deactivated)
            else {
                return stale();
            };
            if body.force
                || replica(&swap.intent, source)?.current_role != 4
                || body.configuration != swap.intent
                || deactivation(current)? != body.deactivation
            {
                return stale();
            }
            return Ok(Transition {
                update: Update::None,
                effects: effects(current),
                disposition: Disposition::Applied,
            });
        }
        Kind::DeactivateReply {
            source,
            generation: header,
        } => {
            if !generation(source, peer, header, admission)? {
                return stale();
            }
            let body: DeactivateReply = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid survivor DeactivateReply"))?;
            let Some(swap) = current
                .swap
                .as_ref()
                .filter(|s| s.failover && s.stage == SwapStage::Deactivate)
            else {
                return stale();
            };
            let remote = replica(&swap.body, peer)?;
            let expected = Replica {
                node: remote.node.clone(),
                id: remote.id,
                instance: remote.instance,
                up: false,
                state: 3,
                ..Replica::default()
            };
            if !remote.up
                || remote.current_role != 3
                || swap.deactivated.contains(peer)
                || body.unit != swap.body.unit
                || body.replica != expected
                || body.error != 0
            {
                return stale();
            }
            let s = state.swap.as_mut().expect("failover");
            s.deactivated.push(peer.clone());
            if s.body
                .replicas
                .0
                .iter()
                .filter(|r| r.up && r.current_role == 3)
                .all(|r| s.deactivated.contains(&r.node))
            {
                s.stage = SwapStage::Activate;
            }
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
            let remote = replica(&expected, peer)?;
            if !remote.up
                || remote.current_role != 3
                || swap.activated.contains(peer)
                || body.unit != expected.unit
                || body.error != 0
                || !same_replica(&body.replica, remote)
                || body.replica.first_lsn < 0
                || body.replica.last_lsn < 0
                || !body.replica.service_endpoint.is_empty()
            {
                return stale();
            }
            let s = state.swap.as_mut().expect("swap");
            replace(&mut s.body, body.replica)?;
            // EndReconfiguration still needs the transition's PC roles.
            let original = replica(&s.intent, peer)?;
            s.body
                .replicas
                .0
                .iter_mut()
                .find(|r| r.node == *peer)
                .expect("peer")
                .previous_role = original.previous_role;
            s.activated.push(peer.clone());
            if s.body
                .replicas
                .0
                .iter()
                .filter(|r| r.up && r.current_role == 3)
                .all(|r| s.activated.contains(&r.node))
            {
                s.stage = SwapStage::End;
                s.rap = s.body.clone();
            }
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
            if s.stage == SwapStage::GetLsn {
                advance_get_lsn(&mut state, config)?;
            }
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
