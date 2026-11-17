use super::*;
use std::time::Instant;

const GET_LSN_NOT_FOUND: i64 = 0x8007_1cf0_u32 as i32 as i64;

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
    pub restarting: Vec<NodeInstance>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ForcedDeactivation {
    pub source: NodeInstance,
    pub request: Deactivate,
}

pub(super) fn forced_deactivation_reply(state: &EntityState) -> Option<Effect> {
    let owner = state.forced_deactivation.as_ref()?;
    (state.phase == Phase::Closed).then(|| Effect::PeerDeactivateReply {
        target: owner.source.clone(),
        body: Box::new(DeactivateReply {
            unit: owner.request.configuration.unit.clone(),
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
    })
}

fn forced_deactivation(
    work: &Submission,
    current: &EntityState,
    admission: &Admission,
    peers: &[NodeInstance],
) -> Result<Option<Transition>> {
    let Kind::Deactivate {
        source,
        generation: header,
    } = &work.kind
    else {
        return Ok(None);
    };
    let request: Deactivate =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid forced Deactivate"))?;
    if !request.force {
        return Ok(None);
    }
    if !peers.contains(source) {
        return Err(Failure::Admission("unbound forced Deactivate source"));
    }
    if !generation(source, source, header, admission)?
        || request
            .configuration
            .unit
            .current
            .older_than(current.body.unit.current)
        || replica(&request.configuration, &admission.local_node)?.instance
            < current.body.replica.instance
    {
        return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
    }
    if let Some(owner) = &current.forced_deactivation {
        if owner.source != *source || owner.request != request {
            return Err(Failure::Unsupported(
                "forced Deactivate supersession is gated",
            ));
        }
        let mut out = vec![];
        if let Some(reply) = forced_deactivation_reply(current) {
            out.push(reply);
        } else {
            lifecycle::close_retry(current, &mut out);
        }
        return Ok(Some(Transition {
            update: Update::None,
            effects: out,
            disposition: Disposition::Applied,
        }));
    }
    let closed = current.phase == Phase::Closed
        && current.runtime.is_none()
        && current.usage.is_none()
        && admission.node_open
        && admission.owner_ready
        && admission.node_activated;
    if !ready(current, admission) && !closed {
        return Ok(Some(Transition::ignored(Disposition::IgnoredNotReady)));
    }
    let active = reconfiguration::completed_configuration(current).ok_or(Failure::Unsupported(
        "forced Deactivate requires active membership",
    ))?;
    let local = replica(&request.configuration, &admission.local_node)?;
    if current.body.service.persisted
        || request.deactivation != ReplicaDeactivationInfo::default()
        || local.current_role != 3
        || local.id != current.body.replica.id
        || local.instance != current.body.replica.instance
        || replica(&request.configuration, source)?.current_role != 4
        || request
            .configuration
            .replicas
            .0
            .iter()
            .any(|r| r.previous_role != 1)
        || current
            .progress_query
            .as_ref()
            .is_some_and(|q| q.source != *source || q.request.unit != request.configuration.unit)
        || current.swap.as_ref().is_some_and(|s| {
            s.stage != SwapStage::Complete
                && (s.stage != SwapStage::Continue || s.intent.unit != request.configuration.unit)
        })
    {
        return Err(Failure::Admission(
            "forced Deactivate changed the planned handoff owner",
        ));
    }
    let mut intent = request.configuration.clone();
    for r in &mut intent.replicas.0 {
        let previous = replica(active, &r.node)?;
        r.previous_role = previous.current_role;
        if r.node == *source {
            r.service_endpoint = previous.service_endpoint.clone();
        }
    }
    validate(&intent, current)?;
    let mut state = current.clone();
    state.forced_deactivation = Some(ForcedDeactivation {
        source: source.clone(),
        request,
    });
    let mut body = (*state.body).clone();
    body.unit = intent.unit;
    state.body = Arc::new(body);
    let mut out = vec![];
    lifecycle::start_close(&mut state, CloseMode::Deactivate, work.id, &mut out);
    Ok(Some(lifecycle::changed(state, out, true)))
}

fn deactivation(state: &EntityState) -> Result<ReplicaDeactivationInfo> {
    state
        .primary_deactivation
        .map(|(epoch, catchup_lsn)| ReplicaDeactivationInfo { epoch, catchup_lsn })
        .or_else(|| state.peer_creation.as_ref().map(|p| p.deactivation.clone()))
        .ok_or(Failure::Admission("missing original replica history"))
}

fn failover_membership<'a>(
    state: &'a EntityState,
    unit: &FailoverUnit,
) -> Result<(&'a Configuration, bool)> {
    let completed = reconfiguration::completed_configuration(state);
    if let Some(active) = state.reconfiguration.as_ref().filter(|c| {
        completed.is_none_or(|latest| !c.request.unit.current.older_than(latest.unit.current))
    }) {
        if active.stage != ConfigurationStage::Complete
            && !reconfiguration::secondary_removal_in_progress(state)
        {
            return Err(Failure::Unsupported(
                "failover cannot interrupt this membership change",
            ));
        }
        if active.request.replicas.0.len() >= 2 && retains_removal_pc(unit, active) {
            return Ok((&active.request, true));
        }
    }
    completed
        .filter(|c| c.replicas.0.len() >= 2)
        .map(|c| (c, false))
        .ok_or(Failure::Unsupported(
            "failover requires completed membership or exact Secondary removal",
        ))
}

fn retains_removal_pc(unit: &FailoverUnit, active: &Reconfiguration) -> bool {
    // Local Secondary completion can precede the Primary/FM's final completion.
    active.secondary_loss
        && reconfiguration::secondary_removal(&active.request)
        && (active.stage != ConfigurationStage::Complete
            || unit.previous == active.request.unit.previous)
}

fn failover_epoch(
    unit: &FailoverUnit,
    active: &Configuration,
    removal: bool,
    interrupted: bool,
) -> bool {
    let old = &active.unit;
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
    let (active, removal) = failover_membership(state, &body.unit)?;
    let mut stable;
    let old = if !removal {
        stable = reconfiguration::stable(active);
        stable.replicas.0.retain(|r| r.current_role >= 3);
        &stable
    } else {
        active
    };
    // Ordinary failover can also retain the down former Primary to preserve minimum.
    let interrupted = removal || failover_epoch(&body.unit, active, removal, true);
    if body.replicas.0.len() != old.replicas.0.len()
        || body.service != old.service
        || !valid_stateful_sizes(&body.service)
        || body.unit.target != old.unit.target
        || body.unit.minimum != old.unit.minimum
        || body.unit.id != old.unit.id
        || body.unit.consistency != old.unit.consistency
        || !failover_epoch(&body.unit, active, removal, interrupted)
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
                r.up || r.state != 3
                    || !matches!(r.current_role, 1 | 3)
                    || (interrupted && r.current_role != 3)
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
            let (membership, removal) = failover_membership(current, &request.unit)?;
            if request.unit.current.older_than(current.body.unit.current) {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            if replica(membership, source)?.current_role != 3
                || current.body.replica.current_role != 3
                || request.replica.node != admission.local_node
                || request.replica.id != current.body.replica.id
                || request.replica.instance != current.body.replica.instance
                || request.service != membership.service
                || request.unit.id != membership.unit.id
                || request.unit.consistency != membership.unit.consistency
                || request.unit.target != membership.unit.target
                || request.unit.minimum != membership.unit.minimum
                || !(failover_epoch(&request.unit, membership, removal, false)
                    || (membership.replicas.0.len() == membership.service.target as usize
                        && membership.service.minimum < membership.service.target
                        && failover_epoch(&request.unit, membership, removal, true)))
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
            // Keep completed membership as the baseline for the next Activate.
            if state
                .swap
                .as_ref()
                .is_some_and(|s| s.stage == SwapStage::ChangeConfiguration)
            {
                state.swap = None;
            }
            let mut body = (*state.body).clone();
            body.unit = request.unit.clone();
            body.unit.previous = if removal {
                membership.unit.previous
            } else {
                membership.unit.current
            };
            if let Some(configuration) = &mut state.configuration {
                configuration.unit = body.unit.clone();
                for r in &mut configuration.replicas.0 {
                    r.previous_role = if removal {
                        replica(membership, &r.node)?.previous_role
                    } else {
                        r.current_role
                    };
                }
            }
            body.replica.previous_role = if removal {
                replica(membership, &body.replica.node)?.previous_role
            } else {
                body.replica.current_role
            };
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
        Kind::Activate {
            source,
            generation: header,
        } if current
            .swap
            .as_ref()
            .is_none_or(|s| s.stage == SwapStage::Complete) =>
        {
            let body: Activate = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid planned activation"))?;
            let Some(active) = reconfiguration::completed_configuration(current).filter(|c| {
                current.body.replica.current_role == 3
                    && body.configuration.unit.current.configuration & !0xffff_ffff
                        != c.unit.current.configuration & !0xffff_ffff
            }) else {
                return Ok(None);
            };
            if !peers.contains(source) {
                return Err(Failure::Admission("unbound planned activation source"));
            }
            if !generation(source, source, header, admission)? {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            if body
                .configuration
                .unit
                .current
                .older_than(current.body.unit.current)
            {
                return Ok(Some(Transition::ignored(Disposition::IgnoredStale)));
            }
            let primary = replica(&body.configuration, source)?;
            let mut intent = body.configuration.clone();
            for r in &mut intent.replicas.0 {
                if !r.up && r.state == 3 && r.current_role == 3 && r.node != admission.local_node {
                    r.up = true;
                    r.state = 2;
                }
            }
            let mut original_endpoint = primary.clone();
            original_endpoint.service_endpoint = replica(active, source)?.service_endpoint.clone();
            replace(&mut intent, original_endpoint)?;
            validate(&intent, current)?;
            if current.progress_query.as_ref().is_some_and(|query| {
                body.configuration.unit != query.request.unit || *source != query.source
            }) || primary.current_role != 4
                || primary.service_endpoint.is_empty()
                || replica(&body.configuration, &admission.local_node)?.current_role != 3
                || body.deactivation.epoch != body.configuration.unit.current
                || body.deactivation.catchup_lsn < 0
            {
                return Err(Failure::Admission(
                    "planned activation requires exact unchanged membership and current history",
                ));
            }
            let mut state = current.clone();
            let mut swap = new_transition(body.configuration, false, SwapStage::Complete);
            swap.body = stable(&swap.body);
            swap.lsn_reply = current
                .progress_query
                .as_ref()
                .and_then(|query| query.reply.clone());
            state.swap = Some(swap);
            state.primary_deactivation =
                Some((body.deactivation.epoch, body.deactivation.catchup_lsn));
            state.progress_query = None;
            apply(&mut state, &admission.local_node)?;
            let out = effects(&state);
            Ok(Some(lifecycle::changed(state, out, true)))
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
        restarting: vec![],
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
    if matches!(work.kind, Kind::Deactivate { .. })
        && from_slice::<Deactivate>(&work.bytes)
            .map_err(|_| Failure::Admission("invalid Deactivate"))?
            .force
    {
        return Ok(true);
    }
    if state.is_some_and(|s| {
        s.swap
            .as_ref()
            .is_some_and(|s| s.stage == SwapStage::Complete)
    }) {
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
        Kind::Activate { .. } => {
            let body: Activate = from_slice(&work.bytes)
                .map_err(|_| Failure::Admission("invalid swap activation"))?;
            state.is_some_and(|s| {
                s.swap.is_some()
                    || s.progress_query.is_some()
                    || (s.body.replica.current_role == 3
                        && body.configuration.unit.current.configuration & !0xffff_ffff
                            != s.body.unit.current.configuration & !0xffff_ffff)
            })
        }
        Kind::Deactivate { .. }
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
    let swap = state.swap.as_ref().expect("swap");
    let body = &swap.body;
    if swap.stage != SwapStage::ChangeConfiguration {
        state.body = Arc::new(AddInstance {
            unit: body.unit.clone(),
            service: body.service.clone(),
            replica: replica(body, node)?.clone(),
        });
    }
    let mut configuration = body.clone();
    if swap.stage == SwapStage::ChangeConfiguration {
        for replica in &mut configuration.replicas.0 {
            replica.current_role = replica.previous_role;
            replica.first_lsn = -1;
            replica.last_lsn = -1;
        }
        configuration
            .replicas
            .0
            .retain(|replica| replica.node == *node || replica.previous_role != 2);
    }
    state.configuration = Some(configuration);
    state.intermediate_configuration = matches!(
        swap.stage,
        SwapStage::Deactivate | SwapStage::Deactivated | SwapStage::Activate | SwapStage::End
    )
    .then(|| body.clone());
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
            "swap requires the current FM generation",
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
    if state
        .reconfiguration
        .as_ref()
        .is_some_and(|c| c.stage != ConfigurationStage::Complete)
    {
        return Err(Failure::Unsupported(
            "swap cannot interrupt membership changes",
        ));
    }
    let active = reconfiguration::completed_configuration(state).ok_or(Failure::Unsupported(
        "swap requires completed active membership",
    ))?;
    let mut old = stable(active);
    old.replicas.0.retain(|r| r.current_role >= 3);
    if !matches!((old.unit.target, old.unit.minimum), (2, 1) | (3, 2))
        || body.replicas.0.len() != old.unit.target as usize
        || old.replicas.0.len() != body.replicas.0.len()
        || body.service != old.service
        || body.unit.target != old.unit.target
        || body.unit.minimum != old.unit.minimum
        || body.unit.id != old.unit.id
        || body.unit.consistency != old.unit.consistency
        || body.unit.previous != old.unit.current
        || body.unit.current.data_loss != old.unit.current.data_loss
        || body.unit.current.configuration
            != old
                .unit
                .current
                .configuration
                .checked_add(0x1_0000_0001)
                .ok_or(Failure::Capacity)?
    {
        return Err(Failure::Unsupported(
            "only healthy unchanged-membership 2/1 and 3/2 planned swaps are enabled",
        ));
    }
    if old
        .replicas
        .0
        .iter()
        .filter(|r| r.current_role == 4)
        .count()
        != 1
        || body
            .replicas
            .0
            .iter()
            .filter(|r| r.current_role == 4)
            .count()
            != 1
    {
        return Err(Failure::Admission(
            "planned swap requires exactly one Primary",
        ));
    }
    for (index, r) in body.replicas.0.iter().enumerate() {
        let expected = replica(&old, &r.node)?;
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
            || !matches!(expected.current_role, 3 | 4)
            || if expected.current_role == 4 {
                r.current_role != 3
            } else {
                !matches!(r.current_role, 3 | 4)
            }
            || !r.up
            || r.state != 2
            || body.replicas.0[..index]
                .iter()
                .any(|other| other.node.id == r.node.id || other.id == r.id)
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
                .filter(|r| {
                    r.current_role == 3
                        && r.up
                        && !swap.restarting.contains(&r.node)
                        && !swap.activated.contains(&r.node)
                })
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
    if matches!(swap.stage, SwapStage::Catchup | SwapStage::Activate) {
        for target in &swap.restarting {
            let mut configuration = swap.intent.clone();
            for r in &mut configuration.replicas.0 {
                r.previous_role = 1;
            }
            result.push(Effect::PeerDeactivate {
                target: target.clone(),
                body: Box::new(Deactivate {
                    configuration,
                    force: true,
                    deactivation: ReplicaDeactivationInfo::default(),
                }),
            });
        }
    }
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
    let Some(local) = &swap.local_progress else {
        return Ok(());
    };
    let progress: Vec<_> = std::iter::once((local.clone(), history))
        .chain(
            swap.remote_progress
                .iter()
                .filter(|p| p.replica.error == 0)
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
            r.up && r.current_role >= 3
                && r.node != local.node
                && !swap
                    .remote_progress
                    .iter()
                    .any(|p| p.replica.replica.node == r.node)
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
    if swap.failover && candidate.0.node == local.node {
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
    if !swap.failover && selected.node != local_node {
        return Err(Failure::Admission(
            "forwarded swap candidate must retain the latest eligible progress",
        ));
    }
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
        if !swap.failover {
            swap.restarting = planned_secondary_restarts(swap);
            for r in &mut swap.rap.replicas.0 {
                if swap.restarting.contains(&r.node) {
                    r.up = false;
                }
            }
        }
        swap.stage = SwapStage::Catchup;
    }
    Ok(())
}

fn planned_secondary_restarts(swap: &PrimarySwap) -> Vec<NodeInstance> {
    let local = swap
        .local_progress
        .as_ref()
        .expect("selected swap candidate");
    swap.body
        .replicas
        .0
        .iter()
        .filter(|r| r.up && r.current_role == 3)
        .filter(|r| {
            swap.remote_progress
                .iter()
                .find(|p| p.replica.replica.node == r.node)
                .is_none_or(|p| {
                    let last = p.replica.replica.last_lsn;
                    p.replica.error == GET_LSN_NOT_FOUND
                        || (last < local.last_lsn
                            && (local.first_lsn == 0 || last < local.first_lsn - 1))
                })
        })
        .map(|r| r.node.clone())
        .collect()
}

fn finish_activation(swap: &mut PrimarySwap) {
    if swap.restarting.is_empty()
        && swap
            .body
            .replicas
            .0
            .iter()
            .filter(|r| r.up && r.current_role == 3)
            .all(|r| swap.activated.contains(&r.node))
    {
        swap.stage = SwapStage::End;
        swap.rap = swap.body.clone();
    }
}

pub(super) fn transition(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
    config: &NativeConfig,
    peers: &[NodeInstance],
) -> Result<Transition> {
    let Some(current) = current else {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    };
    if let Some(result) = forced_deactivation(work, current, admission, peers)? {
        return Ok(result);
    }
    if !ready(current, admission) {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
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
        Kind::DoReconfiguration { source, .. } if source != &admission.fm_source => Some(source),
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
                .find(|r| r.current_role == 4 && r.node != admission.local_node)
                .or_else(|| {
                    body.replicas
                        .0
                        .iter()
                        .find(|r| r.up && r.current_role >= 3 && r.node != admission.local_node)
                })
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
                let replay = if from_fm
                    && !swap.failover
                    && swap.stage != SwapStage::Demote
                    && replica(&swap.intent, &admission.local_node)?.previous_role == 4
                {
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
                        || replica(&body.configuration, source)?.previous_role != 4
                        || replica(&body.configuration, source)?.current_role != 3
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
            state.progress_query = None;
            if failover {
                state.remote_idles.clear();
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
            let unknown = !swap.failover
                && body.replica.error == GET_LSN_NOT_FOUND
                && body.deactivation
                    == (ReplicaDeactivationInfo {
                        epoch: Epoch::default(),
                        catchup_lsn: -2,
                    });
            // A survivor reports its own PC, not the candidate's requested PC.
            let mut reported_unit = body.replica.unit.clone();
            reported_unit.previous = swap.body.unit.previous;
            if reported_unit != swap.body.unit
                || (body.replica.error != 0 && !unknown)
                || !expected.up
                || expected.current_role != 3
                || !same_replica(&body.replica.replica, expected)
                || (!unknown
                    && (body.replica.replica.first_lsn < 0
                        || body.replica.replica.last_lsn < 0
                        || body.deactivation.epoch.data_loss != swap.body.unit.previous.data_loss
                        || swap.body.unit.previous.older_than(body.deactivation.epoch)
                        || body.deactivation.catchup_lsn < 0))
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
            if swap.stage == SwapStage::Catchup
                && swap
                    .local_progress
                    .as_ref()
                    .is_none_or(|p| p.last_lsn != body.local.last_lsn)
            {
                return Err(Failure::Admission(
                    "Primary catch-up changed progress without a data-loss exchange",
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
                .is_some_and(|s| s.stage == SwapStage::Complete)
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
                let expected = replica(&swap.rap, &r.node)?;
                let mut actual = r.clone();
                if !r.up && r.state == 3 && r.current_role == 3 && r.node != admission.local_node {
                    actual.up = expected.up;
                    actual.state = expected.state;
                }
                if !same_replica(&actual, expected) {
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
            s.stage = if s.failover {
                SwapStage::Secondary
            } else {
                s.body = stable(&s.body);
                SwapStage::Complete
            };
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
            if let Some(swap) = current.swap.as_ref().filter(|s| {
                !s.failover
                    && matches!(s.stage, SwapStage::Catchup | SwapStage::Activate)
                    && s.restarting.contains(source)
            }) {
                let remote = replica(&swap.body, source)?;
                let expected = Replica {
                    node: remote.node.clone(),
                    id: remote.id,
                    instance: remote.instance,
                    up: false,
                    state: 3,
                    ..Replica::default()
                };
                if body.unit != swap.body.unit || body.replica != expected || body.error != 0 {
                    return stale();
                }
                let s = state.swap.as_mut().expect("swap");
                let mut dropped = remote.clone();
                dropped.up = false;
                dropped.state = 3;
                replace(&mut s.body, dropped)?;
                s.restarting.retain(|node| node != source);
                if s.stage == SwapStage::Activate {
                    finish_activation(s);
                }
                apply(&mut state, &admission.local_node)?;
                let out = effects(&state);
                return Ok(lifecycle::changed(state, out, true));
            }
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
                || swap.restarting.contains(peer)
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
            finish_activation(s);
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
    apply(&mut state, &admission.local_node)?;
    let out = effects(&state);
    Ok(lifecycle::changed(state, out, true))
}
