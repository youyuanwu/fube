//! Selected stateless native helper closure. Source baseline and branch/scenario
//! mappings are in docs/cluster/RaStatelessRemoval.md; these are trusted adapter
//! events, not a live Hosting/RAP implementation.
use super::*;
use fube_transport::serialization::serde::ObjectArray;

pub const APPLICATION_HOST_CRASH: i64 = -2_147_017_573;
pub const STALE_REQUEST: i64 = SERVICE_TYPE_NOT_REGISTERED + 50;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRegistration {
    /// Actual Hosting registration identifier, not requested descriptor spelling.
    /// The trusted lookup adapter certifies semantic matching to its request.
    pub service_type: ServiceType,
    pub host: String,
    pub runtime: String,
    /// Reference acquisition identity, equal to the issued lookup ID.
    pub acquisition: u64,
    pub code_package: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistrationCompletion {
    pub unit: PartitionId,
    pub token: u64,
    pub lookup: u64,
    pub registration: RuntimeRegistration,
    pub error: i64,
    /// Hosting FindServiceTypeRegistrationErrorCode.IsRetryable. Exactly one
    /// result/acquired reference per issued lookup; replaying its callback MUST
    /// NOT acquire again. Unissued lookup ownership remains with the caller.
    pub retryable: bool,
    pub version: VersionInstance,
    /// Native upgrade-version map entry used in mismatch selection, if present.
    pub required_upgrade: u64,
    pub upgrade_present: bool,
    /// A separately acquired usage lease owned by this issued lookup.
    pub usage_acquired: bool,
    /// Failed Hosting increment is retryable without incrementing FSR count.
    pub usage_error: i64,
    pub error_message: String,
}
/// Success certifies matching RAP cleanup INCLUDING queued/in-flight Open:
/// cancelled/drained operations, closed proxy and resources. A termination
/// callback alone does not satisfy this contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseCompletion {
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub error: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostDown {
    pub unit: PartitionId,
    pub host: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum CloseMode {
    Delete = 1,
    Obliterate = 2,
    ForceDelete = 3,
    ForceAbort = 4,
    Drop = 5,
    AppHostDown = 6,
}
impl CloseMode {
    fn deletes(self) -> bool {
        matches!(self, Self::Delete | Self::ForceDelete)
    }
    fn force(self) -> bool {
        matches!(
            self,
            Self::Obliterate | Self::ForceDelete | Self::ForceAbort
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum FmStage {
    None = 0,
    ReplicaDropped = 1,
    ReplicaDown = 2,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthEvent {
    Close,
    CloseWarning,
    ClearCloseWarning,
    ServiceTypeRegistrationWarning,
    ClearServiceTypeRegistrationWarning,
    OpenWarning,
    ClearOpenWarning,
}

pub(super) fn changed(state: EntityState, effects: Vec<Effect>, durable: bool) -> Transition {
    Transition {
        update: if durable {
            Update::Durable(state)
        } else {
            Update::Memory(state)
        },
        effects,
        disposition: Disposition::Applied,
    }
}
pub(super) fn canonical(state: &EntityState, dropped: bool) -> Replica {
    Replica {
        node: state.body.replica.node.clone(),
        id: state.body.replica.id,
        instance: state.body.replica.instance,
        up: !dropped,
        state: if dropped { 3 } else { 2 },
        ..Replica::default()
    }
}
pub(super) fn mutate_body(state: &mut EntityState, f: impl FnOnce(&mut AddInstance)) {
    let mut body = (*state.body).clone();
    f(&mut body);
    state.body = Arc::new(body);
}
fn mark_deleted(state: &mut EntityState) {
    state.local_deleted = true;
    state.fm_stage = FmStage::None;
    if state.phase == Phase::Closed {
        state.cleanup_pending = true;
    }
}
fn report(state: &EntityState) -> ReplicaUp {
    ReplicaUp {
        dropped: ObjectArray(vec![FailoverUnitInfo {
            service: state.body.service.clone(),
            unit: state.body.unit.clone(),
            intermediate: Epoch::default(), // native Epoch::InvalidEpoch() == (0,0)
            report_from_primary: false,
            replicas: ObjectArray(vec![ReplicaInfo {
                description: canonical(state, true),
                intermediate_role: 1,
            }]),
            // Native TryGetConfigurationInternal deliberately uses false here.
            local_deleted: false,
        }]),
        ..ReplicaUp::default()
    }
}
fn semantic_reply(state: &EntityState, mode: CloseMode, effects: &mut Vec<Effect>) {
    if mode == CloseMode::Delete && state.phase == Phase::Closed && state.local_deleted {
        effects.push(Effect::RemoveReply(Box::new(RemoveInstanceReply {
            unit: state.body.unit.clone(),
            replica: canonical(state, true),
            error: 0,
        })));
    }
}
fn finish_close(state: &mut EntityState, effects: &mut Vec<Effect>) {
    let mode = state.close_mode.expect("pending close mode");
    let health = state.phase != Phase::Closed && state.body.replica.up;
    state.phase = Phase::Closed;
    state.close_mode = None;
    state.close_failures = 0;
    state.open_failures = 0;
    state.registration_failures = 0;
    state.close_warning = false;
    state.registration_warning = false;
    state.cleanup_pending = state.local_deleted;
    let replica = canonical(state, true);
    mutate_body(state, |body| body.replica = replica);
    if let Some(registration) = state.runtime.take() {
        effects.push(Effect::ReleaseRegistration(registration));
    }
    opening::release_usage(state, effects);
    if !state.local_deleted {
        state.fm_stage = if mode == CloseMode::AppHostDown {
            FmStage::ReplicaDown
        } else {
            FmStage::ReplicaDropped
        };
        effects.push(Effect::RequestReportRetry);
    }
    if health {
        effects.push(Effect::Health(HealthEvent::Close));
    }
}
fn close_retry(state: &EntityState, effects: &mut Vec<Effect>) {
    if state.phase != Phase::Closing || state.close_mode == Some(CloseMode::Obliterate) {
        return;
    }
    if let Some(registration) = &state.runtime {
        let body = AddInstance {
            unit: state.body.unit.clone(),
            replica: canonical(state, false),
            service: state.body.service.clone(),
        };
        effects.push(Effect::Close {
            token: state.close_token,
            body: Arc::new(body),
            registration: registration.clone(),
            flags: if state.close_mode.is_some_and(CloseMode::force) {
                4
            } else {
                8
            },
        });
    } else {
        effects.push(Effect::FindRegistration {
            token: state.close_token,
            lookup: 0,
            body: state.body.clone(),
            usage: state.usage,
        });
    }
}
fn can_close(state: &EntityState, mode: CloseMode) -> bool {
    if state.phase == Phase::Closed {
        return mode != CloseMode::AppHostDown;
    }
    if !state.body.replica.up {
        return mode.force();
    }
    if state.phase == Phase::Closing {
        return mode == CloseMode::AppHostDown
            || (mode == CloseMode::Obliterate && state.close_mode != Some(mode))
            || (mode.force() && state.runtime.is_none());
    }
    true
}
fn start_close(
    state: &mut EntityState,
    mode: CloseMode,
    token: u64,
    effects: &mut Vec<Effect>,
) -> bool {
    if !can_close(state, mode) {
        return false;
    }
    start_close_inner(state, mode, token, effects, true);
    true
}
fn start_close_inner(
    state: &mut EntityState,
    mode: CloseMode,
    token: u64,
    effects: &mut Vec<Effect>,
    dispatch_retry: bool,
) {
    if state.phase == Phase::Closed {
        if mode.deletes() {
            mark_deleted(state);
        }
        let replica = canonical(state, true);
        mutate_body(state, |body| body.replica = replica);
        semantic_reply(state, mode, effects);
        return;
    }
    if mode == CloseMode::AppHostDown && state.close_mode == Some(CloseMode::Obliterate) {
        finish_close(state, effects);
        return;
    }
    let was_creating = state.phase == Phase::Opening;
    if mode.deletes() {
        mark_deleted(state);
    }
    state.phase = Phase::Closing;
    state.close_mode = Some(mode);
    state.close_token = token;
    state.close_failures = 0;
    state.close_warning = false;
    mutate_body(state, |body| body.replica.state = 3);
    if mode == CloseMode::AppHostDown || (state.runtime.is_none() && (mode.force() || was_creating))
    {
        finish_close(state, effects);
        semantic_reply(state, mode, effects);
    } else if mode == CloseMode::Obliterate {
        effects.push(Effect::TerminateHost {
            registration: state.runtime.clone().expect("active host"),
        });
    } else if dispatch_retry {
        close_retry(state, effects);
    }
}
fn admit(
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
) -> Result<()> {
    if source != &admission.fm_source {
        return Err(Failure::Admission("configured FM session mismatch"));
    }
    if generation.for_fm_replica {
        return Err(Failure::Unsupported("FMM profile is disabled"));
    }
    Ok(())
}
fn check_stateless(body: &AddInstance, admission: &Admission) -> Result<()> {
    if body.replica.node != admission.local_node {
        return Err(Failure::Admission(
            "configured local node incarnation mismatch",
        ));
    }
    if body.service.stateful
        || body.service.persisted
        || body.service.group
        || body.replica.id < 0
        || body.replica.instance < 0
    {
        return Err(Failure::Unsupported(
            "only admitted stateless removal is enabled",
        ));
    }
    Ok(())
}

/// Directly dependent native Closed Add branches. Broad replacement of an open
/// replica/service description remains explicitly disabled.
pub(super) fn service_matches(state: &EntityState, service: &Service) -> bool {
    let mut normalized = service.clone();
    if normalized.version == state.requested_version {
        normalized.version = state.body.service.version.clone();
    }
    normalized == state.body.service
}
pub(super) fn add_existing(
    work: &Submission,
    current: &EntityState,
    body: &AddInstance,
) -> Option<Result<Transition>> {
    if current.phase == Phase::Closing {
        return Some(Ok(Transition::ignored(Disposition::IgnoredPending)));
    }
    if current.phase != Phase::Closed {
        return None;
    }
    Some((|| {
        if !current.local_deleted && body.unit.current.older_than(current.body.unit.current) {
            return Ok(Transition::ignored(Disposition::IgnoredStale));
        }
        if !service_matches(current, &body.service) {
            return Err(Failure::Unsupported(
                "service update choreography is disabled",
            ));
        }
        if body.replica.instance > current.body.replica.instance {
            let mut state = current.clone();
            state.requested_version = body.service.version.clone();
            state.body = Arc::new(create_body(body.clone()));
            state.phase = Phase::Opening;
            state.open_token = work.id;
            state.runtime = None;
            state.close_mode = None;
            state.close_token = 0;
            state.local_deleted = false;
            state.cleanup_pending = false;
            state.fm_stage = FmStage::None;
            state.retry_attempts = 0;
            state.open_failures = 0;
            state.usage = None;
            state.last_retry_error = 0;
            let effects = vec![Effect::Open {
                token: work.id,
                lookup: 0,
                body: state.body.clone(),
                usage: None,
            }];
            return Ok(changed(state, effects, true));
        }

        let mut effects = Vec::new();
        if !current.local_deleted {
            effects.push(Effect::ReplicaUp(Box::new(report(current))));
        }
        if body.replica.instance < current.body.replica.instance {
            let mut reply_body = body.clone();
            reply_body.unit = current.body.unit.clone();
            effects.push(reply(&reply_body, STALE_REQUEST));
        }
        Ok(Transition {
            update: Update::None,
            effects,
            disposition: Disposition::Applied,
        })
    })())
}

/// StartCreateLocalReplica + InternalSetServiceDescription share this
/// normalization for initial creation and recreation, not just removal tests.
pub(super) fn create_body(mut body: AddInstance) -> AddInstance {
    body.unit.previous = Epoch::default();
    body.unit.target = body.service.target;
    body.unit.minimum = body.service.minimum;
    body
}

pub(super) fn transition(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
    config: &NativeConfig,
) -> Result<Transition> {
    let invalid = |_| Failure::Admission("invalid native lifecycle payload");
    match &work.kind {
        Kind::Remove { source, generation } => {
            admit(source, generation, admission)?;
            if generation.number.older_than(&admission.generation) {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let request: RemoveInstance = from_slice(&work.bytes).map_err(invalid)?;
            let body = request.replica.0;
            check_stateless(&body, admission)?;
            if !admission.node_open || !admission.owner_ready {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            if let Some(current) = current {
                if current.phase != Phase::Closed
                    && body.replica.id == current.body.replica.id
                    && body.replica.instance > current.body.replica.instance
                {
                    return Err(Failure::Admission(
                        "native impossible newer replica instance",
                    ));
                }
            }
            let mut state = current.cloned().unwrap_or_else(|| EntityState {
                requested_version: body.service.version.clone(),
                // TryCreateFailoverUnit constructs from ID, consistency and
                // service, NOT the incoming previous epoch/target/minimum.
                body: Arc::new(AddInstance {
                    unit: FailoverUnit {
                        id: body.unit.id.clone(),
                        consistency: body.unit.consistency.clone(),
                        ..FailoverUnit::default()
                    },
                    replica: Replica {
                        node: admission.local_node.clone(),
                        ..Replica::default()
                    },
                    service: body.service.clone(),
                }),
                phase: Phase::Closed,
                open_token: 0,
                retry_attempts: 0,
                open_failures: 0,
                usage: None,
                last_retry_error: 0,
                runtime: None,
                close_mode: None,
                close_token: 0,
                local_deleted: false,
                cleanup_pending: false,
                close_failures: 0,
                registration_failures: 0,
                close_warning: false,
                registration_warning: false,
                fm_stage: FmStage::None,
                report_sequence: 0,
                registration_lookups: BTreeMap::new(),
            });
            let mut durable = current.is_none();
            // UpdateInstanceForClosedReplica executes BEFORE epoch/ID guards.
            if state.phase == Phase::Closed {
                let mut local = (*state.body).clone();
                if local.unit.current.older_than(body.unit.current) {
                    local.unit.current = body.unit.current;
                }
                if body.replica.id > local.replica.id {
                    local.replica.id = body.replica.id;
                    local.replica.instance = body.replica.instance;
                }
                durable |= local != *state.body;
                state.body = Arc::new(local);
            }
            if (!request.deletion.0.force && body.unit.current.older_than(state.body.unit.current))
                || body.replica.id != state.body.replica.id
            {
                return Ok(if durable {
                    changed(state, vec![], true)
                } else {
                    Transition::ignored(Disposition::IgnoredStale)
                });
            }
            let mode = if state.phase == Phase::Closed || !request.deletion.0.force {
                CloseMode::Delete
            } else {
                CloseMode::Obliterate
            };
            let mut effects = Vec::new();
            let before = state.clone();
            let started = start_close(&mut state, mode, work.id, &mut effects);
            durable |= state != before;
            if !started && !durable {
                return Ok(Transition::ignored(Disposition::IgnoredPending));
            }
            Ok(if durable {
                changed(state, effects, true)
            } else {
                Transition {
                    update: Update::None,
                    effects,
                    disposition: Disposition::Applied,
                }
            })
        }
        Kind::Registered => {
            let event: RegistrationCompletion = from_slice(&work.bytes).map_err(invalid)?;
            let Some(current) = current else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            let Some(&(token, close_lookup)) = current.registration_lookups.get(&event.lookup)
            else {
                // Consumed callback replay has no new acquired ownership.
                // Unissued results are not transferred to this executor.
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            let acquired = event.error == 0
                || (event.error == HOSTING_REGISTRATION_VERSION_MISMATCH
                    && !event.registration.host.is_empty());
            if token != event.token
                || (!acquired && event.usage_acquired)
                || (acquired
                    && (event.registration.acquisition != event.lookup
                        || event.registration.host.is_empty()
                        || event.registration.runtime.is_empty()))
            {
                return Err(Failure::Admission(
                    "registration result must match issued acquisition",
                ));
            }
            let mut state = current.clone();
            state.registration_lookups.remove(&event.lookup);
            let valid =
                (!close_lookup && state.phase == Phase::Opening && event.token == state.open_token)
                    || (close_lookup
                        && state.phase == Phase::Closing
                        && state.close_mode != Some(CloseMode::Obliterate)
                        && event.token == state.close_token);
            if !valid {
                // No RAP Open was authorized for an uncommitted registration.
                // Release the acquired lookup reference; never open a closed FT.
                let effects = opening::dispose(&event);
                return Ok(Transition {
                    update: Update::Memory(state),
                    effects,
                    disposition: Disposition::IgnoredStale,
                });
            }
            if state.runtime.is_some() {
                // Even identical host/runtime strings can own a DIFFERENT
                // acquired reference. Dispose the unused acquisition, never the
                // adopted reference. Replayed callbacks were filtered above.
                let effects = opening::dispose(&event);
                return Ok(changed(state, effects, false));
            }
            if state.phase == Phase::Opening {
                return opening::registered(state, event, config);
            }
            let mut effects = Vec::new();
            if opening::registration_success(&event) {
                if event.registration.host.is_empty() || event.registration.runtime.is_empty() {
                    return Err(Failure::Admission(
                        "active registration requires host and runtime IDs",
                    ));
                }
                if event.usage_error != 0 && state.usage.is_none() {
                    if event.usage_acquired {
                        return Err(Failure::Admission("failed usage cannot transfer lease"));
                    }
                    return Ok(changed(
                        state,
                        vec![Effect::ReleaseRegistration(event.registration)],
                        false,
                    ));
                }
                opening::adopt_usage(&mut state, &event, &mut effects)?;
                if config
                    .policy(RetryState::RegistrationAtDrop)
                    .clear(state.registration_failures)
                {
                    effects.push(Effect::Health(
                        HealthEvent::ClearServiceTypeRegistrationWarning,
                    ));
                }
                state.registration_warning = false;
                state.registration_failures = 0;
                state.runtime = Some(event.registration.clone());
                mutate_body(&mut state, |body| {
                    body.service.version = event.version.clone();
                    body.replica.version = event.version;
                });
                close_retry(&state, &mut effects);
                return Ok(changed(state, effects, false));
            }
            effects.extend(opening::dispose(&event));
            // Existing-replica Hosting policy retries every error except deleted
            // application, unlike the new-instance closed allowlist.
            if event.error != APPLICATION_INSTANCE_DELETED {
                let action = config
                    .policy(RetryState::RegistrationAtDrop)
                    .failure(&mut state.registration_failures)?;
                if matches!(action, opening::Action::Warning) {
                    state.registration_warning = true;
                    effects.push(Effect::Health(HealthEvent::ServiceTypeRegistrationWarning));
                } else if matches!(action, opening::Action::Drop) {
                    let mode = state.close_mode.unwrap();
                    finish_close(&mut state, &mut effects);
                    semantic_reply(&state, mode, &mut effects);
                    return Ok(changed(state, effects, true));
                }
                return Ok(changed(state, effects, false));
            }
            let mode = state.close_mode.unwrap();
            finish_close(&mut state, &mut effects);
            semantic_reply(&state, mode, &mut effects);
            Ok(changed(state, effects, true))
        }
        Kind::CompleteClose => {
            let event: CloseCompletion = from_slice(&work.bytes).map_err(invalid)?;
            let Some(current) = current else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            if current.phase != Phase::Closing
                || current.close_mode == Some(CloseMode::Obliterate)
                || !current.body.replica.up
                || event.replica.id != current.body.replica.id
                || event.replica.instance != current.body.replica.instance
                || event.unit.current.older_than(current.body.unit.current)
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            let mut state = current.clone();
            let mode = state.close_mode.unwrap();
            let mut effects = Vec::new();
            if event.error == 0 {
                if config
                    .policy(if state.local_deleted {
                        RetryState::Deleting
                    } else {
                        RetryState::Closing
                    })
                    .clear(state.close_failures)
                {
                    effects.push(Effect::Health(HealthEvent::ClearCloseWarning));
                }
                finish_close(&mut state, &mut effects);
                semantic_reply(&state, mode, &mut effects);
                return Ok(changed(state, effects, true));
            }
            if event.error == APPLICATION_HOST_CRASH {
                if let Some(registration) = state.runtime.take() {
                    effects.push(Effect::ReleaseRegistration(registration));
                }
                state.registration_failures = 0;
                state.registration_warning = false;
            }
            let action = config
                .policy(if state.local_deleted {
                    RetryState::Deleting
                } else {
                    RetryState::Closing
                })
                .failure(&mut state.close_failures)?;
            if matches!(action, opening::Action::Warning) {
                state.close_warning = true;
                effects.push(Effect::Health(HealthEvent::CloseWarning));
            } else if matches!(action, opening::Action::Drop) {
                // Native error helper calls StartClose directly, not the RA
                // CanClose wrapper. RAP Abort is sent by the next close retry.
                start_close_inner(
                    &mut state,
                    CloseMode::ForceAbort,
                    work.id,
                    &mut effects,
                    false,
                );
                semantic_reply(&state, mode, &mut effects);
                return Ok(changed(state, effects, true));
            }
            Ok(changed(state, effects, false))
        }
        Kind::HostDown => {
            let event: HostDown = from_slice(&work.bytes).map_err(invalid)?;
            let Some(current) = current else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            if current.phase == Phase::Closed
                || !current
                    .runtime
                    .as_ref()
                    .is_some_and(|r| r.host == event.host)
            {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            if current.phase == Phase::Opening {
                return opening::runtime_failure(
                    current.clone(),
                    APPLICATION_HOST_CRASH,
                    ProxyErrorDetail::default(),
                    config,
                );
            }
            let mut state = current.clone();
            let mut effects = Vec::new();
            start_close(&mut state, CloseMode::AppHostDown, work.id, &mut effects);
            Ok(changed(state, effects, true))
        }
        Kind::RetryClose => {
            if !admission.owner_ready {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            let Some(state) = current.filter(|s| s.phase == Phase::Closing) else {
                return Ok(Transition::ignored(Disposition::IgnoredPending));
            };
            let mut effects = Vec::new();
            close_retry(state, &mut effects);
            Ok(Transition {
                update: Update::None,
                effects,
                disposition: Disposition::Applied,
            })
        }
        Kind::RetryReport => {
            if !admission.owner_ready {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            let Some(current) = current.filter(|s| s.fm_stage != FmStage::None && !s.local_deleted)
            else {
                return Ok(Transition::ignored(Disposition::IgnoredPending));
            };
            let mut state = current.clone();
            state.report_sequence = state
                .report_sequence
                .checked_add(1)
                .ok_or(Failure::Capacity)?;
            let effects = vec![Effect::ReplicaUp(Box::new(report(&state)))];
            Ok(changed(state, effects, false))
        }
        Kind::ReplicaUpReply { source, generation } => {
            admit(source, generation, admission)?;
            if generation.number.older_than(&admission.generation) {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            if !admission.owner_ready {
                return Ok(Transition::ignored(Disposition::IgnoredNotReady));
            }
            let body: ReplicaUp = from_slice(&work.bytes).map_err(invalid)?;
            let dropped = !body.dropped.0.is_empty();
            let info = body
                .replicas
                .0
                .first()
                .or_else(|| body.dropped.0.first())
                .unwrap();
            let Some(current) = current else {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            };
            if info.replicas.0.len() != 1 {
                return Err(Failure::Unsupported("one local replica per FT is enabled"));
            }
            let replica = &info.replicas.0[0].description;
            if replica.id != current.body.replica.id {
                return Ok(Transition::ignored(Disposition::IgnoredStale));
            }
            if info.service != current.body.service {
                return Err(Failure::Unsupported(
                    "ack service-description update is disabled",
                ));
            }
            let mut state = current.clone();
            if (state.fm_stage == FmStage::ReplicaDropped && replica.state == 3)
                || (state.fm_stage == FmStage::ReplicaDown
                    && !replica.up
                    && replica.instance == state.body.replica.instance)
            {
                state.fm_stage = FmStage::None;
            }
            let mut effects = Vec::new();
            let before = state.clone();
            let started = if dropped {
                start_close(&mut state, CloseMode::ForceDelete, work.id, &mut effects)
            } else if state.phase == Phase::Closed && replica.state == 3 {
                start_close(&mut state, CloseMode::Delete, work.id, &mut effects)
            } else {
                false
            };
            let durable = started && state != before;
            Ok(changed(state, effects, durable))
        }
        _ => unreachable!("non-lifecycle command"),
    }
}
