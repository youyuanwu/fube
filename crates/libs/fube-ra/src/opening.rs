//! Native stateless creation policy. Source decisions and evidence classes:
//! SpecResearch.md and docs/cluster/RaStatelessOpening.md.
use super::*;

const fn hr(value: u32) -> i64 {
    value as i32 as i64
}
pub const HOSTING_SERVICE_TYPE_NOT_REGISTERED: i64 = hr(0x80071bd2);
pub const HOSTING_DEPLOYMENT_IN_PROGRESS: i64 = hr(0x80071d34);
pub const HOSTING_ACTIVATION_IN_PROGRESS: i64 = hr(0x80071d35);
pub const HOSTING_APPLICATION_VERSION_MISMATCH: i64 = hr(0x80071d2c);
pub const HOSTING_PACKAGE_VERSION_MISMATCH: i64 = hr(0x80071d2b);
pub const HOSTING_REGISTRATION_VERSION_MISMATCH: i64 = hr(0x80071d2a);
pub const HOSTING_RUNTIME_NOT_REGISTERED: i64 = hr(0x80071d30);
pub const HOSTING_NOT_FOUND: i64 = hr(0x80071cf0);
pub const HOSTING_SERVICE_TYPE_DISABLED: i64 = hr(0x80071d33);
pub const APPLICATION_INSTANCE_DELETED: i64 = hr(0x80071cca);
pub const PROXY_STATE_CHANGED_ON_DATA_LOSS: i64 = hr(0x80071d28);
pub const ABORT: i64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub warning: i32,
    pub drop: i32,
    pub restart: i32,
    pub error: i32,
}
impl RetryPolicy {
    pub const fn registration() -> Self {
        Self {
            warning: i32::MAX,
            drop: i32::MAX,
            restart: i32::MAX,
            error: i32::MAX,
        }
    }
    pub const fn open() -> Self {
        Self {
            warning: 10,
            drop: 40,
            ..Self::registration()
        }
    }
    pub(super) fn read(mut get: impl FnMut(Threshold) -> i32) -> Self {
        Self {
            warning: get(Threshold::Warning),
            drop: get(Threshold::Drop),
            restart: get(Threshold::Restart),
            error: get(Threshold::Error),
        }
    }
    pub(super) fn failure(self, count: &mut u64) -> Result<Action> {
        *count = count.checked_add(1).ok_or(Failure::Capacity)?;
        if *count > i32::MAX as u64 {
            return Err(Failure::Capacity);
        }
        Ok(if i128::from(*count) == i128::from(self.warning) {
            Action::Warning
        } else if i128::from(*count) == i128::from(self.drop) {
            Action::Drop
        } else {
            Action::None
        }) // Restart/error actions are not consumed by these native handlers.
    }
    pub(super) fn clear(self, count: u64) -> bool {
        i128::from(count) >= i128::from(self.warning) || i128::from(count) >= i128::from(self.error)
    }
    pub(super) fn flags(self, count: u64) -> i64 {
        if i128::from(count) == i128::from(self.drop) - 1 {
            ABORT
        } else {
            0
        }
    }
}
pub(super) enum Action {
    None,
    Warning,
    Drop,
}

/// Trusted ProxyErrorCode detail, not a newly enabled proxy wire codec.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiNameDescription {
    pub api: i64,
    pub interface: i64,
    pub metadata: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyErrorDetail {
    pub message: String,
    pub api: ApiNameDescription,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpeningHealthReport {
    pub event: HealthEvent,
    pub error: i64,
    pub detail: ProxyErrorDetail,
}
fn health(event: HealthEvent, error: i64, detail: ProxyErrorDetail) -> Effect {
    Effect::OpeningHealth(Box::new(OpeningHealthReport {
        event,
        error,
        detail,
    }))
}
pub fn registration_retryable(error: i64) -> bool {
    // Hosting.FindServiceTypeRegistrationErrorList.cpp:35-65: closed allowlist.
    matches!(
        error,
        HOSTING_DEPLOYMENT_IN_PROGRESS
            | HOSTING_ACTIVATION_IN_PROGRESS
            | HOSTING_SERVICE_TYPE_NOT_REGISTERED
            | HOSTING_NOT_FOUND
            | HOSTING_APPLICATION_VERSION_MISMATCH
            | HOSTING_PACKAGE_VERSION_MISMATCH
            | HOSTING_REGISTRATION_VERSION_MISMATCH
            | HOSTING_RUNTIME_NOT_REGISTERED
    )
}
pub(super) fn dispose(event: &RegistrationCompletion) -> Vec<Effect> {
    let mut effects = Vec::new();
    if event.error == 0
        || (event.error == HOSTING_REGISTRATION_VERSION_MISMATCH
            && !event.registration.host.is_empty())
    {
        effects.push(Effect::ReleaseRegistration(event.registration.clone()));
        if event.usage_acquired {
            effects.push(Effect::ReleaseUsage {
                acquisition: event.lookup,
            });
        }
    }
    effects
}
pub(super) fn adopt_usage(
    state: &mut EntityState,
    event: &RegistrationCompletion,
    effects: &mut Vec<Effect>,
) -> Result<()> {
    match (state.usage, event.usage_acquired) {
        (None, true) => state.usage = Some(event.lookup),
        (Some(_), false) => {}
        (Some(_), true) => effects.push(Effect::ReleaseUsage {
            acquisition: event.lookup,
        }),
        _ => {
            return Err(Failure::Admission(
                "registration must transfer required usage or retain existing lease",
            ))
        }
    }
    Ok(())
}
pub(super) fn release_usage(state: &mut EntityState, effects: &mut Vec<Effect>) {
    if let Some(acquisition) = state.usage.take() {
        effects.push(Effect::ReleaseUsage { acquisition });
    }
}
fn fail(state: &mut EntityState, error: i64, effects: &mut Vec<Effect>) {
    // FailoverUnit.cpp:5212-5235,5610-5657: capture reply before cleanup.
    let captured = state.body.clone();
    state.phase = Phase::Closed;
    state.open_failures = 0;
    state.registration_failures = 0;
    state.registration_warning = false;
    state.last_retry_error = 0;
    state.close_mode = None;
    state.fm_stage = FmStage::None;
    state.cleanup_pending = state.local_deleted;
    let replica = lifecycle::canonical(state, true);
    lifecycle::mutate_body(state, |body| body.replica = replica);
    if let Some(registration) = state.runtime.take() {
        effects.push(Effect::ReleaseRegistration(registration));
    }
    release_usage(state, effects);
    effects.push(Effect::Health(HealthEvent::Close));
    effects.push(reply(&captured, error));
}
pub(super) fn registration_success(event: &RegistrationCompletion) -> bool {
    event.error == 0
        || (event.error == HOSTING_REGISTRATION_VERSION_MISMATCH
            && !event.registration.host.is_empty()
            && (!event.upgrade_present || event.required_upgrade <= event.version.instance))
}
pub(super) fn registered(
    mut state: EntityState,
    event: RegistrationCompletion,
    config: &NativeConfig,
) -> Result<Transition> {
    let mut effects = Vec::new();
    // ReconfigurationAgent.cpp:986-1033: use returned version when no upgrade
    // entry exists or its instance is no greater than the returned candidate.
    let success = registration_success(&event);
    if success {
        if event.usage_error != 0 && state.usage.is_none() {
            if event.usage_acquired {
                return Err(Failure::Admission(
                    "failed usage increment cannot transfer usage",
                ));
            }
            effects.push(Effect::ReleaseRegistration(event.registration));
            return Ok(lifecycle::changed(state, effects, false));
        }
        adopt_usage(&mut state, &event, &mut effects)?;
        if config
            .policy(RetryState::Registration)
            .clear(state.registration_failures)
        {
            effects.push(health(
                HealthEvent::ClearServiceTypeRegistrationWarning,
                0,
                ProxyErrorDetail::default(),
            ));
        }
        state.registration_failures = 0;
        state.registration_warning = false;
        state.last_retry_error = 0;
        lifecycle::mutate_body(&mut state, |body| {
            body.service.version = event.version.clone();
            body.replica.version = event.version;
        });
        state.runtime = Some(event.registration.clone());
        effects.push(Effect::RuntimeOpen {
            token: state.open_token,
            body: state.body.clone(),
            registration: event.registration,
            flags: config
                .policy(RetryState::Opening)
                .flags(state.open_failures),
        });
        return Ok(lifecycle::changed(state, effects, false));
    }
    effects.extend(dispose(&event));
    state.last_retry_error = event.error;
    if registration_retryable(event.error) {
        match config
            .policy(RetryState::Registration)
            .failure(&mut state.registration_failures)?
        {
            Action::Warning => {
                state.registration_warning = true;
                effects.push(health(
                    HealthEvent::ServiceTypeRegistrationWarning,
                    event.error,
                    ProxyErrorDetail {
                        message: event.error_message,
                        ..ProxyErrorDetail::default()
                    },
                ));
            }
            Action::Drop => fail(&mut state, event.error, &mut effects),
            Action::None => {}
        }
    } else {
        fail(&mut state, event.error, &mut effects);
    }
    // Native registration retry job selects memory update even for terminal FSR.
    Ok(lifecycle::changed(state, effects, false))
}
pub(super) fn runtime_failure(
    mut state: EntityState,
    error: i64,
    detail: ProxyErrorDetail,
    config: &NativeConfig,
) -> Result<Transition> {
    state.last_retry_error = error;
    let mut effects = Vec::new();
    match config
        .policy(RetryState::Opening)
        .failure(&mut state.open_failures)?
    {
        Action::Drop => fail(&mut state, error, &mut effects),
        Action::Warning => effects.push(health(HealthEvent::OpenWarning, error, detail)),
        Action::None => {}
    }
    if error == APPLICATION_HOST_CRASH && state.phase == Phase::Opening {
        effects.push(Effect::ReleaseRegistration(
            state.runtime.take().expect("matched registration"),
        ));
        state.registration_failures = 0;
        state.registration_warning = false;
    }
    Ok(lifecycle::changed(state, effects, true))
}
pub(super) fn complete(
    work: &Submission,
    current: Option<&EntityState>,
    config: &NativeConfig,
) -> Result<Transition> {
    let event: OpenCompletion =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid Open completion"))?;
    let Some(current) = current else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if current.phase != Phase::Opening
        || !current.body.replica.up
        || event.unit.current.older_than(current.body.unit.current)
        || event.replica.instance != current.body.replica.instance
        || event.replica.state == 0
    // native StandBy, not pre-Ready normalization
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if current.runtime.is_none()
        || (config
            .policy(RetryState::Opening)
            .flags(current.open_failures)
            == ABORT
            && event.flags != ABORT)
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if event.error != 0 && event.error != PROXY_STATE_CHANGED_ON_DATA_LOSS {
        return runtime_failure(current.clone(), event.error, event.detail, config);
    }
    let mut state = current.clone();
    let mut effects = Vec::new();
    // Native applies Ready and endpoints before the separate current-policy clear.
    state.phase = Phase::Ready;
    lifecycle::mutate_body(&mut state, |body| {
        body.replica.state = 2;
        body.replica.service_endpoint = event.replica.service_endpoint;
        body.replica.replication_endpoint = event.replica.replication_endpoint;
    });
    if config
        .policy(RetryState::Opening)
        .clear(state.open_failures)
    {
        effects.push(health(
            HealthEvent::ClearOpenWarning,
            0,
            ProxyErrorDetail::default(),
        ));
    }
    state.open_failures = 0;
    state.last_retry_error = 0;
    effects.push(reply(&state.body, 0));
    Ok(lifecycle::changed(state, effects, true))
}
