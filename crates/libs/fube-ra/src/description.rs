//! Direct Add-carried service-description helpers. Native authority:
//! FailoverUnit.cpp:5660–5699,5862–5973; ReconfigurationAgent.cpp:1041–1060,
//! 1614–1627,1660–1668 at 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d.
use super::*;

/// Raw trusted RAP result, not a wire-enabled IPC body or provenance witness.
/// Carries the native full description; only its update version is consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDescriptionCompletion {
    pub error: i64,
    pub unit: FailoverUnit,
    pub replica: Replica,
    pub service: Service,
}

fn send(state: &EntityState) -> Option<Effect> {
    // Actual AddActionSendMessageToRAProxy consumer (RA.cpp:3582–3614)
    // suppresses send for null registration; pending survives opening host loss.
    state
        .runtime
        .clone()
        .map(|registration| Effect::UpdateServiceDescription {
            body: state.body.clone(),
            registration,
        })
}

/// Failed helper eligibility is a no-op, not rejection of the enclosing Add.
pub(super) fn update(
    state: &mut EntityState,
    incoming: &Service,
    effects: &mut Vec<Effect>,
) -> bool {
    let local = &state.body.service;
    if incoming.name != local.name
        || incoming.instance != local.instance
        || incoming.update_version <= local.update_version
    {
        return false;
    }
    let mut service = incoming.clone();
    if state.phase != Phase::Closed {
        service.version = state.body.replica.version.clone();
    }
    lifecycle::mutate_body(state, |body| {
        body.unit.target = service.target;
        body.unit.minimum = service.minimum;
        body.service = service;
    });
    if matches!(state.phase, Phase::Ready | Phase::Opening)
        && state.body.replica.up
        && (state.phase != Phase::Opening || state.runtime.is_some())
    {
        state.service_description_pending = true;
        effects.extend(send(state));
    }
    true
}

pub(super) fn transition(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
) -> Result<Transition> {
    if matches!(work.kind, Kind::RetryServiceDescription) {
        if !admission.owner_ready {
            return Ok(Transition::ignored(Disposition::IgnoredNotReady));
        }
        return Ok(match current.filter(|s| s.service_description_pending) {
            Some(state) => Transition {
                update: Update::None,
                effects: send(state).into_iter().collect(),
                disposition: Disposition::Applied,
            },
            None => Transition::ignored(Disposition::IgnoredPending),
        });
    }
    let event: ServiceDescriptionCompletion = from_slice(&work.bytes)
        .map_err(|_| Failure::Admission("invalid service-description completion"))?;
    let Some(state) = current else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    // Generic FT IPC admission excludes pending Open/Close and Closed/down.
    // Consumer adds pending/success/exact identity/nonlower update version.
    // Native higher-version TESTASSERT is diagnostic, NOT equality admission.
    if state.phase != Phase::Ready
        || !state.body.replica.up
        || event.unit.current.older_than(state.body.unit.current)
        || !state.service_description_pending
        || event.error != 0
        || event.replica.id != state.body.replica.id
        || event.replica.instance != state.body.replica.instance
        || event.service.update_version < state.body.service.update_version
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut state = state.clone();
    state.service_description_pending = false;
    Ok(lifecycle::changed(state, vec![], false))
}
