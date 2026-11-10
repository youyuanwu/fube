use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerCreation {
    pub source: NodeInstance,
    pub deactivation: ReplicaDeactivationInfo,
    /// Terminal registration failure before any adopted runtime/usage.
    pub registration_error: Option<i64>,
    pub open_error: Option<i64>,
    pub host_down: bool,
}

impl EntityState {
    pub fn peer_host_loss(&self) -> bool {
        self.peer_creation
            .as_ref()
            .is_some_and(|peer| peer.host_down)
    }

    pub fn failed_open_error(&self) -> Option<i64> {
        if self.phase != Phase::Closed
            || self.open_token == 0
            || self.close_token != 0
            || self.runtime.is_some()
            || self.usage.is_some()
        {
            return None;
        }
        self.peer_creation
            .as_ref()?
            .open_error
            .filter(|error| *error != 0)
    }

    pub fn failed_registration_error(&self) -> Option<i64> {
        if self.phase != Phase::Closed
            || self.open_token == 0
            || self.close_token != 0
            || self.runtime.is_some()
            || self.usage.is_some()
        {
            return None;
        }
        self.peer_creation
            .as_ref()?
            .registration_error
            .filter(|error| *error != 0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteIdle {
    pub target: NodeInstance,
    pub body: Arc<CreateReplica>,
    pub reply: Option<CreateReplicaReply>,
    pub build: Option<RemoteBuild>,
    pub removal: Option<RemoteRemoval>,
    pub fm_request: Option<Arc<AddReplica>>,
    pub ready: bool,
}

impl RemoteIdle {
    pub fn creation_failed(&self) -> bool {
        self.reply.as_ref().is_some_and(|reply| reply.error != 0)
    }

    pub fn remove_reply(&self) -> Option<RemoveReplicaReply> {
        let removal = self.removal.as_ref()?;
        removal.fm_request.as_ref()?;
        (removal.result == Some(0)).then(|| RemoveReplicaReply {
            unit: removal.body.unit.clone(),
            replica: removal.replica.clone(),
            error: 0,
        })
    }

    pub fn add_reply(&self) -> Option<AddReplicaReply> {
        self.fm_request.as_ref()?;
        if self.removal.is_some() {
            return None;
        }
        let mut reply = self.reply.clone()?;
        if reply.error == 0 {
            if !self.ready {
                return None;
            }
            reply.replica.state = 2;
        }
        Some(reply)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteBuild {
    pub body: Arc<AddInstance>,
    /// None is pending, not proof of failure or permission to replace a peer.
    pub result: Option<i64>,
    pub detail: ProxyErrorDetail,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteRemoval {
    pub body: Arc<AddInstance>,
    pub replica: Replica,
    pub fm_request: Option<Arc<RemoveReplica>>,
    pub result: Option<i64>,
    pub detail: ProxyErrorDetail,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildCompletion {
    pub unit: FailoverUnit,
    pub local: Replica,
    pub remote: Replica,
    pub error: i64,
    pub detail: ProxyErrorDetail,
}

pub type RemovalCompletion = BuildCompletion;

impl Executor {
    /// One original configured peer, bound before any local FT is accepted.
    pub fn bind_peer(&self, source: NodeInstance) -> Result<()> {
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        if view.peer.is_some()
            || !view.entities.is_empty()
            || view.generations_touched
            || source.id == self.shared.local_node.id
            || source.id == self.shared.fm_source.id
            || source.instance == 0
        {
            return Err(Failure::Unsupported(
                "peer requires a fresh original configured node",
            ));
        }
        view.peer = Some(source);
        Ok(())
    }

    pub fn peer(&self) -> Option<NodeInstance> {
        lock(&self.shared.view).peer.clone()
    }
}

pub(super) fn admit(
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<()> {
    if peer != Some(source) || source == &admission.local_node {
        return Err(Failure::Admission(
            "message requires the exact original peer",
        ));
    }
    if generation.for_fm_replica {
        return Err(Failure::Unsupported("FM-service peer creation is gated"));
    }
    Ok(())
}

pub(super) fn creation_reply(state: &EntityState, body: &AddInstance, error: i64) -> Effect {
    if let Some(peer) = &state.peer_creation {
        Effect::PeerCreateReplicaReply {
            target: peer.source.clone(),
            body: Box::new(CreateReplicaReply {
                unit: body.unit.clone(),
                replica: body.replica.clone(),
                error,
            }),
        }
    } else {
        reply(body, error, state.fm_primary)
    }
}

pub(super) fn start_remote(
    work: &Submission,
    current: Option<&EntityState>,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let target: Replica = from_slice(&work.bytes)
        .map_err(|_| Failure::Admission("invalid remote Idle descriptor"))?;
    if current
        .and_then(|state| state.remote_idle.as_ref())
        .is_some_and(|remote| remote.fm_request.is_some())
    {
        return Err(Failure::Unsupported("FM owns this remote Idle"));
    }
    create_remote(target, current, admission, peer)
}

fn create_remote(
    target: Replica,
    current: Option<&EntityState>,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let current = current.ok_or(Failure::Admission(
        "remote creation needs an existing Primary",
    ))?;
    if !admission.node_open || !admission.owner_ready || !admission.node_activated {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    let deactivation = current.primary_deactivation.ok_or(Failure::Unsupported(
        "remote creation requires initial Primary deactivation",
    ))?;
    if current.phase != Phase::Ready
        || !current.fm_primary
        || current.peer_creation.is_some()
        || current.body.replica.current_role != 4
        || current.body.replica.state != 2
        || current.runtime.is_none()
        || current.body.replica.replication_endpoint.is_empty()
        || peer != Some(&target.node)
        || target.node.id == admission.local_node.id
        || target.id == current.body.replica.id
        || target.id < 0
        || target.instance < 0
        || target.current_role != 2
        || target.previous_role != 1
        || target.state != 1
        || !target.up
        || target.version != current.body.service.version
        || !target.service_endpoint.is_empty()
        || !target.replication_endpoint.is_empty()
        || deactivation != (current.body.unit.current, 0)
    {
        return Err(Failure::Unsupported(
            "not the admitted fresh remote Idle profile",
        ));
    }
    let body = CreateReplica::new(
        AddInstance {
            unit: current.body.unit.clone(),
            replica: target.clone(),
            service: current.body.service.clone(),
        },
        ReplicaDeactivationInfo {
            epoch: deactivation.0,
            catchup_lsn: deactivation.1,
        },
    );
    if let Some(previous) = &current.remote_idle {
        if previous.body.as_ref() != &body || previous.target != target.node {
            return Err(Failure::Unsupported("remote Idle replacement is gated"));
        }
        if previous.reply.is_some() || previous.removal.is_some() {
            return Ok(Transition::ignored(Disposition::IgnoredStale));
        }
    }
    let mut state = current.clone();
    state.remote_idle = Some(RemoteIdle {
        target: target.node.clone(),
        body: Arc::new(body.clone()),
        reply: None,
        build: None,
        removal: None,
        fm_request: None,
        ready: false,
    });
    Ok(lifecycle::changed(
        state,
        vec![Effect::PeerCreateReplica {
            target: target.node,
            body: Box::new(body),
        }],
        false,
    ))
}

pub(super) fn add_from_fm(
    work: &Submission,
    current: Option<&EntityState>,
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    if source != &admission.fm_source {
        return Err(Failure::Admission("configured FM session mismatch"));
    }
    if generation.for_fm_replica {
        return Err(Failure::Unsupported("FM-service replica build is gated"));
    }
    if generation.number.older_than(&admission.generation) {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if generation.number != admission.generation {
        return Err(Failure::Unsupported(
            "peer build requires the original fixed generation",
        ));
    }
    let body: AddReplica =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid AddReplica body"))?;
    let Some(current) = current.filter(|state| state.phase == Phase::Ready && state.fm_primary)
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if !admission.node_open || !admission.owner_ready || !admission.node_activated {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    if body.unit.current.older_than(current.body.unit.current) {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if body.unit != current.body.unit || body.service != current.body.service {
        return Err(Failure::Unsupported(
            "AddReplica requires the original FT and service",
        ));
    }
    if let Some(remote) = &current.remote_idle {
        if body.replica.node == remote.target
            && body.replica.instance < remote.body.replica.0.replica.instance
        {
            return Ok(Transition::ignored(Disposition::IgnoredStale));
        }
        if remote.fm_request.as_deref() != Some(&body) {
            return Err(Failure::Unsupported(
                "remote replacement or manual-build adoption is gated",
            ));
        }
        if remote.removal.is_some() {
            return Ok(Transition::ignored(Disposition::IgnoredStale));
        }
        if let Some(reply) = remote.add_reply() {
            return Ok(lifecycle::changed(
                current.clone(),
                vec![Effect::AddReplicaReply(reply)],
                false,
            ));
        }
        if remote.reply.is_some() {
            return start_build(Some(current), admission, peer);
        }
        return Ok(lifecycle::changed(
            current.clone(),
            vec![Effect::PeerCreateReplica {
                target: remote.target.clone(),
                body: Box::new((*remote.body).clone()),
            }],
            false,
        ));
    }
    let mut transition = create_remote(body.replica.clone(), Some(current), admission, peer)?;
    if let Update::Memory(state) = &mut transition.update {
        state
            .remote_idle
            .as_mut()
            .expect("created remote")
            .fm_request = Some(Arc::new(body));
    }
    Ok(transition)
}

pub(super) fn complete_remote(
    work: &Submission,
    current: Option<&EntityState>,
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    admit(source, generation, admission, peer)?;
    if generation.number.older_than(&admission.generation) {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let body: CreateReplicaReply = from_slice(&work.bytes)
        .map_err(|_| Failure::Admission("invalid peer CreateReplica reply"))?;
    let Some(current) = current.filter(|state| state.phase == Phase::Ready && state.fm_primary)
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    let Some(pending) = &current.remote_idle else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    let expected = &pending.body.replica.0;
    if pending.target != *source
        || current.body.unit != expected.unit
        || body.unit != expected.unit
        || body.replica.node != expected.replica.node
        || body.replica.id != expected.replica.id
        || body.replica.instance != expected.replica.instance
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if pending.removal.is_some() {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if let Some(previous) = &pending.reply {
        if previous != &body || pending.fm_request.is_none() {
            return Ok(Transition::ignored(Disposition::IgnoredStale));
        }
        if let Some(reply) = pending.add_reply() {
            return Ok(lifecycle::changed(
                current.clone(),
                vec![Effect::AddReplicaReply(reply)],
                false,
            ));
        }
        return start_build(Some(current), admission, peer);
    }
    if body.error == 0
        && (body.replica.current_role != 2
            || body.replica.previous_role != 1
            || body.replica.state != 1
            || !body.replica.up
            || body.replica.version.version != expected.replica.version.version
            || !body.replica.service_endpoint.is_empty()
            || body.replica.replication_endpoint.is_empty())
    {
        return Err(Failure::Admission(
            "peer reply is not an opened InBuild Idle",
        ));
    }
    let mut state = current.clone();
    let succeeded = body.error == 0;
    state
        .remote_idle
        .as_mut()
        .expect("checked remote owner")
        .reply = Some(body);
    if state
        .remote_idle
        .as_ref()
        .expect("checked remote")
        .fm_request
        .is_some()
    {
        if succeeded {
            return start_build(Some(&state), admission, peer);
        }
        let reply = state
            .remote_idle
            .as_ref()
            .and_then(RemoteIdle::add_reply)
            .expect("failed Open reply");
        return Ok(lifecycle::changed(
            state,
            vec![Effect::AddReplicaReply(reply)],
            false,
        ));
    }
    Ok(lifecycle::changed(state, vec![], false))
}

pub(super) fn start_build(
    current: Option<&EntityState>,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let current = current.ok_or(Failure::Admission("build requires an existing Primary"))?;
    if !admission.node_open || !admission.owner_ready || !admission.node_activated {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    let remote = current
        .remote_idle
        .as_ref()
        .ok_or(Failure::Unsupported("no opened peer Idle"))?;
    if remote.removal.is_some() {
        return Err(Failure::Unsupported("peer Idle removal is terminal"));
    }
    let reply = remote
        .reply
        .as_ref()
        .filter(|reply| reply.error == 0)
        .ok_or(Failure::Unsupported("peer Open has not succeeded"))?;
    let registration = current
        .runtime
        .as_ref()
        .ok_or(Failure::Unsupported("no original runtime"))?;
    if current.phase != Phase::Ready
        || !current.fm_primary
        || current.body.replica.current_role != 4
        || current.peer_creation.is_some()
        || peer != Some(&remote.target)
    {
        return Err(Failure::Unsupported(
            "build requires the original current Primary and peer",
        ));
    }
    if remote
        .build
        .as_ref()
        .is_some_and(|build| build.result == Some(0))
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut state = current.clone();
    let body = remote
        .build
        .as_ref()
        .map_or_else(|| current.body.clone(), |build| build.body.clone());
    state.remote_idle.as_mut().expect("checked remote").build = Some(RemoteBuild {
        body: body.clone(),
        result: None,
        detail: ProxyErrorDetail::default(),
    });
    Ok(lifecycle::changed(
        state,
        vec![Effect::BuildIdle {
            body,
            replica: Box::new(reply.replica.clone()),
            registration: registration.clone(),
        }],
        false,
    ))
}

pub(super) fn complete_build(
    work: &Submission,
    current: Option<&EntityState>,
) -> Result<Transition> {
    let event: BuildCompletion =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid build completion"))?;
    let Some(current) = current.filter(|state| state.phase == Phase::Ready && state.fm_primary)
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    let Some(remote) = &current.remote_idle else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    let Some(build) = &remote.build else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if remote.removal.is_some()
        || build.result == Some(0)
        || !matches_local(&build.body, &event)
        || remote
            .reply
            .as_ref()
            .is_none_or(|reply| reply.error != 0 || reply.replica != event.remote)
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut state = current.clone();
    let build = state
        .remote_idle
        .as_mut()
        .expect("checked remote")
        .build
        .as_mut()
        .expect("checked build");
    build.result = Some(event.error);
    build.detail = event.detail;
    let remote = state.remote_idle.as_mut().expect("checked remote");
    if event.error == 0 && remote.fm_request.is_some() {
        remote.ready = true;
    }
    let effects = remote
        .add_reply()
        .map(Effect::AddReplicaReply)
        .into_iter()
        .collect();
    Ok(lifecycle::changed(state, effects, false))
}

pub(super) fn start_removal(
    current: Option<&EntityState>,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    let current = current.ok_or(Failure::Admission("removal requires an existing Primary"))?;
    if !admission.node_open || !admission.owner_ready || !admission.node_activated {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    let remote = current
        .remote_idle
        .as_ref()
        .ok_or(Failure::Unsupported("no opened peer Idle"))?;
    let reply = remote
        .reply
        .as_ref()
        .filter(|reply| reply.error == 0)
        .ok_or(Failure::Unsupported("peer Open has not succeeded"))?;
    let registration = current
        .runtime
        .as_ref()
        .ok_or(Failure::Unsupported("no original runtime"))?;
    if current.phase != Phase::Ready
        || !current.fm_primary
        || current.body.replica.current_role != 4
        || current.peer_creation.is_some()
        || peer != Some(&remote.target)
    {
        return Err(Failure::Unsupported(
            "removal requires the original current Primary and peer",
        ));
    }
    if remote
        .removal
        .as_ref()
        .is_some_and(|removal| removal.result == Some(0))
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut removal = remote.removal.clone().unwrap_or_else(|| {
        let mut replica = reply.replica.clone();
        // Native RA InDrop maps to Dropped on the RAP wire, not remote deletion.
        replica.state = 3;
        RemoteRemoval {
            body: current.body.clone(),
            replica,
            fm_request: None,
            result: None,
            detail: ProxyErrorDetail::default(),
        }
    });
    removal.result = None;
    let effect = Effect::RemoveIdle {
        body: removal.body.clone(),
        replica: Box::new(removal.replica.clone()),
        registration: registration.clone(),
    };
    let mut state = current.clone();
    let remote = state.remote_idle.as_mut().expect("checked remote");
    remote.ready = false;
    remote.removal = Some(removal);
    Ok(lifecycle::changed(state, vec![effect], false))
}

pub(super) fn remove_from_fm(
    work: &Submission,
    current: Option<&EntityState>,
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
    peer: Option<&NodeInstance>,
) -> Result<Transition> {
    if source != &admission.fm_source {
        return Err(Failure::Admission("configured FM session mismatch"));
    }
    if generation.for_fm_replica {
        return Err(Failure::Unsupported("FM-service replica removal is gated"));
    }
    if generation.number.older_than(&admission.generation) {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    if generation.number != admission.generation {
        return Err(Failure::Unsupported(
            "peer removal requires the original fixed generation",
        ));
    }
    let body: RemoveReplica =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid RemoveReplica body"))?;
    let Some(current) = current.filter(|state| state.phase == Phase::Ready && state.fm_primary)
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if !admission.node_open || !admission.owner_ready || !admission.node_activated {
        return Ok(Transition::ignored(Disposition::IgnoredNotReady));
    }
    if body.unit.current.older_than(current.body.unit.current) {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let remote = current
        .remote_idle
        .as_ref()
        .ok_or(Failure::Unsupported("unknown-Idle removal is gated"))?;
    if current.body.replica.current_role != 4
        || current.body.replica.state != 2
        || current.runtime.is_none()
        || current.peer_creation.is_some()
        || peer != Some(&remote.target)
    {
        return Err(Failure::Unsupported(
            "removal requires the original current Primary and peer",
        ));
    }
    let local_only = remote.reply.is_none() || remote.creation_failed();
    let expected = &remote.body.replica.0.replica;
    if body.replica.node == remote.target && body.replica.instance < expected.instance {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut replica = body.replica.clone();
    // FM may still have InBuild/empty endpoint after losing AddReplicaReply.
    replica.state = expected.state;
    replica.replication_endpoint = expected.replication_endpoint.clone();
    if body.unit != current.body.unit
        || body.service != current.body.service
        || replica != *expected
        || !matches!(body.replica.state, 1 | 2)
        || (!body.replica.replication_endpoint.is_empty()
            && remote.reply.as_ref().is_none_or(|opened| {
                body.replica.replication_endpoint != opened.replica.replication_endpoint
            }))
    {
        return Err(Failure::Unsupported(
            "RemoveReplica requires the original Idle and FT",
        ));
    }
    if local_only && remote.fm_request.as_deref() != Some(&body) {
        return Err(Failure::Unsupported(
            "creation cleanup requires the original FM request",
        ));
    }
    if let Some(removal) = &remote.removal {
        if removal.fm_request.as_deref() != Some(&body) {
            return Err(Failure::Unsupported(
                "replacement or manual-removal adoption is gated",
            ));
        }
        if let Some(reply) = remote.remove_reply() {
            return Ok(lifecycle::changed(
                current.clone(),
                vec![Effect::RemoveReplicaReply(reply)],
                false,
            ));
        }
    }
    if local_only {
        let mut state = current.clone();
        let remote = state.remote_idle.as_mut().expect("checked remote");
        remote.ready = false;
        // Native InCreate/absent-after-abort cleanup echoes FM without InDrop or RAP.
        remote.removal = Some(RemoteRemoval {
            body: current.body.clone(),
            replica: body.replica.clone(),
            fm_request: Some(Arc::new(body)),
            result: Some(0),
            detail: ProxyErrorDetail::default(),
        });
        let reply = remote
            .remove_reply()
            .expect("completed local creation cleanup");
        return Ok(lifecycle::changed(
            state,
            vec![Effect::RemoveReplicaReply(reply)],
            false,
        ));
    }
    let mut transition = start_removal(Some(current), admission, peer)?;
    if let Update::Memory(state) = &mut transition.update {
        state
            .remote_idle
            .as_mut()
            .expect("checked remote")
            .removal
            .as_mut()
            .expect("started removal")
            .fm_request = Some(Arc::new(body));
    }
    Ok(transition)
}

pub(super) fn complete_removal(
    work: &Submission,
    current: Option<&EntityState>,
) -> Result<Transition> {
    let event: RemovalCompletion =
        from_slice(&work.bytes).map_err(|_| Failure::Admission("invalid removal completion"))?;
    let Some(current) = current.filter(|state| state.phase == Phase::Ready && state.fm_primary)
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    let Some(removal) = current
        .remote_idle
        .as_ref()
        .and_then(|remote| remote.removal.as_ref())
    else {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    };
    if removal.result == Some(0)
        || !matches_local(&removal.body, &event)
        || event.remote != removal.replica
    {
        return Ok(Transition::ignored(Disposition::IgnoredStale));
    }
    let mut state = current.clone();
    let removal = state
        .remote_idle
        .as_mut()
        .expect("checked remote")
        .removal
        .as_mut()
        .expect("checked removal");
    removal.result = Some(event.error);
    removal.detail = event.detail;
    let effects = state
        .remote_idle
        .as_ref()
        .and_then(RemoteIdle::remove_reply)
        .map(Effect::RemoveReplicaReply)
        .into_iter()
        .collect();
    Ok(lifecycle::changed(state, effects, false))
}

fn matches_local(body: &AddInstance, event: &BuildCompletion) -> bool {
    let mut local = event.local.clone();
    // RAP may refresh progress while retaining this exact invocation identity.
    local.first_lsn = body.replica.first_lsn;
    local.last_lsn = body.replica.last_lsn;
    event.unit == body.unit && local == body.replica
}
