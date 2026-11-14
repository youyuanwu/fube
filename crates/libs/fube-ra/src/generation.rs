use super::*;
use fube_transport::serialization::serde::ObjectArray;

/// Session-only ordinary-FM generations; these do not restore runtime authority.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenerationState {
    pub proposed: GenerationNumber,
    pub receive: GenerationNumber,
    pub send: GenerationNumber,
    pub upload_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationUpdateOutcome {
    IgnoredStale,
    IgnoredNotReady,
    Rejected(GenerationNumber),
    /// Update accepted; its owned per-entity snapshot must complete before upload.
    UploadRequired,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LfumInventory {
    /// Local upload ownership, not a native wire field or generation.
    pub upload: u64,
    pub generation: GenerationNumber,
    pub units: ObjectArray<Vec<FailoverUnitInfo>>,
    pub any_replica_found: bool,
}

pub(super) struct LfumSnapshot {
    generation: GenerationNumber,
    entries: BTreeMap<EntityId, bool>,
    remaining: usize,
    units: BTreeMap<EntityId, FailoverUnitInfo>,
    error: Option<Failure>,
    claimed: bool,
}

impl LfumSnapshot {
    fn new(generation: GenerationNumber, entries: impl Iterator<Item = EntityId>) -> Self {
        let entries: BTreeMap<_, _> = entries.map(|id| (id, false)).collect();
        Self {
            generation,
            remaining: entries.len(),
            entries,
            units: BTreeMap::new(),
            error: None,
            claimed: false,
        }
    }

    pub(super) fn complete(&mut self, id: EntityId, state: Option<&EntityState>) {
        if self.entries.get(&id) != Some(&true) {
            return;
        }
        match configuration(state) {
            Ok(Some(info)) => {
                self.units.insert(id, info);
            }
            Ok(None) => {}
            Err(error) => self.error = Some(error),
        }
        self.remaining -= 1;
    }

    fn inventory(&self, upload: u64) -> Result<Option<LfumInventory>> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok((self.remaining == 0).then(|| LfumInventory {
            upload,
            generation: self.generation.clone(),
            units: ObjectArray(self.units.values().cloned().collect()),
            any_replica_found: !self.entries.is_empty(),
        }))
    }
}

fn configuration(state: Option<&EntityState>) -> Result<Option<FailoverUnitInfo>> {
    let Some(state) = state.filter(|state| !state.local_deleted) else {
        return Ok(None);
    };
    let replicated = !state.remote_idles.is_empty()
        || state.peer_creation.is_some()
        || state.reconfiguration.is_some()
        || state.swap.is_some();
    let dropped = state.phase == Phase::Closed || state.body.replica.state == 3;
    let mut intermediate = Epoch::default();
    let replicas = if dropped {
        vec![lifecycle::dropped_description(state)]
    } else if let Some((configuration, ic)) = swap::lfum_configuration(state) {
        intermediate = ic;
        configuration.replicas.0
    } else if peer::initial_idle_build(state) {
        let mut replicas = vec![state.body.replica.clone()];
        for remote in &state.remote_idles {
            let mut description = remote.reply.as_ref().map_or_else(
                || remote.body.replica.0.replica.clone(),
                |reply| reply.replica.clone(),
            );
            // SF's Primary tracks build completion; the local Idle remains IB.
            description.state = if remote.ready { 2 } else { 1 };
            replicas.push(description);
        }
        replicas
    } else if replicated {
        let config = state
            .reconfiguration
            .as_ref()
            .filter(|c| {
                c.stage == ConfigurationStage::Complete
                    || reconfiguration::initial_membership_configuration(state).is_some()
            })
            .ok_or(Failure::Unsupported(
                "LFUM upload requires completed membership or initial Secondary admission",
            ))?;
        let membership = if config.stage == ConfigurationStage::Complete {
            reconfiguration::stable(&config.request)
        } else {
            // SF retains PC during catch-up; Primary phase 4 also reports IC=CC.
            if state.fm_primary
                && matches!(
                    config.stage,
                    ConfigurationStage::Activate | ConfigurationStage::End
                )
            {
                intermediate = state.body.unit.current;
            }
            config.request.clone()
        };
        if state.phase != Phase::Ready
            || state.swap.is_some()
            || state.progress_query.is_some()
            || membership.unit != state.body.unit
            || membership.replicas.0.len() != state.body.service.target as usize
            || membership
                .replicas
                .0
                .iter()
                .any(|r| !r.up || r.state != 2 || !matches!(r.current_role, 3 | 4))
            || state.remote_idles.iter().any(|r| {
                !r.ready
                    || r.removal.is_some()
                    || !membership
                        .replicas
                        .0
                        .iter()
                        .any(|member| member.node == r.target)
            })
        {
            return Err(Failure::Unsupported(
                "LFUM upload requires full healthy peer membership without recovery",
            ));
        }
        // Admitted membership, not the original Idle creation/build reply,
        // owns remote roles and epochs. Preserve the actual local endpoints.
        membership
            .replicas
            .0
            .into_iter()
            .map(|r| {
                if r.node == state.body.replica.node {
                    state.body.replica.clone()
                } else {
                    r
                }
            })
            .collect()
    } else {
        vec![state.body.replica.clone()]
    };
    Ok(Some(FailoverUnitInfo {
        service: state.body.service.clone(),
        unit: state.body.unit.clone(),
        intermediate,
        report_from_primary: !dropped
            && state.body.replica.up
            && state.body.replica.current_role == 4
            && matches!(state.body.replica.state, 1 | 2),
        replicas: ObjectArray(
            replicas
                .into_iter()
                .map(|mut description| {
                    description.first_lsn = -1;
                    description.last_lsn = -1;
                    let intermediate_role = if intermediate == Epoch::default() {
                        1
                    } else {
                        description.current_role
                    };
                    ReplicaInfo {
                        description,
                        intermediate_role,
                    }
                })
                .collect(),
        ),
        local_deleted: false,
    }))
}

impl GenerationState {
    pub(super) fn seeded(number: GenerationNumber) -> Self {
        Self {
            proposed: number.clone(),
            receive: number.clone(),
            send: number,
            upload_pending: false,
        }
    }
}

impl Executor {
    pub fn generation_state(&self) -> GenerationState {
        lock(&self.shared.view).generations.clone()
    }

    fn admit_generation(&self, source: &NodeInstance, header: &GenerationHeader) -> Result<()> {
        if source != &self.shared.fm_source {
            return Err(Failure::Admission("configured FM session mismatch"));
        }
        if header.for_fm_replica {
            return Err(Failure::Unsupported("FMM generation negotiation is gated"));
        }
        Ok(())
    }

    /// Native common-layer request preprocessing bypasses receive-generation
    /// admission and updates proposed even before the RA is ready.
    pub fn process_generation_proposal(
        &self,
        source: &NodeInstance,
        header: &GenerationHeader,
    ) -> Result<GenerationProposalReply> {
        self.admit_generation(source, header)?;
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        view.generations_touched = true;
        if view.generations.proposed.older_than(&header.number) {
            view.generations.proposed = header.number.clone();
        }
        Ok(GenerationProposalReply {
            proposed: header.number.clone(),
            current: view.generations.proposed.clone(),
            error: if self.shared.node_open {
                0
            } else {
                RA_NOT_READY
            },
        })
    }

    /// Native application-FT path only. The same lock gates entity admission;
    /// already-running entity commits retain their original ownership.
    pub fn process_generation_update(
        &self,
        source: &NodeInstance,
        _body: &GenerationUpdate,
        header: &GenerationHeader,
    ) -> Result<GenerationUpdateOutcome> {
        self.admit_generation(source, header)?;
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        view.generations_touched = true;
        if !self.shared.node_open {
            return Ok(GenerationUpdateOutcome::IgnoredNotReady);
        }
        if header.number.older_than(&view.generations.receive) {
            return Ok(GenerationUpdateOutcome::IgnoredStale);
        }
        if header.number != view.generations.proposed {
            return Ok(GenerationUpdateOutcome::Rejected(
                view.generations.proposed.clone(),
            ));
        }
        // Constants::FMServiceGuid: 00000000-0000-0000-0000-000000000001.
        // Its epoch/ForceAbort branch requires the stage-7 FM-service owner.
        let mut fm_service = [0; 16];
        fm_service[15] = 1;
        if view.entities.contains_key(&fm_service) {
            return Err(Failure::Unsupported(
                "local FM-service generation update requires FMM support",
            ));
        }
        if view.lfum.len() >= self.shared.settings.max_outstanding {
            return Err(Failure::Capacity);
        }
        let upload = view.lfum_sequence.checked_add(1).ok_or(Failure::Capacity)?;
        let snapshot = LfumSnapshot::new(header.number.clone(), view.entities.keys().copied());
        view.lfum.insert(upload, snapshot);
        view.lfum_sequence = upload;
        view.generations.receive = header.number.clone();
        view.generations.upload_pending = true;
        Ok(GenerationUpdateOutcome::UploadRequired)
    }

    /// Observe the newest update's inventory without claiming its upload.
    /// Read jobs wait behind admitted work, including private/uncertain commits.
    pub fn lfum_inventory(&self) -> Result<Option<LfumInventory>> {
        let mut view = lock(&self.shared.view);
        self.prepare_lfum(&mut view)?;
        view.lfum
            .last_key_value()
            .map(|(upload, snapshot)| snapshot.inventory(*upload))
            .transpose()
            .map(Option::flatten)
    }

    /// Transfer each completed update's inventory to one request/retry owner.
    pub fn take_lfum_inventory(&self) -> Result<Option<LfumInventory>> {
        let mut view = lock(&self.shared.view);
        self.prepare_lfum(&mut view)?;
        for (upload, snapshot) in &mut view.lfum {
            if !snapshot.claimed {
                if let Some(inventory) = snapshot.inventory(*upload)? {
                    snapshot.claimed = true;
                    return Ok(Some(inventory));
                }
            }
        }
        Ok(None)
    }

    pub fn pending_lfum_uploads(&self) -> usize {
        lock(&self.shared.view).lfum.len()
    }

    fn prepare_lfum(&self, view: &mut View) -> Result<()> {
        if !view.accepting {
            return Err(Failure::Closed);
        }
        loop {
            let next = view.lfum.iter().find_map(|(upload, snapshot)| {
                snapshot
                    .entries
                    .iter()
                    .find_map(|(id, queued)| (!queued).then_some((*upload, *id)))
            });
            let Some((upload, id)) = next else {
                return Ok(());
            };
            match self.enqueue_locked(id, Kind::GetLfum { upload }, to_vec(&upload), view) {
                Ok(_) => {
                    *view
                        .lfum
                        .get_mut(&upload)
                        .unwrap()
                        .entries
                        .get_mut(&id)
                        .unwrap() = true;
                }
                Err(Failure::Capacity) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    /// Native generation-state callback; tracked request owners use
    /// `complete_lfum_upload` to also retire their own upload.
    pub fn process_lfum_upload_reply(
        &self,
        source: &NodeInstance,
        header: &GenerationHeader,
    ) -> Result<bool> {
        self.admit_generation(source, header)?;
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        Ok(Self::acknowledge_lfum(&mut view, header))
    }

    /// A correlated semantic reply ends its upload even when its generation is
    /// now stale. Other same-generation requests retain independent ownership.
    pub fn complete_lfum_upload(
        &self,
        upload: u64,
        source: &NodeInstance,
        header: &GenerationHeader,
    ) -> Result<bool> {
        self.admit_generation(source, header)?;
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        if view
            .lfum
            .get(&upload)
            .is_none_or(|snapshot| !snapshot.claimed)
        {
            return Err(Failure::Admission(
                "LFUM reply requires an owned completed upload",
            ));
        }
        let advanced = Self::acknowledge_lfum(&mut view, header);
        view.lfum.remove(&upload);
        Ok(advanced)
    }

    fn acknowledge_lfum(view: &mut View, header: &GenerationHeader) -> bool {
        if header.number != view.generations.receive {
            return false;
        }
        view.generations.send = header.number.clone();
        view.generations.upload_pending = false;
        true
    }
}
