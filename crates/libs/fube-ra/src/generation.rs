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
    /// Receive advanced; the owned per-entity snapshot must complete before upload.
    UploadRequired,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LfumInventory {
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
        }
    }

    pub(super) fn complete(
        &mut self,
        generation: &GenerationNumber,
        id: EntityId,
        state: Option<&EntityState>,
    ) {
        if generation != &self.generation || self.entries.get(&id) != Some(&true) {
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
}

fn configuration(state: Option<&EntityState>) -> Result<Option<FailoverUnitInfo>> {
    let Some(state) = state.filter(|state| !state.local_deleted) else {
        return Ok(None);
    };
    // The deployed upload profile excludes peer topology/reconfiguration. Do
    // not silently omit remote replicas or invent intermediate configuration.
    if !state.remote_idles.is_empty()
        || state.peer_creation.is_some()
        || state.reconfiguration.is_some()
        || state.swap.is_some()
    {
        return Err(Failure::Unsupported(
            "peer/reconfiguration LFUM upload is gated",
        ));
    }
    let dropped = state.phase == Phase::Closed || state.body.replica.state == 3;
    let replica = if dropped {
        lifecycle::dropped_description(state)
    } else {
        let mut replica = state.body.replica.clone();
        replica.first_lsn = -1;
        replica.last_lsn = -1;
        replica
    };
    Ok(Some(FailoverUnitInfo {
        service: state.body.service.clone(),
        unit: state.body.unit.clone(),
        intermediate: Epoch::default(),
        report_from_primary: !dropped
            && replica.up
            && replica.current_role == 4
            && matches!(replica.state, 1 | 2),
        replicas: ObjectArray(vec![ReplicaInfo {
            description: replica,
            intermediate_role: 1,
        }]),
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
        if !view.generations.upload_pending
            || view
                .lfum
                .as_ref()
                .is_none_or(|snapshot| snapshot.generation != header.number)
        {
            view.lfum = Some(LfumSnapshot::new(
                header.number.clone(),
                view.entities.keys().copied(),
            ));
        }
        view.generations.receive = header.number.clone();
        view.generations.upload_pending = true;
        Ok(GenerationUpdateOutcome::UploadRequired)
    }

    /// Enqueue read jobs behind already admitted entity work. A private or
    /// uncertain commit cannot be sampled through the published map.
    pub fn lfum_inventory(&self) -> Result<Option<LfumInventory>> {
        let mut view = lock(&self.shared.view);
        if !view.accepting {
            return Err(Failure::Closed);
        }
        loop {
            let Some(snapshot) = &view.lfum else {
                return Ok(None);
            };
            if let Some(error) = &snapshot.error {
                return Err(error.clone());
            }
            let generation = snapshot.generation.clone();
            let Some(id) = snapshot
                .entries
                .iter()
                .find_map(|(id, queued)| (!queued).then_some(*id))
            else {
                return Ok((snapshot.remaining == 0).then(|| LfumInventory {
                    generation,
                    units: ObjectArray(snapshot.units.values().cloned().collect()),
                    any_replica_found: !snapshot.entries.is_empty(),
                }));
            };
            match self.enqueue_locked(
                id,
                Kind::GetLfum {
                    generation: generation.clone(),
                },
                to_vec(&generation),
                &mut view,
            ) {
                Ok(_) => {
                    *view.lfum.as_mut().unwrap().entries.get_mut(&id).unwrap() = true;
                }
                Err(Failure::Capacity) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
    }

    /// Only a consuming LFUM request operation may deliver this acknowledgment.
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
        if header.number != view.generations.receive {
            return Ok(false);
        }
        if view.lfum.as_ref().is_none_or(|snapshot| {
            snapshot.generation != header.number
                || snapshot.remaining != 0
                || snapshot.error.is_some()
        }) {
            return Err(Failure::Admission(
                "LFUM acknowledgment precedes owned snapshot completion",
            ));
        }
        view.generations.send = header.number.clone();
        view.generations.upload_pending = false;
        Ok(true)
    }
}
