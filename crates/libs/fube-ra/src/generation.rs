use super::*;

/// Session-only ordinary-FM generations. Upload/restart adoption is not enabled.
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
    /// Receive advanced, but no snapshot, LFUM send or upload acknowledgment exists.
    UploadRequired,
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
        view.generations.receive = header.number.clone();
        view.generations.upload_pending = true;
        Ok(GenerationUpdateOutcome::UploadRequired)
    }
}
