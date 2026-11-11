//! Native node intent and multi-entity completion, independent of transport ACKs.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeActivationInfo {
    pub activated: bool,
    pub sequence: i64,
}
#[derive(Clone)]
struct Deactivation {
    sequence: i64,
    units: Vec<EntityId>,
    remaining_jobs: Vec<EntityId>,
    complete: bool,
    started: Option<i64>,
    terminated: bool,
}
#[derive(Clone)]
struct ServiceUpdate {
    body: NodeUpdateServiceRequest,
    remaining: Vec<EntityId>,
    failed: bool,
}
#[derive(Clone)]
pub(super) struct Coordinator {
    pub activation: NodeActivationInfo,
    deactivation: Option<Deactivation>,
    updates: BTreeMap<u64, ServiceUpdate>,
}
impl Default for Coordinator {
    fn default() -> Self {
        Self::new(true)
    }
}
impl Coordinator {
    pub fn new(activated: bool) -> Self {
        Self {
            activation: NodeActivationInfo {
                activated,
                sequence: -1,
            },
            deactivation: None,
            updates: BTreeMap::new(),
        }
    }
    pub fn activate(&mut self, request: &NodeActivationRequest) -> Vec<Effect> {
        if request.sequence > self.activation.sequence
            || (request.sequence == self.activation.sequence && self.activation.activated)
        {
            self.activation = NodeActivationInfo {
                activated: true,
                sequence: request.sequence,
            };
            self.deactivation = None;
        }
        vec![Effect::NodeActivateReply(NodeActivationReply {
            sequence: request.sequence,
        })]
    }
    pub fn deactivate(
        &mut self,
        request: &NodeActivationRequest,
        units: Vec<EntityId>,
    ) -> Vec<Effect> {
        if request.sequence < self.activation.sequence {
            return vec![];
        }
        if request.sequence == self.activation.sequence && !self.activation.activated {
            return if self
                .deactivation
                .as_ref()
                .is_some_and(|pending| pending.complete)
            {
                vec![Effect::NodeDeactivateReply(NodeActivationReply {
                    sequence: request.sequence,
                })]
            } else {
                vec![]
            };
        }
        self.activation = NodeActivationInfo {
            activated: false,
            sequence: request.sequence,
        };
        self.deactivation = Some(Deactivation {
            sequence: request.sequence,
            units: units.clone(),
            remaining_jobs: units.clone(),
            complete: false,
            started: None,
            terminated: false,
        });
        if units.is_empty() {
            self.deactivation.as_mut().unwrap().complete = true;
            vec![Effect::NodeDeactivateReply(NodeActivationReply {
                sequence: request.sequence,
            })]
        } else {
            vec![Effect::NodeDeactivateUnits {
                sequence: request.sequence,
                units,
            }]
        }
    }
    pub fn pending_deactivation(&self) -> Option<i64> {
        self.deactivation
            .as_ref()
            .filter(|pending| !pending.complete)
            .map(|pending| pending.sequence)
    }
    pub fn reserved_replies(&self) -> usize {
        usize::from(self.pending_deactivation().is_some()) + self.updates.len()
    }
    pub fn check(
        &mut self,
        sequence: i64,
        entities: &BTreeMap<EntityId, Snapshot>,
        now: i64,
        config: &NativeConfig,
    ) -> Vec<Effect> {
        if self.pending_deactivation() != Some(sequence) {
            return vec![];
        }
        if let Some(reply) = self.check_closed(entities) {
            return vec![reply];
        }
        let pending = self.deactivation.as_mut().unwrap();
        let mut effects = vec![];
        if pending.started.is_some_and(|started| {
            i128::from(now) - i128::from(started)
                >= i128::from(config.node(NodeProperty::DeactivationCloseTimeoutTicks))
        }) && !pending.terminated
        {
            let mut hosts = std::collections::BTreeSet::new();
            for unit in &pending.units {
                if let Some(registration) = entities
                    .get(unit)
                    .filter(|entry| !entry.fenced)
                    .and_then(|entry| entry.committed.as_ref())
                    .filter(|state| state.phase != Phase::Closed)
                    .and_then(|state| state.runtime.as_ref())
                {
                    if hosts.insert(registration.host.clone()) {
                        effects.push(Effect::TerminateHost {
                            registration: registration.clone(),
                        });
                    }
                }
            }
            pending.terminated = !effects.is_empty();
        }
        effects.push(Effect::ArmNodeDeactivationCheck {
            sequence,
            after_ticks: config
                .node(NodeProperty::DeactivationCheckIntervalTicks)
                .max(1),
        });
        effects
    }
    pub fn check_closed(&mut self, entities: &BTreeMap<EntityId, Snapshot>) -> Option<Effect> {
        let pending = self.deactivation.as_mut()?;
        if pending.complete
            || !pending.remaining_jobs.is_empty()
            || self.activation.activated
            || pending.sequence != self.activation.sequence
        {
            return None;
        }
        if !pending.units.iter().all(|unit| {
            entities.get(unit).is_none_or(|entry| {
                !entry.fenced
                    && entry
                        .committed
                        .as_ref()
                        .is_none_or(|state| state.phase == Phase::Closed)
            })
        }) {
            return None;
        }
        pending.complete = true;
        Some(Effect::NodeDeactivateReply(NodeActivationReply {
            sequence: pending.sequence,
        }))
    }
    pub fn admits_close(&self, sequence: i64, unit: EntityId) -> bool {
        self.deactivation.as_ref().is_some_and(|pending| {
            !self.activation.activated
                && pending.sequence == sequence
                && self.activation.sequence == sequence
                && pending.remaining_jobs.contains(&unit)
        })
    }
    pub fn complete_deactivation_job(&mut self, sequence: i64, unit: EntityId, now: i64) {
        if let Some(pending) = &mut self.deactivation {
            if pending.sequence == sequence {
                pending.remaining_jobs.retain(|entry| *entry != unit);
                if pending.remaining_jobs.is_empty() && pending.started.is_none() {
                    pending.started = Some(now);
                }
            }
        }
    }
    pub fn admits_update(
        &self,
        work: u64,
        unit: EntityId,
        body: &NodeUpdateServiceRequest,
    ) -> bool {
        self.updates
            .get(&work)
            .is_some_and(|pending| pending.body == *body && pending.remaining.contains(&unit))
    }
    pub fn update_service(
        &mut self,
        work: u64,
        body: NodeUpdateServiceRequest,
        units: Vec<EntityId>,
        maximum: usize,
    ) -> Result<Vec<Effect>> {
        if self.updates.len() >= maximum {
            return Err(Failure::Capacity);
        }
        if units.is_empty() {
            return Ok(vec![Self::service_reply(&body)]);
        }
        self.updates.insert(
            work,
            ServiceUpdate {
                body: body.clone(),
                remaining: units.clone(),
                failed: false,
            },
        );
        Ok(vec![Effect::NodeUpdateServiceUnits {
            work,
            body: Box::new(body),
            units,
        }])
    }
    pub fn complete_update(
        &mut self,
        work: u64,
        unit: EntityId,
        changed: bool,
        success: bool,
    ) -> Option<Effect> {
        let pending = self.updates.get_mut(&work)?;
        let index = pending.remaining.iter().position(|item| *item == unit)?;
        pending.remaining.remove(index);
        pending.failed |= changed && !success;
        if !pending.remaining.is_empty() {
            return None;
        }
        let pending = self.updates.remove(&work)?;
        (!pending.failed).then(|| Self::service_reply(&pending.body))
    }
    fn service_reply(body: &NodeUpdateServiceRequest) -> Effect {
        Effect::NodeUpdateServiceReply(NodeUpdateServiceReply {
            service: body.service.name.clone(),
            instance: body.service.instance,
            version: body.service.update_version,
        })
    }
}

pub(super) fn admit(
    source: &NodeInstance,
    generation: &GenerationHeader,
    admission: &Admission,
) -> Result<bool> {
    if source != &admission.fm_source || generation.for_fm_replica {
        return Err(Failure::Admission(
            "node control requires configured normal FM",
        ));
    }
    Ok(!generation.number.older_than(&admission.generation)
        && admission.owner_ready
        && admission.node_open)
}
