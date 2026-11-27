use crate::status::{ConfigurationState, RepairState};
use crate::{
    fm::{Accepted, Bootstrap, Ledger, Observed},
    management::Operation,
    manager::Manager,
    profile::*,
    Cluster, Command, CommandAction, OperationPhase, OperationState,
};
use fube_ra::protocol::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryMode {
    #[default]
    Manual,
}

/// An exact replica predecessor on a statically configured placement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaRef {
    pub node: u8,
    pub node_instance: u64,
    pub replica: i64,
    pub incarnation: i64,
}

impl Cluster {
    pub async fn stop_node(&mut self, op: &str, node: u8) -> Result<Value> {
        self.execute(Command::new(op, CommandAction::StopNode { node }))
            .await
    }

    pub async fn kill_node(&mut self, op: &str, node: u8) -> Result<Value> {
        self.execute(Command::new(op, CommandAction::KillNode { node }))
            .await
    }

    /// Wait for the retained semantic outcome, never local submission or replica registration.
    pub async fn wait_operation(&mut self, op: &str, budget: Duration) -> Result<Value> {
        self.wait_operation_typed(op, budget)
            .await
            .map(|outcome| outcome.json())
    }

    pub async fn wait_operation_typed(
        &mut self,
        op: &str,
        budget: Duration,
    ) -> Result<crate::OperationOutcome> {
        Command::new(op, CommandAction::Status).validate()?;
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or("operation wait budget exceeds the clock range")?;
        loop {
            let status = self.status_typed();
            let outcome = status
                .operations
                .iter()
                .chain(status.node_controls.iter().flatten())
                .find(|outcome| outcome.op == op)
                .ok_or_else(|| format!("operation {op} is not retained"))?;
            match outcome.state {
                OperationState::Complete | OperationState::Down => return Ok(outcome.clone()),
                OperationState::Rejected
                | OperationState::Failed
                | OperationState::Unknown
                | OperationState::Cancelled
                | OperationState::Superseded => {
                    return Err(format!(
                        "operation {op} ended as {}: {}",
                        json!(outcome.state),
                        outcome.error.as_ref().unwrap_or(&Value::Null)
                    ))
                }
                OperationState::Pending => {}
                _ => {
                    return Err(format!(
                        "operation {op} has an invalid retained outcome: {}",
                        outcome.json()
                    ))
                }
            }
            if self.manager.finished.is_some() {
                return Err(format!("cluster closed before operation {op} completed"));
            }
            if Instant::now() >= deadline || !self.poll_operation_progress(deadline).await? {
                return Err(format!(
                    "operation {op} wait deadline elapsed; waiting does not cancel the operation"
                ));
            }
            tokio::time::sleep_until((Instant::now() + Duration::from_millis(5)).min(deadline))
                .await;
        }
    }

    async fn poll_operation_progress(&mut self, deadline: Instant) -> Result<bool> {
        self.observe_node_owners();
        if let Some(remote) = &mut self.remote {
            if remote.poll_status_until(deadline).await?.is_none() {
                return Ok(false);
            }
        } else if tokio::time::timeout_at(deadline, self.manager.drive())
            .await
            .is_err()
        {
            if self.manager.progressing_io {
                let error = "operation wait interrupted native FM progress; original control authority is unknown";
                self.manager.interrupt_progress(error);
                return Err(error.into());
            }
            return Ok(false);
        }
        self.refresh_node_controls();
        Ok(true)
    }

    /// Select the recovery policy before any FM or Node is started.
    pub fn set_recovery_mode(&mut self, mode: RecoveryMode) -> Result<()> {
        if !self.manager.children.is_empty()
            || self.remote.is_some()
            || self.manager.finished.is_some()
        {
            return Err("recovery mode is immutable after cluster startup".into());
        }
        self.manager.ledger.recovery_mode = mode;
        Ok(())
    }

    pub async fn create_partition(&mut self, op: &str, partition: &str, node: u8) -> Result<Value> {
        self.execute_fm_command(Command::new(
            op,
            CommandAction::CreatePartition {
                partition: partition.into(),
                node,
            },
        ))
        .await
    }

    pub async fn reconfigure_partition(
        &mut self,
        op: &str,
        partition: &str,
        predecessor: ReplicaRef,
    ) -> Result<Value> {
        self.execute_fm_command(Command::new(
            op,
            CommandAction::ReconfigurePartition {
                partition: partition.into(),
                predecessor,
                operation: None,
            },
        ))
        .await
    }

    pub async fn rebuild_replica(
        &mut self,
        op: &str,
        partition: &str,
        predecessor: ReplicaRef,
        destination: u8,
    ) -> Result<Value> {
        self.execute_fm_command(Command::new(
            op,
            CommandAction::RebuildReplica {
                partition: partition.into(),
                predecessor,
                node: destination,
            },
        ))
        .await
    }

    pub async fn supersede_rebuild(
        &mut self,
        op: &str,
        partition: &str,
        predecessor: ReplicaRef,
        operation: &str,
    ) -> Result<Value> {
        self.execute_fm_command(Command::new(
            op,
            CommandAction::ReconfigurePartition {
                partition: partition.into(),
                predecessor,
                operation: Some(operation.into()),
            },
        ))
        .await
    }

    pub async fn cancel_rebuild(&mut self, op: &str, operation: &str) -> Result<Value> {
        self.execute_fm_command(Command::new(
            op,
            CommandAction::CancelRebuild {
                operation: operation.into(),
            },
        ))
        .await
    }

    /// Infrastructure-only cold rejoin. The caller must first report a fenced NodeDown.
    pub async fn restart_node_infrastructure(&mut self, node: u8) -> Result<Value> {
        self.restart_node_owned(node, false).await
    }

    pub(crate) async fn finish_node_infrastructure(&mut self, index: usize) -> Result<Value> {
        if let Some(remote) = &mut self.remote {
            remote
                .request(Operation::FinishNodeInfrastructure { index })
                .await
        } else {
            self.manager.finish_node_infrastructure(index)
        }
    }
}

impl Ledger {
    pub(crate) fn validate_partition(&self, partition: &str) -> Result<()> {
        uuid::Uuid::parse_str(partition).map_err(err)?;
        if !self
            .placements
            .iter()
            .any(|p| p.launch.partition == partition)
        {
            return Err("partition is not in the current static profile".into());
        }
        Ok(())
    }

    pub(crate) fn predecessor_index(
        &self,
        partition: &str,
        predecessor: &ReplicaRef,
    ) -> Result<usize> {
        self.validate_partition(partition)?;
        let index = usize::from(
            predecessor
                .node
                .checked_sub(1)
                .ok_or("Node numbers start at one")?,
        );
        let placement = self
            .placements
            .get(index)
            .ok_or("unknown predecessor placement")?;
        let matches = |node: &NodeInstance, replica: i64, incarnation: i64| {
            node.instance == predecessor.node_instance
                && replica == predecessor.replica
                && incarnation == predecessor.incarnation
        };
        if placement.launch.partition != partition {
            return Err("predecessor belongs to another partition".into());
        }
        if self.known_spare_loss(placement) {
            return Err(
                "predecessor was already replaced on a spare and is no longer a current failure"
                    .into(),
            );
        }
        if matches(
            &placement.launch.node,
            placement.replica,
            placement.incarnation,
        ) {
            if placement.loss_reported {
                return Ok(index);
            }
        } else if !placement.created
            && placement.node_down.is_none()
            && self.node_failures.iter().any(|failure| {
                failure.index == index
                    && failure.body.as_ref().is_some_and(|body| {
                        matches(
                            &failure.receipt.node,
                            body.replica.id,
                            body.replica.instance,
                        )
                    })
                    && failure
                        .replacement
                        .as_ref()
                        .is_none_or(|launch| launch.node == placement.launch.node)
            })
        {
            return Ok(index);
        }
        Err("predecessor is not the exact current failed replica lineage".into())
    }

    pub(crate) fn reconfigure_failed_replica(
        &mut self,
        partition: &str,
        predecessor: &ReplicaRef,
    ) -> Result<Option<DoReconfiguration>> {
        let index = self.predecessor_index(partition, predecessor)?;
        if self.node_failures.iter().any(|failure| {
            failure.index == index && failure.receipt.node.instance == predecessor.node_instance
        }) {
            return self.reconfigure_node_down(index);
        }
        if self.failed_primary_index() == Some(index) {
            return Ok(None);
        }
        if self.repair.as_ref().is_some_and(|repair| {
            repair
                .removal
                .as_ref()
                .is_some_and(|removal| removal.reply.is_none())
        }) {
            return Err(
                "pending Secondary removal requires an explicit rebuild operation to supersede"
                    .into(),
            );
        }
        if self.primary_index() != index {
            return Err("reconfiguration predecessor is not the failed current Primary".into());
        }
        self.start_primary_failover(Instant::now())?
            .map(Some).ok_or_else(|| "no supported survivor failover transition; use explicit replica rebuild for Secondary app-host repair".into())
    }

    pub(crate) fn begin_explicit_rebuild(
        &mut self,
        partition: &str,
        predecessor: &ReplicaRef,
        destination: usize,
        op: &str,
    ) -> Result<Accepted> {
        let index = self.predecessor_index(partition, predecessor)?;
        if destination >= self.placements.len()
            || self.placements[destination].launch.partition != partition
        {
            return Err("rebuild destination is outside the partition's static topology".into());
        }
        if !self.placements[destination].launch.profile.stateful() {
            if destination != index || self.placements[destination].created {
                return Err(
                    "stateless rebuild requires the fresh rejoined original placement".into(),
                );
            }
            return self.begin_creation(destination, op);
        }
        if self.node_failures.iter().any(|failure| {
            failure.index == index && failure.receipt.node.instance == predecessor.node_instance
        }) {
            if self.begin_node_replica_rebuild(index, destination)? {
                return Ok(Accepted::Build);
            }
            return Ok(Accepted::Progress);
        }
        if destination != index {
            if self.begin_spare_replacement(destination, Some(index), Instant::now())? {
                return Ok(Accepted::Build);
            }
        } else if self.restoration.is_some() {
            if let Some(body) = self.begin_secondary_repair(index, Instant::now())? {
                return Ok(Accepted::Reconfigure(Box::new(body)));
            }
        } else if self.failed_primary_index() == Some(index) {
            if self.begin_primary_repair(Instant::now())? {
                return Ok(Accepted::Build);
            }
        } else if let Some(body) = self.begin_secondary_repair(index, Instant::now())? {
            return Ok(Accepted::Reconfigure(Box::new(body)));
        }
        Ok(Accepted::Progress)
    }

    pub(crate) fn refresh_scenario_operations(&mut self) {
        let mut updates = Vec::new();
        for operation in &self.operations {
            if operation.outcome.state != OperationState::Pending {
                continue;
            }
            let command = &operation.command;
            let progress = match &command.action {
                CommandAction::ReconfigurePartition { .. } => {
                    let status = self.configuration_view();
                    if Some(
                        status
                            .request
                            .as_ref()
                            .map(|request| request.configuration.unit.current),
                    ) != operation.outcome.epoch
                    {
                        Some((OperationState::Unknown, OperationPhase::Unknown))
                    } else if status.ready {
                        Some((OperationState::Complete, OperationPhase::Ready))
                    } else if status.state == ConfigurationState::Unknown {
                        Some((OperationState::Unknown, OperationPhase::Unknown))
                    } else {
                        None
                    }
                }
                CommandAction::RebuildReplica { node, .. } => {
                    if !self.placements[usize::from(node - 1)]
                        .launch
                        .profile
                        .stateful()
                    {
                        let p = &self.placements[usize::from(node - 1)];
                        if Some(p.launch.node.instance) != operation.outcome.node_instance
                            || p.loss_reported
                        {
                            Some((OperationState::Unknown, OperationPhase::Unknown))
                        } else if p.observed == Observed::Running {
                            Some((OperationState::Complete, OperationPhase::Ready))
                        } else {
                            None
                        }
                    } else {
                        let status = self.repair_view();
                        let same = self.repair.as_ref().is_some_and(|repair| {
                            operation.outcome.owns_replica(&repair.build.body.replica)
                        });
                        if !same || status.state == RepairState::Unknown {
                            Some((OperationState::Unknown, OperationPhase::Unknown))
                        } else if status.ready {
                            Some((OperationState::Complete, OperationPhase::Ready))
                        } else if status.state == RepairState::Failed {
                            Some((OperationState::Failed, OperationPhase::BuildFailed))
                        } else if self
                            .repair
                            .as_ref()
                            .is_some_and(|repair| repair.replicator_removed())
                        {
                            Some((OperationState::Cancelled, OperationPhase::Removed))
                        } else if self.repair.as_ref().is_some_and(|repair| {
                            repair
                                .build
                                .reply
                                .as_ref()
                                .is_some_and(|reply| reply.error == 0)
                        }) {
                            Some((OperationState::Pending, OperationPhase::Admitting))
                        } else {
                            Some((OperationState::Pending, OperationPhase::Building))
                        }
                    }
                }
                CommandAction::CancelRebuild { .. } => {
                    let same = self.repair.as_ref().is_some_and(|repair| {
                        operation.outcome.owns_replica(&repair.build.body.replica)
                    });
                    if !same {
                        Some((OperationState::Unknown, OperationPhase::Unknown))
                    } else if self
                        .repair
                        .as_ref()
                        .is_some_and(|repair| repair.replicator_removed())
                    {
                        Some((OperationState::Complete, OperationPhase::Removed))
                    } else if self.repair_view().state == RepairState::Unknown {
                        Some((OperationState::Unknown, OperationPhase::Unknown))
                    } else {
                        None
                    }
                }
                CommandAction::CreatePartition { node, .. } => Some((
                    OperationState::Pending,
                    if self.placements[usize::from(node - 1)].observed == Observed::Running {
                        OperationPhase::BuildingInitialMembership
                    } else {
                        OperationPhase::Opening
                    },
                )),
                _ => None,
            };
            if let Some((state, phase)) = progress {
                let mut outcome = operation.outcome.clone();
                outcome.state = state;
                outcome.phase = Some(phase);
                if state == OperationState::Unknown {
                    outcome.error =
                        Some(json!("original recovery authority was lost or superseded"));
                }
                if state == OperationState::Failed {
                    outcome.error = Some(json!(self
                        .repair
                        .as_ref()
                        .and_then(|repair| repair.build.reply.as_ref())
                        .map(|reply| reply.error)));
                }
                updates.push((command.op.clone(), outcome));
            }
        }
        for (op, outcome) in updates {
            self.complete(&op, outcome);
        }
    }
}

impl Manager {
    pub(crate) fn finish_node_infrastructure(&self, index: usize) -> Result<Value> {
        let placement = self
            .ledger
            .placements
            .get(index)
            .ok_or("unknown restart placement")?;
        self.ledger.restart_desired(index)?;
        if self.finished.is_some()
            || !placement.link_alive
            || placement.bootstrap != Bootstrap::Admitted
        {
            return Err("restart completion requires an admitted current Node incarnation".into());
        }
        Ok(
            json!({"state":"complete","kind":"node_restart","node":index + 1,
            "node_instance":placement.launch.node.instance,"replica_recovery":crate::ReplicaRecovery::NotRequested}),
        )
    }
}
