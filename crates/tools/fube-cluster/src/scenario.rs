use crate::{
    fm::{Accepted, Bootstrap, Ledger, Observed},
    management::Operation,
    manager::Manager,
    profile::*,
    Cluster, Command,
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
    Legacy,
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
        self.execute(Command {
            op: op.into(),
            action: "stop-node".into(),
            node: Some(node),
            ..Command::default()
        })
        .await
    }

    pub async fn kill_node(&mut self, op: &str, node: u8) -> Result<Value> {
        self.execute(Command {
            op: op.into(),
            action: "kill-node".into(),
            node: Some(node),
            ..Command::default()
        })
        .await
    }

    /// Wait for the retained semantic outcome, never local submission or replica registration.
    pub async fn wait_operation(&mut self, op: &str, budget: Duration) -> Result<Value> {
        Command {
            op: op.into(),
            action: "status".into(),
            ..Command::default()
        }
        .validate()?;
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or("operation wait budget exceeds the clock range")?;
        loop {
            let status = self.status();
            let outcome = status["operations"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(status["node_controls"].as_array().into_iter().flatten())
                .find(|outcome| outcome["op"] == op)
                .ok_or_else(|| format!("operation {op} is not retained"))?;
            match outcome["state"].as_str() {
                Some("complete" | "down") => return Ok(outcome.clone()),
                Some("rejected" | "failed" | "unknown" | "cancelled") => {
                    return Err(format!(
                        "operation {op} ended as {}: {}",
                        outcome["state"], outcome["error"]
                    ))
                }
                Some("pending") => {}
                _ => {
                    return Err(format!(
                        "operation {op} has an invalid retained outcome: {outcome}"
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
        self.execute_fm_command(Command {
            op: op.into(),
            action: "create-partition".into(),
            node: Some(node),
            partition: Some(partition.into()),
            ..Command::default()
        })
        .await
    }

    pub async fn reconfigure_partition(
        &mut self,
        op: &str,
        partition: &str,
        predecessor: ReplicaRef,
    ) -> Result<Value> {
        self.execute_fm_command(Command {
            op: op.into(),
            action: "reconfigure-partition".into(),
            partition: Some(partition.into()),
            predecessor: Some(predecessor),
            ..Command::default()
        })
        .await
    }

    pub async fn rebuild_replica(
        &mut self,
        op: &str,
        partition: &str,
        predecessor: ReplicaRef,
        destination: u8,
    ) -> Result<Value> {
        self.execute_fm_command(Command {
            op: op.into(),
            action: "rebuild-replica".into(),
            partition: Some(partition.into()),
            predecessor: Some(predecessor),
            node: Some(destination),
            ..Command::default()
        })
        .await
    }

    pub async fn cancel_rebuild(&mut self, op: &str, operation: &str) -> Result<Value> {
        self.execute_fm_command(Command {
            op: op.into(),
            action: "cancel-rebuild".into(),
            operation: Some(operation.into()),
            ..Command::default()
        })
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
            if operation.outcome["state"] != "pending" {
                continue;
            }
            let command = &operation.command;
            let progress = match command.action.as_str() {
                "reconfigure-partition" => {
                    let status = self.configuration_status();
                    if status["request"]["configuration"]["unit"]["current"]
                        != operation.outcome["epoch"]
                    {
                        Some(("unknown", "unknown"))
                    } else if status["ready"] == true {
                        Some(("complete", "ready"))
                    } else if status["state"] == "unknown" {
                        Some(("unknown", "unknown"))
                    } else {
                        None
                    }
                }
                "rebuild-replica" => {
                    if command.node.is_some_and(|node| {
                        !self.placements[usize::from(node - 1)]
                            .launch
                            .profile
                            .stateful()
                    }) {
                        let p = &self.placements[usize::from(command.node.unwrap() - 1)];
                        if json!(p.launch.node.instance) != operation.outcome["node_instance"]
                            || p.loss_reported
                        {
                            Some(("unknown", "unknown"))
                        } else if p.observed == Observed::Running {
                            Some(("complete", "ready"))
                        } else {
                            None
                        }
                    } else {
                        let status = self.repair_status();
                        let same = self.repair.as_ref().is_some_and(|repair| {
                            json!(repair.build.body.replica.id) == operation.outcome["replica"]
                                && json!(repair.build.body.replica.instance)
                                    == operation.outcome["incarnation"]
                                && json!(repair.build.body.replica.node.instance)
                                    == operation.outcome["node_instance"]
                        });
                        if !same || status["state"] == "unknown" {
                            Some(("unknown", "unknown"))
                        } else if status["ready"] == true {
                            Some(("complete", "ready"))
                        } else if status["state"] == "failed" {
                            Some(("failed", "build_failed"))
                        } else if status["replicator_removed"] == true {
                            Some(("cancelled", "removed"))
                        } else if status["built"] == true {
                            Some(("pending", "admitting"))
                        } else {
                            Some(("pending", "building"))
                        }
                    }
                }
                "cancel-rebuild" => {
                    let same = self.repair.as_ref().is_some_and(|repair| {
                        json!(repair.build.body.replica.id) == operation.outcome["replica"]
                            && json!(repair.build.body.replica.instance)
                                == operation.outcome["incarnation"]
                            && json!(repair.build.body.replica.node.instance)
                                == operation.outcome["node_instance"]
                    });
                    if !same {
                        Some(("unknown", "unknown"))
                    } else if self.repair_status()["replicator_removed"] == true {
                        Some(("complete", "removed"))
                    } else if self.repair_status()["state"] == "unknown" {
                        Some(("unknown", "unknown"))
                    } else {
                        None
                    }
                }
                "create-partition" => Some((
                    "pending",
                    if command.node.is_some_and(|node| {
                        self.placements[usize::from(node - 1)].observed == Observed::Running
                    }) {
                        "building_initial_membership"
                    } else {
                        "opening"
                    },
                )),
                _ => None,
            };
            if let Some((state, phase)) = progress {
                let mut outcome = operation.outcome.clone();
                outcome["state"] = json!(state);
                outcome["phase"] = json!(phase);
                if state == "unknown" {
                    outcome["error"] = json!("original recovery authority was lost or superseded");
                }
                if state == "failed" {
                    outcome["error"] = self.repair_status()["build_reply"]["error"].clone();
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
            "node_instance":placement.launch.node.instance,"replica_recovery":"not_requested"}),
        )
    }
}
