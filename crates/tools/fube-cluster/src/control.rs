use crate::{profile::Result, ReplicaRef};
use fube_ra::protocol::{Epoch, Replica};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Compatibility DTO for the public JSON controls. Convert before dispatch.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub op: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<ReplicaRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandAction {
    Create {
        node: u8,
    },
    Delete {
        node: u8,
    },
    Activate {
        node: u8,
    },
    Deactivate {
        node: u8,
    },
    Status,
    Quit,
    NodeDown {
        node: u8,
    },
    RestartNode {
        node: u8,
    },
    StopNode {
        node: u8,
    },
    KillNode {
        node: u8,
    },
    CreatePartition {
        partition: String,
        node: u8,
    },
    ReconfigurePartition {
        partition: String,
        predecessor: ReplicaRef,
        operation: Option<String>,
    },
    RebuildReplica {
        partition: String,
        predecessor: ReplicaRef,
        node: u8,
    },
    CancelRebuild {
        operation: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Command", into = "Command")]
pub struct CommandRequest {
    pub op: String,
    pub action: CommandAction,
}

impl Command {
    pub fn new(op: impl Into<String>, action: CommandAction) -> Self {
        CommandRequest {
            op: op.into(),
            action,
        }
        .into()
    }

    pub fn validate(&self) -> Result<()> {
        self.request().map(|_| ())
    }

    pub fn request(&self) -> Result<CommandRequest> {
        self.clone().try_into()
    }
}

impl TryFrom<Command> for CommandRequest {
    type Error = String;

    fn try_from(command: Command) -> Result<Self> {
        let Command {
            op,
            action,
            node,
            partition,
            predecessor,
            operation,
        } = command;
        for identity in std::iter::once(&op).chain(operation.iter()) {
            if identity.is_empty()
                || identity.len() > 64
                || !identity
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            {
                return Err("operation identity must be 1..64 ASCII identifier bytes".into());
            }
        }
        if let Some(partition) = &partition {
            uuid::Uuid::parse_str(partition).map_err(crate::profile::err)?;
        }
        if predecessor.as_ref().is_some_and(|p| {
            !(1..=64).contains(&p.node)
                || p.node_instance == 0
                || p.replica <= 0
                || p.incarnation <= 0
        }) {
            return Err("invalid exact replica predecessor".into());
        }
        let action = match (action.as_str(), node, partition, predecessor, operation) {
            ("create-partition", Some(node @ 1..=64), Some(partition), None, None) =>
                CommandAction::CreatePartition { partition, node },
            ("reconfigure-partition", None, Some(partition), Some(predecessor), operation) =>
                CommandAction::ReconfigurePartition { partition, predecessor, operation },
            ("rebuild-replica", Some(node @ 1..=64), Some(partition), Some(predecessor), None) =>
                CommandAction::RebuildReplica { partition, predecessor, node },
            ("cancel-rebuild", None, None, None, Some(operation)) =>
                CommandAction::CancelRebuild { operation },
            (_, _, partition, predecessor, operation)
                if partition.is_some() || predecessor.is_some() || operation.is_some() =>
                return Err("control fields do not match the command action".into()),
            ("node-down", Some(node @ 1..=64), None, None, None) => CommandAction::NodeDown { node },
            ("restart-node", Some(node @ 1..=64), None, None, None) => CommandAction::RestartNode { node },
            ("stop-node", Some(node @ 1..=64), None, None, None) => CommandAction::StopNode { node },
            ("kill-node", Some(node @ 1..=64), None, None, None) => CommandAction::KillNode { node },
            ("create", Some(node @ 1..=2), None, None, None) => CommandAction::Create { node },
            ("delete", Some(node @ 1..=2), None, None, None) => CommandAction::Delete { node },
            ("activate", Some(node @ 1..=2), None, None, None) => CommandAction::Activate { node },
            ("deactivate", Some(node @ 1..=2), None, None, None) => CommandAction::Deactivate { node },
            ("status", None, None, None, None) => CommandAction::Status,
            ("quit", None, None, None, None) => CommandAction::Quit,
            _ => return Err("expected create/delete/activate/deactivate with node 1|2, or status/quit without node".into()),
        };
        Ok(Self { op, action })
    }
}

impl From<CommandRequest> for Command {
    fn from(request: CommandRequest) -> Self {
        let mut command = Self {
            op: request.op,
            ..Self::default()
        };
        command.action = match request.action {
            CommandAction::Create { node } => {
                command.node = Some(node);
                "create"
            }
            CommandAction::Delete { node } => {
                command.node = Some(node);
                "delete"
            }
            CommandAction::Activate { node } => {
                command.node = Some(node);
                "activate"
            }
            CommandAction::Deactivate { node } => {
                command.node = Some(node);
                "deactivate"
            }
            CommandAction::Status => "status",
            CommandAction::Quit => "quit",
            CommandAction::NodeDown { node } => {
                command.node = Some(node);
                "node-down"
            }
            CommandAction::RestartNode { node } => {
                command.node = Some(node);
                "restart-node"
            }
            CommandAction::StopNode { node } => {
                command.node = Some(node);
                "stop-node"
            }
            CommandAction::KillNode { node } => {
                command.node = Some(node);
                "kill-node"
            }
            CommandAction::CreatePartition { partition, node } => {
                command.partition = Some(partition);
                command.node = Some(node);
                "create-partition"
            }
            CommandAction::ReconfigurePartition {
                partition,
                predecessor,
                operation,
            } => {
                command.partition = Some(partition);
                command.predecessor = Some(predecessor);
                command.operation = operation;
                "reconfigure-partition"
            }
            CommandAction::RebuildReplica {
                partition,
                predecessor,
                node,
            } => {
                command.partition = Some(partition);
                command.predecessor = Some(predecessor);
                command.node = Some(node);
                "rebuild-replica"
            }
            CommandAction::CancelRebuild { operation } => {
                command.operation = Some(operation);
                "cancel-rebuild"
            }
        }
        .into();
        command
    }
}

impl CommandRequest {
    pub(crate) fn supervisor_control(&self) -> bool {
        matches!(
            self.action,
            CommandAction::NodeDown { .. }
                | CommandAction::RestartNode { .. }
                | CommandAction::StopNode { .. }
                | CommandAction::KillNode { .. }
        )
    }

    pub(crate) fn node(&self) -> Option<u8> {
        match self.action {
            CommandAction::Create { node }
            | CommandAction::Delete { node }
            | CommandAction::Activate { node }
            | CommandAction::Deactivate { node }
            | CommandAction::NodeDown { node }
            | CommandAction::RestartNode { node }
            | CommandAction::StopNode { node }
            | CommandAction::KillNode { node }
            | CommandAction::CreatePartition { node, .. }
            | CommandAction::RebuildReplica { node, .. } => Some(node),
            _ => None,
        }
    }

    pub(crate) fn partition(&self) -> Option<&str> {
        match &self.action {
            CommandAction::CreatePartition { partition, .. }
            | CommandAction::ReconfigurePartition { partition, .. }
            | CommandAction::RebuildReplica { partition, .. } => Some(partition),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Complete,
    Down,
    Rejected,
    Failed,
    Unknown,
    Cancelled,
    Superseded,
    Lost,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    Opening,
    BuildingInitialMembership,
    MembershipReady,
    Building,
    Admitting,
    Ready,
    Removing,
    Removed,
    BuildFailed,
    Unknown,
    Superseded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRecovery {
    Pending,
    Ready,
    NotRequired,
    NotRequested,
}

// Retained owner fields distinguish an explicit JSON null from an omitted field.
pub(crate) fn present<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

/// Retained semantic outcome. Extra fields are receipts/diagnostics, not dispatch state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationOutcome {
    pub op: String,
    pub state: OperationState,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub phase: Option<OperationPhase>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub replica: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub incarnation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub node_instance: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub partition: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub epoch: Option<Option<Epoch>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub error: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_recovery: Option<ReplicaRecovery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_reconfiguration: Option<crate::status::ReplicaReconfiguration>,
    #[serde(flatten)]
    pub(crate) details: Map<String, Value>,
}

impl OperationOutcome {
    pub fn new(op: impl Into<String>, state: OperationState) -> Self {
        Self {
            op: op.into(),
            state,
            phase: None,
            replica: None,
            incarnation: None,
            node_instance: None,
            partition: None,
            epoch: None,
            operation: None,
            superseded_by: None,
            error: None,
            replica_recovery: None,
            replica_reconfiguration: None,
            details: Map::new(),
        }
    }

    pub(crate) fn with_error(mut self, error: impl Into<Value>) -> Self {
        self.error = Some(error.into());
        self
    }

    pub(crate) fn detail(mut self, key: &str, value: impl Serialize) -> Self {
        self.details.insert(key.into(), serde_json::json!(value));
        self
    }

    pub(crate) fn bind_replica(&mut self, replica: &Replica) {
        self.replica = Some(replica.id);
        self.incarnation = Some(replica.instance);
        self.node_instance = Some(replica.node.instance);
    }

    pub(crate) fn owns_replica(&self, replica: &Replica) -> bool {
        self.replica == Some(replica.id)
            && self.incarnation == Some(replica.instance)
            && self.node_instance == Some(replica.node.instance)
    }

    pub(crate) fn retain_owner(&mut self, previous: &Self) {
        self.replica = self.replica.or(previous.replica);
        self.incarnation = self.incarnation.or(previous.incarnation);
        self.node_instance = self.node_instance.or(previous.node_instance);
        self.partition = self.partition.take().or_else(|| previous.partition.clone());
        self.epoch = self.epoch.or(previous.epoch);
    }

    pub fn json(&self) -> Value {
        serde_json::json!(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_commands_preserve_canonical_json_and_reject_mixed_payloads() {
        let predecessor = ReplicaRef {
            node: 1,
            node_instance: 42,
            replica: 7,
            incarnation: 9,
        };
        let partition = "01234567-89ab-cdef-0123-456789abcdef";
        let request = CommandRequest {
            op: "reconfigure".into(),
            action: CommandAction::ReconfigurePartition {
                partition: partition.into(),
                predecessor: predecessor.clone(),
                operation: Some("rebuild".into()),
            },
        };
        let expected = json!({
            "op":"reconfigure","action":"reconfigure-partition","partition":partition,
            "predecessor":predecessor,"operation":"rebuild"
        });
        assert_eq!(serde_json::to_value(&request).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<CommandRequest>(expected.clone()).unwrap(),
            request
        );
        for (key, value) in [("node", json!(1)), ("extra", json!(true))] {
            let mut mixed = expected.clone();
            mixed[key] = value;
            assert!(serde_json::from_value::<CommandRequest>(mixed).is_err());
        }
        let invalid = Command::new("invalid", CommandAction::StopNode { node: 0 });
        assert!(invalid.request().is_err());
        assert!(Command::new("bad id", CommandAction::Status)
            .request()
            .is_err());
        assert_eq!(
            Command::new("stop", CommandAction::StopNode { node: 1 })
                .request()
                .unwrap()
                .node(),
            Some(1)
        );
    }

    #[test]
    fn typed_outcomes_preserve_nulls_and_exact_owner_on_completion() {
        let expected = json!({
            "op":"rebuild","state":"pending","replica":7,"incarnation":9,
            "node_instance":42,"partition":"partition","epoch":null,"error":null
        });
        let previous: OperationOutcome = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(previous.json(), expected);
        let mut complete = OperationOutcome::new("rebuild", OperationState::Complete);
        complete.retain_owner(&previous);
        assert_eq!(complete.replica, Some(7));
        assert_eq!(complete.incarnation, Some(9));
        assert_eq!(complete.node_instance, Some(42));
        assert_eq!(complete.epoch, Some(None));
        assert!(!complete.json().as_object().unwrap().contains_key("error"));
        let replica = Replica {
            id: 7,
            instance: 9,
            node: fube_ra::protocol::NodeInstance {
                instance: 43,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!complete.owns_replica(&replica));
        for (key, value) in [
            ("state", json!("typo")),
            ("phase", json!("typo")),
            ("node_instance", json!("42")),
            ("node_instance", Value::Null),
        ] {
            let mut invalid = expected.clone();
            invalid[key] = value;
            assert!(serde_json::from_value::<OperationOutcome>(invalid).is_err());
        }
    }
}
