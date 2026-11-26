use crate::{
    manager::Manager,
    process_control::{frame, Lines},
    profile::*,
    Command,
};
use fube_local_host::process::OwnedApp;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufReader},
    time::Instant,
};

/// Private same-host deployment manifest, not an SF reliability message.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub schema_version: u32,
    pub launches: Vec<PathBuf>,
    pub recovery_mode: crate::RecoveryMode,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: u64,
    operation: Operation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Attach {
        index: usize,
        metadata: Metadata,
    },
    NodeOwner {
        index: usize,
    },
    NodeDown {
        index: usize,
        receipt: NodeDownReceipt,
    },
    PrepareNodeRestart {
        index: usize,
    },
    FinishNodeRestart {
        index: usize,
    },
    FinishNodeInfrastructure {
        index: usize,
    },
    Command {
        command: Command,
    },
    Status,
    Generation,
    RepairPrimary,
    RepairSecondary {
        node: u8,
    },
    CancelRepair,
    DeleteCancelledRepair,
    ReplaceReplica {
        node: u8,
    },
    AddIdle,
    AddIdleOn {
        node: u8,
    },
    ReplaceIdle,
    ActivateIdle,
    TransferPrimary,
    TransferPrimaryTo {
        node: u8,
    },
    RetireSecondary,
    RemoveIdle,
    DeleteIdle {
        force: bool,
    },
    UpdateService {
        index: usize,
        service: fube_ra::protocol::Service,
    },
    Stop,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    id: u64,
    result: Result<Value>,
    status: Option<crate::status::ClusterStatus>,
}

fn decode_response(line: &[u8], id: u64) -> Result<(Result<Value>, crate::status::ClusterStatus)> {
    let response: Response = serde_json::from_slice(line).map_err(err)?;
    if response.id != id {
        return Err("FM management reply identity mismatch".into());
    }
    let status = match response.status {
        Some(status) => status,
        None => serde_json::from_value(
            response
                .result
                .as_ref()
                .ok()
                .and_then(|result| result.get("status"))
                .cloned()
                .ok_or("FM management reply lacks status")?,
        )
        .map_err(err)?,
    };
    Ok((response.result, status))
}

// A timed-out observer retains the original read-only exchange, including partial writes.
struct PendingStatus {
    id: u64,
    bytes: Vec<u8>,
    written: usize,
    deadline: Instant,
}

impl PendingStatus {
    fn new(id: u64) -> Result<Self> {
        Ok(Self {
            id,
            bytes: frame(
                &Request {
                    id,
                    operation: Operation::Status,
                },
                LIMIT,
            )?,
            written: 0,
            deadline: Instant::now() + Duration::from_secs(35),
        })
    }

    async fn advance(
        &mut self,
        input: &mut (impl AsyncWrite + Unpin),
        output: &mut Lines<impl AsyncBufRead + Unpin>,
    ) -> Result<(Result<Value>, crate::status::ClusterStatus)> {
        while self.written < self.bytes.len() {
            let count = input
                .write(&self.bytes[self.written..])
                .await
                .map_err(err)?;
            if count == 0 {
                return Err("FM management stdin closed during status write".into());
            }
            self.written += count;
        }
        input.flush().await.map_err(err)?;
        let line = output
            .next()
            .await?
            .ok_or("FM management stdout closed during status polling")?;
        decode_response(&line, self.id)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShutdownState {
    pub metadata: Vec<Option<Metadata>>,
    pub hosts: Vec<HostCleanupOwner>,
}

async fn operate(manager: &mut Manager, operation: Operation) -> Result<Value> {
    match operation {
        Operation::Attach { index, metadata } => {
            serde_json::to_value(manager.attach(index, metadata).await?).map_err(err)
        }
        Operation::NodeOwner { index } => serde_json::to_value(
            manager
                .children
                .get(index)
                .and_then(|child| child.metadata.clone())
                .ok_or("Node metadata is not admitted")?,
        )
        .map_err(err),
        Operation::NodeDown { index, receipt } => manager.node_down(index, receipt).await,
        Operation::PrepareNodeRestart { index } => {
            serde_json::to_value(manager.prepare_node_restart(index).await?).map_err(err)
        }
        Operation::FinishNodeRestart { index } => manager.finish_node_restart(index).await,
        Operation::FinishNodeInfrastructure { index } => manager.finish_node_infrastructure(index),
        Operation::Command { command } => manager.execute(command).await,
        Operation::Status => Ok(Value::Null),
        Operation::Generation => manager.begin_generation_handshake(),
        Operation::RepairPrimary => manager.repair_primary_replica().await,
        Operation::RepairSecondary { node } => manager.repair_secondary_replica(node).await,
        Operation::CancelRepair => manager.cancel_repair_build().await,
        Operation::DeleteCancelledRepair => manager.delete_cancelled_repair().await,
        Operation::ReplaceReplica { node } => manager.replace_replica_on(node).await,
        Operation::AddIdle => manager.add_idle_replica().await,
        Operation::AddIdleOn { node } => manager.add_idle_replica_on(node).await,
        Operation::ReplaceIdle => manager.replace_idle_replica().await,
        Operation::ActivateIdle => manager.activate_idle_replica().await,
        Operation::TransferPrimary => manager.transfer_primary().await,
        Operation::TransferPrimaryTo { node } => manager.transfer_primary_to(node).await,
        Operation::RetireSecondary => manager.retire_secondary_replica().await,
        Operation::RemoveIdle => manager.remove_idle_replica_via_fm().await,
        Operation::DeleteIdle { force } => {
            manager.delete_idle_replica_via_fm_with_force(force).await
        }
        Operation::UpdateService { index, service } => {
            manager.update_service(index, service).await?;
            Ok(Value::Null)
        }
        Operation::Stop => {
            manager.quiesce().await?;
            let metadata: Vec<_> = manager
                .children
                .iter()
                .map(|child| child.metadata.clone())
                .collect();
            let hosts = manager.host_cleanup_owners();
            manager.close().await?;
            serde_json::to_value(ShutdownState { metadata, hosts }).map_err(err)
        }
    }
}

/// Run one fresh, ephemeral FM. EOF closes FM sessions, never the Node processes.
pub async fn run(path: &Path) -> Result<()> {
    let manifest: Manifest = read_json(path)?;
    if manifest.schema_version != 1 || !(1..=64).contains(&manifest.launches.len()) {
        return Err("unsupported FM deployment manifest".into());
    }
    let launches = manifest
        .launches
        .iter()
        .map(|path| read_json(path))
        .collect::<Result<Vec<Launch>>>()?;
    let mut manager = Manager::new(launches)?;
    manager.ledger.recovery_mode = manifest.recovery_mode;
    let mut lines = Lines::new(BufReader::new(tokio::io::stdin()), LIMIT);
    let mut expected = 1u64;
    let serving = async {
        loop {
            manager.drive().await;
            // The line decoder retains partial input across these bounded polls.
            let line = match tokio::time::timeout(Duration::from_millis(5), lines.next()).await {
                Err(_) => continue,
                Ok(result) => result?,
            };
            let Some(line) = line else {
                return Ok(());
            };
            let request: Request = serde_json::from_slice(&line).map_err(err)?;
            if request.id != expected {
                return Err("out-of-order FM management request identity".into());
            }
            expected = expected
                .checked_add(1)
                .ok_or("FM management sequence exhausted")?;
            let stopping = matches!(request.operation, Operation::Stop);
            let result = operate(&mut manager, request.operation).await;
            let stopped = result.as_ref().map(|_| ()).map_err(Clone::clone);
            let contains_status = result
                .as_ref()
                .is_ok_and(|value| value["status"].is_object());
            let status = (!contains_status).then(|| manager.status_typed());
            let response = Response {
                id: request.id,
                result,
                status,
            };
            let bytes = frame(&response, LIMIT)?;
            let stdout = std::io::stdout();
            let mut writer = stdout.lock();
            writer
                .write_all(&bytes)
                .and_then(|()| writer.flush())
                .map_err(err)?;
            if stopping {
                return stopped;
            }
        }
    }
    .await;
    let closing = manager.close().await;
    match (serving, closing) {
        (Err(error), Err(closing)) => Err(format!("{error}; FM channel cleanup: {closing}")),
        (Err(error), _) | (_, Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub(crate) struct Client {
    pub(crate) process: Option<OwnedApp>,
    input: Option<tokio::process::ChildStdin>,
    output: Option<Lines<BufReader<tokio::process::ChildStdout>>>,
    next: u64,
    status: crate::status::ClusterStatus,
    error: Option<String>,
    pending_status: Option<PendingStatus>,
}
impl Client {
    pub(crate) fn new(status: crate::status::ClusterStatus) -> Self {
        Self {
            process: None,
            input: None,
            output: None,
            next: 1,
            status,
            error: None,
            pending_status: None,
        }
    }

    pub(crate) async fn start(&mut self, executable: &Path, manifest: &Path) -> Result<()> {
        let mut command = ProcessCommand::new(executable);
        command.arg("--config").arg(manifest);
        let (input, output) =
            OwnedApp::start_with_pipes(&mut self.process, command).map_err(err)?;
        self.input = Some(tokio::process::ChildStdin::from_std(input).map_err(err)?);
        self.output = Some(Lines::new(
            BufReader::new(tokio::process::ChildStdout::from_std(output).map_err(err)?),
            LIMIT,
        ));
        self.request(Operation::Status).await?;
        Ok(())
    }

    pub(crate) fn status_typed(&self) -> crate::status::ClusterStatus {
        let mut status = self.status.clone();
        status.details.insert(
            "fm".into(),
            json!({
                "pid": self.process.as_ref().map(OwnedApp::pid),
                "separate_process": true,
                "control_unknown": self.error.is_some(),
                "error": self.error,
            }),
        );
        if self.error.is_some() {
            for node in &mut status.nodes {
                node.observed = crate::Observed::Unknown;
                node.endpoint = None;
                node.link_alive = false;
            }
            for operation in &mut status.operations {
                if operation.state == crate::OperationState::Pending {
                    operation.state = crate::OperationState::Unknown;
                    operation.error = Some(json!(self.error));
                }
            }
        }
        status
    }

    pub(crate) fn cleanup_status(&self) -> Value {
        let process = self.process.as_ref().map(OwnedApp::status);
        json!({
            "pid": process.map(|status| status.pid),
            "reaped": process.is_some_and(|status| status.reaped),
            "group_drained": process.is_some_and(|status| status.group_drained),
            "exit_observed": process.is_some_and(|status| status.exit_observed),
            "forced": process.is_some_and(|status| status.cleanup_forced),
            "control_unknown": self.error.is_some(),
            "error": self.error,
        })
    }

    pub(crate) async fn request(&mut self, operation: Operation) -> Result<Value> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        let deadline = Instant::now() + Duration::from_secs(35);
        if self.pending_status.is_some() {
            let outcome = self.poll_status_until(deadline).await?;
            let Some(outcome) = outcome else {
                return Err(self.record_error("FM management exchange deadline".into()));
            };
            if matches!(operation, Operation::Status) {
                return Ok(outcome);
            }
        }
        let result = tokio::time::timeout_at(deadline, self.exchange(operation))
            .await
            .map_err(|_| "FM management exchange deadline".to_string())
            .and_then(|result| result);
        match result {
            Ok((outcome, status)) => {
                self.status = status;
                outcome
            }
            Err(error) => Err(self.record_error(error)),
        }
    }

    fn record_error(&mut self, error: String) -> String {
        if self.error.is_none() {
            eprintln!("FM control unavailable: {error}");
            self.error = Some(error);
        }
        self.error.as_ref().unwrap().clone()
    }

    pub(crate) async fn poll_status_until(&mut self, deadline: Instant) -> Result<Option<Value>> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        if self.pending_status.is_none() {
            let process = self.process.as_mut().ok_or("FM process not started")?;
            if let Err(error) = process.check_alive().map_err(err) {
                return Err(self.record_error(error));
            }
            let pending = PendingStatus::new(self.next)?;
            self.next = self
                .next
                .checked_add(1)
                .ok_or("FM management sequence exhausted")?;
            self.pending_status = Some(pending);
        }
        let protocol_deadline = self.pending_status.as_ref().unwrap().deadline;
        let result = tokio::time::timeout_at(deadline.min(protocol_deadline), async {
            let pending = self.pending_status.as_mut().unwrap();
            let input = self.input.as_mut().ok_or("FM protocol stdin absent")?;
            let output = self.output.as_mut().ok_or("FM protocol stdout absent")?;
            pending.advance(input, output).await
        })
        .await;
        match result {
            Err(_) if Instant::now() >= protocol_deadline => {
                Err(self.record_error("FM management exchange deadline".into()))
            }
            Err(_) => Ok(None),
            Ok(Err(error)) => Err(self.record_error(error)),
            Ok(Ok((outcome, status))) => {
                self.pending_status.take();
                self.status = status;
                outcome.map(Some)
            }
        }
    }

    async fn exchange(
        &mut self,
        operation: Operation,
    ) -> Result<(Result<Value>, crate::status::ClusterStatus)> {
        let process = self.process.as_mut().ok_or("FM process not started")?;
        process.check_alive().map_err(err)?;
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or("FM management sequence exhausted")?;
        let input = self.input.as_mut().ok_or("FM protocol stdin absent")?;
        input
            .write_all(&frame(&Request { id, operation }, LIMIT)?)
            .await
            .map_err(err)?;
        input.flush().await.map_err(err)?;
        let line = self
            .output
            .as_mut()
            .ok_or("FM protocol stdout absent")?
            .next()
            .await?
            .ok_or_else(|| {
                format!(
                    "FM management stdout closed; stderr={:?}",
                    process.logs().iter().rev().take(8).collect::<Vec<_>>()
                )
            })?;
        decode_response(&line, id)
    }

    pub(crate) async fn stop(&mut self) -> Result<ShutdownState> {
        let response = self.request(Operation::Stop).await;
        self.input.take();
        self.output.take();
        let process = self.process.as_mut().ok_or("FM process not started")?;
        let exit = process
            .wait_exit(std::time::Instant::now() + Duration::from_secs(5))
            .map_err(err);
        let cleanup = process.cleanup().map_err(err);
        let status = process.status();
        let value = response?;
        exit?;
        cleanup?;
        if !status.reaped || !status.group_drained || status.cleanup_forced {
            return Err("FM process did not exit and clean up normally".into());
        }
        serde_json::from_value(value).map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_fixture() -> crate::status::ClusterStatus {
        serde_json::from_value(json!({
            "nodes":[], "operations":[],
            "configuration":{"state":"absent","ready":false},
            "repair":{"state":"absent","ready":false},
            "idle_build":{"state":"absent","ready":false},
            "idle_removal":{"state":"absent","pending_remove":false,"removed":false},
            "idle_deletion":{"state":"absent","destination_deleted":false,"disposed":false,
                "never_created":false,"pre_open_cancelled":false,"failed_registration":false,
                "failed_open":false,"complete":false},
            "generation_handshake":null
        }))
        .unwrap()
    }

    #[test]
    fn management_rejects_malformed_typed_control_facts() {
        let mut status = status_fixture();
        status.operations.push(crate::OperationOutcome::new(
            "build",
            crate::OperationState::Pending,
        ));
        let response = serde_json::to_value(Response {
            id: 1,
            result: Ok(Value::Null),
            status: Some(status),
        })
        .unwrap();
        for (field, invalid) in [("state", json!("typo")), ("phase", json!("typo"))] {
            let mut response = response.clone();
            response["status"]["operations"][0][field] = invalid;
            assert!(decode_response(&serde_json::to_vec(&response).unwrap(), 1).is_err());
        }
        let mut invalid = response.clone();
        invalid["status"]["configuration"]["ready"] = json!("true");
        assert!(decode_response(&serde_json::to_vec(&invalid).unwrap(), 1).is_err());
        let mut missing = response;
        missing["status"]["repair"]
            .as_object_mut()
            .unwrap()
            .remove("state");
        assert!(decode_response(&serde_json::to_vec(&missing).unwrap(), 1).is_err());
    }

    #[test]
    fn manifests_require_explicit_manual_policy_and_reject_legacy() {
        let mut value = serde_json::json!({
            "schema_version":1,"launches":[],"recovery_mode":"manual"
        });
        assert!(serde_json::from_value::<Manifest>(value.clone()).is_ok());
        value["recovery_mode"] = serde_json::json!("legacy");
        assert!(serde_json::from_value::<Manifest>(value.clone())
            .err()
            .expect("Legacy policy must be rejected")
            .to_string()
            .contains("unknown variant"));
        value.as_object_mut().unwrap().remove("recovery_mode");
        assert!(serde_json::from_value::<Manifest>(value)
            .err()
            .expect("Missing policy must be rejected")
            .to_string()
            .contains("missing field"));
    }

    #[tokio::test]
    async fn status_observation_deadlines_preserve_partial_writes_reads_and_request_identity() {
        use tokio::io::AsyncReadExt;
        let (mut input, mut request_reader) = tokio::io::duplex(8);
        let (mut response_writer, response_reader) = tokio::io::duplex(4096);
        let mut output = Lines::new(BufReader::new(response_reader), LIMIT);
        let mut pending = PendingStatus::new(1).unwrap();
        let expected = pending.bytes.clone();
        assert!(tokio::time::timeout(
            Duration::from_millis(1),
            pending.advance(&mut input, &mut output)
        )
        .await
        .is_err());
        assert!(pending.written > 0 && pending.written < expected.len());
        let mut status = status_fixture();
        status.operations.push(crate::OperationOutcome::new(
            "build",
            crate::OperationState::Pending,
        ));
        let response = frame(
            &Response {
                id: 1,
                result: Ok(Value::Null),
                status: Some(status),
            },
            LIMIT,
        )
        .unwrap();
        let delivery = async {
            let mut request = vec![0; expected.len()];
            request_reader.read_exact(&mut request).await.unwrap();
            assert_eq!(request, expected);
            response_writer.write_all(&response[..10]).await.unwrap();
            response_writer
        };
        let mut response_writer = {
            let progress = pending.advance(&mut input, &mut output);
            tokio::pin!(progress);
            tokio::select! {
                result = &mut progress => panic!("status reply completed before its tail: {result:?}"),
                writer = delivery => writer,
            }
        };
        assert!(tokio::time::timeout(
            Duration::from_millis(1),
            pending.advance(&mut input, &mut output)
        )
        .await
        .is_err());
        response_writer.write_all(&response[10..]).await.unwrap();
        let (result, status) = pending.advance(&mut input, &mut output).await.unwrap();
        assert_eq!(result.unwrap(), Value::Null);
        assert_eq!(status.operations[0].state, crate::OperationState::Pending);
        let mut extra = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(1), request_reader.read(&mut extra))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn pending_status_rejects_a_reply_from_another_exchange() {
        let (mut input, _request_reader) = tokio::io::duplex(128);
        let (mut response_writer, response_reader) = tokio::io::duplex(4096);
        let mut output = Lines::new(BufReader::new(response_reader), LIMIT);
        response_writer
            .write_all(
                &frame(
                    &Response {
                        id: 2,
                        result: Ok(Value::Null),
                        status: Some(status_fixture()),
                    },
                    LIMIT,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let error = PendingStatus::new(1)
            .unwrap()
            .advance(&mut input, &mut output)
            .await
            .unwrap_err();
        assert!(error.contains("identity mismatch"), "{error}");
    }

    #[tokio::test]
    async fn management_frames_retain_partial_input_and_reject_truncation_or_overflow() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, reader) = tokio::io::duplex(128);
        let mut lines = Lines::new(BufReader::new(reader), LIMIT);
        writer.write_all(b"{\"id\":").await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(1), lines.next())
            .await
            .is_err());
        writer.write_all(b"1}\n").await.unwrap();
        assert_eq!(lines.next().await.unwrap().unwrap(), b"{\"id\":1}\n");
        writer.write_all(b"unfinished").await.unwrap();
        drop(writer);
        assert!(lines.next().await.unwrap_err().contains("truncated"));
        let bytes = vec![b'x'; LIMIT + 1];
        let mut lines = Lines::new(BufReader::new(bytes.as_slice()), LIMIT);
        assert!(lines.next().await.unwrap_err().contains("65536 bytes"));
    }
}
