use crate::{manager::Manager, profile::*, Command};
use fube_local_host::process::OwnedApp;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::Duration,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Private same-host deployment manifest, not an SF reliability message.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub schema_version: u32,
    pub launches: Vec<PathBuf>,
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
    status: Option<Value>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShutdownState {
    pub metadata: Vec<Option<Metadata>>,
    pub hosts: Vec<HostCleanupOwner>,
}

struct Lines<R> {
    input: R,
    bytes: Vec<u8>,
}
impl<R: AsyncBufRead + Unpin> Lines<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            bytes: Vec::new(),
        }
    }

    async fn next(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let chunk = self.input.fill_buf().await.map_err(err)?;
            if chunk.is_empty() {
                return if self.bytes.is_empty() {
                    Ok(None)
                } else {
                    Err("truncated FM management frame".into())
                };
            }
            let newline = chunk.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(chunk.len(), |index| index + 1);
            if self.bytes.len() + count > LIMIT {
                return Err("FM management frame exceeds 64 KiB".into());
            }
            self.bytes.extend_from_slice(&chunk[..count]);
            self.input.consume(count);
            if newline.is_some() {
                return Ok(Some(std::mem::take(&mut self.bytes)));
            }
        }
    }
}

fn frame(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(err)?;
    if bytes.len() >= LIMIT {
        return Err("FM management frame exceeds 64 KiB".into());
    }
    bytes.push(b'\n');
    Ok(bytes)
}

async fn operate(manager: &mut Manager, operation: Operation) -> Result<Value> {
    match operation {
        Operation::Attach { index, metadata } => {
            serde_json::to_value(manager.attach(index, metadata).await?).map_err(err)
        }
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
    let mut lines = Lines::new(BufReader::new(tokio::io::stdin()));
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
            let status = (!contains_status).then(|| manager.status());
            let response = Response {
                id: request.id,
                result,
                status,
            };
            let bytes = frame(&response)?;
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
    status: Value,
    error: Option<String>,
}
impl Client {
    pub(crate) fn new() -> Self {
        Self {
            process: None,
            input: None,
            output: None,
            next: 1,
            status: Value::Null,
            error: None,
        }
    }

    pub(crate) async fn start(&mut self, executable: &Path, manifest: &Path) -> Result<()> {
        let mut command = ProcessCommand::new(executable);
        command.arg("--config").arg(manifest);
        let (input, output) =
            OwnedApp::start_with_pipes(&mut self.process, command).map_err(err)?;
        self.input = Some(tokio::process::ChildStdin::from_std(input).map_err(err)?);
        self.output = Some(Lines::new(BufReader::new(
            tokio::process::ChildStdout::from_std(output).map_err(err)?,
        )));
        self.request(Operation::Status).await?;
        Ok(())
    }

    pub(crate) fn status(&self) -> Value {
        let mut status = self.status.clone();
        status["fm"] = json!({
            "pid": self.process.as_ref().map(OwnedApp::pid),
            "separate_process": true,
            "control_unknown": self.error.is_some(),
            "error": self.error,
        });
        if self.error.is_some() {
            if let Some(nodes) = status["nodes"].as_array_mut() {
                for node in nodes {
                    node["observed"] = json!("unknown");
                    node["endpoint"] = Value::Null;
                    node["link_alive"] = json!(false);
                }
            }
            if let Some(operations) = status["operations"].as_array_mut() {
                for operation in operations {
                    if operation["state"] == "pending" {
                        operation["state"] = json!("unknown");
                        operation["error"] = json!(self.error);
                    }
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
        let result = tokio::time::timeout(Duration::from_secs(35), self.exchange(operation))
            .await
            .map_err(|_| "FM management exchange deadline".to_string())
            .and_then(|result| result);
        match result {
            Ok((outcome, status)) => {
                self.status = status;
                outcome
            }
            Err(error) => {
                eprintln!("FM control unavailable: {error}");
                self.error = Some(error.clone());
                Err(error)
            }
        }
    }

    async fn exchange(&mut self, operation: Operation) -> Result<(Result<Value>, Value)> {
        let process = self.process.as_mut().ok_or("FM process not started")?;
        process.check_alive().map_err(err)?;
        let id = self.next;
        self.next = id
            .checked_add(1)
            .ok_or("FM management sequence exhausted")?;
        let input = self.input.as_mut().ok_or("FM protocol stdin absent")?;
        input
            .write_all(&frame(&Request { id, operation })?)
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
        let response: Response = serde_json::from_slice(&line).map_err(err)?;
        if response.id != id {
            return Err("FM management reply identity mismatch".into());
        }
        let status = response
            .status
            .or_else(|| {
                response
                    .result
                    .as_ref()
                    .ok()
                    .and_then(|result| result.get("status"))
                    .cloned()
            })
            .filter(Value::is_object)
            .ok_or("FM management reply lacks status")?;
        Ok((response.result, status))
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

    #[tokio::test]
    async fn management_frames_retain_partial_input_and_reject_truncation_or_overflow() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, reader) = tokio::io::duplex(128);
        let mut lines = Lines::new(BufReader::new(reader));
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
        let mut lines = Lines::new(BufReader::new(bytes.as_slice()));
        assert!(lines.next().await.unwrap_err().contains("64 KiB"));
    }
}
