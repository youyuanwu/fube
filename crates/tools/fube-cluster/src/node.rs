use crate::{
    profile::*,
    wire::{self, Link},
};
use fube_hosting::local::{Activation, NodeContext, Options};
use fube_node::local::{FmWork, LocalDisposition, Node};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::{MemoryBackend, Settings, Store};
use std::{collections::VecDeque, fs, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    time::Instant,
};

struct Escrow {
    original: ra::Dispatched,
    due: Instant,
    submitted: Option<u64>,
    sent: bool,
}
#[derive(Default)]
struct Adapter {
    escrow: VecDeque<Escrow>,
    replies: VecDeque<ra::Dispatched>,
    error: Option<String>,
    notification_pending: Option<ServiceTypeNotification>,
    report_pending: Option<ReplicaUp>,
    acknowledgements: Vec<(u64, SemanticAck)>,
}
enum SemanticAck {
    Notification(ServiceTypeNotification),
    Report(ReplicaUp),
}
fn ticks(ticks: i64) -> Duration {
    Duration::from_nanos((ticks.max(0) as u64).saturating_mul(100))
}
impl Adapter {
    fn capture(&mut self, node: &mut Node) {
        for dispatch in node.take_external_up_to(32 - self.escrow.len()) {
            let delay = match &dispatch.effect {
                ra::Effect::RequestServiceTypeWork { after_ticks, .. }
                | ra::Effect::ArmServiceTypeRetry { after_ticks, .. } => ticks(*after_ticks),
                ra::Effect::RequestReportRetry => Duration::from_secs(15),
                _ => Duration::ZERO,
            };
            self.escrow.push_back(Escrow {
                original: dispatch,
                due: Instant::now() + delay,
                submitted: None,
                sent: false,
            });
        }
        // Receipt capacity is separate from original external-effect escrow.
        // Never transfer a whole receipt batch unless all 32 slots are free.
        if self.replies.is_empty() {
            for receipt in node.take_effect_receipts() {
                if matches!(receipt.disposition, LocalDisposition::LocalResponse)
                    && matches!(
                        receipt.dispatch.effect,
                        ra::Effect::Reply(_) | ra::Effect::RemoveReply(_)
                    )
                {
                    self.replies.push_back(receipt.dispatch);
                }
            }
        }
    }
    fn commands(&mut self, node: &mut Node) -> Result<()> {
        for receipt in node.take_command_receipts() {
            let disposition = receipt.result.map_err(err)?;
            if let Some(index) = self
                .acknowledgements
                .iter()
                .position(|(id, _)| *id == receipt.id)
            {
                let (_, acknowledgement) = self.acknowledgements.remove(index);
                if disposition == ra::Disposition::Applied {
                    match acknowledgement {
                        SemanticAck::Notification(body) => {
                            if let Some(pending) = &mut self.notification_pending {
                                pending.infos.0.retain(|info| !body.infos.0.contains(info));
                                if pending.infos.0.is_empty() {
                                    self.notification_pending = None;
                                }
                            }
                        }
                        SemanticAck::Report(body) => {
                            if self.report_pending.as_ref() == Some(&body) {
                                self.report_pending = None;
                            }
                        }
                    }
                }
            }
            if let Some(index) = self
                .escrow
                .iter()
                .position(|item| item.submitted == Some(receipt.id))
            {
                self.escrow.remove(index);
            }
        }
        Ok(())
    }
    async fn advance(&mut self, node: &mut Node, link: Option<&Link>) -> Result<()> {
        self.capture(node);
        self.commands(node)?;
        if let Some(link) = link {
            if let Some(reply) = self.replies.front() {
                let message = match &reply.effect {
                    ra::Effect::Reply(body) => add_instance_reply_message(body, Owner::Fm),
                    ra::Effect::RemoveReply(body) => remove_instance_reply_message(body, Owner::Fm),
                    _ => return Err("unimplemented semantic reply retained".into()),
                }
                .map_err(err)?;
                link.send(message).await?;
                self.replies.pop_front();
            }
        }
        // Cancel only the exact original armed timer, never notification
        // semantic pending state. Cancellation and work completion differ.
        if let Some(index) = self.escrow.iter().position(|item| {
            matches!(
                item.original.effect,
                ra::Effect::CancelServiceTypeRetry { .. }
            )
        }) {
            let timer = match self.escrow[index].original.effect {
                ra::Effect::CancelServiceTypeRetry { timer } => timer,
                _ => unreachable!(),
            };
            self.escrow.remove(index);
            if let Some(armed) = self.escrow.iter().position(|item|
                matches!(item.original.effect, ra::Effect::ArmServiceTypeRetry { timer: id, .. } if id == timer)
                && item.submitted.is_none())
            {
                self.escrow.remove(armed);
            }
        }
        for index in 0..self.escrow.len() {
            let item = &self.escrow[index];
            if item.submitted.is_some() || item.due > Instant::now() {
                continue;
            }
            let effect = item.original.effect.clone();
            let sent = item.sent;
            let entity = item.original.entity;
            let work = match &effect {
                ra::Effect::RequestServiceTypeWork { work, .. } => {
                    Some(FmWork::CaptureServiceTypeWork(*work))
                }
                ra::Effect::ArmServiceTypeRetry { timer, after_ticks } => {
                    if *after_ticks == i64::MAX {
                        continue;
                    }
                    Some(FmWork::ServiceTypeRetryTimer(*timer))
                }
                ra::Effect::RequestReportRetry => Some(FmWork::RetryReport(PartitionId {
                    guid: fube_transport::serialization::serde::Guid(entity),
                })),
                ra::Effect::ServiceTypeWorkCaptured { work, body } => {
                    if !sent {
                        if let Some(body) = body {
                            let Some(link) = link else {
                                continue;
                            };
                            if self
                                .notification_pending
                                .as_ref()
                                .is_some_and(|prior| prior != body.as_ref())
                            {
                                return Err("unsupported replaced notification remains semantically pending".into());
                            }
                            self.notification_pending = Some((**body).clone());
                            link.send(service_type_notification_message(body).map_err(err)?)
                                .await?;
                        }
                        self.escrow[index].sent = true;
                    }
                    Some(FmWork::CompleteServiceTypeWork(*work))
                }
                ra::Effect::ReplicaUp(body) => {
                    let Some(link) = link else {
                        continue;
                    };
                    if self
                        .report_pending
                        .as_ref()
                        .is_some_and(|prior| prior != body.as_ref())
                    {
                        return Err(
                            "unsupported replaced report remains semantically pending".into()
                        );
                    }
                    self.report_pending = Some((**body).clone());
                    link.send(replica_up_message(body, Owner::Fm).map_err(err)?)
                        .await?;
                    self.escrow.remove(index);
                    return Ok(());
                }
                _ => continue, // Unsupported original work remains retained.
            };
            if let Some(work) = work {
                match node.fm_work(work) {
                    Ok(id) => self.escrow[index].submitted = Some(id),
                    Err(fube_node::local::Error::Capacity) => return Ok(()),
                    Err(error) => return Err(err(error)),
                }
            }
            return Ok(());
        }
        Ok(())
    }
    fn pending(&self) -> usize {
        self.escrow.len() + self.replies.len() + self.acknowledgements.len()
    }
    fn semantic_pending(&self) -> usize {
        self.notification_pending
            .as_ref()
            .map_or(0, |body| body.infos.0.len())
            + usize::from(self.report_pending.is_some())
    }
}

async fn control_line(input: &mut BufReader<tokio::io::Stdin>) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let buffer = input.fill_buf().await.map_err(err)?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err("truncated owner control".into())
            };
        }
        let count = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |i| i + 1);
        if line.len() + count > 256 {
            return Err("owner control capacity".into());
        }
        line.extend_from_slice(&buffer[..count]);
        input.consume(count);
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
        }
    }
}

pub async fn run(config: &Path) -> Result<()> {
    let launch: Launch = read_json(config)?;
    launch.validate()?;
    let result = run_owned(&launch).await;
    if result.is_err()
        && !launch.directory.join("activation-acquired.json").exists()
        && unsafe { libc::getppid() } as u32 == launch.parent_pid
    {
        // The original child can prove it never constructed an Activation.
        // A dead/missing child cannot manufacture this negative acquisition
        // receipt; the foreground still independently reaps its owned process.
        write_json(
            &launch.directory.join("cleanup.json"),
            &CleanupReceipt {
                deployment: launch.deployment.clone(),
                session: launch.session.clone(),
                node: launch.node.clone(),
                node_pid: std::process::id(),
                app_pid: None,
                runtime_root: None,
                activation_acquired: false,
                original_activation_cleaned: false,
                executor_closed: false,
                store_closed: false,
                external_pending: 0,
                adapter_pending: 0,
                semantic_pending: 0,
            },
        )?;
    }
    result
}
async fn run_owned(launch: &Launch) -> Result<()> {
    if unsafe { libc::getppid() } as u32 != launch.parent_pid {
        return Err("internal node requires original launcher parent".into());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
    let address = listener.local_addr().map_err(err)?;
    let mut metadata = Metadata {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        pid: std::process::id(),
        address,
        profile: None,
    };
    write_json(&launch.directory.join("listener.json"), &metadata)?;
    let mut control = BufReader::with_capacity(256, tokio::io::stdin());
    let admission = tokio::time::timeout(Duration::from_secs(20), control_line(&mut control))
        .await
        .map_err(err)??
        .ok_or("owner vanished before admission")?;
    if admission != format!("admit {} {}\n", launch.session, launch.capability).as_bytes() {
        return Err("wrong one-use local launch capability".into());
    }
    let (stream, _) = tokio::time::timeout(Duration::from_secs(20), listener.accept())
        .await
        .map_err(err)?
        .map_err(err)?;
    drop(listener); // Exactly one original session; no reconnect listener.
    let channel = fube_transport::accept_unsecured_loopback(
        stream,
        wire::composition(false).map_err(err)?,
        wire::limits(),
        wire::deadline(),
    )
    .await
    .map_err(err)?;
    let link = Link::new(
        channel,
        launch.node.clone(),
        launch.fm.clone(),
        launch.generation.clone(),
    )?;
    let fabric = FabricVersionInstance::native_default();
    let up = NodeUp::fresh_stateless(
        launch.name(),
        "fube-cluster-local".into(),
        address,
        fabric.clone(),
    )
    .map_err(err)?;
    let reply = link.request(node_up_message(&up).map_err(err)?).await?;
    let (ack, generation) = decode_node_up_ack(&reply).map_err(err)?;
    ack.validate_fresh_activated(&fabric).map_err(err)?;
    if generation != launch.generation {
        return Err("foreign/stale NodeUpAck generation".into());
    }
    let final_upload = ReplicaUp {
        last: true,
        ..ReplicaUp::default()
    };
    link.send(replica_up_message(&final_upload, Owner::Fm).map_err(err)?)
        .await?;
    let mut incoming = link
        .receive(wire::deadline())
        .await?
        .ok_or("final upload reply timeout")?;
    wire::predecode(&incoming.message)?;
    let (uploaded, generation) = decode_replica_up_reply(&incoming.message).map_err(err)?;
    if uploaded != final_upload || generation != launch.generation {
        link.channel.dispose();
        return Err("foreign/stale/nonfinal bootstrap upload reply".into());
    }
    if let Some(handoff) = incoming.handoff.as_mut() {
        handoff.acknowledge();
    }
    // No RA/Hosting/process authority exists until BOTH native semantic
    // bootstrap exchanges completed for this original session.
    private_dir(&launch.directory.join("hosting"))?;
    let mut options = Options::new(launch.directory.join("hosting"));
    options.node = Some(NodeContext {
        id: node_hex(&launch.node),
        instance: launch.node.instance,
        name: launch.name(),
    });
    let store = Arc::new(
        Store::new(Box::new(MemoryBackend::new()), Settings::default())
            .await
            .map_err(err)?,
    );
    let admission = ra::Admission {
        local_node: launch.node.clone(),
        fm_source: launch.fm.clone(),
        generation: launch.generation.number.clone(),
        owner_ready: true,
        node_open: true,
        node_activated: true,
        fabric_upgrading: false,
    };
    write_json(
        &launch.directory.join("activation-acquired.json"),
        &serde_json::json!({"deployment":launch.deployment,"session":launch.session,
            "node_pid":std::process::id()}),
    )?;
    let mut node = Node::new(
        Activation::new(options).map_err(err)?,
        store.clone(),
        admission,
        ra::Settings {
            max_outstanding: 32,
            max_service_types: 1,
            notification_batch_count: 1,
            notification_batch_bytes: LIMIT,
            max_queued_bytes: 2 * 1024 * 1024,
            ..ra::Settings::default()
        },
    )
    .map_err(err)?;
    let mut adapter = Adapter::default();
    let started = node.start(&launch.package, APPLICATION, &[]).map_err(err);
    let startup = if started.is_ok() {
        actual_profile(&node).and_then(|profile| {
            profile.validate(launch)?;
            metadata.profile = Some(profile);
            write_json(&launch.directory.join("profile.json"), &metadata)
        })
    } else {
        started
    };
    let mut link = Some(link);
    let mut failure = startup.err();
    let mut next_observation = Instant::now();
    if failure.is_none() {
        loop {
            // Owned stop remains independent of FM connectivity. Link loss is
            // not permission to stop an existing app or fake lifecycle replies.
            tokio::select! {
                control = control_line(&mut control) => {
                    match control {
                        Ok(Some(line)) if line == format!("stop {}\n", launch.capability).as_bytes() => {},
                        Ok(None) => { failure = Some("original owner control closed".into()); },
                        other => { failure = Some(format!("invalid original owner control: {other:?}")); },
                    }
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
            if let Err(error) = node.drive() {
                failure = Some(err(error));
                break;
            }
            if let Err(error) = adapter.advance(&mut node, link.as_ref()).await {
                adapter.error = Some(error);
                if let Some(old) = link.take() {
                    let _ = old.close().await;
                }
            }
            if let Some(current) = &link {
                if adapter.acknowledgements.len() == 32 {
                    continue;
                }
                match current
                    .receive(Instant::now() + Duration::from_millis(1))
                    .await
                {
                    Ok(Some(mut incoming)) => {
                        match receive_command(launch, &mut node, &incoming.message) {
                            Ok(acknowledgement) => {
                                if let Some(acknowledgement) = acknowledgement {
                                    adapter.acknowledgements.push(acknowledgement);
                                }
                                if let Some(handoff) = incoming.handoff.as_mut() {
                                    handoff.acknowledge();
                                }
                            }
                            Err(error) => {
                                adapter.error = Some(error);
                                current.channel.dispose();
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        adapter.error = Some(error);
                        if let Some(old) = link.take() {
                            let _ = old.close().await;
                        }
                    }
                }
            }
            if next_observation <= Instant::now() {
                let observation = serde_json::json!({
                    "deployment":launch.deployment, "session":launch.session,
                    "node_pid":std::process::id(), "external_pending":node.external_pending().len(),
                    "adapter_pending":adapter.pending(), "commands":node.pending_commands(),
                    "semantic_pending":adapter.semantic_pending(),
                    "staged_status":format!("{:?}", node.pending_status()),
                    "error":adapter.error, "link_alive":link.is_some(),
                });
                if let Err(error) =
                    write_json(&launch.directory.join("observation.json"), &observation)
                {
                    failure = Some(error);
                    break;
                }
                next_observation = Instant::now() + Duration::from_millis(250);
            }
        }
    }
    if let Some(link) = link.take() {
        let _ = link.close().await;
    }
    let receipt = cleanup(launch, &mut node, &store, &mut adapter).await?;
    write_json(&launch.directory.join("cleanup.json"), &receipt)?;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

fn actual_profile(node: &Node) -> Result<ActualProfile> {
    let activation = node.activation();
    let identity = activation
        .identity()
        .ok_or("missing generated activation identity")?;
    let premise = activation
        .premise()
        .ok_or("missing generated package premise")?;
    let declared = activation
        .declared_type()
        .ok_or("missing actual declared type")?;
    let version = &premise.version;
    Ok(ActualProfile {
        service: premise.service_name.clone(),
        service_type: ServiceType {
            package: PackageId {
                application: ApplicationId {
                    type_name: declared.package.application.type_name,
                    number: declared.package.application.number,
                },
                name: declared.package.name,
            },
            name: declared.name,
        },
        version: VersionInstance {
            version: PackageVersion {
                application: ApplicationVersion {
                    value: Rollout {
                        major: version.application.major,
                        minor: version.application.minor,
                    },
                },
                value: Rollout {
                    major: version.package.major,
                    minor: version.package.minor,
                },
            },
            instance: version.instance,
        },
        code_package: identity.code.id.name.clone(),
        app_pid: activation
            .process_status()
            .ok_or("missing original app process")?
            .pid,
        runtime_root: activation
            .root()
            .ok_or("missing original deployment")?
            .to_owned(),
    })
}
fn receive_command(
    launch: &Launch,
    node: &mut Node,
    message: &fube_transport::message::Message,
) -> Result<Option<(u64, SemanticAck)>> {
    wire::predecode(message)?;
    match message.properties().map_err(err)?.action.as_str() {
        "AddInstance" => {
            let (body, generation) = decode_add_instance(message).map_err(err)?;
            node.add_from_fm(body, &launch.fm, &generation)
                .map_err(err)?;
        }
        "RemoveInstance" => {
            let (body, generation) = decode_remove_instance(message).map_err(err)?;
            node.fm_work(FmWork::Remove(
                Box::new(body),
                launch.fm.clone(),
                generation,
            ))
            .map_err(err)?;
        }
        "ReplicaUpReply" => {
            let (body, generation) = decode_replica_up_reply(message).map_err(err)?;
            let id = node
                .fm_work(FmWork::ReplicaUpReply(
                    body.clone(),
                    launch.fm.clone(),
                    generation,
                ))
                .map_err(err)?;
            return Ok(Some((id, SemanticAck::Report(body))));
        }
        "ServiceTypeNotificationReply" => {
            let (body, generation) =
                decode_service_type_notification_reply(message).map_err(err)?;
            let id = node
                .fm_work(FmWork::ServiceTypeReply(
                    body.clone(),
                    launch.fm.clone(),
                    generation,
                ))
                .map_err(err)?;
            return Ok(Some((id, SemanticAck::Notification(body))));
        }
        _ => return Err("unsupported command".into()),
    }
    Ok(None)
}
async fn cleanup(
    launch: &Launch,
    node: &mut Node,
    store: &Store,
    adapter: &mut Adapter,
) -> Result<CleanupReceipt> {
    let app_pid = node.activation().process_status().map(|status| status.pid);
    let root = node.activation().root().map(Path::to_path_buf);
    let end = Instant::now() + Duration::from_secs(20);
    let partition = PartitionId {
        guid: fube_transport::serialization::serde::Guid(
            uuid::Uuid::parse_str(&launch.partition)
                .map_err(err)?
                .to_bytes_le(),
        ),
    };
    // Orderly owner shutdown disposes through real RAP while it is available.
    // This local Drop is cleanup authority, not a forged native Remove reply.
    let graceful = Instant::now() + Duration::from_secs(8);
    let mut drop_requested = false;
    loop {
        match node.snapshot(partition.guid.0).committed {
            Some(state) if state.phase == ra::Phase::Closed && node.pending_commands() == 0 => {
                break
            }
            Some(state) if !drop_requested && state.phase != ra::Phase::Closing => {
                node.close(partition.clone(), ra::CloseMode::Drop)
                    .map_err(err)?;
                drop_requested = true;
            }
            None if node.pending_commands() == 0 => break,
            _ => {}
        }
        if Instant::now() >= graceful {
            break;
        }
        node.drive().map_err(err)?;
        adapter.advance(node, None).await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    loop {
        let terminated = match node.terminate() {
            Ok(value) => value,
            Err(fube_node::local::Error::Activation(fube_hosting::local::LocalError::Busy)) => {
                false
            }
            Err(error) => return Err(err(error)),
        };
        if terminated && node.quiescent() {
            break;
        }
        node.drive().map_err(err)?;
        adapter.advance(node, None).await?;
        if node.activation().cleanup_status().logical_retired
            && node.pending_commands() == 0
            && node
                .snapshot(partition.guid.0)
                .committed
                .is_some_and(|state| {
                    state.phase == ra::Phase::Opening
                        && state.runtime.is_none()
                        && state.usage.is_some()
                })
        {
            node.close(partition.clone(), ra::CloseMode::Drop)
                .map_err(err)?;
        }
        if terminated && node.quiescent() {
            break;
        }
        if Instant::now() >= end {
            return Err("original node cleanup incomplete".into());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    node.close_executor(Duration::from_secs(3))
        .await
        .map_err(err)?;
    let closed = store.close().await.map_err(err)?;
    if !closed.is_clean() {
        return Err("store cleanup incomplete".into());
    }
    let status = node.activation().cleanup_status();
    let process = node.activation().process_status();
    if !status.directory_released
        || !status.logical_retired
        || process.is_some_and(|p| !p.reaped || !p.group_drained)
        || root.as_ref().is_some_and(|path| path.exists())
        || app_pid.is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists())
        || fs::read_dir(launch.directory.join("hosting"))
            .map_err(err)?
            .next()
            .is_some()
    {
        return Err("original Activation physical cleanup not observed".into());
    }
    Ok(CleanupReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        node_pid: std::process::id(),
        app_pid,
        runtime_root: root,
        activation_acquired: true,
        original_activation_cleaned: true,
        executor_closed: true,
        store_closed: true,
        external_pending: node.external_pending().len(),
        adapter_pending: adapter.pending(),
        semantic_pending: adapter.semantic_pending(),
    })
}
