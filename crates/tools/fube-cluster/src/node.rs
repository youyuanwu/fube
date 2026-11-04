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
const REPORT_RETRY: Duration = Duration::from_secs(15);

fn report_cleared(node: &Node, entity: ra::EntityId) -> bool {
    let snapshot = node.snapshot(entity);
    !snapshot.fenced
        && snapshot
            .committed
            .is_some_and(|state| state.fm_stage == ra::FmStage::None || state.local_deleted)
}

fn current_report(node: &Node, entity: ra::EntityId, body: &ReplicaUp) -> bool {
    let snapshot = node.snapshot(entity);
    if snapshot.fenced {
        return false;
    }
    let Some(state) = snapshot.committed else {
        return false;
    };
    if state.fm_stage == ra::FmStage::None || state.local_deleted {
        return false;
    }
    let Some(info) = body.dropped.0.first().or_else(|| body.replicas.0.first()) else {
        return false;
    };
    info.unit == state.body.unit
        && info.service == state.body.service
        && info.replicas.0.len() == 1
        && info.replicas.0[0].description.node == state.body.replica.node
        && info.replicas.0[0].description.id == state.body.replica.id
        && info.replicas.0[0].description.instance == state.body.replica.instance
}

fn ticks(ticks: i64) -> Duration {
    Duration::from_nanos((ticks.max(0) as u64).saturating_mul(100))
}
impl Adapter {
    fn capture(&mut self, node: &mut Node) {
        for dispatch in node.take_external_up_to(32 - self.escrow.len()) {
            if matches!(dispatch.effect, ra::Effect::RequestReportRetry)
                && self.escrow.iter().any(|item| {
                    item.original.entity == dispatch.entity
                        && matches!(item.original.effect, ra::Effect::RequestReportRetry)
                })
            {
                continue; // The retained current-stage owner also owns this retry request.
            }
            let delay = match &dispatch.effect {
                ra::Effect::RequestServiceTypeWork { after_ticks, .. }
                | ra::Effect::ArmServiceTypeRetry { after_ticks, .. } => ticks(*after_ticks),
                ra::Effect::RequestReportRetry => REPORT_RETRY,
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
                            if self.report_pending.as_ref() == Some(&body)
                                && body
                                    .dropped
                                    .0
                                    .first()
                                    .or_else(|| body.replicas.0.first())
                                    .is_some_and(|info| report_cleared(node, info.unit.id.guid.0))
                            {
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
                let item = &mut self.escrow[index];
                if matches!(item.original.effect, ra::Effect::RequestReportRetry) {
                    if disposition == ra::Disposition::Applied {
                        // Keep this pass in flight until its report dispatch is
                        // sent or superseded; the receipt is not a semantic ACK.
                        item.sent = true;
                    } else {
                        item.submitted = None;
                        item.due = Instant::now() + REPORT_RETRY;
                    }
                } else {
                    self.escrow.remove(index);
                }
            }
        }
        Ok(())
    }
    fn finish_report_pass(&mut self, entity: ra::EntityId, delay: Duration) {
        if let Some(item) = self.escrow.iter_mut().find(|item| {
            item.original.entity == entity
                && matches!(item.original.effect, ra::Effect::RequestReportRetry)
        }) {
            item.submitted = None;
            item.sent = false;
            item.due = Instant::now() + delay;
        }
    }
    fn retire_reports(&mut self, node: &Node) {
        self.escrow.retain(|item| {
            !matches!(item.original.effect, ra::Effect::RequestReportRetry)
                || (item.submitted.is_some() && !item.sent)
                || !report_cleared(node, item.original.entity)
        });
        if self.report_pending.as_ref().is_some_and(|body| {
            body.dropped
                .0
                .first()
                .or_else(|| body.replicas.0.first())
                .is_some_and(|info| report_cleared(node, info.unit.id.guid.0))
        }) {
            self.report_pending = None;
        }
    }
    async fn advance(&mut self, node: &mut Node, link: Option<&Link>) -> Result<()> {
        self.capture(node);
        self.commands(node)?;
        self.retire_reports(node);
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
                ra::Effect::RequestReportRetry => {
                    if link.is_none() {
                        continue;
                    }
                    Some(FmWork::RetryReport(PartitionId {
                        guid: fube_transport::serialization::serde::Guid(entity),
                    }))
                }
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
                    let snapshot = node.snapshot(entity);
                    if node.pending_commands() != 0
                        || snapshot.fenced
                        || snapshot.committed.is_none()
                    {
                        continue;
                    }
                    if !current_report(node, entity, body) {
                        self.escrow.remove(index);
                        self.finish_report_pass(entity, Duration::ZERO);
                        return Ok(());
                    }
                    if self.escrow.iter().any(|item| {
                        item.original.entity == entity
                            && matches!(item.original.effect, ra::Effect::RequestReportRetry)
                            && item.submitted.is_some()
                            && !item.sent
                    }) {
                        continue;
                    }
                    let Some(link) = link else {
                        continue;
                    };
                    self.report_pending = Some((**body).clone());
                    link.send(replica_up_message(body, Owner::Fm).map_err(err)?)
                        .await?;
                    self.escrow.remove(index);
                    self.finish_report_pass(entity, REPORT_RETRY);
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
    fn replica_drained(&self, node: &Node) -> bool {
        let replica_work = |dispatch: &ra::Dispatched| {
            matches!(
                dispatch.effect,
                ra::Effect::RequestReportRetry | ra::Effect::ReplicaUp(_)
            )
        };
        self.replies.is_empty()
            && self.report_pending.is_none()
            && !self
                .acknowledgements
                .iter()
                .any(|(_, ack)| matches!(ack, SemanticAck::Report(_)))
            && !self.escrow.iter().any(|item| replica_work(&item.original))
            && !node.external_pending().iter().any(replica_work)
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
    let mut published_retirement = None;
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
            if link.is_some() && adapter.error.is_none() && adapter.replica_drained(&node) {
                if let Some(disposal) = node.instance_disposal() {
                    let state = node.snapshot(disposal.body.unit.id.guid.0);
                    let deleted = !state.fenced
                        && state.committed.is_some_and(|s| {
                            s.local_deleted && s.cleanup_pending && s.fm_stage == ra::FmStage::None
                        });
                    let lifetime = (disposal.body.replica.id, disposal.body.replica.instance);
                    if deleted && published_retirement != Some(lifetime) {
                        let profile = match actual_profile(&node) {
                            Ok(profile) => profile,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                        let receipt = RetirementReceipt {
                            deployment: launch.deployment.clone(),
                            session: launch.session.clone(),
                            node: launch.node.clone(),
                            fm: launch.fm.clone(),
                            generation: launch.generation.clone(),
                            node_pid: std::process::id(),
                            app_pid: profile.app_pid,
                            runtime_root: profile.runtime_root,
                            disposal,
                        };
                        if let Err(error) =
                            write_json(&launch.directory.join("retirement.json"), &receipt)
                        {
                            failure = Some(error);
                            break;
                        }
                        published_retirement = Some(lifetime);
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

#[cfg(test)]
pub(crate) mod report_tests {
    use super::*;

    #[derive(Default)]
    struct Traffic {
        reports: Vec<ReplicaUp>,
        added: bool,
        removed: bool,
        add_reply: Option<AddInstanceReply>,
        remove_reply: Option<AddInstanceReply>,
    }

    async fn links(launch: &Launch) -> Result<(Link, Link)> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
        let endpoint = fube_transport::Endpoint::unsecured_loopback(
            listener.local_addr().map_err(err)?,
            wire::composition(true).map_err(err)?,
        )
        .map_err(err)?
        .channel_limits(wire::limits());
        let (fm, node) = tokio::join!(endpoint.connect_until(wire::deadline()), async {
            let (stream, _) = listener.accept().await.map_err(err)?;
            fube_transport::accept_unsecured_loopback(
                stream,
                wire::composition(false).map_err(err)?,
                wire::limits(),
                wire::deadline(),
            )
            .await
            .map_err(err)
        });
        Ok((
            Link::new(
                fm.map_err(err)?,
                launch.fm.clone(),
                launch.node.clone(),
                launch.generation.clone(),
            )?,
            Link::new(
                node?,
                launch.node.clone(),
                launch.fm.clone(),
                launch.generation.clone(),
            )?,
        ))
    }

    async fn receive(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        link: &Link,
    ) -> Result<()> {
        if let Some(mut incoming) = link
            .receive(Instant::now() + Duration::from_millis(1))
            .await?
        {
            if let Some(ack) = receive_command(launch, node, &incoming.message)? {
                adapter.acknowledgements.push(ack);
            }
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
        }
        Ok(())
    }

    async fn step(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        traffic: &mut Traffic,
    ) -> Result<()> {
        node.drive().map_err(err)?;
        adapter.advance(node, Some(link)).await?;
        receive(launch, node, adapter, link).await?;
        if let Some(mut incoming) = fm
            .receive(Instant::now() + Duration::from_millis(1))
            .await?
        {
            match incoming.message.properties().map_err(err)?.action.as_str() {
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).map_err(err)?;
                    fm.send(
                        service_type_notification_reply_message(&body, &launch.generation)
                            .map_err(err)?,
                    )
                    .await?;
                }
                "AddInstanceReply" => {
                    let (body, _) = decode_add_instance_reply(&incoming.message).map_err(err)?;
                    if body.error != 0
                        || body.replica.state != 2
                        || body.replica.node != launch.node
                        || body.replica.instance != launch.incarnation
                        || body.replica.service_endpoint.is_empty()
                    {
                        return Err(
                            "real sample did not semantically open the expected replica".into()
                        );
                    }
                    traffic.added = true;
                    traffic.add_reply = Some(body);
                }
                "ReplicaUp" => {
                    let (body, owner) = decode_replica_up(&incoming.message).map_err(err)?;
                    if owner != Owner::Fm || traffic.reports.len() == 16 {
                        return Err("unexpected report authority or unbounded retry".into());
                    }
                    traffic.reports.push(body);
                }
                "RemoveInstanceReply" => {
                    let (body, _) = decode_remove_instance_reply(&incoming.message).map_err(err)?;
                    if body.error != 0 || body.replica.state != 3 {
                        return Err("real removal did not dispose the instance".into());
                    }
                    traffic.removed = true;
                    traffic.remove_reply = Some(body);
                }
                other => return Err(format!("unexpected FM test ingress {other}")),
            }
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
        }
        Ok(())
    }

    async fn until(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        traffic: &mut Traffic,
        ready: impl Fn(&Node, &Adapter, &Traffic) -> bool,
    ) -> Result<()> {
        let end = Instant::now() + Duration::from_secs(10);
        while !ready(node, adapter, traffic) {
            if Instant::now() >= end {
                return Err("live adapter/RA progress deadline".into());
            }
            step(launch, node, adapter, fm, link, traffic).await?;
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    fn timer(adapter: &mut Adapter) -> Result<&mut Escrow> {
        let mut owners = adapter
            .escrow
            .iter_mut()
            .filter(|item| matches!(item.original.effect, ra::Effect::RequestReportRetry));
        let owner = owners
            .next()
            .ok_or("report retry owner disappeared before semantic clearance")?;
        if owners.next().is_some() {
            return Err("more than one current report retry owner".into());
        }
        Ok(owner)
    }

    async fn queued_report(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        traffic: &mut Traffic,
    ) -> Result<()> {
        timer(adapter)?.due = Instant::now();
        until(launch, node, adapter, fm, link, traffic, |_, adapter, _| {
            adapter.escrow.iter().any(|item| {
                matches!(item.original.effect, ra::Effect::RequestReportRetry)
                    && item.submitted.is_some()
            })
        })
        .await?;
        let end = Instant::now() + Duration::from_secs(3);
        while node.pending_commands() != 0
            || !node
                .external_pending()
                .iter()
                .any(|item| matches!(item.effect, ra::Effect::ReplicaUp(_)))
        {
            if Instant::now() >= end {
                return Err("RA did not produce the accepted report pass".into());
            }
            node.drive().map_err(err)?;
            tokio::task::yield_now().await;
        }
        adapter.capture(node);
        Ok(())
    }

    async fn acknowledge_without_sending_captured_report(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        body: &ReplicaUp,
    ) -> Result<()> {
        fm.send(replica_up_reply_message(body, &launch.generation).map_err(err)?)
            .await?;
        let end = Instant::now() + Duration::from_secs(3);
        let before = adapter.acknowledgements.len();
        while adapter.acknowledgements.len() == before {
            if Instant::now() >= end {
                return Err("semantic ACK did not cross the original live channel".into());
            }
            receive(launch, node, adapter, link).await?;
        }
        while node.pending_commands() != 0 {
            if Instant::now() >= end {
                return Err("semantic ACK did not commit in RA".into());
            }
            node.drive().map_err(err)?;
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn exercise(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
    ) -> Result<()> {
        let body = add_body(launch, &actual_profile(node)?);
        let entity = body.unit.id.guid.0;
        let mut traffic = Traffic::default();
        fm.send(add_instance_message(&body, &launch.generation).map_err(err)?)
            .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, _, traffic| {
                traffic.added
                    && node
                        .snapshot(entity)
                        .committed
                        .is_some_and(|s| s.phase == ra::Phase::Ready)
            },
        )
        .await?;
        node.close(body.unit.id.clone(), ra::CloseMode::Drop)
            .map_err(err)?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, adapter, _| {
                node.pending_commands() == 0
                    && node.snapshot(entity).committed.is_some_and(|s| {
                        s.phase == ra::Phase::Closed && s.fm_stage == ra::FmStage::ReplicaDropped
                    })
                    && adapter
                        .escrow
                        .iter()
                        .any(|item| matches!(item.original.effect, ra::Effect::RequestReportRetry))
            },
        )
        .await?;
        let original = timer(adapter)?.original.clone();
        for count in 1..=3 {
            timer(adapter)?.due = Instant::now();
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |_, _, traffic| traffic.reports.len() == count,
            )
            .await?;
            if timer(adapter)?.original != original
                || timer(adapter)?.submitted.is_some()
                || adapter.report_pending.as_ref() != traffic.reports.first()
                || !node
                    .snapshot(entity)
                    .committed
                    .is_some_and(|s| s.fm_stage == ra::FmStage::ReplicaDropped && !s.local_deleted)
            {
                return Err("write/pass receipt lost the original unresolved report owner".into());
            }
        }
        let first = traffic.reports[0].clone();
        if first.dropped.0.len() != 1 || first.dropped.0[0].replicas.0.len() != 1 {
            return Err("report must describe exactly the original dropped replica".into());
        }
        let info = &first.dropped.0[0];
        if first.last
            || first.from_fmm
            || !first.replicas.0.is_empty()
            || info.unit != body.unit
            || info.service != body.service
            || info.replicas.0[0].description
                != (Replica {
                    node: launch.node.clone(),
                    id: launch.replica,
                    instance: launch.incarnation,
                    up: false,
                    state: 3,
                    ..Replica::default()
                })
            || traffic.reports.iter().any(|report| report != &first)
        {
            return Err("repeated report changed its expected native authority/body".into());
        }

        let mut stale = first.clone();
        stale.dropped.0[0].replicas.0[0].description.id += 1;
        let mut nonclearing = first.clone();
        nonclearing.replicas.0 = std::mem::take(&mut nonclearing.dropped.0);
        nonclearing.replicas.0[0].replicas.0[0].description.state = 2;
        nonclearing.replicas.0[0].replicas.0[0].description.up = true;
        for rejected in [stale, nonclearing.clone()] {
            acknowledge_without_sending_captured_report(launch, node, adapter, fm, link, &rejected)
                .await?;
            adapter.advance(node, Some(link)).await?;
            if !node
                .snapshot(entity)
                .committed
                .is_some_and(|s| s.fm_stage == ra::FmStage::ReplicaDropped && !s.local_deleted)
                || adapter.report_pending.as_ref() != Some(&first)
            {
                return Err(
                    "stale/foreign/nonclearing ACK retired current report semantics".into(),
                );
            }
            let count = traffic.reports.len() + 1;
            timer(adapter)?.due = Instant::now();
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |_, _, traffic| traffic.reports.len() == count,
            )
            .await?;
            if traffic.reports.last() != Some(&first) || timer(adapter)?.original != original {
                return Err("nonclearing ACK stopped or changed the next report retry".into());
            }
        }
        let pending = node.pending_commands();
        let mut foreign_source = launch.fm.clone();
        foreign_source.instance += 1;
        if !matches!(
            node.fm_work(FmWork::ReplicaUpReply(
                first.clone(),
                foreign_source,
                launch.generation.clone()
            )),
            Err(fube_node::local::Error::Protocol)
        ) {
            return Err("foreign FM incarnation admitted".into());
        }
        let mut stale_generation = launch.generation.clone();
        stale_generation.number.generation -= 1;
        if receive_command(
            launch,
            node,
            &replica_up_reply_message(&first, &stale_generation).map_err(err)?,
        )
        .is_ok()
            || node.pending_commands() != pending
        {
            return Err("stale generation changed accepted adapter work".into());
        }

        // Freeze an old dispatch, then commit a newer current description
        // without clearing the report stage. The old body must never escape.
        queued_report(launch, node, adapter, fm, link, &mut traffic).await?;
        nonclearing.replicas.0[0].service.update_version += 1;
        acknowledge_without_sending_captured_report(launch, node, adapter, fm, link, &nonclearing)
            .await?;
        let count = traffic.reports.len() + 1;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |_, _, traffic| traffic.reports.len() == count,
        )
        .await?;
        let mut current = first;
        current.dropped.0[0].service.update_version += 1;
        if traffic.reports.last() != Some(&current)
            || adapter.report_pending.as_ref() != Some(&current)
        {
            return Err("adapter replayed a superseded captured report body".into());
        }

        queued_report(launch, node, adapter, fm, link, &mut traffic).await?;
        acknowledge_without_sending_captured_report(launch, node, adapter, fm, link, &current)
            .await?;
        let count = traffic.reports.len();
        for _ in 0..6 {
            step(launch, node, adapter, fm, link, &mut traffic).await?;
        }
        if traffic.reports.len() != count
            || adapter.report_pending.is_some()
            || adapter.escrow.iter().any(|item| {
                matches!(
                    item.original.effect,
                    ra::Effect::RequestReportRetry | ra::Effect::ReplicaUp(_)
                )
            })
            || !node
                .snapshot(entity)
                .committed
                .is_some_and(|s| s.fm_stage == ra::FmStage::None && s.local_deleted)
        {
            return Err("real RA clearance failed to stop retries/stale captured sends".into());
        }
        Ok(())
    }

    async fn exercise_reuse(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
    ) -> Result<()> {
        let body = add_body(launch, &actual_profile(node)?);
        let entity = body.unit.id.guid.0;
        let mut traffic = Traffic::default();
        let mut ledger = crate::fm::Ledger::new([launch.clone(), launch.clone()]);
        ledger.placements[0].bootstrap = crate::fm::Bootstrap::Metadata;
        ledger.placements[0].admit_profile(actual_profile(node)?)?;
        let command = |op: &str, action: &str| crate::fm::Command {
            op: op.into(),
            action: action.into(),
            node: Some(1),
        };
        ledger.accept(command("a", "create"))?;
        fm.send(add_instance_message(&body, &launch.generation).map_err(err)?)
            .await?;
        until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
            t.added
        })
        .await?;
        let old_reply = traffic.add_reply.clone().unwrap();
        ledger.reply(0, old_reply.clone(), false)?;
        let endpoint = node
            .snapshot(entity)
            .committed
            .unwrap()
            .body
            .replica
            .service_endpoint
            .clone();
        fm.send(
            remove_instance_message(
                &RemoveInstance::new(body.clone(), false),
                &launch.generation,
            )
            .map_err(err)?,
        )
        .await?;
        until(launch, node, adapter, fm, link, &mut traffic, |n, a, t| {
            t.removed && a.replica_drained(n) && n.instance_disposal().is_some()
        })
        .await?;
        assert!(std::net::TcpStream::connect(&endpoint).is_err());
        assert!(adapter.replica_drained(node));
        let proof = node.instance_disposal().unwrap();
        assert_eq!(proof.body.replica.id, launch.replica);
        ledger.accept(command("d", "delete"))?;
        let old_remove = traffic.remove_reply.clone().unwrap();
        ledger.reply(0, old_remove.clone(), true)?;
        let profile = actual_profile(node)?;
        ledger.retirement(
            0,
            RetirementReceipt {
                deployment: launch.deployment.clone(),
                session: launch.session.clone(),
                node: launch.node.clone(),
                fm: launch.fm.clone(),
                generation: launch.generation.clone(),
                node_pid: std::process::id(),
                app_pid: profile.app_pid,
                runtime_root: profile.runtime_root,
                disposal: proof,
            },
            std::process::id(),
        )?;
        let mut next = launch.clone();
        next.replica += 1;
        next.incarnation += 1;
        let successor = add_body(&next, &actual_profile(node)?);
        traffic.added = false;
        ledger.accept(command("b", "create"))?;
        fm.send(add_instance_message(&successor, &launch.generation).map_err(err)?)
            .await?;
        until(&next, node, adapter, fm, link, &mut traffic, |_, _, t| {
            t.added
        })
        .await?;
        ledger.reply(0, traffic.add_reply.clone().unwrap(), false)?;
        let before = node.snapshot(entity).committed.unwrap();
        assert_eq!(before.body.replica.id, launch.replica + 1);
        assert_eq!(before.body.replica.instance, launch.incarnation + 1);
        assert!(node.instance_disposal().is_none());
        // Actual predecessor requests/ACKs over the same original Channel.
        let old_report = ReplicaUp {
            dropped: fube_transport::serialization::serde::ObjectArray(vec![FailoverUnitInfo {
                unit: body.unit.clone(),
                service: body.service.clone(),
                replicas: fube_transport::serialization::serde::ObjectArray(vec![ReplicaInfo {
                    description: Replica {
                        state: 3,
                        up: false,
                        version: VersionInstance::invalid(),
                        ..body.replica.clone()
                    },
                    ..Default::default()
                }]),
                ..Default::default()
            }]),
            ..Default::default()
        };
        for message in [
            add_instance_message(&body, &launch.generation).map_err(err)?,
            remove_instance_message(
                &RemoveInstance::new(body.clone(), false),
                &launch.generation,
            )
            .map_err(err)?,
            replica_up_reply_message(&old_report, &launch.generation).map_err(err)?,
        ] {
            fm.send(message).await?;
            for _ in 0..12 {
                step(&next, node, adapter, fm, link, &mut traffic).await?;
            }
            let after = node.snapshot(entity).committed.unwrap();
            assert_eq!(after.phase, ra::Phase::Ready);
            assert_eq!(after.body, before.body);
            assert!(node.instance_disposal().is_none());
            assert!(adapter.report_pending.is_none());
        }
        // Replay actual captured old completions and an old native report from
        // the original node-side Link. Source/PToP/generation/schema checks
        // still run; none may mutate the successor or dispose its session.
        let settled = ledger.status();
        for message in [
            add_instance_reply_message(&old_reply, Owner::Fm).map_err(err)?,
            remove_instance_reply_message(&old_remove, Owner::Fm).map_err(err)?,
            replica_up_message(&old_report, Owner::Fm).map_err(err)?,
        ] {
            link.send(message).await?;
            let mut incoming = fm
                .receive(wire::deadline())
                .await?
                .ok_or("replay missing")?;
            wire::predecode(&incoming.message)?;
            match incoming.message.properties().map_err(err)?.action.as_str() {
                "AddInstanceReply" => ledger.reply(
                    0,
                    decode_add_instance_reply(&incoming.message).map_err(err)?.0,
                    false,
                )?,
                "RemoveInstanceReply" => ledger.reply(
                    0,
                    decode_remove_instance_reply(&incoming.message)
                        .map_err(err)?
                        .0,
                    true,
                )?,
                "ReplicaUp" => assert!(ledger.placements[0]
                    .report(&decode_replica_up(&incoming.message).map_err(err)?.0)?
                    .is_none()),
                _ => return Err("wrong replay action".into()),
            }
            assert_eq!(ledger.status(), settled);
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
        }
        // A fresh valid duplicate over that same session still completes.
        traffic.added = false;
        fm.send(add_instance_message(&successor, &launch.generation).map_err(err)?)
            .await?;
        until(&next, node, adapter, fm, link, &mut traffic, |_, _, t| {
            t.added
        })
        .await?;
        Ok(())
    }

    pub(crate) async fn run(launch: &Launch) -> Result<Result<()>> {
        run_case(launch, false).await
    }
    pub(crate) async fn run_reuse(launch: &Launch) -> Result<Result<()>> {
        run_case(launch, true).await
    }
    async fn run_case(launch: &Launch, reuse: bool) -> Result<Result<()>> {
        private_dir(&launch.directory)?;
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
        let mut node = Node::new(
            Activation::new(options).map_err(err)?,
            store.clone(),
            ra::Admission {
                local_node: launch.node.clone(),
                fm_source: launch.fm.clone(),
                generation: launch.generation.number.clone(),
                owner_ready: true,
                node_open: true,
                node_activated: true,
                fabric_upgrading: false,
            },
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
        let result = async {
            node.start(&launch.package, APPLICATION, &[]).map_err(err)?;
            let (fm, link) = links(launch).await?;
            let result = if reuse {
                exercise_reuse(launch, &mut node, &mut adapter, &fm, &link).await
            } else {
                exercise(launch, &mut node, &mut adapter, &fm, &link).await
            };
            let fm_closed = fm.close().await;
            let node_closed = link.close().await;
            result.and(fm_closed).and(node_closed)
        }
        .await;
        let cleaned = cleanup(launch, &mut node, &store, &mut adapter).await;
        cleaned?;
        Ok(result)
    }
}
