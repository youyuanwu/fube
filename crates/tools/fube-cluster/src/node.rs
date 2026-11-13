use crate::{
    profile::*,
    wire::{self, Link},
};
use fube_hosting::local::{Activation, NodeContext, Options};
use fube_node::local::{FmWork, LocalDisposition, Node};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::{MemoryBackend, Settings, Store};
use std::{
    collections::VecDeque, fs, future::Future, path::Path, pin::Pin, sync::Arc, time::Duration,
};
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
    status_due: Option<Instant>,
    status_last_start: Option<Instant>,
    status_requested: bool,
    status_cleanup_due: Option<Instant>,
    status_cleanup_command: Option<u64>,
    status_capture: Option<u64>,
    escrow: VecDeque<Escrow>,
    replies: VecDeque<ra::Dispatched>,
    peer_replies: VecDeque<ra::Dispatched>,
    peer_last_reply: Option<CreateReplicaReply>,
    peer_error: Option<String>,
    peer_command_error: Option<String>,
    peer_commands: Vec<u64>,
    error: Option<String>,
    notification_pending: Option<ServiceTypeNotification>,
    report_pending: Option<ReplicaUp>,
    acknowledgements: Vec<(u64, SemanticAck)>,
    rejected_change_configuration_reply: Option<FailoverUnitReply>,
}
enum SemanticAck {
    Notification(ServiceTypeNotification),
    Report(ReplicaUp),
}
struct PeerConnection {
    link: Option<Link>,
    incoming: Option<
        fube_transport::channel::IncomingMessage<fube_transport::native_channel::GatewayTarget>,
    >,
    dialed: bool,
}

type UploadRequest =
    Pin<Box<dyn Future<Output = Result<Option<fube_transport::message::Message>>> + Send>>;

struct LfumUploadState {
    request: Option<UploadRequest>,
    body: Option<LfumUpload>,
    retry_at: Instant,
}

impl Default for LfumUploadState {
    fn default() -> Self {
        Self {
            request: None,
            body: None,
            retry_at: Instant::now(),
        }
    }
}

impl LfumUploadState {
    fn complete(
        &mut self,
        node: &Node,
        source: &NodeInstance,
        reply: Result<Option<fube_transport::message::Message>>,
        now: Instant,
    ) -> Result<()> {
        self.request = None;
        self.retry_at = now
            + ticks(
                node.native_config()
                    .node(ra::NodeProperty::LfumUploadRetryIntervalTicks)
                    .max(1),
            );
        if let Some(message) = reply? {
            wire::predecode(&message)?;
            let generation = decode_lfum_upload_reply(&message).map_err(err)?;
            if node
                .process_lfum_upload_reply(source, &generation)
                .map_err(err)?
            {
                self.body = None;
            }
        }
        Ok(())
    }

    fn advance(
        &mut self,
        node: &Node,
        description: &NodeDescription,
        link: Option<&Link>,
        now: Instant,
    ) -> Result<()> {
        let generations = node.generation_state();
        if self.request.is_none()
            && self
                .body
                .as_ref()
                .is_some_and(|body| body.generation != generations.receive)
        {
            self.body = None;
            self.retry_at = now;
        }
        if generations.upload_pending && self.body.is_none() && self.request.is_none() {
            self.retry_at = now;
        }
        let Some(link) = link else { return Ok(()) };
        if !generations.upload_pending || self.request.is_some() || now < self.retry_at {
            return Ok(());
        }
        let Some(inventory) = node.lfum_inventory().map_err(err)? else {
            return Ok(());
        };
        let body = self.body.get_or_insert_with(|| LfumUpload {
            generation: inventory.generation,
            node: description.clone(),
            units: inventory.units,
            any_replica_found: inventory.any_replica_found,
        });
        let request = lfum_upload_message(body).map_err(err)?;
        let link = link.clone();
        self.request = Some(Box::pin(
            async move { link.request_optional(request).await },
        ));
        Ok(())
    }
}

fn peer_target(effect: &ra::Effect) -> Option<&NodeInstance> {
    match effect {
        ra::Effect::PeerCreateReplica { target, .. }
        | ra::Effect::PeerCreateReplicaReply { target, .. }
        | ra::Effect::PeerActivate { target, .. }
        | ra::Effect::PeerActivateReply { target, .. }
        | ra::Effect::PeerDeactivate { target, .. }
        | ra::Effect::PeerDeactivateReply { target, .. }
        | ra::Effect::PeerContinueSwap { target, .. }
        | ra::Effect::PeerGetLsn { target, .. }
        | ra::Effect::PeerGetLsnReply { target, .. } => Some(target),
        _ => None,
    }
}
const REPORT_RETRY: Duration = Duration::from_secs(15);
const STATEFUL_REPORT_RETRY: Duration = Duration::from_secs(30);

fn report_retry(node: &Node, entity: ra::EntityId) -> Duration {
    if node
        .snapshot(entity)
        .committed
        .is_some_and(|state| state.fm_primary || state.peer_terminal_report())
    {
        STATEFUL_REPORT_RETRY
    } else {
        REPORT_RETRY
    }
}

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
    fn reject_deprecated(
        &mut self,
        node: &Node,
        message: &fube_transport::message::Message,
    ) -> Result<bool> {
        if message.properties().map_err(err)?.action != "ChangeConfigurationReply" {
            return Ok(false);
        }
        wire::predecode(message)?;
        let (body, generation) = decode_change_configuration_reply(message).map_err(err)?;
        if generation.for_fm_replica || generation.number != node.generation_state().receive {
            return Err("deprecated reply has a foreign FM generation".into());
        }
        if self.rejected_change_configuration_reply.as_ref() != Some(&body) {
            eprintln!("rejected deprecated native action ChangeConfigurationReply");
            self.rejected_change_configuration_reply = Some(body);
        }
        Ok(true)
    }

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
                | ra::Effect::ArmServiceTypeRetry { after_ticks, .. }
                | ra::Effect::ArmNodeDeactivationCheck { after_ticks, .. } => ticks(*after_ticks),
                ra::Effect::RequestReportRetry
                    if node
                        .snapshot(dispatch.entity)
                        .committed
                        .is_some_and(|s| s.fm_primary || s.peer_terminal_report()) =>
                {
                    Duration::ZERO
                }
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
        if self.replies.is_empty() && self.peer_replies.is_empty() {
            for receipt in node.take_effect_receipts() {
                if matches!(receipt.disposition, LocalDisposition::LocalResponse)
                    && matches!(
                        receipt.dispatch.effect,
                        ra::Effect::PeerCreateReplica { .. }
                            | ra::Effect::PeerCreateReplicaReply { .. }
                            | ra::Effect::PeerActivate { .. }
                            | ra::Effect::PeerActivateReply { .. }
                            | ra::Effect::PeerDeactivate { .. }
                            | ra::Effect::PeerDeactivateReply { .. }
                            | ra::Effect::PeerContinueSwap { .. }
                            | ra::Effect::PeerGetLsn { .. }
                            | ra::Effect::PeerGetLsnReply { .. }
                    )
                {
                    if let ra::Effect::PeerCreateReplicaReply { body, .. } =
                        &receipt.dispatch.effect
                    {
                        self.peer_last_reply = Some((**body).clone());
                    }
                    self.peer_replies.push_back(receipt.dispatch);
                    continue;
                }
                if matches!(receipt.disposition, LocalDisposition::LocalResponse)
                    && matches!(
                        receipt.dispatch.effect,
                        ra::Effect::NodeActivateReply(_)
                            | ra::Effect::ChangeConfiguration(_)
                            | ra::Effect::DoReconfigurationReply(_)
                            | ra::Effect::ReplicaEndpointUpdated(_)
                            | ra::Effect::NodeDeactivateReply(_)
                            | ra::Effect::NodeUpdateServiceReply(_)
                            | ra::Effect::Reply(_)
                            | ra::Effect::PrimaryReply(_)
                            | ra::Effect::AddReplicaReply(_)
                            | ra::Effect::RemoveReplicaReply(_)
                            | ra::Effect::RemoveReply(_)
                            | ra::Effect::DeleteReplicaReply(_)
                    )
                {
                    self.replies.push_back(receipt.dispatch);
                }
            }
        }
    }
    fn commands(&mut self, node: &mut Node) -> Result<()> {
        for receipt in node.take_command_receipts() {
            if let Some(index) = self.peer_commands.iter().position(|id| *id == receipt.id) {
                self.peer_commands.remove(index);
                if let Err(error) = receipt.result {
                    self.peer_command_error = Some(err(error));
                }
                continue;
            }
            let disposition = receipt.result.map_err(err)?;
            if self.status_capture == Some(receipt.id) {
                self.status_capture = None;
                self.status_last_start = Some(Instant::now());
            }
            if self.status_cleanup_command == Some(receipt.id) {
                self.status_cleanup_command = None;
            }
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
                            if self.report_pending.as_ref().is_some_and(|pending| {
                                let sent = pending.dropped.0.first().or_else(|| pending.replicas.0.first());
                                let ack = body.dropped.0.first().or_else(|| body.replicas.0.first());
                                matches!((sent, ack), (Some(sent), Some(ack)) if sent.unit.id == ack.unit.id)
                            }) && body
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
                        item.due = Instant::now() + report_retry(node, item.original.entity);
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
        self.advance_with_peer(node, link, None, None).await
    }
    async fn advance_with_peer(
        &mut self,
        node: &mut Node,
        link: Option<&Link>,
        peer_link: Option<&Link>,
        peer: Option<&Peer>,
    ) -> Result<()> {
        self.capture(node);
        self.commands(node)?;
        if let (Some(link), Some(peer), Some(reply)) = (peer_link, peer, self.peer_replies.front())
        {
            let swap_unit = match &reply.effect {
                ra::Effect::PeerContinueSwap { body, .. } => Some(&body.configuration.unit),
                ra::Effect::PeerGetLsn { body, .. } => Some(&body.unit),
                ra::Effect::PeerGetLsnReply { body, .. } => Some(&body.replica.unit),
                _ => None,
            };
            if let Some(unit) = swap_unit {
                let snapshot = node.snapshot(reply.entity);
                if snapshot.fenced
                    || snapshot.committed.is_none_or(|s| {
                        s.phase != ra::Phase::Ready
                            || (s.swap.is_none() && s.progress_query.is_none())
                            || s.body.unit.current != unit.current
                    })
                {
                    self.peer_replies.pop_front();
                    return Ok(());
                }
            }
            if let ra::Effect::PeerDeactivateReply { body, .. } = &reply.effect {
                let snapshot = node.snapshot(reply.entity);
                if snapshot.fenced
                    || snapshot.committed.is_none_or(|state| {
                        let survivor_ack =
                            state.phase == ra::Phase::Ready
                                && (state.reconfiguration.as_ref().is_some_and(|c| {
                                    c.stage == ra::ConfigurationStage::Deactivated
                                }) || state.swap.as_ref().is_some_and(|s| {
                                    s.failover && s.stage == ra::SwapStage::Deactivated
                                }));
                        (!survivor_ack && state.phase != ra::Phase::Closed)
                            || state.body.unit != body.unit
                            || state.body.replica.id != body.replica.id
                            || state.body.replica.instance != body.replica.instance
                            || (!survivor_ack
                                && state
                                    .peer_creation
                                    .as_ref()
                                    .is_none_or(|p| !p.close_completed || p.host_down))
                    })
                {
                    self.peer_replies.pop_front();
                    return Ok(());
                }
            }
            if let ra::Effect::PeerActivateReply { body, .. } = &reply.effect {
                let snapshot = node.snapshot(reply.entity);
                if snapshot.fenced
                    || snapshot.committed.is_none_or(|state| {
                        state.phase != ra::Phase::Ready
                            || state.body.unit != body.unit
                            || state.body.replica != body.replica
                            || if state
                                .reconfiguration
                                .as_ref()
                                .is_some_and(|c| c.request.unit.current == body.unit.current)
                            {
                                state.reconfiguration.as_ref().is_none_or(|config| {
                                    config.stage != ra::ConfigurationStage::Complete
                                })
                            } else {
                                state
                                    .swap
                                    .as_ref()
                                    .is_none_or(|swap| swap.stage != ra::SwapStage::Complete)
                            }
                    })
                {
                    self.peer_replies.pop_front();
                    return Ok(());
                }
            }
            let sent = async {
                let message = match &reply.effect {
                    ra::Effect::PeerCreateReplica { target, body } if target == &peer.node => {
                        create_replica_message(body, &link.generation())
                    }
                    ra::Effect::PeerCreateReplicaReply { target, body } if target == &peer.node => {
                        create_replica_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerActivate { target, body } if target == &peer.node => {
                        activate_message(body, &link.generation())
                    }
                    ra::Effect::PeerActivateReply { target, body } if target == &peer.node => {
                        activate_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerDeactivate { target, body } if target == &peer.node => {
                        deactivate_message(body, &link.generation())
                    }
                    ra::Effect::PeerDeactivateReply { target, body } if target == &peer.node => {
                        deactivate_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerContinueSwap { target, body } if target == &peer.node => {
                        do_reconfiguration_message(body, &link.generation())
                    }
                    ra::Effect::PeerGetLsn { target, body } if target == &peer.node => {
                        get_lsn_message(body, &link.generation())
                    }
                    ra::Effect::PeerGetLsnReply { target, body } if target == &peer.node => {
                        get_lsn_reply_message(body, &link.generation())
                    }
                    _ => return Err("peer receipt does not match the original bound peer".into()),
                }
                .map_err(err)?;
                link.send(message).await
            }
            .await;
            match sent {
                Ok(()) => {
                    self.peer_replies.pop_front();
                }
                Err(error) => {
                    self.peer_error = Some(error);
                    link.channel.dispose();
                }
            }
        }
        self.retire_reports(node);
        self.escrow.retain(|item| {
            !matches!(item.original.effect, ra::Effect::ArmNodeDeactivationCheck { sequence, .. }
                if item.submitted.is_none() && node.pending_node_deactivation() != Some(sequence))
        });
        if !node.has_service_type_status() {
            self.status_due = None;
            self.status_requested = false;
        }
        let now = Instant::now();
        let cleanup_interval = ticks(
            node.native_config()
                .node(ra::NodeProperty::StatusCleanupIntervalTicks),
        )
        .max(Duration::from_nanos(100));
        let cleanup_due = self
            .status_cleanup_due
            .get_or_insert(now + cleanup_interval);
        if *cleanup_due <= now && self.status_cleanup_command.is_none() {
            self.status_cleanup_command =
                Some(node.fm_work(FmWork::CleanupTypeStatus).map_err(err)?);
            *cleanup_due = now + cleanup_interval;
        }
        let minimum = ticks(
            node.native_config()
                .node(ra::NodeProperty::StatusNodeIntervalTicks),
        );
        if (self.status_requested || self.status_due.is_some_and(|due| due <= now))
            && self
                .status_last_start
                .is_none_or(|start| now >= start + minimum)
            && self.status_capture.is_none()
            && link.is_some()
        {
            self.status_capture = Some(node.fm_work(FmWork::CaptureTypeStatus).map_err(err)?);
            self.status_requested = false;
            self.status_due = None;
        }
        if let Some(link) = link {
            if let Some(reply) = self.replies.front() {
                if let ra::Effect::ReplicaEndpointUpdated(body) = &reply.effect {
                    let snapshot = node.snapshot(reply.entity);
                    if snapshot.fenced
                        || snapshot.committed.is_none_or(|s| {
                            s.phase != ra::Phase::Ready
                                || s.body.as_ref() != body.as_ref()
                                || s.body.replica.current_role != 4
                                || s.swap.as_ref().is_none_or(|swap| !swap.endpoint_available)
                        })
                    {
                        self.replies.pop_front();
                        return Ok(());
                    }
                }
                if let ra::Effect::DoReconfigurationReply(body) = &reply.effect {
                    let snapshot = node.snapshot(reply.entity);
                    if snapshot.fenced
                        || snapshot.committed.is_none_or(|state| {
                            state.phase != ra::Phase::Ready
                                || state.body.replica.current_role != 4
                                || state.body.unit != body.unit
                                || if state.reconfiguration.as_ref().is_some_and(|config| {
                                    config.request.unit.current == body.unit.current
                                }) {
                                    state.reconfiguration.as_ref().is_none_or(|config| {
                                        config.stage != ra::ConfigurationStage::Complete
                                    })
                                } else {
                                    state.swap.as_ref().is_none_or(|swap| {
                                        swap.stage != ra::SwapStage::Complete
                                            || swap.body.unit != body.unit
                                    })
                                }
                        })
                    {
                        self.replies.pop_front();
                        return Ok(());
                    }
                    if let ra::Effect::ChangeConfiguration(body) = &reply.effect {
                        let snapshot = node.snapshot(reply.entity);
                        if snapshot.fenced
                            || snapshot.committed.is_none_or(|s| {
                                s.phase != ra::Phase::Ready
                                    || s.swap.as_ref().is_none_or(|swap| {
                                        swap.stage != ra::SwapStage::ChangeConfiguration
                                            || swap.change.as_ref() != Some(body.as_ref())
                                    })
                            })
                        {
                            self.replies.pop_front();
                            return Ok(());
                        }
                    }
                }
                if let ra::Effect::AddReplicaReply(body) | ra::Effect::RemoveReplicaReply(body) =
                    &reply.effect
                {
                    let removing = matches!(reply.effect, ra::Effect::RemoveReplicaReply(_));
                    let snapshot = node.snapshot(reply.entity);
                    if snapshot.fenced
                        || snapshot.committed.as_ref().is_none_or(|state| {
                            state.phase != ra::Phase::Ready
                                || !state.fm_primary
                                || state
                                    .remote_idle(&body.replica.node)
                                    .and_then(|remote| {
                                        if removing {
                                            remote.remove_reply()
                                        } else {
                                            remote.add_reply()
                                        }
                                    })
                                    .as_ref()
                                    != Some(body)
                        })
                    {
                        self.replies.pop_front();
                        return Ok(());
                    }
                }
                let message = match &reply.effect {
                    ra::Effect::NodeActivateReply(body) => {
                        node_activate_reply_message(body, Owner::Fm)
                    }
                    ra::Effect::NodeDeactivateReply(body) => {
                        node_deactivate_reply_message(body, Owner::Fm)
                    }
                    ra::Effect::NodeUpdateServiceReply(body) => {
                        node_update_service_reply_message(body, Owner::Fm)
                    }
                    ra::Effect::Reply(body) => add_instance_reply_message(body, Owner::Fm),
                    ra::Effect::DoReconfigurationReply(body) => {
                        do_reconfiguration_reply_message(body, Owner::Fm)
                    }
                    ra::Effect::ChangeConfiguration(body) => {
                        change_configuration_message(body, Owner::Fm)
                    }
                    ra::Effect::ReplicaEndpointUpdated(body) => {
                        replica_endpoint_updated_message(body, Owner::Fm)
                    }
                    ra::Effect::PrimaryReply(body) => add_primary_reply_message(body, Owner::Fm),
                    ra::Effect::AddReplicaReply(body) => add_replica_reply_message(body, Owner::Fm),
                    ra::Effect::RemoveReplicaReply(body) => {
                        remove_replica_reply_message(body, Owner::Fm)
                    }
                    ra::Effect::RemoveReply(body) => remove_instance_reply_message(body, Owner::Fm),
                    ra::Effect::DeleteReplicaReply(body) => {
                        delete_replica_reply_message(body, Owner::Fm)
                    }
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
                ra::Effect::ServiceTypeStatusWork => {
                    self.status_requested = true;
                    self.escrow.remove(index);
                    return Ok(());
                }
                ra::Effect::ServiceTypeStatus { enabled, body } => {
                    let Some(link) = link else {
                        continue;
                    };
                    let message = if *enabled {
                        service_type_enabled_message(body)
                    } else {
                        service_type_disabled_message(body)
                    }
                    .map_err(err)?;
                    link.send(message).await?;
                    self.status_due = Some(
                        Instant::now()
                            + ticks(
                                node.native_config()
                                    .node(ra::NodeProperty::StatusRetryIntervalTicks),
                            )
                            .max(Duration::from_nanos(100)),
                    );
                    self.escrow.remove(index);
                    return Ok(());
                }
                ra::Effect::ReportLoad(body) => {
                    let Some(link) = link else {
                        continue;
                    };
                    link.send(report_load_message(body).map_err(err)?).await?;
                    self.escrow.remove(index);
                    return Ok(());
                }
                ra::Effect::ArmNodeDeactivationCheck { sequence, .. } => {
                    Some(FmWork::CheckNodeDeactivation(*sequence))
                }
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
                    self.finish_report_pass(entity, report_retry(node, entity));
                    return Ok(());
                }
                ra::Effect::StatefulClosePending { body, .. }
                | ra::Effect::StatefulDroppedPending(body) => {
                    // Native deleted metadata suppresses reporting, not cleanup proof.
                    let snapshot = node.snapshot(entity);
                    if !snapshot.fenced
                        && snapshot.committed.is_some_and(|state| {
                            state.local_deleted
                                && state.phase == ra::Phase::Closed
                                && state.body.unit == body.unit
                                && state.body.replica.id == body.replica.id
                                && state.body.replica.instance == body.replica.instance
                        })
                    {
                        self.escrow.remove(index);
                        return Ok(());
                    }
                    continue;
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
        self.escrow.len()
            + self.replies.len()
            + self.peer_replies.len()
            + self.peer_commands.len()
            + self.acknowledgements.len()
            + usize::from(self.status_capture.is_some())
            + usize::from(self.status_cleanup_command.is_some())
    }
    fn semantic_pending(&self) -> usize {
        self.notification_pending
            .as_ref()
            .map_or(0, |body| body.infos.0.len())
            + usize::from(self.report_pending.is_some())
            + usize::from(self.status_due.is_some() || self.status_requested)
    }
    fn replica_drained(&self, node: &Node) -> bool {
        let replica_work = |dispatch: &ra::Dispatched| dispatch.effect.is_replica_reporting();
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
    let mut peer_listeners = vec![];
    let mut peer_addresses = vec![];
    for _ in &launch.peers {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
        peer_addresses.push(listener.local_addr().map_err(err)?);
        peer_listeners.push(Some(listener));
    }
    let peer_address = peer_addresses.first().copied();
    let mut metadata = Metadata {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        pid: std::process::id(),
        address,
        peer_address,
        peer_addresses,
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
    let channel = fube_transport::accept_unsecured_tcp(
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
    if launch.profile.peer_replicas() {
        options.service_package_version_instance = Some(1);
    }
    if launch.profile.stateful() {
        options.profile = if launch.profile.peer_replicas() && launch.placement != 1 {
            fube_hosting::local::Profile::VolatileStatefulLifecycle
        } else {
            fube_hosting::local::Profile::VolatileStatefulFmPrimary
        };
    }
    options.node = Some(NodeContext {
        id: node_hex(&launch.node),
        instance: launch.node.instance,
        name: launch.name(),
        ip: std::net::Ipv4Addr::LOCALHOST.into(),
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
    if !launch.peers.is_empty() {
        node.bind_peers(launch.peers.iter().map(|p| p.node.clone()).collect())
            .map_err(err)?;
    }
    node.apply_fresh_node_up_ack(&ack, &generation)
        .map_err(err)?;
    let mut adapter = Adapter::default();
    let arguments = if launch.profile.peer_replicas() && launch.placement != 1 {
        vec![(
            "Arguments".into(),
            "--stateful-lifecycle --data-replicator".into(),
        )]
    } else if launch.profile.stateful() {
        vec![("Arguments".into(), "--stateful-primary".into())]
    } else {
        vec![]
    };
    let started = node
        .start(&launch.package, APPLICATION, &arguments)
        .map_err(err);
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
    let mut connections: Vec<_> = launch
        .peers
        .iter()
        .map(|_| PeerConnection {
            link: None,
            incoming: None,
            dialed: false,
        })
        .collect();
    let mut failure = startup.err();
    let mut next_observation = Instant::now();
    let mut published_retirement = None;
    let mut published_absence = None;
    let mut published_pre_open = None;
    let mut published_failed_registration = None;
    let mut published_failed_open = None;
    let mut published_host_loss = false;
    let mut repair_requested = None;
    let mut repaired_hosts = Vec::new();
    let mut upload = LfumUploadState::default();
    if failure.is_none() {
        loop {
            // Owned stop remains independent of FM connectivity. Link loss is
            // not permission to stop an existing app or fake lifecycle replies.
            tokio::select! {
                reply = async {
                    match upload.request.as_mut() {
                        Some(request) => request.await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(error) = upload.complete(&node, &launch.fm, reply, Instant::now()) {
                        adapter.error = Some(error);
                    }
                }
                control = control_line(&mut control) => {
                    match control {
                        Ok(Some(line)) if line == format!("stop {}\n", launch.capability).as_bytes() => break,
                        Ok(Some(line)) => {
                            let command = std::str::from_utf8(&line).map_err(err);
                            let command = command.as_ref().map(|line| line.split_whitespace().collect::<Vec<_>>());
                            match command {
                                Ok(parts) if parts.len() == 5 && parts[0] == "replace-host"
                                    && parts[1] == launch.session && parts[2] == launch.capability
                                    && launch.profile.mesh_peers() => {
                                    let identity = parts[3].parse::<i64>().and_then(|id| {
                                        parts[4].parse::<i64>().map(|instance| (id, instance))
                                    });
                                    let identity = match identity {
                                        Ok(identity) => identity,
                                        Err(error) => {
                                            failure = Some(format!("invalid replacement predecessor: {error}"));
                                            break;
                                        }
                                    };
                                    if !repaired_hosts.contains(&identity) {
                                        let current = uuid::Uuid::parse_str(&launch.partition).ok()
                                            .and_then(|id| node.snapshot(id.to_bytes_le()).committed);
                                        if current.as_ref().is_none_or(|s| {
                                            (s.body.replica.id, s.body.replica.instance) != identity
                                                || s.phase != ra::Phase::Closed
                                        }) || repair_requested.is_some_and(|pending| pending != identity) {
                                            failure = Some("replacement command is not the current closed owner".into());
                                            break;
                                        }
                                        repair_requested = Some(identity);
                                    }
                                }
                                Ok(parts) if matches!(parts.len(), 4 | 5) && parts[0] == "peer"
                                    && parts[1] == launch.session && parts[2] == launch.capability
                                    && (launch.placement == 1 || launch.profile.mesh_peers()) && !launch.peers.is_empty() => {
                                    let index = if parts.len() == 5 {
                                        match parts[3].parse::<usize>() {
                                            Ok(index) => index,
                                            Err(error) => {
                                                failure = Some(format!("invalid original peer index: {error}"));
                                                break;
                                            }
                                        }
                                    } else { 0 };
                                    let Some(connection) = connections.get_mut(index).filter(|c| !c.dialed) else {
                                        failure = Some("invalid or already dialed original peer".into());
                                        break;
                                    };
                                    connection.dialed = true;
                                    peer_listeners[index].take();
                                    match dial_peer_to(launch, &launch.peers[index], parts[parts.len() - 1]).await {
                                        Ok(link) => connection.link = Some(link),
                                        Err(error) => adapter.peer_error = Some(error),
                                    }
                                }
                                Ok(parts) if parts.len() == 3 && matches!(parts[0], "create-idle" | "build-idle" | "remove-idle")
                                    && parts[1] == launch.session && parts[2] == launch.capability
                                    && launch.placement == 1 && !launch.peers.is_empty() => {
                                    let submitted = if adapter.peer_commands.len() == 32 {
                                        Err("peer command receipt capacity".into())
                                    } else {
                                        match parts[0] {
                                            "create-idle" => create_idle(launch, &mut node),
                                            "build-idle" => node.build_idle_on_peer().map_err(err),
                                            "remove-idle" => node.remove_idle_on_peer().map_err(err),
                                            _ => unreachable!(),
                                        }
                                    };
                                    match submitted {
                                        Ok(id) => adapter.peer_commands.push(id),
                                        Err(error) => adapter.peer_command_error = Some(error),
                                    }
                                }
                                _ => {
                                    failure = Some("invalid original owner control".into());
                                    break;
                                }
                            }
                        }
                        Ok(None) => { failure = Some("original owner control closed".into()); break; },
                        other => { failure = Some(format!("invalid original owner control: {other:?}")); break; },
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(2)) => {},
            }
            for (index, listener_slot) in peer_listeners.iter_mut().enumerate() {
                if let Some(listener) = listener_slot {
                    match tokio::time::timeout(Duration::from_millis(1), listener.accept()).await {
                        Ok(Ok((stream, _))) => {
                            listener_slot.take();
                            match accept_peer_from(launch, &launch.peers[index], stream).await {
                                Ok(link) => connections[index].link = Some(link),
                                Err(error) => adapter.peer_error = Some(error),
                            }
                        }
                        Ok(Err(error)) => {
                            adapter.peer_error = Some(err(error));
                            listener_slot.take();
                        }
                        Err(_) => {}
                    }
                }
            }
            if let Err(error) = node.drive() {
                failure = Some(err(error));
                break;
            }
            if let Err(error) = upload.advance(
                &node,
                &up.node,
                link.as_ref().filter(|_| adapter.error.is_none()),
                Instant::now(),
            ) {
                adapter.error = Some(error);
            }
            if adapter.error.is_some() {
                if let Some(old) = link.take() {
                    let _ = old.close().await;
                }
            }
            adapter.capture(&mut node);
            let selected_peer = adapter
                .peer_replies
                .front()
                .and_then(|reply| peer_target(&reply.effect))
                .and_then(|target| launch.peers.iter().position(|peer| &peer.node == target));
            let selected_link = selected_peer.and_then(|index| connections[index].link.as_ref());
            if let Err(error) = adapter
                .advance_with_peer(
                    &mut node,
                    link.as_ref(),
                    selected_link,
                    selected_peer.map(|index| &launch.peers[index]),
                )
                .await
            {
                adapter.error = Some(error);
                if let Some(old) = link.take() {
                    let _ = old.close().await;
                }
            }
            for (peer, connection) in launch.peers.iter().zip(&mut connections) {
                if let Some(current) = &connection.link {
                    if connection.incoming.is_none() && adapter.peer_commands.len() < 32 {
                        match current
                            .receive(Instant::now() + Duration::from_millis(1))
                            .await
                        {
                            Ok(incoming) => connection.incoming = incoming,
                            Err(error) => adapter.peer_error = Some(error),
                        }
                    }
                    if let Some(incoming) = &mut connection.incoming {
                        match receive_peer_from(launch, peer, &mut node, &incoming.message) {
                            Ok(Some(id)) => {
                                adapter.peer_commands.push(id);
                                if let Some(handoff) = incoming.handoff.as_mut() {
                                    handoff.acknowledge();
                                }
                                connection.incoming = None;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                adapter.peer_error = Some(error);
                                connection.incoming = None;
                                current.channel.dispose();
                            }
                        }
                    }
                    if adapter.peer_error.is_some() {
                        if let Some(old) = connection.link.take() {
                            let _ = old.close().await;
                        }
                    }
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
                        let received =
                            match receive_generation(launch, &node, current, &mut incoming).await {
                                Ok(true) => Ok(None),
                                Ok(false) => {
                                    match adapter.reject_deprecated(&node, &incoming.message) {
                                        Ok(true) => Ok(None),
                                        Ok(false) => {
                                            receive_command(launch, &mut node, &incoming.message)
                                        }
                                        Err(error) => Err(error),
                                    }
                                }
                                Err(error) => Err(error),
                            };
                        match received {
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
                if launch.profile == ServiceProfile::StatefulPeerIdle {
                    if let Err(error) = publish_host_loss(launch, &node, &mut published_host_loss) {
                        failure = Some(error);
                        break;
                    }
                }
                if let Err(error) = publish_absence(launch, &node, &mut published_absence) {
                    failure = Some(error);
                    break;
                }
                if let Err(error) =
                    publish_pre_open_cancellation(launch, &node, &mut published_pre_open)
                {
                    failure = Some(error);
                    break;
                }
                if let Err(error) =
                    publish_failed_registration(launch, &node, &mut published_failed_registration)
                {
                    failure = Some(error);
                    break;
                }
                if let Err(error) = publish_failed_open(launch, &node, &mut published_failed_open) {
                    failure = Some(error);
                    break;
                }
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
            if let Some(identity) = repair_requested.filter(|_| adapter.replica_drained(&node)) {
                match node.retire_lost_replica_host() {
                    Ok(true) => match replace_replica_host(launch, &mut node) {
                        Ok(receipt) => {
                            metadata.profile = Some(receipt.profile.clone());
                            if let Err(error) = write_json(
                                &launch.host_replacement_path(identity.0, identity.1),
                                &receipt,
                            ) {
                                failure = Some(error);
                                break;
                            }
                            repaired_hosts.push(identity);
                            repair_requested = None;
                        }
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    },
                    Ok(false) => {}
                    Err(error) => {
                        failure = Some(err(error));
                        break;
                    }
                }
            }
            if next_observation <= Instant::now() {
                let generations = node.generation_state();
                let entity = uuid::Uuid::parse_str(&launch.partition)
                    .map_err(err)?
                    .to_bytes_le();
                let snapshot = node.snapshot(entity);
                let state = snapshot.committed.as_ref().filter(|_| !snapshot.fenced);
                let local_replica = state.map(|state| {
                    serde_json::json!({
                        "open":state.phase == ra::Phase::Ready && state.runtime.is_some(),
                        "phase":format!("{:?}",state.phase), "replica":state.body.replica,
                        "open_token":state.open_token, "usage":format!("{:?}",state.usage),
                        "fm_stage":format!("{:?}",state.fm_stage),
                        "local_deleted":state.local_deleted,
                        "swap":state.swap.as_ref().map(|swap| serde_json::json!({"stage":format!("{:?}",swap.stage),"error":swap.error})),
                        "progress_query":state.progress_query.as_ref().map(|query| serde_json::json!({
                            "source":query.source,"epoch":query.request.unit.current,
                            "pending":query.reply.is_none(),"error":query.error,
                        })),
                        "configuration":state.reconfiguration.as_ref().map(|config| serde_json::json!({
                            "stage":format!("{:?}", config.stage), "request":config.request, "error":config.error,
                        })),
                        "pre_open_failure":node.pre_open_registration_failure(),
                        "runtime_open_failure":node.runtime_open_failure(),
                        "host_lost":node.activation().process_status().is_some_and(|process| process.exit_observed),
                        "retained_usage":node.retained_usage(), "reply":adapter.peer_last_reply,
                        "registration":state.runtime.as_ref().map(|runtime| serde_json::json!({
                            "host":runtime.host, "runtime":runtime.runtime, "acquisition":runtime.acquisition,
                        })),
                        "package_sequence":node.activation().identity().map(|identity| identity.code.package_sequence),
                    })
                });
                let remote_idle =
                    state
                        .and_then(|state| state.remote_idles.first())
                        .map(|remote| {
                            serde_json::json!({
                                "replica":remote.body.replica.0.replica,
                                "fm_owned":remote.fm_request.is_some(),
                                "ready":remote.ready && remote.removal.is_none(),
                                "reply":remote.reply,
                                "creation_aborted":remote.creation_failed(),
                                "build":remote.build.as_ref().map(|build| serde_json::json!({
                                    "result":build.result, "detail":build.detail,
                                })),
                                "removal":remote.removal.as_ref().map(|removal| serde_json::json!({
                                    "fm_owned":removal.fm_request.is_some(),
                                    "replica":removal.replica,
                                    "result":removal.result, "detail":removal.detail,
                                })),
                            })
                        });
                let app_logs = node.activation().logs();
                let idle_data = app_logs
                    .iter()
                    .rev()
                    .find_map(|line| line.split_once("LOCAL_IDLE_DATA ").map(|(_, body)| body))
                    .map(|body| {
                        serde_json::from_str::<serde_json::Value>(body)
                            .unwrap_or_else(|error| serde_json::json!({"error":err(error)}))
                    });
                let observation = serde_json::json!({
                    "deployment":launch.deployment, "session":launch.session,
                    "node_pid":std::process::id(), "external_pending":node.external_pending().len(),
                    "adapter_pending":adapter.pending(), "commands":node.pending_commands(),
                    "semantic_pending":adapter.semantic_pending(),
                    "staged_status":format!("{:?}", node.pending_status()),
                    "deprecated_change_configuration_reply":adapter.rejected_change_configuration_reply.as_ref()
                        .map(|body| serde_json::json!({"state":"rejected","reason":"deprecated native action","body":body})),
                    "error":adapter.error, "link_alive":link.is_some(),
                    "peer_link_alive":!connections.is_empty() && connections.iter().all(|c| c.link.is_some()), "peer_error":adapter.peer_error,
                    "remote_idles":state.map(|s| s.remote_idles.iter().map(|r| serde_json::json!({
                        "replica":r.body.replica.0.replica, "reply":r.reply,
                        "ready":r.ready && r.removal.is_none(),
                        "build":r.build.as_ref().map(|b| serde_json::json!({"result":b.result,"detail":b.detail})),
                    })).collect::<Vec<_>>()),
                    "peer_command_error":adapter.peer_command_error,
                    "peer_idle":remote_idle, "local_replica":local_replica,
                    "app_observation":{
                        "idle_data":idle_data,
                        "idle_data_error":app_logs.iter().rev()
                            .find_map(|line| line.split_once("LOCAL_IDLE_DATA_ERROR ").map(|(_, error)| error)),
                        "opens":app_logs.iter().filter(|line| line.contains("LOCAL_CALLBACK open")).count(),
                        "provider_acquisitions":app_logs.iter().filter(|line| line.contains("LOCAL_DATA_PROVIDER acquired")).count(),
                        "primary_last_admitted":app_logs.iter().rev().find_map(|line|
                            line.split_once("LOCAL_PRIMARY admitted sequence=").and_then(|(_, n)| n.trim().parse::<i64>().ok())),
                        "primary_last_acknowledged":app_logs.iter().rev().find_map(|line|
                            line.split_once("LOCAL_PRIMARY acknowledged sequence=").and_then(|(_, n)| n.trim().parse::<i64>().ok())),
                        "idle_access_denied":app_logs.iter().any(|line| line.contains("LOCAL_ACCESS denied")),
                        "idle_write_denials":app_logs.iter().filter(|line| line.contains("LOCAL_DATA write_denied NotPrimary assigned=0")).count(),
                    },
                    "generations": {
                        "proposed":generations.proposed, "receive":generations.receive,
                        "send":generations.send, "upload_pending":generations.upload_pending,
                        "upload_supported":!launch.profile.peer_replicas(),
                    },
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
    drop(upload);
    if let Some(link) = link.take() {
        let _ = link.close().await;
    }
    for connection in &mut connections {
        if let Some(link) = connection.link.take() {
            let _ = link.close().await;
        }
    }
    let receipt = cleanup(launch, &mut node, &store, &mut adapter).await?;
    write_json(&launch.directory.join("cleanup.json"), &receipt)?;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
async fn dial_peer(launch: &Launch, address: &str) -> Result<Link> {
    dial_peer_to(
        launch,
        launch.peers.first().ok_or("no original peer binding")?,
        address,
    )
    .await
}
async fn dial_peer_to(launch: &Launch, peer: &Peer, address: &str) -> Result<Link> {
    let address: std::net::SocketAddr = address.parse().map_err(err)?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err("peer requires original numeric loopback listener".into());
    }
    let channel =
        fube_transport::Endpoint::unsecured_tcp(address, wire::peer_composition().map_err(err)?)
            .map_err(err)?
            .channel_limits(wire::limits())
            .connect_until(wire::deadline())
            .await
            .map_err(err)?;
    Link::new(
        channel,
        launch.node.clone(),
        peer.node.clone(),
        launch.generation.clone(),
    )
}

#[cfg(test)]
async fn accept_peer(launch: &Launch, stream: tokio::net::TcpStream) -> Result<Link> {
    accept_peer_from(
        launch,
        launch.peers.first().ok_or("no original peer binding")?,
        stream,
    )
    .await
}
async fn accept_peer_from(
    launch: &Launch,
    peer: &Peer,
    stream: tokio::net::TcpStream,
) -> Result<Link> {
    let channel = fube_transport::accept_unsecured_tcp(
        stream,
        wire::peer_composition().map_err(err)?,
        wire::limits(),
        wire::deadline(),
    )
    .await
    .map_err(err)?;
    Link::new(
        channel,
        launch.node.clone(),
        peer.node.clone(),
        launch.generation.clone(),
    )
}

fn create_idle(launch: &Launch, node: &mut Node) -> Result<u64> {
    let peer = launch.peers.first().ok_or("no original peer binding")?;
    let package_version = actual_profile(node)?.version;
    node.create_idle_on_peer(Replica {
        node: peer.node.clone(),
        id: peer.replica,
        instance: peer.incarnation,
        previous_role: 1,
        current_role: 2,
        state: 1,
        up: true,
        version: package_version,
        ..Replica::default()
    })
    .map_err(err)
}

#[cfg(test)]
fn receive_peer(
    launch: &Launch,
    node: &mut Node,
    message: &fube_transport::message::Message,
) -> Result<Option<u64>> {
    receive_peer_from(
        launch,
        launch.peers.first().ok_or("unbound peer ingress")?,
        node,
        message,
    )
}
fn receive_peer_from(
    launch: &Launch,
    peer: &Peer,
    node: &mut Node,
    message: &fube_transport::message::Message,
) -> Result<Option<u64>> {
    wire::predecode(message)?;
    let action = message.properties().map_err(err)?.action;
    let result = match action.as_str() {
        "DoReconfiguration" if launch.placement == 2 => {
            let (body, generation) = decode_do_reconfiguration(message).map_err(err)?;
            node.continue_swap_from_peer(body, &peer.node, &generation)
        }
        "GetLSN" => {
            let (body, generation) = decode_get_lsn(message).map_err(err)?;
            node.get_lsn_from_peer(body, &peer.node, &generation)
        }
        "GetLSNReply" => {
            let (body, generation) = decode_get_lsn_reply(message).map_err(err)?;
            node.get_lsn_reply_from_peer(body, &peer.node, &generation)
        }
        "Deactivate" if launch.placement == 2 || launch.profile.mesh_peers() => {
            let (body, generation) = decode_deactivate(message).map_err(err)?;
            launch.check_service_sizes(&body.configuration.unit, &body.configuration.service)?;
            node.deactivate_replica_from_peer(body, &peer.node, &generation)
        }
        "DeactivateReply" if launch.placement == 1 || launch.profile.mesh_peers() => {
            let (body, generation) = decode_deactivate_reply(message).map_err(err)?;
            fixed_idle_target(
                launch,
                &body.unit.id,
                &body.replica,
                &peer.node,
                peer.replica,
                peer.incarnation,
            )?;
            node.deactivate_reply_from_peer(body, &peer.node, &generation)
        }
        "Activate" => {
            let (body, generation) = decode_activate(message).map_err(err)?;
            launch.check_service_sizes(&body.configuration.unit, &body.configuration.service)?;
            node.activate_replica_from_peer(body, &peer.node, &generation)
        }
        "ActivateReply" => {
            let (body, generation) = decode_activate_reply(message).map_err(err)?;
            fixed_idle_target(
                launch,
                &body.unit.id,
                &body.replica,
                &peer.node,
                peer.replica,
                peer.incarnation,
            )?;
            node.activate_reply_from_peer(body, &peer.node, &generation)
        }
        "CreateReplica" if launch.placement != 1 || launch.profile.mesh_peers() => {
            let (body, generation) = decode_create_replica(message).map_err(err)?;
            launch.check_service_sizes(&body.replica.0.unit, &body.replica.0.service)?;
            fixed_idle_target(
                launch,
                &body.replica.0.unit.id,
                &body.replica.0.replica,
                &launch.node,
                launch.replica,
                launch.incarnation,
            )?;
            node.create_replica_from_peer(body, &peer.node, &generation)
        }
        "CreateReplicaReply" if launch.placement == 1 || launch.profile.mesh_peers() => {
            let (body, generation) = decode_create_replica_reply(message).map_err(err)?;
            fixed_idle_target(
                launch,
                &body.unit.id,
                &body.replica,
                &peer.node,
                peer.replica,
                peer.incarnation,
            )?;
            node.create_replica_reply_from_peer(body, &peer.node, &generation)
        }
        _ => return Err("unsupported peer action for original placement".into()),
    };
    match result {
        Ok(id) => Ok(Some(id)),
        Err(fube_node::local::Error::Capacity) => Ok(None),
        Err(fube_node::local::Error::Busy) if action == "CreateReplica" => Ok(None),
        Err(error) => Err(err(error)),
    }
}

fn fixed_peer_target(
    launch: &Launch,
    partition: &PartitionId,
    replica: &Replica,
    node: &NodeInstance,
    id: i64,
    instance: i64,
) -> Result<()> {
    if partition.guid.0
        != uuid::Uuid::parse_str(&launch.partition)
            .map_err(err)?
            .to_bytes_le()
        || &replica.node != node
        || replica.id != id
        || replica.instance != instance
    {
        return Err("peer body is outside the original fixed partition/replica topology".into());
    }
    Ok(())
}

fn fixed_idle_target(
    launch: &Launch,
    partition: &PartitionId,
    replica: &Replica,
    node: &NodeInstance,
    id: i64,
    instance: i64,
) -> Result<()> {
    let expected = if (replica.id, replica.instance) == (id, instance) {
        (id, instance)
    } else if launch.profile.mesh_peers() && replica.id > id && replica.instance > instance {
        // The live RA validates the exact current or freshly admitted incarnation.
        (replica.id, replica.instance)
    } else {
        launch.idle_successor()?
    };
    fixed_peer_target(launch, partition, replica, node, expected.0, expected.1)
}

fn replace_replica_host(launch: &Launch, node: &mut Node) -> Result<HostReplacementReceipt> {
    let predecessor = actual_profile(node)?;
    let old_host = node
        .activation()
        .identity()
        .ok_or("missing retired host identity")?
        .host_id
        .clone();
    let mut options = Options::new(launch.directory.join("hosting"));
    options.profile = fube_hosting::local::Profile::VolatileStatefulLifecycle;
    options.service_package_version_instance = Some(1);
    options.node = Some(NodeContext {
        id: node_hex(&launch.node),
        instance: launch.node.instance,
        name: launch.name(),
        ip: std::net::Ipv4Addr::LOCALHOST.into(),
    });
    node.replace_lost_replica_host(Activation::new(options).map_err(err)?)
        .map_err(err)?;
    node.start(
        &launch.package,
        APPLICATION,
        &[(
            "Arguments".into(),
            "--stateful-lifecycle --data-replicator".into(),
        )],
    )
    .map_err(err)?;
    let profile = actual_profile(node)?;
    profile.validate(launch)?;
    let new_host = node
        .activation()
        .identity()
        .ok_or("missing replacement host identity")?
        .host_id
        .clone();
    Ok(HostReplacementReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node_pid: std::process::id(),
        predecessor,
        profile,
        old_host,
        new_host,
    })
}
fn publish_host_loss(launch: &Launch, node: &Node, published: &mut bool) -> Result<()> {
    if *published {
        return Ok(());
    }
    let Some(loss) = node.replica_loss() else {
        return Ok(());
    };
    let profile = actual_profile(node)?;
    let proof = HostLossReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        fm: launch.fm.clone(),
        generation: launch.generation.clone(),
        node_pid: std::process::id(),
        app_pid: profile.app_pid,
        runtime_root: profile.runtime_root,
        loss,
    };
    write_json(&launch.directory.join("host-loss.json"), &proof)?;
    *published = true;
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
fn publish_absence(launch: &Launch, node: &Node, published: &mut Option<(i64, i64)>) -> Result<()> {
    let Some(absence) = node.replica_absence() else {
        return Ok(());
    };
    let lifetime = (absence.body.replica.id, absence.body.replica.instance);
    if *published == Some(lifetime) {
        return Ok(());
    }
    let profile = actual_profile(node)?;
    let receipt = AbsenceReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        fm: launch.fm.clone(),
        generation: launch.generation.clone(),
        node_pid: std::process::id(),
        app_pid: profile.app_pid,
        runtime_root: profile.runtime_root,
        absence,
    };
    write_json(&launch.directory.join("absence.json"), &receipt)?;
    *published = Some(lifetime);
    Ok(())
}
fn publish_pre_open_cancellation(
    launch: &Launch,
    node: &Node,
    published: &mut Option<(i64, i64)>,
) -> Result<()> {
    let Some(cancellation) = node.pre_open_cancellation() else {
        return Ok(());
    };
    let lifetime = (
        cancellation.body.replica.id,
        cancellation.body.replica.instance,
    );
    if *published == Some(lifetime) {
        return Ok(());
    }
    let profile = actual_profile(node)?;
    let receipt = PreOpenCancellationReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        fm: launch.fm.clone(),
        generation: launch.generation.clone(),
        node_pid: std::process::id(),
        app_pid: profile.app_pid,
        runtime_root: profile.runtime_root,
        cancellation,
    };
    write_json(
        &launch.directory.join("pre-open-cancellation.json"),
        &receipt,
    )?;
    *published = Some(lifetime);
    Ok(())
}
fn publish_failed_registration(
    launch: &Launch,
    node: &Node,
    published: &mut Option<(i64, i64)>,
) -> Result<()> {
    let Some(deletion) = node.failed_registration_deletion() else {
        return Ok(());
    };
    let lifetime = (
        deletion.failure.body.replica.id,
        deletion.failure.body.replica.instance,
    );
    if *published == Some(lifetime) {
        return Ok(());
    }
    let profile = actual_profile(node)?;
    let receipt = FailedRegistrationReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        fm: launch.fm.clone(),
        generation: launch.generation.clone(),
        node_pid: std::process::id(),
        app_pid: profile.app_pid,
        runtime_root: profile.runtime_root,
        deletion,
    };
    write_json(
        &launch.directory.join("failed-registration-deletion.json"),
        &receipt,
    )?;
    *published = Some(lifetime);
    Ok(())
}
fn publish_failed_open(
    launch: &Launch,
    node: &Node,
    published: &mut Option<(i64, i64)>,
) -> Result<()> {
    let Some(deletion) = node.failed_open_deletion() else {
        return Ok(());
    };
    let replica = &deletion.failure.disposal.body.replica;
    let lifetime = (replica.id, replica.instance);
    if *published == Some(lifetime) {
        return Ok(());
    }
    let profile = actual_profile(node)?;
    let receipt = FailedOpenReceipt {
        deployment: launch.deployment.clone(),
        session: launch.session.clone(),
        node: launch.node.clone(),
        fm: launch.fm.clone(),
        generation: launch.generation.clone(),
        node_pid: std::process::id(),
        app_pid: profile.app_pid,
        runtime_root: profile.runtime_root,
        deletion,
    };
    write_json(
        &launch.directory.join("failed-open-deletion.json"),
        &receipt,
    )?;
    *published = Some(lifetime);
    Ok(())
}

fn receive_command(
    launch: &Launch,
    node: &mut Node,
    message: &fube_transport::message::Message,
) -> Result<Option<(u64, SemanticAck)>> {
    wire::predecode(message)?;
    match message.properties().map_err(err)?.action.as_str() {
        "ServiceTypeEnabledReply" | "ServiceTypeDisabledReply" => {
            let enabled = message.properties().map_err(err)?.action == "ServiceTypeEnabledReply";
            let (body, generation) = if enabled {
                decode_service_type_enabled_reply(message)
            } else {
                decode_service_type_disabled_reply(message)
            }
            .map_err(err)?;
            node.fm_work(FmWork::TypeStatusReply(
                enabled,
                body,
                launch.fm.clone(),
                generation,
            ))
            .map_err(err)?;
        }
        "NodeActivateRequest" => {
            let (body, generation) = decode_node_activate_request(message).map_err(err)?;
            node.fm_work(FmWork::NodeActivate(body, launch.fm.clone(), generation))
                .map_err(err)?;
        }
        "NodeDeactivateRequest" => {
            let (body, generation) = decode_node_deactivate_request(message).map_err(err)?;
            node.fm_work(FmWork::NodeDeactivate(body, launch.fm.clone(), generation))
                .map_err(err)?;
        }
        "NodeUpdateServiceRequest" => {
            let (body, generation) = decode_node_update_service_request(message).map_err(err)?;
            node.fm_work(FmWork::NodeUpdateService(
                Box::new(body),
                launch.fm.clone(),
                generation,
            ))
            .map_err(err)?;
        }
        "AddInstance" => {
            let (body, generation) = decode_add_instance(message).map_err(err)?;
            node.add_from_fm(body, &launch.fm, &generation)
                .map_err(err)?;
        }
        "AddPrimary" => {
            let (body, generation) = decode_add_primary(message).map_err(err)?;
            launch.check_service_sizes(&body.unit, &body.service)?;
            node.add_primary_from_fm(body, &launch.fm, &generation)
                .map_err(err)?;
        }
        "DoReconfiguration" => {
            if !launch.profile.peer_replicas()
                || (launch.placement != 1
                    && !launch.profile.mesh_peers()
                    && launch.profile.configured_sizes().is_none())
            {
                return Err("DoReconfiguration requires the original coordinating Primary".into());
            }
            let (body, generation) = decode_do_reconfiguration(message).map_err(err)?;
            launch.check_service_sizes(&body.configuration.unit, &body.configuration.service)?;
            node.reconfigure_from_fm(body, &launch.fm, &generation)
                .map_err(err)?;
        }
        "ReplicaEndpointUpdatedReply" => {
            let (body, generation) = decode_replica_endpoint_updated_reply(message).map_err(err)?;
            node.endpoint_reply_from_fm(body, &launch.fm, &generation)
                .map_err(err)?;
        }
        "AddReplica" | "RemoveReplica" => {
            let removing = message.properties().map_err(err)?.action == "RemoveReplica";
            let (body, generation) = if removing {
                decode_remove_replica(message)
            } else {
                decode_add_replica(message)
            }
            .map_err(err)?;
            launch.check_service_sizes(&body.unit, &body.service)?;
            let peer = launch
                .peers
                .iter()
                .find(|peer| peer.node == body.replica.node)
                .filter(|_| launch.placement == 1 || launch.profile.mesh_peers())
                .ok_or("replica control requires the original coordinating Primary")?;
            fixed_idle_target(
                launch,
                &body.unit.id,
                &body.replica,
                &peer.node,
                peer.replica,
                peer.incarnation,
            )?;
            if removing {
                node.remove_replica_from_fm(body, &launch.fm, &generation)
            } else {
                node.add_replica_from_fm(body, &launch.fm, &generation)
            }
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
        "DeleteReplica" => {
            let (body, generation) = decode_delete_replica(message).map_err(err)?;
            if launch.profile == ServiceProfile::StatefulPeerIdle && launch.placement == 2 {
                let incoming = &body.replica.0;
                // Native Delete matches ReplicaId, not FM's potentially older instance.
                let (id, expected_instance) = if incoming.replica.id == launch.replica {
                    (launch.replica, launch.incarnation)
                } else {
                    launch.idle_successor()?
                };
                let instance = if incoming.replica.instance < expected_instance
                    && node
                        .snapshot(incoming.unit.id.guid.0)
                        .committed
                        .is_some_and(|state| state.peer_creation.is_some())
                {
                    incoming.replica.instance
                } else {
                    expected_instance
                };
                fixed_peer_target(
                    launch,
                    &body.replica.0.unit.id,
                    &body.replica.0.replica,
                    &launch.node,
                    id,
                    instance,
                )?;
            }
            node.fm_work(FmWork::DeleteReplica(
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
async fn receive_generation(
    launch: &Launch,
    node: &Node,
    link: &Link,
    incoming: &mut fube_transport::channel::IncomingMessage<
        fube_transport::native_channel::GatewayTarget,
    >,
) -> Result<bool> {
    let message = &incoming.message;
    match message.properties().map_err(err)?.action.as_str() {
        "GenerationProposal" => {
            wire::predecode(message)?;
            let header = decode_generation_proposal(message).map_err(err)?;
            let body = node
                .process_generation_proposal(&launch.fm, &header)
                .map_err(err)?;
            let reply = generation_proposal_reply_message(&body).map_err(err)?;
            incoming
                .reply
                .take()
                .ok_or("GenerationProposal missing consuming request context")?
                .reply(link.seal(reply)?, wire::deadline())
                .await
                .map_err(err)?;
        }
        "GenerationUpdate" => {
            wire::predecode(message)?;
            let (body, header) = decode_generation_update(message).map_err(err)?;
            match node
                .process_generation_update(&launch.fm, &body, &header)
                .map_err(err)?
            {
                ra::GenerationUpdateOutcome::Rejected(proposed) => {
                    link.send(generation_update_reject_message(&proposed).map_err(err)?)
                        .await?;
                }
                ra::GenerationUpdateOutcome::UploadRequired => link.advance_generation(&header),
                _ => {}
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
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

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum StatefulDown {
        Crash,
        LeaseLoss,
        CloseProvider,
        CloseService,
        DropProvider,
        DropService,
        AbortProvider,
        AbortService,
        ForceAbortProvider,
        ForceAbortService,
        ForceDelete,
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum NodeClose {
        Provider,
        Service,
        Timeout,
        Cancel,
    }

    #[derive(Default)]
    struct Traffic {
        reports: Vec<ReplicaUp>,
        added: bool,
        removed: bool,
        add_reply: Option<AddInstanceReply>,
        remove_reply: Option<AddInstanceReply>,
        idle_removals: Vec<RemoveReplicaReply>,
        idle_adds: Vec<AddReplicaReply>,
        activations: Vec<i64>,
        deactivations: Vec<i64>,
        service_updates: Vec<NodeUpdateServiceReply>,
        loads: Vec<ReportLoad>,
        statuses: Vec<(bool, ServiceTypeUpdate)>,
    }

    async fn links(launch: &Launch) -> Result<(Link, Link)> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
        let endpoint = fube_transport::Endpoint::unsecured_tcp(
            listener.local_addr().map_err(err)?,
            wire::composition(true).map_err(err)?,
        )
        .map_err(err)?
        .channel_limits(wire::limits());
        let (fm, node) = tokio::join!(endpoint.connect_until(wire::deadline()), async {
            let (stream, _) = listener.accept().await.map_err(err)?;
            fube_transport::accept_unsecured_tcp(
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
                "NodeActivateReply" | "NodeDeactivateReply" => {
                    let activate =
                        incoming.message.properties().map_err(err)?.action == "NodeActivateReply";
                    let (body, owner) = if activate {
                        decode_node_activate_reply(&incoming.message)
                    } else {
                        decode_node_deactivate_reply(&incoming.message)
                    }
                    .map_err(err)?;
                    if owner != Owner::Fm {
                        return Err("unexpected FMM node reply".into());
                    }
                    if activate {
                        traffic.activations.push(body.sequence);
                    } else {
                        traffic.deactivations.push(body.sequence);
                    }
                }
                "NodeUpdateServiceReply" => {
                    let (body, owner) =
                        decode_node_update_service_reply(&incoming.message).map_err(err)?;
                    if owner != Owner::Fm {
                        return Err("unexpected FMM service reply".into());
                    }
                    traffic.service_updates.push(body);
                }
                "ReportLoad" => {
                    traffic
                        .loads
                        .push(decode_report_load(&incoming.message).map_err(err)?);
                }
                action @ ("ServiceTypeEnabled" | "ServiceTypeDisabled") => {
                    let enabled = action == "ServiceTypeEnabled";
                    let body = if enabled {
                        decode_service_type_enabled(&incoming.message)
                    } else {
                        decode_service_type_disabled(&incoming.message)
                    }
                    .map_err(err)?;
                    let reply = ServiceTypeUpdateReply {
                        sequence: body.sequence,
                    };
                    fm.send(
                        if enabled {
                            service_type_enabled_reply_message(&reply, &launch.generation)
                        } else {
                            service_type_disabled_reply_message(&reply, &launch.generation)
                        }
                        .map_err(err)?,
                    )
                    .await?;
                    traffic.statuses.push((enabled, body));
                }
                "ServiceTypeNotification" => {
                    let body = decode_service_type_notification(&incoming.message).map_err(err)?;
                    fm.send(
                        service_type_notification_reply_message(&body, &launch.generation)
                            .map_err(err)?,
                    )
                    .await?;
                }
                action @ ("AddInstanceReply" | "AddPrimaryReply") => {
                    let (body, _) = if action == "AddPrimaryReply" {
                        decode_add_primary_reply(&incoming.message)
                    } else {
                        decode_add_instance_reply(&incoming.message)
                    }
                    .map_err(err)?;
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
                action @ ("RemoveInstanceReply" | "DeleteReplicaReply") => {
                    let (body, _) = if action == "DeleteReplicaReply" {
                        decode_delete_replica_reply(&incoming.message)
                    } else {
                        decode_remove_instance_reply(&incoming.message)
                    }
                    .map_err(err)?;
                    if body.error != 0 || body.replica.state != 3 {
                        return Err("real removal did not dispose the instance".into());
                    }
                    traffic.removed = true;
                    traffic.remove_reply = Some(body);
                }
                "RemoveReplicaReply" => {
                    let (body, owner) =
                        decode_remove_replica_reply(&incoming.message).map_err(err)?;
                    if owner != Owner::Fm || body.error != 0 || traffic.idle_removals.len() == 4 {
                        return Err("unexpected Idle removal reply or unbounded retry".into());
                    }
                    traffic.idle_removals.push(body);
                }
                "AddReplicaReply" => {
                    let (body, owner) = decode_add_replica_reply(&incoming.message).map_err(err)?;
                    if owner != Owner::Fm || traffic.idle_adds.len() == 4 {
                        return Err("unexpected Idle build reply or unbounded retry".into());
                    }
                    traffic.idle_adds.push(body);
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
        until_with_budget(
            launch,
            node,
            adapter,
            fm,
            link,
            traffic,
            Duration::from_secs(10),
            ready,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn until_with_budget(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        traffic: &mut Traffic,
        budget: Duration,
        ready: impl Fn(&Node, &Adapter, &Traffic) -> bool,
    ) -> Result<()> {
        let end = Instant::now() + budget;
        while !ready(node, adapter, traffic) {
            if Instant::now() >= end {
                return Err(format!(
                    "live adapter/RA progress deadline: added={} service_replies={} loads={} activate={:?} deactivate={:?} commands={} error={:?} logs={:?}",
                    traffic.added, traffic.service_updates.len(), traffic.loads.len(), traffic.activations,
                    traffic.deactivations, node.pending_commands(), node.last_error(), node.activation().logs(),
                ));
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

    async fn data_request(endpoint: &str, command: &[u8]) -> Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut socket = tokio::net::TcpStream::connect(endpoint)
                .await
                .map_err(err)?;
            socket.write_all(command).await.map_err(err)?;
            let mut response = String::new();
            socket
                .take(512)
                .read_to_string(&mut response)
                .await
                .map_err(err)?;
            Ok(response)
        })
        .await
        .map_err(err)?
    }

    async fn exercise_node_coordination(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        mode: NodeClose,
    ) -> Result<()> {
        let body = add_body(launch, &actual_profile(node)?);
        let entity = body.unit.id.guid.0;
        let mut traffic = Traffic::default();
        fm.send(add_primary_message(&body, &launch.generation).map_err(err)?)
            .await?;
        until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
            t.added
        })
        .await?;
        let endpoint = traffic
            .add_reply
            .as_ref()
            .ok_or("missing original Primary")?
            .replica
            .service_endpoint
            .clone();
        let identity = node
            .activation()
            .identity()
            .ok_or("missing original host")?
            .clone();
        let usage = node
            .snapshot(entity)
            .committed
            .ok_or("missing original state")?
            .usage;
        let ready_end = Instant::now() + Duration::from_secs(2);
        loop {
            let response = data_request(&endpoint, b"read\n").await?;
            if response.starts_with("OK ") {
                break;
            }
            if response != "ERR NotPrimary\n" || Instant::now() >= ready_end {
                return Err(format!("public Primary readiness failed: {response}"));
            }
            step(launch, node, adapter, fm, link, &mut traffic).await?;
        }
        let mut service = body.service.clone();
        service.update_version = 1;
        service.metrics = fube_transport::serialization::serde::ObjectArray(vec![Metric {
            name: "LocalWork".into(),
            weight: 1,
            primary: 0,
            secondary: 0,
        }]);
        fm.send(
            node_update_service_request_message(
                &NodeUpdateServiceRequest {
                    service: service.clone(),
                },
                &launch.generation,
            )
            .map_err(err)?,
        )
        .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, _, t| {
                t.service_updates.len() == 1
                    && node
                        .snapshot(entity)
                        .committed
                        .is_some_and(|state| !state.service_description_pending)
            },
        )
        .await?;
        if traffic.service_updates[0].version != 1
            || node
                .snapshot(entity)
                .committed
                .is_none_or(|state| state.body.service != service || state.usage != usage)
            || node
                .activation()
                .identity()
                .is_none_or(|current| current.host_id != identity.host_id)
        {
            return Err(
                "service update changed original lifetime or did not commit native description"
                    .into(),
            );
        }
        let report = data_request(&endpoint, b"load\n").await?;
        if report != "OK load=7\n" {
            return Err(format!(
                "public partition report_load failed after real RAP description update: {report}"
            ));
        }
        until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
            !t.loads.is_empty()
        })
        .await?;
        let load = &traffic.loads[0];
        if !load.legacy.0.is_empty()
            || load.reports.0.len() != 1
            || load.reports.0[0].partition != body.unit.id.guid
            || load.reports.0[0].service != service.name
            || !load.reports.0[0].stateful
            || load.reports.0[0].primary.0.len() != 1
            || load.reports.0[0].primary.0[0].name != "LocalWork"
            || load.reports.0[0].primary.0[0].value != 7
        {
            return Err("public load was not forwarded with native Primary identity/value".into());
        }
        if !data_request(&endpoint, b"write\n")
            .await?
            .contains("assigned=1 applied=1 committed=1 value=acknowledged")
        {
            return Err("node-control fixture did not retain real nonempty data".into());
        }
        if mode == NodeClose::Timeout {
            node.native_config()
                .set_node(ra::NodeProperty::DeactivationCloseTimeoutTicks, 0);
            node.native_config()
                .set_node(ra::NodeProperty::DeactivationCheckIntervalTicks, 100_000);
        }
        let inactive = NodeActivationRequest {
            sequence: 2,
            from_fmm: false,
        };
        fm.send(node_deactivate_request_message(&inactive, &launch.generation).map_err(err)?)
            .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, _, _| {
                node.snapshot(entity)
                    .committed
                    .is_some_and(|state| state.phase == ra::Phase::Closing)
            },
        )
        .await?;
        if mode != NodeClose::Timeout {
            let marker = if mode == NodeClose::Service {
                "LOCAL_DISPOSE begin"
            } else {
                "LOCAL_DATA_PROVIDER dispose begin"
            };
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |node, _, _| {
                    node.activation()
                        .logs()
                        .iter()
                        .any(|line| line.contains(marker))
                },
            )
            .await?;
            for _ in 0..10 {
                step(launch, node, adapter, fm, link, &mut traffic).await?;
            }
            if !traffic.deactivations.is_empty()
                || node.instance_disposal().is_some()
                || node
                    .snapshot(entity)
                    .committed
                    .is_none_or(|state| state.usage != usage)
            {
                return Err("DeactivateReply escaped held original disposal".into());
            }
            fm.send(node_activate_request_message(&inactive, &launch.generation).map_err(err)?)
                .await?;
            until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
                t.activations == vec![2]
            })
            .await?;
            if mode == NodeClose::Cancel {
                fm.send(
                    node_activate_request_message(
                        &NodeActivationRequest {
                            sequence: 3,
                            from_fmm: false,
                        },
                        &launch.generation,
                    )
                    .map_err(err)?,
                )
                .await?;
                until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
                    t.activations == vec![2, 3]
                })
                .await?;
            }
            release_disposal(
                node,
                if mode == NodeClose::Service {
                    StatefulDown::CloseService
                } else {
                    StatefulDown::CloseProvider
                },
            )?;
        }
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, _, t| {
                node.snapshot(entity)
                    .committed
                    .is_some_and(|state| state.phase == ra::Phase::Closed)
                    && (mode == NodeClose::Cancel || t.deactivations == vec![2])
            },
        )
        .await?;
        if mode == NodeClose::Cancel && !traffic.deactivations.is_empty() {
            return Err("cancelled close monitor sent obsolete deactivation reply".into());
        }
        if mode == NodeClose::Timeout {
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |node, _, _| {
                    node.activation()
                        .process_status()
                        .is_some_and(|status| status.exit_observed)
                },
            )
            .await?;
            if node.instance_disposal().is_some() {
                return Err("forced host termination fabricated disposal proof".into());
            }
        } else {
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |node, _, _| node.instance_disposal().is_some(),
            )
            .await?;
        }
        fm.send(
            node_activate_request_message(
                &NodeActivationRequest {
                    sequence: 4,
                    from_fmm: false,
                },
                &launch.generation,
            )
            .map_err(err)?,
        )
        .await?;
        until(launch, node, adapter, fm, link, &mut traffic, |_, _, t| {
            t.activations.last() == Some(&4)
        })
        .await?;
        if node
            .snapshot(entity)
            .committed
            .is_none_or(|state| state.phase != ra::Phase::Closed)
            || node
                .activation()
                .logs()
                .iter()
                .filter(|line| line.contains("LOCAL_CALLBACK create_replica"))
                .count()
                != 1
        {
            return Err("node activation adopted or recreated lost volatile data".into());
        }
        Ok(())
    }

    pub(crate) async fn run_node_coordination(
        launch: &Launch,
        mode: NodeClose,
    ) -> Result<Result<()>> {
        let (mut node, store) = create_node(launch).await?;
        let mut adapter = Adapter::default();
        let result = async {
            node.start(
                &launch.package,
                APPLICATION,
                &[(
                    "Arguments".into(),
                    if mode == NodeClose::Service {
                        "--stateful-primary --block-dispose"
                    } else {
                        "--stateful-primary --block-provider-dispose"
                    }
                    .into(),
                )],
            )
            .map_err(err)?;
            let (fm, link) = links(launch).await?;
            let result =
                exercise_node_coordination(launch, &mut node, &mut adapter, &fm, &link, mode).await;
            if mode != NodeClose::Timeout {
                release_disposal(
                    &node,
                    if mode == NodeClose::Service {
                        StatefulDown::CloseService
                    } else {
                        StatefulDown::CloseProvider
                    },
                )?;
            }
            let fm_closed = fm.close().await;
            let node_closed = link.close().await;
            result.and(fm_closed).and(node_closed)
        }
        .await;
        cleanup(launch, &mut node, &store, &mut adapter).await?;
        Ok(result)
    }

    fn release_disposal(node: &Node, mode: StatefulDown) -> Result<()> {
        release_fixture_barrier(
            node,
            if matches!(
                mode,
                StatefulDown::CloseProvider
                    | StatefulDown::DropProvider
                    | StatefulDown::AbortProvider
                    | StatefulDown::ForceAbortProvider
            ) {
                "local-provider-dispose.release"
            } else {
                "local-dispose.release"
            },
        )
    }

    fn release_fixture_barrier(node: &Node, name: &str) -> Result<()> {
        let application = &node
            .activation()
            .premise()
            .ok_or("missing package")?
            .package
            .package
            .application
            .type_name;
        let path = node
            .activation()
            .root()
            .ok_or("missing owned root")?
            .join(format!("deployment/{application}_App1/work"))
            .join(name);
        fs::write(path, b"release original callback/disposer").map_err(err)
    }

    async fn exercise_stateful_down(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
        ledger: &mut crate::fm::Ledger,
        mode: StatefulDown,
    ) -> Result<()> {
        use crate::fm::{Accepted, Bootstrap, Command, Observed};
        let index = usize::from(launch.placement - 1);
        ledger.placements[index].bootstrap = Bootstrap::Metadata;
        ledger.placements[index].admit_profile(actual_profile(node)?)?;
        let Accepted::Create(_, body) = ledger.accept(Command {
            op: "loss-primary".into(),
            action: "create".into(),
            node: Some(launch.placement),
        })?
        else {
            return Err("original Primary create not accepted".into());
        };
        let entity = body.unit.id.guid.0;
        let mut traffic = Traffic::default();
        fm.send(add_primary_message(&body, &launch.generation).map_err(err)?)
            .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |_, _, traffic| traffic.added,
        )
        .await?;
        ledger.reply(
            index,
            traffic.add_reply.clone().ok_or("missing Primary reply")?,
            false,
        )?;
        let endpoint = ledger.placements[index]
            .endpoint
            .clone()
            .ok_or("missing Primary endpoint")?;
        let ready_end = Instant::now() + Duration::from_secs(2);
        loop {
            let response = data_request(&endpoint, b"read\n").await?;
            if response.starts_with("OK ") {
                break;
            }
            if response != "ERR NotPrimary\n" || Instant::now() >= ready_end {
                return Err(format!("public Primary readiness failed: {response}"));
            }
            step(launch, node, adapter, fm, link, &mut traffic).await?;
        }
        let written = data_request(&endpoint, b"write\n").await?;
        if !written.contains("assigned=1 applied=1 committed=1 value=acknowledged")
            || data_request(&endpoint, b"read\n").await? != written
        {
            return Err("independent public write/readback did not complete".into());
        }
        let close_mode = match mode {
            StatefulDown::CloseProvider | StatefulDown::CloseService => Some(ra::CloseMode::Close),
            StatefulDown::DropProvider | StatefulDown::DropService => Some(ra::CloseMode::Drop),
            StatefulDown::AbortProvider | StatefulDown::AbortService => Some(ra::CloseMode::Abort),
            StatefulDown::ForceAbortProvider | StatefulDown::ForceAbortService => {
                Some(ra::CloseMode::ForceAbort)
            }
            StatefulDown::Crash | StatefulDown::LeaseLoss | StatefulDown::ForceDelete => None,
        };
        let graceful = close_mode.is_some();
        let expected_stage = if matches!(
            close_mode,
            Some(ra::CloseMode::Drop | ra::CloseMode::Abort | ra::CloseMode::ForceAbort)
        ) || mode == StatefulDown::ForceDelete
        {
            ra::FmStage::ReplicaDropped
        } else {
            ra::FmStage::ReplicaDown
        };
        let replication_endpoint = traffic
            .add_reply
            .as_ref()
            .ok_or("missing original reply")?
            .replica
            .replication_endpoint
            .clone();
        let replication_address = replication_endpoint
            .split_once('/')
            .ok_or("replication endpoint lacks native actor suffix")?
            .0;
        if let Some(close_mode) = close_mode {
            node.close(body.unit.id.clone(), close_mode).map_err(err)?;
            let provider_barrier = matches!(
                mode,
                StatefulDown::CloseProvider
                    | StatefulDown::DropProvider
                    | StatefulDown::AbortProvider
                    | StatefulDown::ForceAbortProvider
            );
            let marker = if provider_barrier {
                "LOCAL_DATA_PROVIDER dispose begin"
            } else {
                "LOCAL_DISPOSE begin"
            };
            until(
                launch,
                node,
                adapter,
                fm,
                link,
                &mut traffic,
                |node, _, _| {
                    node.activation()
                        .logs()
                        .iter()
                        .any(|line| line.contains(marker))
                },
            )
            .await?;
            node.close(body.unit.id.clone(), close_mode).map_err(err)?;
            for _ in 0..20 {
                step(launch, node, adapter, fm, link, &mut traffic).await?;
            }
            if node.snapshot(entity).committed.is_none_or(|state| {
                state.phase != ra::Phase::Closing
                    || state.usage.is_none()
                    || state.fm_stage != ra::FmStage::None
            }) || node.retained_usage() != 1
                || node.instance_disposal().is_some()
                || !traffic.reports.is_empty()
                || traffic.removed
                || adapter.report_pending.is_some()
            {
                return Err(
                    "held graceful disposal published completion/report or released usage".into(),
                );
            }
            if close_mode != ra::CloseMode::Close {
                if provider_barrier {
                    for command in [b"write\n".as_slice(), b"read\n".as_slice()] {
                        let response = data_request(&endpoint, command).await?;
                        if response != "ERR NotPrimary\n" && response != "ERR ObjectClosed\n" {
                            return Err(format!(
                                "held terminal disposal left public access enabled: {response}"
                            ));
                        }
                    }
                } else if tokio::net::TcpStream::connect(&endpoint).await.is_ok()
                    || tokio::net::TcpStream::connect(replication_address)
                        .await
                        .is_ok()
                {
                    return Err(
                        "held service disposal retained an application/replication listener".into(),
                    );
                }
            }
            release_disposal(node, mode)?;
        } else if mode == StatefulDown::LeaseLoss {
            node.activation()
                .host_lease()
                .ok_or("missing original lease")?
                .stop_renewal();
        } else if mode == StatefulDown::ForceDelete {
            fm.send(
                delete_replica_message(
                    &DeleteReplica::new((*body).clone(), true),
                    &launch.generation,
                )
                .map_err(err)?,
            )
            .await?;
        }
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |_, _, traffic| !traffic.reports.is_empty(),
        )
        .await?;
        let report = traffic.reports[0].clone();
        let state = node
            .snapshot(entity)
            .committed
            .ok_or("lost FT disappeared")?;
        if state.phase != ra::Phase::Closed
            || state.fm_stage != expected_stage
            || state.local_deleted
            || node.retained_usage() != 0
            || node.instance_disposal().is_some() != graceful
            || adapter.report_pending.as_ref() != Some(&report)
        {
            return Err(
                "terminal completion did not retain the exact native reporting stage".into(),
            );
        }
        let disposal = node.instance_disposal();
        fm.send(add_primary_message(&body, &launch.generation).map_err(err)?)
            .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |_, _, traffic| traffic.reports.len() == 2,
        )
        .await?;
        if traffic.reports[1] != report
            || node
                .snapshot(entity)
                .committed
                .is_none_or(|state| state.fm_stage != expected_stage || state.usage.is_some())
        {
            return Err(
                "closed AddPrimary changed the original stage or allocated a lifetime".into(),
            );
        }
        if let Some(proof) = &disposal {
            if proof.body.replica.id != body.replica.id
                || proof.body.replica.instance != body.replica.instance
                || proof.body.replica.current_role != 4
                || proof.body.unit != body.unit
                || tokio::net::TcpStream::connect(&endpoint).await.is_ok()
                || tokio::net::TcpStream::connect(replication_address)
                    .await
                    .is_ok()
            {
                return Err(
                    "graceful disposal lacks original lineage or joined listener release".into(),
                );
            }
            if node
                .activation()
                .process_status()
                .is_none_or(|status| status.exit_observed)
            {
                return Err("graceful Close lost the original app host".into());
            }
        }
        let reply = ledger
            .report(index, &report)?
            .ok_or("loss report was not processed")?;
        if reply.replicas != report.dropped
            || !reply.dropped.0.is_empty()
            || ledger.placements[index].observed != Observed::Lost
            || ledger.placements[index].retirement.is_some()
        {
            return Err("FM did not process loss into the native processed list".into());
        }
        if timer(adapter)?
            .due
            .saturating_duration_since(Instant::now())
            < Duration::from_secs(25)
        {
            return Err(
                "stateful report retry did not retain the native 30-second interval".into(),
            );
        }
        for stale in 0..4 {
            let mut invalid = reply.clone();
            let info = &mut invalid.replicas.0[0];
            match stale {
                0 => info.replicas.0[0].description.up = true,
                1 => info.replicas.0[0].description.instance -= 1,
                2 => info.unit.current.configuration -= 1,
                _ if expected_stage == ra::FmStage::ReplicaDropped => {
                    info.replicas.0[0].description.state = 2;
                }
                _ => info.replicas.0[0].description.node.instance += 1,
            }
            acknowledge_without_sending_captured_report(launch, node, adapter, fm, link, &invalid)
                .await?;
            adapter.advance(node, Some(link)).await?;
            if node
                .snapshot(entity)
                .committed
                .is_none_or(|s| s.fm_stage != expected_stage)
                || adapter.report_pending.is_none()
                || node.instance_disposal().is_some() != graceful
            {
                return Err("stale ACK retired the original loss obligation".into());
            }
        }
        if matches!(
            mode,
            StatefulDown::LeaseLoss
                | StatefulDown::CloseProvider
                | StatefulDown::DropProvider
                | StatefulDown::ForceAbortProvider
        ) {
            // The interval is checked above; fire its retained owner without wall-clock delay.
            timer(adapter)?.due = Instant::now();
            let retry_end = Instant::now() + Duration::from_secs(10);
            let before_retry = traffic.reports.len();
            while traffic.reports.len() == before_retry {
                if Instant::now() >= retry_end {
                    return Err("lost ACK did not retry".into());
                }
                step(launch, node, adapter, fm, link, &mut traffic).await?;
            }
            if traffic.reports.last() != Some(&report) || node.retained_usage() != 0 {
                return Err("retry changed lifetime or acquired another use".into());
            }
        }
        fm.send(replica_up_reply_message(&reply, &launch.generation).map_err(err)?)
            .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, adapter, traffic| traffic.removed && adapter.replica_drained(node),
        )
        .await?;
        let state = node
            .snapshot(entity)
            .committed
            .ok_or("closed FT disappeared")?;
        if state.fm_stage != ra::FmStage::None
            || !state.local_deleted
            || !state.cleanup_pending
            || node.instance_disposal().is_some() != graceful
            || node.retained_usage() != 0
        {
            return Err("native terminal ACK did not delete only the closed metadata".into());
        }
        if let Some(original) = &disposal {
            let after = node
                .instance_disposal()
                .ok_or("ACK lost the original disposal proof")?;
            if serde_json::to_value(original).map_err(err)?
                != serde_json::to_value(after).map_err(err)?
            {
                return Err("metadata ACK changed original disposal lineage".into());
            }
        }
        ledger.reply(
            index,
            traffic
                .remove_reply
                .clone()
                .ok_or("missing native DeleteReplicaReply")?,
            true,
        )?;
        if ledger.placements[index].observed != Observed::Lost
            || ledger.placements[index].reusable
            || ledger.placements[index].retirement.is_some()
        {
            return Err("native deletion was mistaken for disposal/recovery".into());
        }
        if let Some(disposal) = &disposal {
            let profile = actual_profile(node)?;
            let receipt = RetirementReceipt {
                deployment: launch.deployment.clone(),
                session: launch.session.clone(),
                node: launch.node.clone(),
                fm: launch.fm.clone(),
                generation: launch.generation.clone(),
                node_pid: std::process::id(),
                app_pid: profile.app_pid,
                runtime_root: profile.runtime_root,
                disposal: disposal.clone(),
            };
            let path = launch.directory.join("retirement.json");
            write_json(&path, &receipt)?;
            let before = ledger.status();
            ledger.read_retirement(index, &path, std::process::id())?;
            if ledger.status() != before || !ledger.placements[index].link_alive {
                return Err(
                    "valid unsolicited disposal retired loss or broke the live session".into(),
                );
            }
        }
        let before = traffic.reports.len();
        fm.send(replica_up_reply_message(&reply, &launch.generation).map_err(err)?)
            .await?;
        for _ in 0..20 {
            step(launch, node, adapter, fm, link, &mut traffic).await?;
        }
        if traffic.reports.len() != before
            || !adapter.replica_drained(node)
            || node.instance_disposal().is_some() != graceful
        {
            return Err("duplicate ACK resurrected report work or disposal proof".into());
        }
        if ledger
            .accept(Command {
                op: "replacement".into(),
                action: "create".into(),
                node: Some(launch.placement),
            })
            .is_ok()
        {
            return Err("loss authorized replacement".into());
        }
        let logs = node.activation().logs().join("\n");
        if graceful {
            for marker in [
                "LOCAL_CALLBACK create_replica",
                "LOCAL_DATA_PROVIDER disposed written",
                "LOCAL_PRIMARY released",
                "LOCAL_DISPOSE complete",
            ] {
                if logs.lines().filter(|line| line.contains(marker)).count() != 1 {
                    return Err(format!(
                        "graceful disposal did not complete exactly once: {marker}"
                    ));
                }
            }
            if node
                .activation()
                .process_status()
                .is_none_or(|status| status.exit_observed)
            {
                return Err("metadata ACK lost the original app host".into());
            }
        } else if logs.contains("LOCAL_DATA_PROVIDER disposed")
            || logs.contains("LOCAL_DISPOSE complete")
        {
            return Err("hard process loss fabricated graceful destructor receipts".into());
        }
        Ok(())
    }

    pub(crate) async fn run_stateful_down(
        launch: &Launch,
        ledger: &mut crate::fm::Ledger,
        mode: StatefulDown,
    ) -> Result<Result<()>> {
        let (mut node, store) = create_node(launch).await?;
        let mut adapter = Adapter::default();
        let result = async {
            let args = match mode {
                StatefulDown::Crash => "--stateful-primary --crash-after-ms 5000",
                StatefulDown::LeaseLoss | StatefulDown::ForceDelete => "--stateful-primary",
                StatefulDown::CloseProvider
                | StatefulDown::DropProvider
                | StatefulDown::AbortProvider
                | StatefulDown::ForceAbortProvider => "--stateful-primary --block-provider-dispose",
                StatefulDown::CloseService
                | StatefulDown::DropService
                | StatefulDown::AbortService
                | StatefulDown::ForceAbortService => "--stateful-primary --block-dispose",
            };
            node.start(
                &launch.package,
                APPLICATION,
                &[("Arguments".into(), args.into())],
            )
            .map_err(err)?;
            let (fm, link) = links(launch).await?;
            let result =
                exercise_stateful_down(launch, &mut node, &mut adapter, &fm, &link, ledger, mode)
                    .await;
            let released = if matches!(
                mode,
                StatefulDown::CloseProvider
                    | StatefulDown::CloseService
                    | StatefulDown::DropProvider
                    | StatefulDown::DropService
                    | StatefulDown::AbortProvider
                    | StatefulDown::AbortService
                    | StatefulDown::ForceAbortProvider
                    | StatefulDown::ForceAbortService
            ) {
                release_disposal(&node, mode)
            } else {
                Ok(())
            };
            let fm_closed = fm.close().await;
            let node_closed = link.close().await;
            result.and(released).and(fm_closed).and(node_closed)
        }
        .await;
        cleanup(launch, &mut node, &store, &mut adapter).await?;
        Ok(result)
    }

    pub(crate) async fn run(launch: &Launch) -> Result<Result<()>> {
        run_case(launch, false).await
    }
    pub(crate) async fn run_reuse(launch: &Launch) -> Result<Result<()>> {
        run_case(launch, true).await
    }

    async fn next_lfum(
        upload: &mut LfumUploadState,
        fm: &Link,
    ) -> Result<
        fube_transport::channel::IncomingMessage<fube_transport::native_channel::GatewayTarget>,
    > {
        let request = upload.request.as_mut().ok_or("missing LFUM request")?;
        tokio::select! {
            incoming = fm.receive(wire::deadline()) => incoming?.ok_or("missing LFUM upload".into()),
            reply = request => Err(format!("LFUM request completed before FM received it: {reply:?}")),
        }
    }

    pub(crate) async fn run_lfum_retry(launch: &Launch) -> Result<Result<()>> {
        let (mut node, store) = create_node(launch).await?;
        let mut adapter = Adapter::default();
        let result = async {
            node.start(&launch.package, APPLICATION, &[]).map_err(err)?;
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
            let up = NodeUp::fresh_stateless(
                launch.name(),
                "fube-cluster-local".into(),
                listener.local_addr().map_err(err)?,
                FabricVersionInstance::native_default(),
            )
            .map_err(err)?;
            let (fm, link) = links(launch).await?;
            let operation = async {
                let mut generation = launch.generation.clone();
                generation.number.generation += 1;
                let proposal = node
                    .process_generation_proposal(&launch.fm, &generation)
                    .map_err(err)?;
                if proposal.error != 0
                    || node
                        .process_generation_update(
                            &launch.fm,
                            &GenerationUpdate::default(),
                            &generation,
                        )
                        .map_err(err)?
                        != ra::GenerationUpdateOutcome::UploadRequired
                {
                    return Err("native generation did not require LFUM upload".into());
                }
                let mut upload = LfumUploadState::default();
                let end = Instant::now() + Duration::from_secs(3);
                while upload.request.is_none() {
                    node.drive().map_err(err)?;
                    upload.advance(&node, &up.node, Some(&link), Instant::now())?;
                    if Instant::now() >= end {
                        return Err("LFUM inventory read barrier deadline".into());
                    }
                    tokio::task::yield_now().await;
                }
                let mut first = next_lfum(&mut upload, &fm).await?;
                let frozen = decode_lfum_upload(&first.message).map_err(err)?;
                if let Some(handoff) = first.handoff.as_mut() {
                    handoff.acknowledge();
                }
                drop(first);
                let reply = upload.request.as_mut().unwrap().await?;
                if reply.is_some() {
                    return Err("lost LFUM reply unexpectedly completed".into());
                }
                let completed = Instant::now();
                upload.complete(&node, &launch.fm, Ok(None), completed)?;
                let due = upload.retry_at;
                if due - completed
                    != ticks(
                        node.native_config()
                            .node(ra::NodeProperty::LfumUploadRetryIntervalTicks),
                    )
                    || upload.body.as_ref() != Some(&frozen)
                    || !node.generation_state().upload_pending
                {
                    return Err(
                        "lost reply changed the frozen inventory or native retry interval".into(),
                    );
                }
                upload.advance(&node, &up.node, Some(&link), due - Duration::from_nanos(1))?;
                if upload.request.is_some() {
                    return Err("LFUM retried before its native deadline".into());
                }
                upload.advance(&node, &up.node, Some(&link), due)?;
                let mut retry = next_lfum(&mut upload, &fm).await?;
                if decode_lfum_upload(&retry.message).map_err(err)? != frozen {
                    return Err("LFUM retry changed the frozen upload".into());
                }
                retry
                    .reply
                    .take()
                    .ok_or("LFUM retry missing reply authority")?
                    .reply(
                        fm.seal(lfum_upload_reply_message(&generation).map_err(err)?)?,
                        wire::deadline(),
                    )
                    .await
                    .map_err(err)?;
                if let Some(handoff) = retry.handoff.as_mut() {
                    handoff.acknowledge();
                }
                let reply = upload.request.as_mut().unwrap().await;
                upload.complete(&node, &launch.fm, reply, Instant::now())?;
                if node.generation_state().upload_pending
                    || node.generation_state().send != generation.number
                    || upload.body.is_some()
                {
                    return Err("matched LFUM reply did not settle the original upload".into());
                }
                upload.advance(&node, &up.node, Some(&link), due + Duration::from_secs(60))?;
                if upload.request.is_some() {
                    return Err("completed LFUM upload retried".into());
                }
                Ok(())
            }
            .await;
            let fm_closed = fm.close().await;
            let node_closed = link.close().await;
            operation.and(fm_closed).and(node_closed)
        }
        .await;
        cleanup(launch, &mut node, &store, &mut adapter).await?;
        Ok(result)
    }

    async fn exercise_stateful_tombstone(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        fm: &Link,
        link: &Link,
    ) -> Result<()> {
        let body = add_body(launch, &actual_profile(node)?);
        let entity = body.unit.id.guid.0;
        let mut traffic = Traffic::default();
        fm.send(
            delete_replica_message(&DeleteReplica::new(body.clone(), true), &launch.generation)
                .map_err(err)?,
        )
        .await?;
        until(
            launch,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, adapter, traffic| {
                traffic.removed && adapter.replica_drained(node) && node.quiescent()
            },
        )
        .await?;
        if node.snapshot(entity).committed.is_none_or(|state| {
            state.phase != ra::Phase::Closed || !state.local_deleted || state.open_token != 0
        }) || node.retained_usage() != 0
            || node.instance_disposal().is_some()
            || node
                .activation()
                .logs()
                .iter()
                .any(|line| line.contains("LOCAL_CALLBACK create_replica"))
        {
            return Err("metadata-only Delete created a service or fabricated disposal".into());
        }
        let mut successor = body.clone();
        successor.replica.id += 1;
        successor.replica.instance += 1;
        let mut next = launch.clone();
        next.replica = successor.replica.id;
        next.incarnation = successor.replica.instance;
        traffic.removed = false;
        fm.send(add_primary_message(&successor, &launch.generation).map_err(err)?)
            .await?;
        until(
            &next,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |_, _, traffic| traffic.added,
        )
        .await?;
        let endpoint = traffic
            .add_reply
            .as_ref()
            .ok_or("missing fresh reply")?
            .replica
            .service_endpoint
            .clone();
        let end = Instant::now() + Duration::from_secs(2);
        loop {
            let response = data_request(&endpoint, b"read\n").await?;
            if response.contains("assigned=0 applied=0 committed=0 value=\n") {
                break;
            }
            if response != "ERR NotPrimary\n" || Instant::now() >= end {
                return Err(format!(
                    "metadata tombstone did not admit fresh empty data: {response}"
                ));
            }
            step(&next, node, adapter, fm, link, &mut traffic).await?;
        }
        fm.send(
            delete_replica_message(
                &DeleteReplica::new(successor.clone(), false),
                &launch.generation,
            )
            .map_err(err)?,
        )
        .await?;
        until(
            &next,
            node,
            adapter,
            fm,
            link,
            &mut traffic,
            |node, adapter, traffic| {
                traffic.removed
                    && adapter.replica_drained(node)
                    && node.instance_disposal().is_some()
            },
        )
        .await?;
        if node.instance_disposal().is_none_or(|proof| {
            proof.body.replica.id != successor.replica.id
                || proof.body.replica.instance != successor.replica.instance
        }) || node.retained_usage() != 0
        {
            return Err("fresh service lacked its own exact original disposal".into());
        }
        Ok(())
    }
    pub(crate) async fn run_stateful_tombstone(launch: &Launch) -> Result<Result<()>> {
        let (mut node, store) = create_node(launch).await?;
        let mut adapter = Adapter::default();
        let result = async {
            node.start(
                &launch.package,
                APPLICATION,
                &[("Arguments".into(), "--stateful-primary".into())],
            )
            .map_err(err)?;
            let (fm, link) = links(launch).await?;
            let result =
                exercise_stateful_tombstone(launch, &mut node, &mut adapter, &fm, &link).await;
            let fm_closed = fm.close().await;
            let node_closed = link.close().await;
            result.and(fm_closed).and(node_closed)
        }
        .await;
        cleanup(launch, &mut node, &store, &mut adapter).await?;
        Ok(result)
    }
    async fn create_node(launch: &Launch) -> Result<(Node, Arc<Store>)> {
        private_dir(&launch.directory)?;
        private_dir(&launch.directory.join("hosting"))?;
        let mut options = Options::new(launch.directory.join("hosting"));
        if launch.profile == ServiceProfile::StatefulPeerIdle {
            options.service_package_version_instance = Some(1);
        }
        if launch.profile.stateful() {
            options.profile =
                if launch.profile == ServiceProfile::StatefulPeerIdle && launch.placement == 2 {
                    fube_hosting::local::Profile::VolatileStatefulLifecycle
                } else {
                    fube_hosting::local::Profile::VolatileStatefulFmPrimary
                };
            if launch.profile == ServiceProfile::StatefulSingleton {
                options.lease_duration = Duration::from_secs(2);
            }
        }
        options.node = Some(NodeContext {
            id: node_hex(&launch.node),
            instance: launch.node.instance,
            name: launch.name(),
            ip: std::net::Ipv4Addr::LOCALHOST.into(),
        });
        let store = Arc::new(
            Store::new(Box::new(MemoryBackend::new()), Settings::default())
                .await
                .map_err(err)?,
        );
        let node = Node::new(
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
        if let Some(peer) = launch.peers.first() {
            node.bind_peer(peer.node.clone()).map_err(err)?;
        }
        Ok((node, store))
    }

    async fn peer_step(
        launch: &Launch,
        node: &mut Node,
        adapter: &mut Adapter,
        peer: &Link,
        fm: Option<&Link>,
    ) -> Result<()> {
        node.drive().map_err(err)?;
        adapter
            .advance_with_peer(node, fm, Some(peer), launch.peers.first())
            .await?;
        if let Some(mut incoming) = peer
            .receive(Instant::now() + Duration::from_millis(1))
            .await?
        {
            let id = receive_peer(launch, node, &incoming.message)
                .map_err(|error| format!("node{} peer ingress: {error}", launch.placement))?
                .ok_or("unexpected peer command capacity in bounded test")?;
            adapter.peer_commands.push(id);
            if let Some(handoff) = incoming.handoff.as_mut() {
                handoff.acknowledge();
            }
        }
        Ok(())
    }

    fn reject_unbound_peer_topology(
        launches: &[Launch; 2],
        source: &mut Node,
        target: &mut Node,
    ) -> Result<()> {
        let mut original = add_body(&launches[1], &actual_profile(target)?);
        original.replica.current_role = 2;
        let entity = original.unit.id.guid.0;
        let deactivation = ReplicaDeactivationInfo {
            epoch: original.unit.current,
            catchup_lsn: 0,
        };
        let mut foreign_partition = original.clone();
        foreign_partition.unit.id.guid.0[0] ^= 0xff;
        foreign_partition.unit.consistency.id = foreign_partition.unit.id.clone();
        let foreign = foreign_partition.unit.id.guid.0;
        let mut foreign_replica = original.clone();
        foreign_replica.replica.id += 100;
        let mut old_instance = original.clone();
        old_instance.replica.instance -= 1;
        let mut new_instance = original.clone();
        new_instance.replica.instance += 1;
        let mut foreign_node = original.clone();
        foreign_node.replica.node.instance += 1;
        let before = (source.pending_commands(), target.pending_commands());
        for body in [
            foreign_partition,
            foreign_replica,
            old_instance,
            new_instance,
            foreign_node,
        ] {
            let request = CreateReplica::new(body.clone(), deactivation.clone());
            let request = create_replica_message(&request, &launches[0].generation).map_err(err)?;
            let reply = create_replica_reply_message(
                &CreateReplicaReply {
                    unit: body.unit,
                    replica: body.replica,
                    error: 0,
                },
                &launches[1].generation,
            )
            .map_err(err)?;
            if receive_peer(&launches[1], target, &request).is_ok()
                || receive_peer(&launches[0], source, &reply).is_ok()
            {
                return Err("unbound peer topology reached first-FT admission".into());
            }
        }
        if (source.pending_commands(), target.pending_commands()) != before
            || [entity, foreign].iter().any(|id| {
                source.snapshot(*id).committed.is_some() || target.snapshot(*id).committed.is_some()
            })
            || source.retained_usage() != 0
            || target.retained_usage() != 0
        {
            return Err("unbound peer topology poisoned selection or acquired usage".into());
        }
        Ok(())
    }

    #[derive(Clone, Copy)]
    pub(crate) enum PeerCreationOutcome {
        Pending,
        Opened,
        Failed,
        DeleteProvider,
        DeleteService,
        DeleteAbsent,
        DeleteAbsentForce,
        DeleteAbsentAfterHostExit,
        DeleteOpeningProvider,
        DeleteOpeningService,
        DeletePreRegistration,
        DeletePreRegistrationForce,
        DeletePreRegistrationAfterHostExit,
        DeleteFailedRegistration,
        DeleteFailedRegistrationAfterHostExit,
        DeleteFailedOpenProvider,
        DeleteFailedOpenService,
        DeleteFailedOpenAfterHostExit,
        DeleteProviderAfterHostExit,
        CloseLossProvider,
        CloseLossService,
        GracefulCloseProvider,
        GracefulCloseService,
        DropProvider,
        DropService,
        DropLossProvider,
        DropLossService,
        AbortProvider,
        AbortService,
        DeleteTerminal {
            mode: ra::CloseMode,
            provider: bool,
            during: bool,
        },
        DeleteForceDuringDisposal {
            provider: bool,
        },
    }

    pub(crate) async fn run_peer_creation_removal(
        launches: &[Launch; 2],
        outcome: PeerCreationOutcome,
    ) -> Result<Result<()>> {
        let after_host_exit = matches!(
            outcome,
            PeerCreationOutcome::DeleteAbsentAfterHostExit
                | PeerCreationOutcome::DeletePreRegistrationAfterHostExit
                | PeerCreationOutcome::DeleteFailedRegistrationAfterHostExit
                | PeerCreationOutcome::DeleteFailedOpenAfterHostExit
                | PeerCreationOutcome::DeleteProviderAfterHostExit
        );
        let outcome = match outcome {
            PeerCreationOutcome::DeleteAbsentAfterHostExit => PeerCreationOutcome::DeleteAbsent,
            PeerCreationOutcome::DeletePreRegistrationAfterHostExit => {
                PeerCreationOutcome::DeletePreRegistration
            }
            PeerCreationOutcome::DeleteFailedRegistrationAfterHostExit => {
                PeerCreationOutcome::DeleteFailedRegistration
            }
            PeerCreationOutcome::DeleteFailedOpenAfterHostExit => {
                PeerCreationOutcome::DeleteFailedOpenProvider
            }
            PeerCreationOutcome::DeleteProviderAfterHostExit => PeerCreationOutcome::DeleteProvider,
            other => other,
        };
        let terminal_deletion = match outcome {
            PeerCreationOutcome::DeleteTerminal {
                mode,
                provider,
                during,
            } => Some((mode, provider, during)),
            _ => None,
        };
        let force_during_disposal = matches!(
            outcome,
            PeerCreationOutcome::DeleteForceDuringDisposal { .. }
        );
        let deleting = force_during_disposal
            || matches!(
                outcome,
                PeerCreationOutcome::DeleteProvider
                    | PeerCreationOutcome::DeleteService
                    | PeerCreationOutcome::DeleteOpeningProvider
                    | PeerCreationOutcome::DeleteOpeningService
            );
        let provider = matches!(
            outcome,
            PeerCreationOutcome::DeleteForceDuringDisposal { provider: true }
        ) || terminal_deletion.is_some_and(|(_, provider, _)| provider)
            || matches!(
                outcome,
                PeerCreationOutcome::DeleteProvider
                    | PeerCreationOutcome::DeleteOpeningProvider
                    | PeerCreationOutcome::DeleteFailedOpenProvider
                    | PeerCreationOutcome::CloseLossProvider
                    | PeerCreationOutcome::GracefulCloseProvider
                    | PeerCreationOutcome::DropProvider
                    | PeerCreationOutcome::DropLossProvider
                    | PeerCreationOutcome::AbortProvider
            );
        let opening = matches!(
            outcome,
            PeerCreationOutcome::DeleteOpeningProvider | PeerCreationOutcome::DeleteOpeningService
        );
        let absent = matches!(
            outcome,
            PeerCreationOutcome::DeleteAbsent | PeerCreationOutcome::DeleteAbsentForce
        );
        let pre_registration = matches!(
            outcome,
            PeerCreationOutcome::DeletePreRegistration
                | PeerCreationOutcome::DeletePreRegistrationForce
        );
        let force_before_open = matches!(
            outcome,
            PeerCreationOutcome::DeleteAbsentForce
                | PeerCreationOutcome::DeletePreRegistrationForce
        );
        let failed_registration = matches!(outcome, PeerCreationOutcome::DeleteFailedRegistration);
        let failed_open = matches!(
            outcome,
            PeerCreationOutcome::DeleteFailedOpenProvider
                | PeerCreationOutcome::DeleteFailedOpenService
        );
        let actual_failure = failed_registration || failed_open;
        let closing_loss = matches!(
            outcome,
            PeerCreationOutcome::CloseLossProvider
                | PeerCreationOutcome::CloseLossService
                | PeerCreationOutcome::DropLossProvider
                | PeerCreationOutcome::DropLossService
        );
        let dropping = terminal_deletion.is_some_and(|(mode, _, _)| mode == ra::CloseMode::Drop)
            || matches!(
                outcome,
                PeerCreationOutcome::DropProvider
                    | PeerCreationOutcome::DropService
                    | PeerCreationOutcome::DropLossProvider
                    | PeerCreationOutcome::DropLossService
            );
        let aborting = terminal_deletion.is_some_and(|(mode, _, _)| mode == ra::CloseMode::Abort)
            || matches!(
                outcome,
                PeerCreationOutcome::AbortProvider | PeerCreationOutcome::AbortService
            );
        let graceful = terminal_deletion.is_some()
            || matches!(
                outcome,
                PeerCreationOutcome::GracefulCloseProvider
                    | PeerCreationOutcome::GracefulCloseService
                    | PeerCreationOutcome::DropProvider
                    | PeerCreationOutcome::DropService
                    | PeerCreationOutcome::AbortProvider
                    | PeerCreationOutcome::AbortService
            );
        let normal_close = closing_loss || graceful;
        let (mut source, source_store) = create_node(&launches[0]).await?;
        let (mut target, target_store) = create_node(&launches[1]).await?;
        if failed_registration {
            target
                .native_config()
                .set(ra::RetryState::Registration, ra::Threshold::Drop, 1);
        }
        if failed_open {
            target
                .native_config()
                .set(ra::RetryState::Opening, ra::Threshold::Drop, 1);
        }
        let mut source_adapter = Adapter::default();
        let mut target_adapter = Adapter::default();
        let result = async {
            source
                .start(
                    &launches[0].package,
                    APPLICATION,
                    &[("Arguments".into(), "--stateful-primary".into())],
                )
                .map_err(err)?;
            target
                .start(
                    &launches[1].package,
                    APPLICATION,
                    &[(
                        "Arguments".into(),
                        format!("--stateful-lifecycle --data-replicator{}{}{}{}", if deleting || failed_open || normal_close {
                            if provider { " --block-provider-dispose" } else { " --block-dispose" }
                        } else { "" }, if opening { " --block-open" } else { "" },
                        if failed_registration { " --block-registration" } else { "" },
                        if failed_open { " --fail-open" } else { "" }),
                    )],
                )
                .map_err(err)?;
            let (fm, fm_link) = links(&launches[0]).await?;
            let body = add_body(&launches[0], &actual_profile(&source)?);
            let entity = body.unit.id.guid.0;
            fm.send(add_primary_message(&body, &launches[0].generation).map_err(err)?)
                .await?;
            let mut traffic = Traffic::default();
            until(
                &launches[0],
                &mut source,
                &mut source_adapter,
                &fm,
                &fm_link,
                &mut traffic,
                |_, adapter, traffic| traffic.added && adapter.notification_pending.is_none(),
            )
            .await?;
            let endpoint = traffic
                .add_reply
                .as_ref()
                .ok_or("missing Primary reply")?
                .replica
                .service_endpoint
                .clone();
            let ready_end = Instant::now() + Duration::from_secs(2);
            while !data_request(&endpoint, b"read\n").await?.starts_with("OK ") {
                if Instant::now() >= ready_end {
                    return Err("Primary readiness deadline".into());
                }
                step(
                    &launches[0],
                    &mut source,
                    &mut source_adapter,
                    &fm,
                    &fm_link,
                    &mut traffic,
                )
                .await?;
            }
            if !data_request(&endpoint, b"write\n")
                .await?
                .contains("committed=1 value=acknowledged")
            {
                return Err("Primary write before cancellation failed".into());
            }
            let mut ledger = crate::fm::Ledger::new(launches.clone());
            for (index, node) in [&source, &target].into_iter().enumerate() {
                ledger.placements[index].bootstrap = crate::fm::Bootstrap::Metadata;
                ledger.placements[index].admit_profile(actual_profile(node)?)?;
            }
            ledger.accept(crate::fm::Command {
                op: "original-primary".into(),
                action: "create".into(),
                node: Some(1),
            })?;
            ledger.reply(
                0,
                traffic
                    .add_reply
                    .clone()
                    .ok_or("missing original Primary reply")?,
                false,
            )?;
            let request = ledger.request_idle(Instant::now())?;
            if deleting {
                let mut foreign = request.clone();
                foreign.replica.instance += 1;
                if receive_command(&launches[1], &mut target,
                    &delete_replica_message(&DeleteReplica::new(foreign, false), &launches[1].generation).map_err(err)?).is_ok()
                {
                    return Err("peer Delete admitted an unrelated destination incarnation".into());
                }
            }
            fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?)
                .await?;
            until(
                &launches[0],
                &mut source,
                &mut source_adapter,
                &fm,
                &fm_link,
                &mut traffic,
                |_, adapter, _| !adapter.peer_replies.is_empty(),
            )
            .await?;
            let held = source_adapter
                .peer_replies
                .pop_front()
                .ok_or("missing original CreateReplica")?;
            let ra::Effect::PeerCreateReplica { body: create, .. } = held.effect else {
                return Err("unexpected original peer effect".into());
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
            let address = listener.local_addr().map_err(err)?.to_string();
            let (source_link, target_link) =
                tokio::join!(dial_peer(&launches[0], &address), async {
                    let (stream, _) = listener.accept().await.map_err(err)?;
                    accept_peer(&launches[1], stream).await
                });
            drop(listener);
            let source_link = source_link?;
            let target_link = target_link?;
            let operation = async {
                if pre_registration {
                    let end = Instant::now() + Duration::from_secs(5);
                    loop {
                        target.drive().map_err(err)?;
                        target_adapter.advance(&mut target, None).await?;
                        if target.pending_commands() == 0 && target.activation().logs().iter().any(|line| line.contains("LOCAL_REGISTERED")) {
                            break;
                        }
                        if Instant::now() >= end { return Err("original factory registration deadline".into()); }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                    let mut incoming = target_link.receive(Instant::now() + Duration::from_secs(2)).await?
                        .ok_or("missing original Create before lookup")?;
                    let id = receive_peer(&launches[1], &mut target, &incoming.message)?.ok_or("original Create capacity")?;
                    target_adapter.peer_commands.push(id);
                    if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                    target.drive().map_err(err)?;
                    while target.snapshot(entity).committed.is_none() {
                        if Instant::now() >= end { return Err("original pre-registration Create commit deadline".into()); }
                        tokio::task::yield_now().await;
                    }
                    let opening = target.snapshot(entity).committed.ok_or("missing original opening Idle")?;
                    if opening.phase != ra::Phase::Opening || opening.runtime.is_some()
                        || opening.usage.is_some() || opening.registration_lookups.is_empty()
                        || target.retained_usage() != 0
                    { return Err("pre-registration fixture adopted a runtime before Delete".into()); }
                }
                if matches!(outcome, PeerCreationOutcome::Opened) || deleting || actual_failure || normal_close {
                    source_link
                        .send(
                            create_replica_message(&create, &launches[0].generation)
                                .map_err(err)?,
                        )
                        .await?;
                    let end = Instant::now() + Duration::from_secs(8);
                    while if failed_open {
                        !target.activation().logs().iter().any(|line| line.contains(
                            if provider { "LOCAL_DATA_PROVIDER dispose begin" } else { "LOCAL_DISPOSE begin" }))
                    } else if opening {
                        !target.activation().logs().iter().any(|line| line.contains("LOCAL_OPEN held"))
                    } else { target_adapter.peer_last_reply.is_none() } {
                        peer_step(
                            &launches[1],
                            &mut target,
                            &mut target_adapter,
                            &target_link,
                            None,
                        )
                        .await?;
                        step(
                            &launches[0],
                            &mut source,
                            &mut source_adapter,
                            &fm,
                            &fm_link,
                            &mut traffic,
                        )
                        .await?;
                        if Instant::now() >= end {
                            return Err("destination Open deadline".into());
                        }
                    }
                    if opening && target.close(body.unit.id.clone(), ra::CloseMode::Abort).is_ok() {
                        return Err("unopened peer acquired non-force Abort authority".into());
                    }
                    if failed_open {
                        if target.runtime_open_failure().is_some() || target.failed_open_deletion().is_some()
                            || target.instance_disposal().is_some() || target.retained_usage() != 1
                            || target_adapter.peer_last_reply.is_some() || !traffic.idle_adds.is_empty()
                            || ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::FailedOpen).is_ok()
                            || !data_request(&endpoint, b"read\n").await?.contains("value=acknowledged")
                        { return Err("held failed-Open disposal published failure/cleanup or changed Primary serving".into()); }
                        release_disposal(&target, if provider { StatefulDown::CloseProvider } else { StatefulDown::CloseService })?;
                        let end = Instant::now() + Duration::from_secs(5);
                        while target_adapter.peer_last_reply.is_none() {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, None).await?;
                            if Instant::now() >= end { return Err("failed Open disposal/reply deadline".into()); }
                        }
                    }
                }
                let failed = matches!(outcome, PeerCreationOutcome::Failed) || actual_failure;
                let creation_error = if failed_registration { ra::HOSTING_SERVICE_TYPE_NOT_REGISTERED }
                    else if failed_open { 0x80004005u32 as i32 as i64 } else { ra::RA_NOT_READY };
                if actual_failure {
                    let end = Instant::now() + Duration::from_secs(5);
                    while traffic.idle_adds.is_empty() {
                        peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await?;
                        step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        if Instant::now() >= end { return Err("original registration AddReplicaReply deadline".into()); }
                    }
                    if traffic.idle_adds[0].error != creation_error
                        || (failed_registration && target.pre_open_registration_failure().is_none())
                        || (failed_open && target.runtime_open_failure().is_none())
                        || target.failed_registration_deletion().is_some() || target.failed_open_deletion().is_some()
                    { return Err("registration failure was not actual original pre-Open lineage".into()); }
                    ledger.idle_reply(0, traffic.idle_adds[0].clone())?;
                }
                if failed && !actual_failure {
                    let failure = CreateReplicaReply {
                        unit: request.unit.clone(),
                        replica: request.replica.clone(),
                        error: ra::RA_NOT_READY,
                    };
                    target_link
                        .send(
                            create_replica_reply_message(&failure, &launches[1].generation)
                                .map_err(err)?,
                        )
                        .await?;
                    let end = Instant::now() + Duration::from_secs(8);
                    while traffic.idle_adds.is_empty() {
                        peer_step(
                            &launches[0],
                            &mut source,
                            &mut source_adapter,
                            &source_link,
                            Some(&fm_link),
                        )
                        .await?;
                        step(
                            &launches[0],
                            &mut source,
                            &mut source_adapter,
                            &fm,
                            &fm_link,
                            &mut traffic,
                        )
                        .await?;
                        if Instant::now() >= end {
                            return Err("failed AddReplicaReply deadline".into());
                        }
                    }
                    if traffic.idle_adds[0] != failure {
                        return Err("original peer error was not forwarded to FM".into());
                    }
                    ledger.idle_reply(0, traffic.idle_adds[0].clone())?;
                    if ledger.idle_status()["state"] != "failed"
                        || ledger.idle_status()["to_be_dropped"] != true
                        || ledger.idle_removal_status()["pending_remove"] != true
                    {
                        return Err("failed add lost its error or pending-remove obligation".into());
                    }
                }
                // Hold either CreateReplica or its real Open reply outside the Primary RA.
                let removing = ledger.request_idle_removal(Instant::now())?;
                if removing != request {
                    return Err("cleanup adopted the failed reply descriptor".into());
                }
                fm.send(remove_replica_message(&removing, &launches[0].generation).map_err(err)?)
                    .await?;
                until(
                    &launches[0],
                    &mut source,
                    &mut source_adapter,
                    &fm,
                    &fm_link,
                    &mut traffic,
                    |_, _, traffic| traffic.idle_removals.len() == 1,
                )
                .await?;
                let expected = RemoveReplicaReply {
                    unit: request.unit.clone(),
                    replica: request.replica.clone(),
                    error: 0,
                };
                if traffic.idle_removals[0] != expected {
                    return Err("local cleanup did not echo the original FM descriptor".into());
                }
                // Deliberately lose the first reply, then retry the original request.
                if ledger.idle_removal_status()["state"] != "pending" {
                    return Err(
                        "transport/RA progress completed FM without its semantic reply".into(),
                    );
                }
                let retry = ledger
                    .idle_removal_retry(0, Instant::now() + Duration::from_secs(11))
                    .ok_or("missing original FM cleanup retry")?;
                if retry != removing {
                    return Err("FM cleanup retry changed the original request".into());
                }
                fm.send(remove_replica_message(&retry, &launches[0].generation).map_err(err)?)
                    .await?;
                until(
                    &launches[0],
                    &mut source,
                    &mut source_adapter,
                    &fm,
                    &fm_link,
                    &mut traffic,
                    |_, _, traffic| traffic.idle_removals.len() == 2,
                )
                .await?;
                if traffic.idle_removals[1] != expected {
                    return Err("local cleanup replay changed the reply".into());
                }
                if !deleting && !absent && !pre_registration && !actual_failure && !normal_close {
                    ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                }
                if ledger.idle_removal_status()["pending_remove"] != (deleting || absent || pre_registration || actual_failure || normal_close)
                    || (failed
                        && (ledger.idle_status()["state"] != "failed"
                            || ledger.idle_status()["error"] != creation_error
                            || ledger.idle_status()["destination_cleanup_required"] != true
                            || (!actual_failure && ledger.idle_removal_status()["completion"]
                                != "failed_creation_retired")))
                {
                    return Err(
                        "semantic cleanup erased the original failure or claimed disposal".into(),
                    );
                }
                if after_host_exit && !deleting {
                    let pid = target.activation().process_status().ok_or("missing original app process")?.pid;
                    if !std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success() {
                        return Err("failed to terminate the original completed/no-runtime app".into());
                    }
                }
                if normal_close {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let result = async {
                        let original = target.snapshot(entity).committed.ok_or("missing original opened Idle")?;
                        if original.phase != ra::Phase::Ready || original.runtime.is_none() || target.retained_usage() != 1 {
                            return Err("terminal-report fixture did not own an opened Idle".into());
                        }
                        let mode = if aborting { ra::CloseMode::Abort } else if dropping { ra::CloseMode::Drop } else { ra::CloseMode::Close };
                        let expected_stage = if (dropping || aborting) && graceful { ra::FmStage::ReplicaDropped } else { ra::FmStage::ReplicaDown };
                        target.close(body.unit.id.clone(), mode).map_err(err)?;
                        let mut target_traffic = Traffic::default();
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, _, _| node.activation().logs().iter().any(|line|
                                line.contains(if provider { "LOCAL_DATA_PROVIDER dispose begin" } else { "LOCAL_DISPOSE begin" }))).await?;
                        let closing = target.snapshot(entity).committed.ok_or("missing committed Close")?;
                        if closing.phase != ra::Phase::Closing || closing.close_mode != Some(mode)
                            || !closing.peer_creation.as_ref().is_some_and(|peer| peer.close_started_open)
                            || closing.peer_terminal_report() || target.retained_usage() != 1
                            || target.instance_disposal().is_some() || !target_traffic.reports.is_empty()
                        { return Err("held Close/Drop published a report or disposal before cleanup".into()); }
                        if terminal_deletion.is_some_and(|(_, _, during)| during) {
                            let deleting = ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::Runtime)?;
                            target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                            for _ in 0..15 {
                                step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            }
                            if target.snapshot(entity).committed.is_none_or(|state| state.local_deleted || state.close_mode != Some(mode))
                                || target_traffic.removed || target.instance_disposal().is_some()
                            { return Err("normal Delete superseded or acknowledged a held existing close".into()); }
                        }
                        if graceful {
                            release_disposal(&target, if provider { StatefulDown::CloseProvider } else { StatefulDown::CloseService })?;
                        } else {
                            let pid = target.activation().process_status().ok_or("missing original app process")?.pid;
                            if !std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success() {
                                return Err("failed to terminate the original Closing app".into());
                            }
                        }
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, _, traffic| !traffic.reports.is_empty()
                                && (!graceful || node.instance_disposal().is_some())).await?;
                        let report = target_traffic.reports[0].clone();
                        let disposal = target.instance_disposal();
                        let lost = target.snapshot(entity).committed.ok_or("missing lost Closing Idle")?;
                        if lost.phase != ra::Phase::Closed || lost.fm_stage != expected_stage
                            || !lost.peer_terminal_report() || lost.peer_host_loss() != !graceful
                            || lost.local_deleted || lost.usage.is_some()
                            || target.retained_usage() != 0 || disposal.is_some() != graceful
                            || !report.replicas.0.is_empty() || report.dropped.0.len() != 1
                        { return Err("Close/Drop terminal origin did not preserve native reporting/disposal semantics".into()); }
                        if let Some(proof) = &disposal {
                            let runtime = original.runtime.as_ref().ok_or("missing original runtime")?;
                            if original.usage != Some(proof.lookup) || proof.host != runtime.host || proof.runtime != runtime.runtime
                                || proof.acquisition != runtime.acquisition || proof.command == 0 || proof.ra_operation == 0
                                || proof.body.unit != original.body.unit || proof.body.service != original.body.service
                                || proof.body.replica.node != original.body.replica.node
                                || proof.body.replica.id != original.body.replica.id
                                || proof.body.replica.instance != original.body.replica.instance
                                || proof.body.replica.current_role != 2
                            { return Err("successful Close/Drop lost exact original disposal/accounting lineage".into()); }
                            let replication_address = original.body.replica.replication_endpoint.split_once('/')
                                .ok_or("original Idle replication endpoint lacks actor suffix")?.0;
                            if tokio::net::TcpStream::connect(replication_address).await.is_ok()
                                || target.activation().process_status().is_none_or(|status| status.exit_observed)
                            { return Err("successful Close/Drop did not retire the listener in its original live host".into()); }
                        }
                        let reply = ledger.report(1, &report)?.ok_or("missing native processed loss ACK")?;
                        if reply.replicas != report.dropped || !reply.dropped.0.is_empty()
                            || ledger.idle_removal_status()["pending_remove"] != true
                            || ledger.idle_status()["state"] != "lost"
                        { return Err("Closing loss altered Primary pending-remove or ACK list semantics".into()); }
                        if terminal_deletion.is_some_and(|(_, _, during)| !during) {
                            ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::Runtime)?;
                        }
                        if let Some(proof) = &disposal {
                            let profile = actual_profile(&target)?;
                            ledger.retirement(1, RetirementReceipt {
                                deployment: launches[1].deployment.clone(), session: launches[1].session.clone(),
                                node: launches[1].node.clone(), fm: launches[1].fm.clone(), generation: launches[1].generation.clone(),
                                node_pid: std::process::id(), app_pid: profile.app_pid, runtime_root: profile.runtime_root,
                                disposal: proof.clone(),
                            }, std::process::id())?;
                        }
                        for stale_kind in 0..if expected_stage == ra::FmStage::ReplicaDropped { 2 } else { 1 } {
                            let mut stale = reply.clone();
                            if stale_kind == 1 {
                                stale.replicas.0[0].replicas.0[0].description.state = 2;
                            } else {
                                stale.replicas.0[0].replicas.0[0].description.instance -= 1;
                            }
                            acknowledge_without_sending_captured_report(&launches[1], &mut target, &mut target_adapter,
                                &target_fm, &target_fm_link, &stale).await?;
                            target_adapter.advance(&mut target, Some(&target_fm_link)).await?;
                            if target.snapshot(entity).committed.is_none_or(|state| state.fm_stage != expected_stage)
                                || target.instance_disposal().is_some() != graceful
                            { return Err("stale/non-dropped ACK completed the original terminal-report obligation".into()); }
                        }
                        if graceful || closing_loss {
                            if timer(&mut target_adapter)?.due.saturating_duration_since(Instant::now()) < Duration::from_secs(25) {
                                return Err("Close/Drop did not retain the native 30-second report retry".into());
                            }
                            if provider && terminal_deletion.is_none() {
                                timer(&mut target_adapter)?.due = Instant::now();
                                let before = target_traffic.reports.len();
                                let end = Instant::now() + Duration::from_secs(10);
                                while target_traffic.reports.len() == before {
                                    peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await
                                        .map_err(|error| format!("Close report retry source peer progress: {error}"))?;
                                    peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await
                                        .map_err(|error| format!("Close report retry destination peer progress: {error}"))?;
                                    step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await
                                        .map_err(|error| format!("Close report retry source FM progress: {error}"))?;
                                    step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await
                                        .map_err(|error| format!("Close report retry destination FM progress: {error}"))?;
                                    if Instant::now() >= end { return Err("Close/Drop lost-ACK retry deadline".into()); }
                                }
                                if target_traffic.reports.last() != Some(&report) || target.retained_usage() != 0 {
                                    return Err("Close/Drop lost-ACK retry changed the original lifetime".into());
                                }
                            }
                        }
                        target_fm.send(replica_up_reply_message(&reply, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, traffic| traffic.removed && adapter.replica_drained(node)).await?;
                        let first = target_traffic.remove_reply.take().ok_or("missing Closing-loss metadata Delete reply")?;
                        let retry = ledger.deletion_retry(1, Instant::now() + Duration::from_secs(11))?
                            .ok_or("missing original Closing-loss metadata retry")?;
                        if retry != DeleteReplica::new(request.clone(), false) {
                            return Err("Closing-loss metadata retry changed original Idle lineage".into());
                        }
                        target_traffic.removed = false;
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&first) {
                            return Err("Closing-loss metadata reply replay changed the original tombstone".into());
                        }
                        ledger.reply(1, first, true)?;
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?).await?;
                        for _ in 0..20 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await?;
                            step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        let logs = target.activation().logs();
                        if target.snapshot(entity).committed.is_none_or(|state| !state.local_deleted || !state.peer_terminal_report()
                                || state.peer_host_loss() != !graceful
                                || state.phase != ra::Phase::Closed || state.fm_stage != ra::FmStage::None
                                || state.open_token != original.open_token)
                            || target.retained_usage() != 0 || target.instance_disposal().is_some() != graceful
                            || serde_json::to_value(target.instance_disposal()).map_err(err)? != serde_json::to_value(&disposal).map_err(err)?
                            || target.replica_absence().is_some() || target.pre_open_cancellation().is_some()
                            || target.failed_open_deletion().is_some() || target.failed_registration_deletion().is_some()
                            || ledger.placements[1].retirement.is_some() != terminal_deletion.is_some() || ledger.placements[1].reusable
                            || ledger.idle_deletion_status()["complete"] != terminal_deletion.is_some()
                            || ledger.idle_removal_status()["pending_remove"] != true
                            || ledger.idle_status()["state"] != "lost"
                            || target_adapter.peer_command_error.is_some() || source_adapter.peer_command_error.is_some()
                            || logs.iter().filter(|line| line.contains("LOCAL_CALLBACK open")).count() != 1
                            || logs.iter().filter(|line| line.contains("LOCAL_CALLBACK close")).count() != if aborting { 0 } else if graceful { 1 } else { usize::from(!provider) }
                            || logs.iter().filter(|line| line.contains("LOCAL_CALLBACK abort")).count() != usize::from(aborting)
                            || logs.iter().filter(|line| line.contains("LOCAL_DATA_PROVIDER acquired")).count() != 1
                            || logs.iter().any(|line| line.contains("LOCAL_DISPOSE complete")) != graceful
                        { return Err(format!("terminal ACK/late traffic violated ownership (mode={mode:?}, graceful={graceful}): state={:?}, usage={}, disposal={:?}, pending_remove={}, idle={}, target_error={:?}, source_error={:?}, logs={logs:?}",
                            target.snapshot(entity).committed.map(|state| (state.phase, state.fm_stage, state.local_deleted, state.peer_host_loss())),
                            target.retained_usage(), target.instance_disposal(),
                            ledger.idle_removal_status()["pending_remove"], ledger.idle_status()["state"],
                            target_adapter.peer_command_error, source_adapter.peer_command_error)); }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued") || data_request(&endpoint, b"read\n").await? != continued
                            || ledger.idle_removal_status()["pending_remove"] != false
                        { return Err("Closing-loss cleanup changed healthy Primary serving or removal ownership".into()); }
                        Ok(())
                    }.await;
                    let closed = target_fm.close().await;
                    let node_closed = target_fm_link.close().await;
                    return result.and(closed).and(node_closed);
                }
                if failed_open {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let result = async {
                        let deleting = ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::FailedOpen)?;
                        target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                        let mut target_traffic = Traffic::default();
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, traffic| traffic.removed && adapter.replica_drained(node)
                                && node.failed_open_deletion().is_some()).await?;
                        let lost = target_traffic.remove_reply.take().ok_or("missing failed Open DeleteReplicaReply")?;
                        let proof = target.failed_open_deletion().ok_or("missing original failed Open disposal lineage")?;
                        let disposal = &proof.failure.disposal;
                        if proof.failure.error != creation_error || disposal.ra_operation == proof.ra_operation
                            || proof.failure.creation == proof.ra_operation || lost.unit != disposal.body.unit
                            || disposal.lookup == 0 || disposal.acquisition == 0 || target.retained_usage() != 0
                            || target.instance_disposal().is_some() || target.replica_absence().is_some()
                            || target.pre_open_cancellation().is_some() || target.failed_registration_deletion().is_some()
                            || target.snapshot(entity).committed.is_none_or(|state| state.close_token != 0)
                        { return Err("failed Open deletion fabricated Close or substituted a no-runtime proof".into()); }
                        let mut published = None;
                        publish_failed_open(&launches[1], &target, &mut published)?;
                        ledger.read_failed_open(1, &launches[1].directory.join("failed-open-deletion.json"), std::process::id())?;
                        if ledger.idle_deletion_status()["state"] != "pending"
                            || ledger.idle_deletion_status()["disposed"] != true
                            || ledger.idle_deletion_status()["destination_deleted"] != false
                            || ledger.idle_deletion_status()["completion"] != serde_json::Value::Null
                        { return Err("failed Open disposal alone completed the lost semantic Delete reply".into()); }
                        let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                            .ok_or("missing original failed Open Delete retry")?;
                        if retry != deleting { return Err("failed Open Delete retry changed the descriptor".into()); }
                        target_traffic.removed = false;
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&lost) { return Err("failed Open Delete replay changed its reply".into()); }
                        ledger.reply(1, lost, true)?;
                        if ledger.idle_deletion_status()["completion"] != "failed_open"
                            || ledger.idle_deletion_status()["pending_remove"] != true
                            || ledger.idle_status()["error"] != creation_error || ledger.idle_status()["state"] != "failed"
                            || ledger.placements[1].reusable
                        { return Err("failed Open deletion erased Add failure or completed Primary removal".into()); }
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?).await?;
                        for _ in 0..20 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        let logs = target.activation().logs();
                        if serde_json::to_value(target.failed_open_deletion()).map_err(err)?
                                != serde_json::to_value(Some(&proof)).map_err(err)?
                            || target.snapshot(entity).committed.is_none_or(|state| !state.local_deleted || state.phase != ra::Phase::Closed)
                            || target.retained_usage() != 0 || target_adapter.peer_command_error.is_some()
                            || !target_traffic.reports.is_empty()
                            || logs.iter().filter(|line| line.contains("LOCAL_CALLBACK open")).count() != 1
                            || logs.iter().filter(|line| line.contains("LOCAL_CALLBACK abort")).count() != 1
                            || logs.iter().any(|line| line.contains("LOCAL_CALLBACK close"))
                            || logs.iter().filter(|line| line.contains("LOCAL_DATA_PROVIDER acquired")).count() != 1
                            || !logs.iter().any(|line| line.contains("LOCAL_DATA_PROVIDER disposed empty"))
                            || !logs.iter().any(|line| line.contains("LOCAL_DISPOSE complete"))
                        { return Err("late Create resurrected the failed deleted lifetime or required another Close".into()); }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued") || data_request(&endpoint, b"read\n").await? != continued
                            || ledger.idle_status()["error"] != creation_error
                            || ledger.idle_removal_status()["completion"] != "failed_creation_retired"
                        { return Err("failed Open cleanup changed Primary serving or original failure".into()); }
                        Ok(())
                    }.await;
                    let closed = target_fm.close().await;
                    let node_closed = target_fm_link.close().await;
                    return result.and(closed).and(node_closed);
                }
                if failed_registration {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let result = async {
                        let deleting = ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::FailedRegistration)?;
                        if target.failed_registration_deletion().is_some() {
                            return Err("failed registration certified Delete before its command".into());
                        }
                        target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                        let mut target_traffic = Traffic::default();
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, traffic| traffic.removed && adapter.replica_drained(node)
                                && node.failed_registration_deletion().is_some()).await?;
                        let lost = target_traffic.remove_reply.take().ok_or("missing failed registration DeleteReplicaReply")?;
                        let proof = target.failed_registration_deletion().ok_or("missing original failed registration lineage")?;
                        if proof.failure.error != creation_error || proof.failure.ra_operation == proof.ra_operation
                            || proof.failure.creation == proof.ra_operation || lost.unit != proof.failure.body.unit
                            || target.instance_disposal().is_some() || target.replica_absence().is_some()
                            || target.pre_open_cancellation().is_some() || target.retained_usage() != 0
                        { return Err("failed registration deletion fabricated disposal, absence or command lineage".into()); }
                        let mut published = None;
                        publish_failed_registration(&launches[1], &target, &mut published)?;
                        let path = launches[1].directory.join("failed-registration-deletion.json");
                        ledger.read_failed_registration(1, &path, std::process::id())?;
                        if ledger.idle_deletion_status()["state"] != "pending"
                            || ledger.idle_deletion_status()["failed_registration"] != true
                            || ledger.idle_deletion_status()["destination_deleted"] != false
                            || ledger.idle_deletion_status()["completion"] != serde_json::Value::Null
                        { return Err("failed registration proof completed a lost semantic Delete reply".into()); }
                        let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                            .ok_or("missing original failed registration Delete retry")?;
                        if retry != deleting { return Err("failed registration Delete retry changed the original descriptor".into()); }
                        target_traffic.removed = false;
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&lost) { return Err("failed registration Delete replay changed its reply".into()); }
                        ledger.reply(1, lost, true)?;
                        if ledger.idle_deletion_status()["completion"] != "failed_registration"
                            || ledger.idle_deletion_status()["disposed"] != false
                            || ledger.idle_deletion_status()["never_created"] != false
                            || ledger.idle_deletion_status()["pre_open_cancelled"] != false
                            || ledger.idle_deletion_status()["pending_remove"] != true
                            || ledger.idle_status()["error"] != creation_error || ledger.idle_status()["state"] != "failed"
                            || ledger.placements[1].created || ledger.placements[1].reusable
                        { return Err("failed registration deletion erased Add failure or completed Primary removal".into()); }
                        if !after_host_exit {
                            release_fixture_barrier(&target, "local-registration.release")?;
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, _, _| node.activation().logs().iter().any(|line| line.contains("LOCAL_REGISTERED"))).await?;
                        }
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?).await?;
                        for _ in 0..20 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        if serde_json::to_value(target.failed_registration_deletion()).map_err(err)?
                                != serde_json::to_value(Some(&proof)).map_err(err)?
                            || target.snapshot(entity).committed.is_none_or(|state| !state.local_deleted || state.phase != ra::Phase::Closed)
                            || target.retained_usage() != 0 || target_adapter.peer_command_error.is_some()
                            || !target_traffic.reports.is_empty()
                            || target.activation().logs().iter().any(|line| line.contains("LOCAL_CALLBACK")
                                || line.contains("LOCAL_DATA_PROVIDER acquired") || line.contains("LOCAL_RESOURCE acquired"))
                        { return Err("late registration/Create resurrected the failed deleted lifetime".into()); }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued") || data_request(&endpoint, b"read\n").await? != continued
                            || ledger.idle_status()["error"] != creation_error
                            || ledger.idle_removal_status()["completion"] != "failed_creation_retired"
                        { return Err("failed registration cleanup changed Primary serving or original failure".into()); }
                        Ok(())
                    }.await;
                    if !after_host_exit { release_fixture_barrier(&target, "local-registration.release")?; }
                    let closed = target_fm.close().await;
                    let node_closed = target_fm_link.close().await;
                    return result.and(closed).and(node_closed);
                }
                if pre_registration {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let result = async {
                        let deleting = ledger.request_idle_deletion_with_force(Instant::now(), crate::fm::IdleDeleteTarget::PreRegistration, force_before_open)?;
                        let lost = if force_before_open {
                            let mut observed = Traffic::default();
                            let before = target.pending_commands();
                            target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                            let end = Instant::now() + Duration::from_secs(5);
                            while target.pending_commands() == before {
                                receive(&launches[1], &mut target, &mut target_adapter, &target_fm_link).await?;
                                if Instant::now() >= end { return Err("force pre-Open Delete ingress deadline".into()); }
                            }
                            while target.snapshot(entity).committed.is_none_or(|state| state.phase != ra::Phase::Closed) {
                                target.drive().map_err(err)?;
                                target_adapter.capture(&mut target);
                                if Instant::now() >= end { return Err("force pre-Open Close commit deadline".into()); }
                                tokio::task::yield_now().await;
                            }
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut observed, |_, _, traffic| !traffic.reports.is_empty()).await?;
                            if target.pre_open_cancellation().is_some() || target.instance_disposal().is_some()
                                || target.replica_loss().is_some()
                                || target.activation().process_status().is_none_or(|status| status.exit_observed)
                                || target.snapshot(entity).committed.is_none_or(|state|
                                    state.phase != ra::Phase::Closed || state.local_deleted || state.fm_stage != ra::FmStage::ReplicaDropped)
                            { return Err("unregistered force Delete fabricated metadata/disposal or killed the app".into()); }
                            let ack = ledger.report(1, &observed.reports[0])?.ok_or("missing pre-Open force ACK")?;
                            target_fm.send(replica_up_reply_message(&ack, &launches[1].generation).map_err(err)?).await?;
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut observed, |node, _, traffic| traffic.removed && node.pre_open_cancellation().is_some()).await?;
                            observed.remove_reply.take().ok_or("missing force pre-Open metadata reply")?
                        } else {
                        let before = target.pending_commands();
                        target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                        let end = Instant::now() + Duration::from_secs(5);
                        while target.pending_commands() == before {
                            receive(&launches[1], &mut target, &mut target_adapter, &target_fm_link).await?;
                            if Instant::now() >= end { return Err("pre-registration Delete ingress deadline".into()); }
                        }
                        while !target_adapter.replies.iter().any(|work| matches!(work.effect, ra::Effect::DeleteReplicaReply(_))) {
                            target.drive().map_err(err)?;
                            target_adapter.capture(&mut target);
                            if Instant::now() >= end { return Err("pre-registration Delete effect deadline".into()); }
                            tokio::task::yield_now().await;
                        }
                        if (!after_host_exit && (target.retained_usage() != 1 || target.pre_open_cancellation().is_some()))
                            || target.instance_disposal().is_some() || target.replica_absence().is_some()
                            || target.snapshot(entity).committed.is_none_or(|state| state.phase != ra::Phase::Closed
                                || state.runtime.is_some() || state.usage.is_some() || !state.local_deleted)
                        { return Err("held late lookup ownership was released or certified early".into()); }
                        let lost = loop {
                            target_adapter.advance(&mut target, Some(&target_fm_link)).await?;
                            if let Some(mut incoming) = target_fm.receive(Instant::now() + Duration::from_millis(1)).await? {
                                let reply = decode_delete_replica_reply(&incoming.message).map_err(err)?.0;
                                if let Some(handoff) = incoming.handoff.as_mut() { handoff.acknowledge(); }
                                break reply;
                            }
                            if Instant::now() >= end { return Err("pre-registration semantic reply deadline".into()); }
                        };
                        if !after_host_exit && (target.retained_usage() != 1 || target.pre_open_cancellation().is_some()) {
                            return Err("Delete reply was mistaken for draining the original lookup".into());
                        }
                        lost
                        };
                        let mut target_traffic = Traffic::default();
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, _| adapter.replica_drained(node)
                                && node.pre_open_cancellation().is_some()).await?;
                        let mut published = None;
                        publish_pre_open_cancellation(&launches[1], &target, &mut published)?;
                        ledger.read_pre_open_cancellation(1, &launches[1].directory.join("pre-open-cancellation.json"), std::process::id())?;
                        if ledger.idle_deletion_status()["pre_open_cancelled"] != true
                            || ledger.idle_deletion_status()["state"] != "pending"
                            || ledger.idle_deletion_status()["completion"] != serde_json::Value::Null
                            || ledger.idle_deletion_status()["disposed"] != false
                            || ledger.idle_deletion_status()["never_created"] != false
                        { return Err("pre-Open proof completed a lost native reply or fabricated another proof".into()); }
                        let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                            .ok_or("missing pre-registration Delete retry")?;
                        if retry != deleting { return Err("pre-registration Delete retry changed lineage".into()); }
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&lost) { return Err("pre-registration Delete replay changed reply".into()); }
                        ledger.reply(1, lost, true)?;
                        if ledger.idle_deletion_status()["completion"] != "pre_open_cancelled"
                            || ledger.idle_deletion_status()["pending_remove"] != true
                            || ledger.placements[1].created != force_before_open || ledger.placements[1].reusable
                            || ledger.placements[1].retirement.is_some()
                        { return Err("pre-registration cancellation fabricated runtime creation, disposal or Primary removal".into()); }
                        let proof = serde_json::to_value(target.pre_open_cancellation()).map_err(err)?;
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?).await?;
                        for _ in 0..15 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        if target.retained_usage() != 0 || target.replica_absence().is_some() || target.instance_disposal().is_some()
                            || serde_json::to_value(target.pre_open_cancellation()).map_err(err)? != proof
                            || target_adapter.peer_last_reply.is_some() || target_adapter.peer_command_error.is_some()
                            || !target_traffic.reports.is_empty()
                            || target.activation().logs().iter().any(|line| line.contains("LOCAL_CALLBACK")
                                || line.contains("LOCAL_DATA_PROVIDER acquired") || line.contains("LOCAL_RESOURCE acquired"))
                        { return Err("late pre-registration traffic reopened or reacquired the deleted Idle".into()); }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued") || data_request(&endpoint, b"read\n").await? != continued {
                            return Err("pre-registration cancellation changed healthy Primary serving".into());
                        }
                        Ok(())
                    }.await;
                    let closed = target_fm.close().await;
                    let node_closed = target_fm_link.close().await;
                    return result.and(closed).and(node_closed);
                }
                if absent {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let operation = async {
                        if target.snapshot(entity).committed.is_some() || target.replica_absence().is_some() {
                            return Err("absent deletion did not start before original Create delivery".into());
                        }
                        let deleting = ledger.request_idle_deletion_with_force(Instant::now(), crate::fm::IdleDeleteTarget::Unobserved, force_before_open)?;
                        let mut target_traffic = Traffic::default();
                        target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, traffic| traffic.removed
                                && adapter.replica_drained(node) && node.replica_absence().is_some()).await?;
                        let lost = target_traffic.remove_reply.take().ok_or("missing absent DeleteReplicaReply")?;
                        let tombstone = target.snapshot(entity).committed.ok_or("missing absent tombstone")?;
                        if lost.unit != tombstone.body.unit || lost.replica != tombstone.body.replica
                            || lost.unit.target != 0 || lost.unit.minimum != 0
                            || target.instance_disposal().is_some() || target.retained_usage() != 0
                        {
                            return Err("absent Delete fabricated runtime disposal or a created FT description".into());
                        }
                        let mut published = None;
                        publish_absence(&launches[1], &target, &mut published)?;
                        let path = launches[1].directory.join("absence.json");
                        ledger.read_absence(1, &path, std::process::id())?;
                        if ledger.idle_deletion_status()["state"] != "pending"
                            || ledger.idle_deletion_status()["never_created"] != true
                            || ledger.idle_deletion_status()["disposed"] != false
                            || ledger.idle_deletion_status()["destination_deleted"] != false
                        {
                            return Err("absence observation completed the lost semantic Delete reply".into());
                        }
                        let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                            .ok_or("missing absent Delete retry")?;
                        if retry != deleting { return Err("absent Delete retry changed the original descriptor".into()); }
                        target_traffic.removed = false;
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&lost) {
                            return Err("absent Delete replay changed its canonical native reply".into());
                        }
                        ledger.reply(1, lost, true)?;
                        if ledger.idle_deletion_status()["state"] != "deleted"
                            || ledger.idle_deletion_status()["completion"] != "never_created"
                            || ledger.idle_deletion_status()["pending_remove"] != true
                            || ledger.placements[1].created || ledger.placements[1].reusable
                            || ledger.placements[1].retirement.is_some()
                        {
                            return Err("absent Delete fabricated disposal, creation, reuse or Primary removal".into());
                        }
                        let original_proof = serde_json::to_value(target.replica_absence()).map_err(err)?;
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        let end = Instant::now() + Duration::from_secs(5);
                        loop {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            if !target_adapter.peer_commands.is_empty() { break; }
                            if Instant::now() >= end { return Err("held original Create did not reach deleted destination".into()); }
                        }
                        for _ in 0..15 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        if target.snapshot(entity).committed.is_none_or(|state| state.open_token != 0
                                || state.phase != ra::Phase::Closed || !state.local_deleted)
                            || target.retained_usage() != 0 || target.instance_disposal().is_some()
                            || target_adapter.peer_command_error.is_some() || target_adapter.peer_last_reply.is_some()
                            || !target_traffic.reports.is_empty()
                            || serde_json::to_value(target.replica_absence()).map_err(err)? != original_proof
                            || target.activation().logs().iter().any(|line| line.contains("LOCAL_CALLBACK create_replica")
                                || line.contains("LOCAL_CALLBACK open") || line.contains("LOCAL_CALLBACK close")
                                || line.contains("LOCAL_DATA_PROVIDER acquired") || line.contains("LOCAL_DISPOSE begin"))
                        {
                            return Err("late Create opened/acquired/disposed a never-created destination".into());
                        }
                        let primary = source.snapshot(entity).committed.ok_or("Primary disappeared")?;
                        if primary.remote_idles.first().is_none_or(|remote| remote.build.is_some() || remote.ready || remote.reply.is_some()) {
                            return Err("late Create resurrected the cancelled source build".into());
                        }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued")
                            || data_request(&endpoint, b"read\n").await? != continued
                            || ledger.idle_deletion_status()["pending_remove"] != false
                        {
                            return Err("absent destination cleanup changed healthy Primary serving".into());
                        }
                        Ok(())
                    }.await;
                    let fm_closed = target_fm.close().await;
                    let node_closed = target_fm_link.close().await;
                    return operation.and(fm_closed).and(node_closed);
                }
                if !opening {
                if !matches!(outcome, PeerCreationOutcome::Opened) && !deleting {
                    source_link
                        .send(
                            create_replica_message(&create, &launches[0].generation)
                                .map_err(err)?,
                        )
                        .await?;
                }
                let end = Instant::now() + Duration::from_secs(8);
                loop {
                    peer_step(
                        &launches[1],
                        &mut target,
                        &mut target_adapter,
                        &target_link,
                        None,
                    )
                    .await?;
                    if let Some(mut incoming) = source_link
                        .receive(Instant::now() + Duration::from_millis(1))
                        .await?
                    {
                        let reply = decode_create_replica_reply(&incoming.message)
                            .map_err(err)?
                            .0;
                        if reply.error != 0 || reply.replica.replication_endpoint.is_empty() {
                            return Err("destination did not actually Open".into());
                        }
                        let command = receive_peer(&launches[0], &mut source, &incoming.message)?
                            .ok_or("late peer reply capacity")?;
                        source_adapter.peer_commands.push(command);
                        if let Some(handoff) = incoming.handoff.as_mut() {
                            handoff.acknowledge();
                        }
                        break;
                    }
                    if Instant::now() >= end {
                        return Err("late CreateReplicaReply deadline".into());
                    }
                }
                until(
                    &launches[0],
                    &mut source,
                    &mut source_adapter,
                    &fm,
                    &fm_link,
                    &mut traffic,
                    |_, adapter, _| adapter.peer_commands.is_empty(),
                )
                .await?;
                let current = source
                    .snapshot(entity)
                    .committed
                    .ok_or("Primary disappeared")?;
                let remote = current
                    .remote_idles.first()
                    .ok_or("cancellation tombstone disappeared")?;
                if remote.reply.as_ref().map(|reply| reply.error)
                    != failed.then_some(ra::RA_NOT_READY)
                    || remote.build.is_some()
                    || remote.ready
                    || remote.remove_reply().as_ref() != Some(&expected)
                    || traffic.idle_adds.len() != usize::from(failed)
                {
                    return Err(
                        "late Open resurrected cancelled creation or started RAP build".into(),
                    );
                }
                let idle = target
                    .snapshot(entity)
                    .committed
                    .ok_or("destination disappeared")?;
                if idle.phase != ra::Phase::Ready
                    || idle.body.replica.current_role != 2
                    || idle.body.replica.state != 1
                    || target.retained_usage() != 1
                    || target
                        .activation()
                        .logs()
                        .iter()
                        .filter(|line| line.contains("LOCAL_CALLBACK open"))
                        .count()
                        != 1
                {
                    return Err("cancellation disposed or reopened the original destination".into());
                }
                let continued = data_request(&endpoint, if deleting { b"read\n" } else { b"write-more\n" }).await?;
                if !continued.contains(if deleting { "committed=1 value=acknowledged" } else { "committed=2 value=continued" })
                    || data_request(&endpoint, b"read\n").await? != continued
                {
                    return Err("cancellation changed healthy Primary serving".into());
                }
                let state: serde_json::Value =
                    serde_json::from_str(&data_request(&endpoint, b"state\n").await?)
                        .map_err(err)?;
                if state["peers"]
                    .as_array()
                    .is_none_or(|peers| !peers.is_empty())
                {
                    return Err("cancelled creation reached the Primary replicator".into());
                }
                }
                if deleting {
                    let (target_fm, target_fm_link) = links(&launches[1]).await?;
                    let deletion = async {
                        let deleting = ledger.request_idle_deletion(Instant::now(), crate::fm::IdleDeleteTarget::Runtime)?;
                        let mut foreign_force = DeleteReplica::new(deleting.replica.0.clone(), true);
                        foreign_force.replica.0.replica.id += 1;
                        if receive_command(&launches[1], &mut target,
                            &delete_replica_message(&foreign_force,
                                &launches[1].generation).map_err(err)?).is_ok()
                        {
                            return Err("peer Delete admitted foreign force/host termination".into());
                        }
                        let mut older_instance = deleting.clone();
                        older_instance.replica.0.replica.instance -= 1;
                        target_fm.send(delete_replica_message(&older_instance, &launches[1].generation).map_err(err)?).await?;
                        let mut target_traffic = Traffic::default();
                        if opening {
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, _, _| node.activation().logs().iter()
                                    .any(|line| line.contains("LOCAL_OPEN cancelled"))).await?;
                            let closing = target.snapshot(entity).committed.ok_or("missing closing original Idle")?;
                            if closing.phase != ra::Phase::Closing || !closing.local_deleted || closing.cleanup_pending
                                || closing.runtime.is_none() || target.retained_usage() != 1
                                || target.instance_disposal().is_some() || target.replica_absence().is_some()
                                || target_traffic.removed || !target_traffic.reports.is_empty()
                                || target_adapter.peer_last_reply.is_some()
                            {
                                return Err("held registered Open completed deletion/creation or released ownership early".into());
                            }
                            source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                        }
                        let continued = data_request(&endpoint, b"write-more\n").await?;
                        if !continued.contains("committed=2 value=continued") {
                            return Err("held destination deletion blocked the Primary".into());
                        }
                        let marker = if provider { "LOCAL_DATA_PROVIDER dispose begin" } else { "LOCAL_DISPOSE begin" };
                        if opening {
                            release_fixture_barrier(&target, "local-open.release")?;
                        }
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, _, _| node.activation().logs().iter().any(|line| line.contains(marker))).await?;
                        target_fm.send(delete_replica_message(&deleting, &launches[1].generation).map_err(err)?).await?;
                        for _ in 0..15 {
                            step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                        }
                        let closing = target.snapshot(entity).committed.ok_or("destination disappeared during Delete")?;
                        if closing.phase != ra::Phase::Closing || !closing.local_deleted || closing.cleanup_pending
                            || target.retained_usage() != 1 || target.instance_disposal().is_some()
                            || target_traffic.removed || !target_traffic.reports.is_empty()
                            || ledger.idle_deletion_status()["complete"] != false
                            || ledger.idle_removal_status()["pending_remove"] != true
                        {
                            return Err("held Idle disposal published deletion or released its original use".into());
                        }
                        if data_request(&endpoint, b"read\n").await? != continued {
                            return Err("held destination disposal changed the Primary".into());
                        }
                        if force_during_disposal {
                            let forced = ledger.request_idle_deletion_with_force(Instant::now(), crate::fm::IdleDeleteTarget::Runtime, true)?;
                            target_fm.send(delete_replica_message(&forced, &launches[1].generation).map_err(err)?).await?;
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, _, _| node.retained_usage() == 0
                                    && node.activation().process_status().is_some_and(|status| status.exit_observed)).await?;
                            if target.instance_disposal().is_some() || target_traffic.removed || !target_traffic.reports.is_empty()
                                || ledger.idle_deletion_status()["complete"] != false
                            { return Err("force escalation fabricated disposal, reporting or a Delete reply".into()); }
                            let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                                .ok_or("missing closed force Delete retry")?;
                            if retry != forced { return Err("force escalation lost its original request".into()); }
                            target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, adapter, traffic| traffic.removed
                                    && adapter.replica_drained(node) && node.replica_loss().is_some()).await?;
                            let profile = actual_profile(&target)?;
                            ledger.host_loss(1, HostLossReceipt {
                                deployment: launches[1].deployment.clone(), session: launches[1].session.clone(),
                                node: launches[1].node.clone(), fm: launches[1].fm.clone(), generation: launches[1].generation.clone(),
                                node_pid: std::process::id(), app_pid: profile.app_pid, runtime_root: profile.runtime_root,
                                loss: target.replica_loss().ok_or("missing exact original host-loss proof")?,
                            }, std::process::id())?;
                            if ledger.idle_deletion_status()["complete"] != false {
                                return Err("host loss alone acknowledged the missing Delete reply".into());
                            }
                            ledger.reply(1, target_traffic.remove_reply.take().ok_or("missing closed force reply")?, true)?;
                            if ledger.idle_deletion_status()["completion"] != "host_lost"
                                || ledger.idle_deletion_status()["disposed"] != false
                                || ledger.idle_deletion_status()["pending_remove"] != true
                                || ledger.placements[1].retirement.is_some() || ledger.placements[1].reusable
                            { return Err("force escalation merged disposal, removal or replacement authority".into()); }
                            let original_loss = serde_json::to_value(target.replica_loss()).map_err(err)?;
                            source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                            for _ in 0..15 {
                                peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                                peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await?;
                                step(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link, &mut target_traffic).await?;
                            }
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, adapter, _| adapter.replica_drained(node) && node.replica_loss().is_some()).await?;
                            if target_adapter.peer_command_error.is_some()
                                || serde_json::to_value(target.replica_loss()).map_err(err)? != original_loss
                                || target.snapshot(entity).committed.is_none_or(|state| !state.local_deleted
                                    || state.phase != ra::Phase::Closed || state.open_token != closing.open_token)
                            { return Err("late Create altered the forced original tombstone or loss proof".into()); }
                            ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                            if data_request(&endpoint, b"read\n").await? != continued {
                                return Err("forced destination deletion changed healthy Primary data".into());
                            }
                            return Ok(());
                        }
                        release_disposal(&target, if provider { StatefulDown::CloseProvider } else { StatefulDown::CloseService })?;
                        if opening {
                            until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                                &mut target_traffic, |node, _, _| node.activation().logs().iter()
                                    .any(|line| line.contains("LOCAL_DISPOSE complete"))).await?;
                            if target.snapshot(entity).committed.is_some_and(|state| state.phase == ra::Phase::Closing) {
                                target.retry(request.unit.id.clone()).map_err(err)?;
                            }
                        }
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |node, adapter, traffic| traffic.removed
                                && adapter.replica_drained(node) && node.instance_disposal().is_some()).await?;
                        let profile = actual_profile(&target)?;
                        let proof = RetirementReceipt {
                            deployment: launches[1].deployment.clone(), session: launches[1].session.clone(),
                            node: launches[1].node.clone(), fm: launches[1].fm.clone(), generation: launches[1].generation.clone(),
                            node_pid: std::process::id(), app_pid: profile.app_pid, runtime_root: profile.runtime_root,
                            disposal: target.instance_disposal().ok_or("missing original Idle disposal proof")?,
                        };
                        let first_reply = target_traffic.remove_reply.take().ok_or("missing destination DeleteReplicaReply")?;
                        if after_host_exit {
                            let pid = target.activation().process_status().ok_or("missing disposed app process")?.pid;
                            if !std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().map_err(err)?.success() {
                                return Err("failed to terminate the already disposed original app".into());
                            }
                        }
                        ledger.retirement(1, proof.clone(), std::process::id())?;
                        if ledger.idle_deletion_status()["state"] != "pending"
                            || ledger.idle_deletion_status()["destination_deleted"] != false
                            || ledger.idle_deletion_status()["disposed"] != true
                        {
                            return Err("disposal alone completed a lost destination DeleteReplicaReply".into());
                        }
                        let retry = ledger.idle_deletion_retry(1, Instant::now() + Duration::from_secs(11))
                            .ok_or("missing original destination Delete retry")?;
                        if retry != deleting { return Err("Idle deletion retry changed its original request".into()); }
                        target_traffic.removed = false;
                        target_fm.send(delete_replica_message(&retry, &launches[1].generation).map_err(err)?).await?;
                        until(&launches[1], &mut target, &mut target_adapter, &target_fm, &target_fm_link,
                            &mut target_traffic, |_, _, traffic| traffic.removed).await?;
                        if target_traffic.remove_reply.as_ref() != Some(&first_reply) {
                            return Err("destination deletion replay changed its semantic reply".into());
                        }
                        ledger.reply(1, first_reply, true)?;
                        if ledger.idle_deletion_status()["state"] != "deleted"
                            || ledger.idle_deletion_status()["pending_remove"] != true
                            || ledger.placements[1].reusable
                            || target.retained_usage() != 0 || target.replica_absence().is_some()
                        {
                            return Err("destination Delete completed Primary removal or authorized replacement".into());
                        }
                        if !opening && !after_host_exit {
                            let mut fresh = create.clone();
                            let (id, instance) = launches[1].idle_successor()?;
                            fresh.replica.0.replica.id = id;
                            fresh.replica.0.replica.instance = instance;
                            let message = create_replica_message(&fresh, &launches[0].generation).map_err(err)?;
                            receive_command(&launches[1], &mut target,
                                &delete_replica_message(&deleting, &launches[1].generation).map_err(err)?)?;
                            let pending = target.pending_commands();
                            let before = target.snapshot(entity).committed.ok_or("missing closed owner before busy Create")?;
                            // Queued old-owner work must backpressure a fresh Create, not close the peer.
                            for _ in 0..2 {
                                if receive_peer(&launches[1], &mut target, &message)?.is_some()
                                    || target.pending_commands() != pending
                                    || target.snapshot(entity).committed.is_none_or(|state|
                                        state.body != before.body || !state.local_deleted)
                                {
                                    return Err("busy Create changed the closed owner or consumed admission".into());
                                }
                            }
                        }
                        source_link.send(create_replica_message(&create, &launches[0].generation).map_err(err)?).await?;
                        fm.send(add_replica_message(&request, &launches[0].generation).map_err(err)?).await?;
                        for _ in 0..15 {
                            peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, Some(&target_fm_link)).await?;
                            step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
                        }
                        let closed = target.snapshot(entity).committed.ok_or("deleted destination tombstone disappeared")?;
                        if closed.phase != ra::Phase::Closed || !closed.local_deleted || target.retained_usage() != 0
                            || serde_json::to_value(target.instance_disposal()).map_err(err)?
                                != serde_json::to_value(Some(&proof.disposal)).map_err(err)?
                            || target_adapter.peer_command_error.is_some()
                            || target.activation().logs().iter().filter(|line| line.contains("LOCAL_CALLBACK open")).count() != 1
                            || target.activation().logs().iter().filter(|line| line.contains("LOCAL_CALLBACK close")).count() != usize::from(!opening)
                            || target.activation().logs().iter().filter(|line| line.contains("LOCAL_CALLBACK abort")).count() != usize::from(opening)
                            || target.activation().logs().iter().filter(|line| line.contains("LOCAL_DATA_PROVIDER acquired")).count() != 1
                            || !target_traffic.reports.is_empty()
                            || source.snapshot(entity).committed.is_none_or(|state| state.remote_idles.first().is_none_or(|remote| remote.build.is_some() || remote.ready))
                        {
                            return Err("late peer/build traffic resurrected or reacquired the deleted Idle".into());
                        }
                        if opening && (target_adapter.peer_last_reply.is_some()
                            || !traffic.idle_adds.is_empty()
                            || !target.activation().logs().iter().any(|line| line.contains("LOCAL_OPEN released")))
                        {
                            return Err("cancelled Open published peer/FM creation success".into());
                        }
                        ledger.idle_removal_reply(0, traffic.idle_removals[1].clone())?;
                        if ledger.idle_deletion_status()["pending_remove"] != false
                            || data_request(&endpoint, b"read\n").await? != continued
                        {
                            return Err("independent deletion/removal changed healthy Primary serving".into());
                        }
                        Ok(())
                    }.await;
                    let target_fm_closed = target_fm.close().await;
                    let target_node_closed = target_fm_link.close().await;
                    deletion.and(target_fm_closed).and(target_node_closed)?;
                }
                Ok(())
            }
            .await;
            if failed_registration {
                release_fixture_barrier(&target, "local-registration.release")?;
            }
            if failed_open || normal_close {
                release_disposal(&target, if provider { StatefulDown::CloseProvider } else { StatefulDown::CloseService })?;
            }
            if deleting {
                if opening {
                    release_fixture_barrier(&target, "local-open.release")?;
                }
                release_disposal(&target, if provider { StatefulDown::CloseProvider } else { StatefulDown::CloseService })?;
            }
            let source_closed = source_link.close().await;
            let target_closed = target_link.close().await;
            let fm_closed = fm.close().await;
            let node_closed = fm_link.close().await;
            operation
                .and(source_closed)
                .and(target_closed)
                .and(fm_closed)
                .and(node_closed)
        }
        .await;
        let source_cleanup = cleanup(
            &launches[0],
            &mut source,
            &source_store,
            &mut source_adapter,
        )
        .await;
        let target_cleanup = cleanup(
            &launches[1],
            &mut target,
            &target_store,
            &mut target_adapter,
        )
        .await;
        source_cleanup.map_err(|error| format!("source cleanup: {error}; operation={result:?}"))?;
        target_cleanup.map_err(|error| format!("target cleanup: {error}; operation={result:?}"))?;
        Ok(result)
    }

    pub(crate) async fn run_peer_idle_stale(launches: &[Launch; 2]) -> Result<Result<()>> {
        let (mut source, source_store) = create_node(&launches[0]).await?;
        let (mut target, target_store) = create_node(&launches[1]).await?;
        let mut source_adapter = Adapter::default();
        let mut target_adapter = Adapter::default();
        let result = async {
            source.start(&launches[0].package, APPLICATION,
                &[("Arguments".into(), "--stateful-primary".into())]).map_err(err)?;
            target.start(&launches[1].package, APPLICATION,
                &[("Arguments".into(), "--stateful-lifecycle --data-replicator".into())]).map_err(err)?;
            reject_unbound_peer_topology(launches, &mut source, &mut target)?;
            let (fm, fm_link) = links(&launches[0]).await?;
            let body = add_body(&launches[0], &actual_profile(&source)?);
            fm.send(add_primary_message(&body, &launches[0].generation).map_err(err)?).await?;
            let mut traffic = Traffic::default();
            until(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link,
                &mut traffic, |_, adapter, traffic| traffic.added && adapter.notification_pending.is_none()).await?;
            let endpoint = traffic.add_reply.as_ref().ok_or("missing Primary reply")?
                .replica.service_endpoint                .clone();
            let ready_end = Instant::now() + Duration::from_secs(2);
            while !data_request(&endpoint, b"read\n").await?.starts_with("OK ") {
                if Instant::now() >= ready_end {
                    return Err("public Primary readiness deadline".into());
                }
                step(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link, &mut traffic).await?;
            }
            let written = data_request(&endpoint, b"write\n").await?;
            if !written.contains("value=acknowledged") {
                return Err("Primary mutation did not complete".into());
            }
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
            let address = listener.local_addr().map_err(err)?.to_string();
            let (source_link, target_link) = tokio::join!(
                dial_peer(&launches[0], &address),
                async {
                    let (stream, _) = listener.accept().await.map_err(err)?;
                    accept_peer(&launches[1], stream).await
                }
            );
            drop(listener);
            let source_link = source_link?;
            let target_link = target_link?;
            let operation = async {
                create_idle(&launches[0], &mut source)?;
                let entity = body.unit.id.guid.0;
                let end = Instant::now() + Duration::from_secs(8);
                loop {
                    peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await?;
                    peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, None).await?;
                    if source.snapshot(entity).committed.as_ref()
                        .and_then(|state| state.remote_idles.first())
                        .is_some_and(|remote| remote.reply.is_some())
                    {
                        break;
                    }
                    if Instant::now() >= end {
                        return Err(format!("peer Open deadline: {:?}, {:?}", source_adapter.peer_command_error, target_adapter.peer_command_error));
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                let original = target.snapshot(entity).committed.ok_or("missing Idle")?;
                let request = source.snapshot(entity).committed.ok_or("missing Primary")?
                    .remote_idles.first().ok_or("missing remote request")?.body.clone();
                let mut stale_epoch = (*request).clone();
                stale_epoch.replica.0.unit.current.configuration -= 1;
                for request in [&stale_epoch, request.as_ref()] {
                    source_link.send(create_replica_message(request, &launches[0].generation).map_err(err)?).await?;
                    for _ in 0..12 {
                        peer_step(&launches[0], &mut source, &mut source_adapter, &source_link, Some(&fm_link)).await?;
                        peer_step(&launches[1], &mut target, &mut target_adapter, &target_link, None).await?;
                    }
                    let current = target.snapshot(entity).committed.ok_or("Idle disappeared")?;
                    if current.body != original.body || current.open_token != original.open_token
                        || current.usage != original.usage || target.retained_usage() != 1
                    {
                        return Err("duplicate/stale/replacement request changed the original Idle lifetime".into());
                    }
                }
                let logs = target.activation().logs();
                if logs.iter().filter(|line| line.contains("LOCAL_CALLBACK open")).count() != 1
                    || logs.iter().filter(|line| line.contains("LOCAL_DATA_PROVIDER acquired")).count() != 1
                    || !logs.iter().any(|line| line.contains("LOCAL_ACCESS denied"))
                {
                    return Err("Idle Open/acquisition/access observations differ from one real denied Open".into());
                }
                Ok(())
            }.await;
            let source_closed = source_link.close().await;
            let target_closed = target_link.close().await;
            let still_serving = data_request(&endpoint, b"read\n").await?;
            if still_serving != written
                || source.snapshot(body.unit.id.guid.0).committed
                    .is_none_or(|state| state.phase != ra::Phase::Ready || state.body.replica.current_role != 4)
            {
                return Err("peer loss changed healthy Primary serving".into());
            }
            traffic.added = false;
            fm.send(add_primary_message(&body, &launches[0].generation).map_err(err)?).await?;
            until(&launches[0], &mut source, &mut source_adapter, &fm, &fm_link,
                &mut traffic, |_, _, traffic| traffic.added).await?;
            fm.close().await?;
            fm_link.close().await?;
            operation.and(source_closed).and(target_closed)
        }.await;
        let source_cleanup = cleanup(
            &launches[0],
            &mut source,
            &source_store,
            &mut source_adapter,
        )
        .await;
        let target_cleanup = cleanup(
            &launches[1],
            &mut target,
            &target_store,
            &mut target_adapter,
        )
        .await;
        source_cleanup?;
        target_cleanup?;
        Ok(result)
    }
    async fn run_case(launch: &Launch, reuse: bool) -> Result<Result<()>> {
        let (mut node, store) = create_node(launch).await?;
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
