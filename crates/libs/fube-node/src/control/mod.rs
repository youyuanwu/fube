//! One caller-driven FM/peer adapter per owned local Node.
//!
//! Transport writes, applied RA commands, semantic ACKs and physical cleanup
//! remain separate. Shared ingress owns capability admission and dispatch;
//! compositions retain original Channels and process ownership. The adapter
//! retains bounded sends, timers and ACK observations.

pub mod ingress;
pub mod wire;

use crate::local::{FmWork, LocalDisposition, Node};
use fube_ra::{entity as ra, protocol::*};
use std::{collections::VecDeque, future::Future, pin::Pin, time::Duration};
use tokio::time::Instant;
use wire::Link;

pub const MAX_BODY_BYTES: usize = 64 * 1024;
pub type Result<T> = std::result::Result<T, String>;

pub(crate) fn err(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

#[doc(hidden)]
pub struct Escrow {
    pub original: ra::Dispatched,
    pub due: Instant,
    pub submitted: Option<u64>,
    pub sent: bool,
}
#[derive(Default)]
pub struct Adapter {
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
pub enum SemanticAck {
    Notification(ServiceTypeNotification),
    Report(ReplicaUp),
}
type UploadRequest =
    Pin<Box<dyn Future<Output = Result<Option<fube_transport::message::Message>>> + Send>>;

pub struct LfumUploadState {
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
    pub fn is_in_flight(&self) -> bool {
        self.request.is_some()
    }

    pub fn retry_at(&self) -> Instant {
        self.retry_at
    }

    pub fn frozen_body(&self) -> Option<&LfumUpload> {
        self.body.as_ref()
    }

    /// Poll the original request, or wait while none is in flight.
    pub async fn receive(&mut self) -> Result<Option<fube_transport::message::Message>> {
        match self.request.as_mut() {
            Some(request) => request.await,
            None => std::future::pending().await,
        }
    }

    pub fn complete(
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

    pub fn advance(
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

pub fn peer_target(effect: &ra::Effect) -> Option<&NodeInstance> {
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

pub fn ticks(ticks: i64) -> Duration {
    Duration::from_nanos((ticks.max(0) as u64).saturating_mul(100))
}
impl Adapter {
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn peer_error(&self) -> Option<&str> {
        self.peer_error.as_deref()
    }

    pub fn peer_command_error(&self) -> Option<&str> {
        self.peer_command_error.as_deref()
    }

    pub fn record_error(&mut self, error: String) {
        self.error = Some(error);
    }

    pub fn record_peer_error(&mut self, error: String) {
        self.peer_error = Some(error);
    }

    pub fn record_peer_command_error(&mut self, error: String) {
        self.peer_command_error = Some(error);
    }

    pub fn last_peer_reply(&self) -> Option<&CreateReplicaReply> {
        self.peer_last_reply.as_ref()
    }

    pub fn rejected_change_configuration_reply(&self) -> Option<&FailoverUnitReply> {
        self.rejected_change_configuration_reply.as_ref()
    }

    pub fn notification_pending(&self) -> Option<&ServiceTypeNotification> {
        self.notification_pending.as_ref()
    }

    pub fn report_pending(&self) -> Option<&ReplicaUp> {
        self.report_pending.as_ref()
    }

    pub fn peer_commands(&self) -> &[u64] {
        &self.peer_commands
    }

    pub fn acknowledgements(&self) -> &[(u64, SemanticAck)] {
        &self.acknowledgements
    }

    pub fn peer_replies(&self) -> &VecDeque<ra::Dispatched> {
        &self.peer_replies
    }

    pub fn replies(&self) -> &VecDeque<ra::Dispatched> {
        &self.replies
    }

    pub fn retained_effects(&self) -> impl Iterator<Item = &ra::Dispatched> {
        self.escrow.iter().map(|item| &item.original)
    }

    pub fn report_retry_submitted(&self) -> bool {
        self.escrow.iter().any(|item| {
            matches!(item.original.effect, ra::Effect::RequestReportRetry)
                && item.submitted.is_some()
        })
    }

    pub fn track_peer_command(&mut self, id: u64) -> Result<()> {
        if self.peer_commands.len() == 32 {
            return Err("peer command observation capacity".into());
        }
        self.peer_commands.push(id);
        Ok(())
    }

    pub fn track_acknowledgement(&mut self, acknowledgement: (u64, SemanticAck)) -> Result<()> {
        if self.acknowledgements.len() == 32 {
            return Err("semantic acknowledgement observation capacity".into());
        }
        self.acknowledgements.push(acknowledgement);
        Ok(())
    }

    #[cfg(feature = "qualification")]
    pub fn qualification_report_retry(&mut self) -> Result<&mut Escrow> {
        let mut owners = self
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

    #[cfg(feature = "qualification")]
    pub fn qualification_take_peer_reply(&mut self) -> Option<ra::Dispatched> {
        self.peer_replies.pop_front()
    }

    pub fn reject_deprecated(
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

    pub fn capture(&mut self, node: &mut Node) {
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
    pub fn commands(&mut self, node: &mut Node) -> Result<()> {
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
    pub async fn advance(&mut self, node: &mut Node, link: Option<&Link>) -> Result<()> {
        self.advance_with_peer(node, link, None, None).await
    }
    pub async fn advance_with_peer(
        &mut self,
        node: &mut Node,
        link: Option<&Link>,
        peer_link: Option<&Link>,
        peer: Option<&NodeInstance>,
    ) -> Result<()> {
        self.capture(node);
        self.commands(node)?;
        if let Some(link) = peer_link {
            // Peer control follows the Node's FM send generation, not socket bootstrap.
            link.advance_generation(&GenerationHeader {
                number: node.generation_state().send,
                for_fm_replica: false,
            });
        }
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
                    ra::Effect::PeerCreateReplica { target, body } if target == peer => {
                        create_replica_message(body, &link.generation())
                    }
                    ra::Effect::PeerCreateReplicaReply { target, body } if target == peer => {
                        create_replica_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerActivate { target, body } if target == peer => {
                        activate_message(body, &link.generation())
                    }
                    ra::Effect::PeerActivateReply { target, body } if target == peer => {
                        activate_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerDeactivate { target, body } if target == peer => {
                        deactivate_message(body, &link.generation())
                    }
                    ra::Effect::PeerDeactivateReply { target, body } if target == peer => {
                        deactivate_reply_message(body, &link.generation())
                    }
                    ra::Effect::PeerContinueSwap { target, body } if target == peer => {
                        do_reconfiguration_message(body, &link.generation())
                    }
                    ra::Effect::PeerGetLsn { target, body } if target == peer => {
                        get_lsn_message(body, &link.generation())
                    }
                    ra::Effect::PeerGetLsnReply { target, body } if target == peer => {
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
                    Err(crate::local::Error::Capacity) => return Ok(()),
                    Err(error) => return Err(err(error)),
                }
            }
            return Ok(());
        }
        Ok(())
    }
    pub fn pending(&self) -> usize {
        self.escrow.len()
            + self.replies.len()
            + self.peer_replies.len()
            + self.peer_commands.len()
            + self.acknowledgements.len()
            + usize::from(self.status_capture.is_some())
            + usize::from(self.status_cleanup_command.is_some())
    }
    pub fn semantic_pending(&self) -> usize {
        self.notification_pending
            .as_ref()
            .map_or(0, |body| body.infos.0.len())
            + usize::from(self.report_pending.is_some())
            + usize::from(self.status_due.is_some() || self.status_requested)
    }
    pub fn replica_drained(&self, node: &Node) -> bool {
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
