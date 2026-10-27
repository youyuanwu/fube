use super::*;

const DEMOTE_COMPLETED: i64 = 0x80071d24u32 as i32 as i64;
const STATE_CHANGED: i64 = 0x80071d28u32 as i32 as i64;
const MAX_REMOTE_TOMBSTONES: usize = 256;

pub(super) struct Pending {
    pub(super) cancel: Cancellation,
    sequence: u64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum RemotePhase {
    Building,
    Ready,
    Failed,
    Dropped,
}
pub(super) struct Remote {
    descriptor: w::Replica,
    phase: RemotePhase,
    pub(super) busy: bool,
    pub(super) cancel: Cancellation,
    sequence: u64,
}
impl Remote {
    pub(super) fn building(&self) -> bool {
        self.busy && self.phase == RemotePhase::Building
    }
}

impl Registry {
    pub(super) fn validate_control(&self, action: &str, request: &w::ProxyRequest) -> Result<()> {
        let empty_runtime = matches!(
            action,
            "UpdateConfiguration" | "ReplicatorBuildIdleReplica" | "ReplicatorRemoveIdleReplica"
        );
        if (empty_runtime && !request.runtime.is_empty())
            || (!empty_runtime && request.runtime != self.current.snapshot().runtime.id)
            || !request.service.name.is_empty()
            || !request.service.application.is_empty()
            || !request.service.service_type.name.is_empty()
            || request.service.stateful
            || request.service.persisted
            || request.service.group
            || request.remote.0.len() > MAX_REPLICA_SET_SIZE
            || request.unit.current.configuration < 0
            || request.unit.current.data_loss < 0
            || (request.unit.current.configuration == 0 && request.unit.current.data_loss != 0)
        {
            return Err(Error::Protocol("invalid native stateful control context"));
        }
        let valid_shape = match action {
            "UpdateConfiguration" => request.flags & !3 == 0 && !request.remote.0.is_empty(),
            "ReplicatorBuildIdleReplica" | "ReplicatorRemoveIdleReplica" => {
                request.flags == 0 && request.remote.0.len() == 1
            }
            _ => request.flags == 0 && request.remote.0.is_empty(),
        };
        if !valid_shape {
            return Err(Error::Protocol(
                "invalid stateful action flags/replica vector",
            ));
        }
        Ok(())
    }

    pub(super) async fn control(
        &self,
        unit: &Arc<Unit>,
        action: &str,
        request: w::ProxyRequest,
    ) -> Result<()> {
        {
            let state = unit.state.lock().unwrap();
            if stale(&state.request, &request) || !state.valid.load(Ordering::Acquire) {
                return Ok(());
            }
            if state.request.local.instance < request.local.instance {
                return Err(Error::Protocol(
                    "stateful control overlaps newer local incarnation",
                ));
            }
            let status = state.status.lock().unwrap();
            if status.lifecycle != ReplicaLifecycle::Open {
                return Ok(());
            }
        }
        match action {
            "ProxyReplicaEndpointUpdatedReply" => {
                let mut state = unit.state.lock().unwrap();
                let status = state.status.lock().unwrap();
                let matches = request.local.id == state.request.local.id
                    && request.local.instance == state.request.local.instance
                    && role(request.local.current_role)? == status.current_role
                    && request.local.service_endpoint == status.service_address;
                drop(status);
                if matches {
                    if let Some(pending) = state.endpoint_pending.take() {
                        pending.cancel();
                    }
                }
                Ok(())
            }
            "ReplicatorBuildIdleReplica" => self.build(unit, request).await,
            "ReplicatorRemoveIdleReplica" => self.remove(unit, request, true).await,
            "CancelCatchupReplicaSet" => self.cancel_catch_up(unit, request).await,
            "UpdateConfiguration" => self.reconfigure(unit, request).await,
            "ReplicatorGetStatus" | "ReplicatorUpdateEpochAndGetStatus" => {
                let Ok(_gate) = unit.gate.clone().try_acquire_owned() else {
                    return Ok(());
                };
                let (replicator, cancel) = participants(unit)?;
                let mut failed_api = (6, 4);
                let mut revision = primary_fence(unit);
                let result = async {
                    if action == "ReplicatorUpdateEpochAndGetStatus" {
                        if !self.admit_epoch(unit, &request)? {
                            return Err(Error::Closed);
                        }
                        revision = primary_fence(unit);
                        self.update_epoch(unit, &replicator, &cancel).await?;
                    }
                    failed_api = (12, 4);
                    self.progress(unit, replicator).await
                }
                .await;
                if matches!(result, Err(Error::Closed)) {
                    return Ok(());
                }
                let mut response = request;
                if let Ok(progress) = &result {
                    response.local.first_lsn = progress.first;
                    response.local.last_lsn = progress.last;
                }
                self.reply_guarded(
                    unit,
                    &format!("{action}Reply"),
                    response,
                    result.err(),
                    ReplyControl {
                        api: Some(failed_api),
                        revision: Some(revision),
                    },
                )
                .await
            }
            _ => Err(Error::Protocol("unsupported stateful control")),
        }
    }

    fn admit_epoch(&self, unit: &Unit, request: &w::ProxyRequest) -> Result<bool> {
        let mut state = unit.state.lock().unwrap();
        if stale(&state.request, request) || !state.valid.load(Ordering::Acquire) {
            return Ok(false);
        }
        let epoch = api_epoch(&request.unit.current);
        if epoch == Epoch::INVALID {
            return Err(Error::Protocol("invalid stateful current epoch"));
        }
        if state
            .cancellation_epoch
            .is_some_and(|cancelled| epoch <= cancelled)
        {
            return Ok(false);
        }
        if epoch > api_epoch(&state.request.unit.current) {
            advance(&state.full_revision)?;
            if epoch.primary() != api_epoch(&state.request.unit.current).primary() {
                advance(&state.primary_revision)?;
            }
            state.cancellation_epoch = None;
            let mut status = state.status.lock().unwrap();
            status.configuration_stage =
                if role(request.local.current_role)? == ReplicaRole::Secondary {
                    ConfigurationStage::PreWriteRevoke
                } else {
                    ConfigurationStage::CatchUp
                };
        }
        state.request.unit = request.unit.clone();
        state.request.local.current_role = request.local.current_role;
        state.request.local.previous_role = request.local.previous_role;
        if let Some(reporting) = &state.reporting {
            reporting.update(&state.request);
        }
        let mut status = state.status.lock().unwrap();
        status.current_epoch = epoch;
        status.previous_epoch = api_epoch(&request.unit.previous);
        status.current_role = role(request.local.current_role)?;
        Ok(true)
    }

    async fn update_epoch(
        &self,
        unit: &Unit,
        replicator: &Arc<dyn Replicator>,
        cancel: &Cancellation,
    ) -> Result<()> {
        let epoch = {
            let mut state = unit.state.lock().unwrap();
            let epoch = api_epoch(&state.request.unit.current).primary();
            if epoch <= state.last_primary_epoch {
                return Ok(());
            }
            if !state.valid.load(Ordering::Acquire)
                || state.status.lock().unwrap().lifecycle != ReplicaLifecycle::Open
                || state.role_changing
                || state.configuring
                || state.removing
                || state.remotes.values().any(|remote| remote.busy)
            {
                return Err(Error::Closed);
            }
            state.role_changing = true;
            state.error_api = (6, 4);
            state.replicator_operation = 8;
            epoch
        };
        let replicator = replicator.clone();
        let operation_cancel = cancel.clone();
        let result = self
            .complete(async move { replicator.update_epoch(epoch, operation_cancel).await })
            .await;
        let mut state = unit.state.lock().unwrap();
        state.role_changing = false;
        if !state.valid.load(Ordering::Acquire) || cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        if result.is_ok() {
            state.last_primary_epoch = epoch;
        }
        state.replicator_operation = 1;
        result
    }

    pub(super) async fn progress(
        &self,
        unit: &Unit,
        replicator: Arc<dyn Replicator>,
    ) -> Result<ReplicationProgress> {
        {
            let mut state = unit.state.lock().unwrap();
            if state.role_changing
                || state.configuring
                || state.remotes.values().any(|remote| remote.busy)
            {
                return Err(Error::Closed);
            }
            state.role_changing = true;
            state.error_api = (12, 4);
        }
        let progress = tokio::time::timeout(CALLBACK_TIMEOUT, {
            let guard = self.authority.callback_deadline(CALLBACK_TIMEOUT, false)?;
            tokio::task::spawn_blocking(move || {
                let result = replicator.progress();
                drop(guard);
                result
            })
        })
        .await;
        let progress = match progress {
            Ok(Ok(result)) => {
                unit.state.lock().unwrap().role_changing = false;
                result?
            }
            failed => {
                self.authority.retire();
                self.connection.abort();
                return Err(if failed.is_err() {
                    Error::Timeout
                } else {
                    Error::Protocol("replicator progress callback panicked")
                });
            }
        };
        progress.ensure_initialized()?;
        let mut state = unit.state.lock().unwrap();
        if !state.valid.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        state.status.lock().unwrap().progress = progress;
        state.progress_initialized = true;
        Ok(progress)
    }

    async fn reconfigure(&self, unit: &Arc<Unit>, request: w::ProxyRequest) -> Result<()> {
        // A membership update is compatible with an existing catch-up wait,
        // but cannot launch a second wait or run configuration callbacks twice.
        let pending = unit.state.lock().unwrap().catch_up_operation.is_some();
        if pending {
            if !self.admit_epoch(unit, &request)? {
                return Ok(());
            }
            let Ok(_configuration) = unit.configuration_gate.clone().try_acquire_owned() else {
                return Ok(());
            };
            let (replicator, cancel) = participants(unit)?;
            let revision = full_fence(unit);
            let result = self
                .configuration(unit, &replicator, &request, false, &cancel)
                .await;
            if cancel.is_cancelled() || matches!(result, Err(Error::Closed)) {
                return Ok(());
            }
            let mut response = request;
            response.flags = 0;
            return self
                .reply_guarded(
                    unit,
                    "UpdateConfigurationReply",
                    response,
                    result.err(),
                    ReplyControl {
                        api: Some((13, 4)),
                        revision: Some(revision),
                    },
                )
                .await;
        }
        let Ok(_gate) = unit.gate.clone().try_acquire_owned() else {
            return Ok(());
        };
        let same_epoch = {
            let state = unit.state.lock().unwrap();
            api_epoch(&state.request.unit.current) == api_epoch(&request.unit.current)
        };
        if !self.admit_epoch(unit, &request)? {
            return Ok(());
        }
        let requested = role(request.local.current_role)?;
        if !matches!(requested, ReplicaRole::Primary | ReplicaRole::Secondary) {
            return Ok(());
        }
        let (replicator, cancel) = participants(unit)?;
        let (old_service, old_replicator) = {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            (status.service_role, status.replicator_role)
        };
        let revision = full_fence(unit);
        let (data_loss_reported, late_catch_up, completed_catch_up) = {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            let primary = requested == ReplicaRole::Primary
                && old_service == requested
                && old_replicator == requested
                && request.flags & 1 == 0;
            (
                primary
                    && same_epoch
                    && request.flags & 2 != 0
                    && status.catch_up == CatchUpResult::DataLossReported,
                primary
                    && request.flags & 2 != 0
                    && status.configuration_stage == ConfigurationStage::Current,
                primary
                    && same_epoch
                    && request.flags & 2 != 0
                    && status.catch_up == CatchUpResult::Completed,
            )
        };
        if late_catch_up {
            return Ok(());
        }
        if data_loss_reported {
            return self
                .reply_guarded(
                    unit,
                    "UpdateConfigurationReply",
                    request,
                    Some(Error::Remote {
                        code: STATE_CHANGED,
                        message: "state changed on data loss".into(),
                    }),
                    ReplyControl {
                        api: Some((0, 0)),
                        revision: Some(revision),
                    },
                )
                .await;
        }
        if completed_catch_up {
            let (_, _, remotes) = project(&request)?;
            let unchanged = unit
                .state
                .lock()
                .unwrap()
                .status
                .lock()
                .unwrap()
                .configuration
                == remotes;
            let result = if unchanged {
                Ok(())
            } else {
                self.configuration(unit, &replicator, &request, false, &cancel)
                    .await
            };
            if matches!(result, Err(Error::Closed)) {
                return Ok(());
            }
            return self
                .reply_guarded(
                    unit,
                    "UpdateConfigurationReply",
                    request,
                    result.err(),
                    ReplyControl {
                        api: Some((13, 4)),
                        revision: Some(revision),
                    },
                )
                .await;
        }
        // Native API admission blocks actual replicator ChangeRole while Build
        // runs. Ordinary role transitions do not implicitly cancel those builds.
        if old_replicator != requested
            && unit.state.lock().unwrap().remotes.values().any(|r| r.busy)
        {
            return Ok(());
        }
        let mut failed_api = (0, 0);
        let result = async {
            if requested == ReplicaRole::Secondary && old_replicator == ReplicaRole::Primary {
                {
                    let state = unit.state.lock().unwrap();
                    let mut status = state.status.lock().unwrap();
                    status.catch_up = CatchUpResult::NotStarted;
                    // Only a newer native epoch can enter PreWriteRevoke.
                    // A retry must not undo an already completed revocation.
                }
                failed_api = (13, 4);
                self.configuration(unit, &replicator, &request, false, &cancel)
                    .await?;
                failed_api = (9, 4);
                self.catch_up(unit, &replicator, CatchUpMode::All, true, &cancel)
                    .await?;
                failed_api = (6, 4);
                self.update_epoch(unit, &replicator, &cancel).await?;
                failed_api = (13, 4);
                self.configuration(unit, &replicator, &request, false, &cancel)
                    .await?;
                failed_api = (9, 4);
                self.catch_up(unit, &replicator, CatchUpMode::All, false, &cancel)
                    .await?;
                let changed = self
                    .change_role(
                        unit,
                        requested,
                        api_epoch(&request.unit.current).primary(),
                        cancel.clone(),
                    )
                    .await;
                failed_api = role_failure_api(unit, requested);
                changed?;
                {
                    let state = unit.state.lock().unwrap();
                    state.status.lock().unwrap().configuration_stage = ConfigurationStage::Current;
                }
                failed_api = (12, 4);
                self.progress(unit, replicator.clone()).await?;
            } else if requested == ReplicaRole::Primary
                && (old_replicator != ReplicaRole::Primary || old_service != ReplicaRole::Primary)
            {
                {
                    let mut state = unit.state.lock().unwrap();
                    state.ready = false;
                    state.status.lock().unwrap().catch_up = CatchUpResult::NotStarted;
                }
                self.promote(unit, &replicator, &request, &cancel, &mut failed_api)
                    .await?;
                failed_api = (13, 4);
                self.configuration(unit, &replicator, &request, false, &cancel)
                    .await?;
                failed_api = (9, 4);
                self.catch_up(unit, &replicator, CatchUpMode::WriteQuorum, false, &cancel)
                    .await?;
            } else if requested == ReplicaRole::Secondary {
                let changed = self
                    .change_role(
                        unit,
                        requested,
                        api_epoch(&request.unit.current).primary(),
                        cancel.clone(),
                    )
                    .await;
                failed_api = role_failure_api(unit, requested);
                changed?;
                if request.flags & 2 != 0 {
                    failed_api = (12, 4);
                    self.progress(unit, replicator.clone()).await?;
                }
            } else if request.flags & 1 != 0 {
                failed_api = (14, 4);
                self.configuration(unit, &replicator, &request, true, &cancel)
                    .await?;
                let state = unit.state.lock().unwrap();
                let mut status = state.status.lock().unwrap();
                status.catch_up = CatchUpResult::Completed;
                status.configuration_stage = ConfigurationStage::Current;
            } else if request.flags & 2 != 0 {
                {
                    let state = unit.state.lock().unwrap();
                    let mut status = state.status.lock().unwrap();
                    status.catch_up = CatchUpResult::NotStarted;
                }
                failed_api = (6, 4);
                self.update_epoch(unit, &replicator, &cancel).await?;
                failed_api = (13, 4);
                self.configuration(unit, &replicator, &request, false, &cancel)
                    .await?;
                failed_api = (9, 4);
                self.catch_up(unit, &replicator, CatchUpMode::WriteQuorum, false, &cancel)
                    .await?;
            } else {
                let current = unit
                    .state
                    .lock()
                    .unwrap()
                    .status
                    .lock()
                    .unwrap()
                    .configuration_stage
                    == ConfigurationStage::Current;
                failed_api = (if current { 14 } else { 13 }, 4);
                self.configuration(unit, &replicator, &request, current, &cancel)
                    .await?;
            }
            Ok(())
        }
        .await;
        if matches!(result, Err(Error::Closed))
            || cancel.is_cancelled()
            || unit.state.lock().unwrap().cancellation_epoch.is_some()
        {
            return Ok(());
        }
        let control_error =
            matches!(&result, Err(Error::Remote { code, .. }) if *code == STATE_CHANGED);
        self.reply_guarded(
            unit,
            "UpdateConfigurationReply",
            request,
            result.err(),
            ReplyControl {
                api: Some(if control_error { (0, 0) } else { failed_api }),
                revision: Some(revision),
            },
        )
        .await
    }

    async fn promote(
        &self,
        unit: &Arc<Unit>,
        replicator: &Arc<dyn Replicator>,
        request: &w::ProxyRequest,
        cancel: &Cancellation,
        failed_api: &mut (i64, i64),
    ) -> Result<()> {
        let epoch = api_epoch(&request.unit.current).primary();
        let old = unit
            .state
            .lock()
            .unwrap()
            .status
            .lock()
            .unwrap()
            .replicator_role;
        if old != ReplicaRole::Primary {
            *failed_api = (8, 4);
            {
                let mut state = unit.state.lock().unwrap();
                state.error_api = (8, 4);
                state.replicator_operation = 4;
            }
            self.change_replicator_role(unit, ReplicaRole::Primary, epoch, cancel)
                .await?;
        }
        *failed_api = (6, 4);
        self.update_epoch(unit, replicator, cancel).await?;
        let service = unit
            .state
            .lock()
            .unwrap()
            .service
            .clone()
            .ok_or(Error::Closed)?;
        {
            let mut state = unit.state.lock().unwrap();
            state.error_api = (8, 1);
            state.operation = 4;
            state.operation_start = super::super::lifecycle::filetime();
        }
        *failed_api = (8, 1);
        let operation_cancel = cancel.clone();
        let changed = self
            .complete(async move {
                service
                    .change_role(ReplicaRole::Primary, operation_cancel)
                    .await
            })
            .await;
        unit.state.lock().unwrap().operation = 1;
        let address = changed?;
        if address.is_empty() {
            return Err(Error::InvalidInput(
                "Primary returned empty service endpoint",
            ));
        }
        {
            let state = unit.state.lock().unwrap();
            let mut status = state.status.lock().unwrap();
            status.service_role = ReplicaRole::Primary;
            status.service_address = address;
        }
        self.notify_endpoint(unit, request).await?;
        let previous = api_epoch(&request.unit.previous);
        if previous != Epoch::INVALID && previous.data_loss != request.unit.current.data_loss {
            *failed_api = (7, 4);
            if self.data_loss(unit, replicator, cancel).await? {
                {
                    let state = unit.state.lock().unwrap();
                    state.status.lock().unwrap().catch_up = CatchUpResult::DataLossReported;
                }
                self.progress(unit, replicator.clone()).await?;
                return Err(Error::Remote {
                    code: STATE_CHANGED,
                    message: "state changed on data loss".into(),
                });
            }
        }
        unit.state.lock().unwrap().ready = true;
        Ok(())
    }

    pub(super) async fn report_data_loss_fault(&self, unit: &Unit) -> Result<()> {
        let reporting = unit
            .state
            .lock()
            .unwrap()
            .reporting
            .clone()
            .ok_or(Error::Closed)?;
        reporting.fault(crate::FaultType::Transient).await?;
        Ok(())
    }

    pub(super) async fn data_loss(
        &self,
        unit: &Unit,
        replicator: &Arc<dyn Replicator>,
        cancel: &Cancellation,
    ) -> Result<bool> {
        {
            let mut state = unit.state.lock().unwrap();
            if state.role_changing
                || state.configuring
                || state.remotes.values().any(|remote| remote.busy)
            {
                return Err(Error::Closed);
            }
            state.role_changing = true;
            state.ready = false;
            state.error_api = (7, 4);
            state.replicator_operation = 64;
        }
        let replicator = replicator.clone();
        let operation_cancel = cancel.clone();
        let result = self
            .complete(async move { replicator.on_data_loss(operation_cancel).await })
            .await;
        {
            let mut state = unit.state.lock().unwrap();
            state.role_changing = false;
            state.replicator_operation = 1;
            if result.is_ok() && !cancel.is_cancelled() {
                state.ready = true;
            }
        }
        if result.is_err() && !cancel.is_cancelled() {
            self.report_data_loss_fault(unit).await?;
        }
        result
    }

    async fn configuration(
        &self,
        unit: &Unit,
        replicator: &Arc<dyn Replicator>,
        request: &w::ProxyRequest,
        current_only: bool,
        cancel: &Cancellation,
    ) -> Result<()> {
        let (current, previous, remotes) = project(request)?;
        {
            let state = unit.state.lock().unwrap();
            let status = state.status.lock().unwrap();
            if status.replicator_role != ReplicaRole::Primary {
                return Err(Error::Closed);
            }
            for incoming in &remotes {
                if let Some(old) = status
                    .configuration
                    .iter()
                    .find(|r| r.node_id == incoming.node_id)
                {
                    if incoming.incarnation < old.incarnation
                        || (incoming.incarnation == old.incarnation
                            && ((old.ready
                                && request.remote.0.iter().any(|r| {
                                    r.node.id.0.as_slice() == incoming.node_id
                                        && matches!(r.state, 0 | 1)
                                }))
                                || (!old.up && incoming.up)))
                    {
                        return Err(Error::Closed);
                    }
                }
            }
        }
        let revision = {
            let mut state = unit.state.lock().unwrap();
            if state.configuring
                || state.role_changing
                || !state.valid.load(Ordering::Acquire)
                || state.cancellation_epoch.is_some()
            {
                return Err(Error::Closed);
            }
            state.configuring = true;
            state.error_api = (if current_only { 14 } else { 13 }, 4);
            state.status.lock().unwrap().configuration = remotes;
            state.full_revision.load(Ordering::Acquire)
        };
        let replicator = replicator.clone();
        let operation_cancel = cancel.clone();
        let result = self
            .complete(async move {
                if current_only {
                    replicator
                        .update_current_configuration(current, operation_cancel)
                        .await
                } else {
                    replicator
                        .update_catch_up_configuration(current, previous, operation_cancel)
                        .await
                }
            })
            .await;
        let mut state = unit.state.lock().unwrap();
        state.configuring = false;
        if cancel.is_cancelled()
            || !state.valid.load(Ordering::Acquire)
            || state.full_revision.load(Ordering::Acquire) != revision
            || state
                .cancellation_epoch
                .is_some_and(|epoch| api_epoch(&request.unit.current) <= epoch)
        {
            return Err(Error::Closed);
        }
        result
    }

    async fn catch_up(
        &self,
        unit: &Unit,
        replicator: &Arc<dyn Replicator>,
        mode: CatchUpMode,
        pre_revoke: bool,
        lifetime: &Cancellation,
    ) -> Result<()> {
        let (cancel, sequence, revision) = {
            let mut state = unit.state.lock().unwrap();
            if state.cancellation_epoch.is_some() {
                return Err(Error::Closed);
            }
            state.sequence = state
                .sequence
                .checked_add(1)
                .ok_or(Error::ResourceLimit("stateful operations"))?;
            let sequence = state.sequence;
            let cancel = Cancellation::default();
            state.catch_up_operation = Some(Pending {
                cancel: cancel.clone(),
                sequence,
            });
            state.error_api = (9, 4);
            state.replicator_operation = 128;
            (
                cancel,
                sequence,
                state.full_revision.load(Ordering::Acquire),
            )
        };
        let replicator = replicator.clone();
        let operation_cancel = cancel.clone();
        let result = self
            .complete(async move { replicator.wait_for_catch_up(mode, operation_cancel).await })
            .await;
        let mut state = unit.state.lock().unwrap();
        if state
            .catch_up_operation
            .as_ref()
            .is_some_and(|pending| pending.sequence == sequence)
        {
            state.catch_up_operation = None;
        }
        state.replicator_operation = 1;
        let current = state.full_revision.load(Ordering::Acquire) == revision;
        if result.is_ok() && !cancel.is_cancelled() && !lifetime.is_cancelled() && current {
            let mut status = state.status.lock().unwrap();
            if pre_revoke {
                status.configuration_stage = ConfigurationStage::CatchUp;
            } else {
                status.catch_up = CatchUpResult::Completed;
            }
        }
        if cancel.is_cancelled() || lifetime.is_cancelled() || !current {
            Err(Error::Closed)
        } else {
            result
        }
    }

    async fn cancel_catch_up(&self, unit: &Arc<Unit>, request: w::ProxyRequest) -> Result<()> {
        let (pending, revision) = {
            let mut state = unit.state.lock().unwrap();
            if stale(&state.request, &request) || !state.valid.load(Ordering::Acquire) {
                return Ok(());
            }
            let epoch = api_epoch(&state.request.unit.current);
            if state.cancellation_epoch != Some(epoch) {
                advance(&state.full_revision)?;
                state.cancellation_epoch = Some(epoch);
            }
            let draining = if let Some(pending) = &state.catch_up_operation {
                pending.cancel.cancel();
                true
            } else {
                state.configuring
            };
            (
                draining,
                (
                    state.full_revision.clone(),
                    state.full_revision.load(Ordering::Acquire),
                ),
            )
        };
        if pending {
            return Ok(());
        }
        let Ok(_gate) = unit.gate.clone().try_acquire_owned() else {
            return Ok(());
        };
        let (service_role, replicator_role, requested, result) = {
            let state = unit.state.lock().unwrap();
            if stale(&state.request, &request)
                || !state.valid.load(Ordering::Acquire)
                || state.configuring
                || !Arc::ptr_eq(&state.full_revision, &revision.0)
                || state.full_revision.load(Ordering::Acquire) != revision.1
            {
                return Ok(());
            }
            let status = state.status.lock().unwrap();
            (
                status.service_role,
                status.replicator_role,
                status.current_role,
                status.catch_up,
            )
        };
        if result == CatchUpResult::Completed
            && requested == ReplicaRole::Secondary
            && (replicator_role != ReplicaRole::Secondary || service_role != ReplicaRole::Secondary)
        {
            let (_, cancel) = participants(unit)?;
            let changed = self
                .change_role(
                    unit,
                    ReplicaRole::Secondary,
                    api_epoch(&request.unit.current).primary(),
                    cancel,
                )
                .await;
            if matches!(changed, Err(Error::Closed)) {
                return Ok(());
            }
            changed?;
        } else if service_role != replicator_role {
            return Ok(());
        }
        let secondary = unit
            .state
            .lock()
            .unwrap()
            .status
            .lock()
            .unwrap()
            .service_role
            == ReplicaRole::Secondary;
        let error = secondary.then(|| Error::Remote {
            code: DEMOTE_COMPLETED,
            message: "demotion completed".into(),
        });
        self.reply_guarded(
            unit,
            "CancelCatchupReplicaSetReply",
            request,
            error,
            ReplyControl {
                api: Some((0, 0)),
                revision: Some(revision),
            },
        )
        .await
    }

    async fn build(&self, unit: &Arc<Unit>, request: w::ProxyRequest) -> Result<()> {
        let descriptor = request.remote.0[0].clone();
        let info = information(&descriptor, false)?;
        let (replicator, lifetime) = participants(unit)?;
        let revision = primary_fence(unit);
        let replacement = {
            let state = unit.state.lock().unwrap();
            state
                .remotes
                .get(&info.node_id)
                .filter(|old| {
                    descriptor.instance > old.descriptor.instance
                        && !old.busy
                        && old.phase != RemotePhase::Dropped
                })
                .map(|old| old.descriptor.clone())
        };
        if let Some(old) = replacement {
            let mut cleanup = request;
            cleanup.remote.0[0] = old;
            self.remove(unit, cleanup, false).await?;
            return Ok(());
        }
        let (cancel, sequence, cached) = {
            let mut state = unit.state.lock().unwrap();
            if stale(&state.request, &request) {
                return Ok(());
            }
            {
                let status = state.status.lock().unwrap();
                if !state.valid.load(Ordering::Acquire)
                    || status.lifecycle != ReplicaLifecycle::Open
                    || state.role_changing
                    || state.configuring
                    || !state.ready
                    || state.last_primary_epoch != api_epoch(&request.unit.current).primary()
                    || status.service_role != ReplicaRole::Primary
                    || status.replicator_role != ReplicaRole::Primary
                {
                    return Ok(());
                }
            }
            let key = info.node_id;
            if let Some(remote) = state.remotes.get(&key) {
                if remote.descriptor.instance > descriptor.instance
                    || remote.phase == RemotePhase::Dropped
                        && remote.descriptor.instance == descriptor.instance
                {
                    return Ok(());
                }
                if remote.busy {
                    if descriptor.instance > remote.descriptor.instance
                        && remote.phase == RemotePhase::Building
                    {
                        remote.cancel.cancel();
                    }
                    return Ok(());
                }
                if descriptor.instance > remote.descriptor.instance
                    && remote.phase == RemotePhase::Ready
                {
                    // Native removes the old custom-replicator resource before
                    // accepting a replacement; require explicit remove retry.
                    return Ok(());
                }
                if descriptor.instance == remote.descriptor.instance
                    && remote.phase == RemotePhase::Ready
                {
                    (Cancellation::default(), remote.sequence, true)
                } else {
                    state.sequence = state
                        .sequence
                        .checked_add(1)
                        .ok_or(Error::ResourceLimit("stateful operations"))?;
                    let sequence = state.sequence;
                    let cancel = Cancellation::default();
                    state.remotes.insert(
                        key,
                        Remote {
                            descriptor: descriptor.clone(),
                            phase: RemotePhase::Building,
                            busy: true,
                            cancel: cancel.clone(),
                            sequence,
                        },
                    );
                    (cancel, sequence, false)
                }
            } else {
                if state.remotes.len() >= MAX_REMOTE_TOMBSTONES {
                    return Err(Error::ResourceLimit("remote tombstones"));
                }
                state.sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit("stateful operations"))?;
                let sequence = state.sequence;
                let cancel = Cancellation::default();
                state.remotes.insert(
                    key,
                    Remote {
                        descriptor: descriptor.clone(),
                        phase: RemotePhase::Building,
                        busy: true,
                        cancel: cancel.clone(),
                        sequence,
                    },
                );
                (cancel, sequence, false)
            }
        };
        if cached {
            return self
                .reply_guarded(
                    unit,
                    "ReplicatorBuildIdleReplicaReply",
                    request,
                    None,
                    ReplyControl {
                        api: Some((0, 0)),
                        revision: Some(revision),
                    },
                )
                .await;
        }
        let operation_cancel = cancel.clone();
        let operation_info = info.clone();
        let result = self
            .complete(async move {
                replicator
                    .build_replica(operation_info, operation_cancel)
                    .await
            })
            .await;
        {
            let mut state = unit.state.lock().unwrap();
            if let Some(remote) = state.remotes.get_mut(&info.node_id) {
                if remote.sequence == sequence {
                    remote.busy = false;
                    remote.phase = if result.is_ok()
                        && !cancel.is_cancelled()
                        && !lifetime.is_cancelled()
                        && revision.0.load(Ordering::Acquire) == revision.1
                    {
                        RemotePhase::Ready
                    } else {
                        RemotePhase::Failed
                    };
                }
            }
        }
        if cancel.is_cancelled() || lifetime.is_cancelled() {
            return Ok(());
        }
        self.reply_guarded(
            unit,
            "ReplicatorBuildIdleReplicaReply",
            request,
            result.err(),
            ReplyControl {
                api: Some((0, 0)),
                revision: Some(revision),
            },
        )
        .await
    }

    async fn remove(
        &self,
        unit: &Arc<Unit>,
        request: w::ProxyRequest,
        emit_reply: bool,
    ) -> Result<()> {
        let descriptor = request.remote.0[0].clone();
        let info = information(&descriptor, false)?;
        let (replicator, lifetime) = participants(unit)?;
        let revision = primary_fence(unit);
        let (sequence, cancel, absent) = {
            let mut state = unit.state.lock().unwrap();
            if stale(&state.request, &request) {
                return Ok(());
            }
            if !state.valid.load(Ordering::Acquire)
                || state.status.lock().unwrap().lifecycle != ReplicaLifecycle::Open
                || state.role_changing
                || state.configuring
                || state.removing
            {
                return Ok(());
            }
            if let Some(remote) = state.remotes.get(&info.node_id) {
                if remote.descriptor.instance > descriptor.instance {
                    return Ok(());
                }
                if remote.busy {
                    if remote.phase == RemotePhase::Building {
                        remote.cancel.cancel();
                    }
                    return Ok(());
                }
                if remote.phase == RemotePhase::Dropped
                    && remote.descriptor.instance == descriptor.instance
                {
                    (remote.sequence, Cancellation::default(), true)
                } else {
                    state.sequence = state
                        .sequence
                        .checked_add(1)
                        .ok_or(Error::ResourceLimit("stateful operations"))?;
                    let sequence = state.sequence;
                    let cancel = Cancellation::default();
                    state.remotes.insert(
                        info.node_id,
                        Remote {
                            descriptor: descriptor.clone(),
                            phase: RemotePhase::Failed,
                            busy: true,
                            cancel: cancel.clone(),
                            sequence,
                        },
                    );
                    state.removing = true;
                    (sequence, cancel, false)
                }
            } else {
                if state.remotes.len() >= MAX_REMOTE_TOMBSTONES {
                    return Err(Error::ResourceLimit("remote tombstones"));
                }
                state.sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or(Error::ResourceLimit("stateful operations"))?;
                let sequence = state.sequence;
                state.remotes.insert(
                    info.node_id,
                    Remote {
                        descriptor: descriptor.clone(),
                        phase: RemotePhase::Dropped,
                        busy: false,
                        cancel: Cancellation::default(),
                        sequence,
                    },
                );
                (sequence, Cancellation::default(), true)
            }
        };
        if absent {
            if !emit_reply {
                return Ok(());
            }
            return self
                .reply_guarded(
                    unit,
                    "ReplicatorRemoveIdleReplicaReply",
                    request,
                    None,
                    ReplyControl {
                        api: Some((0, 0)),
                        revision: Some(revision),
                    },
                )
                .await;
        }
        unit.state.lock().unwrap().error_api = (11, 4);
        let operation_cancel = cancel.clone();
        let operation_info = info.clone();
        let result = self
            .complete(async move {
                replicator
                    .remove_replica(operation_info, operation_cancel)
                    .await
            })
            .await;
        {
            let mut state = unit.state.lock().unwrap();
            state.removing = false;
            if let Some(remote) = state.remotes.get_mut(&info.node_id) {
                if remote.sequence == sequence {
                    remote.busy = false;
                    remote.phase = if result.is_ok() {
                        RemotePhase::Dropped
                    } else {
                        RemotePhase::Failed
                    };
                }
            }
        }
        if cancel.is_cancelled() || lifetime.is_cancelled() {
            return Ok(());
        }
        if !emit_reply {
            return Ok(());
        }
        self.reply_guarded(
            unit,
            "ReplicatorRemoveIdleReplicaReply",
            request,
            result.err(),
            ReplyControl {
                api: Some((11, 4)),
                revision: Some(revision),
            },
        )
        .await
    }

    async fn notify_endpoint(&self, unit: &Arc<Unit>, request: &w::ProxyRequest) -> Result<()> {
        let completed = {
            let mut retries = self.retries.lock().unwrap();
            let mut completed = Vec::new();
            let mut index = 0;
            while index < retries.len() {
                if retries[index].is_finished() {
                    completed.push(retries.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            completed
        };
        for task in completed {
            if task.await.is_err() {
                self.authority.retire();
                self.connection.abort();
                return Err(Error::Protocol("endpoint retry worker panicked"));
            }
        }
        let (body, cancel, valid) = {
            let mut state = unit.state.lock().unwrap();
            if let Some(old) = state.endpoint_pending.take() {
                old.cancel();
            }
            let cancel = Cancellation::default();
            state.endpoint_pending = Some(cancel.clone());
            let status = state.status.lock().unwrap();
            let mut local = request.local.clone();
            local.service_endpoint = status.service_address.clone();
            local.replication_endpoint = status.replication_address.clone();
            let body = to_vec(&w::ReplicaMessage {
                unit: request.unit.clone(),
                local,
                service: state.request.service.clone(),
            })?;
            (body, cancel, state.valid.clone())
        };
        let connection = self.connection.clone();
        let authority = self.authority.clone();
        let task = tokio::spawn(async move {
            loop {
                let fence_authority = Arc::downgrade(&authority);
                let fence_valid = valid.clone();
                let fence_cancel = cancel.clone();
                let sent = connection
                    .send_one_way_fenced(
                        12,
                        "ProxyReplicaEndpointUpdated",
                        body.clone(),
                        match deadline_after(Duration::from_secs(5)) {
                            Ok(d) => d,
                            Err(_) => {
                                authority.retire();
                                return;
                            }
                        },
                        SendStatus::fenced(Arc::new(move || {
                            fence_authority.upgrade().is_some_and(|a| a.check().is_ok())
                                && fence_valid.load(Ordering::Acquire)
                                && !fence_cancel.is_cancelled()
                        })),
                    )
                    .await;
                if cancel.is_cancelled() || !valid.load(Ordering::Acquire) {
                    return;
                }
                if let Err(error) = sent {
                    authority.retire();
                    connection.abort_with_error(error);
                    return;
                }
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        });
        let mut retries = self.retries.lock().unwrap();
        if retries.len() >= crate::MAX_SERVICE_UNITS * 2 {
            task.abort();
            return Err(Error::ResourceLimit("endpoint retry owners"));
        }
        retries.push(task);
        Ok(())
    }
}

fn participants(unit: &Unit) -> Result<(Arc<dyn Replicator>, Cancellation)> {
    let state = unit.state.lock().unwrap();
    if !state.valid.load(Ordering::Acquire) || !state.status.lock().unwrap().replicator_open {
        return Err(Error::Closed);
    }
    Ok((
        state.replicator.clone().ok_or(Error::Closed)?,
        state.cancel.clone(),
    ))
}
pub(super) fn advance(revision: &AtomicU64) -> Result<()> {
    revision
        .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(1)
        })
        .map(|_| ())
        .map_err(|_| Error::ResourceLimit("stateful revision"))
}
fn full_fence(unit: &Unit) -> (Arc<AtomicU64>, u64) {
    let state = unit.state.lock().unwrap();
    (
        state.full_revision.clone(),
        state.full_revision.load(Ordering::Acquire),
    )
}
fn primary_fence(unit: &Unit) -> (Arc<AtomicU64>, u64) {
    let state = unit.state.lock().unwrap();
    (
        state.primary_revision.clone(),
        state.primary_revision.load(Ordering::Acquire),
    )
}
fn role_failure_api(unit: &Unit, requested: ReplicaRole) -> (i64, i64) {
    let state = unit.state.lock().unwrap();
    let role = state.status.lock().unwrap().replicator_role;
    (8, if role == requested { 1 } else { 4 })
}
fn information(replica: &w::Replica, must_catch_up: bool) -> Result<ReplicaInformation> {
    if replica.id <= 0
        || replica.instance <= 0
        || replica.node.id.0.len() != 16
        || !(0..=3).contains(&replica.state)
    {
        return Err(Error::Protocol("invalid remote replica descriptor"));
    }
    Ok(ReplicaInformation {
        node_id: replica
            .node
            .id
            .0
            .as_slice()
            .try_into()
            .map_err(|_| Error::Protocol("invalid node ID"))?,
        node_instance: replica.node.instance,
        replica_id: replica.id,
        incarnation: replica.instance,
        role: role(replica.current_role)?,
        previous_role: role(replica.previous_role)?,
        up: replica.up,
        ready: replica.state == 2,
        address: replica.replication_endpoint.clone(),
        progress: ReplicationProgress {
            first: replica.first_lsn,
            last: replica.last_lsn,
        },
        must_catch_up,
    })
}
fn project(
    request: &w::ProxyRequest,
) -> Result<(
    ReplicaSetConfiguration,
    ReplicaSetConfiguration,
    Vec<ReplicaInformation>,
)> {
    let mut cc = vec![];
    let mut pc = vec![];
    let (mut ncc, mut ndcc, mut npc, mut ndpc) = (0, 0, 0, 0);
    let mut remotes = vec![];
    let mut ids = std::collections::HashSet::new();
    let mut nodes = std::collections::HashSet::new();
    let mut local_seen = false;
    let (mut local_cc, mut local_pc) = (false, false);
    for replica in &request.remote.0 {
        let mut info = information(replica, replica.current_role == 4)?;
        if !ids.insert(replica.id) || !nodes.insert(info.node_id) {
            return Err(Error::Protocol("duplicate configuration identity/node"));
        }
        if replica.current_role >= 3 {
            ncc += 1;
            ndcc += usize::from(replica.state != 3);
        }
        if replica.previous_role >= 3 {
            npc += 1;
            ndpc += usize::from(replica.state != 3);
        }
        if replica.id == request.local.id {
            if replica.instance != request.local.instance
                || replica.node.id.0 != request.local.node.id.0
                || replica.node.instance != request.local.node.instance
                || replica.current_role != request.local.current_role
            {
                return Err(Error::Protocol("configuration changed local owner"));
            }
            local_seen = true;
            local_cc = replica.current_role >= 3 && replica.state != 3;
            local_pc = replica.previous_role >= 3 && replica.state != 3;
            continue;
        }
        remotes.push(info.clone());
        if !replica.up || replica.state != 2 {
            continue;
        }
        info.role = ReplicaRole::Secondary;
        if replica.current_role >= 3 {
            cc.push(info.clone());
        }
        if replica.previous_role >= 3 {
            let primary_change = api_epoch(&request.unit.current).primary()
                != api_epoch(&request.unit.previous).primary();
            if primary_change && replica.current_role < 3 && replica.last_lsn < 0 {
                continue;
            }
            info.must_catch_up = false;
            pc.push(info);
        }
    }
    if !local_seen {
        return Err(Error::Protocol("configuration omitted local member"));
    }
    Ok((
        ReplicaSetConfiguration {
            epoch: api_epoch(&request.unit.current).primary(),
            replicas: cc,
            write_quorum: (ncc / 2 + 1).min(ndcc),
            local_member: local_cc,
        },
        ReplicaSetConfiguration {
            epoch: api_epoch(&request.unit.previous).primary(),
            replicas: pc,
            write_quorum: (npc / 2 + 1).min(ndpc),
            local_member: local_pc,
        },
        remotes,
    ))
}
