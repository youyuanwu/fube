//! Permanently empty application state, with real bounded peer-control checks.
//! This is not a storage engine or the native FabricReplicator wire protocol.
use super::*;
use fube_runtime::extensions::{
    CallLane, CallOrigin, NativeControlBinding, NativeInvocation, NativeInvocationKind,
};
use std::{collections::BTreeMap, net::SocketAddr, sync::Weak, time::Instant};

#[path = "stateful_binding.rs"]
mod binding;
#[cfg(test)]
#[path = "stateful_tests.rs"]
mod tests;
const PEER_LIMIT: usize = 256;
const PEER_TIMEOUT: Duration = Duration::from_secs(1);
const EMPTY: ReplicationProgress = ReplicationProgress::EMPTY;
type PeerKey = ([u8; 16], u64, i64, i64);
fn key(peer: &ReplicaInformation) -> PeerKey {
    (
        peer.node_id,
        peer.node_instance,
        peer.replica_id,
        peer.incarnation,
    )
}
fn failure(message: &'static str) -> RuntimeError {
    RuntimeError::Remote {
        code: 0x80004005u32 as i32 as i64,
        message: message.into(),
    }
}

trait View: Send + Sync {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus>;
}
impl View for NativeRuntimeIntegration {
    fn snapshot(&self) -> RuntimeResult<ReplicaStatus> {
        self.status()
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Created,
    Open,
    Closed,
}
struct EmptyState {
    phase: Phase,
    role: ReplicaRole,
    epoch: Option<Epoch>,
    current: Option<ReplicaSetConfiguration>,
    previous: Option<ReplicaSetConfiguration>,
    revision: u64,
    address: String,
    built: BTreeMap<PeerKey, ()>,
    removed: BTreeMap<PeerKey, ReplicaRemoval>,
    build_tokens: BTreeMap<PeerKey, u64>,
}
pub(super) struct EmptyReplicator {
    me: Weak<Self>,
    view: Arc<dyn View>,
    identity: ReplicaIdentity,
    peer_path: String,
    peers: Arc<PeerServer>,
    state: Mutex<EmptyState>,
    stopped: Cancellation,
    changed: tokio::sync::Notify,
    active: AtomicUsize,
    incoming: AtomicUsize,
    probes: AtomicUsize,
    wait_millis: std::sync::atomic::AtomicU64,
    native: Mutex<Option<NativeRuntimeIntegration>>,
    native_actor: Mutex<Option<uuid::Uuid>>,
    binder: Mutex<Option<Arc<dyn NativeControlBinding>>>,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl EmptyReplicator {
    fn new(
        view: Arc<dyn View>,
        identity: ReplicaIdentity,
        path: String,
        peers: Arc<PeerServer>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            view,
            identity,
            peer_path: format!("/empty{path}"),
            peers,
            state: Mutex::new(EmptyState {
                phase: Phase::Created,
                role: ReplicaRole::Unknown,
                epoch: None,
                current: None,
                previous: None,
                revision: 0,
                address: String::new(),
                built: BTreeMap::new(),
                removed: BTreeMap::new(),
                build_tokens: BTreeMap::new(),
            }),
            stopped: Cancellation::default(),
            changed: tokio::sync::Notify::new(),
            active: AtomicUsize::new(0),
            incoming: AtomicUsize::new(0),
            probes: AtomicUsize::new(0),
            wait_millis: std::sync::atomic::AtomicU64::new(8_000),
            native: Mutex::new(None),
            native_actor: Mutex::new(None),
            binder: Mutex::new(None),
        })
    }
    fn operation(&self, primary: bool) -> RuntimeResult<Active<'_>> {
        self.view.snapshot()?;
        let state = self.state.lock().unwrap();
        if state.phase != Phase::Open
            || self.stopped.is_cancelled()
            || (primary && state.role != ReplicaRole::Primary)
        {
            return Err(RuntimeError::Closed);
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Active(&self.active))
    }
    fn validate_config(
        &self,
        configuration: &ReplicaSetConfiguration,
        current: bool,
    ) -> RuntimeResult<()> {
        if configuration.replicas.len() >= MAX_REPLICA_SET_SIZE
            || (current && (!configuration.local_member || configuration.write_quorum == 0))
        {
            return Err(failure(
                "invalid empty-state configuration/local membership",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut nodes = std::collections::BTreeSet::new();
        for peer in &configuration.replicas {
            if peer.replica_id == self.identity.replica_id
                || peer.node_id == self.identity.node_id
                || !peer.up
                || !peer.ready
                || peer.role != ReplicaRole::ActiveSecondary
                || !ids.insert(peer.replica_id)
                || !nodes.insert(peer.node_id)
                || peer.address.is_empty()
            {
                return Err(failure("invalid empty-state remote configuration"));
            }
            let available = configuration.replicas.len() + usize::from(configuration.local_member);
            if available > 0 && configuration.write_quorum < available / 2 + 1 {
                return Err(failure(
                    "configuration quorum is below available membership majority",
                ));
            }
        }
        Ok(())
    }
    fn config_epoch(&self, current: &ReplicaSetConfiguration) -> RuntimeResult<()> {
        let native = self.view.snapshot()?;
        let state = self.state.lock().unwrap();
        if current.epoch != native.current_epoch.primary() {
            return Err(failure("configuration is not the native epoch"));
        }
        let pre_revoke = native.current_role == ReplicaRole::ActiveSecondary
            && native.configuration_stage == ConfigurationStage::PreWriteRevoke;
        if state.epoch != Some(current.epoch) && !pre_revoke {
            return Err(failure("configuration precedes replicator epoch update"));
        }
        Ok(())
    }
    fn retire(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.phase = Phase::Closed;
            state.role = ReplicaRole::None;
            state.current = None;
            state.previous = None;
            state.built.clear();
            state.removed.clear();
            state.build_tokens.clear();
        }
        self.stopped.cancel();
        self.changed.notify_waiters();
        self.peers.routes.lock().unwrap().remove(&self.peer_path);
    }
    fn evidence(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        let config = |value: &Option<ReplicaSetConfiguration>| {
            value.as_ref().map(|value|
            serde_json::json!({"quorum":value.write_quorum,"local_member":value.local_member,
                "members":value.replicas.iter().map(|r| serde_json::json!({
                    "replica":r.replica_id,"incarnation":r.incarnation,"must_catch_up":r.must_catch_up
                })).collect::<Vec<_>>()}))
        };
        serde_json::json!({"state":"empty","frontier":0,"phase":format!("{:?}",state.phase),
            "role":format!("{:?}",state.role),"epoch":state.epoch.map(epoch_json),
            "current":config(&state.current),"previous":config(&state.previous),
            "built":state.built.len(),"removed":state.removed.len(),"peer_probes":self.probes.load(Ordering::Acquire),
            "replication_address":state.address,"application_writes":false,"durability":false})
    }
    fn peer_body(&self) -> RuntimeResult<String> {
        let native = self.view.snapshot()?;
        let state = self.state.lock().unwrap();
        if native.lifecycle != ReplicaLifecycle::Open
            || !native.replicator_open
            || state.phase != Phase::Open
            || state.epoch != Some(native.current_epoch.primary())
            || native.replicator_role != state.role
            || native.service_role != state.role
            || !matches!(
                state.role,
                ReplicaRole::IdleSecondary | ReplicaRole::ActiveSecondary | ReplicaRole::Primary
            )
        {
            return Err(RuntimeError::Closed);
        }
        Ok(serde_json::json!({"version":1,"state":"empty","first":0,"last":0,
            "partition":self.identity.partition_id.to_string(),"replica":self.identity.replica_id,
            "incarnation":self.identity.incarnation,"node":self.identity.node_id,"node_instance":self.identity.node_instance,
            "role":format!("{:?}",state.role),"epoch":epoch_json(state.epoch.ok_or(RuntimeError::Closed)?)
        }).to_string())
    }
    async fn probe(
        &self,
        peer: &ReplicaInformation,
        epoch: Epoch,
        previous: Option<Epoch>,
        build: bool,
        cancellation: &Cancellation,
    ) -> RuntimeResult<()> {
        self.probes.fetch_add(1, Ordering::AcqRel);
        let network = async {
            let value = http_peer(&peer.address).await?;
            let observed = Epoch {
                data_loss_number: value["epoch"]["data_loss_number"]
                    .as_i64()
                    .ok_or(failure("missing peer epoch"))?,
                configuration_number: value["epoch"]["configuration_number"]
                    .as_i64()
                    .ok_or(failure("missing peer epoch"))?,
            };
            let role = value["role"].as_str().ok_or(failure("missing peer role"))?;
            let role_ok = if build {
                role == "IdleSecondary"
            } else {
                matches!(role, "IdleSecondary" | "ActiveSecondary")
                    || (role == "Primary"
                        && (peer.must_catch_up || peer.previous_role == ReplicaRole::Primary))
            };
            if value["version"] != 1
                || value["state"] != "empty"
                || value["partition"] != self.identity.partition_id.to_string()
                || value["replica"].as_i64() != Some(peer.replica_id)
                || value["incarnation"].as_i64() != Some(peer.incarnation)
                || value["node_instance"].as_u64() != Some(peer.node_instance)
                || value["node"] != serde_json::json!(peer.node_id)
                || value["first"].as_i64() != Some(0)
                || value["last"].as_i64() != Some(0)
                || !role_ok
                || (observed != epoch && Some(observed) != previous)
            {
                return Err(failure("peer identity/role/epoch/empty frontier mismatch"));
            }
            Ok(())
        };
        tokio::select! {
            result = tokio::time::timeout(PEER_TIMEOUT, network) =>
                result.map_err(|_| failure("peer control deadline"))?,
            _ = cancellation.cancelled() => Err(RuntimeError::Closed),
            _ = self.stopped.cancelled() => Err(RuntimeError::Closed),
        }
    }
}
impl EmptyReplicator {
    fn open(&self, cancellation: Cancellation) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            self.view.snapshot()?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            let address = self.peers.listen().await?;
            let mut state = self.state.lock().unwrap();
            if state.phase != Phase::Created {
                return Err(failure("replicator Open is not a new lifetime"));
            }
            let mut routes = self.peers.routes.lock().unwrap();
            if routes.len() >= MAX_SERVICE_UNITS || routes.contains_key(&self.peer_path) {
                return Err(failure("peer route ownership bound/collision"));
            }
            routes.insert(self.peer_path.clone(), self.me.clone());
            state.phase = Phase::Open;
            state.address = format!("http://{address}{}", self.peer_path);
            Ok(state.address.clone())
        })
    }
    fn change_role(
        &self,
        epoch: Epoch,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            if role != ReplicaRole::None {
                let native = self.view.snapshot()?;
                if native.current_role != role || native.current_epoch.primary() != epoch {
                    return Err(failure("role/epoch was not selected by native RAP"));
                }
            }
            let mut state = self.state.lock().unwrap();
            if state.phase != Phase::Open
                || state.epoch.is_some_and(|old| epoch < old)
                || role == ReplicaRole::Unknown
            {
                return Err(failure("stale/closed role transition"));
            }
            if state.epoch != Some(epoch) || role != ReplicaRole::Primary {
                state.built.clear();
                state.current = None;
                state.previous = None;
            }
            state.role = role;
            state.epoch = Some(epoch);
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or(failure("empty control revision exhausted"))?;
            self.changed.notify_waiters();
            Ok(())
        })
    }
    fn update_epoch(&self, epoch: Epoch, cancellation: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let _active = self.operation(false)?;
            if cancellation.is_cancelled() || self.view.snapshot()?.current_epoch.primary() != epoch
            {
                return Err(failure("epoch was not selected by native RAP"));
            }
            let mut state = self.state.lock().unwrap();
            if state.phase != Phase::Open || state.epoch.is_some_and(|old| epoch < old) {
                return Err(failure("stale epoch"));
            }
            if state.epoch != Some(epoch) {
                state.built.clear();
                state.current = None;
                state.previous = None;
            }
            state.epoch = Some(epoch);
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or(failure("empty control revision exhausted"))?;
            self.changed.notify_waiters();
            Ok(())
        })
    }
    fn progress(&self) -> RuntimeResult<ReplicationProgress> {
        let _active = self.operation(false)?;
        Ok(EMPTY)
    }
    fn on_data_loss(&self, cancellation: Cancellation) -> ServiceFuture<'_, bool> {
        Box::pin(async move {
            let _active = self.operation(true)?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            // Exactly one possible application state: nothing can have changed.
            Ok(false)
        })
    }
    fn update_catch_up_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let _active = self.operation(true)?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            self.validate_config(&current, true)?;
            self.validate_config(&previous, false)?;
            self.config_epoch(&current)?;
            if previous.epoch != self.view.snapshot()?.previous_epoch.primary() {
                return Err(failure("previous configuration is not the native epoch"));
            }
            {
                let mut state = self.state.lock().unwrap();
                if state.phase != Phase::Open || state.role != ReplicaRole::Primary {
                    return Err(RuntimeError::Closed);
                }
                state.current = Some(current);
                state.previous = Some(previous);
                state.revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(failure("empty control revision exhausted"))?;
            }
            self.changed.notify_waiters();
            if self.view.snapshot()?.catch_up == CatchUpResult::Completed {
                self.wait_for_catch_up(CatchUpMode::Write, cancellation)
                    .await?;
            }
            Ok(())
        })
    }
    fn update_current_configuration(
        &self,
        current: ReplicaSetConfiguration,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let _active = self.operation(true)?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            self.validate_config(&current, true)?;
            self.config_epoch(&current)?;
            // End-reconfiguration does not claim data replication; previously
            // completed empty catch-up and native membership remain separate.
            {
                let mut state = self.state.lock().unwrap();
                if state.phase != Phase::Open || state.role != ReplicaRole::Primary {
                    return Err(RuntimeError::Closed);
                }
                state.current = Some(current);
                state.previous = None;
                state.revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(failure("empty control revision exhausted"))?;
            }
            self.changed.notify_waiters();
            self.wait_for_catch_up(CatchUpMode::Write, cancellation)
                .await
        })
    }
    fn wait_for_catch_up(
        &self,
        mode: CatchUpMode,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            let _active = self.operation(true)?;
            let deadline =
                Instant::now() + Duration::from_millis(self.wait_millis.load(Ordering::Acquire));
            loop {
                if cancellation.is_cancelled() || self.stopped.is_cancelled() {
                    return Err(RuntimeError::Closed);
                }
                let native = self.view.snapshot()?;
                let (current, previous, revision) = {
                    let state = self.state.lock().unwrap();
                    (
                        state
                            .current
                            .clone()
                            .ok_or(failure("catch-up has no current configuration"))?,
                        state.previous.clone(),
                        state.revision,
                    )
                };
                // Native demotes before promoting the target. Both ALL checks
                // surround our UpdateEpoch; neither advances peers' epochs.
                // An actual peer in the native previous epoch therefore proves
                // empty readiness during this transition only, not primary
                // quorum readiness or application replication.
                let old_epoch = if mode == CatchUpMode::All
                    && native.current_role == ReplicaRole::ActiveSecondary
                    && native.replicator_role == ReplicaRole::Primary
                    && matches!(
                        native.configuration_stage,
                        ConfigurationStage::PreWriteRevoke | ConfigurationStage::CatchUp
                    ) {
                    previous.as_ref().map(|old| old.epoch)
                } else {
                    None
                };
                let mut receipts = BTreeMap::new();
                for peer in current
                    .replicas
                    .iter()
                    .chain(previous.iter().flat_map(|p| &p.replicas))
                {
                    if receipts.contains_key(&key(peer)) {
                        continue;
                    }
                    if self.state.lock().unwrap().removed.contains_key(&key(peer)) {
                        receipts.insert(key(peer), false);
                        continue;
                    }
                    let verified = self
                        .probe(peer, current.epoch, old_epoch, false, &cancellation)
                        .await
                        .is_ok();
                    receipts.insert(key(peer), verified);
                }
                let satisfied = |config: &ReplicaSetConfiguration| {
                    let ready = config
                        .replicas
                        .iter()
                        .filter(|peer| receipts.get(&key(peer)) == Some(&true))
                        .count();
                    ready + usize::from(config.local_member) >= config.write_quorum
                        && config.replicas.iter().all(|peer| {
                            (!peer.must_catch_up && mode != CatchUpMode::All)
                                || receipts.get(&key(peer)) == Some(&true)
                        })
                };
                if satisfied(&current) && previous.as_ref().is_none_or(satisfied) {
                    let ready = {
                        let state = self.state.lock().unwrap();
                        state.phase == Phase::Open
                            && state.role == ReplicaRole::Primary
                            && state.revision == revision
                            && !cancellation.is_cancelled()
                            && !self.stopped.is_cancelled()
                            && receipts.iter().all(|(peer, verified)| {
                                !*verified || !state.removed.contains_key(peer)
                            })
                    };
                    if ready {
                        if self.view.snapshot()?.current_epoch.primary() != current.epoch {
                            return Err(failure("catch-up configuration was superseded"));
                        }
                        return Ok(());
                    }
                }
                if Instant::now() >= deadline {
                    return Err(failure("empty-state quorum/target readiness not proven"));
                }
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(RuntimeError::Closed),
                    _ = self.stopped.cancelled() => return Err(RuntimeError::Closed),
                    _ = self.changed.notified() => {},
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {},
                }
            }
        })
    }
    #[cfg(test)]
    fn build_replica(
        &self,
        peer: ReplicaInformation,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        self.build_frozen(peer, cancellation, None)
    }
    fn build_frozen(
        &self,
        peer: ReplicaInformation,
        cancellation: Cancellation,
        proof: Option<NativeInvocation>,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if let Some(proof) = &proof {
                proof.validate()?;
            }
            let _active = self.operation(true)?;
            if peer.role != ReplicaRole::IdleSecondary
                || peer.replica_id == self.identity.replica_id
                || peer.node_id == self.identity.node_id
            {
                return Err(failure("invalid idle build target"));
            }
            let (epoch, token) = {
                let mut state = self.state.lock().unwrap();
                if state.removed.get(&key(&peer)) == Some(&ReplicaRemoval::Idle) {
                    return Err(failure("removed incarnation cannot be rebuilt"));
                }
                let epoch = state.epoch.ok_or(failure("uninitialized epoch"))?;
                if state.build_tokens.len() >= PEER_LIMIT
                    && !state.build_tokens.contains_key(&key(&peer))
                {
                    return Err(failure("empty build token bound"));
                }
                state.revision = state
                    .revision
                    .checked_add(1)
                    .ok_or(failure("empty control revision exhausted"))?;
                let token = state.revision;
                state.build_tokens.insert(key(&peer), token);
                state.removed.remove(&key(&peer));
                (epoch, token)
            };
            self.probe(&peer, epoch, None, true, &cancellation).await?;
            if let Some(proof) = &proof {
                proof.validate()?;
            }
            self.view.snapshot()?;
            let mut state = self.state.lock().unwrap();
            if state.phase != Phase::Open
                || state.epoch != Some(epoch)
                || state.role != ReplicaRole::Primary
                || state.removed.contains_key(&key(&peer))
                || state.build_tokens.get(&key(&peer)) != Some(&token)
                || cancellation.is_cancelled()
            {
                return Err(RuntimeError::Closed);
            }
            if state.built.len() >= PEER_LIMIT && !state.built.contains_key(&key(&peer)) {
                return Err(failure("empty peer bound"));
            }
            state.built.insert(key(&peer), ());
            Ok(())
        })
    }
    #[cfg(test)]
    fn remove_replica(
        &self,
        peer: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, ()> {
        Box::pin(async move { self.remove_frozen(peer, reason, cancellation) })
    }
    fn remove_frozen(
        &self,
        peer: ReplicaInformation,
        reason: ReplicaRemoval,
        cancellation: Cancellation,
    ) -> RuntimeResult<()> {
        let _active = self.operation(false)?;
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Closed);
        }
        let mut state = self.state.lock().unwrap();
        if state.phase != Phase::Open {
            return Err(RuntimeError::Closed);
        }
        if state.removed.len() >= PEER_LIMIT && !state.removed.contains_key(&key(&peer)) {
            return Err(failure("empty removal tombstone bound"));
        }
        let was_built = state.built.remove(&key(&peer)).is_some();
        state.build_tokens.remove(&key(&peer));
        let newly_removed = state.removed.insert(key(&peer), reason) != Some(reason);
        if was_built || newly_removed {
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or(failure("empty control revision exhausted"))?;
        }
        self.changed.notify_waiters();
        Ok(())
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            self.retire();
            while self.active.load(Ordering::SeqCst) != 0
                || self.incoming.load(Ordering::SeqCst) != 0
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(())
        })
    }
    fn abort(&self) {
        self.retire();
        let deadline = Instant::now() + Duration::from_millis(150);
        while self.active.load(Ordering::SeqCst) != 0 || self.incoming.load(Ordering::SeqCst) != 0 {
            assert!(
                Instant::now() < deadline,
                "empty replicator cleanup requires process fencing"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

pub(super) struct PeerServer {
    ip: String,
    port: u16,
    listener: OnceCell<SocketAddr>,
    listener_tasks: task_ownership::Listener,
    routes: Mutex<HashMap<String, Weak<EmptyReplicator>>>,
}
impl PeerServer {
    pub(super) fn new(ip: String, port: u16) -> Arc<Self> {
        Arc::new(Self {
            ip,
            port,
            listener: OnceCell::new(),
            listener_tasks: task_ownership::Listener::default(),
            routes: Mutex::new(HashMap::new()),
        })
    }
    async fn listen(self: &Arc<Self>) -> RuntimeResult<SocketAddr> {
        self.listener
            .get_or_try_init(|| async {
                let listener = TcpListener::bind((self.ip.as_str(), self.port)).await?;
                let address = listener.local_addr()?;
                if address.ip().is_unspecified() {
                    return Err(failure("unspecified replication/control address"));
                }
                let server = self.clone();
                self.listener_tasks
                    .start(listener, Duration::from_secs(2), move |stream| {
                        let server = server.clone();
                        async move {
                            let _ = server.request(stream).await;
                        }
                    })?;
                Ok(address)
            })
            .await
            .copied()
    }
    // Shared across replicas; retiring a replica removes its route and drains
    // incoming requests without stopping this application's listener.
    pub(super) fn stop(&self) {
        self.listener_tasks.stop();
    }
    pub(super) async fn join(&self) -> RuntimeResult<()> {
        self.listener_tasks.join().await
    }
    async fn request(&self, mut stream: TcpStream) -> RuntimeResult<()> {
        let header = read_header(&mut stream).await?;
        let path = header
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("GET "))
            .and_then(|line| line.strip_suffix(" HTTP/1.1"))
            .ok_or(failure("invalid control method"))?;
        let peer = {
            let routes = self.routes.lock().unwrap();
            routes.get(path).and_then(Weak::upgrade).inspect(|peer| {
                peer.incoming.fetch_add(1, Ordering::SeqCst);
            })
        };
        if let Some(peer) = peer {
            let _active = Active(&peer.incoming);
            match peer.peer_body() {
                Ok(body) => response(&mut stream, 200, &body).await,
                Err(_) => response(&mut stream, 503, "{}").await,
            }
        } else {
            response(&mut stream, 404, "{}").await
        }
    }
}
#[cfg(test)]
#[path = "stateful_listener_tests.rs"]
mod listener_ownership_tests;
async fn read_header(stream: &mut TcpStream) -> RuntimeResult<String> {
    let mut buffer = vec![0; 4096];
    let mut size = 0;
    loop {
        if size == buffer.len() {
            return Err(failure("HTTP header bound"));
        }
        let read = stream.read(&mut buffer[size..]).await?;
        if read == 0 {
            return Err(RuntimeError::Closed);
        }
        size += read;
        if buffer[..size].windows(4).any(|p| p == b"\r\n\r\n") {
            return String::from_utf8(buffer[..size].to_vec())
                .map_err(|_| failure("invalid HTTP encoding"));
        }
    }
}
async fn response(stream: &mut TcpStream, status: u16, body: &str) -> RuntimeResult<()> {
    let frame = format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(frame.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}
async fn http_peer(address: &str) -> RuntimeResult<serde_json::Value> {
    let (authority, path) = address
        .strip_prefix("http://")
        .and_then(|s| s.split_once('/'))
        .ok_or(failure("invalid control URI"))?;
    let socket: SocketAddr = authority
        .parse()
        .map_err(|_| failure("control URI requires numeric IP/port"))?;
    if socket.ip().is_unspecified()
        || socket.port() == 0
        || path.len() > 1024
        || !path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-_".contains(&b))
    {
        return Err(failure("invalid control URI profile"));
    }
    let mut stream = TcpStream::connect(socket).await?;
    stream
        .write_all(
            format!("GET /{path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut bytes = vec![];
    (&mut stream).take(16_385).read_to_end(&mut bytes).await?;
    if bytes.len() > 16_384 {
        return Err(failure("peer response bound"));
    }
    let reply = std::str::from_utf8(&bytes).map_err(|_| failure("peer response encoding"))?;
    if !reply.starts_with("HTTP/1.1 200 ") {
        return Err(failure("peer is not ready"));
    }
    let (_, body) = reply
        .split_once("\r\n\r\n")
        .ok_or(failure("peer HTTP framing"))?;
    serde_json::from_str(body).map_err(|_| failure("peer JSON"))
}
fn epoch_json(epoch: Epoch) -> serde_json::Value {
    serde_json::json!({"configuration_number":epoch.configuration_number,"data_loss_number":epoch.data_loss_number})
}

pub(super) struct ReplicaRoute {
    partition: NativeRuntimeIntegration,
    replicator: Arc<EmptyReplicator>,
    active: AtomicUsize,
    generation: u64,
}
pub(super) struct Factory(pub(super) Arc<Rpc>);
pub(super) struct Creation {
    pub(super) identity: ReplicaIdentity,
    pub(super) initialization_data: Vec<u8>,
    pub(super) activation: Arc<ActivationContext>,
    pub(super) native: NativeRuntimeIntegration,
}
struct Service {
    rpc: Arc<Rpc>,
    path: String,
    context: Creation,
    route: Mutex<Option<Arc<ReplicaRoute>>>,
}
static ROUTE_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
impl IStatefulServiceFactory for Factory {
    fn create_replica(
        &self,
        service_type: String,
        service_name: String,
        initialization_data: &[u8],
        partition_id: uuid::Uuid,
        replica_id: i64,
    ) -> RuntimeResult<Box<dyn IStatefulServiceReplica>> {
        let native = self.0.native.capture(partition_id, replica_id)?;
        if native.identity().service_type != service_type
            || native.identity().service_name != service_name
        {
            return Err(RuntimeError::InvalidInput(
                "factory arguments differ from native identity",
            ));
        }
        let context = Creation {
            identity: native.identity().clone(),
            initialization_data: initialization_data.to_vec(),
            activation: self.0.current.snapshot(),
            native,
        };
        if data::selected(&context.initialization_data) {
            return Ok(
                data::Service::new(self.0.clone(), context) as Box<dyn IStatefulServiceReplica>
            );
        }
        let generation = ROUTE_GENERATION
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
            .map_err(|_| failure("route generation exhausted"))?;
        let path = format!(
            "/stateful/{}/{}/{}/{}-{generation}",
            context.identity.partition_id,
            context.identity.replica_id,
            context.identity.incarnation,
            context.activation.runtime_id()
        );
        Ok(Box::new(Service {
            rpc: self.0.clone(),
            path,
            context,
            route: Mutex::new(None),
        }) as Box<dyn IStatefulServiceReplica>)
    }
}
impl Service {
    fn remove(&self) -> Option<Arc<ReplicaRoute>> {
        self.rpc.stateful_routes.lock().unwrap().remove(&self.path);
        self.route.lock().unwrap().take()
    }
}
impl IStatefulServiceReplica for Service {
    fn open(
        &self,
        mode: OpenMode,
        partition: Arc<dyn IStatefulServicePartition>,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, Box<dyn IPrimaryReplicator>> {
        Box::pin(async move {
            mode.validate()?;
            let _ = partition.get_partition_information()?;
            let partition = self.context.native.clone();
            if cancellation.is_cancelled() || partition.identity() != &self.context.identity {
                return Err(RuntimeError::Closed);
            }
            partition.validate()?;
            let replicator = EmptyReplicator::new(
                Arc::new(partition.clone()),
                self.context.identity.clone(),
                self.path.clone(),
                self.rpc.peers.clone(),
            );
            replicator.attach_native(partition.clone())?;
            self.rpc.listen().await?;
            let route = Arc::new(ReplicaRoute {
                partition,
                replicator: replicator.clone(),
                active: AtomicUsize::new(0),
                generation: self.context.activation.generation(),
            });
            self.rpc
                .stateful_routes
                .lock()
                .unwrap()
                .insert(self.path.clone(), route.clone());
            *self.route.lock().unwrap() = Some(route);
            Ok(Box::new(binding::EmptyControl(replicator)) as Box<dyn IPrimaryReplicator>)
        })
    }
    fn change_role(
        &self,
        role: ReplicaRole,
        cancellation: Cancellation,
    ) -> ServiceFuture<'_, String> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Closed);
            }
            if role != ReplicaRole::None {
                let route = self
                    .route
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or(RuntimeError::Closed)?;
                route.partition.validate()?;
            }
            println!(
                "STATEFUL_ROLE partition={} replica={} incarnation={} role={role:?} pid={}",
                self.context.identity.partition_id,
                self.context.identity.replica_id,
                self.context.identity.incarnation,
                std::process::id()
            );
            Ok(format!(
                "http://{}:{}{}/service",
                self.rpc.ip, self.rpc.port, self.path
            ))
        })
    }
    fn close(&self, _: Cancellation) -> ServiceFuture<'_, ()> {
        Box::pin(async move {
            if let Some(route) = self.remove() {
                while route.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            println!(
                "STATEFUL_CLOSE partition={} replica={} pid={}",
                self.context.identity.partition_id,
                self.context.identity.replica_id,
                std::process::id()
            );
            Ok(())
        })
    }
    fn abort(&self) {
        if let Some(route) = self.remove() {
            let deadline = Instant::now() + Duration::from_millis(150);
            while route.active.load(Ordering::SeqCst) != 0 {
                assert!(
                    Instant::now() < deadline,
                    "stateful RPC cleanup requires fencing"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        println!(
            "STATEFUL_ABORT partition={} replica={} pid={}",
            self.context.identity.partition_id,
            self.context.identity.replica_id,
            std::process::id()
        );
    }
}
fn readable(snapshot: &ReplicaStatus) -> bool {
    snapshot.lifecycle == ReplicaLifecycle::Open
        && snapshot.current_role == ReplicaRole::Primary
        && snapshot.service_role == ReplicaRole::Primary
        && snapshot.replicator_role == ReplicaRole::Primary
        && snapshot.access().0 == AccessStatus::Granted
}
impl Rpc {
    pub(super) async fn stateful_request(
        &self,
        mut stream: TcpStream,
        path: &str,
        query: &str,
    ) -> RuntimeResult<()> {
        let (base, operation) = path.rsplit_once('/').unwrap_or(("", ""));
        let route = {
            let routes = self.stateful_routes.lock().unwrap();
            routes
                .get(base)
                .inspect(|route| {
                    route.active.fetch_add(1, Ordering::SeqCst);
                })
                .cloned()
        };
        let Some(route) = route else {
            return response(&mut stream, 404, "{}").await;
        };
        let _active = Active(&route.active);
        if !query.is_empty() || !matches!(operation, "service" | "diagnostics") {
            return response(&mut stream, 400, "{}").await;
        }
        let Ok(snapshot) = route.partition.status() else {
            return response(&mut stream, 503, "{}").await;
        };
        if operation == "service" && !readable(&snapshot) {
            return response(&mut stream, 503, "{}").await;
        }
        let id = route.partition.identity();
        let (read, write) = snapshot.access();
        let counters = self.metrics.snapshot();
        let body = serde_json::json!({"pid":std::process::id(),"partition":id.partition_id.to_string(),
            "replica":id.replica_id,"incarnation":id.incarnation,"service":id.service_name,
            "node":self.node,"node_type":self.node_type,"host":self.host,"code_sequence":self.code_sequence,
            "creation_generation":route.generation,"packages":self.package_evidence()?,
            "node_host_pid":self.node_host_pid,"fresh_poll_ttl_ticks":self.fresh_poll_ttl,
            "initial_ttl_ticks":self.initial_ttl,"fresh_poll_elapsed_micros":self.fresh_poll_elapsed_micros,
            "remaining_authority_millis":route.partition.authority_remaining().map(|d|d.as_millis() as u64).unwrap_or(0),
            "stateful":true,"persisted":false,"role":format!("{:?}",snapshot.service_role),
            "replicator_role":format!("{:?}",snapshot.replicator_role),"current_role":format!("{:?}",snapshot.current_role),
            "epoch":epoch_json(snapshot.current_epoch),"previous_epoch":epoch_json(snapshot.previous_epoch),
            "read_status":format!("{read:?}"),"write_status":format!("{write:?}"),
            "replicator":route.replicator.evidence(),"service_address":snapshot.service_address,
            "replication_address":snapshot.replication_address,"native_actions":{
                "open":counters.open_commands,"close":counters.close_commands,
                "abort":counters.abort_commands,"abort_callbacks":counters.abort_callbacks,
                "open_failures":counters.open_failures,"close_failures":counters.close_failures
            }}).to_string();
        let latest = route.partition.status();
        if latest.as_ref().is_err()
            || (operation == "service" && !latest.as_ref().is_ok_and(readable))
        {
            return response(&mut stream, 503, "{}").await;
        }
        response(&mut stream, 200, &body).await
    }
}
