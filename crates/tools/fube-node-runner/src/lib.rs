//! Fresh, single-partition volatile Nodes under one externally owned FM.
//! Thin process/transport ownership around the cluster-tested Node control ingress.
//! No reconnect, peer replacement or runtime/store adoption.
#![forbid(unsafe_code)]

pub mod config;

pub use config::{Config, Fm, Identity, Placement};
use fube_hosting::local::{Activation, NodeContext, Options, Profile};
use fube_local_host::manifest::Package;
use fube_node::{
    control::{
        ingress::{Admission, Ingress},
        peer_target,
        wire::{self, Link},
        Adapter, LfumUploadState,
    },
    local::Node,
};
use fube_ra::{entity as ra, protocol::*};
use fube_ra_store::{MemoryBackend, Settings, Store};
use fube_transport::{channel::IncomingMessage, native_channel::GatewayTarget, Endpoint};
use serde::Serialize;
use std::{
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, time::Instant};

pub type Result<T> = std::result::Result<T, String>;
type Incoming = IncomingMessage<GatewayTarget>;

pub(crate) fn err(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn emit(writer: &mut impl Write, value: &serde_json::Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(err)?;
    writer.write_all(b"\n").map_err(err)
}

#[derive(Debug, Serialize)]
pub struct Outcome {
    /// Owned local shutdown succeeded; this is not evidence of healthy control.
    pub clean: bool,
    pub activation_acquired: bool,
    pub application_cleaned: bool,
    pub executor_closed: bool,
    pub store_closed: bool,
    pub channels_closed: bool,
    pub pending_work: usize,
    pub error: Option<String>,
    /// Original-session uncertainty survives successful local cleanup.
    pub control_error: Option<String>,
}

struct Peer {
    placement: Placement,
    link: Option<Link>,
    incoming: Option<Incoming>,
    loss: Option<String>,
}

struct Owner {
    config: Config,
    peers: Vec<Peer>,
    fm: Option<Link>,
    fm_incoming: Option<Incoming>,
    fm_loss: Option<String>,
    node: Option<Node>,
    store: Option<Arc<Store>>,
    adapter: Adapter,
    ingress: Ingress,
    upload: LfumUploadState,
    description: Option<NodeDescription>,
    bootstrapped: bool,
    error: Option<String>,
}

impl Owner {
    fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let ingress = config.ingress()?;
        Package::load_with_profile(
            &config.package,
            &config.application,
            &config.parameters,
            Profile::VolatileStatefulExternalFm,
        )
        .map_err(err)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&config.run_directory)
            .map_err(err)?;
        fs::create_dir(config.run_directory.join("hosting")).map_err(err)?;
        let peers = config
            .peers
            .iter()
            .cloned()
            .map(|placement| Peer {
                placement,
                link: None,
                incoming: None,
                loss: None,
            })
            .collect();
        let owner = Self {
            config,
            peers,
            fm: None,
            fm_incoming: None,
            fm_loss: None,
            node: None,
            store: None,
            adapter: Adapter::default(),
            ingress,
            upload: LfumUploadState::default(),
            description: None,
            bootstrapped: false,
            error: None,
        };
        owner.status("starting", None)?;
        Ok(owner)
    }

    fn fail(&mut self, error: String) {
        if self.error.is_none() {
            self.error = Some(error);
            if let Err(error) = emit(
                &mut std::io::stderr().lock(),
                &serde_json::json!({"event": "control_unknown", "error": self.error}),
            ) {
                self.error
                    .as_mut()
                    .expect("retained failure")
                    .push_str(&format!("; diagnostic output failed: {error}"));
            }
        }
    }

    fn live(link: &Link) -> bool {
        link.channel.target().is_ok()
    }

    fn control_error(&self) -> Option<&str> {
        self.fm_loss
            .as_deref()
            .or_else(|| self.peers.iter().find_map(|peer| peer.loss.as_deref()))
    }

    fn observe_channels(&mut self, stopping: bool) -> Result<()> {
        if let Some(error) = self
            .fm
            .as_ref()
            .and_then(|link| link.channel.terminal_error())
        {
            channel_loss(&mut self.fm_loss, "FM", error, stopping)?;
        }
        for peer in &mut self.peers {
            if let Some(error) = peer
                .link
                .as_ref()
                .and_then(|link| link.channel.terminal_error())
            {
                channel_loss(
                    &mut peer.loss,
                    &format!("peer {}", peer.placement.identity.id),
                    error,
                    stopping,
                )?;
            }
        }
        Ok(())
    }

    fn status(&self, stage: &str, outcome: Option<&Outcome>) -> Result<()> {
        let snapshot = self.node.as_ref().map(|node| {
            node.snapshot(
                self.config
                    .partition_id()
                    .expect("validated partition")
                    .guid
                    .0,
            )
        });
        let state = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.committed.as_ref());
        let status = serde_json::json!({
            "schema_version": 1, "protocol_profile": "external-fm-volatile-three-v1",
            "deployment": self.config.deployment, "node": self.config.node.identity,
            "name": self.config.name, "address": self.config.node.address, "stage": stage,
            "control_bootstrapped": self.bootstrapped,
            "control_unknown": self.error.is_some() || self.control_error().is_some(),
            "fm_connected": self.fm.as_ref().is_some_and(Self::live),
            "peers_connected": self.peers.iter().filter(|peer| peer.link.as_ref().is_some_and(Self::live)).count(),
            "application_pid": self.node.as_ref().and_then(|node| node.activation().process_status()).map(|process| process.pid),
            "registered": self.node.as_ref().is_some_and(|node| node.package_registration().registered),
            "replica_phase": state.map(|state| format!("{:?}", state.phase)),
            "replica_role": state.map(|state| state.body.replica.current_role),
            "local_configuration_complete": state.is_some_and(|state| state.reconfiguration.as_ref()
                .is_some_and(|configuration| configuration.stage == ra::ConfigurationStage::Complete)),
            "generations": self.node.as_ref().map(|node| {
                let state = node.generation_state();
                serde_json::json!({
                    "receive": state.receive.generation,
                    "send": state.send.generation,
                    "upload_pending": state.upload_pending,
                    "pending_uploads": node.pending_lfum_uploads(),
                })
            }),
            "service_endpoint": state.map(|state| &state.body.replica.service_endpoint),
            "replication_endpoint": state.map(|state| &state.body.replica.replication_endpoint),
            "pending_commands": self.node.as_ref().map_or(0, Node::pending_commands),
            "adapter_pending": self.adapter.pending(), "semantic_pending": self.adapter.semantic_pending(),
            "adapter_error": self.adapter.error(), "peer_error": self.adapter.peer_error(),
            "peer_command_error": self.adapter.peer_command_error(),
            "control_error": self.control_error(),
            "error": self.error.as_deref().or(self.control_error()), "cleanup": outcome,
        });
        let bytes = serde_json::to_vec(&status).map_err(err)?;
        if bytes.len() > fube_node::control::MAX_BODY_BYTES {
            return Err("Node status exceeds 64 KiB".into());
        }
        let temporary = self.config.run_directory.join("status.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(err)?;
        file.write_all(&bytes).map_err(err)?;
        drop(file);
        fs::rename(temporary, self.config.run_directory.join("status.json")).map_err(err)
    }

    async fn connect(&self, target: SocketAddr, peer: bool, deadline: Instant) -> Result<Link> {
        let composition = if peer {
            wire::peer_composition()
        } else {
            wire::composition(false)
        }
        .map_err(err)?;
        let endpoint = Endpoint::unsecured_tcp(target, composition)
            .map_err(err)?
            .local_ip((*self.config.node.address.ip()).into())
            .channel_limits(wire::limits());
        loop {
            match endpoint.connect_until(deadline).await {
                Ok(channel) => {
                    let remote = if peer {
                        self.peers
                            .iter()
                            .find(|peer| SocketAddr::V4(peer.placement.address) == target)
                            .ok_or("unconfigured peer destination")?
                            .placement
                            .identity
                            .native()?
                    } else {
                        self.config.fm.identity.native()?
                    };
                    return Link::new(
                        channel,
                        self.config.node.identity.native()?,
                        remote,
                        self.config.generation()?,
                    );
                }
                Err(fube_transport::Error::Io(error)) if Instant::now() < deadline => {
                    if deadline.saturating_duration_since(Instant::now())
                        <= Duration::from_millis(20)
                    {
                        return Err(format!("initial TCP connection failed: {error}"));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(err(error)),
            }
        }
    }

    async fn startup(&mut self) -> Result<()> {
        let deadline = Instant::now() + self.config.startup_budget();
        let local = self.config.node.identity.native()?;
        let number = u128::from_le_bytes(local.id);
        let listener = TcpListener::bind(self.config.node.address)
            .await
            .map_err(err)?;
        let mut outgoing: Vec<_> = self
            .peers
            .iter()
            .enumerate()
            .filter_map(|(index, peer)| {
                let id = peer.placement.identity.native().expect("validated peer");
                (number < u128::from_le_bytes(id.id)).then_some((u128::from_le_bytes(id.id), index))
            })
            .collect();
        outgoing.sort_unstable();
        for (_, index) in outgoing {
            let link = self
                .connect(
                    SocketAddr::V4(self.peers[index].placement.address),
                    true,
                    deadline,
                )
                .await?;
            if let Some(error) = self
                .adapter
                .peer_error()
                .or(self.adapter.peer_command_error())
            {
                return Err(format!("original peer work failed: {error}"));
            }
            self.peers[index].link = Some(link);
        }
        while self.peers.iter().any(|peer| peer.link.is_none()) {
            let (stream, source) = tokio::time::timeout_at(deadline, listener.accept())
                .await
                .map_err(err)?
                .map_err(err)?;
            let index = self.peers.iter().position(|peer| {
                source.ip() == *peer.placement.address.ip()
                    && peer.link.is_none()
                    && peer
                        .placement
                        .identity
                        .native()
                        .is_ok_and(|id| u128::from_le_bytes(id.id) < number)
            });
            let Some(index) = index else {
                emit(
                    &mut std::io::stderr().lock(),
                    &serde_json::json!({
                        "event": "peer_rejected", "source": source.to_string(),
                        "reason": "unconfigured, duplicate or wrong-direction peer socket"
                    }),
                )?;
                drop(stream);
                continue;
            };
            let channel = fube_transport::accept_unsecured_tcp(
                stream,
                wire::peer_composition().map_err(err)?,
                wire::limits(),
                deadline.min(Instant::now() + Duration::from_secs(3)),
            )
            .await
            .map_err(err)?;
            self.peers[index].link = Some(Link::new(
                channel,
                local.clone(),
                self.peers[index].placement.identity.native()?,
                self.config.generation()?,
            )?);
        }
        drop(listener);
        self.fm = Some(
            self.connect(SocketAddr::V4(self.config.fm.address), false, deadline)
                .await?,
        );
        let link = self.fm.as_ref().expect("original FM Channel");
        let version = FabricVersionInstance::native_default();
        let up = NodeUp::fresh_stateless(
            self.config.name.clone(),
            "fube-node".into(),
            self.config.node.address.into(),
            version.clone(),
        )
        .map_err(err)?;
        let reply =
            tokio::time::timeout_at(deadline, link.request(node_up_message(&up).map_err(err)?))
                .await
                .map_err(err)??;
        wire::predecode(&reply)?;
        let (ack, generation) = decode_node_up_ack(&reply).map_err(err)?;
        ack.validate_fresh_activated(&version).map_err(err)?;
        if generation != self.config.generation()? {
            return Err("foreign NodeUpAck generation".into());
        }
        let upload = ReplicaUp {
            last: true,
            ..ReplicaUp::default()
        };
        link.send(replica_up_message(&upload, fube_ra::protocol::Owner::Fm).map_err(err)?)
            .await?;
        let mut incoming = link
            .receive(deadline)
            .await?
            .ok_or("final inventory reply timeout")?;
        wire::predecode(&incoming.message)?;
        let (body, generation) = decode_replica_up_reply(&incoming.message).map_err(err)?;
        if body != upload || generation != self.config.generation()? {
            return Err("fresh bootstrap requires the matching final empty inventory reply".into());
        }
        if let Some(handoff) = incoming.handoff.as_mut() {
            handoff.acknowledge();
        }
        let mut options = Options::new(self.config.run_directory.join("hosting"));
        options.profile = Profile::VolatileStatefulExternalFm;
        options.node = Some(NodeContext {
            id: self.config.node.identity.id.clone(),
            instance: local.instance,
            name: self.config.name.clone(),
            ip: (*self.config.node.address.ip()).into(),
        });
        options.service_package_version_instance = Some(self.config.package_version_instance);
        self.store = Some(Arc::new(
            Store::new(Box::new(MemoryBackend::new()), Settings::default())
                .await
                .map_err(err)?,
        ));
        let node = Node::new(
            Activation::new(options).map_err(err)?,
            self.store.as_ref().expect("original memory store").clone(),
            ra::Admission {
                local_node: local,
                fm_source: self.config.fm.identity.native()?,
                generation: self.config.generation()?.number,
                owner_ready: true,
                node_open: true,
                node_activated: true,
                fabric_upgrading: false,
            },
            ra::Settings {
                max_outstanding: 32,
                max_service_types: 1,
                ..ra::Settings::default()
            },
        )
        .map_err(err)?;
        self.node = Some(node);
        let node = self.node.as_mut().expect("original Node owner");
        node.bind_peers(
            self.config
                .peers
                .iter()
                .map(|peer| peer.identity.native())
                .collect::<Result<Vec<_>>>()?,
        )
        .map_err(err)?;
        node.apply_fresh_node_up_ack(&ack, &generation)
            .map_err(err)?;
        node.start(
            &self.config.package,
            &self.config.application,
            &self.config.parameters,
        )
        .map_err(err)?;
        self.bootstrapped = true;
        self.description = Some(up.node);
        self.status("running", None)?;
        emit(
            &mut std::io::stdout().lock(),
            &serde_json::json!({"event": "running", "status": self.config.run_directory.join("status.json")}),
        )?;
        Ok(())
    }

    async fn step(&mut self, stopping: bool) -> Result<()> {
        let Some(node) = self.node.as_mut() else {
            return Ok(());
        };
        node.drive().map_err(err)?;
        self.observe_channels(stopping)?;
        let control_unknown = self.error.is_some() || self.control_error().is_some();
        let node = self.node.as_mut().expect("retained Node");
        if let Some(description) = &self.description {
            tokio::select! {
                biased;
                reply = self.upload.receive() => {
                    self.upload.complete(node, &self.config.fm.identity.native()?, reply, Instant::now())?;
                }
                _ = std::future::ready(()) => {}
            }
            if !stopping && !control_unknown {
                self.upload.advance(
                    node,
                    description,
                    self.fm.as_ref().filter(|link| Self::live(link)),
                    Instant::now(),
                )?;
            }
        }
        self.adapter.capture(node);
        let target = self
            .adapter
            .peer_replies()
            .front()
            .and_then(|reply| peer_target(&reply.effect));
        let selected = target.and_then(|target| {
            self.peers.iter().position(|peer| {
                peer.placement
                    .identity
                    .native()
                    .is_ok_and(|identity| &identity == target)
            })
        });
        self.adapter
            .advance_with_peer(
                node,
                self.fm.as_ref().filter(|link| Self::live(link)),
                selected
                    .and_then(|index| self.peers[index].link.as_ref())
                    .filter(|link| Self::live(link)),
                selected
                    .map(|index| self.peers[index].placement.identity.native())
                    .transpose()?
                    .as_ref(),
            )
            .await?;
        if control_unknown && !stopping {
            return Ok(());
        }
        for peer in &mut self.peers {
            let Some(link) = peer.link.as_ref().filter(|link| Self::live(link)) else {
                continue;
            };
            if self.adapter.peer_commands().len() == 32 {
                break;
            }
            if peer.incoming.is_none() {
                match link
                    .receive_checked(Instant::now() + Duration::from_millis(1))
                    .await
                {
                    Ok(incoming) => peer.incoming = incoming,
                    Err(wire::ReceiveError::Transport(error)) => {
                        channel_loss(
                            &mut peer.loss,
                            &format!("peer {}", peer.placement.identity.id),
                            error,
                            stopping,
                        )?;
                        if !stopping {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(wire::ReceiveError::Invalid(error)) => return Err(error),
                }
            }
            if let Some(mut incoming) = peer.incoming.take() {
                match self.ingress.peer(
                    node,
                    &peer.placement.identity.native()?,
                    &incoming.message,
                    stopping,
                )? {
                    Some(id) => {
                        self.adapter.track_peer_command(id)?;
                        if let Some(handoff) = incoming.handoff.as_mut() {
                            handoff.acknowledge();
                        }
                    }
                    None => peer.incoming = Some(incoming),
                }
            }
        }
        if self.adapter.acknowledgements().len() == 32 {
            return Ok(());
        }
        if let Some(link) = self.fm.as_ref().filter(|link| Self::live(link)) {
            if self.fm_incoming.is_none() {
                match link
                    .receive_checked(Instant::now() + Duration::from_millis(1))
                    .await
                {
                    Ok(incoming) => self.fm_incoming = incoming,
                    Err(wire::ReceiveError::Transport(error)) => {
                        channel_loss(&mut self.fm_loss, "FM", error, stopping)?;
                        return Ok(());
                    }
                    Err(wire::ReceiveError::Invalid(error)) => return Err(error),
                }
            }
            if let Some(mut incoming) = self.fm_incoming.take() {
                if !stopping {
                    if let Some(admission) =
                        self.ingress.generation(node, link, &mut incoming).await?
                    {
                        match admission {
                            Admission::Retained => self.fm_incoming = Some(incoming),
                            Admission::Submitted(_) => {
                                if let Some(handoff) = incoming.handoff.as_mut() {
                                    handoff.acknowledge();
                                }
                            }
                        }
                        return Ok(());
                    }
                }
                if self.adapter.reject_deprecated(node, &incoming.message)? {
                    if let Some(handoff) = incoming.handoff.as_mut() {
                        handoff.acknowledge();
                    }
                    return Ok(());
                }
                match self.ingress.fm(node, &incoming.message, stopping)? {
                    Admission::Retained => self.fm_incoming = Some(incoming),
                    Admission::Submitted(acknowledgement) => {
                        if let Some(acknowledgement) = acknowledgement {
                            self.adapter.track_acknowledgement(acknowledgement)?;
                        }
                        if let Some(handoff) = incoming.handoff.as_mut() {
                            handoff.acknowledge();
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn cleanup_node(&mut self, deadline: Instant) -> Result<()> {
        let partition = self.config.partition_id()?;
        let Some(node) = &self.node else {
            return Ok(());
        };
        let app_pid = node
            .activation()
            .process_status()
            .map(|process| process.pid);
        let root = node.activation().root().map(Path::to_path_buf);
        let graceful = deadline.min(Instant::now() + Duration::from_secs(8));
        let mut requested = false;
        loop {
            let node = self.node.as_mut().expect("retained Node");
            match node.snapshot(partition.guid.0).committed {
                Some(state) if state.phase == ra::Phase::Closed && node.pending_commands() == 0 => {
                    break
                }
                Some(state) if !requested && state.phase != ra::Phase::Closing => {
                    node.close(partition.clone(), ra::CloseMode::Close)
                        .map_err(err)?;
                    requested = true;
                }
                None if node.pending_commands() == 0 => break,
                _ => {}
            }
            if Instant::now() >= graceful {
                break;
            }
            self.step(true).await?;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        loop {
            let node = self.node.as_mut().expect("retained Node");
            let terminated = match node.terminate() {
                Ok(terminated) => terminated,
                Err(fube_node::local::Error::Activation(fube_hosting::local::LocalError::Busy)) => {
                    false
                }
                Err(error) => return Err(err(error)),
            };
            if terminated && node.quiescent() {
                break;
            }
            self.step(true).await?;
            if Instant::now() >= deadline {
                return Err("original Node physical cleanup incomplete".into());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let node = self.node.as_ref().expect("retained Node");
        node.close_executor(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(3)),
        )
        .await
        .map_err(err)?;
        let cleanup = node.activation().cleanup_status();
        if !cleanup.directory_released
            || !cleanup.logical_retired
            || node
                .activation()
                .process_status()
                .is_some_and(|process| !process.reaped || !process.group_drained)
            || root.is_some_and(|root| root.exists())
            || app_pid.is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists())
        {
            return Err("original application disposal/group/root cleanup is unproved".into());
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Outcome {
        let deadline = Instant::now() + self.config.shutdown_budget();
        let node_result = self.cleanup_node(deadline).await;
        if let Err(error) = &node_result {
            self.fail(error.clone());
            if let Some(node) = &mut self.node {
                node.stop_admission();
                if let Err(error) = node.terminate() {
                    self.fail(format!("physical retirement: {error:?}"));
                }
            }
        }
        let store_closed = if let Some(store) = &self.store {
            match store.close().await {
                Ok(report) if report.is_clean() => true,
                Ok(report) => {
                    self.fail(format!("memory store shutdown is not clean: {report:?}"));
                    false
                }
                Err(error) => {
                    self.fail(format!("memory store close failed: {error:?}"));
                    false
                }
            }
        } else {
            true
        };
        if let Err(error) = self.observe_channels(true) {
            self.fail(error);
        }
        let mut channels_closed = true;
        for link in self
            .fm
            .iter()
            .chain(self.peers.iter().filter_map(|peer| peer.link.as_ref()))
        {
            link.channel.dispose();
            if let Err(error) = link.channel.closed(deadline).await {
                channels_closed = false;
                self.adapter.record_error(format!(
                    "original control Channel cleanup failed: {error:?}"
                ));
            }
        }
        if !channels_closed {
            self.fail("original control Channel cleanup incomplete".into());
        }
        let pending_work = self.adapter.pending()
            + self.adapter.semantic_pending()
            + self.node.as_ref().map_or(0, |node| {
                node.external_pending().len() + node.pending_lfum_uploads()
            });
        Outcome {
            clean: node_result.is_ok() && store_closed && channels_closed && self.error.is_none(),
            activation_acquired: self.node.is_some(),
            application_cleaned: self.node.is_some() && node_result.is_ok(),
            executor_closed: self.node.is_some() && node_result.is_ok(),
            store_closed,
            channels_closed,
            pending_work,
            error: self.error.clone(),
            control_error: self.control_error().map(str::to_owned),
        }
    }
}

fn channel_loss(
    retained: &mut Option<String>,
    name: &str,
    error: fube_transport::Error,
    stopping: bool,
) -> Result<()> {
    if !matches!(
        error,
        fube_transport::Error::Io(_) | fube_transport::Error::Closed
    ) {
        return Err(format!("original {name} Channel failed: {error:?}"));
    }
    if retained.is_none() {
        let error = format!("original {name} Channel lost: {error:?}");
        emit(
            &mut std::io::stderr().lock(),
            &serde_json::json!({
                "event": "control_unknown", "stopping": stopping, "error": error
            }),
        )?;
        *retained = Some(error);
    }
    Ok(())
}

/// Run once. Losing an admitted session never reconnects or grants promotion;
/// the original owner stays alive until the caller requests local shutdown.
pub async fn run(config: Config, stop: impl Future<Output = Result<()>>) -> Result<Outcome> {
    let mut owner = Owner::new(config)?;
    tokio::pin!(stop);
    let started = tokio::select! {
        result = owner.startup() => result,
        result = &mut stop => result.and_then(|()| Err("stop requested during startup".into())),
    };
    if let Err(error) = started {
        owner.fail(error);
    } else {
        let mut status_at = Instant::now();
        loop {
            tokio::select! {
                result = &mut stop => {
                    if let Err(error) = result { owner.fail(error); }
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(2)) => {
                    if let Err(error) = owner.step(false).await { owner.fail(error); }
                    if Instant::now() >= status_at {
                        if let Err(error) = owner.status("running", None) { owner.fail(error); }
                        status_at = Instant::now() + Duration::from_millis(50);
                    }
                }
            }
        }
    }
    if let Err(error) = owner.status("stopping", None) {
        owner.fail(error);
    }
    let outcome = owner.shutdown().await;
    owner.status("stopped", Some(&outcome))?;
    emit(
        &mut std::io::stdout().lock(),
        &serde_json::json!({"event": "stopped", "cleanup": outcome}),
    )?;
    Ok(outcome)
}
