// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Source-guided peer exchange: Service Fabric Transport/TcpConnection.cpp and
// Transport/ListenInstance.{h,cpp}, revision
// 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d. Framing/serialization are reused
// from furpc, not reimplemented here.

//! Bounded clear-TCP classic peer sessions. Admission is not delivery or commit.
//!
//! A dedicated, joined OS thread owns the listener, runtime and every session.
//! No provider code executes here. Incoming byte charges end when the channel
//! transfers a packet to its consumer; subsequent engine retention is the
//! engine's responsibility. The two traffic classes share neither byte budgets
//! nor admission slots. Within each class incoming and outgoing bytes aggregate.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr, TcpListener as StdListener, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, Mutex, Weak,
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use furpc::{Message, MessageId, Peer};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
use uuid::Uuid;

#[cfg(test)]
use tokio::{io::AsyncReadExt, time::timeout};

use crate::{
    wire::{Actor, Body, Endpoint, Packet, MAX_FRAME_SIZE},
    Error, Result, Settings,
};

mod connection;
use connection::run_session;
#[cfg(test)]
#[path = "../tests/support/ipc_wire.rs"]
pub(crate) mod ipc_wire;
#[cfg(test)]
use ipc_wire::{
    decode_message, read_message, validate_fixed, write_message, ListenInstance, MessageExt,
    FIXED_HEADER_SIZE,
};

#[cfg(test)]
#[path = "transport_tests.rs"]
pub(crate) mod tests;

/// A consumer must continuously drain both channels. A full data channel drops
/// data without acknowledging it; replication retransmission remains external.
pub struct Incoming {
    control: mpsc::Receiver<Ingress>,
    data: mpsc::Receiver<Ingress>,
}

/// Internal ingress capability. A route lookup cannot recreate this grant.
/// Retaining it does not retain the session task/socket or permit replacement.
#[derive(Clone)]
pub(crate) struct SourceTarget {
    route: Route,
    actor: Actor,
    address: String,
    state: Weak<SessionState<Error>>,
    peer: Arc<Peer>,
}

impl SourceTarget {
    pub(crate) fn matches(&self, source: &Endpoint) -> bool {
        // This proves the identity of already delivered work, not permission
        // for a new write. EOF cannot retract a fully received packet.
        self.actor == source.actor && self.address == source.address
    }

    pub(crate) fn is_live(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| state.error().is_none())
    }
}

struct Ingress {
    packet: Packet,
    _permit: OwnedSemaphorePermit,
    source: SourceTarget,
}

impl Incoming {
    /// Receives a payload, preferring ready control over data. Cancellation
    /// consumes nothing. An empty closed control queue returns `None` even if
    /// data remains buffered; use [`Self::recv_data`] to drain that remainder.
    pub async fn recv(&mut self) -> Option<Packet> {
        self.recv_bound().await.map(|(packet, _)| packet)
    }

    /// Receives only control, releasing its transport byte lease at transfer.
    /// Cancellation consumes nothing; buffered control drains before `None`.
    pub async fn recv_control(&mut self) -> Option<Packet> {
        self.control.recv().await.map(|ingress| ingress.packet)
    }

    /// Receives only data, releasing its transport byte lease at transfer.
    /// Cancellation consumes nothing; buffered data drains before `None`.
    pub async fn recv_data(&mut self) -> Option<Packet> {
        self.data.recv().await.map(|ingress| ingress.packet)
    }

    #[cfg(test)]
    fn try_recv_control(&mut self) -> std::result::Result<Packet, mpsc::error::TryRecvError> {
        self.control.try_recv().map(|ingress| ingress.packet)
    }

    #[cfg(test)]
    fn try_recv_data(&mut self) -> std::result::Result<Packet, mpsc::error::TryRecvError> {
        self.data.try_recv().map(|ingress| ingress.packet)
    }

    pub(crate) async fn recv_bound(&mut self) -> Option<(Packet, SourceTarget)> {
        let ingress = tokio::select! {
            biased;
            ingress = self.control.recv() => ingress,
            ingress = self.data.recv() => ingress,
        };
        ingress.map(|ingress| (ingress.packet, ingress.source))
    }
}

#[derive(Clone)]
struct Route {
    address: String,
    socket: Option<SocketAddr>,
    learned_on: Option<u64>,
}

/// A clone shares terminal shutdown. Dropping the last handle joins the network
/// thread, including when the caller has no Tokio runtime or drops on a reader.
#[derive(Clone)]
pub struct PeerTransport {
    address: String,
    handle: Arc<Handle>,
}

struct Handle {
    stopped: AtomicBool,
    admission: Mutex<()>,
    stop: watch::Sender<bool>,
    thread: Mutex<Option<JoinHandle<()>>>,
    data: mpsc::Sender<Outgoing>,
    control: mpsc::Sender<Outgoing>,
    network: Arc<Network>,
}

struct Network {
    settings: Settings,
    routes: Mutex<HashMap<Actor, Route>>,
    data: Class,
    control: Class,
    local: LocalListener,
    retired_addresses: Mutex<HashSet<String>>,
    outgoing_addresses: Mutex<HashMap<String, usize>>,
    live_addresses: Mutex<HashMap<String, usize>>,
    route_revision: watch::Sender<u64>,
    #[cfg(any(test, feature = "qualification"))]
    diagnostics: Mutex<std::collections::BTreeMap<&'static str, (u64, String)>>,
}

struct LocalListener {
    address: String,
    instance: u64,
}

struct SessionState<E> {
    error: Mutex<Option<E>>,
    changed: Notify,
}

impl<E> Default for SessionState<E> {
    fn default() -> Self {
        Self {
            error: Mutex::new(None),
            changed: Notify::new(),
        }
    }
}

impl<E: Clone> SessionState<E> {
    fn error(&self) -> Option<E> {
        self.error.lock().unwrap().clone()
    }
    fn terminate(&self, error: E) {
        let mut current = self.error.lock().unwrap();
        if current.is_none() {
            *current = Some(error);
        }
        drop(current);
        self.changed.notify_waiters();
    }
    async fn stopped(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.error().is_some() {
                return;
            }
            changed.await;
        }
    }
}

struct Class {
    bytes: Arc<Semaphore>,
    outgoing_slots: Arc<Semaphore>,
    incoming: mpsc::WeakSender<Ingress>,
}

impl Class {
    fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        self.bytes
            .clone()
            .try_acquire_many_owned(
                bytes
                    .try_into()
                    .map_err(|_| Error::ResourceLimit("peer bytes"))?,
            )
            .map_err(|_| Error::ResourceLimit("peer bytes"))
    }

    fn deliver(
        &self,
        packet: Packet,
        permit: OwnedSemaphorePermit,
        source: SourceTarget,
    ) -> Result<()> {
        let sender = self.incoming.upgrade().ok_or(Error::Closed)?;
        sender
            .try_send(Ingress {
                packet,
                _permit: permit,
                source,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("incoming peer queue"),
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }
}

struct Outgoing {
    route: Route,
    message: Message,
    peer: Option<Arc<Peer>>,
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
    _address: AddressOwner,
    deadline: Instant,
    #[cfg(any(test, feature = "qualification"))]
    data: bool,
    #[cfg(any(test, feature = "qualification"))]
    completion: Option<(MessageId, tokio::sync::oneshot::Sender<WriteReceipt>)>,
}

fn peer_socket(
    addresses: impl IntoIterator<Item = SocketAddr>,
    local: SocketAddr,
) -> Result<SocketAddr> {
    let mut addresses = addresses.into_iter();
    let first = addresses
        .next()
        .ok_or(Error::InvalidInput("peer address did not resolve"))?;
    // A dual-stack hostname must not select ::1 for an IPv4 listener merely
    // because resolver ordering differs across hosts. Keep single-family
    // destinations usable, and never change the advertised wire identity.
    Ok(std::iter::once(first)
        .chain(addresses)
        .find(|address| address.is_ipv4() == local.is_ipv4())
        .unwrap_or(first))
}

// A queued frame, including a one-shot qualification frame whose temporary
// actor route was removed, still owns its socket until write/disposal.
struct AddressOwner {
    network: Arc<Network>,
    address: String,
    queued: bool,
}
impl Drop for AddressOwner {
    fn drop(&mut self) {
        let pool = if self.queued {
            &self.network.outgoing_addresses
        } else {
            &self.network.live_addresses
        };
        let mut owners = pool.lock().unwrap();
        if let Some(count) = owners.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                owners.remove(&self.address);
            }
        }
        drop(owners);
        let live = self.network.live_addresses.lock().unwrap();
        let outgoing = self.network.outgoing_addresses.lock().unwrap();
        let mut retired = self.network.retired_addresses.lock().unwrap();
        let changed = retired.contains(&self.address);
        if !live.contains_key(&self.address) && !outgoing.contains_key(&self.address) {
            retired.remove(&self.address);
        }
        drop(retired);
        drop(outgoing);
        drop(live);
        if changed {
            self.network
                .route_revision
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }
}

/// Socket completion only, not a peer parse, ACK or rejection receipt. Retained
/// evidence keeps its original queue/byte permits until consumed or cancelled.
#[cfg(any(test, feature = "qualification"))]
pub(crate) struct WriteReceipt {
    pub session: u64,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub remote_listener: String,
    pub message_id: MessageId,
    pub frame: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}

#[cfg(feature = "qualification")]
impl WriteReceipt {
    pub(crate) fn evidence(&self) -> serde_json::Value {
        serde_json::json!({
            "classification": "complete_local_socket_acceptance",
            "session": self.session,
            "local": self.local.to_string(), "remote": self.remote.to_string(),
            "remote_listener": self.remote_listener,
            "message_id": {"guid": Uuid::from_bytes_le(self.message_id.guid).to_string(),
                           "index": self.message_id.index},
            "frame_hex": self.frame.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "frame_bytes": self.frame.len()
        })
    }
}

impl PeerTransport {
    /// Binds the actual socket before starting an owned current-thread runtime.
    /// On return the listener is registered and ready, without a caller runtime.
    pub fn bind(address: &str, settings: Settings) -> Result<(Self, Incoming)> {
        Self::bind_with_advertised_ip(address, None, settings)
    }

    /// Publish a concrete listener identity without changing its bound port.
    pub fn bind_with_advertised_ip(
        address: &str,
        advertised_ip: Option<IpAddr>,
        settings: Settings,
    ) -> Result<(Self, Incoming)> {
        settings.validate()?;
        if advertised_ip.is_some_and(|ip| ip.is_unspecified() || ip.is_multicast()) {
            return Err(Error::InvalidInput("invalid advertised replication IP"));
        }
        let listener = StdListener::bind(address)?;
        let bound = listener.local_addr()?;
        let advertised = match advertised_ip {
            Some(ip)
                if ip.is_ipv4() == bound.is_ipv4()
                    && (bound.ip().is_unspecified() || ip == bound.ip()) =>
            {
                SocketAddr::new(ip, bound.port())
            }
            Some(_) => return Err(Error::InvalidInput("advertised IP does not match listener")),
            None if bound.ip().is_unspecified() => {
                return Err(Error::InvalidInput(
                    "wildcard replication listener requires an advertised IP",
                ));
            }
            None => bound,
        };
        listener.set_nonblocking(true)?;
        let address = advertised.to_string();
        let (in_data, data) = mpsc::channel(settings.queue_operations);
        let (in_control, control) = mpsc::channel(settings.control_messages);
        let (out_data, data_rx) = mpsc::channel(settings.queue_operations);
        let (out_control, control_rx) = mpsc::channel(settings.control_messages);
        let (stop, stop_rx) = watch::channel(false);
        let (route_revision, _) = watch::channel(0);
        let network = Arc::new(Network {
            retired_addresses: Mutex::new(HashSet::new()),
            outgoing_addresses: Mutex::new(HashMap::new()),
            live_addresses: Mutex::new(HashMap::new()),
            route_revision,
            data: Class {
                bytes: Arc::new(Semaphore::new(settings.queue_bytes)),
                outgoing_slots: Arc::new(Semaphore::new(settings.queue_operations)),
                incoming: in_data.downgrade(),
            },
            control: Class {
                bytes: Arc::new(Semaphore::new(settings.control_bytes)),
                outgoing_slots: Arc::new(Semaphore::new(settings.control_messages)),
                incoming: in_control.downgrade(),
            },
            local: LocalListener {
                address: address.clone(),
                instance: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
                    .clamp(1, u64::MAX as u128) as u64,
            },
            settings,
            routes: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "qualification"))]
            diagnostics: Mutex::new(std::collections::BTreeMap::new()),
        });
        let owned_network = network.clone();
        let (ready, readiness) = std_mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("fube-peer".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(Error::Io(error.into())));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let _incoming_owners = (in_data, in_control);
                    let listener = match TcpListener::from_std(listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready.send(Err(Error::Io(error.into())));
                            return;
                        }
                    };
                    if ready.send(Ok(())).is_ok() {
                        supervise(listener, owned_network, data_rx, control_rx, stop_rx).await;
                    }
                });
            })?;
        if let Err(error) = readiness.recv().unwrap_or(Err(Error::Closed)) {
            let _ = thread.join();
            return Err(error);
        }
        Ok((
            Self {
                address,
                handle: Arc::new(Handle {
                    stopped: AtomicBool::new(false),
                    admission: Mutex::new(()),
                    stop,
                    thread: Mutex::new(Some(thread)),
                    data: out_data,
                    control: out_control,
                    network,
                }),
            },
            Incoming { control, data },
        ))
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    #[cfg(any(test, feature = "qualification"))]
    pub(crate) fn diagnostics(&self) -> serde_json::Value {
        let n = &self.handle.network;
        serde_json::json!({"events":*n.diagnostics.lock().unwrap(),
            "data_bytes_free":n.data.bytes.available_permits(),
            "control_bytes_free":n.control.bytes.available_permits(),
            "data_slots":n.data.outgoing_slots.available_permits(),
            "control_slots":n.control.outgoing_slots.available_permits(),
            "routes":n.routes.lock().unwrap().len()})
    }

    /// Seeds an authoritative destination. Packet carries a destination actor,
    /// not a destination address; the engine registers membership endpoints.
    /// DNS resolution, if needed, runs on this caller, never the network thread.
    pub fn register_peer(&self, endpoint: &Endpoint) -> Result<()> {
        self.handle.check()?;
        endpoint.to_string().parse::<Endpoint>()?;
        let local = self
            .address
            .parse()
            .map_err(|_| Error::InvalidInput("local peer listener address"))?;
        let socket = peer_socket(endpoint.address.to_socket_addrs()?, local)?;
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.check()?;
        let mut routes = self.handle.network.routes.lock().unwrap();
        if !routes.contains_key(&endpoint.actor)
            && routes.len() >= self.handle.network.settings.connections
        {
            return Err(Error::ResourceLimit("peer routes"));
        }
        routes.insert(
            endpoint.actor,
            Route {
                // Session/accounting keys are resolved socket identities, not
                // interchangeable DNS spellings. Wire actor addresses remain
                // unchanged and are still compared exactly.
                address: socket.to_string(),
                socket: Some(socket),
                learned_on: None,
            },
        );
        Ok(())
    }

    /// Retire this actor route. Unshared sockets are canceled and joined by the
    /// network owner after all queued frame captures drain. Other legitimate
    /// actors at the same address retain their duplex socket.
    pub fn unregister_peer(&self, actor: &Actor) -> Result<()> {
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.check()?;
        if let Some(route) = self.handle.network.routes.lock().unwrap().remove(actor) {
            let live = self.handle.network.live_addresses.lock().unwrap();
            let outgoing = self.handle.network.outgoing_addresses.lock().unwrap();
            if live.contains_key(&route.address) || outgoing.contains_key(&route.address) {
                self.handle
                    .network
                    .retired_addresses
                    .lock()
                    .unwrap()
                    .insert(route.address);
            }
            self.handle
                .network
                .route_revision
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }

    /// Nonblocking bounded admission only. A successful socket write, failed
    /// session or dropped data packet never fabricates a replication receipt.
    pub fn send(&self, packet: Packet) -> Result<()> {
        self.enqueue(
            packet,
            None,
            #[cfg(any(test, feature = "qualification"))]
            None,
        )
    }

    pub(crate) fn send_bound(&self, packet: Packet, source: &SourceTarget) -> Result<()> {
        self.enqueue(
            packet,
            Some(source),
            #[cfg(any(test, feature = "qualification"))]
            None,
        )
    }

    #[cfg(any(test, feature = "qualification"))]
    pub(crate) fn send_observed(
        &self,
        packet: Packet,
    ) -> Result<tokio::sync::oneshot::Receiver<WriteReceipt>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.enqueue(packet, None, Some(sender))?;
        Ok(receiver)
    }

    fn enqueue(
        &self,
        packet: Packet,
        source: Option<&SourceTarget>,
        #[cfg(any(test, feature = "qualification"))] completion: Option<
            tokio::sync::oneshot::Sender<WriteReceipt>,
        >,
    ) -> Result<()> {
        let deadline = Instant::now() + self.handle.network.settings.callback_timeout;
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.check()?;
        if packet.from.address != self.address {
            return Err(Error::InvalidInput("packet source is not this listener"));
        }
        validate_operation_limits(&packet, &self.handle.network.settings)?;
        let route = if let Some(source) = source {
            if packet.to != source.actor || !source.is_live() {
                return Err(Error::Closed);
            }
            source.route.clone()
        } else {
            self.handle
                .network
                .routes
                .lock()
                .unwrap()
                .get(&packet.to)
                .cloned()
                .ok_or(Error::InvalidInput("unregistered peer actor"))?
        };
        let data = is_data(&packet.body);
        let class = self.handle.network.class(data);
        let slot = class
            .outgoing_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("outgoing peer queue"))?;
        let message = packet.encode(true)?;
        #[cfg(any(test, feature = "qualification"))]
        let message_id = message.headers().effective().message_id;
        let size = 12
            + message
                .headers()
                .records()
                .map(|header| 4 + header.value().len())
                .sum::<usize>()
            + message.body().len();
        #[cfg(any(test, feature = "qualification"))]
        if completion.is_some() && size > 16 * 1024 {
            return Err(Error::ResourceLimit("qualification frame bytes"));
        }
        let permit = class.reserve(size + route.address.len() + 128)?;
        let outgoing = Outgoing {
            _address: self.handle.network.own_address(&route.address),
            deadline,
            route,
            message,
            peer: source.map(|source| source.peer.clone()),
            _bytes: permit,
            _slot: slot,
            #[cfg(any(test, feature = "qualification"))]
            data,
            #[cfg(any(test, feature = "qualification"))]
            completion: completion.map(|sender| (message_id, sender)),
        };
        let sender = if data {
            &self.handle.data
        } else {
            &self.handle.control
        };
        sender.try_send(outgoing).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("outgoing peer queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }

    /// Terminal and idempotent. Return implies all session futures, sockets and
    /// queued outgoing captures have been destroyed and the OS thread joined.
    pub fn shutdown(&self) {
        self.handle.shutdown();
    }
    pub fn shutdown_checked(&self) -> Result<()> {
        self.handle.shutdown_checked()
    }
    pub(crate) fn request_stop(&self) {
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.stopped.store(true, Ordering::Release);
        self.handle.stop.send_replace(true);
    }
}

impl Handle {
    fn check(&self) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn shutdown(&self) {
        let _ = self.shutdown_checked();
    }

    fn shutdown_checked(&self) -> Result<()> {
        {
            let _admission = self.admission.lock().unwrap();
            self.stopped.store(true, Ordering::Release);
            self.stop.send_replace(true);
        }
        // Keep the join lock until termination: concurrent shutdown callers
        // must not return merely because another caller took the handle.
        let mut thread = self.thread.lock().unwrap();
        if let Some(thread) = thread.take() {
            thread
                .join()
                .map_err(|_| Error::Protocol("peer reactor panicked"))?;
        }
        Ok(())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Network {
    fn own_address(self: &Arc<Self>, address: &str) -> AddressOwner {
        *self
            .outgoing_addresses
            .lock()
            .unwrap()
            .entry(address.to_owned())
            .or_default() += 1;
        AddressOwner {
            network: self.clone(),
            address: address.to_owned(),
            queued: true,
        }
    }

    fn own_socket_address(self: &Arc<Self>, address: &str) -> AddressOwner {
        *self
            .live_addresses
            .lock()
            .unwrap()
            .entry(address.to_owned())
            .or_default() += 1;
        AddressOwner {
            network: self.clone(),
            address: address.to_owned(),
            queued: false,
        }
    }

    fn retire_unused(&self, sessions: &HashMap<u64, Session>, _addresses: &HashMap<String, u64>) {
        let retired = self.retired_addresses.lock().unwrap().clone();
        let routes = self.routes.lock().unwrap();
        let outgoing = self.outgoing_addresses.lock().unwrap();
        for address in retired {
            if outgoing.get(&address).is_some_and(|count| *count != 0)
                || routes
                    .values()
                    .any(|route| route.address == address && route.learned_on.is_none())
            {
                continue;
            }
            for session in sessions
                .values()
                .filter(|session| session.address.as_ref() == Some(&address))
            {
                session.stop.terminate(Error::Closed);
            }
        }
    }
    #[cfg(any(test, feature = "qualification"))]
    fn note(&self, kind: &'static str, detail: String) {
        let mut d = self.diagnostics.lock().unwrap();
        let entry = d.entry(kind).or_default();
        entry.0 = entry.0.saturating_add(1);
        entry.1 = detail.chars().take(512).collect();
        #[cfg(feature = "qualification")]
        let first_error = (kind == "connection_error" && entry.0 == 1).then(|| entry.1.clone());
        drop(d);
        #[cfg(feature = "qualification")]
        if let Some(detail) = first_error {
            eprintln!("FUBE_PEER_TRANSPORT {detail}");
        }
    }
    fn class(&self, data: bool) -> &Class {
        if data {
            &self.data
        } else {
            &self.control
        }
    }
}

fn is_data(body: &Body) -> bool {
    matches!(
        body,
        Body::Replication { .. } | Body::Copy { .. } | Body::CopyContext { .. }
    )
}

fn validate_operation_limits(packet: &Packet, settings: &Settings) -> Result<()> {
    let check = |data: &[Vec<u8>]| {
        if data.len() > settings.operation_segments
            || data.iter().map(Vec::len).sum::<usize>() > settings.operation_bytes
        {
            Err(Error::ResourceLimit("peer operation payload"))
        } else {
            Ok(())
        }
    };
    match &packet.body {
        Body::Replication { operations, .. } => {
            for operation in operations {
                check(&operation.data)?;
            }
            Ok(())
        }
        Body::Copy { data, .. } | Body::CopyContext { data, .. } => check(data),
        _ => Ok(()),
    }
}

struct Session {
    data: mpsc::Sender<Outgoing>,
    control: mpsc::Sender<Outgoing>,
    stop: Arc<SessionState<Error>>,
    address: Option<String>,
}

struct Ready {
    id: u64,
    address: String,
}

async fn supervise(
    listener: TcpListener,
    network: Arc<Network>,
    mut data: mpsc::Receiver<Outgoing>,
    mut control: mpsc::Receiver<Outgoing>,
    mut stop: watch::Receiver<bool>,
) {
    let (ready, mut ready_rx) = mpsc::channel::<Ready>(network.settings.connections);
    let mut tasks = JoinSet::new();
    let mut sessions = HashMap::<u64, Session>::new();
    let mut addresses = HashMap::<String, u64>::new();
    let mut next = 0u64;
    let mut route_revision = network.route_revision.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = route_revision.changed() => network.retire_unused(&sessions, &addresses),
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(id) = result {
                    sessions.remove(&id);
                    addresses.retain(|_, owner| *owner != id);
                    network.routes.lock().unwrap()
                        .retain(|_, route| route.learned_on != Some(id));
                }
            }
            Some(Ready { id, address }) = ready_rx.recv() => {
                if let Some(session) = sessions.get_mut(&id) {
                    session.address = Some(address.clone());
                    addresses.entry(address).or_insert(id);
                    network.retire_unused(&sessions, &addresses);
                }
            }
            Some(outgoing) = control.recv() => {
                route_outgoing(outgoing, false, &network, &ready, &mut next,
                    &mut sessions, &mut addresses, &mut tasks);
            }
            Some(outgoing) = data.recv() => {
                route_outgoing(outgoing, true, &network, &ready, &mut next,
                    &mut sessions, &mut addresses, &mut tasks);
            }
            accepted = listener.accept(), if sessions.len() < network.settings.connections => {
                if let Ok((socket, _)) = accepted {
                    next += 1;
                    let id = next;
                    let (session, data_rx, control_rx) = session_channels(&network.settings);
                    let stopped = session.stop.clone();
                    sessions.insert(id, session);
                    let network = network.clone();
                    let ready = ready.clone();
                    tasks.spawn(async move {
                        let deadline = Instant::now() + network.settings.callback_timeout;
                        let _ = run_session(Socket::Accepted(socket), id, network, ready,
                            data_rx, control_rx, stopped, deadline).await;
                        id
                    });
                }
            }
        }
    }
    drop(listener);
    data.close();
    control.close();
    for session in sessions.values() {
        session.stop.terminate(Error::Closed);
    }
    while tasks.join_next().await.is_some() {}
    // Drop buffers while still on the owned thread, before signaling join.
    drop(sessions);
    drop(data);
    drop(control);
    network.routes.lock().unwrap().clear();
}

fn session_channels(
    settings: &Settings,
) -> (Session, mpsc::Receiver<Outgoing>, mpsc::Receiver<Outgoing>) {
    let (data, data_rx) = mpsc::channel(settings.queue_operations);
    let (control, control_rx) = mpsc::channel(settings.control_messages);
    (
        Session {
            data,
            control,
            stop: Arc::new(SessionState::default()),
            address: None,
        },
        data_rx,
        control_rx,
    )
}

#[allow(clippy::too_many_arguments)]
fn route_outgoing(
    outgoing: Outgoing,
    data: bool,
    network: &Arc<Network>,
    ready: &mpsc::Sender<Ready>,
    next: &mut u64,
    sessions: &mut HashMap<u64, Session>,
    addresses: &mut HashMap<String, u64>,
    tasks: &mut JoinSet<u64>,
) {
    if Instant::now() >= outgoing.deadline {
        return;
    }
    #[cfg(any(test, feature = "qualification"))]
    if outgoing
        .completion
        .as_ref()
        .is_some_and(|(_, sender)| sender.is_closed())
    {
        return;
    }
    let address = &outgoing.route.address;
    let id = if let Some(id) = outgoing.route.learned_on {
        // A captured source session is not an address-resolution hint.
        // Retirement or duplication cannot turn it into another connection.
        if !sessions.contains_key(&id) {
            return;
        }
        id
    } else {
        match addresses.get(address).copied() {
            Some(id) => id,
            None => {
                let Some(socket) = outgoing.route.socket else {
                    return;
                };
                if sessions.len() >= network.settings.connections {
                    return;
                }
                *next += 1;
                let id = *next;
                let (mut session, data_rx, control_rx) = session_channels(&network.settings);
                session.address = Some(address.clone());
                let stopped = session.stop.clone();
                sessions.insert(id, session);
                addresses.insert(address.clone(), id);
                let ready = ready.clone();
                let network = network.clone();
                let deadline = outgoing.deadline;
                tasks.spawn(async move {
                    let _ = run_session(
                        Socket::Connect(socket),
                        id,
                        network,
                        ready,
                        data_rx,
                        control_rx,
                        stopped,
                        deadline,
                    )
                    .await;
                    id
                });
                id
            }
        }
    };
    if let Some(session) = sessions.get(&id) {
        let _ = if data {
            session.data.try_send(outgoing)
        } else {
            session.control.try_send(outgoing)
        };
    }
}

enum Socket {
    Connect(SocketAddr),
    Accepted(TcpStream),
}

fn packet_bytes(packet: &Packet) -> usize {
    let segments =
        |data: &[Vec<u8>]| std::mem::size_of_val(data) + data.iter().map(Vec::len).sum::<usize>();
    let body = match &packet.body {
        Body::Replication { operations, .. } => {
            operations.len() * std::mem::size_of::<crate::wire::WireOperation>()
                + operations
                    .iter()
                    .map(|op| segments(&op.data))
                    .sum::<usize>()
        }
        Body::Copy { data, .. } | Body::CopyContext { data, .. } => segments(data),
        Body::InduceFault { reason, .. } => reason.len(),
        _ => 0,
    };
    std::mem::size_of::<Packet>() + packet.from.address.len() + body
}
