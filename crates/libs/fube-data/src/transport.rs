// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
//
// Source-guided peer exchange: Service Fabric Transport/TcpConnection.cpp and
// Transport/ListenInstance.{h,cpp}, revision
// 3988b4518236d2d0a37f4d8bdcdf0a2093c5a61d. Framing/serialization are reused
// from mssf-transport, not reimplemented here.

//! Bounded clear-TCP classic peer sessions. Admission is not delivery or commit.
//!
//! A dedicated, joined OS thread owns the listener, runtime and every session.
//! No provider code executes here. Incoming byte charges end when the channel
//! transfers a packet to its consumer; subsequent engine retention is the
//! engine's responsibility. The two traffic classes share neither byte budgets
//! nor admission slots. Within each class incoming and outgoing bytes aggregate.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{SocketAddr, TcpListener as StdListener, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
};

use mssf_transport::{
    connection::{read_message, write_message},
    frame::{validate_fixed, Frame, FIXED_HEADER_SIZE},
    message::{ListenInstance, Message, MessageId, ACTOR, IPC, MESSAGE_ID},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::timeout,
};
use uuid::Uuid;

use crate::{
    wire::{Actor, Body, Endpoint, Packet},
    Error, Result, Settings,
};

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;

/// A consumer must continuously drain both channels. A full data channel drops
/// data without acknowledging it; replication retransmission remains external.
pub struct Incoming {
    pub control: mpsc::Receiver<Packet>,
    pub data: mpsc::Receiver<Packet>,
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
    local: ListenInstance,
    retired_addresses: Mutex<HashSet<String>>,
    outgoing_addresses: Mutex<HashMap<String, usize>>,
    live_addresses: Mutex<HashMap<String, usize>>,
    route_revision: watch::Sender<u64>,
    #[cfg(any(test, feature = "qualification"))]
    diagnostics: Mutex<std::collections::BTreeMap<&'static str, (u64, String)>>,
}

struct Class {
    bytes: Arc<Semaphore>,
    outgoing_slots: Arc<Semaphore>,
    incoming: mpsc::WeakSender<Packet>,
    // Packet's public channel type has no embedded permit. Reconcile against
    // channel occupancy under this lock before every admission.
    received: Mutex<VecDeque<OwnedSemaphorePermit>>,
}

impl Class {
    fn reap_locked(&self, received: &mut VecDeque<OwnedSemaphorePermit>) {
        let queued = self
            .incoming
            .upgrade()
            .map_or(0, |sender| sender.max_capacity() - sender.capacity());
        while received.len() > queued {
            received.pop_front();
        }
    }

    fn reserve(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        self.reap_locked(&mut self.received.lock().unwrap());
        self.bytes
            .clone()
            .try_acquire_many_owned(
                bytes
                    .try_into()
                    .map_err(|_| Error::ResourceLimit("peer bytes"))?,
            )
            .map_err(|_| Error::ResourceLimit("peer bytes"))
    }

    fn deliver(&self, packet: Packet, permit: OwnedSemaphorePermit) -> Result<()> {
        let mut received = self.received.lock().unwrap();
        self.reap_locked(&mut received);
        let sender = self.incoming.upgrade().ok_or(Error::Closed)?;
        sender.try_send(packet).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Error::ResourceLimit("incoming peer queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })?;
        received.push_back(permit);
        Ok(())
    }
}

struct Outgoing {
    route: Route,
    bytes: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
    _address: AddressOwner,
    #[cfg(any(test, feature = "qualification"))]
    data: bool,
    #[cfg(any(test, feature = "qualification"))]
    completion: Option<(MessageId, tokio::sync::oneshot::Sender<WriteReceipt>)>,
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
            "classification": "socket_write_all_and_flush",
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
        settings.validate()?;
        let listener = StdListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
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
                received: Mutex::new(VecDeque::new()),
            },
            control: Class {
                bytes: Arc::new(Semaphore::new(settings.control_bytes)),
                outgoing_slots: Arc::new(Semaphore::new(settings.control_messages)),
                incoming: in_control.downgrade(),
                received: Mutex::new(VecDeque::new()),
            },
            local: ListenInstance {
                address: address.clone(),
                instance: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
                    .clamp(1, u64::MAX as u128) as u64,
                nonce: [0; 16],
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
                        let _ = ready.send(Err(Error::Io(error)));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let _incoming_owners = (in_data, in_control);
                    let listener = match TcpListener::from_std(listener) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready.send(Err(Error::Io(error)));
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
        let socket = endpoint
            .address
            .to_socket_addrs()?
            .next()
            .ok_or(Error::InvalidInput("peer address did not resolve"))?;
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
        self.enqueue(packet, Some(sender))?;
        Ok(receiver)
    }

    fn enqueue(
        &self,
        packet: Packet,
        #[cfg(any(test, feature = "qualification"))] completion: Option<
            tokio::sync::oneshot::Sender<WriteReceipt>,
        >,
    ) -> Result<()> {
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.check()?;
        if packet.from.address != self.address {
            return Err(Error::InvalidInput("packet source is not this listener"));
        }
        validate_operation_limits(&packet, &self.handle.network.settings)?;
        let route = self
            .handle
            .network
            .routes
            .lock()
            .unwrap()
            .get(&packet.to)
            .cloned()
            .ok_or(Error::InvalidInput("unregistered peer actor"))?;
        let data = is_data(&packet.body);
        let class = self.handle.network.class(data);
        let slot = class
            .outgoing_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceLimit("outgoing peer queue"))?;
        let message = packet.encode(true)?;
        #[cfg(any(test, feature = "qualification"))]
        let message_id = message.properties()?.id;
        let bytes = message.to_frame()?.encode()?;
        #[cfg(any(test, feature = "qualification"))]
        if completion.is_some() && bytes.len() > 16 * 1024 {
            return Err(Error::ResourceLimit("qualification frame bytes"));
        }
        let permit = class.reserve(bytes.len() + route.address.len() + 128)?;
        let outgoing = Outgoing {
            _address: self.handle.network.own_address(&route.address),
            route,
            bytes,
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
        {
            let _admission = self.admission.lock().unwrap();
            self.stopped.store(true, Ordering::Release);
            self.stop.send_replace(true);
        }
        // Keep the join lock until termination: concurrent shutdown callers
        // must not return merely because another caller took the handle.
        let mut thread = self.thread.lock().unwrap();
        if let Some(thread) = thread.take() {
            let _ = thread.join();
        }
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
                session.stop.cancel();
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
    stop: crate::Cancellation,
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
                        tokio::select! {
                            _ = stopped.cancelled() => {},
                            _ = run_session(socket, id, network, ready, data_rx, control_rx, true) => {},
                        }
                        id
                    });
                }
            }
        }
    }
    drop(listener);
    data.close();
    control.close();
    tasks.abort_all();
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
            stop: crate::Cancellation::default(),
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
    #[cfg(any(test, feature = "qualification"))]
    if outgoing
        .completion
        .as_ref()
        .is_some_and(|(_, sender)| sender.is_closed())
    {
        return;
    }
    let address = &outgoing.route.address;
    let id = match addresses.get(address).copied() {
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
            tasks.spawn(async move {
                tokio::select! {
                    _ = stopped.cancelled() => {},
                    _ = async {
                        if let Ok(Ok(stream)) = timeout(network.settings.callback_timeout, TcpStream::connect(socket)).await {
                            let _ = run_session(stream, id, network, ready, data_rx, control_rx, false).await;
                        }
                    } => {},
                };
                id
            });
            id
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

async fn run_session(
    stream: TcpStream,
    id: u64,
    network: Arc<Network>,
    ready: mpsc::Sender<Ready>,
    data: mpsc::Receiver<Outgoing>,
    control: mpsc::Receiver<Outgoing>,
    inbound: bool,
) -> Result<()> {
    let result =
        run_session_inner(stream, id, network.clone(), ready, data, control, inbound).await;
    #[cfg(any(test, feature = "qualification"))]
    if let Err(error) = &result {
        network.note("connection_error", format!("{id}: {error:?}"));
    }
    result
}

async fn run_session_inner(
    stream: TcpStream,
    id: u64,
    network: Arc<Network>,
    ready: mpsc::Sender<Ready>,
    mut data: mpsc::Receiver<Outgoing>,
    mut control: mpsc::Receiver<Outgoing>,
    inbound: bool,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let peer_socket = stream.peer_addr()?;
    #[cfg(any(test, feature = "qualification"))]
    let local = stream.local_addr()?;
    let (mut reader, mut writer) = stream.into_split();
    let deadline = network.settings.callback_timeout;
    // Native TcpConnection.cpp:126 uses zero on an initiating connection and a
    // new listen-side nonce on an accepted one. Transport instance zero is also
    // the native default (TcpDatagramTransport.h:251). Neither is replica proof.
    let local_instance = ListenInstance {
        nonce: if inbound {
            Uuid::new_v4().to_bytes_le()
        } else {
            [0; 16]
        },
        ..network.local.clone()
    };
    let instance = Message::listen_instance(
        MessageId {
            guid: Uuid::new_v4().to_bytes_le(),
            index: 1,
        },
        &local_instance,
    )?;
    timeout(deadline, write_message(&mut writer, &instance))
        .await
        .map_err(|_| Error::Timeout)??;
    let remote = timeout(deadline, read_frame(&mut reader, &network))
        .await
        .map_err(|_| Error::Timeout)??
        .ok_or(Error::Protocol("missing ListenInstance"))?;
    let properties = remote.message.properties()?;
    if properties.actor != 1
        || properties.id == MessageId::default()
        || !properties.action.is_empty()
        || remote
            .message
            .headers
            .iter()
            .filter(|h| h.id == ACTOR)
            .count()
            != 1
        || remote
            .message
            .headers
            .iter()
            .any(|h| matches!(h.id, IPC | 0x8029 | 0x8048))
        || remote
            .message
            .headers
            .iter()
            .filter(|h| h.id == MESSAGE_ID)
            .count()
            != 1
    {
        return Err(Error::Protocol("expected native peer ListenInstance"));
    }
    let remote_instance = ListenInstance::decode(&remote.message.body)?;
    if remote_instance.address.is_empty() {
        return Err(Error::Protocol("invalid peer ListenInstance"));
    }
    // Reuse endpoint validation without resolving peer-supplied hostnames.
    Endpoint {
        address: remote_instance.address.clone(),
        actor: Actor {
            partition: Uuid::new_v4(),
            replica: 1,
            incarnation: Uuid::new_v4(),
        },
        transport_id: None,
    }
    .to_string()
    .parse::<Endpoint>()?;
    drop(remote);
    let remote_listener = match remote_instance.address.parse::<SocketAddr>() {
        Ok(socket) => socket,
        Err(_) => {
            // The advertised hostname is still the exact wire identity. For
            // socket ownership use its listener port and the connected IP,
            // without a blocking DNS lookup on the network reader.
            let port = remote_instance
                .address
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .filter(|port| *port != 0)
                .ok_or(Error::Protocol("invalid peer listener port"))?;
            SocketAddr::new(peer_socket.ip(), port)
        }
    };
    let remote_address = remote_listener.to_string();
    let _socket_owner = network.own_socket_address(&remote_address);
    ready
        .try_send(Ready {
            id,
            address: remote_address.clone(),
        })
        .map_err(|_| Error::ResourceLimit("session announcements"))?;
    let read = async {
        loop {
            tokio::task::yield_now().await;
            let Some(frame) = read_frame(&mut reader, &network).await? else {
                continue;
            };
            let properties = frame.message.properties()?;
            if properties.actor != 0
                || properties.expects_reply
                || properties.relates_to != MessageId::default()
                || frame
                    .message
                    .headers
                    .iter()
                    .filter(|header| header.id == ACTOR)
                    .count()
                    > 1
                || frame.message.headers.iter().any(|header| header.id == IPC)
            {
                return Err(Error::Protocol("invalid classic peer envelope"));
            }
            let packet = Packet::decode(&frame.message)?;
            #[cfg(any(test, feature = "qualification"))]
            network.note(
                if is_data(&packet.body) {
                    "received_data"
                } else {
                    "received_control"
                },
                format!(
                    "{}:{}",
                    id,
                    match &packet.body {
                        Body::Replication { operations, .. } =>
                            operations.first().map_or(0, |o| o.sequence),
                        Body::Copy { sequence, .. } | Body::CopyContext { sequence, .. } =>
                            *sequence,
                        _ => 0,
                    }
                ),
            );
            if packet.from.address != remote_instance.address {
                return Err(Error::Protocol("packet source differs from ListenInstance"));
            }
            validate_operation_limits(&packet, &network.settings)?;
            {
                let mut routes = network.routes.lock().unwrap();
                let full = routes.len() >= network.settings.connections;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    routes.entry(packet.from.actor)
                {
                    if full {
                        return Err(Error::ResourceLimit("peer routes"));
                    }
                    entry.insert(Route {
                        address: remote_address.clone(),
                        socket: Some(remote_listener),
                        learned_on: Some(id),
                    });
                }
            }
            let data = is_data(&packet.body);
            let class = network.class(data);
            let charge = packet_bytes(&packet).max(frame.charge);
            // Replace the frame charge only after disposal of its decoded
            // message. Metadata (including empty segments) counts as bytes.
            drop(frame.message);
            let permit = if charge == frame.charge {
                frame.permit
            } else {
                drop(frame.permit);
                match class.reserve(charge) {
                    Ok(permit) => permit,
                    Err(_) if data => continue,
                    Err(error) => return Err(error),
                }
            };
            match class.deliver(packet, permit) {
                Ok(()) => {}
                Err(Error::ResourceLimit(_)) if data => {
                    #[cfg(any(test, feature = "qualification"))]
                    network.note("incoming_drop", "capacity".into());
                }
                Err(error) => return Err(error),
            }
        }
    };
    let write = async {
        loop {
            tokio::task::yield_now().await;
            let outgoing = tokio::select! {
                biased;
                Some(outgoing) = control.recv() => outgoing,
                Some(outgoing) = data.recv() => outgoing,
                else => return Ok(()),
            };
            #[cfg(any(test, feature = "qualification"))]
            if outgoing
                .completion
                .as_ref()
                .is_some_and(|(_, sender)| sender.is_closed())
            {
                continue;
            }
            timeout(deadline, writer.write_all(&outgoing.bytes))
                .await
                .map_err(|_| Error::Timeout)??;
            #[cfg(any(test, feature = "qualification"))]
            if outgoing.completion.is_some() {
                timeout(deadline, writer.flush())
                    .await
                    .map_err(|_| Error::Timeout)??;
            }
            #[cfg(any(test, feature = "qualification"))]
            network.note(
                if outgoing.data {
                    "written_data"
                } else {
                    "written_control"
                },
                id.to_string(),
            );
            #[cfg(any(test, feature = "qualification"))]
            if let Some((message_id, sender)) = outgoing.completion {
                let _ = sender.send(WriteReceipt {
                    session: id,
                    local,
                    remote: peer_socket,
                    remote_listener: remote_instance.address.clone(),
                    message_id,
                    frame: outgoing.bytes,
                    _bytes: outgoing._bytes,
                    _slot: outgoing._slot,
                });
            }
        }
    };
    let result = tokio::select! {
        result = read => result,
        result = write => result,
    };
    #[cfg(any(test, feature = "qualification"))]
    network.note("session_end", format!("{id}: {result:?}"));
    result
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

struct ReadFrame {
    message: Message,
    permit: OwnedSemaphorePermit,
    charge: usize,
}

/// Idle reads have no deadline. Once a frame starts, its entire remaining
/// header/body (including a discarded data body) has one bounded deadline.
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    network: &Network,
) -> Result<Option<ReadFrame>> {
    let mut fixed = [0; FIXED_HEADER_SIZE];
    reader.read_exact(&mut fixed[..1]).await?;
    timeout(network.settings.callback_timeout, async {
        reader.read_exact(&mut fixed[1..]).await?;
        let (size, header_size) = validate_fixed(&fixed)?;
        let mut prefix = Vec::with_capacity(FIXED_HEADER_SIZE + header_size);
        prefix.extend_from_slice(&fixed);
        prefix.resize(FIXED_HEADER_SIZE + header_size, 0);
        reader.read_exact(&mut prefix[FIXED_HEADER_SIZE..]).await?;
        let headers = Message::from_frame(Frame {
            headers: prefix[FIXED_HEADER_SIZE..].to_vec(),
            body: Vec::new(),
        })?;
        let properties = headers.properties()?;
        if !matches!(properties.actor, 0 | 1)
            || properties.expects_reply
            || properties.relates_to != MessageId::default()
            || headers
                .headers
                .iter()
                .filter(|header| header.id == ACTOR)
                .count()
                > 1
            || headers
                .headers
                .iter()
                .any(|header| matches!(header.id, IPC | 0x8029 | 0x8048))
        {
            return Err(Error::Protocol("invalid clear-TCP peer headers"));
        }
        let data = matches!(
            properties.action.as_str(),
            "ReplicationOperation" | "CopyOperation" | "CopyContextOperation"
        );
        let charge = size + std::mem::size_of::<Packet>();
        let permit = match network.class(data).reserve(charge) {
            Ok(permit) => permit,
            Err(_) if data => {
                let mut remaining = size - prefix.len();
                let mut scratch = [0; 8192];
                while remaining > 0 {
                    let chunk = remaining.min(scratch.len());
                    reader.read_exact(&mut scratch[..chunk]).await?;
                    remaining -= chunk;
                }
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut prefixed = prefix.as_slice().chain(reader);
        let message = read_message(&mut prefixed).await?;
        Ok(Some(ReadFrame {
            message,
            permit,
            charge,
        }))
    })
    .await
    .map_err(|_| Error::Timeout)?
}
