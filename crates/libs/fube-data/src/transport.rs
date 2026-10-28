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
    collections::{HashMap, VecDeque},
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
        let network = Arc::new(Network {
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
                nonce: Uuid::new_v4().to_bytes_le(),
            },
            settings,
            routes: Mutex::new(HashMap::new()),
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
                address: endpoint.address.clone(),
                socket: Some(socket),
                learned_on: None,
            },
        );
        Ok(())
    }

    /// Removes a membership route. A duplex socket may serve other actors and
    /// is therefore not closed by this operation; actor fencing is external.
    pub fn unregister_peer(&self, actor: &Actor) -> Result<()> {
        let _admission = self.handle.admission.lock().unwrap();
        self.handle.check()?;
        self.handle.network.routes.lock().unwrap().remove(actor);
        Ok(())
    }

    /// Nonblocking bounded admission only. A successful socket write, failed
    /// session or dropped data packet never fabricates a replication receipt.
    pub fn send(&self, packet: Packet) -> Result<()> {
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
        let bytes = packet.encode(true)?.to_frame()?.encode()?;
        let permit = class.reserve(bytes.len() + route.address.len() + 128)?;
        let outgoing = Outgoing {
            route,
            bytes,
            _bytes: permit,
            _slot: slot,
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
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(id) = result {
                    sessions.remove(&id);
                    addresses.retain(|_, owner| *owner != id);
                    network.routes.lock().unwrap()
                        .retain(|_, route| route.learned_on != Some(id));
                }
            }
            Some(Ready { id, address }) = ready_rx.recv() => {
                if sessions.contains_key(&id) {
                    addresses.entry(address).or_insert(id);
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
                    sessions.insert(id, session);
                    let network = network.clone();
                    let ready = ready.clone();
                    tasks.spawn(async move {
                        let _ = run_session(socket, id, network, ready, data_rx, control_rx).await;
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
    (Session { data, control }, data_rx, control_rx)
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
            let (session, data_rx, control_rx) = session_channels(&network.settings);
            sessions.insert(id, session);
            addresses.insert(address.clone(), id);
            let ready = ready.clone();
            let network = network.clone();
            tasks.spawn(async move {
                if let Ok(Ok(stream)) = timeout(
                    network.settings.callback_timeout,
                    TcpStream::connect(socket),
                )
                .await
                {
                    let _ = run_session(stream, id, network, ready, data_rx, control_rx).await;
                }
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
    mut data: mpsc::Receiver<Outgoing>,
    mut control: mpsc::Receiver<Outgoing>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();
    let deadline = network.settings.callback_timeout;
    let instance = Message::listen_instance(
        MessageId {
            guid: Uuid::new_v4().to_bytes_le(),
            index: 1,
        },
        &network.local,
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
    if remote_instance.instance == 0
        || remote_instance.nonce == [0; 16]
        || remote_instance.address.is_empty()
    {
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
    ready
        .try_send(Ready {
            id,
            address: remote_instance.address.clone(),
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
                        address: remote_instance.address.clone(),
                        socket: remote_instance.address.parse().ok(),
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
                Err(Error::ResourceLimit(_)) if data => {}
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
            timeout(deadline, writer.write_all(&outgoing.bytes))
                .await
                .map_err(|_| Error::Timeout)??;
        }
    };
    tokio::select! {
        result = read => result,
        result = write => result,
    }
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
