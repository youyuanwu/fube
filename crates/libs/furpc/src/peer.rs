use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};

use tokio::time::Instant;

use crate::{
    connection::{ConnectionHandle, ConnectionObserver, ListenInstance, Phase, SendReceipt},
    service::Registry,
    transport::IoInfo,
    Error, Headers, Message, Operation, Result, WriteReceipt,
};

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub generation: u64,
    pub io: IoInfo,
    pub listen_address: Arc<[u16]>,
    pub listen_instance: u64,
}

#[derive(Debug, Clone)]
pub struct Source {
    info: PeerInfo,
}
impl Source {
    pub fn generation(&self) -> u64 {
        self.info.generation
    }
    pub fn info(&self) -> &PeerInfo {
        &self.info
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    OneWay,
    Request,
    Notification,
}
pub type SendCheck = fn(SendKind, &Headers, &Source) -> Result<()>;

#[derive(Debug, Clone, Copy)]
pub struct SendPolicy {
    check: SendCheck,
}
impl SendPolicy {
    pub fn new(check: SendCheck) -> Self {
        Self { check }
    }
}

pub(crate) struct Generation {
    pub(crate) handle: ConnectionHandle,
    pub(crate) source: Source,
    pub(crate) registry: Arc<Registry>,
}
impl Generation {
    pub(crate) fn new(
        handle: ConnectionHandle,
        io: IoInfo,
        remote: ListenInstance,
        registry: Arc<Registry>,
    ) -> Result<Arc<Self>> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let generation = NEXT
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| Error::ResourceLimit("connection generation space"))?;
        let core = Arc::new(Self {
            handle,
            source: Source {
                info: PeerInfo {
                    generation,
                    io,
                    listen_address: remote.address.into(),
                    listen_instance: remote.instance,
                },
            },
            registry,
        });
        core.registry.track(&core)?;
        Ok(core)
    }
    pub(crate) fn peer(self: &Arc<Self>, policy: Option<SendPolicy>) -> Peer {
        Peer {
            generation: Arc::downgrade(self),
            connection: self.handle.observer(),
            source: self.source.clone(),
            policy,
        }
    }
    pub(crate) fn is_open(&self) -> bool {
        !self.registry.is_stopping() && self.handle.phase() == Phase::Open
    }
}

/// Nonowning authority for one original generation. It never reconnects.
#[derive(Clone)]
pub struct Peer {
    generation: Weak<Generation>,
    connection: ConnectionObserver,
    source: Source,
    policy: Option<SendPolicy>,
}
impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("generation", &self.source.generation())
            .finish_non_exhaustive()
    }
}
impl Peer {
    pub fn info(&self) -> PeerInfo {
        self.source.info.clone()
    }
    pub fn send(&self, message: Message, deadline: Instant) -> Operation<WriteReceipt> {
        Operation::send(self.clone(), message, deadline)
    }
    pub(crate) fn submit(&self, message: Message, deadline: Instant) -> Result<SendReceipt> {
        if let Some(error) = self.connection.error() {
            return Err(error.into());
        }
        let core = self.generation.upgrade().ok_or(Error::Closed)?;
        if !core.is_open() {
            return Err(Error::Closed);
        }
        let common = message.headers().effective();
        if common.expects_reply
            || common.uncorrelated_reply
            || common.relates_to != crate::MessageId::default()
        {
            return Err(Error::Unsupported(
                "raw-only one-way sends cannot originate replies or RPC",
            ));
        }
        let authorization = SendAuthorization {
            headers: message.headers().clone(),
            source: self.source.clone(),
            policy: self.policy,
            registry: Arc::downgrade(&core.registry),
        };
        core.handle
            .send_authorized(message, deadline, Some(authorization))
            .map_err(Error::from)
    }
}

pub(crate) struct SendAuthorization {
    headers: Headers,
    source: Source,
    policy: Option<SendPolicy>,
    registry: Weak<Registry>,
}
impl SendAuthorization {
    pub(crate) fn check(&self) -> Result<()> {
        let registry = self.registry.upgrade().ok_or(Error::Closed)?;
        if registry.is_stopping() {
            return Err(Error::Closed);
        }
        if let Some(policy) = self.policy {
            (policy.check)(SendKind::OneWay, &self.headers, &self.source)?;
        }
        Ok(())
    }
}
