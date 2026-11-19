use std::{future::Future, pin::Pin, time::Duration};

use crate::{
    connection::{ConnectionConfig, Ingress},
    wire::{frame::FrameWireContext, headers::HeaderDecodeContext},
    Error, Result,
};

mod channel;
mod io;
mod server;
pub(crate) mod session;

pub use channel::{Channel, Endpoint};
pub use io::{
    Accept, Accepted, Connected, IoInfo, StreamAcceptor, TcpAcceptor, TcpConnector, TransportIo,
};
pub use server::{Server, ServerBuilder};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Select managed IDs/correlation or caller-owned raw one-way IDs.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectionOptions {
    raw_only: bool,
}

impl ConnectionOptions {
    pub fn managed() -> Self {
        Self::default()
    }
    pub fn raw_only() -> Self {
        Self { raw_only: true }
    }

    pub(crate) fn is_managed(&self) -> bool {
        !self.raw_only
    }
}

/// Native wire limits and explicit Rust execution policies. No heap accounting.
#[derive(Debug, Clone)]
pub struct TransportLimits {
    pub max_receive_frame_bytes: usize,
    pub max_send_frame_bytes: Option<usize>,
    pub send_queue_bytes: usize,
    pub max_pending_sends: usize,
    pub max_pending_requests: usize,
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub max_peer_waiters: usize,
    pub work_bytes: usize,
    pub work_steps: usize,
    pub frame_timeout: Duration,
    pub discard_timeout: Duration,
    pub max_discard_bytes: usize,
    pub write_timeout: Duration,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_receive_frame_bytes: u32::MAX as usize,
            max_send_frame_bytes: Some(u32::MAX as usize),
            send_queue_bytes: 0,
            max_pending_sends: 1024,
            max_pending_requests: 1024,
            max_connections: 128,
            max_handshakes: 16,
            max_peer_waiters: 128,
            work_bytes: 64 * 1024,
            work_steps: 32,
            frame_timeout: Duration::from_secs(30),
            discard_timeout: Duration::from_secs(5),
            max_discard_bytes: u32::MAX as usize,
            write_timeout: Duration::from_secs(30),
        }
    }
}

impl TransportLimits {
    pub fn validate(&self) -> Result<()> {
        self.connection(Duration::from_secs(30))
            .validate()
            .map_err(Error::from)?;
        for value in [
            self.max_connections,
            self.max_handshakes,
            self.max_pending_requests,
        ] {
            if value == 0 || value > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(Error::InvalidInput("invalid connection/handshake count"));
            }
        }
        if self.max_peer_waiters > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(Error::InvalidInput("invalid peer waiter count"));
        }
        Ok(())
    }

    pub(crate) fn connection(&self, drain_timeout: Duration) -> ConnectionConfig {
        ConnectionConfig {
            wire: FrameWireContext::new(0, true, true).expect("clear wire profile"),
            headers: HeaderDecodeContext::native(),
            max_receive_frame_bytes: self.max_receive_frame_bytes,
            max_send_frame_bytes: self.max_send_frame_bytes,
            send_queue_bytes: self.send_queue_bytes,
            max_pending_sends: self.max_pending_sends,
            work_bytes: self.work_bytes,
            work_steps: self.work_steps,
            frame_timeout: self.frame_timeout,
            discard_timeout: self.discard_timeout,
            max_discard_bytes: self.max_discard_bytes,
            write_timeout: self.write_timeout,
            drain_timeout,
            ingress: Ingress::Raw,
        }
    }
}

pub(crate) fn validate_timeout(timeout: Duration) -> Result<()> {
    if timeout.is_zero() || std::time::Instant::now().checked_add(timeout).is_none() {
        Err(Error::InvalidInput(
            "timeout must be positive and representable",
        ))
    } else {
        Ok(())
    }
}
