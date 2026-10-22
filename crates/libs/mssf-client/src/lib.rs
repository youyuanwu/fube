//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Connections support bounded concurrency, have no automatic reconnect, and must only be
//! used with an explicitly selected unsecured development gateway.

mod protocol;

use std::{net::SocketAddr, time::Duration};

use mssf_transport::{
    connection::{deadline_after, Connection},
    message::Header,
};
use tokio::time::Instant;
use uuid::Uuid;

pub use mssf_transport::{Error, Result};

/// Default connection and per-operation timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Identity returned by the native gateway, not by a management endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayDescription {
    pub address: String,
    pub node_id: u128,
    pub node_instance: u64,
    pub node_name: String,
}

/// Cloneable client for the native clear-TCP development profile.
///
/// Clones share one continuously driven connection and its terminal state.
#[derive(Clone)]
pub struct FabricClient {
    connection: Connection,
    timeout: Duration,
}

impl FabricClient {
    /// Connect explicitly without TLS or authentication.
    ///
    /// `timeout` also bounds each subsequent operation. There is no insecure
    /// fallback, automatic reconnect, or replay.
    pub async fn connect_clear(endpoint: SocketAddr, timeout: Duration) -> Result<Self> {
        let deadline = deadline_after(timeout)?;
        let connection = Connection::connect_clear(endpoint, deadline).await?;
        Ok(Self {
            connection,
            timeout,
        })
    }

    /// Discover the native gateway's full node identity.
    pub async fn ping(&self) -> Result<GatewayDescription> {
        let deadline = deadline_after(self.timeout)?;
        self.request("PingRequest", Vec::new(), deadline, |reply| {
            protocol::decode_ping(&reply)
        })
        .await
    }

    /// Test naming-store existence, independently of user-service state.
    ///
    /// Supported inputs are literal `fabric:/` followed by nonempty ASCII
    /// `[A-Za-z0-9_-]+` segments separated by single slashes, at most 4096 bytes.
    /// Root, authority, escapes, query, fragment, whitespace, Unicode, and
    /// trailing or repeated slashes are rejected rather than normalized.
    pub async fn name_exists(&self, name: &str) -> Result<bool> {
        let deadline = deadline_after(self.timeout)?;
        let body = protocol::encode_name(name)?;
        self.request("NameExistsRequest", body, deadline, |reply| {
            protocol::decode_name_exists(&reply)
        })
        .await
    }

    /// Close the shared connection and wait for the background driver to exit.
    /// Dropping one clone does not close the other handles.
    pub async fn close(&self) -> Result<()> {
        self.connection.close().await
    }

    pub fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    /// The first terminal connection cause, including failures detected idle.
    pub fn terminal_error(&self) -> Option<Error> {
        self.connection.terminal_error()
    }

    async fn request<T: Send + 'static>(
        &self,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
        decode: fn(mssf_transport::message::Message) -> Result<T>,
    ) -> Result<T> {
        let headers = vec![
            Header::activity(Uuid::new_v4().to_bytes_le())?,
            Header::version(1, 1)?,
        ];
        self.connection
            .request_decoded(action, 5, headers, body, deadline, decode)
            .await
    }
}
