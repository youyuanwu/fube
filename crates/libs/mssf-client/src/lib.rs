//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Connections are sequential, have no automatic reconnect, and must only be
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

/// A mutable, single-flight client for the native clear-TCP development profile.
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
    pub async fn ping(&mut self) -> Result<GatewayDescription> {
        let deadline = deadline_after(self.timeout)?;
        let reply = self.request("PingRequest", Vec::new(), deadline).await?;
        self.complete(protocol::decode_ping(&reply), deadline)
    }

    /// Test naming-store existence, independently of user-service state.
    ///
    /// Supported inputs are literal `fabric:/` followed by nonempty ASCII
    /// `[A-Za-z0-9_-]+` segments separated by single slashes, at most 4096 bytes.
    /// Root, authority, escapes, query, fragment, whitespace, Unicode, and
    /// trailing or repeated slashes are rejected rather than normalized.
    pub async fn name_exists(&mut self, name: &str) -> Result<bool> {
        let deadline = deadline_after(self.timeout)?;
        let body = protocol::encode_name(name)?;
        let reply = self.request("NameExistsRequest", body, deadline).await?;
        self.complete(protocol::decode_name_exists(&reply), deadline)
    }

    /// Release the connection. No background tasks are owned by the client.
    pub async fn close(&mut self) -> Result<()> {
        self.connection.close().await
    }

    pub fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    async fn request(
        &mut self,
        action: &str,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<mssf_transport::message::Message> {
        let headers = vec![
            Header::activity(Uuid::new_v4().to_bytes_le())?,
            Header::version(1, 1)?,
        ];
        self.connection
            .request(action, 5, headers, body, deadline)
            .await
    }

    fn complete<T>(&mut self, result: Result<T>, deadline: Instant) -> Result<T> {
        if Instant::now() >= deadline {
            self.connection.abort();
            return Err(Error::Timeout);
        }
        if matches!(&result, Err(Error::Protocol(_))) {
            self.connection.abort();
        }
        result
    }
}
