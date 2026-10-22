//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Connections support bounded concurrency, have no automatic reconnect, and must only be
//! used with an explicitly selected unsecured development gateway.

mod applications;
mod health;
mod health_protocol;
mod list_protocol;
mod node_protocol;
mod nodes;
mod protocol;
mod query_protocol;
mod services;
pub use applications::*;
pub use health::*;
pub use nodes::*;
pub use services::*;

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

    /// Explicitly create a naming entry using the default timeout.
    ///
    /// Uses the same supported URI profile as [`Self::name_exists`]. Native
    /// creation may create implicit ancestors or promote an implicit entry;
    /// an already explicitly created entry returns a native error.
    ///
    /// No retry or replay occurs. After sending, timeout, cancellation or
    /// disconnection can leave the write outcome unknown: stopping local
    /// waiting does not prove that the name was not created.
    pub async fn create_name(&self, name: &str) -> Result<()> {
        self.create_name_with_timeout(name, self.timeout).await
    }

    /// Create a name with one deadline covering preparation through decode.
    ///
    /// See [`Self::create_name`] for native semantics and post-send uncertainty.
    pub async fn create_name_with_timeout(&self, name: &str, timeout: Duration) -> Result<()> {
        let deadline = deadline_after(timeout)?;
        let body = protocol::encode_name(name)?;
        self.request("CreateNameRequest", body, deadline, |reply| {
            protocol::decode_name_write(&reply)
        })
        .await
    }

    /// Explicitly delete one naming entry using the default timeout.
    ///
    /// Uses the same URI profile as [`Self::name_exists`]. This is not recursive:
    /// native missing-name and nonempty-name failures remain errors. The service
    /// may also remove empty implicit ancestors.
    ///
    /// No retry or replay occurs. After sending, timeout, cancellation or
    /// disconnection can leave the write outcome unknown; none proves that
    /// deletion did not execute. Native errors are not converted to success.
    pub async fn delete_name(&self, name: &str) -> Result<()> {
        self.delete_name_with_timeout(name, self.timeout).await
    }

    /// Delete a name with one deadline covering preparation through decode.
    ///
    /// See [`Self::delete_name`] for native semantics and post-send uncertainty.
    pub async fn delete_name_with_timeout(&self, name: &str, timeout: Duration) -> Result<()> {
        let deadline = deadline_after(timeout)?;
        let body = protocol::encode_name(name)?;
        let headers = vec![Header::boolean(protocol::DELETE_NAME_HEADER, true)?];
        self.request_with_headers("DeleteNameRequest", headers, body, deadline, |reply| {
            protocol::decode_name_write(&reply)
        })
        .await
    }

    /// Get one native node page. Inspect `continuation_token` even without an
    /// explicit page limit: the server can also page by message size.
    ///
    /// This is best-effort native aggregation, not a membership snapshot.
    pub async fn get_node_list(&self, query: &NodeQueryDescription) -> Result<NodeListPage> {
        self.get_node_list_with_timeout(query, self.timeout).await
    }

    /// Get one page using one deadline covering encoding, admission and reply.
    /// No automatic paging, retry, reconnect or replay occurs.
    pub async fn get_node_list_with_timeout(
        &self,
        query: &NodeQueryDescription,
        timeout: Duration,
    ) -> Result<NodeListPage> {
        let deadline = deadline_after(timeout)?;
        let body = node_protocol::encode_query(query)?;
        self.request("QueryRequest", body, deadline, |reply| {
            node_protocol::decode_reply(&reply)
        })
        .await
    }

    /// Query actual single-node health, events and unhealthy explanations.
    /// This is not GetNodeList's summary. No paging or automatic retries occur.
    pub async fn get_node_health(&self, query: &NodeHealthQueryDescription) -> Result<NodeHealth> {
        self.get_node_health_with_timeout(query, self.timeout).await
    }

    /// Query node health with one deadline covering preparation through decode.
    /// Native errors are operation-local; malformed successful responses retire
    /// the shared connection. See [`NodeHealthQueryDescription`] for defaults.
    pub async fn get_node_health_with_timeout(
        &self,
        query: &NodeHealthQueryDescription,
        timeout: Duration,
    ) -> Result<NodeHealth> {
        let deadline = deadline_after(timeout)?;
        let body = health_protocol::encode_query(query)?;
        self.request("QueryRequest", body, deadline, |reply| {
            health_protocol::decode_reply(&reply)
        })
        .await
    }

    /// Get one application page using the configured default deadline.
    /// Filters are mutually exclusive; parameters are included by default.
    pub async fn get_application_list(
        &self,
        query: &ApplicationQueryDescription,
    ) -> Result<ApplicationListPage> {
        self.get_application_list_with_timeout(query, self.timeout)
            .await
    }

    /// Get one native application page with an explicit whole-operation timeout.
    /// No retries or automatic traversal. Malformed successes retire the stream;
    /// normal native errors remain operation-local.
    pub async fn get_application_list_with_timeout(
        &self,
        query: &ApplicationQueryDescription,
        timeout: Duration,
    ) -> Result<ApplicationListPage> {
        let deadline = deadline_after(timeout)?;
        let body = list_protocol::encode_application(query)?;
        self.request("QueryRequest", body, deadline, |reply| {
            list_protocol::decode_applications(&reply)
        })
        .await
    }

    /// Get one ordinary application-scoped service page with the default timeout.
    /// Ad-hoc/root and System profiles are explicitly unsupported.
    pub async fn get_service_list(
        &self,
        query: &ServiceQueryDescription,
    ) -> Result<ServiceListPage> {
        self.get_service_list_with_timeout(query, self.timeout)
            .await
    }

    /// Get one service page with an explicit whole-operation timeout.
    /// Count is a native per-subquery limit, not a strict CM/FM union cap.
    pub async fn get_service_list_with_timeout(
        &self,
        query: &ServiceQueryDescription,
        timeout: Duration,
    ) -> Result<ServiceListPage> {
        let deadline = deadline_after(timeout)?;
        let body = list_protocol::encode_service(query)?;
        self.request("QueryRequest", body, deadline, |reply| {
            list_protocol::decode_services(&reply)
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
        self.request_with_headers(action, Vec::new(), body, deadline, decode)
            .await
    }

    async fn request_with_headers<T: Send + 'static>(
        &self,
        action: &str,
        mut headers: Vec<Header>,
        body: Vec<u8>,
        deadline: Instant,
        decode: fn(mssf_transport::message::Message) -> Result<T>,
    ) -> Result<T> {
        headers.push(Header::activity(Uuid::new_v4().to_bytes_le())?);
        headers.push(Header::version(1, 1)?);
        self.connection
            .request_decoded(action, 5, headers, body, deadline, decode)
            .await
    }
}
