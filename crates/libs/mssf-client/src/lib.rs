//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Connections support bounded concurrency, have no automatic reconnect, and must only be
//! used with an explicitly selected unsecured development gateway.

mod applications;
mod deployed_applications;
mod deployed_code_packages;
mod deployed_replicas;
mod deployed_service_packages;
mod health;
mod health_protocol;
mod list_protocol;
mod naming_uri;
mod native;
mod node_protocol;
mod nodes;
mod notification_protocol;
mod notification_state;
mod notifications;
mod partitions;
mod protocol;
mod query;
mod query_arguments;
mod query_protocol;
mod replicas;
mod resolution;
mod resolution_protocol;
mod services;
pub use applications::*;
pub use deployed_applications::*;
pub use deployed_code_packages::*;
pub use deployed_replicas::*;
pub use deployed_service_packages::*;
pub use health::*;
pub use naming_uri::NamingUri;
pub use nodes::*;
pub use notifications::*;
pub use partitions::*;
pub use query::*;
pub use replicas::*;
pub use resolution::*;
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
/// Every remote operation accepts an optional whole-operation timeout:
/// `None` uses the timeout configured by [`Self::connect_clear`], while
/// `Some(duration)` overrides it for that call. `None` is not an infinite timeout.
///
/// ```no_run
/// use std::time::Duration;
/// use mssf_client::{FabricClient, DEFAULT_TIMEOUT};
///
/// # async fn example() -> mssf_client::Result<()> {
/// let client = FabricClient::connect_clear("127.0.0.1:19000".parse().unwrap(), DEFAULT_TIMEOUT).await?;
/// let gateway = client.ping(None).await?;
/// let exists = client.name_exists("fabric:/MyApplication", Some(Duration::from_secs(5))).await?;
/// client.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FabricClient {
    connection: Connection,
    timeout: Duration,
}

impl FabricClient {
    /// Resolve on the gateway without retaining endpoint results in this client.
    pub async fn resolve_service_partition(
        &self,
        name: &NamingUri,
        key: &PartitionKey,
        previous: Option<&ResolvedServicePartition>,
        timeout: Option<Duration>,
    ) -> Result<ResolvedServicePartition> {
        let deadline = self.deadline(timeout)?;
        let body = resolution_protocol::encode(name, key, previous)?;
        let result = self
            .request(
                "ResolveServiceRequest",
                body,
                deadline,
                resolution_protocol::decode,
            )
            .await?;
        if &result.service_name != name || !key.selects(&result.partition_information) {
            self.connection.abort();
            return Err(Error::Protocol(
                "resolution result does not match requested selection",
            ));
        }
        Ok(result)
    }

    /// Connect explicitly without TLS or authentication.
    ///
    /// `timeout` also supplies the default for subsequent operations that pass
    /// `None`. There is no insecure fallback, automatic reconnect, or replay.
    pub async fn connect_clear(endpoint: SocketAddr, timeout: Duration) -> Result<Self> {
        let deadline = deadline_after(timeout)?;
        let connection = Connection::connect_clear(endpoint, deadline).await?;
        Ok(Self {
            connection,
            timeout,
        })
    }

    /// Discover the native gateway's full node identity.
    pub async fn ping(&self, timeout: Option<Duration>) -> Result<GatewayDescription> {
        let deadline = self.deadline(timeout)?;
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
    /// Accepts a [`NamingUri`] or a string checked before sending.
    pub async fn name_exists(
        &self,
        name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<bool> {
        let deadline = self.deadline(timeout)?;
        let body = protocol::encode_name(name.as_ref())?;
        self.request("NameExistsRequest", body, deadline, |reply| {
            protocol::decode_name_exists(&reply)
        })
        .await
    }

    /// Explicitly create a naming entry with one deadline from preparation through decode.
    ///
    /// Uses the same supported URI profile as [`Self::name_exists`]. Native
    /// creation may create implicit ancestors or promote an implicit entry;
    /// an already explicitly created entry returns a native error.
    ///
    /// No retry or replay occurs. After sending, timeout, cancellation or
    /// disconnection can leave the write outcome unknown: stopping local
    /// waiting does not prove that the name was not created.
    pub async fn create_name(
        &self,
        name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let deadline = self.deadline(timeout)?;
        let body = protocol::encode_name(name.as_ref())?;
        self.request("CreateNameRequest", body, deadline, |reply| {
            protocol::decode_name_write(&reply)
        })
        .await
    }

    /// Explicitly delete one naming entry with one deadline from preparation through decode.
    ///
    /// Uses the same URI profile as [`Self::name_exists`]. This is not recursive:
    /// native missing-name and nonempty-name failures remain errors. The service
    /// may also remove empty implicit ancestors.
    ///
    /// No retry or replay occurs. After sending, timeout, cancellation or
    /// disconnection can leave the write outcome unknown; none proves that
    /// deletion did not execute. Native errors are not converted to success.
    pub async fn delete_name(
        &self,
        name: impl AsRef<str>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let deadline = self.deadline(timeout)?;
        let body = protocol::encode_name(name.as_ref())?;
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
    /// No automatic paging, retry, reconnect or replay occurs.
    pub async fn get_node_list(
        &self,
        query: &NodeQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<NodeListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// Query actual single-node health, events and unhealthy explanations.
    /// This is not GetNodeList's summary. No paging or automatic retries occur.
    ///
    /// Native errors are operation-local; malformed successful responses retire
    /// the shared connection. See [`NodeHealthQueryDescription`] for defaults.
    pub async fn get_node_health(
        &self,
        query: &NodeHealthQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<NodeHealth> {
        self.query(&query.parameters, None, timeout).await
    }

    /// Get one native application page with one whole-operation deadline.
    /// Filters are mutually exclusive; parameters are included by default.
    ///
    /// No retries or automatic traversal. Malformed successes retire the stream;
    /// normal native errors remain operation-local.
    pub async fn get_application_list(
        &self,
        query: &ApplicationQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<ApplicationListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// Get one ordinary application-scoped service page with one whole-operation deadline.
    /// Ad-hoc/root and System profiles are explicitly unsupported.
    ///
    /// Count is a native per-subquery limit, not a strict CM/FM union cap.
    pub async fn get_service_list(
        &self,
        query: &ServiceQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<ServiceListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// Get one partition page for a service with one whole-operation deadline.
    /// Results describe observed partition state, not service endpoint resolution.
    ///
    /// Native partition paging supports continuation tokens, not MaxResults.
    /// No automatic paging, retries, reconnect or replay occurs.
    pub async fn get_partition_list(
        &self,
        query: &PartitionQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<PartitionListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// Get one replica or stateless instance page for a partition.
    /// Callers inspect and follow continuation tokens within their own overall budget.
    ///
    /// Native replica paging supports continuation tokens, not MaxResults.
    /// No automatic paging, retries, reconnect or replay occurs.
    pub async fn get_replica_list(
        &self,
        query: &ReplicaQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<ReplicaListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// Get one deployed application page on a selected node.
    /// This is node-local deployment state, not the cluster-wide application list.
    ///
    /// No automatic paging, retries, reconnect or replay occurs.
    pub async fn get_deployed_application_list(
        &self,
        query: &DeployedApplicationQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<DeployedApplicationListPage> {
        self.query(&query.parameters, Some(&query.paging), timeout)
            .await
    }

    /// List service packages deployed for one application on a selected node.
    /// This native query is unpaged and does not provide a cluster snapshot.
    ///
    /// No automatic retries, reconnect or replay occurs.
    pub async fn get_deployed_service_package_list(
        &self,
        query: &DeployedServicePackageQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<Vec<DeployedServicePackage>> {
        self.query(&query.parameters, None, timeout).await
    }

    /// List code packages deployed for one application on a selected node.
    /// Includes setup/main entry points, statistics and host details; no paging.
    ///
    /// Native errors remain operation-local; malformed successes retire the stream.
    pub async fn get_deployed_code_package_list(
        &self,
        query: &DeployedCodePackageQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<Vec<DeployedCodePackage>> {
        self.query(&query.parameters, None, timeout).await
    }

    /// List replicas or stateless instances deployed on one node for an application.
    /// Includes activation, host and reconfiguration details; not endpoint resolution.
    ///
    /// No paging, automatic retries, reconnect or replay occurs.
    pub async fn get_deployed_replica_list(
        &self,
        query: &DeployedReplicaQueryDescription,
        timeout: Option<Duration>,
    ) -> Result<Vec<DeployedReplica>> {
        self.query(&query.parameters, None, timeout).await
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

    fn deadline(&self, timeout: Option<Duration>) -> Result<Instant> {
        deadline_after(timeout.unwrap_or(self.timeout))
    }

    async fn query<Q: query_protocol::Query>(
        &self,
        parameters: &Q,
        paging: Option<&Paging>,
        timeout: Option<Duration>,
    ) -> Result<Q::Output> {
        let deadline = self.deadline(timeout)?;
        let body = query_protocol::encode_query(parameters, paging)?;
        self.request("QueryRequest", body, deadline, |reply| {
            query_protocol::decode_reply::<Q>(&reply)
        })
        .await
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
