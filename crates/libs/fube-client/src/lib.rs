//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Connections support bounded concurrency, have no automatic reconnect, and must only be
//! used with an explicitly selected unsecured development gateway.

mod applications;
mod channel_adapter;
pub mod contracts;
mod deployed_applications;
mod deployed_code_packages;
mod deployed_replicas;
mod deployed_service_packages;
mod health;
mod health_protocol;
mod image_store;
mod image_store_protocol;
mod list_protocol;
mod movement;
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
mod typed;
pub use applications::*;
pub use contracts::{CallContext, Client, ClientBinding, ClientError, ClientRequest};
pub use deployed_applications::*;
pub use deployed_code_packages::*;
pub use deployed_replicas::*;
pub use deployed_service_packages::*;
pub use health::*;
pub use image_store::ImageStoreClient;
pub use naming_uri::NamingUri;
pub use nodes::*;
pub use notifications::*;
pub use partitions::*;
pub use query::*;
pub use replicas::*;
pub use resolution::*;
pub use services::*;
pub use typed::*;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use contracts::ClientFuture;
use fube_transport::{
    channel::MessageChannel,
    connection::deadline_after,
    native_channel::{ChannelLimits, GatewayChannel, GatewayTarget},
};
use tokio::time::Instant;
use uuid::Uuid;

pub use fube_transport::{Error, Result};

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
/// Clones explicitly share one GatewayChannel lifecycle owner and terminal state.
/// A [`GatewayClient`] instead borrows a supplied foundation without adopting it.
/// Eight admitted typed calls are retained through result transfer/drop; excess
/// calls fail immediately. Protocol/domain failures do not abort the owner.
/// Every remote operation accepts an optional whole-operation timeout:
/// `None` uses the timeout configured by [`Self::connect_clear`], while
/// `Some(duration)` overrides it for that call. `None` is not an infinite timeout.
///
/// ```no_run
/// use std::time::Duration;
/// use fube_client::{FabricClient, DEFAULT_TIMEOUT};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = FabricClient::connect_clear("127.0.0.1:19000".parse().unwrap(), DEFAULT_TIMEOUT).await?;
/// let gateway = client.ping(None).await?;
/// let exists = client.name_exists("fabric:/MyApplication", Some(Duration::from_secs(5))).await?;
/// client.close().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FabricClient {
    channel: GatewayChannel,
    target: Arc<GatewayTarget>,
    budget: typed::ClientBudget,
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
    ) -> ClientResult<ResolvedServicePartition> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        self.call(
            ResolutionRequest::new(name, key, previous).map_err(ClientError::Local)?,
            CallContext { deadline },
        )
        .await
    }

    /// Connect explicitly without TLS or authentication.
    ///
    /// `timeout` also supplies the default for subsequent operations that pass
    /// `None`. There is no insecure fallback, automatic reconnect, or replay.
    /// Only numeric nonzero-port loopback endpoints are allowed. Notifications
    /// are disabled on this owner; notification clients own separate opted-in
    /// GatewayChannels; image-transfer clients own separate upload channels.
    pub async fn connect_clear(endpoint: SocketAddr, timeout: Duration) -> Result<Self> {
        let deadline = deadline_after(timeout)?;
        let channel = GatewayChannel::connect_unsecured_loopback(
            endpoint,
            channel_adapter::ordinary()?,
            ChannelLimits::default(),
            deadline,
        )
        .await?;
        let target = channel.resolve_target(&endpoint.to_string())?;
        typed::check_deadline(deadline)?;
        Ok(Self {
            channel,
            target,
            budget: typed::default_budget(),
            timeout,
        })
    }

    /// Discover the native gateway's full node identity.
    pub async fn ping(&self, timeout: Option<Duration>) -> ClientResult<GatewayDescription> {
        self.operation(PingRequest, timeout).await
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
    ) -> ClientResult<bool> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        let request = self.name_request(name.as_ref(), NameOperation::Exists, deadline)?;
        drop(name);
        match self.call(request, CallContext { deadline }).await? {
            NameResponse::Exists(exists) => Ok(exists),
            _ => Err(ClientError::Protocol("unexpected naming result")),
        }
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
    ) -> ClientResult<()> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        let request = self.name_request(name.as_ref(), NameOperation::Create, deadline)?;
        drop(name);
        self.call(request, CallContext { deadline })
            .await
            .map(|_| ())
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
    ) -> ClientResult<()> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        let request = self.name_request(name.as_ref(), NameOperation::Delete, deadline)?;
        drop(name);
        self.call(request, CallContext { deadline })
            .await
            .map(|_| ())
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
    ) -> ClientResult<NodeListPage> {
        self.query(query, timeout).await
    }

    /// Query actual single-node health, events and unhealthy explanations.
    /// This is not GetNodeList's summary. No paging or automatic retries occur.
    ///
    /// Native and malformed domain replies are operation-local; transport
    /// envelope failures can terminate the channel. See [`NodeHealthQueryDescription`].
    pub async fn get_node_health(
        &self,
        query: &NodeHealthQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<NodeHealth> {
        self.query(query, timeout).await
    }

    /// Get one native application page with one whole-operation deadline.
    /// Filters are mutually exclusive; parameters are included by default.
    ///
    /// No retries or automatic traversal. Domain validation errors are local to
    /// this call; the channel independently owns transport failure policy.
    pub async fn get_application_list(
        &self,
        query: &ApplicationQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<ApplicationListPage> {
        self.query(query, timeout).await
    }

    /// Get one ordinary application-scoped service page with one whole-operation deadline.
    /// Ad-hoc/root and System profiles are explicitly unsupported.
    ///
    /// Count is a native per-subquery limit, not a strict CM/FM union cap.
    pub async fn get_service_list(
        &self,
        query: &ServiceQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<ServiceListPage> {
        self.query(query, timeout).await
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
    ) -> ClientResult<PartitionListPage> {
        self.query(query, timeout).await
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
    ) -> ClientResult<ReplicaListPage> {
        self.query(query, timeout).await
    }

    /// Get one deployed application page on a selected node.
    /// This is node-local deployment state, not the cluster-wide application list.
    ///
    /// No automatic paging, retries, reconnect or replay occurs.
    pub async fn get_deployed_application_list(
        &self,
        query: &DeployedApplicationQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<DeployedApplicationListPage> {
        self.query(query, timeout).await
    }

    /// List service packages deployed for one application on a selected node.
    /// This native query is unpaged and does not provide a cluster snapshot.
    ///
    /// No automatic retries, reconnect or replay occurs.
    pub async fn get_deployed_service_package_list(
        &self,
        query: &DeployedServicePackageQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<Vec<DeployedServicePackage>> {
        self.query(query, timeout).await
    }

    /// List code packages deployed for one application on a selected node.
    /// Includes setup/main entry points, statistics and host details; no paging.
    ///
    /// Native errors and malformed domain successes remain operation-local.
    pub async fn get_deployed_code_package_list(
        &self,
        query: &DeployedCodePackageQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<Vec<DeployedCodePackage>> {
        self.query(query, timeout).await
    }

    /// List replicas or stateless instances deployed on one node for an application.
    /// Includes activation, host and reconfiguration details; not endpoint resolution.
    ///
    /// No paging, automatic retries, reconnect or replay occurs.
    pub async fn get_deployed_replica_list(
        &self,
        query: &DeployedReplicaQueryDescription,
        timeout: Option<Duration>,
    ) -> ClientResult<Vec<DeployedReplica>> {
        self.query(query, timeout).await
    }

    /// Close the shared connection and wait for the background driver to exit.
    /// Dropping one clone does not close the other handles.
    pub async fn close(&self) -> Result<()> {
        self.channel.close(self.deadline(None)?).await
    }

    pub fn is_closed(&self) -> bool {
        self.channel.terminal_error().is_some()
    }

    /// The first terminal connection cause, including failures detected idle.
    pub fn terminal_error(&self) -> Option<Error> {
        self.channel.terminal_error()
    }

    /// Ask native FM to move a partition's Primary to an explicit node while
    /// enforcing placement constraints. There is deliberately no force/ignore
    /// option. Observe placement/discovery separately; this is not an atomic
    /// cluster snapshot or an application-side membership authority.
    pub async fn move_primary(
        &self,
        partition: Uuid,
        target_node: &str,
        timeout: Option<Duration>,
    ) -> ClientResult<()> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        query_arguments::validate_node_name(
            target_node,
            "primary target must be an explicit native node",
        )
        .map_err(ClientError::Local)?;
        self.call(
            QueryDescription::new(movement::MovePrimary {
                partition,
                target: target_node.to_owned(),
            }),
            CallContext { deadline },
        )
        .await
    }

    fn deadline(&self, timeout: Option<Duration>) -> Result<Instant> {
        deadline_after(timeout.unwrap_or(self.timeout))
    }

    async fn query<Q: ClientRequest<DomainError = DomainFailure> + Clone>(
        &self,
        query: &Q,
        timeout: Option<Duration>,
    ) -> ClientResult<Q::Response> {
        let deadline = self.deadline(timeout).map_err(ClientError::Local)?;
        typed::check_input(query).map_err(ClientError::Local)?;
        self.call(query.clone(), CallContext { deadline }).await
    }

    async fn operation<R: ClientRequest<DomainError = DomainFailure>>(
        &self,
        request: R,
        timeout: Option<Duration>,
    ) -> ClientResult<R::Response> {
        self.call(
            request,
            CallContext {
                deadline: self.deadline(timeout).map_err(ClientError::Local)?,
            },
        )
        .await
    }

    fn name_request(
        &self,
        name: &str,
        operation: NameOperation,
        deadline: Instant,
    ) -> ClientResult<NameRequest> {
        typed::check_deadline(deadline).map_err(ClientError::Local)?;
        let name = NamingUri::parse(name).map_err(ClientError::Local)?;
        Ok(NameRequest { name, operation })
    }
}

impl Client for FabricClient {
    type Channel = GatewayChannel;
    fn binding(&self) -> ClientBinding<'_, GatewayChannel> {
        ClientBinding {
            channel: &self.channel,
            target: &self.target,
        }
    }
    fn call<'a, R: ClientRequest>(
        &'a self,
        request: R,
        context: CallContext,
    ) -> ClientFuture<'a, R::Response, R::DomainError> {
        typed::call(self.binding(), &self.budget, request, context)
    }
}
