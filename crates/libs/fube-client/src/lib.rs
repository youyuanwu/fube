//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! Existing [`FabricClient`] connections support bounded concurrency without
//! automatic reconnect. The separate [`FurpcClient`] is a typed stub over a
//! byte-facing Tower service, defaulting to furpc's Channel, and provides Ping,
//! naming operations, cluster/topology/deployment lists, node health and MovePrimary.
//! These operations are no longer available on FabricClient or its ClientRequest
//! adapters. FabricClient retains the remaining, unmigrated operations.
//! Native connections require an explicitly selected unsecured development gateway.

mod applications;
mod channel_adapter;
pub mod contracts;
mod deployed_applications;
mod deployed_code_packages;
mod deployed_replicas;
mod deployed_service_packages;
mod furpc_client;
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
pub use furpc_client::{FurpcClient, FurpcClientError};
pub use health::*;
pub use image_store::{ImageStoreClient, ImageStoreClientOptions};
pub use movement::{MovePrimaryQueryDescription, MovePrimaryQueryParameters};
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
    channel::MessageChannel, connection::deadline_after, native_channel::GatewayTarget,
};
use tokio::time::Instant;

pub use fube_transport::{Channel, ChannelLimits, Endpoint, Error, Result};

/// Default connection and per-operation timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Local typed-stub defaults, independent of connection settings.
///
/// Limits are conservative counters, not allocated memory. Stub clones share
/// these limits; explicitly constructing another stub creates a fresh budget.
#[derive(Debug, Clone, Copy)]
pub struct ClientOptions {
    pub default_timeout: Duration,
    pub limits: ClientLimits,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            default_timeout: DEFAULT_TIMEOUT,
            limits: ClientLimits::default(),
        }
    }
}

impl ClientOptions {
    fn budget(&self) -> Result<typed::ClientBudget> {
        deadline_after(self.default_timeout)?;
        typed::ClientBudget::new(self.limits)
    }
}

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
/// Legacy implementation for service resolution.
/// Migrated reads and mutations use [`FurpcClient`] only.
///
/// Clones explicitly share one Channel lifecycle owner, typed budget and terminal state.
/// A [`GatewayClient`] instead borrows a supplied foundation without adopting it.
/// Eight admitted typed calls are retained through result transfer/drop; excess
/// calls fail immediately. Protocol/domain failures do not abort the owner.
/// Every remote operation accepts an optional whole-operation timeout:
/// `None` uses [`ClientOptions::default_timeout`], while
/// `Some(duration)` overrides it for that call. `None` is not an infinite timeout.
///
/// ```no_run
/// use std::time::Duration;
/// use fube_client::{ClientOptions, FabricClient, DEFAULT_TIMEOUT};
/// use fube_transport::{channel::MessageChannel, connection::deadline_after};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let endpoint = FabricClient::endpoint_clear("127.0.0.1:19000".parse().unwrap())?
///     .connect_timeout(Duration::from_secs(10));
/// let channel = endpoint.connect().await?;
/// let client = FabricClient::new(channel.clone(), ClientOptions::default())?;
/// let cloned = client.clone(); // same typed admission budget, no new handshake
/// let name = "fabric:/Example/Service".parse()?;
/// let key = fube_client::PartitionKey::Singleton;
/// let (first, second) = tokio::try_join!(
///     client.resolve_service_partition(&name, &key, None, None),
///     cloned.resolve_service_partition(&name, &key, None, Some(Duration::from_secs(5))),
/// )?;
/// channel.close(deadline_after(DEFAULT_TIMEOUT)?).await?; // shared shutdown
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FabricClient {
    channel: Channel,
    target: Arc<GatewayTarget>,
    budget: typed::ClientBudget,
    timeout: Duration,
}

impl FabricClient {
    /// Configure an ordinary plain-TCP connection without opening it.
    ///
    /// Authorization is installed by connection setup, not by stub binding.
    pub fn endpoint_clear(address: SocketAddr) -> Result<Endpoint> {
        Endpoint::unsecured_tcp(address, channel_adapter::ordinary()?)
    }

    /// Bind locally to an established channel and its original target.
    ///
    /// No handshake, application message or permission change occurs. Invalid
    /// options drop only the consumed handle, never explicitly close retained
    /// clones. A target sample is not a lasting liveness guarantee.
    pub fn new(channel: Channel, options: ClientOptions) -> Result<Self> {
        let budget = options.budget()?;
        let target = channel.target()?;
        Ok(Self {
            channel,
            target,
            budget,
            timeout: options.default_timeout,
        })
    }

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

    fn deadline(&self, timeout: Option<Duration>) -> Result<Instant> {
        deadline_after(timeout.unwrap_or(self.timeout))
    }
}

impl Client for FabricClient {
    type Channel = Channel;
    fn binding(&self) -> ClientBinding<'_, Channel> {
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
