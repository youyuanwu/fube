//! Experimental pure Rust, clear-TCP FabricClient subset.
//!
//! This package does not use the native Service Fabric client library.
//! [`FurpcClient`] is the ordinary typed stub over a byte-facing Tower service,
//! defaulting to furpc's Channel. It provides Ping, naming operations,
//! cluster/topology/deployment lists, node health, MovePrimary and resolution.
//! The old FabricClient façade, sealed request adapters and typed budgets are retired.
//! Notification and ImageStore clients retain independently owned legacy transports.
//! Native connections require an explicitly selected unsecured development gateway.

mod applications;
mod channel_adapter;
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
pub use applications::*;
pub use deployed_applications::*;
pub use deployed_code_packages::*;
pub use deployed_replicas::*;
pub use deployed_service_packages::*;
pub use furpc_client::{DomainFailure, FurpcClient, FurpcClientError};
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
use std::time::Duration;

/// Legacy transport types retained for independently owned notification/ImageStore clients.
pub use fube_transport::{Channel, ChannelLimits, Endpoint, Error, Result};

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
