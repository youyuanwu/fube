//! Shared request composition and explicit, single-page query results.
//!
//! Parameter/paging JSON uses native argument names and numeric flag masks.
//! Selectors are flattened and declared defaults are omitted; the outer
//! `parameters`/`paging` fields stay separate from the native request envelope.
//!
//! ```
//! use mssf_client::{
//!     NodeQueryDescription, NodeQueryParameters, NodeStatusFilter, Paging,
//! };
//! let query = NodeQueryDescription {
//!     parameters: NodeQueryParameters {
//!         status_filter: NodeStatusFilter::UP,
//!         ..Default::default()
//!     },
//!     paging: Paging { max_results: std::num::NonZeroU64::new(2), ..Default::default() },
//! };
//! let json = serde_json::to_string(&query).unwrap();
//! assert_eq!(
//!     serde_json::from_str::<serde_json::Value>(&json).unwrap(),
//!     serde_json::json!({
//!         "parameters": { "NodeStatusFilter": 1 },
//!         "paging": { "MaxResults": 2 },
//!     }),
//! );
//! assert_eq!(serde_json::from_str::<NodeQueryDescription>(&json).unwrap(), query);
//! ```

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

/// Parameters for an unpaged query. Unlike [`PagedQuery`], this has no paging.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryDescription<P> {
    pub parameters: P,
}

impl<P> QueryDescription<P> {
    pub fn new(parameters: P) -> Self {
        Self { parameters }
    }
}

/// Parameters and independent paging controls for one native list request.
///
/// Reusing the parameters with a returned token does not promise a snapshot.
/// No request automatically follows a continuation token or retries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PagedQuery<P> {
    pub parameters: P,
    pub paging: Paging,
}

impl<P> PagedQuery<P> {
    pub fn new(parameters: P) -> Self {
        Self {
            parameters,
            paging: Paging::default(),
        }
    }
}

/// Shared request paging, not response metadata or an automatic page iterator.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct Paging {
    /// Opaque server continuation. Empty strings are omitted like absent tokens.
    #[serde(
        default,
        skip_serializing_if = "crate::query_arguments::is_none_or_empty"
    )]
    pub continuation_token: Option<String>,
    /// Positive limit at most i64::MAX, checked before sending. None leaves the
    /// count unspecified; the native server can still page by message size.
    /// Partition and replica queries do not support count limits and require None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_results: Option<NonZeroU64>,
}

/// One server page, not a collected or atomic cluster snapshot.
///
/// None means paging ended, not proof that every native aggregation subquery
/// succeeded. A server can return best-effort data after partial failures, and
/// service-list unions can exceed the requested per-subquery count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPage<T> {
    pub items: Vec<T>,
    pub continuation_token: Option<String>,
}
