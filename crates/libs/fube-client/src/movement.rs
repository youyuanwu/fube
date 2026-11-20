//! Constraint-respecting native FM control used to qualify reconfiguration.
//! Source: client/FabricClientImpl.cpp:1354-1393 and FM QueryHelper.cpp:16-60.
use crate::{
    query_arguments::validate_node_name,
    query_protocol::{Query, Reply, ReplyEnvelope, ReplyError, ReplyResult},
    DomainFailure, QueryDescription,
};
use fubst::serde::{Pointer, RawTagged};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Constraint-enforced movement to an explicit native node, without force/ignore options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MovePrimaryQueryParameters {
    /// Case-preserving target: 1..=4096 UTF-16 units without NUL.
    #[serde(rename = "NodeName")]
    pub target_node: String,
    /// A non-nil partition identity; observe placement separately from completion.
    #[serde(rename = "PartitionId")]
    pub partition_id: Uuid,
}

impl MovePrimaryQueryParameters {
    pub fn new(partition_id: Uuid, target_node: impl Into<String>) -> Self {
        Self {
            target_node: target_node.into(),
            partition_id,
        }
    }
}

/// An unpaged MovePrimary mutation with an empty native QueryResult.
pub type MovePrimaryQueryDescription = QueryDescription<MovePrimaryQueryParameters>;

impl Query for MovePrimaryQueryParameters {
    type Output = ();
    type Reply = EmptyReply;
    const NAME: &'static str = "MovePrimary";
    const SUPPORTS_MAX_RESULTS: bool = false;
    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.target_node,
            "primary target must be an explicit native node",
        )?;
        if self.partition_id.is_nil() {
            return Err(fubst::Error::InvalidInput("primary move needs a partition"));
        }
        Ok(())
    }
}
pub(crate) struct EmptyReply;
#[derive(Deserialize)]
pub(crate) struct Empty {}
#[derive(Deserialize)]
pub(crate) struct Envelope {
    kind: i64,
    payload: Pointer<RawTagged<Empty>>,
    error: i64,
    paging: Pointer<RawTagged<Empty>>,
}
impl Reply for EmptyReply {
    type Record = ();
    type Body = Envelope;
    type Output = ();
    // RSMessage::QueryReply uses CreateFMToCommonMessage / RSActor (13).
    const ENVELOPE: ReplyEnvelope = ReplyEnvelope::Empty(13);

    fn decode_body(
        result: Envelope,
        _: fn(()) -> std::result::Result<(), &'static str>,
    ) -> ReplyResult<()> {
        if result.kind != 0 || result.payload.0.is_some() || result.paging.0.is_some() {
            return Err("movement reply is not an empty QueryResult".into());
        }
        if result.error != 0 {
            return Err(ReplyError::Domain(DomainFailure {
                code: result.error,
                message: "native primary movement".into(),
            }));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn movement_requires_a_partition_and_explicit_native_node() {
        let partition = Uuid::new_v4();
        for node in [" N0010 ".to_owned(), "\u{1f980}".repeat(2048)] {
            let parameters = MovePrimaryQueryParameters::new(partition, &node);
            parameters.validate_parameters().unwrap();
            assert_eq!(parameters.target_node, node);
        }
        for node in ["".to_owned(), "N\0".into(), "\u{1f980}".repeat(2049)] {
            assert!(MovePrimaryQueryParameters::new(partition, node)
                .validate_parameters()
                .is_err());
        }
        assert!(MovePrimaryQueryParameters::new(Uuid::nil(), "N0010")
            .validate_parameters()
            .is_err());
    }
}
