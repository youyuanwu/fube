//! Constraint-respecting native FM control used to qualify reconfiguration.
//! Source: client/FabricClientImpl.cpp:1354-1393 and FM QueryHelper.cpp:16-60.
use crate::{
    query_arguments::validate_node_name,
    query_protocol::{Query, Reply, ReplyEnvelope, ReplyError, ReplyResult},
    DomainFailure,
};
use fubst::serde::{Pointer, RawTagged};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize)]
pub(crate) struct MovePrimary {
    #[serde(rename = "NodeName")]
    pub(crate) target: String,
    #[serde(rename = "PartitionId")]
    pub(crate) partition: Uuid,
}
impl Query for MovePrimary {
    type Output = ();
    type Reply = EmptyReply;
    const NAME: &'static str = "MovePrimary";
    const SUPPORTS_MAX_RESULTS: bool = false;
    fn validate_parameters(&self) -> fubst::Result<()> {
        validate_node_name(
            &self.target,
            "primary target must be an explicit native node",
        )?;
        if self.partition.is_nil() {
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
