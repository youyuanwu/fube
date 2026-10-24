use mssf_transport::message::Message;
use mssf_transport::serialization::serde::{FixedTag, ObjectArray, Pointer};
use serde::Deserialize;

use crate::{health::*, query_arguments::QueryArguments, query_protocol::Query, Error, Result};

impl Query for NodeHealthQueryDescription {
    type Output = NodeHealth;
    const NAME: &'static str = "GetNodeHealth";

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        let query = &self.parameters;
        // Bound caller work before UTF-16 scanning, not just before wire allocation.
        let name = &query.node_name;
        if name.is_empty()
            || name.len() > 4096 * 3
            || name.contains('\0')
            || name.encode_utf16().count() > 4096
        {
            return Err(Error::InvalidInput(
                "node health name must be 1..=4096 UTF-16 units without NUL",
            ));
        }
        arguments.append(query)
    }

    fn decode_reply(message: &Message) -> Result<NodeHealth> {
        decode_reply(message)
    }
}

pub(crate) fn decode_reply(message: &Message) -> Result<NodeHealth> {
    crate::protocol::check_failure(message)?;
    let properties = message.properties()?;
    if properties.actor != 16 || properties.action != "OperationSuccessAction" {
        return Err(Error::Protocol("unexpected node health success envelope"));
    }
    #[derive(Deserialize)]
    struct ItemPayload {
        _list: ObjectArray<Vec<ForbiddenListItem>>,
        item: Pointer<NodeHealth>,
    }
    let result = crate::query_protocol::decode::<FixedTag<ItemPayload, 16>, _>(
        &message.body,
        false,
        |FixedTag(payload)| {
            payload
                .item
                .0
                .ok_or(Error::Protocol("null node health item"))
        },
    )?;
    if result.kind != 1 || result.paging.is_some() {
        return Err(Error::Protocol("node health result is not an unpaged Item"));
    }
    result
        .payload
        .ok_or(Error::Protocol("node health has no typed wrapper"))
}

struct ForbiddenListItem;
impl<'de> Deserialize<'de> for ForbiddenListItem {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom("node health item contains a list"))
    }
}
