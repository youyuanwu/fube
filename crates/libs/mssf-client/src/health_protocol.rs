use crate::{
    health::*,
    query_arguments::validate_node_name,
    query_protocol::{result_tags, ItemReply, Query},
    Result,
};

impl Query for NodeHealthQueryParameters {
    type Output = NodeHealth;
    type Reply = ItemReply<NodeHealth, { result_tags::NODE_HEALTH }, 16>;
    const NAME: &'static str = "GetNodeHealth";

    fn validate_parameters(&self) -> Result<()> {
        validate_node_name(
            &self.node_name,
            "node health name must be 1..=4096 UTF-16 units without NUL",
        )
    }
}
