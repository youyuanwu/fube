use crate::{
    nodes::*,
    query_protocol::{ListReply, Query},
    Error, Result,
};

impl Query for NodeQueryParameters {
    type Output = NodeListPage;
    type Reply = ListReply<Node, 1, false>;
    const NAME: &'static str = "GetNodeList";
    const PAGING_FIRST: bool = true;

    fn validate_record(node: Node) -> Result<Node> {
        if node.id_copy != node.id {
            return Err(Error::Protocol("node ID copies disagree"));
        }
        Ok(node)
    }
}
