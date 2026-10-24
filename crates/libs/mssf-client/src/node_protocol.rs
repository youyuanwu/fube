use mssf_transport::message::Message;

use crate::{
    nodes::*, query_arguments::QueryArguments, query_protocol::Query, Error, Paging, Result,
};

impl Query for NodeQueryDescription {
    type Output = NodeListPage;
    const NAME: &'static str = "GetNodeList";
    const PAGING_FIRST: bool = true;

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        arguments.append(&self.parameters)
    }

    fn paging(&self) -> Option<&Paging> {
        Some(&self.paging)
    }

    fn decode_reply(message: &Message) -> Result<NodeListPage> {
        crate::query_protocol::decode_list::<Node, _, 1>(message, false, |node| {
            if node.id_copy != node.id {
                return Err(Error::Protocol("node ID copies disagree"));
            }
            Ok(node)
        })
    }
}
