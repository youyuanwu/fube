use mssf_transport::message::Message;
use serde::Serialize;

use crate::{
    nodes::*, query_arguments::QueryArguments, query_protocol::Query, Error, Paging, Result,
};

impl Query for NodeQueryDescription {
    type Output = NodeListPage;
    const NAME: &'static str = "GetNodeList";
    const PAGING_FIRST: bool = true;

    fn encode_parameters(&self, arguments: &mut QueryArguments) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct Fields<'a> {
            node_name: Option<&'a str>,
            node_status_filter: Option<NodeStatusFilter>,
        }
        arguments.append(&Fields {
            node_name: self
                .parameters
                .node_name
                .as_deref()
                .filter(|s| !s.is_empty()),
            node_status_filter: (self.parameters.status_filter != NodeStatusFilter::DEFAULT)
                .then_some(self.parameters.status_filter),
        })
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
