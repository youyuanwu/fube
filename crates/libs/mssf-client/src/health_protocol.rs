use crate::{
    health::*,
    query_protocol::{ItemReply, Query},
    Error, Result,
};

impl Query for NodeHealthQueryParameters {
    type Output = NodeHealth;
    type Reply = ItemReply<NodeHealth, 16, 16>;
    const NAME: &'static str = "GetNodeHealth";

    fn validate_parameters(&self) -> Result<()> {
        // Bound caller work before UTF-16 scanning, not just before wire allocation.
        let name = &self.node_name;
        if name.is_empty()
            || name.len() > 4096 * 3
            || name.contains('\0')
            || name.encode_utf16().count() > 4096
        {
            return Err(Error::InvalidInput(
                "node health name must be 1..=4096 UTF-16 units without NUL",
            ));
        }
        Ok(())
    }
}
