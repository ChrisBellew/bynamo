use crate::bynamo_node::NodeId;

pub struct MembershipClient {
    nodes: Vec<NodeId>,
}

impl MembershipClient {
    pub fn from_nodes(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }
    pub fn members(&self) -> &Vec<u32> {
        &self.nodes
    }
}
