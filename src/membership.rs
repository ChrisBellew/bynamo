use std::sync::{Arc, RwLock};

use crate::bynamo_node::NodeId;

#[derive(Clone)]
pub struct MembershipService {
    nodes: Arc<RwLock<Vec<NodeId>>>,
}

impl MembershipService {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(vec![])),
        }
    }
    pub fn add_member(&mut self, node_id: NodeId) {
        self.nodes.write().unwrap().push(node_id);
    }
    pub fn members(&self) -> Vec<u32> {
        self.nodes.read().unwrap().clone()
    }
}
