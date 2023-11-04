use crate::bynamo_node::NodeId;

pub struct MetadataService {
    members: Vec<NodeId>,
    leader: Option<NodeId>,
}

impl MetadataService {
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
            leader: None,
        }
    }
    pub fn clear_leader(&mut self) {
        self.leader = None;
    }
    pub fn announce_leader(&mut self, leader: NodeId) {
        self.leader = Some(leader);
    }
}
