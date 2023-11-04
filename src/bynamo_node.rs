use crate::{
    consensus::{message::Message, role_consensus::RoleConsensus},
    coordinator::Coordinator,
    membership::client::MembershipClient,
};
use anyhow::Result;
use crossbeam::channel::{bounded, Receiver, Sender};
use std::path::PathBuf;

pub type NodeId = u32;

pub struct BynamoNode {
    node_id: NodeId,
    coordinator: Coordinator,
    role_consensus: RoleConsensus,
    // network_sender: Sender<Message>,
    // network_receiver: Receiver<Message>,
}

pub struct BynamoNodeOptions {
    pub write_ahead_log_path: PathBuf,
    pub write_ahead_index_path: PathBuf,
    pub nodes: Vec<NodeId>,
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>,
}

impl BynamoNode {
    pub fn create(node_id: NodeId, options: BynamoNodeOptions) -> Result<BynamoNode> {
        let BynamoNodeOptions {
            write_ahead_log_path,
            write_ahead_index_path,
            nodes,
            sender,
            receiver,
        } = options;

        let membership_client = MembershipClient::from_nodes(nodes);

        // let (node_sender, network_receiver) = bounded(100);
        // let (network_sender, node_receiver) = bounded(100);

        Ok(BynamoNode {
            node_id,
            coordinator: Coordinator::create(write_ahead_log_path, write_ahead_index_path)?,
            role_consensus: RoleConsensus::new(node_id, membership_client, sender, receiver),
        })
    }
    pub fn tick(&mut self) {
        self.role_consensus.tick();
    }
    pub fn write(&mut self, key: String, value: String) -> Result<()> {
        self.coordinator.write(key, value)
    }
    pub fn read(&mut self, key: &String) -> Option<String> {
        self.coordinator.read(key)
    }
}
