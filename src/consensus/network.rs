use super::message::{
    ConsensusReceiver, ConsensusSender, MessageRecvError, MessageSendError, RoleConsensusMessage,
};
use crate::bynamo_node::NodeId;
use async_channel::{bounded, Receiver, Sender};
use async_trait::async_trait;
use std::collections::HashMap;

pub struct DirectHubConsensusNetwork {
    hub_senders: HashMap<NodeId, Sender<RoleConsensusMessage>>,
    node_sender: Sender<RoleConsensusMessage>,
    hub_receiver: Receiver<RoleConsensusMessage>,
}

impl DirectHubConsensusNetwork {
    pub fn new() -> Self {
        let (node_sender, hub_receiver) = bounded(100);
        Self {
            hub_senders: HashMap::new(),
            node_sender,
            hub_receiver,
        }
    }
    pub fn add_node(&mut self, node_id: NodeId) -> (HubMessageSender, HubMessageReceiver) {
        let (hub_sender, node_receiver) = bounded(100);

        self.hub_senders.insert(node_id, hub_sender);

        (
            HubMessageSender {
                sender: self.node_sender.clone(),
            },
            HubMessageReceiver {
                receiver: node_receiver,
            },
        )
    }
    pub async fn tick(&self) {
        match self.hub_receiver.try_recv() {
            Ok(message) => self.forward_message(message).await,
            Err(_) => {}
        };
    }
    async fn forward_message(&self, message: RoleConsensusMessage) {
        match message {
            RoleConsensusMessage::Vote(ref vote) => {
                self.hub_senders.get(&vote.votee).unwrap().send(message)
            }
            RoleConsensusMessage::Heartbeat(ref heartbeat) => self
                .hub_senders
                .get(&heartbeat.receiver)
                .unwrap()
                .send(message),
            RoleConsensusMessage::RequestVote(ref request) => self
                .hub_senders
                .get(&request.requestee)
                .unwrap()
                .send(message),
        }
        .await
        .unwrap();
    }
}

pub struct HubMessageSender {
    sender: Sender<RoleConsensusMessage>,
}

#[async_trait]
impl ConsensusSender for HubMessageSender {
    async fn send(&self, message: RoleConsensusMessage) -> Result<(), MessageSendError> {
        match self.sender.send(message).await {
            Ok(_) => Ok(()),
            Err(_) => Err(MessageSendError {}),
        }
    }
    async fn try_send(&self, message: RoleConsensusMessage) -> bool {
        self.sender.send(message).await.is_ok()
    }
}

pub struct HubMessageReceiver {
    receiver: Receiver<RoleConsensusMessage>,
}

#[async_trait]
impl ConsensusReceiver for HubMessageReceiver {
    async fn recv(&self) -> Result<RoleConsensusMessage, MessageRecvError> {
        match self.receiver.recv().await {
            Ok(message) => Ok(message),
            Err(_) => Err(MessageRecvError {}),
        }
    }
    fn try_recv(&self) -> Option<RoleConsensusMessage> {
        match self.receiver.try_recv() {
            Ok(message) => Some(message),
            Err(_) => None,
        }
    }
}
