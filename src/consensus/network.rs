use super::message::Message;
use crate::bynamo_node::NodeId;
use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};
use std::{collections::HashMap, time::Duration};

pub struct NodeChannel {
    pub node_id: NodeId,
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>,
}

pub struct Network {
    node_channels: HashMap<NodeId, NodeChannel>,
}

impl Network {
    pub fn new(node_channels: HashMap<NodeId, NodeChannel>) -> Self {
        Self { node_channels }
    }
    pub fn run(&mut self) {
        let receive_message = |node_id: NodeId,
                               node_channels: &HashMap<NodeId, NodeChannel>|
         -> Result<(), RecvTimeoutError> {
            match node_channels
                .get(&node_id)
                .unwrap()
                .receiver
                .recv_timeout(Duration::from_millis(10))
            {
                Ok(message) => forward_message(message, &node_channels),
                Err(err) => match err {
                    RecvTimeoutError::Timeout => {}
                    RecvTimeoutError::Disconnected => return Err(err),
                },
            };
            Ok(())
        };

        fn forward_message(message: Message, node_channels: &HashMap<NodeId, NodeChannel>) {
            match message {
                Message::Vote(ref vote) => {
                    node_channels.get(&vote.votee).unwrap().sender.send(message)
                }
                Message::Heartbeat(ref heartbeat) => node_channels
                    .get(&heartbeat.receiver)
                    .unwrap()
                    .sender
                    .send(message),
                Message::RequestVote(ref request) => node_channels
                    .get(&request.requestee)
                    .unwrap()
                    .sender
                    .send(message),
            }
            .unwrap();
        }

        loop {
            for (node_id, _) in &self.node_channels {
                receive_message(*node_id, &self.node_channels).unwrap();
            }
        }
    }
}
