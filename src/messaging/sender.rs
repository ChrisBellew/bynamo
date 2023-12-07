use super::{client::MessageClient, message::Message};
use crate::bynamo_node::NodeId;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
#[derive(Clone)]
pub struct MessageSender {
    clients: Arc<RwLock<HashMap<NodeId, MessageClient>>>,
}

impl MessageSender {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn add_client(&mut self, node_id: NodeId, client: MessageClient) {
        self.clients.write().await.insert(node_id, client);
        println!("added client for node {}", node_id);
    }
    pub async fn send_and_wait(
        &mut self,
        recipient: NodeId,
        message: Message,
    ) -> Result<(), SendError> {
        let lock = self.clients.read().await;
        let client = lock.get(&recipient).cloned();
        drop(lock);

        client.unwrap().send_message(message).await;
        Ok(())
    }
    pub async fn send_and_forget(&mut self, recipient: NodeId, message: Message) {
        let lock = self.clients.read().await;
        let client = lock.get(&recipient).cloned().unwrap();
        drop(lock);

        tokio::spawn(async move {
            client.send_message(message).await;
        });
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("failed to send message")]
    Error,
}
