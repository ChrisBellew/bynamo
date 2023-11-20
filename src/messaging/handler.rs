use crate::{
    consensus::role_consensus::RoleConsensus,
    storage::handle_message::{StorageError, StorageMessageHandler},
};

use super::node_message::Message;

#[derive(Clone)]
pub struct MessageHandler {
    pub storage_handler: StorageMessageHandler,
    pub role_consensus: RoleConsensus,
}

impl MessageHandler {
    pub fn new(storage_handler: StorageMessageHandler, role_consensus: RoleConsensus) -> Self {
        Self {
            storage_handler,
            role_consensus,
        }
    }
    pub async fn handle(&mut self, message: Message) -> Result<(), StorageError> {
        match message {
            Message::Storage(message) => {
                self.storage_handler.handle(message).await?;
            }
            Message::RoleConsensus(message) => {
                self.role_consensus.handle_message(message).await;
            }
        }
        Ok(())
    }
}
