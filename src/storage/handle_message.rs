use super::{
    commands::handlers::{
        write::{self, WriteCommandHandler},
        write_replica::{self, WriteReplicaCommandHandler},
    },
    replicator::StorageReplicator,
    skip_list::SkipList,
    write_ahead_log::WriteAheadLog,
};
use crate::{
    bynamo_node::NodeId,
    messaging::{
        storage_command::Command, storage_message::Message, StorageCommand, StorageMessage,
    },
    role_service::RoleService,
};
use thiserror::Error;

#[derive(Clone)]
pub struct StorageMessageHandler {
    write_command_handler: WriteCommandHandler,
    write_replica_command_handler: WriteReplicaCommandHandler,
}

impl StorageMessageHandler {
    pub fn new(
        node_id: NodeId,
        role_service: RoleService,
        write_ahead_log: WriteAheadLog,
        skip_list: SkipList,
        replicator: StorageReplicator,
    ) -> Self {
        Self {
            write_command_handler: WriteCommandHandler::new(
                node_id,
                role_service,
                write_ahead_log.clone(),
                skip_list.clone(),
                replicator,
            ),
            write_replica_command_handler: WriteReplicaCommandHandler::new(
                node_id,
                write_ahead_log,
                skip_list,
            ),
        }
    }
    pub async fn handle(&mut self, message: StorageMessage) -> Result<(), StorageError> {
        match message.message.unwrap() {
            Message::Command(command) => {
                self.handle_command(command).await?;
            }
            Message::Query(_) => {
                todo!();
            }
        };
        Ok(())
    }
    async fn handle_command(&mut self, command: StorageCommand) -> Result<(), StorageError> {
        match command.command.unwrap() {
            Command::Write(command) => self.write_command_handler.handle(command).await?,
            Command::WriteReplica(command) => {
                self.write_replica_command_handler.handle(command).await?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to process write")]
    WriteError(#[from] write::WriteError),
    #[error("failed to process replica write")]
    WriteReplicaError(#[from] write_replica::WriteError),
}
