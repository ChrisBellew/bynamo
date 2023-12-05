use super::{
    b_tree::{node_store::StringNodeSerializer, tree::BTree},
    commands::{
        commands::StorageCommand,
        handlers::{
            write::{self, WriteCommandHandler},
            write_replica::{self, WriteReplicaCommandHandler},
        },
    },
    message::StorageMessage,
    replicator::StorageReplicator,
    write_ahead_log::WriteAheadLog,
};
use crate::{bynamo_node::NodeId, role_service::RoleService};
use prometheus::Registry;
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
        btree: BTree<String, String, StringNodeSerializer>,
        replicator: StorageReplicator,
        registry: &Registry,
    ) -> Self {
        Self {
            write_command_handler: WriteCommandHandler::new(
                node_id,
                role_service,
                write_ahead_log.clone(),
                btree.clone(),
                replicator,
                registry,
            ),
            write_replica_command_handler: WriteReplicaCommandHandler::new(
                node_id,
                write_ahead_log,
                btree,
            ),
        }
    }
    pub async fn handle(&mut self, message: StorageMessage) -> Result<(), StorageError> {
        match message {
            StorageMessage::Command(command) => {
                self.handle_command(command).await?;
            }
            StorageMessage::Query(_) => {
                todo!();
            }
        };
        Ok(())
    }
    async fn handle_command(&mut self, command: StorageCommand) -> Result<(), StorageError> {
        match command {
            StorageCommand::Write(command) => self.write_command_handler.handle(command).await?,
            StorageCommand::WriteReplica(command) => {
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
