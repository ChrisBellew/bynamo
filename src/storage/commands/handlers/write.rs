use super::super::super::{
    skip_list::{coin_flip, SkipList},
    write_ahead_log::WriteAheadLog,
};
use crate::{
    bynamo_node::NodeId,
    messaging::WriteCommand,
    role_service::RoleService,
    storage::{
        replicator::{ReplicateError, StorageReplicator},
        write_ahead_log,
    },
};

#[derive(Clone)]
pub struct WriteCommandHandler {
    node_id: NodeId,
    role_service: RoleService,
    write_ahead_log: WriteAheadLog,
    skip_list: SkipList,
    replicator: StorageReplicator,
}

impl WriteCommandHandler {
    pub fn new(
        node_id: NodeId,
        role_service: RoleService,
        write_ahead_log: WriteAheadLog,
        skip_list: SkipList,
        replicator: StorageReplicator,
    ) -> Self {
        Self {
            node_id,
            role_service,
            write_ahead_log,
            skip_list,
            replicator,
        }
    }
    pub async fn handle(&mut self, command: WriteCommand) -> Result<(), WriteError> {
        let WriteCommand { key, value } = command;

        match self.role_service.leader().await {
            Some(leader) => {
                if leader != self.node_id {
                    // We are not the leader
                    return Err(WriteError::LeadershipError);
                }
            }
            // There is currently no leader
            None => return Err(WriteError::LeadershipError),
        }

        // Assign next position, treating WAL as the source of truth
        let position = self.write_ahead_log.position().await + 1;

        // Write to the WAL first so that we can recover if the node crashes
        self.write_ahead_log.write(position, &key, &value).await?;

        // Replicate the write to the followers
        self.replicator
            .replicate(position, key.clone(), value.clone(), self.node_id)
            .await?;

        // Write to skip list
        self.skip_list.insert(key, value, coin_flip());

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("failed to write because this node is not the leader")]
    LeadershipError,
    #[error("failed to write to the write ahead log")]
    WriteAheadLogError(#[from] write_ahead_log::WriteError),
    #[error("failed to replicate write")]
    ReplicateError(#[from] ReplicateError),
}
