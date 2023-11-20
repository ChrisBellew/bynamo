use super::super::super::{
    skip_list::{coin_flip, SkipList},
    write_ahead_log::WriteAheadLog,
};
use crate::{bynamo_node::NodeId, messaging::WriteReplicaCommand, storage::write_ahead_log};

#[derive(Clone)]
pub struct WriteReplicaCommandHandler {
    node_id: NodeId,
    write_ahead_log: WriteAheadLog,
    skip_list: SkipList,
}

impl WriteReplicaCommandHandler {
    pub fn new(node_id: NodeId, write_ahead_log: WriteAheadLog, skip_list: SkipList) -> Self {
        Self {
            node_id,
            write_ahead_log,
            skip_list,
        }
    }
    pub async fn handle(&mut self, command: WriteReplicaCommand) -> Result<(), WriteError> {
        let WriteReplicaCommand {
            position,
            follower,
            key,
            value,
        } = command;

        if follower != self.node_id {
            // This command is not for us
            return Err(WriteError::MisdeliveredMessageError());
        }

        if self.write_ahead_log.position().await >= position {
            // We have already written this command
            return Ok(());
        }

        // Write to the WAL first so that we can recover if the node crashes
        self.write_ahead_log.write(position, &key, &value).await?;

        // Write to skip list
        self.skip_list.insert(key, value, coin_flip());

        println!(
            "Written replica of position {} as follower {}",
            position, follower
        );

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("failed to write replica to the write ahead log")]
    WriteAheadLogError(#[from] write_ahead_log::WriteError),
    #[error("failed to write replica as the message was misdelivered")]
    MisdeliveredMessageError(),
}
