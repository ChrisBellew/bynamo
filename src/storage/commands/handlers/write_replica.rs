use super::super::super::{
    skip_list::{coin_flip, SkipList},
    write_ahead_log::WriteAheadLog,
};
use crate::{
    bynamo_node::NodeId,
    //messaging::WriteReplicaCommand,
    storage::{
        b_tree::{node_store::StringNodeSerializer, tree::BTree},
        commands::commands::WriteReplicaCommand,
        write_ahead_log,
    },
};
use lazy_static::lazy_static;
use prometheus::{register_int_counter, IntCounter};

lazy_static! {
    static ref REPLICA_WRITES_COUNTER: IntCounter =
        register_int_counter!("replica_writes", "Number of replica writes processed").unwrap();
}

#[derive(Clone)]
pub struct WriteReplicaCommandHandler {
    node_id: NodeId,
    write_ahead_log: WriteAheadLog,
    btree: BTree<String, String, StringNodeSerializer>,
}

impl WriteReplicaCommandHandler {
    pub fn new(
        node_id: NodeId,
        write_ahead_log: WriteAheadLog,
        btree: BTree<String, String, StringNodeSerializer>,
    ) -> Self {
        Self {
            node_id,
            write_ahead_log,
            btree,
        }
    }
    pub async fn handle(&mut self, command: WriteReplicaCommand) -> Result<(), WriteError> {
        if self.btree.store.flush_buffer_size().await > 5000 {
            return Err(WriteError::BtreeWriteBufferFull());
        }

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

        if self.write_ahead_log.position() >= position {
            // We have already written this command
            return Ok(());
        }

        // Write to the WAL first so that we can recover if the node crashes
        self.write_ahead_log.write(position, &key, &value).await?;

        // Write to skip list
        self.btree.add(key, value).await;

        // println!(
        //     "Written replica of position {} as follower {}",
        //     position, follower
        // );

        REPLICA_WRITES_COUNTER.inc();

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteError {
    #[error("failed to write replica to the write ahead log")]
    WriteAheadLogError(#[from] write_ahead_log::WriteError),
    #[error("failed to write replica as the message was misdelivered")]
    MisdeliveredMessageError(),
    #[error("failed to write because btree buffer is full")]
    BtreeWriteBufferFull(),
}
