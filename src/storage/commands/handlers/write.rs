use std::time::Instant;

use super::super::super::{
    skip_list::{coin_flip, SkipList},
    write_ahead_log::WriteAheadLog,
};
use crate::{
    bynamo_node::NodeId,
    role_service::RoleService,
    storage::{
        b_tree::{node_store::StringNodeSerializer, tree::BTree},
        commands::commands::WriteCommand,
        replicator::{ReplicateError, StorageReplicator},
        write_ahead_log,
    },
};
use lazy_static::lazy_static;
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram, Histogram, HistogramOpts, Registry,
};

#[derive(Clone)]
pub struct WriteCommandHandler {
    node_id: NodeId,
    role_service: RoleService,
    write_ahead_log: WriteAheadLog,
    btree: BTree<String, String, StringNodeSerializer>,
    replicator: StorageReplicator,
    puts_histogram: Histogram,
}

impl WriteCommandHandler {
    pub fn new(
        node_id: NodeId,
        role_service: RoleService,
        write_ahead_log: WriteAheadLog,
        btree: BTree<String, String, StringNodeSerializer>,
        replicator: StorageReplicator,
        registry: &Registry,
    ) -> Self {
        let puts_histogram = Histogram::with_opts(
            HistogramOpts::new("putitem_histogram", "Item put durations in seconds")
                .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry.register(Box::new(puts_histogram.clone())).unwrap();

        Self {
            node_id,
            role_service,
            write_ahead_log,
            btree,
            replicator,
            puts_histogram,
        }
    }
    pub async fn handle(&mut self, command: WriteCommand) -> Result<(), WriteError> {
        let start = Instant::now();
        let WriteCommand { key, value } = command;

        if self.btree.store.flush_buffer_full().await {
            return Err(WriteError::BtreeWriteBufferFull());
        }

        // // // match self.role_service.leader().await {
        // // //     Some(leader) => {
        // // //         if leader != self.node_id {
        // // //             // We are not the leader
        // // //             return Err(WriteError::LeadershipError);
        // // //         }
        // // //     }
        // // //     // There is currently no leader
        // // //     None => return Err(WriteError::LeadershipError),
        // // // }

        // // // Assign next position, treating WAL as the source of truth
        // // //let position = self.write_ahead_log.next_position();

        // // // Write to the WAL first so that we can recover if the node crashes
        self.write_ahead_log.write_new(&key, &value).await?;

        self.btree.add(key, value).await;

        // // Replicate the write to the followers
        // self.replicator
        //     .replicate(position, key.clone(), value.clone(), self.node_id)
        //     .await?;

        self.puts_histogram.observe(start.elapsed().as_secs_f64());

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
    #[error("failed to write because btree buffer is full")]
    BtreeWriteBufferFull(),
}
