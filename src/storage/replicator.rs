use std::time::Instant;

use super::{commands::commands::WriteReplicaCommand, key_value::Position};
use crate::{bynamo_node::NodeId, messaging::sender::MessageSender};
// use lazy_static::lazy_static;
// use prometheus::{exponential_buckets, register_histogram, Histogram};

// lazy_static! {
//     static ref REPLICATOR_WRITES_HISTOGRAM: Histogram = register_histogram!(
//         "replicator_writes_histogram",
//         "Replication durations in microseconds",
//         exponential_buckets(20.0, 3.0, 15).unwrap()
//     )
//     .unwrap();
// }

#[derive(Clone)]
pub struct StorageReplicator {
    message_sender: MessageSender,
    members: Vec<NodeId>,
    //message_sender: MessageSender,
}

impl StorageReplicator {
    pub fn new(members: Vec<NodeId>, message_sender: MessageSender) -> Self {
        Self {
            members,
            message_sender,
        }
    }
    pub async fn replicate(
        &mut self,
        position: Position,
        key: String,
        value: String,
        leader: NodeId,
    ) -> Result<(), ReplicateError> {
        let start = Instant::now();
        let followers = self.members.iter().filter(|id| **id != leader);

        let follower = *followers.into_iter().nth(0).unwrap();
        self.message_sender
            .send_and_wait(
                follower,
                WriteReplicaCommand {
                    position,
                    follower,
                    key: key.clone(),
                    value: value.clone(),
                }
                .into(),
            )
            .await
            .unwrap();

        //REPLICATOR_WRITES_HISTOGRAM.observe(start.elapsed().as_micros() as f64);

        // self.message_sender
        //     .send_and_wait(
        //         follower,
        //         WriteReplicaCommand {
        //             message_id: new_message_id(),
        //             position,
        //             follower,
        //             key: key.clone(),
        //             value: value.clone(),
        //         }
        //         .into(),
        //     )
        //     .await?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReplicateError {
    // #[error("failed to write because all followers failed to write")]
    // AllFollowersFailed(#[from] SendError<RequestEnvelope>),
}
