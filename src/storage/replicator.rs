use super::key_value::Position;
use crate::{
    bynamo_node::NodeId,
    messaging::{sender::MessageSender, WriteReplicaCommand},
};

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
