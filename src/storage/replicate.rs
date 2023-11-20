// use futures::future::select_all;
// use tokio::select;

// use super::{
//     commands::commands::{StorageCommand, WriteReplicaCommand},
//     key_value::Position,
//     message::StorageMessage,
//     message_sender::StorageMessageSender,
// };
// use crate::bynamo_node::NodeId;

// #[derive(thiserror::Error, Debug)]
// pub enum ReplicateError {
//     // #[error("failed to write because this node is not the leader")]
//     // LeadershipError,
// }

// pub async fn replicate<S>(
//     position: Position,
//     key: String,
//     value: String,
//     leader: NodeId,
//     members: Vec<NodeId>,
//     message_sender: S,
// ) -> Result<(), ReplicateError>
// where
//     S: StorageMessageSender,
// {
//     let followers = members.into_iter().filter(|id| *id != leader);

//     let result = select_all(followers.map(|follower| {
//         message_sender.send(StorageMessage::Command(StorageCommand::WriteReplica(
//             WriteReplicaCommand {
//                 position,
//                 follower,
//                 key: key.clone(),
//                 value: value.clone(),
//             },
//         )))
//     }))
//     .await;

//     // Get leader
//     // Get leader's next node
//     // Send write request to leader's next node
//     // Wait for response
//     // If response is error, retry
//     // If response is success, return

//     Ok(())
// }
