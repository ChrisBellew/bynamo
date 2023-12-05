// tonic::include_proto!("storage");

// use crate::messaging::{
//     node_message::Message, storage_command, storage_message, MessageType, NodeMessage,
//     StorageCommand, StorageMessage, WriteCommand, WriteReplicaCommand,
// };

// impl From<WriteCommand> for NodeMessage {
//     fn from(command: WriteCommand) -> Self {
//         NodeMessage {
//             r#type: MessageType::Storage.into(),
//             message: Some(Message::Storage(StorageMessage {
//                 message: Some(storage_message::Message::Command(StorageCommand {
//                     command: Some(storage_command::Command::Write(command)),
//                 })),
//             })),
//         }
//     }
// }

// impl From<WriteReplicaCommand> for NodeMessage {
//     fn from(command: WriteReplicaCommand) -> Self {
//         NodeMessage {
//             r#type: MessageType::Storage.into(),
//             message: Some(Message::Storage(StorageMessage {
//                 message: Some(storage_message::Message::Command(StorageCommand {
//                     command: Some(storage_command::Command::WriteReplica(command)),
//                 })),
//             })),
//         }
//     }
// }

//use super::commands::commands::StorageCommand;

use super::commands::commands::StorageCommand;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum StorageMessage {
    Command(StorageCommand),
    Query(StorageQuery),
}

// impl StorageMessage {
//     pub fn message_id(&self) -> &String {
//         match self {
//             StorageMessage::Command(command) => command.message_id(),
//             StorageMessage::Query(query) => query.message_id(),
//         }
//     }
// }

#[derive(Deserialize, Serialize)]
pub enum StorageQuery {
    Read(ReadQuery),
}

// impl StorageQuery {
//     pub fn message_id(&self) -> &String {
//         match self {
//             StorageQuery::Read(query) => &query.message_id,
//         }
//     }
// }

#[derive(Deserialize, Serialize)]
pub struct ReadQuery {
    pub key: String,
}
