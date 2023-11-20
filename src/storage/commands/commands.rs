// use crate::{
//     bynamo_node::NodeId,
//     messaging::message::Message,
//     storage::{key_value::Position, message::StorageMessage},
// };

// pub enum StorageCommand {
//     Write(WriteCommand),
//     WriteReplica(WriteReplicaCommand),
// }

// impl StorageCommand {
//     pub fn message_id(&self) -> &String {
//         match self {
//             StorageCommand::Write(command) => &command.message_id,
//             StorageCommand::WriteReplica(command) => &command.message_id,
//         }
//     }
// }

// pub struct WriteCommand {
//     pub message_id: String,
//     pub key: String,
//     pub value: String,
// }

// impl From<WriteCommand> for Message {
//     fn from(command: WriteCommand) -> Self {
//         Message::Storage(StorageMessage::Command(StorageCommand::Write(command)))
//     }
// }

// pub struct WriteReplicaCommand {
//     pub message_id: String,
//     pub position: Position,
//     pub follower: NodeId,
//     pub key: String,
//     pub value: String,
// }

// impl From<WriteReplicaCommand> for Message {
//     fn from(command: WriteReplicaCommand) -> Self {
//         Message::Storage(StorageMessage::Command(StorageCommand::WriteReplica(
//             command,
//         )))
//     }
// }
