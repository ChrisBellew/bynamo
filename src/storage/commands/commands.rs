use crate::{
    bynamo_node::NodeId,
    messaging::message::Message,
    storage::{key_value::Position, message::StorageMessage},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum StorageCommand {
    Write(WriteCommand),
    WriteReplica(WriteReplicaCommand),
}

#[derive(Deserialize, Serialize)]
pub struct WriteCommand {
    pub key: String,
    pub value: String,
}

impl From<WriteCommand> for Message {
    fn from(command: WriteCommand) -> Self {
        Message::Storage(StorageMessage::Command(StorageCommand::Write(command)))
    }
}

#[derive(Deserialize, Serialize)]
pub struct WriteReplicaCommand {
    pub position: Position,
    pub follower: NodeId,
    pub key: String,
    pub value: String,
}

impl From<WriteReplicaCommand> for Message {
    fn from(command: WriteReplicaCommand) -> Self {
        Message::Storage(StorageMessage::Command(StorageCommand::WriteReplica(
            command,
        )))
    }
}
