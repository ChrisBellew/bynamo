// use crate::messaging::{
//     node_message::Message, role_consensus_command, HeartbeatCommand, MessageType, NodeMessage,
//     RequestVoteCommand, RoleConsensusCommand, RoleConsensusMessage, VoteCommand,
// };

// impl From<HeartbeatCommand> for NodeMessage {
//     fn from(command: HeartbeatCommand) -> Self {
//         NodeMessage {
//             r#type: MessageType::RoleConsensus.into(),
//             message: Some(Message::RoleConsensus(RoleConsensusMessage {
//                 command: Some(RoleConsensusCommand {
//                     command: Some(role_consensus_command::Command::Heartbeat(command)),
//                 }),
//             })),
//         }
//     }
// }

// impl From<RequestVoteCommand> for NodeMessage {
//     fn from(command: RequestVoteCommand) -> Self {
//         NodeMessage {
//             r#type: MessageType::RoleConsensus.into(),
//             message: Some(Message::RoleConsensus(RoleConsensusMessage {
//                 command: Some(RoleConsensusCommand {
//                     command: Some(role_consensus_command::Command::RequestVote(command)),
//                 }),
//             })),
//         }
//     }
// }

// impl From<VoteCommand> for NodeMessage {
//     fn from(command: VoteCommand) -> Self {
//         NodeMessage {
//             r#type: MessageType::RoleConsensus.into(),
//             message: Some(Message::RoleConsensus(RoleConsensusMessage {
//                 command: Some(RoleConsensusCommand {
//                     command: Some(role_consensus_command::Command::Vote(command)),
//                 }),
//             })),
//         }
//     }
// }

use serde::{Deserialize, Serialize};

use crate::messaging::message::Message;

#[derive(Deserialize, Serialize)]
pub enum RoleConsensusMessage {
    Command(RoleConsensusCommand),
}

#[derive(Deserialize, Serialize)]
pub enum RoleConsensusCommand {
    Heartbeat(HeartbeatCommand),
    RequestVote(RequestVoteCommand),
    Vote(VoteCommand),
}

// message VoteCommand {
//     uint32 term = 1;
//     uint32 voter = 2;
//     uint32 votee = 3;
// }

// message HeartbeatCommand {
//     uint32 term = 1;
//     uint32 sender = 2;
//     uint32 receiver = 3;
// }

// message RequestVoteCommand {
//     uint32 term = 1;
//     uint32 requester = 2;
//     uint32 requestee = 3;
// }

#[derive(Deserialize, Serialize)]
pub struct VoteCommand {
    pub term: u32,
    pub voter: u32,
    pub votee: u32,
}

impl From<VoteCommand> for Message {
    fn from(command: VoteCommand) -> Self {
        Message::RoleConsensus(RoleConsensusMessage::Command(RoleConsensusCommand::Vote(
            command,
        )))
    }
}

#[derive(Deserialize, Serialize)]
pub struct HeartbeatCommand {
    pub term: u32,
    pub sender: u32,
    pub receiver: u32,
}

impl From<HeartbeatCommand> for Message {
    fn from(command: HeartbeatCommand) -> Self {
        Message::RoleConsensus(RoleConsensusMessage::Command(
            RoleConsensusCommand::Heartbeat(command),
        ))
    }
}

#[derive(Deserialize, Serialize)]
pub struct RequestVoteCommand {
    pub term: u32,
    pub requester: u32,
    pub requestee: u32,
}

impl From<RequestVoteCommand> for Message {
    fn from(command: RequestVoteCommand) -> Self {
        Message::RoleConsensus(RoleConsensusMessage::Command(
            RoleConsensusCommand::RequestVote(command),
        ))
    }
}
