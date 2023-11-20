//use crate::messaging::message::{message as inner, Message, MessageType};

// tonic::include_proto!("consensus");

use crate::messaging::{
    node_message::Message, role_consensus_command, HeartbeatCommand, MessageType, NodeMessage,
    RequestVoteCommand, RoleConsensusCommand, RoleConsensusMessage, VoteCommand,
};

impl From<HeartbeatCommand> for NodeMessage {
    fn from(command: HeartbeatCommand) -> Self {
        NodeMessage {
            r#type: MessageType::RoleConsensus.into(),
            message: Some(Message::RoleConsensus(RoleConsensusMessage {
                command: Some(RoleConsensusCommand {
                    command: Some(role_consensus_command::Command::Heartbeat(command)),
                }),
            })),
        }
    }
}

impl From<RequestVoteCommand> for NodeMessage {
    fn from(command: RequestVoteCommand) -> Self {
        NodeMessage {
            r#type: MessageType::RoleConsensus.into(),
            message: Some(Message::RoleConsensus(RoleConsensusMessage {
                command: Some(RoleConsensusCommand {
                    command: Some(role_consensus_command::Command::RequestVote(command)),
                }),
            })),
        }
    }
}

impl From<VoteCommand> for NodeMessage {
    fn from(command: VoteCommand) -> Self {
        NodeMessage {
            r#type: MessageType::RoleConsensus.into(),
            message: Some(Message::RoleConsensus(RoleConsensusMessage {
                command: Some(RoleConsensusCommand {
                    command: Some(role_consensus_command::Command::Vote(command)),
                }),
            })),
        }
    }
}

// pub enum RoleConsensusMessage {
//     Command(RoleConsensusCommand),
// }

// impl RoleConsensusMessage {
//     pub fn message_id(&self) -> &String {
//         match self {
//             RoleConsensusMessage::Command(command) => command.message_id(),
//         }
//     }
// }

// pub enum RoleConsensusCommand {
//     Vote(VoteCommand),
//     Heartbeat(HeartbeatCommand),
//     RequestVote(RequestVoteCommand),
// }

// impl RoleConsensusCommand {
//     pub fn message_id(&self) -> &String {
//         match self {
//             RoleConsensusCommand::Vote(command) => &command.message_id,
//             RoleConsensusCommand::Heartbeat(command) => &command.message_id,
//             RoleConsensusCommand::RequestVote(command) => &command.message_id,
//         }
//     }
// }

// pub struct VoteCommand {
//     pub message_id: String,
//     pub term: TermId,
//     pub voter: NodeId,
//     pub votee: NodeId,
// }

// impl From<VoteCommand> for Message {
//     fn from(command: VoteCommand) -> Self {
//         Message::RoleConsensus(RoleConsensusMessage::Command(RoleConsensusCommand::Vote(
//             command,
//         )))
//     }
// }

// pub struct HeartbeatCommand {
//     pub message_id: String,
//     pub term: TermId,
//     pub sender: NodeId,
//     pub receiver: NodeId,
// }

// impl From<HeartbeatCommand> for Message {
//     fn from(command: HeartbeatCommand) -> Self {
//         Message::RoleConsensus(RoleConsensusMessage::Command(
//             RoleConsensusCommand::Heartbeat(command),
//         ))
//     }
// }

// pub struct RequestVoteCommand {
//     pub message_id: String,
//     pub term: TermId,
//     pub requester: NodeId,
//     pub requestee: NodeId,
// }

// impl From<RequestVoteCommand> for Message {
//     fn from(command: RequestVoteCommand) -> Self {
//         Message::RoleConsensus(RoleConsensusMessage::Command(
//             RoleConsensusCommand::RequestVote(command),
//         ))
//     }
// }
