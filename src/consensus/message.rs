use crate::messaging::message::Message;
use serde::{Deserialize, Serialize};

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
