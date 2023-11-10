use super::term::TermId;
use crate::bynamo_node::NodeId;
use async_trait::async_trait;
use std::fmt::Display;
use thiserror::Error;

pub enum RoleConsensusMessage {
    Vote(VoteMessage),
    Heartbeat(HeartbeatMessage),
    RequestVote(RequestVoteMessage),
}

pub struct VoteMessage {
    pub term: TermId,
    pub voter: NodeId,
    pub votee: NodeId,
}

pub struct HeartbeatMessage {
    pub term: TermId,
    pub sender: NodeId,
    pub receiver: NodeId,
}

pub struct RequestVoteMessage {
    pub term: TermId,
    pub requester: NodeId,
    pub requestee: NodeId,
}

#[async_trait]
pub trait ConsensusSender {
    async fn send(&self, message: RoleConsensusMessage) -> Result<(), MessageSendError>;
    async fn try_send(&self, message: RoleConsensusMessage) -> bool;
}

#[async_trait]
pub trait ConsensusReceiver {
    async fn recv(&self) -> Result<RoleConsensusMessage, MessageRecvError>;
    fn try_recv(&self) -> Option<RoleConsensusMessage>;
}

#[derive(Debug, Error)]
pub struct MessageSendError {}

impl Display for MessageSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MessageSendError")
    }
}

#[derive(Debug)]
pub struct MessageRecvError {}
