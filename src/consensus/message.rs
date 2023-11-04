use crate::bynamo_node::NodeId;

use super::term::TermId;

pub enum Message {
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
