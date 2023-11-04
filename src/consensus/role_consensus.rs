use super::{
    message::{HeartbeatMessage, Message, RequestVoteMessage, VoteMessage},
    role::Role,
    term::TermId,
};
use crate::{bynamo_node::NodeId, membership::client::MembershipClient};
use crossbeam::channel::{Receiver, Sender};
use rand::{thread_rng, Rng};
use std::{
    collections::HashSet,
    thread,
    time::{Duration, Instant},
};

pub struct RoleConsensus {
    role: Role,
    term: TermId,
    voted_for: Option<NodeId>,
    votes_received: HashSet<NodeId>,
    membership_client: MembershipClient,
    id: NodeId,
    next_timeout: Instant,
    next_heartbeat: Instant,
    sender: Sender<Message>,
    receiver: Receiver<Message>,
    will_revive_at: Option<Instant>,
    will_die_at: Option<Instant>,
}

const HEARTBEAT_MILLISECONDS: u64 = 1000;
const ELECTION_TIMEOUT_MILLISECONDS: u64 = 5000;
const MAX_ALIVE_DURATION_MILLISECONDS: u64 = 30_000;
const MAX_DEAD_DURATION_MILLISECONDS: u64 = 10_000;
const MIN_NETWORK_DELAY_MILLISECONDS: u64 = 20;
const MAX_NETWORK_DELAY_MILLISECONDS: u64 = 50;

impl RoleConsensus {
    pub fn new(
        id: u32,
        membership_client: MembershipClient,
        sender: Sender<Message>,
        receiver: Receiver<Message>,
    ) -> Self {
        let mut node = RoleConsensus {
            role: Role::Follower,
            term: 0,
            voted_for: None,
            votes_received: HashSet::new(),
            membership_client,
            id,
            next_timeout: Instant::now(),
            next_heartbeat: Instant::now(),
            sender,
            receiver,
            will_die_at: None,
            will_revive_at: Some(Instant::now()),
        };
        node.set_next_timeout();
        node
    }

    fn set_next_timeout(&mut self) {
        self.next_timeout = Instant::now()
            + Duration::from_millis(thread_rng().gen_range(
                ELECTION_TIMEOUT_MILLISECONDS
                    ..self.membership_client.members().len() as u64 * 100
                        + ELECTION_TIMEOUT_MILLISECONDS,
            ));
    }

    fn set_next_heartbeat(&mut self) {
        self.next_heartbeat = Instant::now() + Duration::from_millis(HEARTBEAT_MILLISECONDS);
    }

    pub fn tick(&mut self) {
        if let Some(will_revive_at) = self.will_revive_at {
            if Instant::now() > will_revive_at {
                // Revive now, plan when to next die
                self.will_revive_at = None;
                self.will_die_at = Some(
                    Instant::now()
                        + Duration::from_millis(
                            thread_rng().gen_range(0..MAX_ALIVE_DURATION_MILLISECONDS),
                        ),
                );
            } else {
                return;
            }
        }

        if let Some(will_die_at) = self.will_die_at {
            if Instant::now() > will_die_at {
                // Die now, plan when to next revive
                self.will_die_at = None;
                self.will_revive_at = Some(
                    Instant::now()
                        + Duration::from_millis(
                            thread_rng().gen_range(0..MAX_DEAD_DURATION_MILLISECONDS),
                        ),
                );
                return;
            }
        }

        if Instant::now() > self.next_heartbeat && self.role == Role::Leader {
            self.send_heartbeat();
        }

        if Instant::now() > self.next_timeout && self.role != Role::Leader {
            self.on_timeout();
        }

        match self.receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(message) => self.receive_message(message),
            Err(_) => (),
        }
    }

    fn send_heartbeat(&mut self) {
        println!(
            "[{}]-{}: sending heartbeat, dieing in {}",
            self.id,
            self.term,
            (self.will_die_at.unwrap() - Instant::now()).as_millis()
        );

        for node_id in self.membership_client.members().iter() {
            if *node_id != self.id {
                self.sender
                    .send(Message::Heartbeat(HeartbeatMessage {
                        term: self.term,
                        sender: self.id,
                        receiver: *node_id,
                    }))
                    .unwrap();
            }
        }

        self.set_next_heartbeat();
    }

    fn on_timeout(&mut self) {
        println!("[{}]-{}: heartbeat timed out", self.id, self.term);

        match self.role {
            Role::Follower | Role::Candidate => {
                self.role = Role::Candidate;
                self.term += 1;
                self.voted_for = Some(self.id);
                self.votes_received.clear();
                self.votes_received.insert(self.id);
                self.set_next_timeout();

                println!("[{}]-{}: proposed self as candidate", self.id, self.term);

                for node_id in self.membership_client.members().iter() {
                    if *node_id != self.id {
                        self.sender
                            .send(Message::RequestVote(RequestVoteMessage {
                                term: self.term,
                                requester: self.id,
                                requestee: *node_id,
                            }))
                            .unwrap();
                    }
                }
            }
            _ => {}
        }
    }

    fn reset_term_as_follower(&mut self, term: TermId) {
        self.term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_received.clear();
        self.set_next_timeout();
    }

    fn vote_for(&mut self, votee: NodeId) {
        if self.voted_for.is_some() {
            panic!("Tried to vote twice");
        }
        println!("[{}]-{}: voting for {}", self.id, self.term, votee);
        self.voted_for = Some(votee);
        self.sender
            .send(Message::Vote(VoteMessage {
                term: self.term,
                voter: self.id,
                votee,
            }))
            .unwrap();

        self.set_next_timeout();
    }

    fn receive_message(&mut self, message: Message) {
        // Simulate delay on network
        thread::sleep(Duration::from_millis(thread_rng().gen_range(
            MIN_NETWORK_DELAY_MILLISECONDS..MAX_NETWORK_DELAY_MILLISECONDS,
        )));

        match message {
            Message::Vote(message) => self.receive_vote_message(message),
            Message::Heartbeat(message) => self.receive_heartbeat_message(message),
            Message::RequestVote(message) => self.receive_request_vote_message(message),
        }
    }

    fn receive_vote_message(&mut self, message: VoteMessage) {
        if message.term < self.term {
            println!(
                "[{}]-{}: ignoring note from previous term from {}",
                self.id, self.term, message.voter
            );
            return;
        }

        if message.term > self.term {
            println!(
                "[{}]-{}: received later term {} vote from {}, resetting as follower",
                self.id, self.term, message.term, message.voter
            );
            self.reset_term_as_follower(message.term);
            return;
        }

        if let Role::Candidate = self.role {
            self.votes_received.insert(message.voter);
            println!(
                "[{}]-{}: vote received from {}",
                self.id, self.term, message.voter
            );
            if self.votes_received.len() > self.membership_client.members().len() / 2 {
                println!("[{}]-{}: ascending to leadership", self.id, self.term);
                self.role = Role::Leader;
                self.send_heartbeat();
            }
        }
    }

    fn receive_heartbeat_message(&mut self, message: HeartbeatMessage) {
        if message.term < self.term {
            return;
        }

        if message.term > self.term {
            println!(
                "[{}]-{}: received later term {} heartbeat from {}, resetting as follower",
                self.id, self.term, message.term, message.sender
            );
            self.reset_term_as_follower(message.term);
        }

        self.set_next_timeout();
    }

    fn receive_request_vote_message(&mut self, message: RequestVoteMessage) {
        if message.term < self.term {
            println!(
                "[{}]-{}: ignoring note from previous term from {}",
                self.id, self.term, message.requester
            );
            return;
        }

        if message.term > self.term {
            println!(
                "[{}]-{}: received later term {} request vote from {}, resetting as follower",
                self.id, self.term, message.term, message.requester
            );
            self.reset_term_as_follower(message.term);
            self.vote_for(message.requester);
            return;
        }

        match self.voted_for {
            None => self.vote_for(message.requester),
            Some(voted) if voted == message.requester => self.vote_for(message.requester),
            _ => {}
        }
    }
}
