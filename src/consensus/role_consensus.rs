use super::{role::Role, term::TermId};
use crate::messaging::role_consensus_command::Command;
use crate::messaging::sender::MessageSender;
use crate::messaging::{HeartbeatCommand, RequestVoteCommand, RoleConsensusMessage, VoteCommand};
use crate::{bynamo_node::NodeId, role_service::RoleService};
use rand::{thread_rng, Rng};
use std::{
    collections::HashSet,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct RoleConsensus {
    consensus: Arc<RwLock<Consensus>>,
}

impl RoleConsensus {
    pub fn new(
        id: u32,
        members: Vec<NodeId>,
        role_service: RoleService,
        message_sender: MessageSender,
    ) -> Self {
        Self {
            consensus: Arc::new(RwLock::new(Consensus::new(
                id,
                members,
                role_service,
                message_sender,
            ))),
        }
    }

    pub async fn tick(&self) {
        self.consensus.write().await.tick().await;
    }

    pub async fn handle_message(&self, message: RoleConsensusMessage) {
        self.consensus.write().await.handle_message(message).await;
    }
}

struct Consensus {
    role: Role,
    term: TermId,
    voted_for: Option<NodeId>,
    votes_received: HashSet<NodeId>,
    members: Vec<NodeId>,
    role_service: RoleService,
    message_sender: MessageSender,
    id: NodeId,
    next_timeout: Instant,
    next_heartbeat: Instant,
    will_revive_at: Option<Instant>,
    will_die_at: Option<Instant>,
    started: Instant,
}

const HEARTBEAT_MILLISECONDS: u64 = 1000;
const ELECTION_TIMEOUT_MILLISECONDS: u64 = 3000;
const MAX_ALIVE_DURATION_MILLISECONDS: u64 = 30_000;
const MAX_DEAD_DURATION_MILLISECONDS: u64 = 10_000;
// const MIN_NETWORK_DELAY_MILLISECONDS: u64 = 0;
// const MAX_NETWORK_DELAY_MILLISECONDS: u64 = 0;

impl Consensus {
    pub fn new(
        id: u32,
        members: Vec<NodeId>,
        role_service: RoleService,
        message_sender: MessageSender,
    ) -> Self {
        let mut node = Consensus {
            role: Role::Follower,
            term: 0,
            voted_for: None,
            votes_received: HashSet::new(),
            members,
            role_service,
            message_sender,
            id,
            next_timeout: Instant::now(),
            next_heartbeat: Instant::now(),
            will_die_at: None,
            will_revive_at: Some(Instant::now()),
            started: Instant::now(),
        };
        node.set_next_timeout();
        node
    }

    fn set_next_timeout(&mut self) {
        self.next_timeout = Instant::now()
            + Duration::from_millis(thread_rng().gen_range(
                ELECTION_TIMEOUT_MILLISECONDS
                    ..self.members.len() as u64 * 100 + ELECTION_TIMEOUT_MILLISECONDS,
            ));
    }

    fn set_next_heartbeat(&mut self) {
        self.next_heartbeat = Instant::now() + Duration::from_millis(HEARTBEAT_MILLISECONDS);
    }

    pub async fn tick(&mut self) {
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
            self.send_heartbeat().await;
        }

        if Instant::now() > self.next_timeout && self.role != Role::Leader {
            self.on_timeout().await;
        }
    }

    async fn send_heartbeat(&mut self) {
        println!(
            "{:7>0}: [{}]-{}: sending heartbeat, dieing in {}",
            (Instant::now() - self.started).as_millis(),
            self.id,
            self.term,
            (self.will_die_at.unwrap() - Instant::now()).as_millis()
        );

        for node_id in self.members.iter() {
            if *node_id != self.id {
                let term = self.term;
                let sender = self.id;
                let receiver = *node_id;
                self.message_sender
                    .send_and_forget(
                        receiver,
                        HeartbeatCommand {
                            term,
                            sender,
                            receiver,
                        }
                        .into(),
                    )
                    .await;
            }
        }

        self.set_next_heartbeat();
    }

    async fn on_timeout(&mut self) {
        println!(
            "{:7>0}: [{}]-{}: heartbeat timed out",
            (Instant::now() - self.started).as_millis(),
            self.id,
            self.term
        );

        match self.role {
            Role::Follower | Role::Candidate => {
                self.role = Role::Candidate;
                self.term += 1;
                self.voted_for = Some(self.id);
                self.votes_received.clear();
                self.votes_received.insert(self.id);
                self.set_next_timeout();
                self.role_service.announce_new_term(self.term).await;

                println!(
                    "{:7>0}: [{}]-{}: proposed self as candidate",
                    (Instant::now() - self.started).as_millis(),
                    self.id,
                    self.term
                );

                // println!(
                //     "(self.members)()) {}",
                //     self.members.len()
                // );

                for node_id in self.members.iter() {
                    if *node_id != self.id {
                        println!(
                            "{:7>0}: [{}]-{}: requesting vote from {}",
                            (Instant::now() - self.started).as_millis(),
                            self.id,
                            self.term,
                            node_id
                        );
                        self.message_sender
                            .send_and_forget(
                                *node_id,
                                RequestVoteCommand {
                                    term: self.term,
                                    requester: self.id,
                                    requestee: *node_id,
                                }
                                .into(),
                            )
                            .await;
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

    async fn vote_for(&mut self, votee: NodeId) {
        if self.voted_for.is_some() {
            panic!("Tried to vote twice");
        }

        self.set_next_timeout();

        println!(
            "{:7>0}: [{}]-{}: voting for {}",
            (Instant::now() - self.started).as_millis(),
            self.id,
            self.term,
            votee
        );
        self.voted_for = Some(votee);
        let term = self.term;
        let voter = self.id;

        self.message_sender
            .send_and_forget(votee, VoteCommand { term, voter, votee }.into())
            .await;
    }

    pub async fn handle_message(&mut self, message: RoleConsensusMessage) {
        // Simulate delay on network
        // thread::sleep(Duration::from_millis(thread_rng().gen_range(
        //     MIN_NETWORK_DELAY_MILLISECONDS..MAX_NETWORK_DELAY_MILLISECONDS,
        // )));

        match message.command.unwrap().command.unwrap() {
            Command::Vote(command) => self.receive_vote_command(command).await,
            Command::Heartbeat(command) => self.receive_heartbeat_command(command),
            Command::RequestVote(command) => self.receive_request_vote_command(command).await,
        }

        // match message.command {
        //     RoleConsensusMessage::Command(command) => match command {
        //         RoleConsensusCommand::Vote(command) => self.receive_vote_command(command).await,
        //         RoleConsensusCommand::Heartbeat(command) => self.receive_heartbeat_command(command),
        //         RoleConsensusCommand::RequestVote(command) => {
        //             self.receive_request_vote_command(command).await
        //         }
        //     },
        // }
    }

    async fn receive_vote_command(&mut self, command: VoteCommand) {
        if command.term < self.term {
            println!(
                "{:7>0}: [{}]-{}: ignoring note from previous term from {}",
                (Instant::now() - self.started).as_millis(),
                self.id,
                self.term,
                command.voter
            );
            return;
        }

        if command.term > self.term {
            println!(
                "{:7>0}: [{}]-{}: received later term {} vote from {}, resetting as follower",
                (Instant::now() - self.started).as_millis(),
                self.id,
                self.term,
                command.term,
                command.voter
            );
            self.reset_term_as_follower(command.term);
            return;
        }

        if let Role::Candidate = self.role {
            self.votes_received.insert(command.voter);
            println!(
                "{:7>0}: [{}]-{}: vote received from {}",
                (Instant::now() - self.started).as_millis(),
                self.id,
                self.term,
                command.voter
            );
            if self.votes_received.len() > self.members.len() / 2 {
                println!(
                    "{:7>0}: [{}]-{}: ascending to leadership",
                    (Instant::now() - self.started).as_millis(),
                    self.id,
                    self.term
                );
                self.role = Role::Leader;
                self.send_heartbeat().await;
                self.role_service.announce_leader(self.id, self.term).await;
            }
        }
    }

    fn receive_heartbeat_command(&mut self, command: HeartbeatCommand) {
        if command.term < self.term {
            return;
        }

        if command.term > self.term {
            println!(
                "{:7>0}: [{}]-{}: received later term {} heartbeat from {}, resetting as follower",
                (Instant::now() - self.started).as_millis(),
                self.id,
                self.term,
                command.term,
                command.sender
            );
            self.reset_term_as_follower(command.term);
        }

        self.set_next_timeout();
    }

    async fn receive_request_vote_command(&mut self, command: RequestVoteCommand) {
        if command.term < self.term {
            println!(
                "{:7>0}: [{}]-{}: ignoring note from previous term from {}",
                (Instant::now() - self.started).as_millis(),
                self.id,
                self.term,
                command.requester
            );
            return;
        }

        if command.term > self.term {
            println!(
                "{:7>0}: [{}]-{}: received later term {} request vote from {}, resetting as follower",
                (Instant::now() - self.started).as_millis(),self.id, self.term, command.term, command.requester
            );
            self.reset_term_as_follower(command.term);
            self.vote_for(command.requester).await;
            return;
        }

        match self.voted_for {
            None => self.vote_for(command.requester).await,
            Some(voted) if voted == command.requester => self.vote_for(command.requester).await,
            _ => {}
        }
    }
}
