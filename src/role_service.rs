use crate::{bynamo_node::NodeId, consensus::term::TermId};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub enum RoleService {
    Memory(MemoryRoleService),
}

impl RoleService {
    pub fn new_memory() -> Self {
        Self::Memory(MemoryRoleService::new())
    }
    pub async fn announce_new_term(&mut self, term: TermId) {
        match self {
            RoleService::Memory(service) => service.announce_new_term(term).await,
        }
    }
    pub async fn announce_leader(&mut self, leader: NodeId, term: TermId) {
        match self {
            RoleService::Memory(service) => service.announce_leader(leader, term).await,
        }
    }
    pub async fn leader(&self) -> Option<NodeId> {
        match self {
            RoleService::Memory(service) => service.leader().await,
        }
    }
}

#[derive(Clone)]
pub struct MemoryRoleService {
    state: Arc<RwLock<State>>,
}

struct State {
    leader: Option<NodeId>,
    term: TermId,
}

impl MemoryRoleService {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State {
                leader: None,
                term: 0,
            })),
        }
    }
    async fn announce_new_term(&mut self, term: TermId) {
        let mut state = self.state.write().await;
        if term > state.term {
            state.term = term;
            state.leader = None;
        }
    }
    async fn announce_leader(&mut self, leader: NodeId, term: TermId) {
        let mut state = self.state.write().await;
        if term < state.term {
            return;
        }
        if term > state.term {
            state.term = term;
        }
        state.leader = Some(leader);
    }
    async fn leader(&self) -> Option<NodeId> {
        let state = self.state.read().await;
        state.leader
    }
}
