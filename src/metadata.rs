use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::{bynamo_node::NodeId, consensus::term::TermId};

#[async_trait]
pub trait MetadataService {
    async fn announce_new_term(&mut self, term: TermId);
    async fn announce_leader(&mut self, leader: NodeId, term: TermId);
    async fn leader(&self) -> Option<NodeId>;
}

#[derive(Clone)]
pub struct MemoryMetadataService {
    state: Arc<RwLock<State>>,
}

struct State {
    leader: Option<NodeId>,
    term: TermId,
}

impl MemoryMetadataService {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State {
                leader: None,
                term: 0,
            })),
        }
    }
}

#[async_trait]
impl MetadataService for MemoryMetadataService {
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
