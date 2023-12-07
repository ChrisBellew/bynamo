use serde::{Deserialize, Serialize};

pub type NodeId = u32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeNode<K, V>
where
    K: Clone,
{
    pub node_id: NodeId,
    pub keys: Vec<K>,
    pub values: Vec<V>,
    pub children: Vec<NodeId>,
}
