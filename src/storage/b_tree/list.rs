use async_recursion::async_recursion;

use super::{
    node::NodeId,
    node_store::{DeserializeNode, SerializeNode},
    tree::BTree,
};
use std::fmt::{Debug, Display};

impl<K, V, S> BTree<K, V, S>
where
    K: Ord + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Display + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn list_keys(&self) -> Vec<K> {
        let mut keys = Vec::new();
        let root = self.root.read().await;
        self.list_keys_recursive(*root, &mut keys).await;
        keys
    }

    #[async_recursion]
    pub async fn list_keys_recursive(&self, node_id: NodeId, keys: &mut Vec<K>) {
        let node = self.store.get(node_id).await.unwrap();
        let node = node.read().await;
        let num_children = node.children.len();

        if num_children == 0 {
            keys.extend(node.keys.iter().cloned());
            return;
        }
        for (i, key) in node.keys.iter().enumerate() {
            self.list_keys_recursive(node.children[i], keys).await;
            keys.push(key.clone());
        }

        self.list_keys_recursive(node.children[num_children - 1], keys)
            .await;
    }
}
