use async_recursion::async_recursion;

use super::node::NodeId;
use super::node_store::{DeserializeNode, SerializeNode};
use super::tree::BTree;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::atomic::Ordering;

type MaxDepth = usize;
type KeyCount = usize;
type NodeCount = usize;

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Display + Debug + Clone + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn validate(&self)
    where
        K: PartialOrd + Clone + Debug + Display + Send + Sync,
        V: PartialEq + Clone + Debug + Send + Sync,
    {
        let root = self.root.read().await;
        let (_, key_count, _node_count) = self.validate_recursive(*root, true, None, None).await;
        let size = self.size.load(Ordering::SeqCst);
        if key_count != size {
            self.print().await;
            panic!("Tree size is invalid. Size: {}. Keys: {}.", size, key_count);
        }
    }

    #[async_recursion]
    async fn validate_recursive(
        &self,
        node: NodeId,
        root: bool,
        parent_key_left: Option<K>,
        parent_key_right: Option<K>,
    ) -> (MaxDepth, KeyCount, NodeCount)
    where
        K: PartialOrd + Clone + Debug + Display,
        V: Clone + Debug,
    {
        let node = self.store.get(node).await.unwrap();
        let node = node.read().await;
        let mut key_count = 0;
        key_count += node.keys.len();

        if !root && node.keys.len() < self.min_keys_per_node() {
            self.print().await;
            panic!(
                "Node has too few keys. Expected: {}. Found: {}.",
                self.min_keys_per_node(),
                node.keys.len()
            );
        }
        if node.keys.len() > self.max_keys_per_node {
            self.print().await;
            panic!(
                "Node has too many keys. Expected: {}. Found: {}.",
                self.max_keys_per_node,
                node.keys.len()
            );
        }
        if node.keys.len() != node.values.len() {
            self.print().await;
            panic!(
                "Node has an invalid number of values. Keys: {}. Values: {}.",
                node.keys.len(),
                node.values.len()
            );
        }
        if !node.children.is_empty() && node.children.len() != node.keys.len() + 1 {
            self.print().await;
            panic!(
                "Node {:?} has invalid number of children. Children: {}. Keys: {}.",
                node.keys,
                node.children.len(),
                node.keys.len()
            );
        }

        let mut max_depth = 0;
        let mut node_count = 1;
        for (i, child) in node.children.iter().enumerate() {
            let key_left = if i == 0 {
                None
            } else {
                Some(&node.keys[i - 1])
            };
            let key_right = if i == node.children.len() - 1 {
                None
            } else {
                Some(&node.keys[i])
            };
            let (child_max_depth, child_key_count, child_node_count) = self
                .validate_recursive(*child, false, key_left.cloned(), key_right.cloned())
                .await;
            if max_depth == 0 {
                max_depth = child_max_depth;
            } else if max_depth != child_max_depth {
                self.print().await;
                panic!(
                    "Node has mixed depth. A: {}. B: {}.",
                    max_depth, child_max_depth,
                );
            }
            key_count += child_key_count;
            node_count += child_node_count;
        }

        for (i, key) in node.keys.iter().enumerate() {
            if i > 0 {
                let left = node.keys.get(i - 1).unwrap();
                if left >= key {
                    self.print().await;
                    panic!(
                        "Left key isn't less than key. Left: {}. Key: {}.",
                        left, key,
                    );
                }
            }
            if let Some(parent_key_left) = &parent_key_left {
                if parent_key_left >= key {
                    self.print().await;
                    panic!(
                        "Parent key left isn't less than key. Parent key left: {}. Key: {}.",
                        parent_key_left, key,
                    );
                }
            }
            if let Some(parent_key_right) = &parent_key_right {
                if parent_key_right <= key {
                    self.print().await;
                    panic!(
                        "Parent key right isn't less than key. Parent key right: {}. Key: {}.",
                        parent_key_right, key,
                    );
                }
            }
        }

        (max_depth, key_count, node_count)
    }
}
