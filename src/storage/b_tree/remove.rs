use super::node::{BTreeNode, NodeId};
use super::node_store::{DeserializeNode, SerializeNode};
use super::tree::BTree;
use async_recursion::async_recursion;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::atomic::Ordering;
use tokio::sync::RwLockWriteGuard;

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Display + Debug + Clone + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn remove_from_root(&self, key: &K) {
        let mut root_node_id = self.root.write().await;
        match self.remove_recursive(*root_node_id, key).await {
            RemoveResult::Removed => {
                let root = self.store.get(*root_node_id).await.unwrap();
                let mut root = root.write().await;
                if root.keys.is_empty() && !root.children.is_empty() {
                    *root_node_id = root.children.remove(0);

                    // TODO: Shouldn't need to do this. Come back and revisit.
                    self.store.persist(root).await;
                }
                self.size.fetch_sub(1, Ordering::SeqCst);
            }
            RemoveResult::NotFound => {}
            RemoveResult::RemovedWithUnderflow => {
                let root = self.store.get(*root_node_id).await.unwrap();
                let mut root = root.write().await;
                if root.keys.is_empty() && !root.children.is_empty() {
                    *root_node_id = root.children.remove(0);

                    // TODO: Shouldn't need to do this. Come back and revisit.
                    self.store.persist(root).await;
                }
                self.size.fetch_sub(1, Ordering::SeqCst);
            }
        };
    }

    #[async_recursion]
    async fn remove_recursive(&self, node_id: NodeId, key: &K) -> RemoveResult {
        let node = self.store.get(node_id).await.unwrap();
        let mut node = node.write().await;
        let num_children = node.children.len();

        for i in 0..node.keys.len() {
            let node_key = &node.keys[i];

            if key < node_key {
                if !node.children.is_empty() {
                    return self.remove_in_subtree(node, i, key).await;
                }
                return RemoveResult::NotFound;
            }
            if key == node_key {
                node.keys.remove(i);
                node.values.remove(i);

                // Can we take the in order predecessor?
                if let Some(left_child) = node.children.get_mut(i) {
                    match self.take_predecessor_recursive(*left_child).await {
                        TakeResult::Taken(key, value) => {
                            node.keys.insert(i, key);
                            node.values.insert(i, value);
                            self.store.persist(node).await;
                            return RemoveResult::Removed;
                        }
                        TakeResult::WouldUnderflow(predecessor, predecessor_value) => {
                            node.keys.insert(i, predecessor.clone());
                            node.values.insert(i, predecessor_value);
                            return self.remove_in_subtree(node, i, &predecessor).await;
                        }
                    }
                }

                // Can we take the in order successor?
                if let Some(right_child) = node.children.get(i + 1) {
                    match self.take_successor_recursive(*right_child).await {
                        TakeResult::Taken(key, value) => {
                            node.keys.insert(i, key);
                            node.values.insert(i, value);
                            self.store.persist(node).await;
                            return RemoveResult::Removed;
                        }
                        TakeResult::WouldUnderflow(successor, successor_value) => {
                            node.keys.insert(i, successor.clone());
                            node.values.insert(i, successor_value.clone());
                            return self.remove_in_subtree(node, i + 1, &successor).await;
                        }
                    }
                }

                let result = if node.keys.len() < self.min_keys_per_node() {
                    RemoveResult::RemovedWithUnderflow
                } else {
                    RemoveResult::Removed
                };

                // Merge children
                if !node.children.is_empty() {
                    let left_child = self.store.get(node.children[i]).await.unwrap();
                    let mut left_child = left_child.write().await;
                    let removed = node.children.remove(i + 1);
                    let right_child = self.store.get(removed).await.unwrap();
                    let mut right_child = right_child.write().await;
                    left_child.keys.append(&mut right_child.keys);
                    left_child.children.append(&mut right_child.children);

                    self.store.persist(node).await;
                    self.store.persist(left_child).await;
                    self.store.remove(right_child).await;
                    return result;
                }

                self.store.persist(node).await;
                return result;
            }
        }

        if num_children > 0 {
            let result = self.remove_in_subtree(node, num_children - 1, key).await;
            return result;
        }

        RemoveResult::NotFound
    }

    async fn remove_in_subtree<'a>(
        &self,
        mut node: RwLockWriteGuard<'a, BTreeNode<K, V>>,
        i: usize,
        key: &K,
    ) -> RemoveResult {
        match self.remove_recursive(node.children[i], key).await {
            RemoveResult::Removed => RemoveResult::Removed,
            RemoveResult::NotFound => RemoveResult::NotFound,
            RemoveResult::RemovedWithUnderflow => {
                // Can we take from the left sibling?
                if i > 0 {
                    let left_sibling = self.store.get(node.children[i - 1]).await.unwrap();
                    let mut left_sibling = left_sibling.write().await;
                    let num_left_sibling_keys = left_sibling.keys.len();
                    if num_left_sibling_keys > self.min_keys_per_node() {
                        // Yes, move the last key up to the parent and the
                        // parent down to the underflowed node
                        let child = self.store.get(node.children[i]).await.unwrap();
                        let mut child = child.write().await;

                        let removed_key = left_sibling.keys.remove(num_left_sibling_keys - 1);
                        let removed_value = left_sibling.values.remove(num_left_sibling_keys - 1);
                        if !left_sibling.children.is_empty() {
                            let num_left_sibling_children = left_sibling.children.len();
                            let removed_child =
                                left_sibling.children.remove(num_left_sibling_children - 1);
                            child.children.insert(0, removed_child);
                        }

                        let left_key = node.keys.get_mut(i - 1).unwrap();
                        child.keys.insert(0, left_key.clone());
                        *left_key = removed_key;

                        let left_value = node.values.get_mut(i - 1).unwrap();
                        child.values.insert(0, left_value.clone());
                        *left_value = removed_value;

                        self.store.persist(node).await;
                        self.store.persist(left_sibling).await;
                        self.store.persist(child).await;
                        return RemoveResult::Removed;
                    }
                }

                // Can we take from the right sibling?
                if i < node.keys.len() {
                    let right_sibling = self.store.get(node.children[i + 1]).await.unwrap();
                    let mut right_sibling = right_sibling.write().await;
                    if right_sibling.keys.len() > self.min_keys_per_node() {
                        // Yes, move the last key up to the parent and the
                        // parent down to the underflowed node
                        let child = self.store.get(node.children[i]).await.unwrap();
                        let mut child = child.write().await;

                        let removed_key = right_sibling.keys.remove(0);
                        let removed_value = right_sibling.values.remove(0);
                        if !right_sibling.children.is_empty() {
                            let removed_child = right_sibling.children.remove(0);
                            child.children.push(removed_child);
                        }

                        let right_key = node.keys.get_mut(i).unwrap();
                        let parent_key = right_key.clone();
                        *right_key = removed_key;
                        child.keys.push(parent_key);

                        let right_value = node.values.get_mut(i).unwrap();
                        let parent_value = right_value.clone();
                        *right_value = removed_value;
                        child.values.push(parent_value);

                        self.store.persist(node).await;
                        self.store.persist(right_sibling).await;
                        self.store.persist(child).await;

                        return RemoveResult::Removed;
                    }
                }

                // No, we must merge

                // Is there a left sibling to merge with?
                if i > 0 {
                    // Yes, merge with the left sibling
                    self.merge_with_left_sibling(&mut node, i).await;
                    let num_keys = node.keys.len();
                    self.store.persist(node).await;
                    if num_keys < self.min_keys_per_node() {
                        return RemoveResult::RemovedWithUnderflow;
                    }
                    return RemoveResult::Removed;
                }

                // Yes, merge with the right sibling
                self.merge_with_right_sibling(&mut node, i).await;
                let num_keys = node.keys.len();
                self.store.persist(node).await;
                if num_keys < self.min_keys_per_node() {
                    return RemoveResult::RemovedWithUnderflow;
                }
                RemoveResult::Removed
            }
        }
    }

    async fn merge_with_left_sibling<'a>(
        &self,
        parent: &mut RwLockWriteGuard<'a, BTreeNode<K, V>>,
        child: usize,
    ) {
        let removed_node = self.store.get(parent.children.remove(child)).await.unwrap();
        let mut removed_node = removed_node.write().await;

        let removed_key = parent.keys.remove(child - 1);
        let removed_value = parent.values.remove(child - 1);

        let left_sibling = self.store.get(parent.children[child - 1]).await.unwrap();
        let mut left_sibling = left_sibling.write().await;
        left_sibling.keys.push(removed_key);
        left_sibling.keys.append(&mut removed_node.keys);
        left_sibling.values.push(removed_value);
        left_sibling.values.append(&mut removed_node.values);
        left_sibling.children.append(&mut removed_node.children);

        self.store.persist(removed_node).await;
        self.store.persist(left_sibling).await;
    }

    async fn merge_with_right_sibling<'a>(
        &self,
        parent: &mut RwLockWriteGuard<'a, BTreeNode<K, V>>,
        child: usize,
    ) {
        let right_sibling = self
            .store
            .get(parent.children.remove(child + 1))
            .await
            .unwrap();
        let mut right_sibling = right_sibling.write().await;

        let removed_key = parent.keys.remove(child);
        let removed_value = parent.values.remove(child);

        let child = self.store.get(parent.children[child]).await.unwrap();
        let mut child = child.write().await;
        child.keys.push(removed_key);
        child.keys.append(&mut right_sibling.keys);
        child.values.push(removed_value);
        child.values.append(&mut right_sibling.values);
        child.children.append(&mut right_sibling.children);

        self.store.persist(right_sibling).await;
        self.store.persist(child).await;
    }

    #[async_recursion]
    async fn take_predecessor_recursive(&self, node: NodeId) -> TakeResult<K, V> {
        let node = self.store.get(node).await.unwrap();
        let mut node = node.write().await;
        let num_keys = node.keys.len();
        let last_child = node.children.iter().last();
        match last_child {
            Some(last_child) => self.take_predecessor_recursive(*last_child).await,
            None => {
                if num_keys > self.min_keys_per_node() {
                    let removed_key = node.keys.remove(num_keys - 1);
                    let removed_value = node.values.remove(num_keys - 1);
                    self.store.persist(node).await;
                    return TakeResult::Taken(removed_key, removed_value);
                }
                return TakeResult::WouldUnderflow(
                    node.keys[num_keys - 1].clone(),
                    node.values[num_keys - 1].clone(),
                );
            }
        }
    }

    #[async_recursion]
    async fn take_successor_recursive(&self, node: NodeId) -> TakeResult<K, V> {
        let node = self.store.get(node).await.unwrap();
        let mut node = node.write().await;
        let num_keys = node.keys.len();
        let first_child = node.children.last();
        match first_child {
            Some(first_child) => self.take_successor_recursive(*first_child).await,
            None => {
                if num_keys > self.min_keys_per_node() {
                    let removed_key = node.keys.remove(0);
                    let removed_value = node.values.remove(0);
                    self.store.persist(node).await;
                    return TakeResult::Taken(removed_key, removed_value);
                }
                return TakeResult::WouldUnderflow(node.keys[0].clone(), node.values[0].clone());
            }
        }
    }
}

enum RemoveResult {
    Removed,
    RemovedWithUnderflow,
    NotFound,
}

enum TakeResult<K, V> {
    Taken(K, V),
    WouldUnderflow(K, V),
}

#[cfg(test)]
mod tests {
    use super::super::node::BTreeNode;
    use super::super::node_store::{DeserializeNode, I32NodeSerializer, NodeStore, SerializeNode};
    use super::super::tree::BTree;
    use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};
    use rand::prelude::SliceRandom;
    use rand::Rng;
    use std::collections::HashSet;
    use std::fmt::{Debug, Display};
    use std::path::Path;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tokio::fs::remove_file;
    use tokio::sync::RwLock;

    async fn create_store(test_name: &str) -> NodeStore<i32, i32, I32NodeSerializer> {
        let serializer = I32NodeSerializer::new();
        let path = &format!("scratch/{}.db", test_name);
        // Delete path if exists
        if Path::new(path).exists() {
            remove_file(path).await.unwrap();
        }
        NodeStore::new_disk(path, serializer, 4, 4, 1, 1, &Registry::new()).await
    }

    fn create_btree<K, V, S>(size: u32, store: NodeStore<K, V, S>) -> BTree<K, V, S>
    where
        K: PartialOrd + Clone + Debug + Display + Send + Sync,
        V: PartialEq + Clone + Debug + Send + Sync,
        S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
    {
        let adds_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_adds_histogram",
                "Btree add durations in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();

        BTree {
            max_keys_per_node: 5,
            size: Arc::new((size as usize).into()),
            root: Arc::new(RwLock::new(0)),
            store,
            next_node_id: Arc::new(size.into()),
            adds_histogram: adds_histogram.clone(),
            add_root_lock_wait_histogram: adds_histogram.clone(),
            add_node_lock_wait_histogram: adds_histogram.clone(),
            add_get_wait_histogram: adds_histogram.clone(),
            add_persist_wait_histogram: adds_histogram.clone(),
            add_insert_wait_histogram: adds_histogram.clone(),
            add_remaining_histogram: adds_histogram.clone(),
        }
    }

    #[tokio::test]
    async fn btree_remove_only_key_in_only_node() {
        let mut store = create_store("btree_remove_only_key_in_only_node").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![10],
                values: vec![10],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(1, store);

        remove_and_validate(&mut tree, 10).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_only_node() {
        let mut store = create_store("btree_remove_first_key_in_only_node").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(2, store);

        remove_and_validate(&mut tree, 10).await;
    }

    #[tokio::test]
    async fn btree_remove_middle_key_in_only_node() {
        let mut store = create_store("btree_remove_middle_key_in_only_node").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![10, 11, 12],
                values: vec![10, 11, 12],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(3, store);

        remove_and_validate(&mut tree, 11).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_only_node() {
        let mut store = create_store("btree_remove_last_key_in_only_node").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![10, 11, 12],
                values: vec![10, 11, 12],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(3, store);

        remove_and_validate(&mut tree, 12).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_leaf_without_underflow() {
        let mut store = create_store("btree_remove_first_key_in_leaf_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13, 14],
                values: vec![11, 12, 13, 14],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(12, store);

        remove_and_validate(&mut tree, 11).await;
    }

    #[tokio::test]
    async fn btree_remove_middle_key_in_leaf_without_underflow() {
        let mut store = create_store("btree_remove_middle_key_in_leaf_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13, 14],
                values: vec![11, 12, 13, 14],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(12, store);

        remove_and_validate(&mut tree, 12).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_leaf_without_underflow() {
        let mut store = create_store("btree_remove_last_key_in_leaf_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13, 14],
                values: vec![11, 12, 13, 14],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(12, store);

        remove_and_validate(&mut tree, 14).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_first_leaf_with_underflow_take_from_right_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_first_key_in_first_leaf_with_underflow_take_from_right_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 11).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_first_leaf_with_underflow_take_from_right_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_last_key_in_first_leaf_with_underflow_take_from_right_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 12).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_middle_leaf_with_underflow_take_from_left_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_first_key_in_middle_leaf_with_underflow_take_from_left_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13],
                values: vec![11, 12, 13],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 21).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_middle_leaf_with_underflow_take_from_left_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_last_key_in_middle_leaf_with_underflow_take_from_left_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13],
                values: vec![11, 12, 13],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 22).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_middle_leaf_with_underflow_take_from_right_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_first_key_in_middle_leaf_with_underflow_take_from_right_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 21).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_middle_leaf_with_underflow_take_from_right_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_last_key_in_middle_leaf_with_underflow_take_from_right_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 22).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_last_leaf_with_underflow_take_from_left_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_first_key_in_last_leaf_with_underflow_take_from_left_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 31).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_last_leaf_with_underflow_take_from_left_sibling_without_underflow(
    ) {
        let mut store = create_store("btree_remove_last_key_in_last_leaf_with_underflow_take_from_left_sibling_without_underflow").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 32).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_first_leaf_with_underflow_cant_take_from_siblings_merge_with_right(
    ) {
        let mut store = create_store("btree_remove_first_key_in_first_leaf_with_underflow_cant_take_from_siblings_merge_with_right").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 11).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_first_leaf_with_underflow_cant_take_from_siblings_merge_with_right(
    ) {
        let mut store = create_store("btree_remove_last_key_in_first_leaf_with_underflow_cant_take_from_siblings_merge_with_right").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 12).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_middle_leaf_with_underflow_cant_take_from_siblings_merge_with_left(
    ) {
        let mut store = create_store("btree_remove_first_key_in_middle_leaf_with_underflow_cant_take_from_siblings_merge_with_left").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 21).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_middle_leaf_with_underflow_cant_take_from_siblings_merge_with_left(
    ) {
        let mut store = create_store("btree_remove_last_key_in_middle_leaf_with_underflow_cant_take_from_siblings_merge_with_left").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 22).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_last_leaf_with_underflow_cant_take_from_siblings_merge_with_left(
    ) {
        let mut store = create_store("btree_remove_first_key_in_last_leaf_with_underflow_cant_take_from_siblings_merge_with_left").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 31).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_last_leaf_with_underflow_cant_take_from_siblings_merge_with_left(
    ) {
        let mut store = create_store("btree_remove_last_key_in_last_leaf_with_underflow_cant_take_from_siblings_merge_with_left").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 32).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_internal_without_underflow_take_predecessor() {
        let mut store =
            create_store("btree_remove_first_key_in_internal_without_underflow_take_predecessor")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12, 13],
                values: vec![11, 12, 13],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 20).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_internal_without_underflow_take_predecessor() {
        let mut store =
            create_store("btree_remove_last_key_in_internal_without_underflow_take_predecessor")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22, 23],
                values: vec![21, 22, 23],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 30).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_internal_without_underflow_take_successor() {
        let mut store =
            create_store("btree_remove_last_key_in_internal_without_underflow_take_successor")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32, 33],
                values: vec![31, 32, 33],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(9, store);

        remove_and_validate(&mut tree, 30).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_internal_with_underflow_merge_children() {
        let mut store =
            create_store("btree_remove_first_key_in_internal_with_underflow_merge_children").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 20).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_internal_with_underflow_merge_children() {
        let mut store =
            create_store("btree_remove_last_key_in_internal_with_underflow_merge_children").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![20, 30],
                values: vec![20, 30],
                children: vec![1, 2, 3],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![11, 12],
                values: vec![11, 12],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![21, 22],
                values: vec![21, 22],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![31, 32],
                values: vec![31, 32],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(8, store);

        remove_and_validate(&mut tree, 30).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_first_internal_with_underflow_take_from_right_sibling() {
        let mut store = create_store(
            "btree_remove_first_key_in_first_internal_with_underflow_take_from_right_sibling",
        )
        .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![60],
                values: vec![60],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![80, 100, 120],
                values: vec![80, 100, 120],
                children: vec![6, 7, 8, 9],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![70, 71],
                values: vec![70, 71],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 9,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(20, store);

        remove_and_validate(&mut tree, 20).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_first_internal_with_underflow_take_from_right_sibling() {
        let mut store = create_store(
            "btree_remove_last_key_in_first_internal_with_underflow_take_from_right_sibling",
        )
        .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![60],
                values: vec![60],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![80, 100, 120],
                values: vec![80, 100, 120],
                children: vec![6, 7, 8, 9],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![70, 71],
                values: vec![70, 71],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 9,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(20, store);

        remove_and_validate(&mut tree, 40).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_last_internal_with_underflow_take_from_left_sibling() {
        let mut store = create_store(
            "btree_remove_first_key_in_last_internal_with_underflow_take_from_left_sibling",
        )
        .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40, 60],
                values: vec![20, 40, 60],
                children: vec![3, 4, 5, 6],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![7, 8, 9],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![70, 71],
                values: vec![70, 71],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 9,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(20, store);

        remove_and_validate(&mut tree, 100).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_last_internal_with_underflow_take_from_left_sibling() {
        let mut store = create_store(
            "btree_remove_last_key_in_last_internal_with_underflow_take_from_left_sibling",
        )
        .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40, 60],
                values: vec![20, 40, 60],
                children: vec![3, 4, 5, 6],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![7, 8, 9],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![70, 71],
                values: vec![70, 71],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 9,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(20, store);

        remove_and_validate(&mut tree, 120).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_first_internal_with_underflow_pull_parent() {
        let mut store =
            create_store("btree_remove_first_key_in_first_internal_with_underflow_pull_parent")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![6, 7, 8],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(17, store);

        remove_and_validate(&mut tree, 20).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_first_internal_with_underflow_pull_parent() {
        let mut store =
            create_store("btree_remove_last_key_in_first_internal_with_underflow_pull_parent")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![6, 7, 8],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(17, store);

        remove_and_validate(&mut tree, 40).await;
    }

    #[tokio::test]
    async fn btree_remove_first_key_in_last_internal_with_underflow_pull_parent() {
        let mut store =
            create_store("btree_remove_first_key_in_last_internal_with_underflow_pull_parent")
                .await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![6, 7, 8],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(17, store);

        remove_and_validate(&mut tree, 100).await;
    }

    #[tokio::test]
    async fn btree_remove_last_key_in_last_internal_with_underflow_pull_parent() {
        let mut store =
            create_store("btree_remove_last_key_in_last_internal_with_underflow_pull_parent").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![6, 7, 8],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(17, store);

        remove_and_validate(&mut tree, 120).await;
    }

    #[tokio::test]
    async fn btree_remove_root_in_three_layer_tree() {
        let mut store = create_store("btree_remove_root_in_three_layer_tree").await;
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: vec![80],
                values: vec![80],
                children: vec![1, 2],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 1,
                keys: vec![20, 40],
                values: vec![20, 40],
                children: vec![3, 4, 5],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 2,
                keys: vec![100, 120],
                values: vec![100, 120],
                children: vec![6, 7, 8],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 3,
                keys: vec![10, 11],
                values: vec![10, 11],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 4,
                keys: vec![30, 31],
                values: vec![30, 31],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 5,
                keys: vec![50, 51],
                values: vec![50, 51],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 6,
                keys: vec![90, 91],
                values: vec![90, 91],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 7,
                keys: vec![110, 111],
                values: vec![110, 111],
                children: vec![],
            })
            .await;
        store
            .insert(BTreeNode {
                node_id: 8,
                keys: vec![130, 131],
                values: vec![130, 131],
                children: vec![],
            })
            .await;
        let mut tree = create_btree(17, store);

        remove_and_validate(&mut tree, 80).await;
    }

    #[tokio::test]
    async fn btree_remove_fuzz() {
        for _ in 0..10000 {
            let store: NodeStore<i32, i32, I32NodeSerializer> =
                NodeStore::<i32, i32, I32NodeSerializer>::new_memory();
            let mut tree =
                BTree::<i32, i32, I32NodeSerializer>::new(5, store, &Registry::new()).await;
            let mut added = HashSet::new();

            for _ in 0..rand::thread_rng().gen_range(1..100) {
                let key = rand::thread_rng().gen_range(0..100);
                tree.add(key, key).await;
                added.insert(key);
                assert!(tree.exists(key).await);

                // Validate with cache intact
                tree.validate().await;

                // Clear cache
                tree.store.flush().await;
                tree.store.clear_cache();

                // // Validate with cache cleared
                tree.validate().await;
            }

            let mut keys = tree.list_keys().await;
            let mut added = added.into_iter().collect::<Vec<_>>();
            added.sort();
            assert_eq!(keys, added);
            assert_eq!(tree.size.load(Ordering::SeqCst), keys.len());
            keys.shuffle(&mut rand::thread_rng());

            for key in keys {
                tree.remove(&key).await;
                assert!(!tree.exists(key).await);

                // Validate with cache intact
                tree.validate().await;

                // Clear cache
                tree.store.flush().await;
                tree.store.clear_cache();

                // Validate with cache cleared
                tree.validate().await;
            }
            assert_eq!(tree.list_keys().await, vec![]);
        }
    }

    async fn remove_and_validate<K, V, S>(tree: &mut BTree<K, V, S>, key_to_remove: K)
    where
        K: PartialOrd + Clone + Display + Debug + Send + Sync,
        V: PartialEq + Clone + Debug + Send + Sync,
        S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
    {
        tree.print().await;
        tree.validate().await;
        let keys_before = tree.list_keys().await;
        assert_eq!(keys_before.len(), tree.size.load(Ordering::SeqCst),);
        tree.remove(&key_to_remove).await;
        tree.validate().await;
        let keys_after = tree.list_keys().await;
        assert_eq!(
            keys_after,
            keys_before
                .into_iter()
                .filter(|key| *key != key_to_remove)
                .collect::<Vec<_>>()
        );
    }
}
