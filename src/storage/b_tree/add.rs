use super::{
    node::{BTreeNode, NodeId},
    node_store::{DeserializeNode, SerializeNode},
    tree::BTree,
};
use async_recursion::async_recursion;
use std::{
    fmt::{Debug, Display},
    sync::{atomic::Ordering, Arc, Mutex},
};
use stopwatch::Stopwatch;
use tokio::sync::RwLock;
use tokio::sync::RwLockWriteGuard;

enum AddResult<K, V>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
{
    AddedToNode,
    AddedToNodeAndSplit(K, V, NodeId),
    AddedToDescendant,
    NotAdded,
}

#[derive(Default)]
struct Timing {
    root_lock: Stopwatch,
    node_lock: Stopwatch,
    get: Stopwatch,
    insert: Stopwatch,
    persist: Stopwatch,
    all: Stopwatch,
}

// struct Locks<'a, K, V>
// where
//     K: PartialOrd + Clone + Debug + Display + Send + Sync,
//     V: PartialEq + Clone + Debug + Send + Sync,
// {
//     root: Option<RwLockWriteGuard<'a, u32>>,
//     nodes: Vec<&'a mut Option<RwLockWriteGuard<'a, BTreeNode<K, V>>>>,
// }

enum NodeLock<'a, K, V>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
{
    Root(RwLockWriteGuard<'a, u32>),
    Node(RwLockWriteGuard<'a, BTreeNode<K, V>>),
}

impl<'a, K, V> NodeLock<'a, K, V>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
{
    fn into_root(self) -> RwLockWriteGuard<'a, u32> {
        match self {
            NodeLock::Root(root) => root,
            NodeLock::Node(_) => panic!("Expected root lock"),
        }
    }
    fn into_node(self) -> RwLockWriteGuard<'a, BTreeNode<K, V>> {
        match self {
            NodeLock::Root(_) => panic!("Expected node lock"),
            NodeLock::Node(node) => node,
        }
    }
}

struct Nodes<K, V>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
{
    nodes: Vec<Arc<RwLock<BTreeNode<K, V>>>>,
}

// struct NodeLock<'a, K, V>
// where
//     K: PartialOrd + Clone + Debug + Display + Send + Sync,
//     V: PartialEq + Clone + Debug + Send + Sync,
// {
//     lock: Option<RwLockWriteGuard<'a, BTreeNode<K, V>>>,
//     parent: Option<Arc<Mutex<NodeLock<'a, K, V>>>>,
//     root: Option<RwLockWriteGuard<'a, u32>>,
// }

// impl<'a, K, V> NodeLock<'a, K, V>
// where
//     K: PartialOrd + Clone + Debug + Display + Send + Sync,
//     V: PartialEq + Clone + Debug + Send + Sync,
// {
//     fn new_with_root(root: RwLockWriteGuard<'a, u32>) -> NodeLock<'a, K, V> {
//         NodeLock {
//             lock: None,
//             parent: None,
//             root: Some(root),
//         }
//     }
//     fn new_with_parent(
//         lock: RwLockWriteGuard<'a, BTreeNode<K, V>>,
//         parent: Arc<Mutex<NodeLock<'a, K, V>>>,
//     ) -> NodeLock<'a, K, V> {
//         NodeLock {
//             lock: Some(lock),
//             parent: Some(parent),
//             root: None,
//         }
//     }
//     fn release_above(&mut self) {
//         if let Some(parent) = &self.parent {
//             let mut parent = parent.lock().unwrap();
//             parent.lock.take();
//             parent.root.take();
//             parent.release_above();
//         }
//     }
//     fn into_lock(self) -> RwLockWriteGuard<'a, BTreeNode<K, V>> {
//         self.lock.unwrap()
//     }
//     fn into_root(self) -> RwLockWriteGuard<'a, u32> {
//         self.root.unwrap()
//     }
// }

// struct LockWrapper<T> {
//     lock: Option<RwLockWriteGuard<'a, BTreeNode<K, V>>>,
// }

// impl<'a, K, V> Locks<'a, K, V>
// where
//     K: PartialOrd + Clone + Debug + Display + Send + Sync,
//     V: PartialEq + Clone + Debug + Send + Sync,
// {
//     fn new_with_root(root: RwLockWriteGuard<'a, u32>) -> Locks<'a, K, V> {
//         Locks {
//             root: Some(root),
//             nodes: Vec::new(),
//         }
//     }
//     fn into_root(self) -> RwLockWriteGuard<'a, u32> {
//         self.root.unwrap()
//     }
//     fn push(&mut self, node: &mut Option<RwLockWriteGuard<'a, BTreeNode<K, V>>>) {
//         self.nodes.push(node);
//     }
//     fn pop(&mut self) {
//         self.nodes.pop().unwrap()
//     }
//     fn release_above(&mut self) {
//         if self.nodes.len() > 0 {
//             self.nodes.drain(0..self.nodes.len() - 1);
//         }
//         self.root = None;
//     }
// }

// add_root_lock_waiters_count: Arc::new(0.into()),
// add_root_lock_waiters_gauge,
// add_node_lock_waiters_count: Arc::new(0.into()),
// add_node_lock_waiters_gauge,
// add_get_waiters_count: Arc::new(0.into()),
// add_get_waiters_gauge,
// add_persist_waiters_count: Arc::new(0.into()),
// add_persist_waiters_gauge,
// add_insert_waiters_count: Arc::new(0.into()),
// add_insert_waiters_gauge,
// add_workers_count: Arc::new(0.into()),
// add_workers_gauge,

struct HeldLocks<'a, K, V>
where
    K: Clone,
{
    root_lock: RwLockWriteGuard<'a, u32>,
    node_locks: Vec<RwLockWriteGuard<'a, BTreeNode<K, V>>>,
}

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn add_from_root(&self, key: K, value: V) {
        self.add_workers_count.fetch_add(1, Ordering::Relaxed);
        let mut timing: Timing = Timing::default();
        timing.all.start();

        timing.root_lock.start();
        self.add_root_lock_waiters_count
            .fetch_add(1, Ordering::Relaxed);
        let root = self.root.write().await;
        self.add_root_lock_waiters_count
            .fetch_sub(1, Ordering::Relaxed);
        timing.root_lock.stop();

        let root_id = *root;

        //let root_lock = NodeLock::Root(root);
        //let nodes = Nodes { nodes: Vec::new() };

        timing.get.start();
        self.add_get_waiters_count.fetch_add(1, Ordering::Relaxed);
        let root_node = self.store.get(root_id).await.unwrap();
        self.add_get_waiters_count.fetch_sub(1, Ordering::Relaxed);
        timing.get.stop();

        timing.node_lock.start();
        self.add_node_lock_waiters_count
            .fetch_add(1, Ordering::Relaxed);
        let root_node = root_node.write().await;
        self.add_node_lock_waiters_count
            .fetch_sub(1, Ordering::Relaxed);
        timing.node_lock.stop();

        let root = if root_node.keys.len() < self.max_keys_per_node {
            drop(root);
            None
        } else {
            Some(root)
        };

        let (result, traversed) = self
            .add_recursive(root_node, key, value, &mut timing, 1)
            .await;
        match result {
            AddResult::AddedToNodeAndSplit(middle, middle_value, new_node) => {
                let node_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
                let new_root = BTreeNode {
                    node_id,
                    keys: vec![middle],
                    values: vec![middle_value],
                    children: vec![root_id, new_node],
                };

                // let mut root = root_lock.unwrap().into_root();

                timing.insert.start();
                self.add_insert_waiters_count
                    .fetch_add(1, Ordering::Relaxed);
                self.store.insert(new_root).await;
                self.add_insert_waiters_count
                    .fetch_sub(1, Ordering::Relaxed);

                timing.insert.stop();

                let mut root: RwLockWriteGuard<'_, u32> = root.unwrap();
                *root = node_id;
                drop(root);

                self.size.fetch_add(1, Ordering::Relaxed);
                let depth = self.depth.fetch_add(1, Ordering::Relaxed);
                self.metrics.depth_gauge.set(depth as i64);
            }
            AddResult::AddedToNode | AddResult::AddedToDescendant => {
                drop(root);
                self.size.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                drop(root);
            }
        };

        // adds_histogram
        // add_lock_wait_histogram
        // add_get_wait_histogram
        // add_persist_wait_histogram
        // add_insert_wait_histogram
        // add_remaining_histogram

        let all = timing.all.elapsed().as_secs_f64();
        let root_lock_wait = timing.root_lock.elapsed().as_secs_f64();
        let node_lock_wait = timing.node_lock.elapsed().as_secs_f64();
        let get_wait = timing.get.elapsed().as_secs_f64();
        let persist_wait = timing.persist.elapsed().as_secs_f64();
        let insert_wait = timing.insert.elapsed().as_secs_f64();
        let remaining =
            all - root_lock_wait - node_lock_wait - get_wait - persist_wait - insert_wait;

        self.metrics.adds_histogram.observe(all);
        self.metrics
            .add_root_lock_wait_histogram
            .observe(root_lock_wait);
        self.metrics
            .add_node_lock_wait_histogram
            .observe(node_lock_wait);
        self.metrics.add_get_wait_histogram.observe(get_wait);
        self.metrics
            .add_persist_wait_histogram
            .observe(persist_wait);
        self.metrics.add_insert_wait_histogram.observe(insert_wait);
        self.metrics.add_remaining_histogram.observe(remaining);
        self.metrics
            .add_traversals_histogram
            .observe(traversed as f64);

        self.add_workers_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[async_recursion]
    async fn add_recursive(
        &self,
        mut node: RwLockWriteGuard<'async_recursion, BTreeNode<K, V>>,
        key: K,
        value: V,
        timing: &mut Timing,
        depth: usize,
    ) -> (AddResult<K, V>, usize) {
        // Iterate through the keys
        for (i, node_key) in node.keys.iter().enumerate() {
            if key == *node_key {
                // The exact key exists, don't add it.
                return (AddResult::NotAdded, depth);
            }
            if key < *node_key {
                // There is no key equal to the search key so it either
                // doesn't exist or exists in child i.

                // Does child i exist?
                if node.children.len() > i {
                    // Yes, continue the search at child i.
                    let child_id = node.children[i];

                    timing.get.start();
                    self.add_get_waiters_count.fetch_add(1, Ordering::Relaxed);
                    let child = self.store.get(child_id).await.unwrap();
                    self.add_get_waiters_count.fetch_sub(1, Ordering::Relaxed);
                    timing.get.stop();

                    timing.node_lock.start();
                    self.add_node_lock_waiters_count
                        .fetch_add(1, Ordering::Relaxed);
                    let child = child.write().await;
                    self.add_node_lock_waiters_count
                        .fetch_sub(1, Ordering::Relaxed);
                    timing.node_lock.stop();

                    let node = if child.keys.len() < self.max_keys_per_node {
                        drop(node);
                        None
                    } else {
                        Some(node)
                    };

                    //let node_lock = NodeLock::Node(node);
                    let (result, depth) = self
                        .add_recursive(child, key, value, timing, depth + 1)
                        .await;
                    let result = self.handle_add(result, i, timing, node).await;
                    return (result, depth);
                } else {
                    // No, so the key doesn't exist, but
                    // it should in this node at position i.
                    node.keys.insert(i, key);
                    node.values.insert(i, value);
                    let result = match self.split_if_necessary(node, timing).await {
                        Some((middle, middle_value, new_node)) => {
                            AddResult::AddedToNodeAndSplit(middle, middle_value, new_node)
                        }
                        None => AddResult::AddedToNode,
                    };
                    return (result, depth);
                }
            }
        }

        // Is there another child?
        let num_children = node.children.len();
        if num_children > 0 {
            // Yes, continue the search at child i
            let child_id = node.children[num_children - 1];
            let num_keys = node.keys.len();
            //let node_lock = NodeLock::Node(node);

            timing.get.start();
            self.add_get_waiters_count.fetch_add(1, Ordering::Relaxed);
            let child = self.store.get(child_id).await.unwrap();
            self.add_get_waiters_count.fetch_sub(1, Ordering::Relaxed);
            timing.get.stop();

            timing.node_lock.start();
            self.add_node_lock_waiters_count
                .fetch_add(1, Ordering::Relaxed);
            let child = child.write().await;
            self.add_node_lock_waiters_count
                .fetch_sub(1, Ordering::Relaxed);
            timing.node_lock.stop();

            let node = if child.keys.len() < self.max_keys_per_node {
                drop(node);
                None
            } else {
                Some(node)
            };

            let (result, depth) = self
                .add_recursive(child, key, value, timing, depth + 1)
                .await;
            let result = self.handle_add(result, num_keys, timing, node).await;
            return (result, depth);
        } else {
            // No, so the key doesn't exist, but
            // it should in this node at position i + 1
            node.keys.push(key);
            node.values.push(value);
            let result = match self.split_if_necessary(node, timing).await {
                Some((middle, middle_value, new_node)) => {
                    AddResult::AddedToNodeAndSplit(middle, middle_value, new_node)
                }
                None => AddResult::AddedToNode,
            };
            return (result, depth);
        }
    }

    async fn split_if_necessary(
        &self,
        mut node: RwLockWriteGuard<'_, BTreeNode<K, V>>,
        timing: &mut Timing,
    ) -> Option<(K, V, NodeId)> {
        //let node_id = node.node_id;
        let num_children = node.children.len();

        // Check number of keys
        if node.keys.len() > self.max_keys_per_node {
            // This node is full, split it.

            // Take the second half of the keys and pass them back so the parent can
            // add them to a new node adjacent to this one.
            let half = node.keys.len() / 2;
            let second_half_keys = node.keys.split_off(half);
            let second_half_values = node.values.split_off(half);
            let second_half_children = node.children.split_off((num_children + 1) / 2);

            let new_node_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
            let new_node = BTreeNode {
                node_id: new_node_id,
                keys: second_half_keys[1..].to_vec(),
                values: second_half_values[1..].to_vec(),
                children: second_half_children,
            };

            timing.persist.start();
            self.add_persist_waiters_count
                .fetch_add(1, Ordering::Relaxed);
            self.store.persist(node).await;
            self.add_persist_waiters_count
                .fetch_sub(1, Ordering::Relaxed);
            timing.persist.stop();

            timing.insert.start();
            self.add_insert_waiters_count
                .fetch_add(1, Ordering::Relaxed);
            self.store.insert(new_node).await;
            self.add_insert_waiters_count
                .fetch_sub(1, Ordering::Relaxed);
            timing.insert.stop();

            return Some((
                second_half_keys[0].clone(),
                second_half_values[0].clone(),
                new_node_id,
            ));
        }

        timing.persist.start();
        self.add_persist_waiters_count
            .fetch_add(1, Ordering::Relaxed);
        self.store.persist(node).await;
        self.add_persist_waiters_count
            .fetch_sub(1, Ordering::Relaxed);
        timing.persist.stop();

        None
    }
    async fn handle_add(
        &self,
        result: AddResult<K, V>,
        i: usize,
        timing: &mut Timing,
        node: Option<RwLockWriteGuard<'_, BTreeNode<K, V>>>,
    ) -> AddResult<K, V> {
        match result {
            AddResult::NotAdded => AddResult::NotAdded,
            AddResult::AddedToNode => {
                // timing.persist.start();
                // self.store.persist(node).await;
                // timing.persist.stop();
                AddResult::AddedToDescendant
            }
            AddResult::AddedToNodeAndSplit(middle, middle_value, new_node) => {
                let mut node = node.unwrap();
                node.keys.insert(i, middle);
                node.values.insert(i, middle_value);
                node.children.insert(i + 1, new_node);
                match self.split_if_necessary(node, timing).await {
                    Some((middle, middle_value, new_node)) => {
                        AddResult::AddedToNodeAndSplit(middle, middle_value, new_node)
                    }
                    None => AddResult::AddedToNode,
                }
            }
            AddResult::AddedToDescendant => AddResult::AddedToDescendant,
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use tokio::fs::remove_file;

    use crate::storage::b_tree::metrics::BTreeMetrics;

    use super::super::{
        node_store::{DeserializeNode, I32NodeSerializer, NodeStore, SerializeNode},
        tree::BTree,
    };
    use std::{
        fmt::{Debug, Display},
        path::Path,
        sync::atomic::Ordering,
    };

    async fn create_tree(test_name: &str) -> BTree<i32, i32, I32NodeSerializer> {
        let serializer = I32NodeSerializer::new();
        let path = &format!("scratch/{}.db", test_name);

        if Path::new(path).exists() {
            remove_file(path).await.unwrap();
        }
        let metrics = BTreeMetrics::register(0, &Registry::new());
        let store = NodeStore::new_disk(path, serializer, 4, 4, 1, 1, metrics.clone()).await;
        BTree::new(0, 5, store, metrics).await
    }

    #[tokio::test]
    async fn btree_add_first() {
        let mut tree = create_tree("btree_add_first").await;
        add_and_validate(&mut tree, 10, 10).await;
    }

    #[tokio::test]
    async fn btree_add_before() {
        let mut tree = create_tree("btree_add_before").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
    }

    #[tokio::test]
    async fn btree_add_between() {
        let mut tree = create_tree("btree_add_between").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
        add_and_validate(&mut tree, 8, 8).await;
    }

    #[tokio::test]
    async fn btree_add_after() {
        let mut tree = create_tree("btree_add_after").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
        add_and_validate(&mut tree, 8, 8).await;
        add_and_validate(&mut tree, 13, 13).await;
    }

    #[tokio::test]
    async fn btree_add_before_and_split() {
        let mut tree = create_tree("btree_add_before_and_split").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
        add_and_validate(&mut tree, 8, 8).await;
        add_and_validate(&mut tree, 13, 13).await;
        add_and_validate(&mut tree, 3, 3).await;
    }

    #[tokio::test]
    async fn btree_add_middle_and_split() {
        let mut tree = create_tree("btree_add_middle_and_split").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
        add_and_validate(&mut tree, 8, 8).await;
        add_and_validate(&mut tree, 13, 13).await;
        add_and_validate(&mut tree, 9, 9).await;
    }

    #[tokio::test]
    async fn btree_add_after_and_split() {
        let mut tree = create_tree("btree_add_after_and_split").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 5, 5).await;
        add_and_validate(&mut tree, 8, 8).await;
        add_and_validate(&mut tree, 13, 13).await;
        add_and_validate(&mut tree, 17, 17).await;
    }

    #[tokio::test]
    async fn btree_add_second_level_and_split() {
        let mut tree = create_tree("btree_add_second_level_and_split").await;
        add_and_validate(&mut tree, 10, 10).await;
        add_and_validate(&mut tree, 20, 20).await;
        add_and_validate(&mut tree, 30, 30).await;
        add_and_validate(&mut tree, 40, 40).await;
        add_and_validate(&mut tree, 50, 50).await;
        add_and_validate(&mut tree, 60, 60).await;
        add_and_validate(&mut tree, 70, 70).await;
        add_and_validate(&mut tree, 80, 80).await;
        add_and_validate(&mut tree, 90, 90).await;
    }

    async fn add_and_validate<K, V, S>(tree: &mut BTree<K, V, S>, key_to_add: K, value_to_add: V)
    where
        K: PartialOrd + Clone + Debug + Display + Send + Sync,
        V: PartialEq + Clone + Debug + Send + Sync,
        S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
    {
        let keys_before = tree.list_keys().await;
        let size_before = tree.size.load(Ordering::SeqCst);
        assert_eq!(keys_before.len(), size_before);
        tree.add(key_to_add.clone(), value_to_add).await;

        // Validate with cache intact
        tree.validate().await;
        let keys_after = tree.list_keys().await;
        assert_eq!(tree.size.load(Ordering::SeqCst), size_before + 1);
        assert_eq!(
            keys_after
                .into_iter()
                .filter(|key| *key != key_to_add)
                .collect::<Vec<_>>(),
            keys_before
        );

        // Clear cache
        tree.store.flush().await;
        tree.store.clear_cache();

        // Validate again with clear cache
        tree.validate().await;
        let keys_after = tree.list_keys().await;
        assert_eq!(tree.size.load(Ordering::SeqCst), size_before + 1);
        assert_eq!(
            keys_after
                .into_iter()
                .filter(|key| *key != key_to_add)
                .collect::<Vec<_>>(),
            keys_before
        );
    }
}
