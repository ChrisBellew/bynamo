use super::{
    node::{BTreeNode, NodeId},
    node_store::{DeserializeNode, SerializeNode},
    tree::BTree,
};
use async_recursion::async_recursion;
use std::{
    fmt::{Debug, Display},
    sync::atomic::Ordering,
    time::Instant,
};
use tokio::sync::RwLockWriteGuard;

// lazy_static! {
//     static ref ADDS_HISTOGRAM: Histogram = register_histogram!(
//         "btree_adds_histogram",
//         "Btree add durations in microseconds",
//         exponential_buckets(20.0, 3.0, 15).unwrap()
//     )
//     .unwrap();
// }

enum AddResult<K, V>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
{
    Added,
    AddedAndSplit(K, V, NodeId),
    NotAdded,
}

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn add_from_root(&self, key: K, value: V) {
        let start = Instant::now();
        let mut root = self.root.write().await;

        match self.add_recursive(*root, key, value).await {
            AddResult::AddedAndSplit(middle, middle_value, new_node) => {
                let node_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);
                let new_root = BTreeNode {
                    node_id,
                    keys: vec![middle],
                    values: vec![middle_value],
                    children: vec![*root, new_node],
                };

                *root = node_id;
                self.store.insert(new_root).await;
                self.size.fetch_add(1, Ordering::SeqCst);
            }
            AddResult::Added => {
                self.size.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        };
        self.adds_histogram
            .observe(start.elapsed().as_micros() as f64);
    }

    #[async_recursion]
    async fn add_recursive(&self, node_id: NodeId, key: K, value: V) -> AddResult<K, V> {
        let node = self.store.get(node_id).await.unwrap();
        let mut node = node.write().await;

        // Iterate through the keys
        for (i, node_key) in node.keys.iter().enumerate() {
            if key == *node_key {
                // The exact key exists, don't add it.
                return AddResult::NotAdded;
            }
            if key < *node_key {
                // There is no key equal to the search key so it either
                // doesn't exist or exists in child i.

                // Does child i exist?
                if node.children.len() > i {
                    // Yes, continue the search at child i.
                    let result = self.add_recursive(node.children[i], key, value).await;
                    let result = self.handle_add(result, node, i).await;
                    return result;
                } else {
                    // No, so the key doesn't exist, but
                    // it should in this node at position i.
                    node.keys.insert(i, key);
                    node.values.insert(i, value);
                    let result = self.split_if_necessary(node).await;
                    return result;
                }
            }
        }

        // Is there another child?
        let num_children = node.children.len();
        if num_children > 0 {
            // Yes, continue the search at child i
            let result = self
                .add_recursive(node.children[num_children - 1], key, value)
                .await;
            let num_keys = node.keys.len();
            let result = self.handle_add(result, node, num_keys).await;
            return result;
        } else {
            // No, so the key doesn't exist, but
            // it should in this node at position i + 1
            node.keys.push(key);
            node.values.push(value);
            let result = self.split_if_necessary(node).await;
            return result;
        }
    }
    async fn split_if_necessary<'a>(
        &self,
        mut node: RwLockWriteGuard<'a, BTreeNode<K, V>>,
    ) -> AddResult<K, V> {
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

            self.store.persist(node).await;
            self.store.insert(new_node).await;

            return AddResult::AddedAndSplit(
                second_half_keys[0].clone(),
                second_half_values[0].clone(),
                new_node_id,
            );
        }
        self.store.persist(node).await;
        AddResult::Added
    }
    async fn handle_add<'a>(
        &self,
        result: AddResult<K, V>,
        mut node: RwLockWriteGuard<'a, BTreeNode<K, V>>,
        i: usize,
    ) -> AddResult<K, V> {
        match result {
            AddResult::Added => {
                self.store.persist(node).await;
                AddResult::Added
            }
            AddResult::NotAdded => {
                self.store.persist(node).await;
                AddResult::NotAdded
            }
            AddResult::AddedAndSplit(middle, middle_value, new_node) => {
                node.keys.insert(i, middle);
                node.values.insert(i, middle_value);
                node.children.insert(i + 1, new_node);
                self.split_if_necessary(node).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use tokio::fs::remove_file;

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
        let store = NodeStore::new_disk(path, serializer, 4, 4, 1, 1, &Registry::new()).await;

        BTree::new(5, store, &Registry::new()).await
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
