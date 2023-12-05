use super::{
    node::NodeId,
    node_store::{DeserializeNode, SerializeNode},
    tree::BTree,
};
use async_recursion::async_recursion;
use std::fmt::{Debug, Display};

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn find_from_root(&self, key: K) -> bool {
        // Starting at the root
        let root = self.root.read().await;
        self.find_recursive(*root, key).await
    }

    #[async_recursion]
    pub async fn find_recursive(&self, node_id: NodeId, key: K) -> bool {
        let node = self.store.get(node_id).await.unwrap();
        let lock = node.read().await;

        if lock.keys.is_empty() {
            return false;
        }

        // Iterate through the keys
        for (i, node_key) in lock.keys.iter().enumerate() {
            if key == *node_key {
                // The exact key exists, return the node.
                return true;
            }
            if key < *node_key {
                // There is no key equal to the search key so it either
                // doesn't exist or exists in child i.

                // Does child i exist?
                if lock.children.len() > i {
                    // Yes, continue the search at child i.
                    return self.find_recursive(lock.children[i], key).await;
                } else {
                    // No, so the key doesn't exist, but
                    // it should in this node at position i.
                    return false;
                }
            }
        }

        // Is there a final child?
        let num_children = lock.children.len();
        if num_children > 0 {
            // Yes, continue the search at the last child
            return self
                .find_recursive(lock.children[num_children - 1], key)
                .await;
        } else {
            // No, so the key doesn't exist, but
            // it should in this node at position i + 1
            return false;
        }
    }
}

#[cfg(test)]
mod tests {

    use prometheus::Registry;

    use super::super::{
        node_store::{I32NodeSerializer, NodeStore},
        tree::BTree,
    };

    #[tokio::test]
    async fn btree_find_first_in_root() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(10).await);
    }

    #[tokio::test]
    async fn btree_find_last_in_root() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(20).await);
    }

    #[tokio::test]
    async fn btree_find_first_in_first() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(4).await);
    }

    #[tokio::test]
    async fn btree_find_middle_in_first() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(5).await);
    }

    #[tokio::test]
    async fn btree_find_last_in_first() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(6).await);
    }

    #[tokio::test]
    async fn btree_find_first_in_middle() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(14).await);
    }

    #[tokio::test]
    async fn btree_find_last_in_middle() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(16).await);
    }

    #[tokio::test]
    async fn btree_find_in_last() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(tree.exists(25).await);
    }

    #[tokio::test]
    async fn btree_find_missing_before_first() {
        let mut tree = BTree::new(
            5,
            NodeStore::<i32, i32, I32NodeSerializer>::new_memory(),
            &Registry::new(),
        )
        .await;
        tree.add(10, 10).await;
        tree.add(20, 20).await;
        tree.add(4, 4).await;
        tree.add(5, 5).await;
        tree.add(6, 6).await;
        tree.add(14, 14).await;
        tree.add(16, 16).await;
        tree.add(25, 25).await;
        assert!(!tree.exists(3).await);
    }
}
