use async_recursion::async_recursion;

use super::{
    node::NodeId,
    node_store::{DeserializeNode, SerializeNode},
    tree::BTree,
};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn print(&self) {
        println!("{}", self.format().await);
    }

    pub async fn format(&self) -> String {
        // Debug print the entire tree in a multiline tree diagram

        let mut out = String::new();

        let mut nodes = HashMap::new();
        let mut position = 0;
        let root = self.root.read().await;
        self.get_leaf_nodes_recursive(*root, &mut position, 0, &mut nodes)
            .await;

        let mut level = 0;
        out += "\n";
        loop {
            let mut position = 0;
            if let Some(nodes) = nodes.remove(&level) {
                for (str, start, end) in nodes {
                    let left_pad = ((end - start) - str.len()) / 2;
                    let padding = " ".repeat(left_pad);
                    out += &padding;
                    position += padding.len();
                    out += &str;
                    position += str.len();
                    if position < end {
                        out += &" ".repeat(end - position);
                    }
                    position = end;
                }
                out += "\n\n";
            } else {
                break;
            }
            level += 1;
        }

        out
    }
}

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    #[async_recursion]
    async fn get_leaf_nodes_recursive(
        &self,
        node_id: NodeId,
        position: &mut usize,
        level: usize,
        nodes: &mut HashMap<usize, Vec<(String, Start, End)>>,
    ) -> (String, Start, End) {
        let node = self.store.get(node_id).await.unwrap();
        let node = node.read().await;

        let str = format!(
            "[{}]",
            node.keys
                .iter()
                .zip(node.values.iter())
                .map(|(key, value)| format!("{:?}:{:?}", key, value))
                .collect::<Vec<_>>()
                .join(", ")
        );

        if node.children.is_empty() {
            let len = str.len();
            let start = *position;
            let end = *position + len + 2;
            let level_nodes = nodes.entry(level).or_default();
            level_nodes.push((str.clone(), start, end));
            *position = end;
            return (str, start, end);
        } else {
            let mut start = None;
            let mut end = None;
            for child in &node.children {
                let child_node = self
                    .get_leaf_nodes_recursive(*child, position, level + 1, nodes)
                    .await;
                start = Some(start.unwrap_or(child_node.1));
                end = Some(child_node.2);
            }
            let level_nodes = nodes.entry(level).or_default();
            level_nodes.push((str.clone(), start.unwrap(), end.unwrap()));
            return (str, start.unwrap(), end.unwrap());
        }
    }
}

type Start = usize;
type End = usize;
