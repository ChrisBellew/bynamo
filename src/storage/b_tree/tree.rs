use super::{
    node::{BTreeNode, NodeId},
    node_store::{DeserializeNode, NodeStore, SerializeNode},
};
use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};
use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU32, AtomicUsize},
        Arc,
    },
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub root: Arc<RwLock<NodeId>>,
    pub size: Arc<AtomicUsize>,
    pub max_keys_per_node: usize,
    pub next_node_id: Arc<AtomicU32>,
    pub store: NodeStore<K, V, S>,
    pub adds_histogram: Histogram,
}

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn new(
        max_keys_per_node: usize,
        store: NodeStore<K, V, S>,
        registry: &Registry,
    ) -> BTree<K, V, S> {
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: Vec::new(),
                values: Vec::new(),
                children: Vec::new(),
            })
            .await;

        let adds_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_adds_histogram",
                "Btree add durations in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry.register(Box::new(adds_histogram.clone())).unwrap();

        BTree {
            root: Arc::new(RwLock::new(0)),
            size: Arc::new(0.into()),
            max_keys_per_node,
            next_node_id: Arc::new(1.into()),
            store,
            adds_histogram,
        }
    }
    pub async fn add(&mut self, key: K, value: V) {
        self.add_from_root(key, value).await;
    }
    pub async fn exists(&mut self, key: K) -> bool {
        self.find_from_root(key).await
    }
    pub async fn remove(&mut self, key: &K) {
        self.remove_from_root(key).await;
    }
    pub fn min_keys_per_node(&self) -> usize {
        (self.max_keys_per_node + 1) / 2 - 1
    }
}
