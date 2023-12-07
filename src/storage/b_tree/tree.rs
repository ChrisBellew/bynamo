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
    pub add_root_lock_wait_histogram: Histogram,
    pub add_node_lock_wait_histogram: Histogram,
    pub add_get_wait_histogram: Histogram,
    pub add_persist_wait_histogram: Histogram,
    pub add_insert_wait_histogram: Histogram,
    pub add_remaining_histogram: Histogram,
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
            HistogramOpts::new("btree_adds_histogram", "Btree add durations in seconds")
                .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry.register(Box::new(adds_histogram.clone())).unwrap();

        let add_root_lock_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_root_lock_wait_histogram",
                "Btree add root lock wait durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_root_lock_wait_histogram.clone()))
            .unwrap();

        let add_node_lock_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_node_lock_wait_histogram",
                "Btree add node lock wait durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_node_lock_wait_histogram.clone()))
            .unwrap();

        let add_get_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_get_wait_histogram",
                "Btree add get wait durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_get_wait_histogram.clone()))
            .unwrap();

        let add_persist_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_persist_wait_histogram",
                "Btree add persist wait durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_persist_wait_histogram.clone()))
            .unwrap();

        let add_insert_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_insert_wait_histogram",
                "Btree add insert wait durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_insert_wait_histogram.clone()))
            .unwrap();

        let add_remaining_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_remaining_histogram",
                "Btree add remaining durations in seconds",
            )
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 20).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_remaining_histogram.clone()))
            .unwrap();

        BTree {
            root: Arc::new(RwLock::new(0)),
            size: Arc::new(0.into()),
            max_keys_per_node,
            next_node_id: Arc::new(1.into()),
            store,
            adds_histogram,
            add_root_lock_wait_histogram,
            add_node_lock_wait_histogram,
            add_get_wait_histogram,
            add_persist_wait_histogram,
            add_insert_wait_histogram,
            add_remaining_histogram,
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
