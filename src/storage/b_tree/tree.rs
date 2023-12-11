use super::{
    metrics::BTreeMetrics,
    node::{BTreeNode, NodeId},
    node_store::{DeserializeNode, NodeStore, SerializeNode},
};
use prometheus::Registry;
use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
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
    pub depth: Arc<AtomicUsize>,
    pub max_keys_per_node: usize,
    pub next_node_id: Arc<AtomicU32>,
    pub store: NodeStore<K, V, S>,
    pub metrics: BTreeMetrics,
    // pub add_root_lock_wait_histogram: Histogram,
    // pub add_node_lock_wait_histogram: Histogram,
    // pub add_get_wait_histogram: Histogram,
    // pub add_persist_wait_histogram: Histogram,
    // pub add_insert_wait_histogram: Histogram,
    // pub add_remaining_histogram: Histogram,
    // pub add_traversals_histogram: Histogram,
    pub add_root_lock_waiters_count: Arc<AtomicU32>,
    pub add_node_lock_waiters_count: Arc<AtomicU32>,
    pub add_get_waiters_count: Arc<AtomicU32>,
    pub add_persist_waiters_count: Arc<AtomicU32>,
    pub add_insert_waiters_count: Arc<AtomicU32>,
    pub add_workers_count: Arc<AtomicU32>,
    //pub depth_gauge: IntGauge,
}

impl<K, V, S> BTree<K, V, S>
where
    K: PartialOrd + Clone + Debug + Display + Send + Sync,
    V: PartialEq + Clone + Debug + Send + Sync,
    S: SerializeNode<K, V> + DeserializeNode<K, V> + Send + Sync + Clone,
{
    pub async fn new(
        tree_id: usize,
        max_keys_per_node: usize,
        store: NodeStore<K, V, S>,
        metrics: BTreeMetrics,
    ) -> BTree<K, V, S> {
        store
            .insert(BTreeNode {
                node_id: 0,
                keys: Vec::new(),
                values: Vec::new(),
                children: Vec::new(),
            })
            .await;

        let add_root_lock_waiters_count = Arc::new(AtomicU32::new(0));
        let add_node_lock_waiters_count = Arc::new(AtomicU32::new(0));
        let add_get_waiters_count = Arc::new(AtomicU32::new(0));
        let add_persist_waiters_count = Arc::new(AtomicU32::new(0));
        let add_insert_waiters_count = Arc::new(AtomicU32::new(0));
        let add_workers_count = Arc::new(AtomicU32::new(0));
        let next_node_id = Arc::new(AtomicU32::new(1));

        {
            let add_root_lock_waiters_count = add_root_lock_waiters_count.clone();
            let add_node_lock_waiters_count = add_node_lock_waiters_count.clone();
            let add_get_waiters_count = add_get_waiters_count.clone();
            let add_persist_waiters_count = add_persist_waiters_count.clone();
            let add_insert_waiters_count = add_insert_waiters_count.clone();
            let add_workers_count = add_workers_count.clone();
            let next_node_id = next_node_id.clone();
            let metrics = metrics.clone();

            tokio::spawn(async move {
                let mut last_set = Instant::now();
                let mut max_root_lock_waiters = 0;
                let mut max_node_lock_waiters = 0;
                let mut max_get_waiters = 0;
                let mut max_persist_waiters = 0;
                let mut max_insert_waiters = 0;
                let mut max_workers = 0;
                loop {
                    max_root_lock_waiters = std::cmp::max(
                        max_root_lock_waiters,
                        add_root_lock_waiters_count.load(Ordering::Relaxed),
                    );
                    max_node_lock_waiters = std::cmp::max(
                        max_node_lock_waiters,
                        add_node_lock_waiters_count.load(Ordering::Relaxed),
                    );
                    max_get_waiters = std::cmp::max(
                        max_get_waiters,
                        add_get_waiters_count.load(Ordering::Relaxed),
                    );
                    max_persist_waiters = std::cmp::max(
                        max_persist_waiters,
                        add_persist_waiters_count.load(Ordering::Relaxed),
                    );
                    max_insert_waiters = std::cmp::max(
                        max_insert_waiters,
                        add_insert_waiters_count.load(Ordering::Relaxed),
                    );
                    max_workers =
                        std::cmp::max(max_workers, add_workers_count.load(Ordering::Relaxed));
                    metrics
                        .node_count_gauge
                        .set(next_node_id.load(Ordering::Relaxed) as i64);

                    if last_set.elapsed().as_secs() > 1 {
                        metrics
                            .add_root_lock_waiters_gauge
                            .set(max_root_lock_waiters as i64);
                        metrics
                            .add_node_lock_waiters_gauge
                            .set(max_node_lock_waiters as i64);
                        metrics.add_get_waiters_gauge.set(max_get_waiters as i64);
                        metrics
                            .add_persist_waiters_gauge
                            .set(max_persist_waiters as i64);
                        metrics
                            .add_insert_waiters_gauge
                            .set(max_insert_waiters as i64);
                        metrics.add_workers_gauge.set(max_workers as i64);
                        max_root_lock_waiters = 0;
                        max_node_lock_waiters = 0;
                        max_get_waiters = 0;
                        max_persist_waiters = 0;
                        max_insert_waiters = 0;
                        max_workers = 0;
                        last_set = Instant::now();
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        BTree {
            root: Arc::new(RwLock::new(0)),
            size: Arc::new(0.into()),
            depth: Arc::new(1.into()),
            max_keys_per_node,
            next_node_id,
            store,
            metrics,
            add_root_lock_waiters_count,
            add_node_lock_waiters_count,
            add_get_waiters_count,
            add_persist_waiters_count,
            add_insert_waiters_count,
            add_workers_count,
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
