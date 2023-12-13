use maplit::hashmap;
use prometheus::{
    exponential_buckets, labels, linear_buckets, Histogram, HistogramOpts, IntGauge, Registry,
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::{self, Duration, Instant},
};
use storage::b_tree::{
    metrics::BTreeMetrics,
    node_store::{calculate_max_keys_per_node, NodeStore, StringNodeSerializer},
    tree::BTree,
};
use util::delete_file_if_exists::delete_file_if_exists;
mod bynamo_node;
mod consensus;
mod messaging;
mod role_service;
mod start;
mod storage;
mod util;

#[tokio::main(worker_threads = 10)]
pub async fn main() {
    console_subscriber::init();

    let node_id = 0;
    let registry = Registry::new_custom(
        None,
        Some(hashmap! {
            "node".to_string() => node_id.to_string(),
        }),
    )
    .unwrap();
    let registry_push = registry.clone();

    thread::spawn(move || loop {
        let metric_families = registry_push.gather();
        //println!("Pushing metrics {:?}", metric_families);
        match prometheus::push_metrics(
            &format!("node_metrics_{}", node_id),
            labels! {},
            "127.0.0.1:9091",
            metric_families,
            None,
        ) {
            Ok(_) => {}
            Err(e) => println!("Failed to push metrics: {:?}", e),
        }
        thread::sleep(time::Duration::from_secs(1));
    });

    let buffer_backoffs_histogram = Histogram::with_opts(
        HistogramOpts::new(
            "node_store_buffer_backoffs_histogram",
            "Backoff buffer sizes in bytes",
        )
        .buckets(exponential_buckets(8.0, 3.0, 15).unwrap()),
    )
    .unwrap();
    registry
        .register(Box::new(buffer_backoffs_histogram.clone()))
        .unwrap();

    // Stress test Btree
    let key_size = 64;
    let value_size = 64;
    let flush_every = 0; //2000;
    let flush_buffer_max_size = 1000;
    let keys = calculate_max_keys_per_node(key_size, value_size);

    let partitions: usize = 1;
    let mut trees = Vec::new();

    for tree_id in 0..partitions {
        let path = format!("scratch/btree_{}", tree_id);
        delete_file_if_exists(&path);

        let serializer = StringNodeSerializer::new();
        let metrics = BTreeMetrics::register(tree_id, &registry);
        let store = NodeStore::new_disk(
            &path,
            serializer,
            key_size,
            value_size,
            flush_every,
            flush_buffer_max_size,
            metrics.clone(),
        )
        .await;

        {
            let store = store.clone();
            tokio::spawn(async move {
                loop {
                    store.flush().await;
                }
            });
        }

        let btree = BTree::new(tree_id, keys, store.clone(), metrics).await;
        trees.push(btree);
    }

    let partitioner = Partitioner::new(partitions);

    // let wal = WriteAheadLog::create("scratch/wal", "wal_index", &registry)
    //     .await
    //     .unwrap();
    let mut handles = Vec::new();
    let count = Arc::new(AtomicU64::new(0));
    let total = 10_000_000;
    let workers = 2;
    for i in 0..workers {
        //let store = store.clone();
        let partitioner = partitioner.clone();
        let mut trees = trees.clone();
        let count = count.clone();
        let buffer_backoffs_histogram = buffer_backoffs_histogram.clone();
        let key = "1234567".repeat(5).to_string();
        let value = "1234567890".repeat(6).to_string();
        handles.push(tokio::spawn(async move {
            for j in 0..total / workers {
                let random = rand::random::<u64>();
                let key = format!("key_{}_{}", key, random);
                //let value = "1234567890".to_string();
                let partition = partitioner.partition(&key);
                let tree = trees.get_mut(partition).unwrap();

                while tree.store.flush_buffer_full().await {
                    buffer_backoffs_histogram.observe(tree.store.flush_buffer_size().await as f64);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                tree.add(key, value.clone()).await;
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }
    tokio::spawn(async move {
        loop {
            let last = Instant::now();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            println!(
                "{}/sec",
                count.load(std::sync::atomic::Ordering::Relaxed) as f64
                    / last.elapsed().as_secs_f64()
            );
            count.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    });
    for handle in handles {
        handle.await.unwrap();
    }
}

#[derive(Clone)]
struct Partitioner {
    partitions: usize,
}

impl Partitioner {
    fn new(partitions: usize) -> Partitioner {
        Partitioner { partitions }
    }

    fn partition(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % self.partitions as u64) as usize
    }
}
