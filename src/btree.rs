use maplit::hashmap;
use prometheus::{exponential_buckets, labels, Histogram, HistogramOpts, Registry};
use std::{
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::{self, Duration, Instant},
};
use storage::b_tree::{
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

#[tokio::main]
pub async fn main() {
    let node_id = 0;
    let registry = Registry::new_custom(
        None,
        Some(hashmap! {
            "node".to_string() => node_id.to_string(),
        }),
    )
    .unwrap();
    let registry_push = registry.clone();

    tokio::task::spawn_blocking(move || loop {
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
    delete_file_if_exists("scratch/btree");
    let key_size = 64;
    let value_size = 64;
    let flush_every = 2000;
    let flush_buffer_max_size = 6000;
    let keys = calculate_max_keys_per_node(key_size, value_size);
    let serializer = StringNodeSerializer::new();
    let store = NodeStore::new_disk(
        "scratch/btree",
        serializer,
        key_size,
        value_size,
        flush_every,
        flush_buffer_max_size,
        &registry,
    )
    .await;

    {
        let store = store.clone();
        tokio::spawn(async move {
            loop {
                if store.flush_required().await {
                    //println!("Flushing");
                    store.flush().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
    }

    let btree = BTree::new(keys, store.clone(), &registry).await;
    // let wal = WriteAheadLog::create("scratch/wal", "wal_index", &registry)
    //     .await
    //     .unwrap();
    let mut handles = Vec::new();
    let count = Arc::new(AtomicU64::new(0));
    let total = 10_000_000;
    let workers = 50;
    for i in 0..workers {
        let store = store.clone();
        let mut btree = btree.clone();
        let count = count.clone();
        let buffer_backoffs_histogram = buffer_backoffs_histogram.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..total / workers {
                while store.flush_buffer_full().await {
                    buffer_backoffs_histogram.observe(store.flush_buffer_size().await as f64);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                btree
                    .add(format!("key_{}_{}", i, j), format!("value_{}_{}", i, j))
                    .await;
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
