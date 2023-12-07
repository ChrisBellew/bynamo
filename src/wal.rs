use prometheus::Registry;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};
use storage::write_ahead_log::WriteAheadLog;
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
    // Stress test WAL
    let registry = Registry::new();
    delete_file_if_exists("scratch/wal");
    let wal = WriteAheadLog::create("scratch/wal", "wal_index", &registry)
        .await
        .unwrap();
    let mut handles = Vec::new();
    let count = Arc::new(AtomicU64::new(0));
    let total = 10_000_000;
    let workers = 20;

    for i in 0..workers {
        let mut wal = wal.clone();
        let count = count.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..total / workers {
                wal.write_new(&format!("key_{}_{}", i, j), &format!("value_{}_{}", i, j))
                    .await
                    .unwrap();
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }));
    }
    tokio::spawn(async move {
        loop {
            let last = Instant::now();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
