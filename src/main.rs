mod bynamo_node;
mod client;
mod consensus;
mod membership_service;
mod messaging;
mod role_service;
mod start;
mod storage;
mod util;
use anyhow::Result;
use bynamo_node::NodeId;
use lazy_static::lazy_static;
use messaging::{handler::MessageHandler, sender::MessageSender};
use prometheus::{
    core::Atomic, exponential_buckets, labels, register_histogram, Histogram, HistogramOpts,
    Registry,
};
use role_service::RoleService;
use start::start_in_memory_node;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::{self, Duration, Instant},
};
use storage::commands::commands::WriteCommand;
use tokio::task::{JoinSet, LocalSet};

use crate::messaging::client::MessageClient;

lazy_static! {
    static ref PUTS_RTT_HISTOGRAM: Histogram = register_histogram!(
        "putitem_round_trip_histogram",
        "Item put round trip durations in microseconds",
        exponential_buckets(20.0, 3.0, 15).unwrap()
    )
    .unwrap();
}

#[tokio::main(worker_threads = 8)]
pub async fn main() -> Result<()> {
    tokio::task::spawn_blocking(move || loop {
        let metric_families = prometheus::gather();
        prometheus::push_metrics(
            "bynamo_client",
            labels! {},
            "127.0.0.1:9091",
            metric_families,
            None,
        )
        .unwrap();
        thread::sleep(time::Duration::from_secs(1));
    });

    let members: Vec<_> = (1..=3).collect();

    let mut message_sender = MessageSender::new();
    let role_service = RoleService::new_memory();
    let mut message_handlers = HashMap::new();

    for node_id in members.clone() {
        let members = members.clone();
        let message_sender = message_sender.clone();
        let role_service = role_service.clone();

        let message_handler = start_in_memory_node(node_id, members, role_service, message_sender)
            .await
            .unwrap();
        message_handlers.insert(node_id, message_handler);
    }

    //tokio::time::sleep(Duration::from_secs(1)).await;

    let stream_open_histogram = Histogram::with_opts(
        HistogramOpts::new(
            "stream_open_histogram",
            "QUIC stream open durations in microseconds",
        )
        .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
    )
    .unwrap();
    prometheus::default_registry()
        .register(Box::new(stream_open_histogram.clone()))
        .unwrap();

    let stream_ttfb_histogram = Histogram::with_opts(
        HistogramOpts::new(
            "stream_ttfb_histogram",
            "QUIC stream time to first byte durations in microseconds",
        )
        .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
    )
    .unwrap();
    prometheus::default_registry()
        .register(Box::new(stream_ttfb_histogram.clone()))
        .unwrap();

    for node_id in members.clone() {
        let addr = format!("127.0.0.1:{}", 50000 + node_id);

        let client = MessageClient::connect(
            &addr,
            stream_open_histogram.clone(),
            stream_ttfb_histogram.clone(),
        )
        .await;
        message_sender.add_client(node_id, client).await;
    }

    let key = Arc::new(AtomicU64::new(0));

    let mut set = JoinSet::new();
    for _ in 0..7 {
        set.spawn(start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ));
    }

    while let Some(res) = set.join_next().await {
        //let out = res.unwrap();
    }

    Ok(())
}

async fn start_load_worker(
    role_service: RoleService,
    mut message_sender: MessageSender,
    mut message_handlers: HashMap<NodeId, MessageHandler>,
    key: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        loop {
            let leader = role_service.leader().await;
            if let Some(leader) = leader {
                let i = key.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // let handler = message_handlers.get_mut(&leader).unwrap();

                // let message = StorageMessage {
                //     message: Some(Message::Command(StorageCommand {
                //         command: Some(storage_command::Command::Write(WriteCommand { key, value })),
                //     })),
                // };

                let mut backoff_millis = 1000;
                loop {
                    let key = i.to_string(); // iformat!("key-{}", rand::random::<u32>());
                    let value = key.clone(); // format!("value-{}", rand::random::<u32>());

                    let start = Instant::now();
                    // Wait for either a response or a timeout
                    let message_future =
                        message_sender.send_and_wait(leader, WriteCommand { key, value }.into());
                    match tokio::time::timeout(Duration::from_secs(1), message_future).await {
                        Ok(result) => {
                            if let Err(error) = result {
                                println!("Received error from server: {:?}", error);
                            } else {
                                break result.unwrap();
                            }
                        }
                        Err(_) => {
                            println!("Timeout, backoff {}", backoff_millis);
                        }
                    }

                    let duration = start.elapsed();
                    let duration_micros = duration.as_micros();
                    PUTS_RTT_HISTOGRAM.observe(duration_micros as f64);
                    tokio::time::sleep(Duration::from_millis(backoff_millis)).await;
                    backoff_millis *= 2;
                }

                // let result = handler
                //     .handle(node_message::Message::Storage(message))
                //     .await;
            } else {
                println!("No leader");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    })
    .await
    .unwrap()
}
