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
use prometheus::{core::Atomic, exponential_buckets, labels, register_histogram, Histogram};
use role_service::RoleService;
use start::start_in_memory_node;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::{self, Duration, Instant},
};
use storage::commands::commands::WriteCommand;

use crate::messaging::client::MessageClient;

lazy_static! {
    static ref PUTS_RTT_HISTOGRAM: Histogram = register_histogram!(
        "putitem_round_trip_histogram",
        "Item put round trip durations in microseconds",
        exponential_buckets(20.0, 3.0, 15).unwrap()
    )
    .unwrap();
}

#[tokio::main]
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

    for node_id in members.clone() {
        let addr = format!("127.0.0.1:{}", 50000 + node_id);
        let client = MessageClient::connect(&addr).await;
        message_sender.add_client(node_id, client).await;
    }

    let key = Arc::new(AtomicU64::new(0));

    tokio::join!(
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
        start_load_worker(
            role_service.clone(),
            message_sender.clone(),
            message_handlers.clone(),
            key.clone(),
        ),
    );

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
                let key = i.to_string(); // iformat!("key-{}", rand::random::<u32>());
                let value = key.clone(); // format!("value-{}", rand::random::<u32>());
                                         // let handler = message_handlers.get_mut(&leader).unwrap();

                // let message = StorageMessage {
                //     message: Some(Message::Command(StorageCommand {
                //         command: Some(storage_command::Command::Write(WriteCommand { key, value })),
                //     })),
                // };

                let start = Instant::now();
                let result = message_sender
                    .send_and_wait(leader, WriteCommand { key, value }.into())
                    .await;
                let duration = start.elapsed();
                let duration_micros = duration.as_micros();
                PUTS_RTT_HISTOGRAM.observe(duration_micros as f64);

                // let result = handler
                //     .handle(node_message::Message::Storage(message))
                //     .await;

                if let Err(error) = result {
                    println!("Received error from server: {:?}", error);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            } else {
                println!("No leader");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    })
    .await
    .unwrap()
}
