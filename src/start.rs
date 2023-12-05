use crate::{
    bynamo_node::NodeId,
    consensus::role_consensus::RoleConsensus,
    messaging::{handler::MessageHandler, sender::MessageSender, server::NodeService},
    role_service::RoleService,
    storage::{
        b_tree::{
            node_store::{calculate_max_keys_per_node, NodeStore, StringNodeSerializer},
            tree::BTree,
        },
        handle_message::StorageMessageHandler,
        replicator::StorageReplicator,
        write_ahead_log::{self, WriteAheadLog},
    },
    util::{delete_file_if_exists::delete_file_if_exists, metrics::init_metrics},
};
use prometheus::Registry;
use std::time::Duration;
use tokio::task::yield_now;

pub async fn start_in_memory_node(
    node_id: NodeId,
    members: Vec<NodeId>,
    role_service: RoleService,
    message_sender: MessageSender,
) -> Result<MessageHandler, NodeRuntimeError> {
    let role_consensus = RoleConsensus::new(
        node_id,
        members.clone(),
        role_service.clone(),
        message_sender.clone(),
    );

    let registry = init_metrics(node_id);

    //let root = "/media/chris/A420D64F20D62856";
    //let root = "/media/chris/B026CFCD26CF92B0";
    let root = ".";
    let log_path = format!("{}/scratch/write_ahead_log_{}.bin", root, node_id);
    let index_path = format!("{}/scratch/write_ahead_index_{}.bin", root, node_id);
    let btree_index_path = format!("{}/scratch/btree_index_{}.bin", root, node_id);
    delete_file_if_exists(&log_path);
    delete_file_if_exists(&index_path);
    delete_file_if_exists(&btree_index_path);
    let write_ahead_log = WriteAheadLog::create(&log_path, &index_path, &registry).await?;
    let index = create_btree_index(&btree_index_path, &registry).await;

    let storage_message_handler = StorageMessageHandler::new(
        node_id,
        role_service,
        write_ahead_log,
        index,
        StorageReplicator::new(members, message_sender),
        &registry,
    );
    let message_handler = MessageHandler::new(storage_message_handler, role_consensus.clone());

    let addr = format!("127.0.0.1:{}", 50000 + node_id);
    let message_server = NodeService::start(&addr, message_handler.clone()).await;

    // tokio::spawn(async move {
    //     Server::builder()
    //         .add_service(NodeService::new(message_server))
    //         .serve(addr.parse().unwrap())
    //         .await
    //         .unwrap();
    // });

    tokio::spawn(async move {
        loop {
            role_consensus.tick().await;
            yield_now().await;
        }
    });

    Ok(message_handler)
}

async fn create_btree_index(
    path: &str,
    registry: &Registry,
) -> BTree<String, String, StringNodeSerializer> {
    let key_size: usize = 64;
    let value_size: usize = 64;
    let max_keys_per_node = calculate_max_keys_per_node(key_size, value_size);

    let serializer = StringNodeSerializer::new();
    let flush_every = 1000;
    let flush_buffer_max_size = 10_000;
    let store: NodeStore<String, String, StringNodeSerializer> = NodeStore::new_disk(
        path,
        serializer,
        key_size,
        value_size,
        flush_every,
        flush_buffer_max_size,
        registry,
    )
    .await;
    let persist_store = store.clone();
    let btree = BTree::new(max_keys_per_node, store, registry).await;
    tokio::spawn(async move {
        loop {
            if persist_store.flush_required().await {
                //println!("Flushing");
                persist_store.flush().await;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });
    btree
}

#[derive(thiserror::Error, Debug)]
pub enum NodeRuntimeError {
    #[error("Failed to create write ahead log")]
    WriteAheadLogError(#[from] write_ahead_log::CreateError),
}
