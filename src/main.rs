mod bynamo_node;
mod client;
mod consensus;
mod membership;
mod metadata;
mod replication;
mod storage;
mod util;
use anyhow::Result;
use consensus::{network::DirectHubConsensusNetwork, role_consensus::RoleConsensus};
use futures::executor::block_on;
use membership::MembershipService;
use metadata::{MemoryMetadataService, MetadataService};
use replication::replicator::NetworkReplicator;
use std::time::Instant;
use storage::{
    coordinator::StorageCoordinator,
    message::{RequestWriteMessage, StorageMessage},
    network::DirectHubStorageNetwork,
};
use tokio::task::yield_now;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    let mut consensus_network = DirectHubConsensusNetwork::new();
    let mut storage_network = DirectHubStorageNetwork::new();
    let membership = MembershipService::new();
    let metadata = MemoryMetadataService::new();

    let nodes = (1..=3).map(|node_id| {
        let metadata = metadata.clone();
        let mut membership = membership.clone();
        membership.add_member(node_id);

        let (consensus_sender, consensus_receiver) = consensus_network.add_node(node_id);
        let (storage_sender, storage_receiver) = storage_network.add_node(node_id);

        let replicator = NetworkReplicator::new(
            node_id,
            membership.clone(),
            metadata.clone(),
            storage_sender.clone(),
        );

        let write_ahead_log_path = format!("scratch/write_ahead_log_{}.bin", node_id).into();
        let write_ahead_index_path = format!("scratch/write_ahead_index_{}.bin", node_id).into();
        let mut storage = block_on(StorageCoordinator::create(
            node_id,
            write_ahead_log_path,
            write_ahead_index_path,
            replicator,
            storage_receiver,
            storage_sender,
        ))
        .unwrap();

        let handle = local.spawn_local(async move {
            let mut consensus = RoleConsensus::new(
                node_id,
                membership,
                metadata,
                consensus_sender,
                consensus_receiver,
            );

            loop {
                consensus.tick().await;
                yield_now().await;
                storage.tick().await.unwrap();
                yield_now().await;
            }
        });

        (node_id, handle)
    });

    //let mut storages = HashMap::new();
    let mut node_handles = Vec::new();
    for node in nodes {
        let (node_id, handle) = node;
        //storages.insert(node_id, storage);
        node_handles.push(handle);
    }

    let network_handle = local.spawn_local(async move {
        let mut last_write = Instant::now();

        loop {
            let leader = metadata.leader().await;
            match leader {
                Some(leader) => {
                    let now = Instant::now();
                    let elapsed = now - last_write;
                    if elapsed.as_secs() >= 1 {
                        let key = format!("key-{}", rand::random::<u32>());
                        let value = format!("value-{}", rand::random::<u32>());
                        println!("Writing {} to {}", key, leader);
                        storage_network
                            .forward_message(StorageMessage::RequestWrite(RequestWriteMessage {
                                writer: leader,
                                requester: 0,
                                request_id: "0".to_string(),
                                key: key.clone(),
                                value: value.clone(),
                            }))
                            .await;
                        last_write = now;
                    }
                    //println!("Writing {} to {}", key, leader);
                    // match storages.get_mut(&leader).unwrap().write(key, value).await {
                    //     Ok(_) => {
                    //         count += 1;
                    //         let now = Instant::now();
                    //         let elapsed = now - last_log;
                    //         if elapsed.as_secs() >= 1 {
                    //             // println!("{} writes per second", count);
                    //             // count = 0;
                    //             last_log = now;
                    //         }
                    //     }
                    //     Err(err) => return Err(err),
                    // }
                }
                None => {
                    //println!("No leader");
                }
            }

            //println!("network tick");
            consensus_network.tick().await;
            yield_now().await;
            storage_network.tick().await;
            yield_now().await;
        }
    });

    // let storage_handle = local.spawn_local(async move {
    //     let mut count = 0;
    //     let mut last_log = Instant::now();
    //     loop {
    //         let leader = metadata.leader().await;
    //         match leader {
    //             Some(leader) => {
    //                 let key = format!("key-{}", rand::random::<u32>());
    //                 let value = format!("value-{}", rand::random::<u32>());
    //                 //println!("Writing {} to {}", key, leader);
    //                 match storages.get_mut(&leader).unwrap().write(key, value).await {
    //                     Ok(_) => {
    //                         count += 1;
    //                         let now = Instant::now();
    //                         let elapsed = now - last_log;
    //                         if elapsed.as_secs() >= 1 {
    //                             println!("{} writes per second", count);
    //                             count = 0;
    //                             last_log = now;
    //                         }
    //                     }
    //                     Err(err) => return Err(err),
    //                 }
    //             }
    //             None => {
    //                 //println!("No leader");
    //             }
    //         }
    //         yield_now().await;

    //         storage.tick

    //         //sleep(Duration::from_millis(10)).await;
    //     }
    // });

    local.await;

    for handle in node_handles {
        handle.await?;
    }

    network_handle.await?;
    //storage_handle.await??;

    Ok(())
}
