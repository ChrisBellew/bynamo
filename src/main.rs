mod bynamo_node;
mod consensus;
mod coordinator;
mod membership;
mod metadata_service;
mod storage;
mod util;
use anyhow::Result;
use bynamo_node::{BynamoNode, BynamoNodeOptions};
use consensus::network::{Network, NodeChannel};
use crossbeam::channel::bounded;
use std::{collections::HashMap, thread};

pub fn main() -> Result<()> {
    let nodes: Vec<_> = (1..=3).into_iter().collect(); // vec![1, 2, 3];

    let mut node_channels = HashMap::new();

    for node_id in nodes.clone() {
        let nodes = nodes.clone();

        let (node_sender, network_receiver) = bounded(100);
        let (network_sender, node_receiver) = bounded(100);
        node_channels.insert(
            node_id,
            NodeChannel {
                node_id,
                sender: network_sender,
                receiver: network_receiver,
            },
        );

        thread::spawn(move || {
            let mut node = BynamoNode::create(
                node_id,
                BynamoNodeOptions {
                    write_ahead_log_path: format!("scratch/write_ahead_log_{}.bin", node_id).into(),
                    write_ahead_index_path: format!("scratch/write_ahead_index_{}.bin", node_id)
                        .into(),
                    nodes,
                    sender: node_sender,
                    receiver: node_receiver,
                },
            )
            .unwrap();

            loop {
                node.tick();
            }
        });
    }

    let mut network = Network::new(node_channels);

    loop {
        network.run();
    }

    Ok(())
}
