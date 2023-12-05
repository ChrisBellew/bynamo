use crate::bynamo_node::NodeId;
use maplit::hashmap;
use prometheus::{labels, Registry};
use std::{thread, time};

pub fn init_metrics(node_id: NodeId) -> Registry {
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

    registry
}
