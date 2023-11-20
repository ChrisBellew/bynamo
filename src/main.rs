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
use messaging::{node_client::NodeClient, sender::MessageSender};
use role_service::RoleService;
use start::start_in_memory_node;
use std::time::Duration;
use tokio::time::Instant;

use crate::messaging::WriteCommand;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<()> {
    let members: Vec<_> = (1..=3).into_iter().collect();

    // let request_channels = members.iter().map(|_| bounded(100)).collect::<Vec<_>>();
    // let response_channels = members.iter().map(|_| bounded(100)).collect::<Vec<_>>();

    // let (request_senders, request_receivers): (
    //     Vec<Sender<RequestEnvelope>>,
    //     Vec<Receiver<RequestEnvelope>>,
    // ) = request_channels.into_iter().unzip();
    // let (response_senders, response_receivers): (Vec<Sender<Response>>, Vec<Receiver<Response>>) =
    //     response_channels.into_iter().unzip();

    // let mut request_senders = request_senders
    //     .into_iter()
    //     .enumerate()
    //     .map(|(i, sender)| ((i + 1) as u32, sender))
    //     .collect::<HashMap<u32, Sender<RequestEnvelope>>>();
    // let response_senders = response_senders
    //     .into_iter()
    //     .enumerate()
    //     .map(|(i, sender)| ((i + 1) as u32, sender))
    //     .collect::<HashMap<u32, Sender<Response>>>();

    let mut message_sender = MessageSender::new(
        //message_clients,
        //request_senders.clone(),
        //response_receiver,
    );

    // let mut message_senders = HashMap::new();
    // for node_id in members.clone() {
    //     let message_sender = MessageSender::new(node_id, message_clients.clone());
    //     message_senders.insert(node_id, message_sender);
    // }

    let role_service = RoleService::new_memory();

    for node_id in members.clone()
    // .zip(request_receivers.into_iter())
    // .zip(response_receivers.into_iter())
    {
        let members = members.clone();

        //let message_sender = MessageSender::new(node_id, message_clients.clone());

        let message_sender = message_sender.clone();
        let role_service = role_service.clone();
        //let response_senders = response_senders.clone();

        //tokio::spawn(async move {
        start_in_memory_node(node_id, members, role_service, message_sender)
            .await
            .unwrap()
        //});
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    //let mut message_clients = HashMap::new();
    for node_id in members.clone() {
        let addr = format!("http://localhost:{}", 50000 + node_id);
        //let channel = Channel::from_shared(addr).unwrap();
        let client = NodeClient::connect(addr).await.unwrap();
        message_sender.add_client(node_id, client).await;
        //message_clients.insert(node_id, client);
    }

    //let mut last_write = Instant::now();

    loop {
        let leader = role_service.leader().await;
        match leader {
            Some(leader) => {
                // let now = Instant::now();
                // let elapsed = now - last_write;
                // if elapsed.as_secs() >= 1 {
                let key = format!("key-{}", rand::random::<u32>());
                let value = format!("value-{}", rand::random::<u32>());
                println!("Writing {} to {}", key, leader);

                message_sender
                    .clone()
                    .send_and_wait(leader, WriteCommand { key, value }.into())
                    .await
                    .unwrap();

                //     last_write = now;
                // }
            }
            None => (),
        }
    }
}
