// use super::message::{Message, RequestEnvelope, Response};
// use crate::{bynamo_node::NodeId, messaging::message::new_message_id};
// use async_channel::{bounded, Receiver, SendError, Sender};
// use std::{collections::HashMap, time::Duration};

// #[derive(Clone)]
// pub enum MessageSender {
//     Memory(MemoryMessageSender),
// }

// impl MessageSender {
//     pub fn new_memory(
//         node_id: NodeId,
//         request_senders: HashMap<NodeId, Sender<RequestEnvelope>>,
//         response_receiver: Receiver<Response>,
//     ) -> Self {
//         MessageSender::Memory(MemoryMessageSender {
//             node_id,
//             request_senders,
//             response_waiters: HashMap::new(),
//             response_receiver,
//         })
//     }
//     pub async fn send_and_wait(
//         &mut self,
//         recipient: NodeId,
//         message: Message,
//     ) -> Result<(), MessageSendError> {
//         match self {
//             MessageSender::Memory(sender) => sender.send_and_wait(recipient, message).await,
//         }
//     }
//     pub async fn send_and_forget(
//         &mut self,
//         recipient: NodeId,
//         message: Message,
//     ) -> Result<(), MessageSendError> {
//         match self {
//             MessageSender::Memory(sender) => sender.send_and_forget(recipient, message).await,
//         }
//     }
//     pub async fn tick(&mut self) {
//         match self {
//             MessageSender::Memory(sender) => sender.tick().await,
//         }
//     }
// }
// type MessageSendError = SendError<RequestEnvelope>;
use crate::bynamo_node::NodeId;
use futures::future;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tonic::{transport::Channel, Request};

use super::{node_client::NodeClient, NodeMessage};

#[derive(Clone)]
pub struct MessageSender {
    // node_id: NodeId,
    clients: Arc<Mutex<HashMap<NodeId, NodeClient<Channel>>>>,
    // request_senders: HashMap<NodeId, Sender<RequestEnvelope>>,
    // response_waiters: HashMap<String, Sender<Response>>,
    // response_receiver: Receiver<Response>,
}

impl MessageSender {
    pub fn new(// node_id: NodeId,
        //clients: HashMap<NodeId, NodeClient<Channel>>,
        // request_senders: HashMap<NodeId, Sender<RequestEnvelope>>,
        // response_receiver: Receiver<Response>,
    ) -> Self {
        Self {
            // node_id,
            clients: Arc::new(Mutex::new(HashMap::new())),
            // request_senders,
            // response_waiters: HashMap::new(),
            // response_receiver,
        }
    }
    pub async fn add_client(&mut self, node_id: NodeId, client: NodeClient<Channel>) {
        self.clients.lock().await.insert(node_id, client);
    }
    pub async fn send_and_wait(
        &mut self,
        recipient: NodeId,
        message: NodeMessage,
    ) -> Result<(), SendError> {
        //println!("Sending message to {}", recipient);
        let lock = self.clients.lock().await;
        let client = lock.get(&recipient).cloned();
        drop(lock);

        client
            .unwrap()
            .send_message(Request::new(message))
            .await
            .map_err(|err| {
                println!("Error sending message: {:?}", err);
                SendError::Error
            })
            .map(|_| {})
    }
    pub async fn send_and_forget(&mut self, recipient: NodeId, message: NodeMessage) {
        //println!("Sending message to {}", recipient);
        let lock = self.clients.lock().await;
        let mut client = lock.get(&recipient).cloned().unwrap();
        drop(lock);

        // let
        let message_clone = message.clone();
        tokio::spawn(async move { client.send_message(Request::new(message_clone)).await });

        // tokio::spawn(future::lazy(move |_| {
        //     client.send_message(Request::new(message))
        // }));
        // client
        //     .unwrap()
        //     .send_message(Request::new(message))
        //     .await
        //     .map_err(|err| {
        //         println!("Error sending message: {:?}", err);
        //         SendError::Error
        //     })
        //     .map(|_| {})
        //     .unwrap();
    }
    // async fn tick(&mut self) {
    //     match self.response_receiver.try_recv() {
    //         Ok(response) => match self.response_waiters.remove(&response.message_id) {
    //             Some(sender) => sender.send(response).await.unwrap(),
    //             None => (),
    //         },
    //         Err(_) => (),
    //     }
    // }
}

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("failed to send message")]
    Error,
}
