// use super::{message::StorageMessage, receiver::StorageReceiver, sender::StorageSender};
// use crate::bynamo_node::NodeId;
// use async_channel::{bounded, Receiver, Sender};
// use async_trait::async_trait;
// use std::collections::HashMap;

// pub struct DirectHubStorageNetwork {
//     hub_senders: HashMap<NodeId, Sender<StorageMessage>>,
//     node_sender: Sender<StorageMessage>,
//     hub_receiver: Receiver<StorageMessage>,
// }

// impl DirectHubStorageNetwork {
//     pub fn new() -> Self {
//         let (node_sender, hub_receiver) = bounded(100);
//         Self {
//             hub_senders: HashMap::new(),
//             node_sender,
//             hub_receiver,
//         }
//     }
//     pub fn add_node(&mut self, node_id: NodeId) -> (HubMessageSender, HubMessageReceiver) {
//         let (hub_sender, node_receiver) = bounded(100);

//         self.hub_senders.insert(node_id, hub_sender);

//         (
//             HubMessageSender {
//                 sender: self.node_sender.clone(),
//             },
//             HubMessageReceiver {
//                 receiver: node_receiver,
//             },
//         )
//     }
//     pub async fn tick(&self) {
//         match self.hub_receiver.try_recv() {
//             Ok(message) => self.forward_message(message).await,
//             Err(_) => {}
//         };
//     }
//     pub async fn forward_message(&self, message: StorageMessage) {
//         match message {
//             StorageMessage::RequestWrite(ref request) => {
//                 println!("forward_message RequestWrite");
//                 self.hub_senders
//                     .get(&request.writer)
//                     .unwrap()
//                     .send(message)
//                     .await
//                     .unwrap();
//             }
//             StorageMessage::AcknowledgeWrite(ref acknowledge) => {
//                 println!("acknowledged write");
//                 match acknowledge.requester {
//                     0 => {
//                         println!("acknowledging external write: {}", acknowledge.request_id);
//                     }
//                     _ => {
//                         println!("acknowledgement from: {}", acknowledge.requester);
//                         self.hub_senders
//                             .get(&acknowledge.writer)
//                             .unwrap()
//                             .send(message)
//                             .await
//                             .unwrap();
//                     }
//                 }
//             }
//         }
//     }
// }

// #[derive(Clone)]
// pub struct HubMessageSender {
//     sender: Sender<StorageMessage>,
// }

// #[async_trait]
// impl StorageSender for HubMessageSender {
//     async fn try_send(&self, message: StorageMessage) -> bool {
//         self.sender.send(message).await.is_ok()
//     }
// }

// pub struct HubMessageReceiver {
//     receiver: Receiver<StorageMessage>,
// }

// #[async_trait]
// impl StorageReceiver for HubMessageReceiver {
//     async fn try_recv(&self) -> Option<StorageMessage> {
//         match self.receiver.try_recv() {
//             Ok(message) => Some(message),
//             Err(_) => None,
//         }
//     }
// }
