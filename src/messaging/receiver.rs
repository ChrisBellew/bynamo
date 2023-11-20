// use super::{
//     handler::MessageHandler,
//     message::{RequestEnvelope, Response},
// };
// use crate::{bynamo_node::NodeId, storage::handle_message::StorageError};
// use async_channel::Sender;
// use std::{collections::HashMap, error::Error, sync::Arc};
// use tokio::sync::Mutex;

// #[derive(Clone)]
// pub enum MessageReceiver {
//     Memory(MemoryMessageReceiver),
// }

// impl MessageReceiver {
//     pub fn new_memory(
//         message_handler: MessageHandler,
//         //request_receiver: Receiver<RequestEnvelope>,
//         response_senders: HashMap<NodeId, Sender<Response>>,
//     ) -> Self {
//         MessageReceiver::Memory(MemoryMessageReceiver {
//             message_handler: Arc::new(Mutex::new(message_handler)),
//             //request_receiver,
//             response_senders,
//         })
//     }
//     pub async fn handle(&self, envelope: RequestEnvelope) -> Result<(), StorageError> {
//         match self {
//             MessageReceiver::Memory(receiver) => receiver.handle(envelope).await,
//         }
//     }
//     pub fn respond(&self, message_id: String, result: Result<(), Box<dyn Error>>) {}
// }

// #[derive(Clone)]
// pub struct MemoryMessageReceiver {
//     message_handler: Arc<Mutex<MessageHandler>>,
//     response_senders: HashMap<NodeId, Sender<Response>>,
// }

// impl MemoryMessageReceiver {
//     async fn handle(&self, envelope: RequestEnvelope) -> Result<(), StorageError> {
//         self.message_handler
//             .lock()
//             .await
//             .handle(envelope.message)
//             .await
//     }
//     pub fn respond(&self, message_id: String, recipient: NodeId) {
//         self.response_senders
//             .get(&recipient)
//             .unwrap()
//             .try_send(Response {
//                 message_id,
//                 recipient,
//             });
//     }
// }
