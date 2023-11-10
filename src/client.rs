// use std::collections::HashMap;

// use async_channel::Sender;

// use crate::{
//     metadata::MetadataService,
//     storage::message::{StorageMessage, WriteMessage},
// };

// pub struct ClusterClient<Metadata: MetadataService> {
//     metadata: Metadata,
//     senders: HashMap<u32, Sender<StorageMessage>>,
// }

// impl<Metadata: MetadataService> ClusterClient<Metadata> {
//     pub fn new(metadata: Metadata) -> Self {
//         Self {
//             senders: HashMap::new(),
//             metadata,
//         }
//     }
//     pub fn add_node(&mut self, node_id: u32, sender: Sender<StorageMessage>) {
//         self.senders.insert(node_id, sender);
//     }
//     pub async fn write(&self, key: String, value: String) {
//         let leader = self.metadata.leader().await;
//         let leader = match leader {
//             None => return println!("Skipping write because no leader"),
//             Some(leader) => leader,
//         };

//         let sender = self.senders.get(&leader).unwrap();
//         sender
//             .send(StorageMessage::Write(WriteMessage {
//                 key: key.clone(),
//                 value: value.clone(),
//             }))
//             .await
//             .unwrap();
//     }
//     // pub async fn read(&self, key: String) -> Option<String> {
//     //     let mut values = Vec::new();
//     //     for (_, sender) in self.senders.iter() {
//     //         sender
//     //             .send(StorageMessage::Read(ReadMessage { key: key.clone() }))
//     //             .await
//     //             .unwrap();
//     //         // match sender.recv().await.unwrap() {
//     //         //     StorageMessage::ReadResponse { value } => values.push(value),
//     //         //     _ => {}
//     //         // }
//     //     }
//     //     values.sort();
//     //     values.pop()
//     // }
// }
