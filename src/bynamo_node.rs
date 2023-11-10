// use crate::storage::message::{StorageMessage, StorageReceiver};
// use crate::{
//     consensus::{
//         message::{ConsensusReceiver, ConsensusSender},
//         role::Role,
//         role_consensus::RoleConsensus,
//     },
//     coordinator::Coordinator,
//     membership::MembershipService,
//     metadata::MetadataService,
// };
// use anyhow::{anyhow, Result};
// use std::path::PathBuf;

pub type NodeId = u32;

// pub struct BynamoNode<
//     S: ConsensusSender,
//     R: ConsensusReceiver,
//     Storage: StorageReceiver,
//     Metadata: MetadataService,
// > {
//     #[allow(dead_code)]
//     node_id: NodeId,
//     coordinator: Coordinator,
//     role_consensus: RoleConsensus<S, R, Metadata>,
//     storage_receiver: Storage,
// }

// pub struct BynamoNodeOptions<
//     S: ConsensusSender,
//     R: ConsensusReceiver,
//     Storage: StorageReceiver,
//     Metadata: MetadataService,
// > {
//     pub write_ahead_log_path: PathBuf,
//     pub write_ahead_index_path: PathBuf,
//     pub consensus_sender: S,
//     pub consensus_receiver: R,
//     pub storage_receiver: Storage,
//     pub metadata: Metadata,
//     pub membership: MembershipService,
// }

// impl<
//         S: ConsensusSender,
//         R: ConsensusReceiver,
//         Storage: StorageReceiver,
//         Metadata: MetadataService,
//     > BynamoNode<S, R, Storage, Metadata>
// {
//     pub async fn create(
//         node_id: NodeId,
//         options: BynamoNodeOptions<S, R, Storage, Metadata>,
//     ) -> Result<BynamoNode<S, R, Storage, Metadata>> {
//         let BynamoNodeOptions {
//             write_ahead_log_path,
//             write_ahead_index_path,
//             consensus_sender,
//             consensus_receiver,
//             storage_receiver,
//             metadata,
//             membership,
//         } = options;

//         Ok(BynamoNode {
//             node_id,
//             coordinator: Coordinator::create(write_ahead_log_path, write_ahead_index_path).await?,
//             role_consensus: RoleConsensus::new(
//                 node_id,
//                 membership,
//                 metadata,
//                 consensus_sender,
//                 consensus_receiver,
//             ),
//             storage_receiver,
//         })
//     }
//     pub async fn consensus_tick(&mut self) {
//         self.role_consensus.tick().await;
//     }
//     pub async fn storage_tick(&mut self) -> Result<()> {
//         let message = self.storage_receiver.recv().await.unwrap();
//         match message {
//             StorageMessage::Write(message) => self.write(message.key, message.value).await?,
//             StorageMessage::Read(_) => todo!(),
//         };
//         Ok(())
//     }
//     async fn write(&mut self, key: String, value: String) -> Result<()> {
//         if self.role_consensus.role() != &Role::Leader {
//             return Err(anyhow!("Not leader"));
//         }
//         self.coordinator.write(key, value).await
//     }
//     fn read(&mut self, key: &String) -> Option<String> {
//         self.coordinator.read(key)
//     }
// }
