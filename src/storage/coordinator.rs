use crate::bynamo_node::NodeId;
use crate::replication::replicator::Replicator;
use crate::storage::{skip_list::SkipList, write_ahead_log::WriteAheadLog};
use anyhow::Result;
use std::path::PathBuf;

use super::message::{AcknowledgeWriteMessage, StorageMessage};
use super::receiver::StorageReceiver;
use super::sender::StorageSender;

pub struct StorageCoordinator<R: Replicator, Receiver: StorageReceiver, Sender: StorageSender> {
    node_id: NodeId,
    skip_list: SkipList,
    write_ahead_log: WriteAheadLog,
    replicator: R,
    receiver: Receiver,
    sender: Sender,
}

impl<R: Replicator, Receiver: StorageReceiver, Sender: StorageSender>
    StorageCoordinator<R, Receiver, Sender>
{
    pub async fn create(
        node_id: NodeId,
        write_ahead_log_path: PathBuf,
        write_ahead_index_path: PathBuf,
        replicator: R,
        receiver: Receiver,
        sender: Sender,
    ) -> Result<Self> {
        Ok(Self {
            node_id,
            skip_list: SkipList::new(),
            write_ahead_log: WriteAheadLog::create_or_open_for_append(
                write_ahead_log_path,
                write_ahead_index_path,
            )
            .await?,
            replicator,
            receiver,
            sender,
        })
    }
    pub async fn tick(&mut self) -> Result<()> {
        match self.receiver.try_recv().await {
            Some(message) => {
                self.receive_message(message).await?;
            }
            None => (),
        }
        Ok(())
    }
    pub async fn receive_message(&mut self, message: StorageMessage) -> Result<()> {
        match message {
            StorageMessage::RequestWrite(message) => {
                self.write(message.key.clone(), message.value.clone())
                    .await?;
                println!(
                    "[{}]: sending acknowledgement to {}",
                    self.node_id, message.requester
                );
                self.sender
                    .try_send(StorageMessage::AcknowledgeWrite(AcknowledgeWriteMessage {
                        request_id: message.request_id,
                        requester: message.requester,
                        writer: message.writer,
                        key: message.key,
                        value: message.value,
                    }))
            }
            StorageMessage::AcknowledgeWrite(_) => todo!(),
        };
        Ok(())
    }
    // pub async fn read(&self, key: &str) -> Option<String> {
    //     self.skip_list.find(key)
    // }
    pub async fn write(&mut self, key: String, value: String) -> Result<()> {
        let position = self.write_ahead_log.position() + 1;
        self.write_ahead_log
            .write_entry(position, &key, &value)
            .await?;
        println!("[{}]: Wrote to WAL: {} {}", self.node_id, key, value);
        self.replicator.replicate(key, value).await;
        // self.skip_list
        //     .insert(key.to_string(), value.to_string(), coin_flip());
        // println!("Size: {}", self.skip_list.size);
        // if self.skip_list.size > 1_000_000 {
        //     SSTable::write_from_iter(self.skip_list.iter())?;
        //     self.skip_list = SkipList::new();
        // }
        Ok(())
    }
    // pub fn start_storage(&self) {
    //     tokio::task::spawn(async {
    //         loop {
    //             let message = self.receiver.recv().await.unwrap();
    //             match message {
    //                 StorageMessage::Write(message) => {
    //                     self.write(message.key, message.value).await?
    //                 }
    //                 StorageMessage::Read(_) => todo!(),
    //             };
    //         }
    //     });
    // }
    // pub fn start_maintenance(&mut self) {
    //     tokio::task::spawn_blocking(|| loop {
    //         sleep(Duration::from_secs(1));
    //         compact_ss_tables().expect("Failure compacting SS tables");
    //     });
    // }
}
