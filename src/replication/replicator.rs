use crate::{
    bynamo_node::NodeId,
    membership::MembershipService,
    metadata::MetadataService,
    storage::{
        message::{RequestWriteMessage, StorageMessage},
        sender::StorageSender,
    },
};
use async_trait::async_trait;

#[async_trait]
pub trait Replicator {
    async fn replicate(&self, key: String, value: String);
}

pub struct NetworkReplicator<
    S: StorageSender + Send + Sync,
    Metadata: MetadataService + Send + Sync,
> {
    node_id: NodeId,
    metadata: Metadata,
    membership: MembershipService,
    sender: S,
}

impl<S: StorageSender + Send + Sync, Metadata: MetadataService + Send + Sync>
    NetworkReplicator<S, Metadata>
{
    pub fn new(
        node_id: NodeId,
        membership: MembershipService,
        metadata: Metadata,
        sender: S,
    ) -> Self {
        Self {
            node_id,
            metadata,
            membership,
            sender,
        }
    }
}

#[async_trait]
impl<S: StorageSender + Send + Sync, Metadata: MetadataService + Send + Sync> Replicator
    for NetworkReplicator<S, Metadata>
{
    async fn replicate(&self, key: String, value: String) {
        if let Some(leader) = self.metadata.leader().await {
            if leader != self.node_id {
                // Don't replicate because we're not the leader
                // println!(
                //     "[{}]: not replicating {} because we're not the leader",
                //     self.node_id, key
                // );
                return;
            }
        }
        let members = self.membership.members();
        for member in members {
            if member != self.node_id {
                println!("{}: replicating {} to {}", self.node_id, key, member);
                self.sender
                    .try_send(StorageMessage::RequestWrite(RequestWriteMessage {
                        requester: self.node_id,
                        writer: member,
                        request_id: "0".to_string(),
                        key: key.clone(),
                        value: value.clone(),
                    }))
                    .await;
            }
        }
    }
}
