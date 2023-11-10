use super::message::StorageMessage;
use async_trait::async_trait;

#[async_trait]
pub trait StorageReceiver {
    async fn try_recv(&self) -> Option<StorageMessage>;
}
