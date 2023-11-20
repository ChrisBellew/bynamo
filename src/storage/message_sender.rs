// use async_trait::async_trait;

// use super::message::StorageMessage;

// #[async_trait]
// pub trait StorageMessageSender {
//     async fn send(&self, message: StorageMessage) -> Result<(), SendError>;
// }

// #[derive(thiserror::Error, Debug)]
// enum SendError {
//     #[error("failed to send storage message due to timeout")]
//     Timeout,
// }
