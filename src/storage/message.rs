use super::commands::commands::StorageCommand;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum StorageMessage {
    Command(StorageCommand),
    Query(StorageQuery),
}

#[derive(Deserialize, Serialize)]
pub enum StorageQuery {
    Read(ReadQuery),
}

#[derive(Deserialize, Serialize)]
pub struct ReadQuery {
    pub key: String,
}
