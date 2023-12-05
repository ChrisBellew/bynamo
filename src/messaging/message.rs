use crate::{consensus::message::RoleConsensusMessage, storage::message::StorageMessage};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum Message {
    Storage(StorageMessage),
    RoleConsensus(RoleConsensusMessage),
}

#[derive(Deserialize, Serialize)]
pub enum Reply {
    Ok,
}
