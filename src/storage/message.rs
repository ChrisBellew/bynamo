use crate::bynamo_node::NodeId;

pub enum StorageMessage {
    RequestWrite(RequestWriteMessage),
    AcknowledgeWrite(AcknowledgeWriteMessage),
}

pub struct RequestWriteMessage {
    pub request_id: String,
    pub requester: NodeId,
    pub writer: NodeId,
    pub key: String,
    pub value: String,
}

pub struct AcknowledgeWriteMessage {
    pub request_id: String,
    pub requester: NodeId,
    pub writer: NodeId,
    pub key: String,
    pub value: String,
}
