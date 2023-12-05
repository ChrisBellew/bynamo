use crate::bynamo_node::NodeId;
pub mod client;
pub mod handler;
pub mod message;
pub mod sender;
pub mod server;

pub type Recipient = NodeId;
