use crate::bynamo_node::NodeId;

pub mod client;
pub mod handler;
pub mod message;
pub mod receiver;
pub mod sender;
pub mod server;

tonic::include_proto!("message");

pub type Recipient = NodeId;
