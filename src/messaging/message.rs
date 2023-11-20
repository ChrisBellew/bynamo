// use crate::{
//     bynamo_node::NodeId, consensus::message::RoleConsensusMessage, storage::message::StorageMessage,
// };

//pub mod message {
// pub mod message {
//     // proto package root
//     tonic::include_proto!("message");

//     pub mod shared {
//         // proto package message.shared
//         tonic::include_proto!("message.storage_message");
//     }
// }

// tonic::include_proto!("message");
// tonic::include_proto!("message.storage_message");
//}

// pub enum Message {
//     Storage(StorageMessage),
//     RoleConsensus(RoleConsensusMessage),
// }

// impl Message {
//     pub fn message_id(&self) -> &String {
//         match self {
//             Message::Storage(message) => message.message_id(),
//             Message::RoleConsensus(message) => message.message_id(),
//         }
//     }
// }

// pub struct RequestEnvelope {
//     pub message_id: String,
//     pub sender: NodeId,
//     pub recipient: NodeId,
//     pub message: Message,
// }

// pub struct Response {
//     pub message_id: String,
//     pub recipient: u32,
// }

pub fn new_message_id() -> String {
    // Generate an 8 character random string
    let mut message_id = String::new();
    for _ in 0..8 {
        let random_char = (rand::random::<f32>() * 26.0) as u8 + 97;
        message_id.push(random_char as char);
    }
    message_id
}
