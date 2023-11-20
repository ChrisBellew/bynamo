use crate::{
    bynamo_node::NodeId,
    consensus::role_consensus::RoleConsensus,
    messaging::{
        handler::MessageHandler, node_server::NodeServer, sender::MessageSender,
        server::NodeService,
    },
    role_service::RoleService,
    storage::{
        handle_message::StorageMessageHandler,
        replicator::StorageReplicator,
        skip_list::SkipList,
        write_ahead_log::{self, WriteAheadLog},
    },
};
use tokio::task::yield_now;
use tonic::transport::Server;

pub async fn start_in_memory_node(
    node_id: NodeId,
    members: Vec<NodeId>,
    role_service: RoleService,
    message_sender: MessageSender,
    //sender: MessageSender,
) -> Result<(), NodeRuntimeError> {
    let role_consensus = RoleConsensus::new(
        node_id,
        members.clone(),
        role_service.clone(),
        message_sender.clone(), //sender.clone(),
    );

    let log_path = format!("scratch/write_ahead_log_{}.bin", node_id).into();
    let index_path = format!("scratch/write_ahead_index_{}.bin", node_id).into();
    let write_ahead_log = WriteAheadLog::create(log_path, index_path).await?;
    let skip_list = SkipList::new();

    let storage_message_handler = StorageMessageHandler::new(
        node_id,
        role_service,
        write_ahead_log,
        skip_list,
        StorageReplicator::new(members, message_sender),
    );
    let message_handler = MessageHandler::new(storage_message_handler, role_consensus.clone());
    let message_server = NodeService::new(message_handler);
    //let message_receiver = MessageReceiver::new_memory(message_handler, response_senders.clone());

    // tokio::spawn(async move {
    let addr = format!("127.0.0.1:{}", 50000 + node_id);
    //let greeter = Node::default();

    tokio::spawn(async move {
        Server::builder()
            .add_service(NodeServer::new(message_server))
            .serve(addr.parse().unwrap())
            .await
            .unwrap();
    });
    // loop {
    //     match request_receiver.recv().await {
    //         Ok(envelope) => {
    //             message_receiver.handle(envelope).await.unwrap();
    //         }
    //         Err(_) => (),
    //     }
    //     yield_now().await;
    // }
    // });

    tokio::spawn(async move {
        loop {
            role_consensus.tick().await;
            yield_now().await;
        }
    });

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum NodeRuntimeError {
    #[error("Failed to create write ahead log")]
    WriteAheadLogError(#[from] write_ahead_log::CreateError),
}
