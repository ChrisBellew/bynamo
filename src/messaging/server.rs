use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use super::{handler::MessageHandler, node_server::Node, NodeMessage, NodeResponse};

// #[derive(Debug, Default)]
pub struct NodeService {
    handler: Arc<Mutex<MessageHandler>>,
}

impl NodeService {
    pub fn new(handler: MessageHandler) -> Self {
        Self {
            handler: Arc::new(Mutex::new(handler)),
        }
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn send_message(
        &self,
        request: Request<NodeMessage>,
    ) -> Result<Response<NodeResponse>, Status> {
        //println!("Got a request: {:?}", request);

        let lock = self.handler.lock().await;
        let mut handler = lock.clone();
        drop(lock);

        //println!("Got lock");

        handler
            .handle(request.into_inner().message.unwrap())
            .await
            .unwrap();

        //println!("Processed a request");

        let reply = NodeResponse {};

        Ok(Response::new(reply))
    }
}
