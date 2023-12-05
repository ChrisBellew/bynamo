use super::handler::MessageHandler;
use crate::{
    messaging::message::{Message, Reply},
    storage::handle_message::StorageError,
};
use quinn::{Endpoint, ServerConfig};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::{io::AsyncReadExt, sync::Mutex};
use tonic::{Request, Response, Status};

pub struct NodeService {
    //handler: Arc<Mutex<MessageHandler>>,
}

impl NodeService {
    pub async fn start(address: &str, handler: MessageHandler) -> Self {
        let (endpoint, _server_cert) = make_server_endpoint(address.parse().unwrap()).unwrap();
        // accept a single connection

        tokio::spawn(async move {
            loop {
                let handler = handler.clone();
                let incoming_conn = endpoint.accept().await.unwrap();
                let conn = incoming_conn.await.unwrap();
                println!(
                    "[server] connection accepted: addr={}",
                    conn.remote_address()
                );

                tokio::spawn(async move {
                    loop {
                        let mut handler = handler.clone();
                        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
                        tokio::spawn(async move {
                            let len = recv.read_u32().await.unwrap() as usize;
                            let mut buf = vec![0; len];
                            recv.read_exact(&mut buf).await.unwrap();
                            let message = bincode::deserialize::<Message>(&buf).unwrap();
                            handler.handle(message).await.unwrap();

                            let response = Reply::Ok;
                            let response = bincode::serialize(&response).unwrap();
                            send.write_all(&response).await.unwrap();
                        });
                    }
                });
            }
        });

        NodeService {}

        // Self {
        //     handler: Arc::new(Mutex::new(handler)),
        // }
    }
}

pub fn make_server_endpoint(bind_addr: SocketAddr) -> Result<(Endpoint, Vec<u8>), Box<dyn Error>> {
    let (server_config, server_cert) = configure_server()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, server_cert))
}

fn configure_server() -> Result<(ServerConfig, Vec<u8>), Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
}

// #[tonic::async_trait]
// impl Node for NodeService {
//     async fn send_message(
//         &self,
//         request: Request<NodeMessage>,
//     ) -> Result<Response<NodeResponse>, Status> {
//         //println!("Got a request: {:?}", request);

//         let lock = self.handler.lock().await;
//         let mut handler = lock.clone();
//         drop(lock);

//         //println!("Got lock");

//         handler
//             .handle(request.into_inner().message.unwrap())
//             .await
//             .map(|_| Response::new(NodeResponse {}))
//             .map_err(|e| match e {
//                 StorageError::WriteError(e) => Status::internal(e.to_string()),
//                 StorageError::WriteReplicaError(e) => Status::internal(e.to_string()),
//             })
//     }
// }
