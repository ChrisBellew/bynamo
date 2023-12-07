use super::handler::MessageHandler;
use crate::{
    messaging::message::{Message, Reply},
    start,
};
use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};
use quinn::{Endpoint, ServerConfig};
use std::{error::Error, net::SocketAddr, sync::Arc, time::Instant};
use tokio::io::AsyncReadExt;

pub struct NodeService {
    //handler: Arc<Mutex<MessageHandler>>,
}

impl NodeService {
    pub async fn start(address: &str, handler: MessageHandler, registry: &Registry) -> Self {
        let (endpoint, _server_cert) = make_server_endpoint(address.parse().unwrap()).unwrap();

        let deserialize_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "message_deserialize_histogram",
                "Message deserialize duration in microseconds",
            )
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(deserialize_histogram.clone()))
            .unwrap();

        // accept a single connection

        tokio::spawn(async move {
            loop {
                let handler = handler.clone();
                let incoming_conn = endpoint.accept().await.unwrap();
                let conn = incoming_conn.await.unwrap();
                let deserialize_histogram = deserialize_histogram.clone();
                println!(
                    "[server] connection accepted: addr={}",
                    conn.remote_address()
                );

                tokio::spawn(async move {
                    loop {
                        let mut handler = handler.clone();
                        let deserialize_histogram = deserialize_histogram.clone();
                        let (mut send, mut recv) = match conn.accept_bi().await {
                            Ok(s) => s,
                            Err(_) => {
                                println!("[server] connection closed");
                                return;
                            }
                        };
                        let start = Instant::now();
                        tokio::spawn(async move {
                            let len = recv.read_u32().await.unwrap() as usize;
                            let mut buf = vec![0; len];
                            recv.read_exact(&mut buf).await.unwrap();
                            let message = bincode::deserialize::<Message>(&buf).unwrap();
                            deserialize_histogram.observe(start.elapsed().as_micros() as f64);

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
