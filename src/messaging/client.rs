use std::sync::Arc;

use quinn::{ClientConfig, Connection, Endpoint, ServerConfig};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use super::message::Message;

#[derive(Clone)]
pub struct MessageClient {
    connection: Arc<Connection>,
}

impl MessageClient {
    pub async fn connect(address: &str) -> Self {
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(configure_client());

        // connect to server
        let connection = endpoint
            .connect(address.parse().unwrap(), "localhost")
            .unwrap()
            .await
            .unwrap();
        println!("[client] connected: addr={}", connection.remote_address());

        // let mut client = MessageClient::connect(address).await?; //"http://[::1]:50051"

        // let request = tonic::Request::new(HelloRequest {
        //     name: "Tonic".into(),
        // });

        // let response = client.say_hello(request).await?;

        // StorageClient {}

        MessageClient {
            connection: Arc::new(connection),
        }
    }

    pub async fn send_message(&self, message: Message) {
        let (mut send, mut recv) = self.connection.open_bi().await.unwrap();
        let bytes = bincode::serialize(&message).unwrap();
        send.write_u32(bytes.len() as u32).await.unwrap();
        send.write_all(&bytes).await.unwrap();
        recv.read_to_end(1024 * 1024).await.unwrap();
    }
}

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

fn configure_client() -> ClientConfig {
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    ClientConfig::new(Arc::new(crypto))
}
