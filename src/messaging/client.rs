use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};
use quinn::{ClientConfig, Connection, Endpoint};
use rand::seq::SliceRandom;
use std::{sync::Arc, time::Instant};
use tokio::io::AsyncWriteExt;

use super::message::Message;

#[derive(Clone)]
pub struct MessageClient {
    connections: Arc<Vec<Connection>>,
    stream_open_histogram: Histogram,
    stream_ttfb_histogram: Histogram,
}

impl MessageClient {
    pub async fn connect(
        address: &str,
        stream_open_histogram: Histogram,
        stream_ttfb_histogram: Histogram,
    ) -> Self {
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(configure_client());

        // connect to server
        let mut connections = Vec::new();
        for _ in 0..2 {
            connections.push(
                endpoint
                    .connect(address.parse().unwrap(), "localhost")
                    .unwrap()
                    .await
                    .unwrap(),
            );
        }

        //println!("[client] connected: addr={}", connection.remote_address());

        MessageClient {
            connections: Arc::new(connections),
            stream_open_histogram,
            stream_ttfb_histogram,
        }
    }

    pub async fn send_message(&self, message: Message) {
        let start = Instant::now();
        let mut backoff = 100;
        let (mut send, mut recv) = loop {
            let client = self.connections.choose(&mut rand::thread_rng());
            match client.unwrap().open_bi().await {
                Ok((send, recv)) => break (send, recv),
                Err(e) => {
                    println!("Error opening stream: {:?}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    backoff *= 2;
                }
            }
        };
        self.stream_open_histogram
            .observe(start.elapsed().as_secs_f64());

        let ttfb = Instant::now();
        let bytes = bincode::serialize(&message).unwrap();
        send.write_u32(bytes.len() as u32).await.unwrap();
        send.write_all(&bytes).await.unwrap();
        recv.read_to_end(1024 * 1024).await.unwrap();
        self.stream_ttfb_histogram
            .observe(ttfb.elapsed().as_secs_f64());
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
