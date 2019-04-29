use futures::stream::Stream;
use futures::Future;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use protobuf::Message;
use reqwest::r#async::Client;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct RemoteNode {
    pub addr: SocketAddr,
    pub signers: Vec<Arc<InMemorySigner>>,
    pub nonces: Vec<u64>,
    pub url: String,
    client: Arc<Client>,
}

impl RemoteNode {
    pub fn new(addr: SocketAddr, signers: &[String], init_nonce: u64) -> Arc<RwLock<Self>> {
        let url = format!("http://{}", addr);
        let signers: Vec<_> = signers
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), s.as_str())))
            .collect();
        let nonces = vec![init_nonce; signers.len()];
        let client = Arc::new(
            Client::builder().use_rustls_tls().connect_timeout(CONNECT_TIMEOUT).build().unwrap(),
        );
        Arc::new(RwLock::new(Self { addr, signers, nonces, url, client }))
    }

    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let transaction: near_protos::signed_transaction::SignedTransaction = transaction.into();
        let tx_bytes = transaction.write_to_bytes().expect("write to bytes failed");
        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        let response = self
            .client
            .post(url.as_str())
            .form(&[("tx", format!("0x{}", hex::encode(&tx_bytes)))])
            .send()
            .map(|_| ())
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    pub fn get_current_height(&self) -> Box<dyn Future<Item = u64, Error = String> + Send> {
        let url = format!("{}{}", self.url, "/status");
        let res = self
            .client
            .post(url.as_str())
            .send()
            .and_then(|mut resp| resp.json())
            .map(|response: serde_json::Value| {
                response["result"]["sync_info"]["latest_block_height"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .map_err(|err| format!("{}", err));
        Box::new(res)
    }

    pub fn get_transactions(
        &self,
        min_height: u64,
        max_height: u64,
    ) -> Box<dyn Future<Item = u64, Error = String> + Send> {
        let chunk_size = 20u64;
        let mut chunks = vec![];
        let mut curr = min_height;
        loop {
            chunks.push((curr, curr + chunk_size));
            curr += chunk_size + 1;
            if curr > max_height {
                break;
            }
        }
        let client = self.client.clone();
        let url = self.url.clone();
        let result =
            futures::stream::iter_ok(chunks).fold(0u64, move |total_tx: u64, (from, to)| {
                let url = format!("{}{}", url, "/blockchain");
                client
                    .post(url.as_str())
                    .form(&[("minHeight", format!("{}", from)), ("maxHeight", format!("{}", to))])
                    .send()
                    .and_then(|mut resp| resp.json())
                    .map(move |response: serde_json::Value| {
                        response["result"]["block_metas"].as_array().unwrap().iter().fold(
                            total_tx,
                            move |num_tx: u64, bm: &serde_json::Value| {
                                num_tx
                                    + bm["header"]["num_txs"]
                                        .as_str()
                                        .unwrap()
                                        .parse::<u64>()
                                        .unwrap()
                            },
                        )
                    })
                    .map_err(|err| format!("{}", err))
            });
        Box::new(result)
    }
}
