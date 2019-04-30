use futures::Future;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use protobuf::Message;
use reqwest::r#async::Client as AsyncClient;
use reqwest::Client as SyncClient;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum number of blocks that can be fetched through a single RPC request.
pub const MAX_BLOCKS_FETCH: u64 = 20;
const VALUE_NOT_STR_ERR: &str = "Value is not str";
const VALUE_NOT_ARR_ERR: &str = "Value is not array";

pub struct RemoteNode {
    pub addr: SocketAddr,
    pub signers: Vec<Arc<InMemorySigner>>,
    pub nonces: Vec<u64>,
    pub url: String,
    async_client: Arc<AsyncClient>,
    sync_client: SyncClient,
}

impl RemoteNode {
    pub fn new(addr: SocketAddr, signers: &[String], init_nonce: u64) -> Arc<RwLock<Self>> {
        let url = format!("http://{}", addr);
        let signers: Vec<_> = signers
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), s.as_str())))
            .collect();
        let nonces = vec![init_nonce; signers.len()];
        let async_client = Arc::new(
            AsyncClient::builder()
                .use_rustls_tls()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .unwrap(),
        );

        let sync_client = SyncClient::builder()
            .use_rustls_tls()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .unwrap();
        Arc::new(RwLock::new(Self { addr, signers, nonces, url, async_client, sync_client }))
    }

    pub fn add_transaction_async(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let transaction: near_protos::signed_transaction::SignedTransaction = transaction.into();
        let tx_bytes = transaction.write_to_bytes().expect("write to bytes failed");
        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        let response = self
            .async_client
            .post(url.as_str())
            .form(&[("tx", format!("0x{}", hex::encode(&tx_bytes)))])
            .send()
            .map(|_| ())
            .map_err(|err| format!("{}", err));
        Box::new(response)
    }

    pub fn get_current_height(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        let response: serde_json::Value = self.sync_client.post(url.as_str()).send()?.json()?;
        Ok(response["result"]["sync_info"]["latest_block_height"]
            .as_str()
            .ok_or(VALUE_NOT_STR_ERR)?
            .parse()?)
    }

    pub fn get_transactions(
        &self,
        min_height: u64,
        max_height: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        assert!(max_height - min_height <= MAX_BLOCKS_FETCH, "Too many blocks to fetch");
        let url = format!("{}{}", self.url, "/blockchain");
        let response: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[
                ("minHeight", format!("{}", min_height)),
                ("maxHeight", format!("{}", max_height)),
            ])
            .send()?
            .json()?;
        let mut result = 0u64;
        for block_meta in response["result"]["block_metas"].as_array().ok_or(VALUE_NOT_ARR_ERR)? {
            result += block_meta["header"]["num_txs"]
                .as_str()
                .ok_or(VALUE_NOT_STR_ERR)?
                .parse::<u64>()?;
        }
        Ok(result)
    }
}
