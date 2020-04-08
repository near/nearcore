use reqwest::Client as AsyncClient;
use std::format;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use borsh::BorshSerialize;
use futures::{future, future::BoxFuture, FutureExt, TryFutureExt};
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_jsonrpc::client::message::Message;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Nonce};
use reqwest::blocking::Client as SyncClient;
use std::convert::TryInto;

use log::{debug, info};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum number of blocks that can be fetched through a single RPC request.
pub const MAX_BLOCKS_FETCH: u64 = 1;
const VALUE_NOT_STR_ERR: &str = "Value is not str";
const VALUE_NOT_ARR_ERR: &str = "Value is not array";
const VALUE_NOT_NUM_ERR: &str = "Value is not number";

/// Maximum number of times we retry a single RPC.
const MAX_RETRIES_PER_RPC: usize = 10;
const MAX_RETRIES_REACHED_ERR: &str = "Exceeded maximum number of retries per RPC";

/// Maximum time we wait for the given RPC.
const MAX_WAIT_RPC: Duration = Duration::from_secs(60);
const MAX_WAIT_REACHED_ERR: &str = "Exceeded maximum wait on RPC";

pub struct RemoteNode {
    pub signers: Vec<Arc<InMemorySigner>>,
    pub nonces: Vec<Nonce>,
    pub url: String,
    pub addr: String,
    sync_client: SyncClient,
    async_client: Arc<AsyncClient>,
}

pub fn wait<F, T>(mut f: F) -> T
where
    F: FnMut() -> Result<T, Box<dyn std::error::Error>>,
{
    let started = Instant::now();
    loop {
        match f() {
            Ok(r) => return r,
            Err(err) => {
                if Instant::now().duration_since(started) > MAX_WAIT_RPC {
                    panic!("{}: {}", MAX_WAIT_REACHED_ERR, err);
                } else {
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}

pub fn try_wait<F, T>(mut f: F) -> Result<T, Box<dyn std::error::Error>>
where
    F: FnMut() -> Result<T, Box<dyn std::error::Error>>,
{
    let started = Instant::now();
    loop {
        match f() {
            Ok(r) => return Ok(r),
            Err(err) => {
                if Instant::now().duration_since(started) > MAX_WAIT_RPC {
                    return Err(err);
                } else {
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
}

pub fn get_result<F, T>(f: F) -> T
where
    F: Fn() -> Result<T, Box<dyn std::error::Error>>,
{
    let mut curr = 0;
    loop {
        match f() {
            Ok(r) => return r,
            Err(err) => {
                if curr == MAX_RETRIES_PER_RPC - 1 {
                    panic!("{}: {}", MAX_RETRIES_REACHED_ERR, err);
                } else {
                    curr += 1;
                }
            }
        };
    }
}

impl RemoteNode {
    pub fn new(addr: &str, signers_accs: &[AccountId]) -> Arc<RwLock<Self>> {
        let addr = addr.to_string();
        let url = addr.clone();
        let signers: Vec<_> = signers_accs
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), KeyType::ED25519, "near")))
            .collect();
        let nonces = vec![0; signers.len()];

        let sync_client = SyncClient::builder()
            .use_rustls_tls()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .unwrap();

        let async_client = Arc::new(
            AsyncClient::builder()
                .use_rustls_tls()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .unwrap(),
        );
        let mut result = Self { signers, nonces, addr, url, sync_client, async_client };

        // Wait for the node to be up.
        wait(|| result.health_ok());

        // Collect nonces.
        result.get_nonces(signers_accs);
        Arc::new(RwLock::new(result))
    }

    /// Get nonces for the given list of signers.
    fn get_nonces(&mut self, accounts: &[AccountId]) {
        let nonces: Vec<Nonce> = accounts
            .iter()
            .zip(self.signers.iter().map(|s| &s.public_key))
            .map(|(account_id, public_key)| get_result(|| self.get_nonce(&account_id, &public_key)))
            .collect();
        self.nonces = nonces;
    }

    pub fn update_accounts(&mut self, signers_accs: &[AccountId]) {
        let signers: Vec<_> = signers_accs
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), KeyType::ED25519, "near")))
            .collect();
        self.signers = signers;
        self.get_nonces(signers_accs);
    }

    fn get_nonce(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Nonce, Box<dyn std::error::Error>> {
        let params = (format!("access_key/{}/{}", account_id, public_key), "".to_string());
        let message =
            Message::request("query".to_string(), Some(serde_json::to_value(params).unwrap()));
        let result: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;
        let nonce = result["result"]["nonce"].as_u64().unwrap();
        debug!("get nonce of {}, nonce {}", account_id, nonce);
        Ok(nonce)
    }

    fn health_ok(&self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        Ok(self.sync_client.get(url.as_str()).send().map(|_| ())?)
    }

    /// Sends transaction using `broadcast_tx_sync` using non-blocking Futures.
    pub fn add_transaction_async(
        &self,
        transaction: SignedTransaction,
    ) -> BoxFuture<'static, Result<String, String>> {
        let bytes = transaction.try_to_vec().unwrap();
        let params = (to_base64(&bytes),);
        let message = Message::request(
            "broadcast_tx_async".to_string(),
            Some(serde_json::to_value(&params).unwrap()),
        );
        self.async_client
            .post(self.url.as_str())
            .json(&message)
            .send()
            .and_then(|r| r.json::<serde_json::Value>())
            .map_err(|err| format!("{}", err))
            .and_then(|j| {
                future::ready(
                    j["result"]
                        .as_str()
                        .map(|s| s.to_string())
                        .ok_or(VALUE_NOT_STR_ERR.to_string()),
                )
            })
            .boxed()
    }

    /// Sends transactions using `broadcast_tx_sync` using blocking code. Return hash of
    /// the transaction.
    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        let bytes = transaction.try_to_vec().unwrap();
        let params = (to_base64(&bytes),);
        let message = Message::request(
            "broadcast_tx_async".to_string(),
            Some(serde_json::to_value(&params).unwrap()),
        );
        let result: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;
        let hash = result["result"].as_str().ok_or(VALUE_NOT_STR_ERR)?.to_owned();
        let account_id = transaction.transaction.signer_id.to_string();
        Ok((hash, account_id))
    }

    pub fn add_transaction_committed(
        &self,
        transaction: SignedTransaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bytes = transaction.try_to_vec().unwrap();
        let params = (to_base64(&bytes),);
        let message = Message::request(
            "broadcast_tx_commit".to_string(),
            Some(serde_json::to_value(&params).unwrap()),
        );
        let result: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;
        info!("{:?}", result);
        Ok(())
    }

    /// Returns () if transaction is completed
    pub fn transaction_committed(
        &self,
        hash: &String,
        account_id: &String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params = (hash, account_id);
        let message =
            Message::request("tx".to_string(), Some(serde_json::to_value(&params).unwrap()));

        let response: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;
        let status = response["result"]["status"].clone();
        if status.get("SuccessValue").is_some() {
            debug!("txn {} success", hash);
            Ok(())
        } else if let Some(err) = status.get("Failure") {
            debug!("txn {} failure: {}", hash, err);
            Ok(())
        } else {
            debug!("txn pending: {}", hash);
            Err(format!("txn not completed: {}", hash).into())
        }
    }

    pub fn get_current_height(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        let response: serde_json::Value = self.sync_client.get(url.as_str()).send()?.json()?;
        Ok(response["sync_info"]["latest_block_height"].as_u64().ok_or(VALUE_NOT_NUM_ERR)?)
    }

    pub fn get_current_block_hash(&self) -> Result<CryptoHash, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        let response: serde_json::Value = self.sync_client.get(url.as_str()).send()?.json()?;
        Ok(response["sync_info"]["latest_block_hash"]
            .as_str()
            .ok_or(VALUE_NOT_STR_ERR)?
            .to_string()
            .try_into()?)
    }

    pub fn get_transactions(&self, height: u64) -> Result<u64, Box<dyn std::error::Error>> {
        let params = (height,);
        let message =
            Message::request("block".to_string(), Some(serde_json::to_value(&params).unwrap()));
        let response: serde_json::Value =
            self.sync_client.post(&format!("http://{}", self.url)).json(&message).send()?.json()?;

        Ok(response["result"]["transactions"].as_array().ok_or(VALUE_NOT_ARR_ERR)?.len() as u64)
    }

    pub fn peer_node_addrs(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        let response: serde_json::Value = self.sync_client.get(url.as_str()).send()?.json()?;
        Ok(response["active_peers"]
            .as_array()
            .ok_or(VALUE_NOT_ARR_ERR)?
            .into_iter()
            .map(|active_peer| {
                let mut socket_addr =
                    active_peer["addr"].as_str().unwrap().parse::<SocketAddr>().unwrap();
                socket_addr.set_port(3030);
                socket_addr.to_string()
            })
            .collect())
    }
}
