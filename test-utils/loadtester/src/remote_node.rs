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
use near_primitives::views::AccessKeyView;
use reqwest::blocking::Client as SyncClient;
use std::convert::TryInto;
use testlib::user::rpc_user::RpcUser;
use testlib::user::User;

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
    pub addr: SocketAddr,
    pub signers: Vec<Arc<InMemorySigner>>,
    pub nonces: Vec<Nonce>,
    pub url: String,
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
    pub fn new(addr: SocketAddr, signers_accs: &[AccountId]) -> Arc<RwLock<Self>> {
        let url = format!("http://{}", addr);
        let signers: Vec<_> = signers_accs
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), KeyType::ED25519, s.as_str())))
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
        let mut result = Self { addr, signers, nonces, url, sync_client, async_client };

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
            .map(|(account_id, public_key)| {
                get_result(|| self.get_access_key(&account_id, &public_key)).nonce
            })
            .collect();
        self.nonces = nonces;
    }

    pub fn update_accounts(&mut self, signers_accs: &[AccountId]) {
        let signers: Vec<_> = signers_accs
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), KeyType::ED25519, s.as_str())))
            .collect();
        self.signers = signers;
        self.get_nonces(signers_accs);
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, Box<dyn std::error::Error>> {
        let user = RpcUser::new(
            &self.addr.to_string(),
            account_id.to_string(),
            self.signers.first().unwrap().clone(),
        );
        let access_key = user.get_access_key(account_id, public_key)?;
        info!("get access key of {}", account_id);
        Ok(access_key)
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
                        .ok_or_else(|| VALUE_NOT_STR_ERR.to_string()),
                )
            })
            .boxed()
    }

    /// Sends transactions using `broadcast_tx_sync` using blocking code. Return hash of
    /// the transaction.
    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let bytes = transaction.try_to_vec().unwrap();
        let params = (to_base64(&bytes),);
        let message = Message::request(
            "broadcast_tx_async".to_string(),
            Some(serde_json::to_value(&params).unwrap()),
        );
        let result: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;

        Ok(result["result"].as_str().ok_or(VALUE_NOT_STR_ERR)?.to_owned())
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
    pub fn transaction_committed(&self, hash: &String) -> Result<(), Box<dyn std::error::Error>> {
        let params = (hash,);
        let message =
            Message::request("tx".to_string(), Some(serde_json::to_value(&params).unwrap()));

        let response: serde_json::Value =
            self.sync_client.post(self.url.as_str()).json(&message).send()?.json()?;

        let status = response["result"]["status"].as_str().ok_or(VALUE_NOT_STR_ERR)?.to_owned();
        if status == "Completed" {
            debug!("txn completed: {}", hash);
            Ok(())
        } else {
            Err(format!("txn not completed: {} status: {}", hash, status).into())
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
        let response: serde_json::Value = self
            .sync_client
            .post(&format!("http://{}", &self.addr.to_string()))
            .json(&message)
            .send()?
            .json()?;

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
