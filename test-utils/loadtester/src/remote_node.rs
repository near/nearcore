use std::format;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use borsh::BorshSerialize;
use futures::Future;
use reqwest::Client as SyncClient;

use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_jsonrpc::client::new_client;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Nonce};
use near_primitives::views::AccessKeyView;
use std::convert::TryInto;
use testlib::user::rpc_user::RpcUser;
use testlib::user::User;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum number of blocks that can be fetched through a single RPC request.
pub const MAX_BLOCKS_FETCH: u64 = 20;
const VALUE_NOT_STR_ERR: &str = "Value is not str";
const VALUE_NOT_ARR_ERR: &str = "Value is not array";
const VALUE_NOT_NUM_ERR: &str = "Value is not number";

/// Maximum number of times we retry a single RPC.
const MAX_RETRIES_PER_RPC: usize = 1;
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
                println!("{:?}", err);
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
        println!("{:?}", signers_accs);
        let signers: Vec<_> = signers_accs
            .iter()
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), KeyType::ED25519, s.as_str())))
            .collect();
        println!("{:?}", signers.len());
        let nonces = vec![0; signers.len()];

        let sync_client = SyncClient::builder()
            .use_rustls_tls()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .unwrap();
        let mut result = Self { addr, signers, nonces, url, sync_client };

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
                get_result(|| self.get_access_key(&account_id, &public_key))
                    .expect("No access key for given public key")
                    .nonce
            })
            .collect();
        self.nonces = nonces;
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKeyView>, Box<dyn std::error::Error>> {
        let user = RpcUser::new(&self.addr.to_string(), self.signers.first().unwrap().clone());
        let access_key = user.get_access_key(account_id, public_key)?;
        Ok(access_key)
    }

    fn health_ok(&self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        Ok(self.sync_client.post(url.as_str()).send().map(|_| ())?)
    }

    /// Sends transaction using `broadcast_tx_sync` using non-blocking Futures.
    pub fn add_transaction_async(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String>> {
        let mut client = new_client(&self.url);
        let bytes = transaction.try_to_vec().unwrap();
        let response = client
            .broadcast_tx_async(to_base64(&bytes))
            .map(|_| ())
            .map_err(|err| format!("{}", err));

        //        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        //        println!("to url {}", &url);
        //        let response = self
        //            .async_client
        //            .post(url.as_str())
        //            .form(&[("tx", format!("0x{}", hex::encode(&bytes)))])
        //            .send()
        //            .map(|_| ())
        //            .map_err(|err| format!("{}", err));
        //        println!("Get response");
        Box::new(response)
    }

    /// Sends transactions using `broadcast_tx_sync` using blocking code. Return hash of
    /// the transaction.
    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let bytes = transaction.try_to_vec().unwrap();
        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        let result: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[("tx", format!("0x{}", hex::encode(&bytes)))])
            .send()?
            .json()?;
        Ok(result["result"]["hash"].as_str().ok_or(VALUE_NOT_STR_ERR)?.to_owned())
    }

    /// Returns block height if transaction is committed to a block.
    pub fn transaction_committed(&self, hash: &String) -> Result<u64, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/tx");
        let response: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[("hash", format!("0x{}", hash))])
            .send()?
            .json()?;
        Ok(response["result"]["height"].as_str().ok_or(VALUE_NOT_STR_ERR)?.parse()?)
    }

    pub fn get_current_height(&self) -> Result<u64, Box<dyn std::error::Error>> {
        println!("aaa");
        let url = format!("{}{}", self.url, "/status");
        println!("{}", url);
        let response: serde_json::Value = self.sync_client.get(url.as_str()).send()?.json()?;
        println!("bbb");
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

    // This does not work because Tendermint RPC returns garbage: https://pastebin.com/RUbEdqt6
    pub fn block_result_codes(
        &self,
        height: u64,
    ) -> Result<Vec<(u32, String)>, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/block_results");
        let response: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[("height", format!("{}", height))])
            .send()?
            .json()?;

        let mut results = vec![];
        for result in response["result"]["results"].as_array().ok_or(VALUE_NOT_ARR_ERR)? {
            results.push((
                result["code"].as_str().ok_or(VALUE_NOT_STR_ERR)?.parse::<u32>()?,
                result["data"].as_str().ok_or(VALUE_NOT_STR_ERR)?.to_owned(),
            ));
        }
        Ok(results)
    }

    pub fn get_transactions(
        &self,
        min_height: u64,
        max_height: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        println!("max_height {} min_height {}", max_height, min_height);
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
