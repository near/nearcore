use futures::Future;
use node_runtime::state_viewer::AccountViewCallResult;
use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::SignedTransaction;
use primitives::types::{AccountId, Nonce};
use protobuf::Message;
use reqwest::r#async::Client as AsyncClient;
use reqwest::Client as SyncClient;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum number of blocks that can be fetched through a single RPC request.
pub const MAX_BLOCKS_FETCH: u64 = 20;
const VALUE_NOT_STR_ERR: &str = "Value is not str";
const VALUE_NOT_ARR_ERR: &str = "Value is not array";

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
    async_client: Arc<AsyncClient>,
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
            .map(|s| Arc::new(InMemorySigner::from_seed(s.as_str(), s.as_str())))
            .collect();
        let nonces = vec![0; signers.len()];
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
        let mut result = Self { addr, signers, nonces, url, async_client, sync_client };

        // Wait for the node to be up.
        wait(|| result.health_ok());

        // Collect nonces.
        result.get_nonces(signers_accs);
        Arc::new(RwLock::new(result))
    }

    /// Get nonces for the given list of signers.
    fn get_nonces(&mut self, signers: &[AccountId]) {
        let nonces: Vec<Nonce> =
            signers.iter().map(|s| get_result(|| self.view_account(s)).nonce).collect();
        self.nonces = nonces;
    }

    fn health_ok(&self) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/status");
        Ok(self.sync_client.post(url.as_str()).send().map(|_| ())?)
    }

    fn view_account(
        &self,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.url, "/abci_query");
        let response: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[("path", format!("\"account/{}\"", account_id))])
            .send()?
            .json()?;
        let bytes = base64::decode(
            response["result"]["response"]["value"].as_str().ok_or(VALUE_NOT_STR_ERR)?,
        )?;
        let s = std::str::from_utf8(&bytes)?;
        Ok(serde_json::from_str(s)?)
    }

    /// Sends transaction using `broadcast_tx_sync` using non-blocking Futures.
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

    /// Sends transactions using `broadcast_tx_sync` using blocking code. Return hash of
    /// the transaction.
    pub fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let transaction: near_protos::signed_transaction::SignedTransaction = transaction.into();
        let tx_bytes = transaction.write_to_bytes().expect("write to bytes failed");
        let url = format!("{}{}", self.url, "/broadcast_tx_sync");
        let result: serde_json::Value = self
            .sync_client
            .post(url.as_str())
            .form(&[("tx", format!("0x{}", hex::encode(&tx_bytes)))])
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
        let url = format!("{}{}", self.url, "/status");
        let response: serde_json::Value = self.sync_client.post(url.as_str()).send()?.json()?;
        Ok(response["result"]["sync_info"]["latest_block_height"]
            .as_str()
            .ok_or(VALUE_NOT_STR_ERR)?
            .parse()?)
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
