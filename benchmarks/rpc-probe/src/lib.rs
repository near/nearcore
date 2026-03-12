use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use near_crypto::{InMemorySigner, PublicKey, SecretKey, Signer};
use near_jsonrpc_client_internal::JsonRpcClient;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_primitives::action::{Action, TransferAction};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    SignedTransaction, Transaction, TransactionNonce, TransactionV1,
};
use near_primitives::types::{AccountId, Balance, BlockReference, Finality, Nonce};
use near_primitives::views::{AccessKeyView, QueryRequest, TxExecutionStatus};
use serde::{Deserialize, Serialize};

pub mod metrics;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    /// Path to a JSON array of `ProbeAccount` entries.
    pub accounts_path: PathBuf,
    /// Probe interval in seconds.
    pub interval_s: f64,
    /// RPC endpoint to probe.
    pub rpc_url: String,
    /// Delay in seconds before starting probes.
    pub startup_delay_s: u64,
    /// Enable write probes (native transfer transactions).
    pub write_probes_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            accounts_path: PathBuf::new(),
            interval_s: 5.0,
            rpc_url: "http://localhost:3030".to_string(),
            startup_delay_s: 120,
            write_probes_enabled: true,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
struct ProbeAccount {
    account_id: AccountId,
    public_key: String,
    secret_key: String,
}

struct WriteProbeAccount {
    account_id: AccountId,
    public_key: PublicKey,
    signer: Signer,
    nonce: AtomicU64,
}

impl WriteProbeAccount {
    fn from_probe_account(pa: &ProbeAccount) -> anyhow::Result<Self> {
        let secret_key: SecretKey = pa.secret_key.parse()?;
        let public_key: PublicKey = pa.public_key.parse()?;
        let signer = InMemorySigner::from_secret_key(pa.account_id.clone(), secret_key);
        Ok(Self { account_id: pa.account_id.clone(), public_key, signer, nonce: AtomicU64::new(0) })
    }

    fn next_nonce(&self) -> Nonce {
        self.nonce.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn set_nonce(&self, nonce: Nonce) {
        self.nonce.store(nonce, Ordering::SeqCst);
    }
}

/// Fetch the current nonce for an access key via `query` RPC (view_access_key).
async fn fetch_nonce(
    client: &JsonRpcClient,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Option<Nonce> {
    let response = client
        .query(RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::None),
            request: QueryRequest::ViewAccessKey {
                account_id: account_id.clone(),
                public_key: public_key.clone(),
            },
        })
        .await
        .ok()?;
    match response.kind {
        QueryResponseKind::AccessKey(AccessKeyView { nonce, .. }) => Some(nonce),
        _ => None,
    }
}

/// Initialize nonces for write probe accounts from the chain.
/// Retries up to 3 times (5s apart). Without RPC sharding, nodes only serve
/// queries for tracked shards, so accounts on untracked shards are expected to
/// fail and are dropped.
async fn init_nonces(
    client: &JsonRpcClient,
    accounts: &[Arc<WriteProbeAccount>],
) -> Vec<Arc<WriteProbeAccount>> {
    let mut pending: Vec<usize> = (0..accounts.len()).collect();
    for attempt in 1..=3 {
        let mut still_pending = Vec::new();
        for &idx in &pending {
            let account = &accounts[idx];
            match fetch_nonce(client, &account.account_id, &account.public_key).await {
                Some(nonce) => {
                    account.set_nonce(nonce);
                    tracing::info!(target: "rpc-probe",
                        account_id = %account.account_id, nonce,
                        "initialized write probe nonce");
                }
                None => {
                    tracing::warn!(target: "rpc-probe",
                        account_id = %account.account_id, attempt,
                        "failed to fetch nonce");
                    still_pending.push(idx);
                }
            }
        }
        pending = still_pending;
        if pending.is_empty() {
            break;
        }
        tracing::warn!(target: "rpc-probe",
            remaining = pending.len(), attempt,
            "retrying nonce initialization in 5s");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    for &idx in &pending {
        tracing::warn!(target: "rpc-probe",
            account_id = %accounts[idx].account_id,
            "dropping write probe account (likely on untracked shard)");
    }
    let failed: std::collections::HashSet<usize> = pending.into_iter().collect();
    accounts
        .iter()
        .enumerate()
        .filter(|(i, _)| !failed.contains(i))
        .map(|(_, a)| a.clone())
        .collect()
}

const TRANSFER_AMOUNT: Balance = Balance::from_yoctonear(1);

/// Build and sign a SendMoney transaction.
fn build_send_money(
    nonce: Nonce,
    signer_id: AccountId,
    receiver_id: AccountId,
    signer: &Signer,
    deposit: Balance,
    block_hash: CryptoHash,
) -> SignedTransaction {
    let tx = Transaction::V1(TransactionV1 {
        signer_id,
        public_key: signer.public_key(),
        nonce: TransactionNonce::from_nonce(nonce),
        receiver_id,
        block_hash,
        actions: vec![Action::Transfer(TransferAction { deposit })],
    });
    let (hash, _size) = tx.get_hash_and_size();
    let signature = signer.sign(hash.as_ref());
    SignedTransaction::new(signature, tx)
}

/// Fire-and-forget write probe: send a native transfer and measure e2e latency.
/// The `block_hash` is provided from the cached view_account response (no extra RPC call).
async fn probe_native_transfer(
    client: Arc<JsonRpcClient>,
    sender: Arc<WriteProbeAccount>,
    receiver_id: AccountId,
    block_hash: CryptoHash,
) {
    let nonce = sender.next_nonce();
    let signed_tx = build_send_money(
        nonce,
        sender.account_id.clone(),
        receiver_id,
        &sender.signer,
        TRANSFER_AMOUNT,
        block_hash,
    );

    let start = std::time::Instant::now();
    let result = client.send_tx(signed_tx, TxExecutionStatus::ExecutedOptimistic).await;
    record_probe("native_transfer", start, &result);
}

/// Start the RPC probe loop as a background tokio task.
pub fn start(config: Config) {
    if config.interval_s <= 0.0 {
        tracing::error!(target: "rpc-probe", "interval_s must be > 0, probe disabled");
        return;
    }

    let accounts = load_accounts(&config.accounts_path);
    if accounts.is_empty() {
        tracing::warn!(target: "rpc-probe", "no probe accounts found, only gas_price probes will run");
    }

    // Parse write probe accounts if enabled.
    let write_accounts = if config.write_probes_enabled && !accounts.is_empty() {
        let parsed: Vec<Arc<WriteProbeAccount>> = accounts
            .iter()
            .filter_map(|pa| {
                WriteProbeAccount::from_probe_account(pa)
                    .map_err(|err| {
                        tracing::error!(target: "rpc-probe",
                            account_id = %pa.account_id, ?err,
                            "failed to parse write probe account");
                    })
                    .ok()
                    .map(Arc::new)
            })
            .collect();
        if parsed.is_empty() {
            tracing::warn!(target: "rpc-probe",
                "no valid write probe accounts, write probes disabled");
            None
        } else {
            Some(parsed)
        }
    } else {
        None
    };

    tokio::spawn(async move {
        probe_loop(config, accounts, write_accounts).await;
    });
}

fn load_accounts(path: &Path) -> Vec<ProbeAccount> {
    if path.as_os_str().is_empty() {
        return vec![];
    }
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(target: "rpc-probe", ?err, ?path, "failed to read probe accounts file");
            return vec![];
        }
    };
    match serde_json::from_str(&content) {
        Ok(accounts) => {
            let accounts: Vec<ProbeAccount> = accounts;
            tracing::info!(target: "rpc-probe", num_accounts = accounts.len(), "loaded probe accounts");
            accounts
        }
        Err(err) => {
            tracing::error!(target: "rpc-probe", ?err, ?path, "failed to parse probe accounts file");
            vec![]
        }
    }
}

async fn probe_loop(
    config: Config,
    accounts: Vec<ProbeAccount>,
    write_accounts: Option<Vec<Arc<WriteProbeAccount>>>,
) {
    let client = Arc::new(near_jsonrpc_client_internal::new_client(&config.rpc_url));

    let interval = Duration::from_secs_f64(config.interval_s);
    let mut account_idx: usize = 0;
    let mut write_idx: usize = 0;

    tracing::info!(target: "rpc-probe",
        rpc_url = %config.rpc_url,
        interval_s = config.interval_s,
        startup_delay_s = config.startup_delay_s,
        num_accounts = accounts.len(),
        write_probes = write_accounts.is_some(),
        "probe loop started");

    if config.startup_delay_s > 0 {
        tokio::time::sleep(Duration::from_secs(config.startup_delay_s)).await;
    }

    // Initialize write probe nonces after startup delay. Without RPC sharding,
    // accounts on untracked shards are expected to fail and are dropped.
    let write_accounts = match write_accounts {
        Some(wa) => {
            let active = init_nonces(&client, &wa).await;
            if active.is_empty() { None } else { Some(active) }
        }
        None => None,
    };

    // Block hash cache: populated by view_account probes, consumed by write probes.
    // This avoids an extra status() RPC call per write probe.
    let latest_block_hash: Arc<Mutex<Option<CryptoHash>>> = Arc::new(Mutex::new(None));

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        // Spawn each probe as a fire-and-forget task so slow probes don't
        // block the ticker or delay each other.
        let gas_client = client.clone();
        tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = gas_client.gas_price(None).await;
            record_probe("gas_price", start, &result);
        });

        if !accounts.is_empty() {
            let account_id = accounts[account_idx % accounts.len()].account_id.clone();
            let view_client = client.clone();
            let hash_cache = latest_block_hash.clone();
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = view_client
                    .query(RpcQueryRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        request: QueryRequest::ViewAccount { account_id },
                    })
                    .await;
                if let Ok(ref resp) = result {
                    *hash_cache.lock() = Some(resp.block_hash);
                }
                record_probe("view_account", start, &result);
            });
            account_idx = account_idx.wrapping_add(1);
        }

        // Write probe: native transfer (fire-and-forget, same pattern as reads).
        // Uses block hash from the most recent view_account response.
        if let Some(ref wa) = write_accounts {
            if !wa.is_empty() {
                if let Some(block_hash) = *latest_block_hash.lock() {
                    let sender = wa[write_idx % wa.len()].clone();
                    let receiver_id = wa[(write_idx + 1) % wa.len()].account_id.clone();
                    let write_client = client.clone();
                    tokio::spawn(async move {
                        probe_native_transfer(write_client, sender, receiver_id, block_hash).await;
                    });
                    write_idx = write_idx.wrapping_add(1);
                }
            }
        }
    }
}

/// Record probe latency and success/error metrics.
fn record_probe<T>(method: &str, start: std::time::Instant, result: &Result<T, RpcError>) {
    let elapsed = start.elapsed();
    match result {
        Ok(_) => {
            tracing::debug!(target: "rpc-probe", method, latency_ms = elapsed.as_millis(), "probe ok");
            metrics::RPC_PROBE_LATENCY.with_label_values(&[method]).observe(elapsed.as_secs_f64());
            metrics::RPC_PROBE_SUCCESS_TOTAL.with_label_values(&[method]).inc();
        }
        Err(err) => {
            tracing::debug!(target: "rpc-probe", method, %err, "probe error");
            metrics::RPC_PROBE_ERROR_TOTAL.with_label_values(&[method]).inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_full() {
        let config: Config = serde_json::from_str(
            r#"{"accounts_path": "/tmp/accounts.json", "interval_s": 10, "rpc_url": "http://localhost:4040", "startup_delay_s": 30, "write_probes_enabled": false}"#,
        )
        .unwrap();
        assert_eq!(config.interval_s, 10.0);
        assert_eq!(config.rpc_url, "http://localhost:4040");
        assert_eq!(config.startup_delay_s, 30);
        assert!(!config.write_probes_enabled);
    }
}
