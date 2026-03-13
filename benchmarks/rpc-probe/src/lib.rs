use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::types::query::RpcQueryRequest;
use near_primitives::types::{AccountId, BlockReference, Finality};
use near_primitives::views::QueryRequest;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            accounts_path: PathBuf::new(),
            interval_s: 5.0,
            rpc_url: "http://localhost:3030".to_string(),
            startup_delay_s: 120,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
struct ProbeAccount {
    account_id: AccountId,
    /// Used by write probes to query nonce via view_access_key.
    #[allow(dead_code)]
    public_key: String,
    /// Used by write probes to sign transactions.
    #[allow(dead_code)]
    secret_key: String,
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

    tokio::spawn(async move {
        probe_loop(config, accounts).await;
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

async fn probe_loop(config: Config, accounts: Vec<ProbeAccount>) {
    let client = Arc::new(near_jsonrpc_client_internal::new_client(&config.rpc_url));

    let interval = Duration::from_secs_f64(config.interval_s);
    let mut account_idx: usize = 0;

    tracing::info!(target: "rpc-probe",
        rpc_url = %config.rpc_url,
        interval_s = config.interval_s,
        startup_delay_s = config.startup_delay_s,
        num_accounts = accounts.len(),
        "probe loop started");

    if config.startup_delay_s > 0 {
        tokio::time::sleep(Duration::from_secs(config.startup_delay_s)).await;
    }

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
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                let result = view_client
                    .query(RpcQueryRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        request: QueryRequest::ViewAccount { account_id },
                    })
                    .await;
                record_probe("view_account", start, &result);
            });
            account_idx = account_idx.wrapping_add(1);
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
            r#"{"accounts_path": "/tmp/accounts.json", "interval_s": 10, "rpc_url": "http://localhost:4040", "startup_delay_s": 30}"#,
        )
        .unwrap();
        assert_eq!(config.interval_s, 10.0);
        assert_eq!(config.rpc_url, "http://localhost:4040");
        assert_eq!(config.startup_delay_s, 30);
    }
}
