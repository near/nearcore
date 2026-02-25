use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub mod metrics;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    /// Path to a JSON array of `ProbeAccount` entries.
    pub accounts_path: std::path::PathBuf,
    /// Probe interval in seconds.
    pub interval_s: u64,
    /// RPC endpoint to probe.
    pub rpc_url: String,
    /// Delay in seconds before starting probes.
    pub startup_delay_s: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            accounts_path: std::path::PathBuf::new(),
            interval_s: 5,
            rpc_url: "http://localhost:3030".to_string(),
            startup_delay_s: 120,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
struct ProbeAccount {
    account_id: String,
    /// Used by write probes to query nonce via view_access_key.
    #[allow(dead_code)]
    public_key: String,
    /// Used by write probes to sign transactions.
    #[allow(dead_code)]
    secret_key: String,
}

/// Start the RPC probe loop as a background tokio task.
pub fn start(config: Config) {
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
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build HTTP client");

    let interval = Duration::from_secs(config.interval_s);
    let rpc_url = config.rpc_url.clone();
    let mut account_idx: usize = 0;

    tracing::info!(target: "rpc-probe",
        rpc_url = %rpc_url,
        interval_s = config.interval_s,
        startup_delay_s = config.startup_delay_s,
        num_accounts = accounts.len(),
        "probe loop started");

    if config.startup_delay_s > 0 {
        tokio::time::sleep(Duration::from_secs(config.startup_delay_s)).await;
    }

    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;

        let gas_price_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "rpc-probe",
            "method": "gas_price",
            "params": [null]
        });
        probe_rpc(&client, &rpc_url, "gas_price", gas_price_body).await;

        if !accounts.is_empty() {
            let account = &accounts[account_idx % accounts.len()];
            let view_account_body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "rpc-probe",
                "method": "query",
                "params": {
                    "request_type": "view_account",
                    "finality": "optimistic",
                    "account_id": account.account_id
                }
            });
            probe_rpc(&client, &rpc_url, "view_account", view_account_body).await;
            account_idx = account_idx.wrapping_add(1);
        }
    }
}

/// Send a JSON-RPC request, measure latency, and record metrics.
async fn probe_rpc(client: &reqwest::Client, rpc_url: &str, method: &str, body: serde_json::Value) {
    let start = std::time::Instant::now();
    let result = client.post(rpc_url).json(&body).send().await;
    let elapsed = start.elapsed();

    let ok = match result {
        Ok(resp) if resp.status().is_success() => match resp.json::<serde_json::Value>().await {
            Ok(json) if json.get("error").is_some() => {
                tracing::debug!(target: "rpc-probe",
                        method, error = %json["error"], "probe returned rpc error");
                false
            }
            Ok(_) => {
                tracing::debug!(target: "rpc-probe",
                        method, latency_ms = elapsed.as_millis(), "probe ok");
                true
            }
            Err(err) => {
                tracing::warn!(target: "rpc-probe",
                        method, ?err, "failed to parse response");
                false
            }
        },
        Ok(resp) => {
            tracing::warn!(target: "rpc-probe",
                method, status = %resp.status(), "probe non-success status");
            false
        }
        Err(err) => {
            tracing::warn!(target: "rpc-probe", method, ?err, "probe failed");
            false
        }
    };

    if ok {
        metrics::RPC_PROBE_LATENCY.with_label_values(&[method]).observe(elapsed.as_secs_f64());
        metrics::RPC_PROBE_SUCCESS_TOTAL.with_label_values(&[method]).inc();
    } else {
        metrics::RPC_PROBE_ERROR_TOTAL.with_label_values(&[method]).inc();
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
        assert_eq!(config.interval_s, 10);
        assert_eq!(config.rpc_url, "http://localhost:4040");
        assert_eq!(config.startup_delay_s, 30);
    }
}
