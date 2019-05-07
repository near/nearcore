use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

use near_client::ClientConfig;
use near_network::NetworkConfig;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::types::{AccountId, Balance, ReadablePublicKey};

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000;
/// Stake used by authorities to validate used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000;

pub struct NearConfig {
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    pub rpc_server_addr: SocketAddr,
}

impl NearConfig {
    pub fn test(seed: &str, port: u16) -> Self {
        NearConfig {
            client_config: ClientConfig::new(),
            network_config: NetworkConfig::from_seed(seed, port),
            rpc_server_addr: format!("127.0.0.1:{}", port + 100).parse().unwrap(),
        }
    }
}

/// Runtime configuration, defining genesis block.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenesisConfig {
    /// Official time of blockchain start.
    pub genesis_time: DateTime<Utc>,
    /// ID of the blockchain. This must be unique for every blockchain.
    /// If your testnet blockchains do not have unique chain IDs, you will have a bad time.
    pub chain_id: String,
    /// Number of shards at genesis.
    pub num_shards: u32,
    /// List of initial authorities / validators.
    pub authorities: Vec<(AccountId, ReadablePublicKey, Balance)>,
    /// List of accounts / balances at genesis.
    pub accounts: Vec<(AccountId, ReadablePublicKey, Balance)>,
}

impl GenesisConfig {
    pub fn test(seeds: Vec<&str>) -> Self {
        let mut authorities = vec![];
        let mut accounts = vec![];
        let balance = TESTING_INIT_BALANCE;
        let stake = TESTING_INIT_STAKE;
        for account in seeds {
            let signer = InMemorySigner::from_seed(account, account);
            authorities.push((account.to_string(), signer.public_key.to_readable(), stake));
            accounts.push((account.to_string(), signer.public_key.to_readable(), balance));
        }
        GenesisConfig {
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_shards: 1,
            authorities,
            accounts,
        }
    }

    /// Reads GenesisConfig from a file.
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open genesis config file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from genesis config file.");
        GenesisConfig::from(content.as_str())
    }

    /// Writes GenesisConfig to the file.
    pub fn write_to_file(&self, path: &PathBuf) {
        let mut file = File::create(path).expect("Failed to create / write a genesis config file.");
        let str =
            serde_json::to_string_pretty(self).expect("Error serializing the genesis config.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a genesis config file {}", err);
        }
    }
}

impl From<&str> for GenesisConfig {
    fn from(config: &str) -> Self {
        serde_json::from_str(config).expect("Failed to deserialize the genesis config.")
    }
}

fn random_chain_id() -> String {
    format!("test-chain-{}", thread_rng().sample_iter(&Alphanumeric).take(5).collect::<String>())
}

/// Initializes genesis and client configs and stores in the given folder
pub fn init_config(dir: &Path, chain_id: Option<String>, account_id: Option<String>) {
    fs::create_dir_all(dir).expect("Failed to create directory");
    let chain_id = chain_id.unwrap_or(random_chain_id());
    match chain_id.as_ref() {
        "testnet" => {
            // TODO:
            unimplemented!();
        }
        "mainnet" => {
            // TODO:
            unimplemented!();
        }
        _ => {
            // Create new random genesis.
            let account_id = account_id.unwrap_or("test.near".to_string());
            let signer = InMemorySigner::from_random();
            let genesis_config = GenesisConfig {
                genesis_time: Utc::now(),
                chain_id,
                num_shards: 1,
                authorities: vec![(
                    account_id.clone(),
                    signer.public_key.to_readable(),
                    TESTING_INIT_STAKE,
                )],
                accounts: vec![(account_id, signer.public_key.to_readable(), TESTING_INIT_BALANCE)],
            };
            genesis_config.write_to_file(&dir.join("genesis.json"));
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use near_primitives::types::ReadablePublicKey;

    use super::GenesisConfig;

    #[test]
    fn test_deserialize() {
        let data = json!({
            "genesis_time": "2019-05-07T00:10:14.434719Z",
            "chain_id": "test-chain-XYQAS",
            "num_shards": 1,
            "accounts": [["alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100]],
            "authorities": [("alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 50)],
        });
        let spec = GenesisConfig::from(data.to_string().as_str());
        assert_eq!(
            spec.authorities[0],
            (
                "alice.near".to_string(),
                ReadablePublicKey("6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string()),
                50
            )
        );
    }
}
