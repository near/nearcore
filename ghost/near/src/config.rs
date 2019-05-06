use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

use near_client::{BlockProducer, ClientConfig};
use near_network::NetworkConfig;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::{AccountId, Balance, ReadablePublicKey};

pub struct NearConfig {
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    pub rpc_server_addr: SocketAddr,
}

impl NearConfig {
    pub fn new(genesis_timestamp: DateTime<Utc>, seed: &str, port: u16) -> Self {
        NearConfig {
            client_config: ClientConfig::new(genesis_timestamp),
            network_config: NetworkConfig::from_seed(seed, port),
            rpc_server_addr: format!("127.0.0.1:{}", port + 100).parse().unwrap(),
        }
    }
}

/// Runtime configuration, defining genesis block.
#[derive(Serialize, Deserialize, Clone)]
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
        let balance = 1_000_000_000;
        let stake = 50_000_000;
        for account in seeds {
            let signer = InMemorySigner::from_seed(account, account);
            authorities.push((account.to_string(), signer.public_key.to_readable(), stake));
            accounts.push((account.to_string(), signer.public_key.to_readable(), balance));
        }
        GenesisConfig {
            genesis_time: Utc::now(),
            chain_id: format!(
                "test-chain-{}",
                thread_rng().sample_iter(&Alphanumeric).take(5).collect::<String>()
            ),
            num_shards: 1,
            authorities,
            accounts,
        }
    }
}
