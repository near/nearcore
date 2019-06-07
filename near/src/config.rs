use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{cmp, fs};

use chrono::{DateTime, NaiveDateTime, Utc};
use log::info;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

use near_client::BlockProducer;
use near_client::ClientConfig;
use near_jsonrpc::RpcConfig;
use near_network::test_utils::open_port;
use near_network::NetworkConfig;
use near_primitives::crypto::signer::{EDSigner, InMemorySigner, KeyFile};
use near_primitives::serialize::{to_base, u128_hex_format};
use near_primitives::types::{AccountId, Balance, BlockIndex, ReadablePublicKey, ValidatorId};

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000_000;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000;

/// One NEAR in atto-NEARs.
pub const NEAR_TOKEN: Balance = 1_000_000_000_000_000_000;

/// Initial token supply.
pub const INITIAL_TOKEN_SUPPLY: Balance = 1_000_000_000 * NEAR_TOKEN;

pub const CONFIG_FILENAME: &str = "config.json";
pub const GENESIS_CONFIG_FILENAME: &str = "genesis.json";
pub const NODE_KEY_FILE: &str = "node_key.json";
pub const VALIDATOR_KEY_FILE: &str = "validator_key.json";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Network {
    /// Address to listen for incoming connections.
    pub addr: String,
    /// Address to advertise to peers for them to connect.
    /// If empty, will use the same port as the addr, and will introspect on the listener.
    pub external_address: String,
    /// Comma separated list of nodes to connect to.
    pub boot_nodes: String,
    /// Maximum number of peers.
    pub max_peers: u32,
    /// Handshake timeout.
    pub handshake_timeout: Duration,
    /// Duration before trying to reconnect to a peer.
    pub reconnect_delay: Duration,
    /// Skip waiting for peers before starting node.
    pub skip_sync_wait: bool,
    /// Ban window for peers who misbehave.
    pub ban_window: Duration,
}

impl Default for Network {
    fn default() -> Self {
        Network {
            addr: "0.0.0.0:24567".to_string(),
            external_address: "".to_string(),
            boot_nodes: "".to_string(),
            max_peers: 40,
            handshake_timeout: Duration::from_secs(20),
            reconnect_delay: Duration::from_secs(60),
            skip_sync_wait: false,
            ban_window: Duration::from_secs(3 * 60 * 60),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Consensus {
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum duration before producing block or skipping height.
    pub max_block_production_delay: Duration,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            min_block_production_delay: Duration::from_secs(1),
            max_block_production_delay: Duration::from_secs(6),
            produce_empty_blocks: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub genesis_file: String,
    pub validator_key_file: String,
    pub node_key_file: String,
    pub rpc: RpcConfig,
    pub network: Network,
    pub consensus: Consensus,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            genesis_file: GENESIS_CONFIG_FILENAME.to_string(),
            validator_key_file: VALIDATOR_KEY_FILE.to_string(),
            node_key_file: NODE_KEY_FILE.to_string(),
            rpc: RpcConfig::default(),
            network: Network::default(),
            consensus: Consensus::default(),
        }
    }
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open config file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from config file.");
        Config::from(content.as_str())
    }

    pub fn write_to_file(&self, path: &PathBuf) {
        let mut file = File::create(path).expect("Failed to create / write a config file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the config.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a config file {}", err);
        }
    }
}

impl From<&str> for Config {
    fn from(content: &str) -> Self {
        serde_json::from_str(content).expect("Failed to deserialize config")
    }
}

#[derive(Clone)]
pub struct NearConfig {
    config: Config,
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    pub rpc_config: RpcConfig,
    pub block_producer: Option<BlockProducer>,
    pub genesis_config: GenesisConfig,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis_config: &GenesisConfig,
        network_key_pair: KeyFile,
        block_producer: Option<&BlockProducer>,
    ) -> Self {
        NearConfig {
            config: config.clone(),
            client_config: ClientConfig {
                chain_id: genesis_config.chain_id.clone(),
                rpc_addr: config.rpc.addr.clone(),
                min_block_production_delay: Duration::from_millis(100),
                max_block_production_delay: Duration::from_millis(2000),
                block_expected_weight: 1000,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: Duration::from_secs(10),
                sync_step_period: Duration::from_millis(10),
                sync_weight_threshold: 0,
                sync_height_threshold: 1,
                min_num_peers: 1,
                fetch_info_period: Duration::from_millis(100),
                log_summary_period: Duration::from_secs(10),
                produce_empty_blocks: config.consensus.produce_empty_blocks,
            },
            network_config: NetworkConfig {
                public_key: network_key_pair.public_key,
                secret_key: network_key_pair.secret_key,
                account_id: block_producer.map(|bp| bp.account_id.clone()),
                addr: if config.network.addr.is_empty() {
                    None
                } else {
                    Some(config.network.addr.parse().unwrap())
                },
                boot_nodes: if config.network.boot_nodes.is_empty() {
                    vec![]
                } else {
                    config
                        .network
                        .boot_nodes
                        .split(",")
                        .map(|chunk| chunk.try_into().unwrap())
                        .collect()
                },
                handshake_timeout: config.network.handshake_timeout,
                reconnect_delay: config.network.reconnect_delay,
                bootstrap_peers_period: Duration::from_secs(60),
                peer_max_count: config.network.max_peers,
                ban_window: config.network.ban_window,
                max_send_peers: 512,
                peer_expiration_duration: Duration::from_secs(7 * 24 * 60 * 60),
            },
            rpc_config: config.rpc,
            genesis_config: genesis_config.clone(),
            block_producer: block_producer.map(Clone::clone),
        }
    }
}

impl NearConfig {
    /// Test tool to save configs back to the folder.
    /// Useful for dynamic creating testnet configs and then saving them in different folders.
    pub fn save_to_dir(&self, dir: &Path) {
        fs::create_dir_all(dir).expect("Failed to create directory");

        self.config.write_to_file(&dir.join(CONFIG_FILENAME));

        if let Some(block_producer) = &self.block_producer {
            block_producer.signer.write_to_file(&dir.join(self.config.validator_key_file.clone()));
        }

        let network_signer = InMemorySigner::from_secret_key(
            "".to_string(),
            self.network_config.public_key.clone(),
            self.network_config.secret_key.clone(),
        );
        network_signer.write_to_file(&dir.join(self.config.node_key_file.clone()));

        self.genesis_config.write_to_file(&dir.join(self.config.genesis_file.clone()));
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AccountInfo {
    pub account_id: AccountId,
    pub public_key: ReadablePublicKey,
    #[serde(with = "u128_hex_format")]
    pub amount: Balance,
}

/// Runtime configuration, defining genesis block.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenesisConfig {
    /// Official time of blockchain start.
    pub genesis_time: DateTime<Utc>,
    /// ID of the blockchain. This must be unique for every blockchain.
    /// If your testnet blockchains do not have unique chain IDs, you will have a bad time.
    pub chain_id: String,
    /// Number of block producer seats at genesis.
    pub num_block_producers: ValidatorId,
    /// Defines number of shards and number of validators per each shard at genesis.
    pub block_producers_per_shard: Vec<ValidatorId>,
    /// Expected number of fisherman per shard.
    pub avg_fisherman_per_shard: Vec<ValidatorId>,
    /// Enable dynamic re-sharding.
    pub dynamic_resharding: bool,
    /// Epoch length counted in blocks.
    pub epoch_length: BlockIndex,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// List of accounts / balances at genesis.
    pub accounts: Vec<AccountInfo>,
    /// List of contract code per accounts. Contract code encoded in base64.
    pub contracts: Vec<(AccountId, String)>,
}

impl GenesisConfig {
    pub fn legacy_test(seeds: Vec<&str>, num_validators: usize) -> Self {
        let mut validators = vec![];
        let mut accounts = vec![];
        let mut contracts = vec![];
        let default_test_contract =
            to_base(include_bytes!("../../runtime/wasm/runtest/res/wasm_with_mem.wasm").as_ref());
        for (i, account) in seeds.iter().enumerate() {
            let signer = InMemorySigner::from_seed(account, account);
            if i < num_validators {
                validators.push(AccountInfo {
                    account_id: account.to_string(),
                    public_key: signer.public_key.to_readable(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            accounts.push(AccountInfo {
                account_id: account.to_string(),
                public_key: signer.public_key.to_readable(),
                amount: TESTING_INIT_BALANCE,
            });
            contracts.push((account.to_string(), default_test_contract.clone()))
        }
        GenesisConfig {
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_block_producers: num_validators,
            block_producers_per_shard: vec![num_validators],
            avg_fisherman_per_shard: vec![0],
            dynamic_resharding: false,
            epoch_length: 10,
            validators,
            accounts,
            contracts,
        }
    }

    pub fn test(seeds: Vec<&str>) -> Self {
        let num_validators = seeds.len();
        Self::legacy_test(seeds, num_validators)
    }

    pub fn testing_spec(num_accounts: usize, num_validators: usize) -> Self {
        let mut accounts = vec![];
        let mut validators = vec![];
        for i in 0..num_accounts {
            let account_id = format!("near.{}", i);
            let signer = InMemorySigner::from_seed(&account_id, &account_id);
            if i < num_validators {
                validators.push(AccountInfo {
                    account_id: account_id.clone(),
                    public_key: signer.public_key.to_readable(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            accounts.push(AccountInfo {
                account_id,
                public_key: signer.public_key.to_readable(),
                amount: TESTING_INIT_BALANCE,
            });
        }
        GenesisConfig {
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_block_producers: num_validators,
            block_producers_per_shard: vec![num_validators],
            avg_fisherman_per_shard: vec![0],
            dynamic_resharding: false,
            epoch_length: 10,
            validators,
            accounts,
            contracts: vec![],
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

/// Official TestNet configuration.
pub fn testnet_genesis() -> GenesisConfig {
    GenesisConfig {
        genesis_time: DateTime::from_utc(NaiveDateTime::from_timestamp(1559628805, 0), Utc),
        chain_id: "testnet".to_string(),
        num_block_producers: 4,
        block_producers_per_shard: vec![4],
        avg_fisherman_per_shard: vec![100],
        dynamic_resharding: true,
        epoch_length: 20000,
        validators: vec![AccountInfo {
            account_id: ".near".to_string(),
            public_key: ReadablePublicKey(
                "DuZSg3DRUQDiR5Wvq5Viifaw2FXPimer2omyNBqUytua".to_string(),
            ),
            amount: 5_000_000 * NEAR_TOKEN,
        }],
        accounts: vec![AccountInfo {
            account_id: ".near".to_string(),
            public_key: ReadablePublicKey(
                "DuZSg3DRUQDiR5Wvq5Viifaw2FXPimer2omyNBqUytua".to_string(),
            ),
            amount: INITIAL_TOKEN_SUPPLY,
        }],
        contracts: vec![],
    }
}

/// Initializes genesis and client configs and stores in the given folder
pub fn init_configs(
    dir: &Path,
    chain_id: Option<&str>,
    account_id: Option<&str>,
    test_seed: Option<&str>,
) {
    fs::create_dir_all(dir).expect("Failed to create directory");
    // Check if config already exists in home dir.
    if dir.join(CONFIG_FILENAME).exists() {
        let config = Config::from_file(&dir.join(CONFIG_FILENAME));
        let genesis_config = GenesisConfig::from_file(&dir.join(config.genesis_file));
        panic!("Found existing config in {} with chain-id = {}. Use unsafe_reset_all to clear the folder.", dir.to_str().unwrap(), genesis_config.chain_id);
    }
    let chain_id = chain_id.map(|c| c.to_string()).unwrap_or(random_chain_id());
    match chain_id.as_ref() {
        "mainnet" => {
            // TODO:
            unimplemented!();
        }
        "testnet" => {
            if test_seed.is_some() {
                panic!("Test seed is not supported for official TestNet");
            }
            let config = Config::default();
            config.write_to_file(&dir.join(CONFIG_FILENAME));

            // If account id was given, create new key pair for this validator.
            if let Some(account_id) = account_id {
                let signer = InMemorySigner::new(account_id.to_string());
                info!(target: "near", "Use key {} for {} to stake.", signer.public_key, account_id);
                signer.write_to_file(&dir.join(config.validator_key_file));
            }

            let network_signer = InMemorySigner::new("".to_string());
            network_signer.write_to_file(&dir.join(config.node_key_file));

            testnet_genesis().write_to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key and genesis file in {}", dir.to_str().unwrap());
        }
        _ => {
            // Create new configuration, key files and genesis for one validator.
            let mut config = Config::default();
            config.network.skip_sync_wait = true;
            config.write_to_file(&dir.join(CONFIG_FILENAME));

            let account_id = account_id.unwrap_or("test.near").to_string();

            let signer = if let Some(test_seed) = test_seed {
                InMemorySigner::from_seed(&account_id, test_seed)
            } else {
                InMemorySigner::new(account_id.clone())
            };
            signer.write_to_file(&dir.join(config.validator_key_file));

            let network_signer = InMemorySigner::new("".to_string());
            network_signer.write_to_file(&dir.join(config.node_key_file));

            let genesis_config = GenesisConfig {
                genesis_time: Utc::now(),
                chain_id,
                num_block_producers: 1,
                block_producers_per_shard: vec![1],
                avg_fisherman_per_shard: vec![0],
                dynamic_resharding: false,
                epoch_length: 1000,
                validators: vec![AccountInfo {
                    account_id: account_id.clone(),
                    public_key: signer.public_key.to_readable(),
                    amount: TESTING_INIT_STAKE,
                }],
                accounts: vec![AccountInfo {
                    account_id,
                    public_key: signer.public_key.to_readable(),
                    amount: TESTING_INIT_BALANCE,
                }],
                contracts: vec![],
            };
            genesis_config.write_to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.to_str().unwrap());
        }
    }
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_non_validators: usize,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemorySigner>, Vec<InMemorySigner>, GenesisConfig) {
    let num_validators = seeds.len() - num_non_validators;
    let signers =
        seeds.iter().map(|seed| InMemorySigner::new(seed.to_string())).collect::<Vec<_>>();
    let network_signers =
        (0..seeds.len()).map(|_| InMemorySigner::from_random()).collect::<Vec<_>>();
    let accounts = seeds
        .iter()
        .enumerate()
        .map(|(i, seed)| AccountInfo {
            account_id: seed.to_string(),
            public_key: signers[i].public_key.to_readable(),
            amount: TESTING_INIT_BALANCE,
        })
        .collect::<Vec<_>>();
    let validators = seeds
        .iter()
        .enumerate()
        .take(seeds.len() - num_non_validators)
        .map(|(i, seed)| AccountInfo {
            account_id: seed.to_string(),
            public_key: signers[i].public_key.to_readable(),
            amount: TESTING_INIT_STAKE,
        })
        .collect::<Vec<_>>();
    let genesis_config = GenesisConfig {
        genesis_time: Utc::now(),
        chain_id: random_chain_id(),
        num_block_producers: num_validators,
        block_producers_per_shard: vec![num_validators],
        avg_fisherman_per_shard: vec![0],
        dynamic_resharding: false,
        epoch_length: 10,
        validators,
        accounts,
        contracts: vec![],
    };
    let mut configs = vec![];
    let first_node_port = open_port();
    for i in 0..seeds.len() {
        let mut config = Config::default();
        if local_ports {
            config.network.addr =
                format!("127.0.0.1:{}", if i == 0 { first_node_port } else { open_port() });
            config.rpc.addr = format!("127.0.0.1:{}", open_port());
            config.network.boot_nodes = if i == 0 {
                "".to_string()
            } else {
                format!("{}@127.0.0.1:{}", network_signers[0].public_key, first_node_port)
            };
            config.network.skip_sync_wait = num_validators == 1;
            config.consensus.min_num_peers =
                cmp::min(num_validators - 1, config.consensus.min_num_peers);
        }
        configs.push(config);
    }
    (configs, signers, network_signers, genesis_config)
}

/// Create testnet configuration. If `local_ports` is true,
/// sets up new ports for all nodes except the first one and sets boot node to it.
pub fn create_testnet_configs(
    num_validators: usize,
    num_non_validators: usize,
    prefix: &str,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemorySigner>, Vec<InMemorySigner>, GenesisConfig) {
    create_testnet_configs_from_seeds(
        (0..(num_validators + num_non_validators))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_non_validators,
        local_ports,
    )
}

pub fn init_testnet_configs(
    dir: &Path,
    num_validators: usize,
    num_non_validators: usize,
    prefix: &str,
) {
    let (configs, signers, network_signers, genesis_config) =
        create_testnet_configs(num_validators, num_non_validators, prefix, false);
    for i in 0..(num_validators + num_non_validators) {
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        signers[i].write_to_file(&node_dir.join(configs[i].validator_key_file.clone()));
        network_signers[i].write_to_file(&node_dir.join(configs[i].node_key_file.clone()));

        genesis_config.write_to_file(&node_dir.join(configs[i].genesis_file.clone()));
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME));
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.to_str().unwrap());
    }
}

pub fn load_config(dir: &Path) -> NearConfig {
    let config = Config::from_file(&dir.join(CONFIG_FILENAME));
    let genesis_config = GenesisConfig::from_file(&dir.join(config.genesis_file.clone()));
    let signer = Arc::new(InMemorySigner::from_file(&dir.join(config.validator_key_file.clone())));
    let block_producer = BlockProducer::from(signer);
    let network_signer = InMemorySigner::from_file(&dir.join(config.node_key_file.clone()));
    NearConfig::new(config, &genesis_config, network_signer.into(), Some(&block_producer))
}

pub fn load_test_config(seed: &str, port: u16, genesis_config: &GenesisConfig) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = format!("0.0.0.0:{}", port);
    config.rpc.addr = format!("0.0.0.0:{}", port + 100);
    let signer = Arc::new(InMemorySigner::from_seed(seed, seed));
    let block_producer = BlockProducer::from(signer.clone());
    NearConfig::new(config, &genesis_config, signer.into(), Some(&block_producer))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use near_primitives::types::ReadablePublicKey;

    use super::*;

    #[test]
    fn test_deserialize() {
        let data = json!({
            "genesis_time": "2019-05-07T00:10:14.434719Z",
            "chain_id": "test-chain-XYQAS",
            "num_block_producers": 1,
            "block_producers_per_shard": [1],
            "avg_fisherman_per_shard": [1],
            "dynamic_resharding": false,
            "epoch_length": 100,
            "accounts": [{"account_id": "alice.near", "public_key": "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", "amount": "0x64"}],
            "validators": [{"account_id": "alice.near", "public_key": "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", "amount": "32"}],
            "contracts": [],
        });
        let spec = GenesisConfig::from(data.to_string().as_str());
        assert_eq!(
            spec.validators[0],
            AccountInfo {
                account_id: "alice.near".to_string(),
                public_key: ReadablePublicKey(
                    "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string()
                ),
                amount: 50
            }
        );
    }
}
