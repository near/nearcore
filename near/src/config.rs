use std::cmp::max;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp, fs};

use chrono::{DateTime, Utc};
use log::info;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

use near_chain::ChainGenesis;
use near_client::BlockProducer;
use near_client::ClientConfig;
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, Signer};
use near_jsonrpc::RpcConfig;
use near_network::test_utils::open_port;
use near_network::types::PROTOCOL_VERSION;
use near_network::NetworkConfig;
use near_primitives::account::AccessKey;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::{to_base64, u128_dec_format};
use near_primitives::types::{AccountId, Balance, BlockIndex, Gas, ShardId, ValidatorId};
use near_primitives::utils::get_num_block_producers_per_shard;
use near_primitives::views::AccountView;
use near_telemetry::TelemetryConfig;
use node_runtime::config::RuntimeConfig;
use node_runtime::StateRecord;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^18.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000;

/// Millinear, 1/1000 of NEAR.
pub const MILLI_NEAR: Balance = NEAR_BASE / 1000;

/// Attonear, 1/10^18 of NEAR.
pub const ATTO_NEAR: Balance = 1;

/// Block production tracking delay.
pub const BLOCK_PRODUCTION_TRACKING_DELAY: u64 = 100;

/// Expected block production time in secs.
pub const MIN_BLOCK_PRODUCTION_DELAY: u64 = 1;

/// Maximum time to delay block production without approvals.
pub const MAX_BLOCK_PRODUCTION_DELAY: u64 = 2;

/// Maximum time until skipping the previous block.
pub const MAX_BLOCK_WAIT_DELAY: u64 = 6;

/// Expected epoch length.
pub const EXPECTED_EPOCH_LENGTH: BlockIndex = (5 * 60) / MIN_BLOCK_PRODUCTION_DELAY;

/// Criterion for kicking out validators.
pub const VALIDATOR_KICKOUT_THRESHOLD: u8 = 90;

/// Fast mode constants for testing/developing.
pub const FAST_MIN_BLOCK_PRODUCTION_DELAY: u64 = 200;
pub const FAST_MAX_BLOCK_PRODUCTION_DELAY: u64 = 500;
pub const FAST_EPOCH_LENGTH: u64 = 60;

/// Time to persist Accounts Id in the router without removing them in seconds.
pub const TTL_ACCOUNT_ID_ROUTER: u64 = 60 * 60;
/// Maximum amount of routes to store for each account id.
pub const MAX_ROUTES_TO_STORE: usize = 5;
/// Expected number of blocks per year
pub const NUM_BLOCKS_PER_YEAR: u64 = 365 * 24 * 60 * 60;

/// Initial gas limit.
pub const INITIAL_GAS_LIMIT: Gas = 10_000_000;

/// Initial gas price.
pub const INITIAL_GAS_PRICE: Balance = 100;

/// The rate at which the gas price can be adjusted (alpha in the formula).
/// The formula is
/// gas_price_t = gas_price_{t-1} * (1 + (gas_used/gas_limit - 1/2) * alpha))
/// This constant is supposedly 0.01 and should be divided by 100 when used
pub const GAS_PRICE_ADJUSTMENT_RATE: u8 = 1;

/// Rewards
pub const PROTOCOL_PERCENT: u8 = 10;
pub const DEVELOPER_PERCENT: u8 = 30;

/// Protocol treasury account
pub const PROTOCOL_TREASURY_ACCOUNT: &str = "near";

/// Maximum inflation rate per year
pub const MAX_INFLATION_RATE: u8 = 5;

/// Number of blocks for which a given transaction is valid
pub const TRANSACTION_VALIDITY_PERIOD: u64 = 100;

/// Number of seats for block producers
pub const NUM_BLOCK_PRODUCERS: ValidatorId = 50;

pub const CONFIG_FILENAME: &str = "config.json";
pub const GENESIS_CONFIG_FILENAME: &str = "genesis.json";
pub const NODE_KEY_FILE: &str = "node_key.json";
pub const VALIDATOR_KEY_FILE: &str = "validator_key.json";

pub const DEFAULT_TELEMETRY_URL: &str = "https://explorer.nearprotocol.com/api/nodes";

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
    /// Duration to check for producing / skipping block.
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    pub max_block_wait_delay: Duration,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            block_production_tracking_delay: Duration::from_millis(BLOCK_PRODUCTION_TRACKING_DELAY),
            min_block_production_delay: Duration::from_secs(MIN_BLOCK_PRODUCTION_DELAY),
            max_block_production_delay: Duration::from_secs(MAX_BLOCK_PRODUCTION_DELAY),
            max_block_wait_delay: Duration::from_secs(MAX_BLOCK_WAIT_DELAY),
            produce_empty_blocks: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    pub genesis_file: String,
    pub validator_key_file: String,
    pub node_key_file: String,
    pub rpc: RpcConfig,
    pub telemetry: TelemetryConfig,
    pub network: Network,
    pub consensus: Consensus,
    pub tracked_accounts: Vec<AccountId>,
    pub tracked_shards: Vec<ShardId>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            genesis_file: GENESIS_CONFIG_FILENAME.to_string(),
            validator_key_file: VALIDATOR_KEY_FILE.to_string(),
            node_key_file: NODE_KEY_FILE.to_string(),
            rpc: RpcConfig::default(),
            telemetry: TelemetryConfig::default(),
            network: Network::default(),
            consensus: Consensus::default(),
            tracked_accounts: vec![],
            tracked_shards: vec![],
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
    pub telemetry_config: TelemetryConfig,
    pub block_producer: Option<BlockProducer>,
    pub genesis_config: GenesisConfig,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis_config: &GenesisConfig,
        network_key_pair: KeyFile,
        block_producer: Option<BlockProducer>,
    ) -> Self {
        NearConfig {
            config: config.clone(),
            client_config: ClientConfig {
                version: Default::default(),
                chain_id: genesis_config.chain_id.clone(),
                rpc_addr: config.rpc.addr.clone(),
                block_production_tracking_delay: config.consensus.block_production_tracking_delay,
                min_block_production_delay: config.consensus.min_block_production_delay,
                max_block_production_delay: config.consensus.max_block_production_delay,
                max_block_wait_delay: config.consensus.max_block_wait_delay,
                block_expected_weight: 1000,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: Duration::from_secs(10),
                sync_step_period: Duration::from_millis(10),
                sync_weight_threshold: 0,
                sync_height_threshold: 1,
                min_num_peers: config.consensus.min_num_peers,
                fetch_info_period: Duration::from_millis(100),
                log_summary_period: Duration::from_secs(10),
                produce_empty_blocks: config.consensus.produce_empty_blocks,
                epoch_length: genesis_config.epoch_length,
                num_block_producers: genesis_config.num_block_producers,
                announce_account_horizon: genesis_config.epoch_length / 2,
                ttl_account_id_router: Duration::from_secs(TTL_ACCOUNT_ID_ROUTER),
                // TODO(1047): this should be adjusted depending on the speed of sync of state.
                block_fetch_horizon: 50,
                state_fetch_horizon: 5,
                block_header_fetch_horizon: 50,
                catchup_step_period: Duration::from_millis(100),
                chunk_request_retry_period: Duration::from_millis(100),
                tracked_accounts: config.tracked_accounts,
                tracked_shards: config.tracked_shards,
            },
            network_config: NetworkConfig {
                public_key: network_key_pair.public_key,
                secret_key: network_key_pair.secret_key,
                account_id: block_producer.as_ref().map(|bp| bp.account_id.clone()),
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
                        .split(',')
                        .map(|chunk| chunk.try_into().expect("Failed to parse PeerInfo"))
                        .collect()
                },
                handshake_timeout: config.network.handshake_timeout,
                reconnect_delay: config.network.reconnect_delay,
                bootstrap_peers_period: Duration::from_secs(60),
                peer_max_count: config.network.max_peers,
                ban_window: config.network.ban_window,
                max_send_peers: 512,
                peer_expiration_duration: Duration::from_secs(7 * 24 * 60 * 60),
                peer_stats_period: Duration::from_secs(5),
                ttl_account_id_router: Duration::from_secs(TTL_ACCOUNT_ID_ROUTER),
                max_routes_to_store: MAX_ROUTES_TO_STORE,
            },
            telemetry_config: config.telemetry,
            rpc_config: config.rpc,
            genesis_config: genesis_config.clone(),
            block_producer,
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

        let network_signer =
            InMemorySigner::from_secret_key("".to_string(), self.network_config.secret_key.clone());
        network_signer.write_to_file(&dir.join(self.config.node_key_file.clone()));

        self.genesis_config.write_to_file(&dir.join(self.config.genesis_file.clone()));
    }
}

/// Account info for validators
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AccountInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
}

/// Runtime configuration, defining genesis block.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenesisConfig {
    /// Protocol version that this genesis works with.
    pub protocol_version: u32,
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
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Initial gas price.
    pub gas_price: Balance,
    /// Criterion for kicking out validators (this is a number between 0 and 100)
    pub validator_kickout_threshold: u8,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: u8,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// Records in storage at genesis (get split into shards at genesis creation).
    pub records: Vec<StateRecord>,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: u64,
    /// Developer reward percentage (this is a number between 0 and 100)
    pub developer_reward_percentage: u8,
    /// Protocol treasury percentage (this is a number between 0 and 100)
    pub protocol_reward_percentage: u8,
    /// Maximum inflation on the total supply every epoch (this is a number between 0 and 100)
    pub max_inflation_rate: u8,
    /// Total supply of tokens at genesis.
    pub total_supply: u128,
    /// Expected number of blocks per year
    pub num_blocks_per_year: u64,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
}

pub fn get_initial_supply(records: &[StateRecord]) -> Balance {
    let mut total_supply = 0;
    for record in records {
        if let StateRecord::Account { account, .. } = record {
            total_supply += account.amount + account.locked;
        }
    }
    total_supply
}

impl From<GenesisConfig> for ChainGenesis {
    fn from(genesis_config: GenesisConfig) -> Self {
        ChainGenesis::new(
            genesis_config.chain_id,
            genesis_config.genesis_time,
            genesis_config.gas_limit,
            genesis_config.gas_price,
            genesis_config.total_supply,
            genesis_config.max_inflation_rate,
            genesis_config.gas_price_adjustment_rate,
            genesis_config.transaction_validity_period,
        )
    }
}

fn add_protocol_account(records: &mut Vec<StateRecord>) {
    let signer = InMemorySigner::from_seed(
        PROTOCOL_TREASURY_ACCOUNT,
        KeyType::ED25519,
        PROTOCOL_TREASURY_ACCOUNT,
    );
    records.extend(state_records_account_with_key(
        PROTOCOL_TREASURY_ACCOUNT,
        &signer.public_key,
        TESTING_INIT_BALANCE,
        0,
        CryptoHash::default(),
    ));
}

impl GenesisConfig {
    fn test_with_seeds(
        seeds: Vec<&str>,
        num_validators: usize,
        validators_per_shard: Vec<usize>,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("../runtime/near-vm-runner/tests/res/test_contract_rs.wasm");
        let default_test_contract = std::fs::read(path).unwrap();
        let encoded_test_contract = to_base64(&default_test_contract);
        let code_hash = hash(&default_test_contract);
        for (i, &account) in seeds.iter().enumerate() {
            let signer = InMemorySigner::from_seed(account, KeyType::ED25519, account);
            if i < num_validators {
                validators.push(AccountInfo {
                    account_id: account.to_string(),
                    public_key: signer.public_key.clone(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            records.extend(
                state_records_account_with_key(
                    account,
                    &signer.public_key.clone(),
                    TESTING_INIT_BALANCE - if i < num_validators { TESTING_INIT_STAKE } else { 0 },
                    if i < num_validators { TESTING_INIT_STAKE } else { 0 },
                    code_hash,
                )
                .into_iter(),
            );
            records.push(StateRecord::Contract {
                account_id: account.to_string(),
                code: encoded_test_contract.clone(),
            });
        }
        add_protocol_account(&mut records);
        let total_supply = get_initial_supply(&records);
        GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_block_producers: num_validators,
            block_producers_per_shard: validators_per_shard.clone(),
            avg_fisherman_per_shard: validators_per_shard.iter().map(|_| 0).collect(),
            dynamic_resharding: false,
            epoch_length: FAST_EPOCH_LENGTH,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price: INITIAL_GAS_PRICE,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            validator_kickout_threshold: VALIDATOR_KICKOUT_THRESHOLD,
            runtime_config: Default::default(),
            validators,
            records,
            developer_reward_percentage: DEVELOPER_PERCENT,
            protocol_reward_percentage: PROTOCOL_PERCENT,
            max_inflation_rate: MAX_INFLATION_RATE,
            total_supply,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.to_string(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
        }
    }

    pub fn test(seeds: Vec<&str>, num_validators: usize) -> Self {
        Self::test_with_seeds(seeds, num_validators, vec![num_validators])
    }

    pub fn test_free(seeds: Vec<&str>, num_validators: usize) -> Self {
        let mut config = Self::test_with_seeds(seeds, num_validators, vec![num_validators]);
        config.runtime_config = RuntimeConfig::free();
        config
    }

    pub fn test_sharded(
        seeds: Vec<&str>,
        num_validators: usize,
        validators_per_shard: Vec<usize>,
    ) -> Self {
        Self::test_with_seeds(seeds, num_validators, validators_per_shard)
    }

    /// Reads GenesisConfig from a file.
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open genesis config file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from genesis config file.");
        GenesisConfig::from(content.as_str())
    }

    /// Writes GenesisConfig to the file.
    pub fn write_to_file(&self, path: &Path) {
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
        let mut config: GenesisConfig =
            serde_json::from_str(config).expect("Failed to deserialize the genesis config.");
        if config.protocol_version != PROTOCOL_VERSION {
            panic!(format!(
                "Incorrect version of genesis config {} expected {}",
                config.protocol_version, PROTOCOL_VERSION
            ));
        }
        let num_shards = config.block_producers_per_shard.len();
        let num_chunk_producers: ValidatorId = config.block_producers_per_shard.iter().sum();
        if num_chunk_producers != max(config.num_block_producers, num_shards) {
            panic!(format!(
                "Number of chunk producers {} does not match number of block producers {}",
                num_chunk_producers, config.num_block_producers
            ));
        }
        let total_supply = get_initial_supply(&config.records);
        config.total_supply = total_supply;
        config
    }
}

fn random_chain_id() -> String {
    format!("test-chain-{}", thread_rng().sample_iter(&Alphanumeric).take(5).collect::<String>())
}

fn state_records_account_with_key(
    account_id: &str,
    public_key: &PublicKey,
    amount: u128,
    staked: u128,
    code_hash: CryptoHash,
) -> Vec<StateRecord> {
    vec![
        StateRecord::Account {
            account_id: account_id.to_string(),
            account: AccountView {
                amount,
                locked: staked,
                code_hash,
                storage_usage: 0,
                storage_paid_at: 0,
            },
        },
        StateRecord::AccessKey {
            account_id: account_id.to_string(),
            public_key: public_key.clone(),
            access_key: AccessKey::full_access().into(),
        },
    ]
}

/// Official TestNet configuration.
pub fn testnet_genesis() -> GenesisConfig {
    let genesis_config_bytes = include_bytes!("../res/testnet.json");
    GenesisConfig::from(
        str::from_utf8(genesis_config_bytes).expect("Failed to read testnet configuration"),
    )
}

/// Initializes genesis and client configs and stores in the given folder
pub fn init_configs(
    dir: &Path,
    chain_id: Option<&str>,
    account_id: Option<&str>,
    test_seed: Option<&str>,
    num_shards: ShardId,
    fast: bool,
) {
    fs::create_dir_all(dir).expect("Failed to create directory");
    // Check if config already exists in home dir.
    if dir.join(CONFIG_FILENAME).exists() {
        let config = Config::from_file(&dir.join(CONFIG_FILENAME));
        let genesis_config = GenesisConfig::from_file(&dir.join(config.genesis_file));
        panic!("Found existing config in {} with chain-id = {}. Use unsafe_reset_all to clear the folder.", dir.to_str().unwrap(), genesis_config.chain_id);
    }
    let chain_id = chain_id
        .and_then(|c| if c.is_empty() { None } else { Some(c.to_string()) })
        .unwrap_or_else(random_chain_id);
    match chain_id.as_ref() {
        "mainnet" => {
            // TODO:
            unimplemented!();
        }
        "testnet" | "staging" => {
            if test_seed.is_some() {
                panic!("Test seed is not supported for official TestNet");
            }
            let mut config = Config::default();
            config.telemetry.endpoints.push(DEFAULT_TELEMETRY_URL.to_string());
            config.write_to_file(&dir.join(CONFIG_FILENAME));

            // If account id was given, create new key pair for this validator.
            if let Some(account_id) =
                account_id.and_then(|x| if x.is_empty() { None } else { Some(x.to_string()) })
            {
                let signer = InMemorySigner::from_random(account_id.clone(), KeyType::ED25519);
                info!(target: "near", "Use key {} for {} to stake.", signer.public_key, account_id);
                signer.write_to_file(&dir.join(config.validator_key_file));
            }

            let network_signer = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
            network_signer.write_to_file(&dir.join(config.node_key_file));

            let mut genesis_config = testnet_genesis();
            genesis_config.chain_id = chain_id;

            genesis_config.write_to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key and genesis file in {}", dir.to_str().unwrap());
        }
        _ => {
            // Create new configuration, key files and genesis for one validator.
            let mut config = Config::default();
            config.network.skip_sync_wait = true;
            if fast {
                config.consensus.min_block_production_delay =
                    Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
                config.consensus.max_block_production_delay =
                    Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
            }
            config.write_to_file(&dir.join(CONFIG_FILENAME));

            let account_id = account_id
                .and_then(|x| if x.is_empty() { None } else { Some(x) })
                .unwrap_or("test.near")
                .to_string();

            let signer = if let Some(test_seed) = test_seed {
                InMemorySigner::from_seed(&account_id, KeyType::ED25519, test_seed)
            } else {
                InMemorySigner::from_random(account_id.clone(), KeyType::ED25519)
            };
            signer.write_to_file(&dir.join(config.validator_key_file));

            let network_signer = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
            network_signer.write_to_file(&dir.join(config.node_key_file));
            let mut records = state_records_account_with_key(
                &account_id,
                &signer.public_key,
                TESTING_INIT_BALANCE,
                TESTING_INIT_STAKE,
                CryptoHash::default(),
            );
            add_protocol_account(&mut records);
            let total_supply = get_initial_supply(&records);

            let genesis_config = GenesisConfig {
                protocol_version: PROTOCOL_VERSION,
                genesis_time: Utc::now(),
                chain_id,
                num_block_producers: NUM_BLOCK_PRODUCERS,
                block_producers_per_shard: get_num_block_producers_per_shard(
                    num_shards,
                    NUM_BLOCK_PRODUCERS,
                ),
                avg_fisherman_per_shard: (0..num_shards).map(|_| 0).collect(),
                dynamic_resharding: false,
                epoch_length: if fast { FAST_EPOCH_LENGTH } else { EXPECTED_EPOCH_LENGTH },
                gas_limit: INITIAL_GAS_LIMIT,
                gas_price: INITIAL_GAS_PRICE,
                gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
                validator_kickout_threshold: VALIDATOR_KICKOUT_THRESHOLD,
                runtime_config: Default::default(),
                validators: vec![AccountInfo {
                    account_id: account_id.clone(),
                    public_key: signer.public_key,
                    amount: TESTING_INIT_STAKE,
                }],
                transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
                records,
                developer_reward_percentage: DEVELOPER_PERCENT,
                protocol_reward_percentage: PROTOCOL_PERCENT,
                max_inflation_rate: MAX_INFLATION_RATE,
                total_supply,
                num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
                protocol_treasury_account: account_id,
            };
            genesis_config.write_to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.to_str().unwrap());
        }
    }
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: usize,
    num_non_validators: usize,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemorySigner>, Vec<InMemorySigner>, GenesisConfig) {
    let num_validators = seeds.len() - num_non_validators;
    let signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed(seed, KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("", KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let genesis_config = GenesisConfig::test_sharded(
        seeds.iter().map(|s| s.as_str()).collect(),
        num_validators,
        get_num_block_producers_per_shard(num_shards as u64, num_validators),
    );
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
        }
        config.consensus.min_num_peers =
            cmp::min(num_validators - 1, config.consensus.min_num_peers);
        configs.push(config);
    }
    (configs, signers, network_signers, genesis_config)
}

/// Create testnet configuration. If `local_ports` is true,
/// sets up new ports for all nodes except the first one and sets boot node to it.
pub fn create_testnet_configs(
    num_shards: usize,
    num_validators: usize,
    num_non_validators: usize,
    prefix: &str,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemorySigner>, Vec<InMemorySigner>, GenesisConfig) {
    create_testnet_configs_from_seeds(
        (0..(num_validators + num_non_validators))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_shards,
        num_non_validators,
        local_ports,
    )
}

pub fn init_testnet_configs(
    dir: &Path,
    num_shards: usize,
    num_validators: usize,
    num_non_validators: usize,
    prefix: &str,
) {
    let (configs, signers, network_signers, genesis_config) =
        create_testnet_configs(num_shards, num_validators, num_non_validators, prefix, false);
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
    let block_producer = if dir.join(config.validator_key_file.clone()).exists() {
        let signer =
            Arc::new(InMemorySigner::from_file(&dir.join(config.validator_key_file.clone())));
        Some(BlockProducer::from(signer))
    } else {
        None
    };
    let network_signer = InMemorySigner::from_file(&dir.join(config.node_key_file.clone()));
    NearConfig::new(config, &genesis_config, (&network_signer).into(), block_producer)
}

pub fn load_test_config(seed: &str, port: u16, genesis_config: &GenesisConfig) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = format!("0.0.0.0:{}", port);
    config.rpc.addr = format!("0.0.0.0:{}", open_port());
    config.consensus.min_block_production_delay =
        Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
    config.consensus.max_block_production_delay =
        Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
    let (signer, block_producer) = if seed.is_empty() {
        let signer = Arc::new(InMemorySigner::from_random("".to_string(), KeyType::ED25519));
        (signer, None)
    } else {
        let signer = Arc::new(InMemorySigner::from_seed(seed, KeyType::ED25519, seed));
        let block_producer = Some(BlockProducer::from(signer.clone()));
        (signer, block_producer)
    };
    NearConfig::new(config, &genesis_config, signer.into(), block_producer)
}

#[cfg(test)]
mod test {
    use crate::config::testnet_genesis;

    /// make sure testnet genesis can be deserialized
    #[test]
    fn test_deserialize_state() {
        let genesis_config = testnet_genesis();
        assert!(genesis_config.total_supply > 0);
    }
}
