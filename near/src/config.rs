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
use serde_derive::{Deserialize, Serialize};

use near_chain::ChainGenesis;
use near_client::ClientConfig;
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, Signer};
use near_jsonrpc::RpcConfig;
use near_network::test_utils::open_port;
use near_network::types::{PROTOCOL_VERSION, ROUTED_MESSAGE_TTL};
use near_network::utils::blacklist_from_vec;
use near_network::NetworkConfig;
use near_primitives::account::AccessKey;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::{to_base64, u128_dec_format};
use near_primitives::types::{
    AccountId, Balance, BlockHeightDelta, Gas, NumBlocks, NumSeats, NumShards, ShardId,
};
use near_primitives::utils::{generate_random_string, get_num_seats_per_shard};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::views::AccountView;
use near_telemetry::TelemetryConfig;
use node_runtime::config::RuntimeConfig;
use node_runtime::StateRecord;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

/// Millinear, 1/1000 of NEAR.
pub const MILLI_NEAR: Balance = NEAR_BASE / 1000;

/// Attonear, 1/10^18 of NEAR.
pub const ATTO_NEAR: Balance = 1;

/// Block production tracking delay.
pub const BLOCK_PRODUCTION_TRACKING_DELAY: u64 = 100;

/// Expected block production time in ms.
pub const MIN_BLOCK_PRODUCTION_DELAY: u64 = 600;

/// Maximum time to delay block production without approvals is ms.
pub const MAX_BLOCK_PRODUCTION_DELAY: u64 = 2_000;

/// Maximum time until skipping the previous block is ms.
pub const MAX_BLOCK_WAIT_DELAY: u64 = 6_000;

/// Reduce wait time for every missing block in ms.
const REDUCE_DELAY_FOR_MISSING_BLOCKS: u64 = 100;

/// Horizon at which instead of fetching block, fetch full state.
const BLOCK_FETCH_HORIZON: BlockHeightDelta = 50;

/// Horizon to step from the latest block when fetching state.
const STATE_FETCH_HORIZON: NumBlocks = 5;

/// Behind this horizon header fetch kicks in.
const BLOCK_HEADER_FETCH_HORIZON: BlockHeightDelta = 50;

/// Time between check to perform catchup.
const CATCHUP_STEP_PERIOD: u64 = 100;

/// Time between checking to re-request chunks.
const CHUNK_REQUEST_RETRY_PERIOD: u64 = 200;

/// Expected epoch length.
pub const EXPECTED_EPOCH_LENGTH: BlockHeightDelta = (5 * 60 * 1000) / MIN_BLOCK_PRODUCTION_DELAY;

/// Criterion for kicking out block producers.
pub const BLOCK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Criterion for kicking out chunk producers.
pub const CHUNK_PRODUCER_KICKOUT_THRESHOLD: u8 = 60;

/// Fast mode constants for testing/developing.
pub const FAST_MIN_BLOCK_PRODUCTION_DELAY: u64 = 120;
pub const FAST_MAX_BLOCK_PRODUCTION_DELAY: u64 = 500;
pub const FAST_EPOCH_LENGTH: BlockHeightDelta = 60;

/// Time to persist Accounts Id in the router without removing them in seconds.
pub const TTL_ACCOUNT_ID_ROUTER: u64 = 60 * 60;
/// Maximum amount of routes to store for each account id.
pub const MAX_ROUTES_TO_STORE: usize = 5;
/// Expected number of blocks per year
pub const NUM_BLOCKS_PER_YEAR: u64 = 365 * 24 * 60 * 60;

/// Initial gas limit.
pub const INITIAL_GAS_LIMIT: Gas = 1_000_000_000_000_000;

/// Initial gas price.
pub const MIN_GAS_PRICE: Balance = 5000;

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

/// Fishermen stake threshold.
pub const FISHERMEN_THRESHOLD: Balance = 10 * NEAR_BASE;

/// Maximum inflation rate per year
pub const MAX_INFLATION_RATE: u8 = 5;

/// Number of blocks for which a given transaction is valid
pub const TRANSACTION_VALIDITY_PERIOD: NumBlocks = 100;

/// Number of seats for block producers
pub const NUM_BLOCK_PRODUCER_SEATS: NumSeats = 50;

/// How much height horizon to give to consider peer up to date.
pub const HIGHEST_PEER_HORIZON: u64 = 5;

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
    /// List of addresses that will not be accepted as valid neighbors.
    /// It can be IP:Port or IP (to blacklist all connections coming from this address).
    #[serde(default)]
    pub blacklist: Vec<String>,
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
            blacklist: vec![],
        }
    }
}

/// Serde default only supports functions without parameters.
fn default_reduce_wait_for_missing_block() -> Duration {
    Duration::from_millis(REDUCE_DELAY_FOR_MISSING_BLOCKS)
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
    /// Duration to reduce the wait for each missed block by validator.
    #[serde(default = "default_reduce_wait_for_missing_block")]
    pub reduce_wait_for_missing_block: Duration,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
    /// Horizon at which instead of fetching block, fetch full state.
    pub block_fetch_horizon: BlockHeightDelta,
    /// Horizon to step from the latest block when fetching state.
    pub state_fetch_horizon: NumBlocks,
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockHeightDelta,
    /// Time between check to perform catchup.
    pub catchup_step_period: Duration,
    /// Time between checking to re-request chunks.
    pub chunk_request_retry_period: Duration,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            block_production_tracking_delay: Duration::from_millis(BLOCK_PRODUCTION_TRACKING_DELAY),
            min_block_production_delay: Duration::from_millis(MIN_BLOCK_PRODUCTION_DELAY),
            max_block_production_delay: Duration::from_millis(MAX_BLOCK_PRODUCTION_DELAY),
            max_block_wait_delay: Duration::from_millis(MAX_BLOCK_WAIT_DELAY),
            reduce_wait_for_missing_block: Duration::from_millis(REDUCE_DELAY_FOR_MISSING_BLOCKS),
            produce_empty_blocks: true,
            block_fetch_horizon: BLOCK_FETCH_HORIZON,
            state_fetch_horizon: STATE_FETCH_HORIZON,
            block_header_fetch_horizon: BLOCK_HEADER_FETCH_HORIZON,
            catchup_step_period: Duration::from_millis(CATCHUP_STEP_PERIOD),
            chunk_request_retry_period: Duration::from_millis(CHUNK_REQUEST_RETRY_PERIOD),
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
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
    pub genesis_config: GenesisConfig,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis_config: &GenesisConfig,
        network_key_pair: KeyFile,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
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
                reduce_wait_for_missing_block: config.consensus.reduce_wait_for_missing_block,
                block_expected_weight: 1000,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: Duration::from_secs(10),
                sync_step_period: Duration::from_millis(10),
                sync_height_threshold: 1,
                header_sync_initial_timeout: Duration::from_secs(10),
                header_sync_progress_timeout: Duration::from_secs(2),
                header_sync_stall_ban_timeout: Duration::from_secs(40),
                header_sync_expected_height_per_second: 10,
                min_num_peers: config.consensus.min_num_peers,
                log_summary_period: Duration::from_secs(10),
                produce_empty_blocks: config.consensus.produce_empty_blocks,
                epoch_length: genesis_config.epoch_length,
                num_block_producer_seats: genesis_config.num_block_producer_seats,
                announce_account_horizon: genesis_config.epoch_length / 2,
                ttl_account_id_router: Duration::from_secs(TTL_ACCOUNT_ID_ROUTER),
                // TODO(1047): this should be adjusted depending on the speed of sync of state.
                block_fetch_horizon: config.consensus.block_fetch_horizon,
                state_fetch_horizon: config.consensus.state_fetch_horizon,
                block_header_fetch_horizon: config.consensus.block_header_fetch_horizon,
                catchup_step_period: Duration::from_millis(CATCHUP_STEP_PERIOD),
                chunk_request_retry_period: Duration::from_millis(CHUNK_REQUEST_RETRY_PERIOD),
                tracked_accounts: config.tracked_accounts,
                tracked_shards: config.tracked_shards,
            },
            network_config: NetworkConfig {
                public_key: network_key_pair.public_key,
                secret_key: network_key_pair.secret_key,
                account_id: validator_signer.as_ref().map(|vs| vs.validator_id().clone()),
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
                max_peer: config.network.max_peers,
                ban_window: config.network.ban_window,
                max_send_peers: 512,
                peer_expiration_duration: Duration::from_secs(7 * 24 * 60 * 60),
                peer_stats_period: Duration::from_secs(5),
                ttl_account_id_router: Duration::from_secs(TTL_ACCOUNT_ID_ROUTER),
                routed_message_ttl: ROUTED_MESSAGE_TTL,
                max_routes_to_store: MAX_ROUTES_TO_STORE,
                highest_peer_horizon: HIGHEST_PEER_HORIZON,
                push_info_period: Duration::from_millis(100),
                blacklist: blacklist_from_vec(&config.network.blacklist),
                outbound_disabled: false,
            },
            telemetry_config: config.telemetry,
            rpc_config: config.rpc,
            genesis_config: genesis_config.clone(),
            validator_signer,
        }
    }
}

impl NearConfig {
    /// Test tool to save configs back to the folder.
    /// Useful for dynamic creating testnet configs and then saving them in different folders.
    pub fn save_to_dir(&self, dir: &Path) {
        fs::create_dir_all(dir).expect("Failed to create directory");

        self.config.write_to_file(&dir.join(CONFIG_FILENAME));

        if let Some(validator_signer) = &self.validator_signer {
            validator_signer.write_to_file(&dir.join(self.config.validator_key_file.clone()));
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

pub const CONFIG_VERSION: u32 = 1;

/// Runtime configuration, defining genesis block.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GenesisConfig {
    /// This is a version of a genesis config structure this version of binary works with.
    /// If the binary tries to load a JSON config with a different version it will panic.
    /// It's not a major protocol version, but used for automatic config migrations using scripts.
    pub config_version: u32,
    /// Protocol version that this genesis works with.
    pub protocol_version: u32,
    /// Official time of blockchain start.
    pub genesis_time: DateTime<Utc>,
    /// ID of the blockchain. This must be unique for every blockchain.
    /// If your testnet blockchains do not have unique chain IDs, you will have a bad time.
    pub chain_id: String,
    /// Number of block producer seats at genesis.
    pub num_block_producer_seats: NumSeats,
    /// Defines number of shards and number of block producer seats per each shard at genesis.
    pub num_block_producer_seats_per_shard: Vec<NumSeats>,
    /// Expected number of hidden validators per shard.
    pub avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    /// Enable dynamic re-sharding.
    pub dynamic_resharding: bool,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    pub min_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: u8,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// Records in storage at genesis (get split into shards at genesis creation).
    pub records: Vec<StateRecord>,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
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
    /// Fishermen stake threshold.
    #[serde(with = "u128_dec_format")]
    pub fishermen_threshold: Balance,
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
            genesis_config.genesis_time,
            genesis_config.gas_limit,
            genesis_config.min_gas_price,
            genesis_config.total_supply,
            genesis_config.max_inflation_rate,
            genesis_config.gas_price_adjustment_rate,
            genesis_config.transaction_validity_period,
            genesis_config.epoch_length,
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

const DEFAULT_TEST_CONTRACT: &'static [u8] =
    include_bytes!("../../runtime/near-vm-runner/tests/res/test_contract_rs.wasm");

impl GenesisConfig {
    fn test_with_seeds(
        seeds: Vec<&str>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        let encoded_test_contract = to_base64(&DEFAULT_TEST_CONTRACT);
        let code_hash = hash(&DEFAULT_TEST_CONTRACT);
        for (i, &account) in seeds.iter().enumerate() {
            let signer = InMemorySigner::from_seed(account, KeyType::ED25519, account);
            let i = i as u64;
            if i < num_validator_seats {
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
                    TESTING_INIT_BALANCE
                        - if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                    if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
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
            config_version: CONFIG_VERSION,
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_block_producer_seats: num_validator_seats,
            num_block_producer_seats_per_shard: num_validator_seats_per_shard.clone(),
            avg_hidden_validator_seats_per_shard: num_validator_seats_per_shard
                .iter()
                .map(|_| 0)
                .collect(),
            dynamic_resharding: false,
            epoch_length: FAST_EPOCH_LENGTH,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
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
            chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
            fishermen_threshold: FISHERMEN_THRESHOLD,
            min_gas_price: MIN_GAS_PRICE,
        }
    }

    pub fn test(seeds: Vec<&str>, num_validator_seats: NumSeats) -> Self {
        Self::test_with_seeds(seeds, num_validator_seats, vec![num_validator_seats])
    }

    pub fn test_free(seeds: Vec<&str>, num_validator_seats: NumSeats) -> Self {
        let mut config =
            Self::test_with_seeds(seeds, num_validator_seats, vec![num_validator_seats]);
        config.runtime_config = RuntimeConfig::free();
        config
    }

    pub fn test_sharded(
        seeds: Vec<&str>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        Self::test_with_seeds(seeds, num_validator_seats, num_validator_seats_per_shard)
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
        let total_supply = get_initial_supply(&config.records);
        config.total_supply = total_supply;
        config
    }
}

fn random_chain_id() -> String {
    format!("test-chain-{}", generate_random_string(5))
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
                let signer =
                    InMemoryValidatorSigner::from_random(account_id.clone(), KeyType::ED25519);
                info!(target: "near", "Use key {} for {} to stake.", signer.public_key(), account_id);
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
                InMemoryValidatorSigner::from_seed(&account_id, KeyType::ED25519, test_seed)
            } else {
                InMemoryValidatorSigner::from_random(account_id.clone(), KeyType::ED25519)
            };
            signer.write_to_file(&dir.join(config.validator_key_file));

            let network_signer = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
            network_signer.write_to_file(&dir.join(config.node_key_file));
            let mut records = state_records_account_with_key(
                &account_id,
                &signer.public_key(),
                TESTING_INIT_BALANCE,
                TESTING_INIT_STAKE,
                CryptoHash::default(),
            );
            add_protocol_account(&mut records);
            let total_supply = get_initial_supply(&records);

            let genesis_config = GenesisConfig {
                protocol_version: PROTOCOL_VERSION,
                config_version: CONFIG_VERSION,
                genesis_time: Utc::now(),
                chain_id,
                num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
                num_block_producer_seats_per_shard: get_num_seats_per_shard(
                    num_shards,
                    NUM_BLOCK_PRODUCER_SEATS,
                ),
                avg_hidden_validator_seats_per_shard: (0..num_shards).map(|_| 0).collect(),
                dynamic_resharding: false,
                epoch_length: if fast { FAST_EPOCH_LENGTH } else { EXPECTED_EPOCH_LENGTH },
                gas_limit: INITIAL_GAS_LIMIT,
                gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
                block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
                runtime_config: Default::default(),
                validators: vec![AccountInfo {
                    account_id: account_id.clone(),
                    public_key: signer.public_key(),
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
                chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
                fishermen_threshold: FISHERMEN_THRESHOLD,
                min_gas_price: MIN_GAS_PRICE,
            };
            genesis_config.write_to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.to_str().unwrap());
        }
    }
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: NumShards,
    num_non_validator_seats: NumSeats,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, GenesisConfig) {
    let num_validator_seats = (seeds.len() - num_non_validator_seats as usize) as NumSeats;
    let validator_signers = seeds
        .iter()
        .map(|seed| InMemoryValidatorSigner::from_seed(seed, KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("", KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let genesis_config = GenesisConfig::test_sharded(
        seeds.iter().map(|s| s.as_str()).collect(),
        num_validator_seats,
        get_num_seats_per_shard(num_shards, num_validator_seats),
    );
    let mut configs = vec![];
    let first_node_port = open_port();
    for i in 0..seeds.len() {
        let mut config = Config::default();
        config.consensus.min_block_production_delay = Duration::from_millis(600);
        config.consensus.max_block_production_delay = Duration::from_millis(2000);
        if local_ports {
            config.network.addr =
                format!("127.0.0.1:{}", if i == 0 { first_node_port } else { open_port() });
            config.rpc.addr = format!("127.0.0.1:{}", open_port());
            config.network.boot_nodes = if i == 0 {
                "".to_string()
            } else {
                format!("{}@127.0.0.1:{}", network_signers[0].public_key, first_node_port)
            };
            config.network.skip_sync_wait = num_validator_seats == 1;
        }
        config.consensus.min_num_peers =
            cmp::min(num_validator_seats as usize - 1, config.consensus.min_num_peers);
        configs.push(config);
    }
    (configs, validator_signers, network_signers, genesis_config)
}

/// Create testnet configuration. If `local_ports` is true,
/// sets up new ports for all nodes except the first one and sets boot node to it.
pub fn create_testnet_configs(
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    local_ports: bool,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, GenesisConfig) {
    create_testnet_configs_from_seeds(
        (0..(num_validator_seats + num_non_validator_seats))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_shards,
        num_non_validator_seats,
        local_ports,
    )
}

pub fn init_testnet_configs(
    dir: &Path,
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
) {
    let (configs, validator_signers, network_signers, genesis_config) = create_testnet_configs(
        num_shards,
        num_validator_seats,
        num_non_validator_seats,
        prefix,
        false,
    );
    for i in 0..(num_validator_seats + num_non_validator_seats) as usize {
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        validator_signers[i].write_to_file(&node_dir.join(configs[i].validator_key_file.clone()));
        network_signers[i].write_to_file(&node_dir.join(configs[i].node_key_file.clone()));

        genesis_config.write_to_file(&node_dir.join(configs[i].genesis_file.clone()));
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME));
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.to_str().unwrap());
    }
}

pub fn load_config(dir: &Path) -> NearConfig {
    let config = Config::from_file(&dir.join(CONFIG_FILENAME));
    let genesis_config = GenesisConfig::from_file(&dir.join(config.genesis_file.clone()));
    let validator_signer = if dir.join(config.validator_key_file.clone()).exists() {
        let signer = Arc::new(InMemoryValidatorSigner::from_file(
            &dir.join(config.validator_key_file.clone()),
        )) as Arc<dyn ValidatorSigner>;
        Some(signer)
    } else {
        None
    };
    let network_signer = InMemorySigner::from_file(&dir.join(config.node_key_file.clone()));
    NearConfig::new(config, &genesis_config, (&network_signer).into(), validator_signer)
}

pub fn load_test_config(seed: &str, port: u16, genesis_config: &GenesisConfig) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = format!("0.0.0.0:{}", port);
    config.rpc.addr = format!("0.0.0.0:{}", open_port());
    config.consensus.min_block_production_delay =
        Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
    config.consensus.max_block_production_delay =
        Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
    let (signer, validator_signer) = if seed.is_empty() {
        let signer = Arc::new(InMemorySigner::from_random("".to_string(), KeyType::ED25519));
        (signer, None)
    } else {
        let signer = Arc::new(InMemorySigner::from_seed(seed, KeyType::ED25519, seed));
        let validator_signer =
            Arc::new(InMemoryValidatorSigner::from_seed(seed, KeyType::ED25519, seed))
                as Arc<dyn ValidatorSigner>;
        (signer, Some(validator_signer))
    };
    NearConfig::new(config, &genesis_config, signer.into(), validator_signer)
}

#[cfg(test)]
mod test {
    use super::*;

    /// make sure testnet genesis can be deserialized
    #[test]
    fn test_deserialize_state() {
        let genesis_config = testnet_genesis();
        assert_eq!(genesis_config.protocol_version, PROTOCOL_VERSION);
        assert_eq!(genesis_config.config_version, CONFIG_VERSION);
        assert!(genesis_config.total_supply > 0);
    }
}
