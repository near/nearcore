use anyhow::{anyhow, bail, Context};
use near_primitives::static_clock::StaticClock;
use near_primitives::test_utils::create_test_signer;
use num_rational::Rational32;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use near_config_utils::{ValidationError, ValidationErrors};

#[cfg(test)]
use tempfile::tempdir;
use tracing::{info, warn};

use crate::download_file::{run_download_file, FileDownloadError};
use near_chain_configs::{
    get_initial_supply, ClientConfig, GCConfig, Genesis, GenesisConfig, GenesisValidationMode,
    LogSummaryStyle, MutableConfigValue,
};
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, Signer};
#[cfg(feature = "json_rpc")]
use near_jsonrpc::RpcConfig;
use near_network::config::NetworkConfig;
use near_network::tcp;
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
#[cfg(test)]
use near_primitives::shard_layout::account_id_to_shard_id;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, EpochHeight, Gas, NumBlocks,
    NumSeats, NumShards, ShardId,
};
use near_primitives::utils::{generate_random_string, get_num_seats_per_shard};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::RosettaRpcConfig;
use near_telemetry::TelemetryConfig;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

/// Millinear, 1/1000 of NEAR.
pub const MILLI_NEAR: Balance = NEAR_BASE / 1000;

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
const CHUNK_REQUEST_RETRY_PERIOD: u64 = 400;

/// Expected epoch length.
pub const EXPECTED_EPOCH_LENGTH: BlockHeightDelta = (5 * 60 * 1000) / MIN_BLOCK_PRODUCTION_DELAY;

/// Criterion for kicking out block producers.
pub const BLOCK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Criterion for kicking out chunk producers.
pub const CHUNK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Fast mode constants for testing/developing.
pub const FAST_MIN_BLOCK_PRODUCTION_DELAY: u64 = 120;
pub const FAST_MAX_BLOCK_PRODUCTION_DELAY: u64 = 500;
pub const FAST_EPOCH_LENGTH: BlockHeightDelta = 60;

/// Expected number of blocks per year
pub const NUM_BLOCKS_PER_YEAR: u64 = 365 * 24 * 60 * 60;

/// Initial gas limit.
pub const INITIAL_GAS_LIMIT: Gas = 1_000_000_000_000_000;

/// Initial and minimum gas price.
pub const MIN_GAS_PRICE: Balance = 100_000_000;

/// Protocol treasury account
pub const PROTOCOL_TREASURY_ACCOUNT: &str = "near";

/// Fishermen stake threshold.
pub const FISHERMEN_THRESHOLD: Balance = 10 * NEAR_BASE;

/// Number of blocks for which a given transaction is valid
pub const TRANSACTION_VALIDITY_PERIOD: NumBlocks = 100;

/// Number of seats for block producers
pub const NUM_BLOCK_PRODUCER_SEATS: NumSeats = 50;

/// The minimum stake required for staking is last seat price divided by this number.
pub const MINIMUM_STAKE_DIVISOR: u64 = 10;

/// Number of epochs before protocol upgrade.
pub const PROTOCOL_UPGRADE_NUM_EPOCHS: EpochHeight = 2;

pub const CONFIG_FILENAME: &str = "config.json";
pub const GENESIS_CONFIG_FILENAME: &str = "genesis.json";
pub const NODE_KEY_FILE: &str = "node_key.json";
pub const VALIDATOR_KEY_FILE: &str = "validator_key.json";

pub const MAINNET: &str = "mainnet";
pub const TESTNET: &str = "testnet";
pub const BETANET: &str = "betanet";

pub const MAINNET_TELEMETRY_URL: &str = "https://explorer.mainnet.near.org/api/nodes";
pub const NETWORK_TELEMETRY_URL: &str = "https://explorer.{}.near.org/api/nodes";

/// The rate at which the gas price can be adjusted (alpha in the formula).
/// The formula is
/// gas_price_t = gas_price_{t-1} * (1 + (gas_used/gas_limit - 1/2) * alpha))
pub const GAS_PRICE_ADJUSTMENT_RATE: Rational32 = Rational32::new_raw(1, 100);

/// Protocol treasury reward
pub const PROTOCOL_REWARD_RATE: Rational32 = Rational32::new_raw(1, 10);

/// Maximum inflation rate per year
pub const MAX_INFLATION_RATE: Rational32 = Rational32::new_raw(1, 20);

/// Protocol upgrade stake threshold.
pub const PROTOCOL_UPGRADE_STAKE_THRESHOLD: Rational32 = Rational32::new_raw(4, 5);

/// Serde default only supports functions without parameters.
fn default_reduce_wait_for_missing_block() -> Duration {
    Duration::from_millis(REDUCE_DELAY_FOR_MISSING_BLOCKS)
}

fn default_header_sync_initial_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_header_sync_progress_timeout() -> Duration {
    Duration::from_secs(2)
}

fn default_header_sync_stall_ban_timeout() -> Duration {
    Duration::from_secs(120)
}

fn default_state_sync_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_header_sync_expected_height_per_second() -> u64 {
    10
}

fn default_sync_check_period() -> Duration {
    Duration::from_secs(10)
}

fn default_sync_step_period() -> Duration {
    Duration::from_millis(10)
}

fn default_sync_height_threshold() -> u64 {
    1
}

fn default_view_client_threads() -> usize {
    4
}

fn default_doomslug_step_period() -> Duration {
    Duration::from_millis(100)
}

fn default_view_client_throttle_period() -> Duration {
    Duration::from_secs(30)
}

fn default_trie_viewer_state_size_limit() -> Option<u64> {
    Some(50_000)
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
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
    /// How much time to wait after initial header sync
    #[serde(default = "default_header_sync_initial_timeout")]
    pub header_sync_initial_timeout: Duration,
    /// How much time to wait after some progress is made in header sync
    #[serde(default = "default_header_sync_progress_timeout")]
    pub header_sync_progress_timeout: Duration,
    /// How much time to wait before banning a peer in header sync if sync is too slow
    #[serde(default = "default_header_sync_stall_ban_timeout")]
    pub header_sync_stall_ban_timeout: Duration,
    /// How much to wait for a state sync response before re-requesting
    #[serde(default = "default_state_sync_timeout")]
    pub state_sync_timeout: Duration,
    /// Expected increase of header head weight per second during header sync
    #[serde(default = "default_header_sync_expected_height_per_second")]
    pub header_sync_expected_height_per_second: u64,
    /// How frequently we check whether we need to sync
    #[serde(default = "default_sync_check_period")]
    pub sync_check_period: Duration,
    /// During sync the time we wait before reentering the sync loop
    #[serde(default = "default_sync_step_period")]
    pub sync_step_period: Duration,
    /// Time between running doomslug timer.
    #[serde(default = "default_doomslug_step_period")]
    pub doomslug_step_period: Duration,
    #[serde(default = "default_sync_height_threshold")]
    pub sync_height_threshold: u64,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            block_production_tracking_delay: Duration::from_millis(BLOCK_PRODUCTION_TRACKING_DELAY),
            min_block_production_delay: Duration::from_millis(MIN_BLOCK_PRODUCTION_DELAY),
            max_block_production_delay: Duration::from_millis(MAX_BLOCK_PRODUCTION_DELAY),
            max_block_wait_delay: Duration::from_millis(MAX_BLOCK_WAIT_DELAY),
            reduce_wait_for_missing_block: default_reduce_wait_for_missing_block(),
            produce_empty_blocks: true,
            block_fetch_horizon: BLOCK_FETCH_HORIZON,
            state_fetch_horizon: STATE_FETCH_HORIZON,
            block_header_fetch_horizon: BLOCK_HEADER_FETCH_HORIZON,
            catchup_step_period: Duration::from_millis(CATCHUP_STEP_PERIOD),
            chunk_request_retry_period: Duration::from_millis(CHUNK_REQUEST_RETRY_PERIOD),
            header_sync_initial_timeout: default_header_sync_initial_timeout(),
            header_sync_progress_timeout: default_header_sync_progress_timeout(),
            header_sync_stall_ban_timeout: default_header_sync_stall_ban_timeout(),
            state_sync_timeout: default_state_sync_timeout(),
            header_sync_expected_height_per_second: default_header_sync_expected_height_per_second(
            ),
            sync_check_period: default_sync_check_period(),
            sync_step_period: default_sync_step_period(),
            doomslug_step_period: default_doomslug_step_period(),
            sync_height_threshold: default_sync_height_threshold(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    pub genesis_file: String,
    pub genesis_records_file: Option<String>,
    pub validator_key_file: String,
    pub node_key_file: String,
    #[cfg(feature = "json_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rosetta_rpc: Option<RosettaRpcConfig>,
    pub telemetry: TelemetryConfig,
    pub network: near_network::config_json::Config,
    pub consensus: Consensus,
    pub tracked_accounts: Vec<AccountId>,
    pub tracked_shards: Vec<ShardId>,
    #[serde(skip_serializing_if = "is_false")]
    pub archive: bool,
    /// If save_trie_changes is not set it will get inferred from the `archive` field as follows:
    /// save_trie_changes = !archive
    /// save_trie_changes should be set to true iff
    /// - archive if false - non-archival nodes need trie changes to perform garbage collection
    /// - archive is true and cold_store is configured - node working in split storage mode
    /// needs trie changes in order to do garbage collection on hot and populate cold State column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub save_trie_changes: Option<bool>,
    pub log_summary_style: LogSummaryStyle,
    /// Garbage collection configuration.
    #[serde(default, flatten)]
    pub gc: GCConfig,
    #[serde(default = "default_view_client_threads")]
    pub view_client_threads: usize,
    pub epoch_sync_enabled: bool,
    #[serde(default = "default_view_client_throttle_period")]
    pub view_client_throttle_period: Duration,
    #[serde(default = "default_trie_viewer_state_size_limit")]
    pub trie_viewer_state_size_limit: Option<u64>,
    /// If set, overrides value in genesis configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_gas_burnt_view: Option<Gas>,
    /// Different parameters to configure underlying storage.
    pub store: near_store::StoreConfig,
    /// Different parameters to configure underlying cold storage.
    /// This feature is under development, do not use in production.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_store: Option<near_store::StoreConfig>,
    /// Configuration for the
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_storage: Option<SplitStorageConfig>,
    // TODO(mina86): Remove those two altogether at some point.  We need to be
    // somewhat careful though and make sure that we don’t start silently
    // ignoring this option without users setting corresponding store option.
    // For the time being, we’re failing inside of create_db_checkpoint if this
    // option is set.
    /// Deprecated; use `store.migration_snapshot` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_db_migration_snapshot: Option<bool>,
    /// Deprecated; use `store.migration_snapshot` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_migration_snapshot_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_shutdown: Option<BlockHeight>,
    /// Options for dumping state of every epoch to S3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_sync: Option<StateSyncConfig>,
}

fn is_false(value: &bool) -> bool {
    !*value
}
impl Default for Config {
    fn default() -> Self {
        Config {
            genesis_file: GENESIS_CONFIG_FILENAME.to_string(),
            genesis_records_file: None,
            validator_key_file: VALIDATOR_KEY_FILE.to_string(),
            node_key_file: NODE_KEY_FILE.to_string(),
            #[cfg(feature = "json_rpc")]
            rpc: Some(RpcConfig::default()),
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc: None,
            telemetry: TelemetryConfig::default(),
            network: Default::default(),
            consensus: Consensus::default(),
            tracked_accounts: vec![],
            tracked_shards: vec![],
            archive: false,
            save_trie_changes: None,
            log_summary_style: LogSummaryStyle::Colored,
            gc: GCConfig::default(),
            epoch_sync_enabled: true,
            view_client_threads: default_view_client_threads(),
            view_client_throttle_period: default_view_client_throttle_period(),
            trie_viewer_state_size_limit: default_trie_viewer_state_size_limit(),
            max_gas_burnt_view: None,
            db_migration_snapshot_path: None,
            use_db_migration_snapshot: None,
            store: near_store::StoreConfig::default(),
            cold_store: None,
            split_storage: None,
            expected_shutdown: None,
            state_sync: None,
        }
    }
}

fn default_enable_split_storage_view_client() -> bool {
    false
}

fn default_cold_store_initial_migration_batch_size() -> usize {
    500_000_000
}

fn default_cold_store_initial_migration_loop_sleep_duration() -> Duration {
    Duration::from_secs(30)
}

fn default_cold_store_loop_sleep_duration() -> Duration {
    Duration::from_secs(1)
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SplitStorageConfig {
    #[serde(default = "default_enable_split_storage_view_client")]
    pub enable_split_storage_view_client: bool,

    #[serde(default = "default_cold_store_initial_migration_batch_size")]
    pub cold_store_initial_migration_batch_size: usize,
    #[serde(default = "default_cold_store_initial_migration_loop_sleep_duration")]
    pub cold_store_initial_migration_loop_sleep_duration: Duration,

    #[serde(default = "default_cold_store_loop_sleep_duration")]
    pub cold_store_loop_sleep_duration: Duration,
}

impl Default for SplitStorageConfig {
    fn default() -> Self {
        SplitStorageConfig {
            enable_split_storage_view_client: default_enable_split_storage_view_client(),
            cold_store_initial_migration_batch_size:
                default_cold_store_initial_migration_batch_size(),
            cold_store_initial_migration_loop_sleep_duration:
                default_cold_store_initial_migration_loop_sleep_duration(),
            cold_store_loop_sleep_duration: default_cold_store_loop_sleep_duration(),
        }
    }
}

impl Config {
    /// load Config from config.json without panic. Do semantic validation on field values.
    /// If config file issues occur, a ValidationError::ConfigFileError will be returned;
    /// If config semantic checks failed, a ValidationError::ConfigSemanticError will be returned
    pub fn from_file(path: &Path) -> Result<Self, ValidationError> {
        Self::from_file_skip_validation(path).and_then(|config| {
            config.validate()?;
            Ok(config)
        })
    }

    /// load Config from config.json without panic.
    /// Skips semantic validation on field values.
    /// This function should only return error for file issues.
    pub fn from_file_skip_validation(path: &Path) -> Result<Self, ValidationError> {
        let json_str =
            std::fs::read_to_string(path).map_err(|_| ValidationError::ConfigFileError {
                error_message: format!("Failed to read config from {}", path.display()),
            })?;
        let mut unrecognised_fields = Vec::new();
        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)
            .map_err(|_| ValidationError::ConfigFileError {
                error_message: format!("Failed to strip comments from {}", path.display()),
            })?;
        let config: Config = serde_ignored::deserialize(
            &mut serde_json::Deserializer::from_str(&json_str_without_comments),
            |field| {
                let field = field.to_string();
                // TODO(mina86): Remove this deprecation notice some time by the
                // end of 2022.
                if field == "network.external_address" {
                    warn!(
                        target: "neard",
                        "{}: {field} is deprecated; please remove it from the config file",
                        path.display(),
                    );
                } else {
                    unrecognised_fields.push(field);
                }
            },
        )
        .map_err(|_| ValidationError::ConfigFileError {
            error_message: format!("Failed to deserialize config from {}", path.display()),
        })?;

        if !unrecognised_fields.is_empty() {
            let s = if unrecognised_fields.len() > 1 { "s" } else { "" };
            let fields = unrecognised_fields.join(", ");
            warn!(
                target: "neard",
                "{}: encountered unrecognised field{s}: {fields}",
                path.display(),
            );
        }

        Ok(config)
    }

    fn validate(&self) -> Result<(), ValidationError> {
        crate::config_validate::validate_config(self)
    }

    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn rpc_addr(&self) -> Option<String> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc {
            return Some(rpc.addr.to_string());
        }
        None
    }

    #[allow(unused_variables)]
    pub fn set_rpc_addr(&mut self, addr: tcp::ListenerAddr) {
        #[cfg(feature = "json_rpc")]
        {
            self.rpc.get_or_insert(Default::default()).addr = addr;
        }
    }
}

#[easy_ext::ext(GenesisExt)]
impl Genesis {
    // Creates new genesis with a given set of accounts and shard layout.
    // The first num_validator_seats from accounts will be treated as 'validators'.
    pub fn test_with_seeds(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
        shard_layout: ShardLayout,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        for (i, account) in accounts.into_iter().enumerate() {
            let signer =
                InMemorySigner::from_seed(account.clone(), KeyType::ED25519, account.as_ref());
            let i = i as u64;
            if i < num_validator_seats {
                validators.push(AccountInfo {
                    account_id: account.clone(),
                    public_key: signer.public_key.clone(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            add_account_with_key(
                &mut records,
                account,
                &signer.public_key.clone(),
                TESTING_INIT_BALANCE - if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                CryptoHash::default(),
            );
        }
        add_protocol_account(&mut records);
        let config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time: StaticClock::utc(),
            chain_id: random_chain_id(),
            num_block_producer_seats: num_validator_seats,
            num_block_producer_seats_per_shard: num_validator_seats_per_shard.clone(),
            avg_hidden_validator_seats_per_shard: vec![0; num_validator_seats_per_shard.len()],
            dynamic_resharding: false,
            protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
            protocol_upgrade_num_epochs: PROTOCOL_UPGRADE_NUM_EPOCHS,
            epoch_length: FAST_EPOCH_LENGTH,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
            validators,
            protocol_reward_rate: PROTOCOL_REWARD_RATE,
            total_supply: get_initial_supply(&records),
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
            fishermen_threshold: FISHERMEN_THRESHOLD,
            min_gas_price: MIN_GAS_PRICE,
            shard_layout,
            ..Default::default()
        };
        Genesis::new(config, records.into()).unwrap()
    }

    pub fn test(accounts: Vec<AccountId>, num_validator_seats: NumSeats) -> Self {
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            vec![num_validator_seats],
            ShardLayout::v0_single_shard(),
        )
    }

    pub fn test_sharded(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 0),
        )
    }

    pub fn test_sharded_new_version(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 1),
        )
    }
}

#[derive(Clone)]
pub struct NearConfig {
    pub config: Config,
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    #[cfg(feature = "json_rpc")]
    pub rpc_config: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    pub rosetta_rpc_config: Option<RosettaRpcConfig>,
    pub telemetry_config: TelemetryConfig,
    pub genesis: Genesis,
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis: Genesis,
        network_key_pair: KeyFile,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
    ) -> anyhow::Result<Self> {
        Ok(NearConfig {
            config: config.clone(),
            client_config: ClientConfig {
                version: Default::default(),
                chain_id: genesis.config.chain_id.clone(),
                rpc_addr: config.rpc_addr().map(|addr| addr),
                expected_shutdown: MutableConfigValue::new(
                    config.expected_shutdown,
                    "expected_shutdown",
                ),
                block_production_tracking_delay: config.consensus.block_production_tracking_delay,
                min_block_production_delay: config.consensus.min_block_production_delay,
                max_block_production_delay: config.consensus.max_block_production_delay,
                max_block_wait_delay: config.consensus.max_block_wait_delay,
                reduce_wait_for_missing_block: config.consensus.reduce_wait_for_missing_block,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: config.consensus.sync_check_period,
                sync_step_period: config.consensus.sync_step_period,
                sync_height_threshold: config.consensus.sync_height_threshold,
                header_sync_initial_timeout: config.consensus.header_sync_initial_timeout,
                header_sync_progress_timeout: config.consensus.header_sync_progress_timeout,
                header_sync_stall_ban_timeout: config.consensus.header_sync_stall_ban_timeout,
                header_sync_expected_height_per_second: config
                    .consensus
                    .header_sync_expected_height_per_second,
                state_sync_timeout: config.consensus.state_sync_timeout,
                min_num_peers: config.consensus.min_num_peers,
                log_summary_period: Duration::from_secs(10),
                produce_empty_blocks: config.consensus.produce_empty_blocks,
                epoch_length: genesis.config.epoch_length,
                num_block_producer_seats: genesis.config.num_block_producer_seats,
                announce_account_horizon: genesis.config.epoch_length / 2,
                ttl_account_id_router: config.network.ttl_account_id_router,
                // TODO(1047): this should be adjusted depending on the speed of sync of state.
                block_fetch_horizon: config.consensus.block_fetch_horizon,
                state_fetch_horizon: config.consensus.state_fetch_horizon,
                block_header_fetch_horizon: config.consensus.block_header_fetch_horizon,
                catchup_step_period: config.consensus.catchup_step_period,
                chunk_request_retry_period: config.consensus.chunk_request_retry_period,
                doosmslug_step_period: config.consensus.doomslug_step_period,
                tracked_accounts: config.tracked_accounts,
                tracked_shards: config.tracked_shards,
                archive: config.archive,
                save_trie_changes: config.save_trie_changes.unwrap_or(!config.archive),
                log_summary_style: config.log_summary_style,
                gc: config.gc,
                view_client_threads: config.view_client_threads,
                epoch_sync_enabled: config.epoch_sync_enabled,
                view_client_throttle_period: config.view_client_throttle_period,
                trie_viewer_state_size_limit: config.trie_viewer_state_size_limit,
                max_gas_burnt_view: config.max_gas_burnt_view,
                enable_statistics_export: config.store.enable_statistics_export,
                client_background_migration_threads: config.store.background_migration_threads,
                flat_storage_creation_period: config.store.flat_storage_creation_period,
                state_sync_dump_enabled: config
                    .state_sync
                    .as_ref()
                    .map_or(false, |x| x.dump_enabled.unwrap_or(false)),
                state_sync_s3_bucket: config
                    .state_sync
                    .as_ref()
                    .map_or(String::new(), |x| x.s3_bucket.clone()),
                state_sync_s3_region: config
                    .state_sync
                    .as_ref()
                    .map_or(String::new(), |x| x.s3_region.clone()),
                state_sync_restart_dump_for_shards: config
                    .state_sync
                    .as_ref()
                    .map_or(vec![], |x| x.drop_state_of_dump.clone().unwrap_or(vec![])),
            },
            network_config: NetworkConfig::new(
                config.network,
                network_key_pair.secret_key,
                validator_signer.clone(),
                config.archive,
            )?,
            telemetry_config: config.telemetry,
            #[cfg(feature = "json_rpc")]
            rpc_config: config.rpc,
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc_config: config.rosetta_rpc,
            genesis,
            validator_signer,
        })
    }

    pub fn rpc_addr(&self) -> Option<String> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc_config {
            return Some(rpc.addr.to_string());
        }
        None
    }
}

impl NearConfig {
    /// Test tool to save configs back to the folder.
    /// Useful for dynamic creating testnet configs and then saving them in different folders.
    pub fn save_to_dir(&self, dir: &Path) {
        fs::create_dir_all(dir).expect("Failed to create directory");

        self.config.write_to_file(&dir.join(CONFIG_FILENAME)).expect("Error writing config");

        if let Some(validator_signer) = &self.validator_signer {
            validator_signer
                .write_to_file(&dir.join(&self.config.validator_key_file))
                .expect("Error writing validator key file");
        }

        let network_signer = InMemorySigner::from_secret_key(
            "node".parse().unwrap(),
            self.network_config.node_key.clone(),
        );
        network_signer
            .write_to_file(&dir.join(&self.config.node_key_file))
            .expect("Error writing key file");

        self.genesis.to_file(&dir.join(&self.config.genesis_file));
    }
}

fn add_protocol_account(records: &mut Vec<StateRecord>) {
    let signer = InMemorySigner::from_seed(
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        KeyType::ED25519,
        PROTOCOL_TREASURY_ACCOUNT,
    );
    add_account_with_key(
        records,
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        &signer.public_key,
        TESTING_INIT_BALANCE,
        0,
        CryptoHash::default(),
    );
}

fn random_chain_id() -> String {
    format!("test-chain-{}", generate_random_string(5))
}

fn add_account_with_key(
    records: &mut Vec<StateRecord>,
    account_id: AccountId,
    public_key: &PublicKey,
    amount: u128,
    staked: u128,
    code_hash: CryptoHash,
) {
    records.push(StateRecord::Account {
        account_id: account_id.clone(),
        account: Account::new(amount, staked, code_hash, 0),
    });
    records.push(StateRecord::AccessKey {
        account_id,
        public_key: public_key.clone(),
        access_key: AccessKey::full_access(),
    });
}

/// Generates or loads a signer key from given file.
///
/// If the file already exists, loads the file (panicking if the file is
/// invalid), checks that account id in the file matches `account_id` if it’s
/// given and returns the key.  `test_seed` is ignored in this case.
///
/// If the file does not exist and `account_id` is not `None`, generates a new
/// key, saves it in the file and returns it.  If `test_seed` is not `None`, the
/// key generation algorithm is seeded with given string making it fully
/// deterministic.
fn generate_or_load_key(
    home_dir: &Path,
    filename: &str,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
) -> anyhow::Result<Option<InMemorySigner>> {
    let path = home_dir.join(filename);
    if path.exists() {
        let signer = InMemorySigner::from_file(&path)
            .with_context(|| format!("Failed initializing signer from {}", path.display()))?;
        if let Some(account_id) = account_id {
            if account_id != signer.account_id {
                return Err(anyhow!(
                    "‘{}’ contains key for {} but expecting key for {}",
                    path.display(),
                    signer.account_id,
                    account_id
                ));
            }
        }
        info!(target: "near", "Reusing key {} for {}", signer.public_key(), signer.account_id);
        Ok(Some(signer))
    } else if let Some(account_id) = account_id {
        let signer = if let Some(seed) = test_seed {
            InMemorySigner::from_seed(account_id, KeyType::ED25519, seed)
        } else {
            InMemorySigner::from_random(account_id, KeyType::ED25519)
        };
        info!(target: "near", "Using key {} for {}", signer.public_key(), signer.account_id);
        signer
            .write_to_file(&path)
            .with_context(|| anyhow!("Failed saving key to ‘{}’", path.display()))?;
        Ok(Some(signer))
    } else {
        Ok(None)
    }
}

#[test]
fn test_generate_or_load_key() {
    let tmp = tempfile::tempdir().unwrap();
    let home_dir = tmp.path();

    let gen = move |filename: &str, account: &str, seed: &str| {
        generate_or_load_key(
            home_dir,
            filename,
            if account.is_empty() { None } else { Some(account.parse().unwrap()) },
            if seed.is_empty() { None } else { Some(seed) },
        )
    };

    let test_ok = |filename: &str, account: &str, seed: &str| {
        let result = gen(filename, account, seed);
        let key = result.unwrap().unwrap();
        assert!(home_dir.join("key").exists());
        if !account.is_empty() {
            assert_eq!(account, key.account_id.as_str());
        }
        key
    };

    let test_err = |filename: &str, account: &str, seed: &str| {
        let result = gen(filename, account, seed);
        assert!(result.is_err());
    };

    // account_id == None → do nothing, return None
    assert!(generate_or_load_key(home_dir, "key", None, None).unwrap().is_none());
    assert!(!home_dir.join("key").exists());

    // account_id == Some, file doesn’t exist → create new key
    let key = test_ok("key", "fred", "");

    // file exists → load key, compare account if given
    assert!(key == test_ok("key", "", ""));
    assert!(key == test_ok("key", "fred", ""));
    test_err("key", "barney", "");

    // test_seed == Some → the same key is generated
    let k1 = test_ok("k1", "fred", "foo");
    let k2 = test_ok("k2", "barney", "foo");
    let k3 = test_ok("k3", "fred", "bar");

    assert!(k1.public_key == k2.public_key && k1.secret_key == k2.secret_key);
    assert!(k1 != k3);

    // file contains invalid JSON -> should return an error
    {
        let mut file = std::fs::File::create(&home_dir.join("bad_key")).unwrap();
        writeln!(file, "not JSON").unwrap();
    }
    test_err("bad_key", "fred", "");
}

/// Checks that validator and node keys exist.
/// If a key is needed and doesn't exist, then it gets created.
fn generate_or_load_keys(
    dir: &Path,
    config: &Config,
    chain_id: &str,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
) -> anyhow::Result<()> {
    generate_or_load_key(dir, &config.node_key_file, Some("node".parse().unwrap()), None)?;
    match chain_id {
        MAINNET | TESTNET | BETANET => {
            generate_or_load_key(dir, &config.validator_key_file, account_id, None)?;
        }
        _ => {
            let account_id = account_id.unwrap_or_else(|| "test.near".parse().unwrap());
            generate_or_load_key(dir, &config.validator_key_file, Some(account_id), test_seed)?;
        }
    }
    Ok(())
}

/// Initializes Genesis, client Config, node and validator keys, and stores in the specified folder.
///
/// This method supports the following use cases:
/// * When no Config, Genesis or key files exist.
/// * When Config and Genesis files exist, but no keys exist.
/// * When all of Config, Genesis, and key files exist.
///
/// Note that this method does not support the case where the configuration file exists but the genesis file does not exist.
pub fn init_configs(
    dir: &Path,
    chain_id: Option<String>,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
    num_shards: NumShards,
    fast: bool,
    genesis: Option<&str>,
    should_download_genesis: bool,
    download_genesis_url: Option<&str>,
    download_records_url: Option<&str>,
    should_download_config: bool,
    download_config_url: Option<&str>,
    boot_nodes: Option<&str>,
    max_gas_burnt_view: Option<Gas>,
) -> anyhow::Result<()> {
    fs::create_dir_all(dir).with_context(|| anyhow!("Failed to create directory {:?}", dir))?;

    assert_ne!(chain_id, Some("".to_string()));
    let chain_id = match chain_id {
        Some(chain_id) => chain_id,
        None => random_chain_id(),
    };

    // Check if config already exists in home dir.
    if dir.join(CONFIG_FILENAME).exists() {
        let config = Config::from_file(&dir.join(CONFIG_FILENAME))
            .with_context(|| anyhow!("Failed to read config {}", dir.display()))?;
        let genesis_file = config.genesis_file.clone();
        let file_path = dir.join(&genesis_file);
        // Check that Genesis exists and can be read.
        // If `config.json` exists, but `genesis.json` doesn't exist,
        // that isn't supported by the `init` command.
        let _genesis = GenesisConfig::from_file(&file_path).with_context(move || {
            anyhow!("Failed to read genesis config {}/{}", dir.display(), genesis_file)
        })?;
        // Check that `node_key.json` and `validator_key.json` exist.
        // Create if needed and they don't exist.
        generate_or_load_keys(dir, &config, &chain_id, account_id, test_seed)?;
        return Ok(());
    }

    let mut config = Config::default();

    if let Some(url) = download_config_url {
        download_config(&url.to_string(), &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    } else if should_download_config {
        let url = get_config_url(&chain_id);
        download_config(&url, &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    }

    if let Some(nodes) = boot_nodes {
        config.network.boot_nodes = nodes.to_string();
    }

    if max_gas_burnt_view.is_some() {
        config.max_gas_burnt_view = max_gas_burnt_view;
    }

    // Before finalizing the Config and Genesis, make sure the node and validator keys exist.
    generate_or_load_keys(dir, &config, &chain_id, account_id, test_seed)?;
    match chain_id.as_ref() {
        MAINNET => {
            if test_seed.is_some() {
                bail!("Test seed is not supported for {chain_id}");
            }

            // Make sure node tracks all shards, see
            // https://github.com/near/nearcore/issues/7388
            config.tracked_shards = vec![0];

            config.telemetry.endpoints.push(MAINNET_TELEMETRY_URL.to_string());
            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            let genesis = near_mainnet_res::mainnet_genesis();

            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated mainnet genesis file in {}", dir.display());
        }
        TESTNET | BETANET => {
            if test_seed.is_some() {
                bail!("Test seed is not supported for {chain_id}");
            }

            // Make sure node tracks all shards, see
            // https://github.com/near/nearcore/issues/7388
            config.tracked_shards = vec![0];

            config.telemetry.endpoints.push(NETWORK_TELEMETRY_URL.replace("{}", &chain_id));
            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            if let Some(ref filename) = config.genesis_records_file {
                let records_path = dir.join(filename);

                if let Some(url) = download_records_url {
                    download_records(&url.to_string(), &records_path)
                        .context(format!("Failed to download the records file from {}", url))?;
                } else if should_download_genesis {
                    let url = get_records_url(&chain_id);
                    download_records(&url, &records_path)
                        .context(format!("Failed to download the records file from {}", url))?;
                }
            }

            // download genesis from s3
            let genesis_path = dir.join("genesis.json");
            let mut genesis_path_str =
                genesis_path.to_str().with_context(|| "Genesis path must be initialized")?;

            if let Some(url) = download_genesis_url {
                download_genesis(&url.to_string(), &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else if should_download_genesis {
                let url = get_genesis_url(&chain_id);
                download_genesis(&url, &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else {
                genesis_path_str = match genesis {
                    Some(g) => g,
                    None => {
                        bail!(
                            "Genesis file is required for {}.\
                             Use <--genesis|--download-genesis>",
                            &chain_id
                        );
                    }
                };
            }

            let mut genesis = match &config.genesis_records_file {
                Some(records_file) => {
                    let records_path = dir.join(records_file);
                    let records_path_str = records_path
                        .to_str()
                        .with_context(|| "Records path must be initialized")?;
                    Genesis::from_files(
                        &genesis_path_str,
                        &records_path_str,
                        GenesisValidationMode::Full,
                    )
                }
                None => Genesis::from_file(&genesis_path_str, GenesisValidationMode::Full),
            }?;

            genesis.config.chain_id = chain_id.clone();

            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated for {} network node key and genesis file in {}", chain_id, dir.display());
        }
        _ => {
            // Create new configuration, key files and genesis for one validator.
            config.network.skip_sync_wait = true;
            if fast {
                config.consensus.min_block_production_delay =
                    Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
                config.consensus.max_block_production_delay =
                    Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
            }

            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            let validator_file = dir.join(&config.validator_key_file);
            let signer = InMemorySigner::from_file(&validator_file).unwrap();

            let mut records = vec![];
            add_account_with_key(
                &mut records,
                signer.account_id.clone(),
                &signer.public_key(),
                TESTING_INIT_BALANCE,
                TESTING_INIT_STAKE,
                CryptoHash::default(),
            );
            add_protocol_account(&mut records);
            let shards = if num_shards > 1 {
                ShardLayout::v1(
                    (0..num_shards - 1)
                        .map(|f| {
                            AccountId::from_str(format!("shard{}.test.near", f).as_str()).unwrap()
                        })
                        .collect(),
                    vec![],
                    None,
                    1,
                )
            } else {
                ShardLayout::v0_single_shard()
            };

            let genesis_config = GenesisConfig {
                protocol_version: PROTOCOL_VERSION,
                genesis_time: StaticClock::utc(),
                chain_id,
                genesis_height: 0,
                num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
                num_block_producer_seats_per_shard: get_num_seats_per_shard(
                    num_shards,
                    NUM_BLOCK_PRODUCER_SEATS,
                ),
                avg_hidden_validator_seats_per_shard: (0..num_shards).map(|_| 0).collect(),
                dynamic_resharding: false,
                protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
                protocol_upgrade_num_epochs: PROTOCOL_UPGRADE_NUM_EPOCHS,
                epoch_length: if fast { FAST_EPOCH_LENGTH } else { EXPECTED_EPOCH_LENGTH },
                gas_limit: INITIAL_GAS_LIMIT,
                gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
                block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
                chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
                online_max_threshold: Rational32::new(99, 100),
                online_min_threshold: Rational32::new(BLOCK_PRODUCER_KICKOUT_THRESHOLD as i32, 100),
                validators: vec![AccountInfo {
                    account_id: signer.account_id.clone(),
                    public_key: signer.public_key(),
                    amount: TESTING_INIT_STAKE,
                }],
                transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
                protocol_reward_rate: PROTOCOL_REWARD_RATE,
                max_inflation_rate: MAX_INFLATION_RATE,
                total_supply: get_initial_supply(&records),
                num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
                protocol_treasury_account: signer.account_id,
                fishermen_threshold: FISHERMEN_THRESHOLD,
                shard_layout: shards,
                min_gas_price: MIN_GAS_PRICE,
                ..Default::default()
            };
            let genesis = Genesis::new(genesis_config, records.into())?;
            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.display());
        }
    }
    Ok(())
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: NumShards,
    num_non_validator_seats: NumSeats,
    local_ports: bool,
    archive: bool,
    fixed_shards: Option<Vec<String>>,
    tracked_shards: Vec<u64>,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis) {
    let num_validator_seats = (seeds.len() - num_non_validator_seats as usize) as NumSeats;
    let validator_signers =
        seeds.iter().map(|seed| create_test_signer(seed.as_str())).collect::<Vec<_>>();
    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("node".parse().unwrap(), KeyType::ED25519, seed))
        .collect::<Vec<_>>();

    let shard_layout = if let Some(ref fixed_shards) = fixed_shards {
        // If fixed shards are set, we expect that they take over all the shards (except for one that would host all the other accounts).
        assert!(fixed_shards.len() == num_shards as usize - 1);
        ShardLayout::v1(
            fixed_shards.iter().map(|it| it.parse().unwrap()).collect(),
            vec![],
            None,
            0,
        )
    } else {
        ShardLayout::v0(num_shards, 0)
    };
    let mut accounts_to_add_to_genesis: Vec<AccountId> =
        seeds.iter().map(|s| s.parse().unwrap()).collect();

    // If we have fixed shards - let's also add those accounts to genesis.
    if let Some(ref fixed_shards_accounts) = fixed_shards {
        accounts_to_add_to_genesis.extend(
            fixed_shards_accounts.iter().map(|s| s.parse().unwrap()).collect::<Vec<AccountId>>(),
        );
    };
    let genesis = Genesis::test_with_seeds(
        accounts_to_add_to_genesis,
        num_validator_seats,
        get_num_seats_per_shard(num_shards, num_validator_seats),
        shard_layout,
    );
    let mut configs = vec![];
    let first_node_addr = tcp::ListenerAddr::reserve_for_test();
    for i in 0..seeds.len() {
        let mut config = Config::default();
        config.rpc.get_or_insert(Default::default()).enable_debug_rpc = true;
        config.consensus.min_block_production_delay = Duration::from_millis(600);
        config.consensus.max_block_production_delay = Duration::from_millis(2000);
        if local_ports {
            config.network.addr = if i == 0 {
                first_node_addr.to_string()
            } else {
                tcp::ListenerAddr::reserve_for_test().to_string()
            };
            config.set_rpc_addr(tcp::ListenerAddr::reserve_for_test());
            config.network.boot_nodes = if i == 0 {
                "".to_string()
            } else {
                format!("{}@{}", network_signers[0].public_key, first_node_addr)
            };
            config.network.skip_sync_wait = num_validator_seats == 1;
        }
        config.archive = archive;
        config.tracked_shards = tracked_shards.clone();
        config.consensus.min_num_peers =
            std::cmp::min(num_validator_seats as usize - 1, config.consensus.min_num_peers);
        configs.push(config);
    }
    (configs, validator_signers, network_signers, genesis)
}

/// Create testnet configuration. If `local_ports` is true,
/// sets up new ports for all nodes except the first one and sets boot node to it.
pub fn create_testnet_configs(
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    local_ports: bool,
    archive: bool,
    fixed_shards: bool,
    tracked_shards: Vec<u64>,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis, Vec<InMemorySigner>)
{
    let fixed_shards = if fixed_shards {
        Some((0..(num_shards - 1)).map(|i| format!("shard{}", i)).collect::<Vec<_>>())
    } else {
        None
    };
    let shard_keys = if let Some(ref fixed_shards) = fixed_shards {
        fixed_shards
            .iter()
            .map(|seed| InMemorySigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed))
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    let (configs, validator_signers, network_signers, genesis) = create_testnet_configs_from_seeds(
        (0..(num_validator_seats + num_non_validator_seats))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_shards,
        num_non_validator_seats,
        local_ports,
        archive,
        fixed_shards,
        tracked_shards,
    );

    (configs, validator_signers, network_signers, genesis, shard_keys)
}

pub fn init_testnet_configs(
    dir: &Path,
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    local_ports: bool,
    archive: bool,
    fixed_shards: bool,
    tracked_shards: Vec<u64>,
) {
    let (configs, validator_signers, network_signers, genesis, shard_keys) = create_testnet_configs(
        num_shards,
        num_validator_seats,
        num_non_validator_seats,
        prefix,
        local_ports,
        archive,
        fixed_shards,
        tracked_shards,
    );
    for i in 0..(num_validator_seats + num_non_validator_seats) as usize {
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        validator_signers[i]
            .write_to_file(&node_dir.join(&configs[i].validator_key_file))
            .expect("Error writing validator key file");
        network_signers[i]
            .write_to_file(&node_dir.join(&configs[i].node_key_file))
            .expect("Error writing key file");
        for key in &shard_keys {
            key.write_to_file(&node_dir.join(format!("{}_key.json", key.account_id)))
                .expect("Error writing shard file");
        }

        genesis.to_file(&node_dir.join(&configs[i].genesis_file));
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME)).expect("Error writing config");
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.display());
    }
}

pub fn get_genesis_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/genesis.json.xz",
        chain_id,
    )
}

pub fn get_records_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/records.json.xz",
        chain_id,
    )
}

pub fn get_config_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/config.json",
        chain_id,
    )
}

pub fn download_genesis(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading genesis file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the genesis file to: {} ...", path.display());
    }
    result
}

pub fn download_records(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading records file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the records file to: {} ...", path.display());
    }
    result
}

pub fn download_config(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading config file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the config file to: {} ...", path.display());
    }
    result
}

#[derive(serde::Deserialize)]
struct NodeKeyFile {
    account_id: String,
    public_key: PublicKey,
    secret_key: near_crypto::SecretKey,
}

impl NodeKeyFile {
    // the file can be JSON with comments
    fn from_file(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)?;

        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)?;

        Ok(serde_json::from_str(&json_str_without_comments)?)
    }
}

impl From<NodeKeyFile> for KeyFile {
    fn from(this: NodeKeyFile) -> Self {
        Self {
            account_id: if this.account_id.is_empty() {
                "node".to_string()
            } else {
                this.account_id
            }
            .try_into()
            .unwrap(),
            public_key: this.public_key,
            secret_key: this.secret_key,
        }
    }
}

pub fn load_config(
    dir: &Path,
    genesis_validation: GenesisValidationMode,
) -> anyhow::Result<NearConfig> {
    let mut validation_errors = ValidationErrors::new();

    // if config.json has file issues, the program will directly panic
    let config = Config::from_file_skip_validation(&dir.join(CONFIG_FILENAME))?;
    // do config.json validation later so that genesis_file, validator_file and genesis_file can be validated before program panic
    if let Err(e) = config.validate() {
        validation_errors.push_errors(e)
    };

    let validator_file = dir.join(&config.validator_key_file);
    let validator_signer = if validator_file.exists() {
        match InMemoryValidatorSigner::from_file(&validator_file) {
            Ok(signer) => Some(Arc::new(signer) as Arc<dyn ValidatorSigner>),
            Err(_) => {
                let error_message = format!(
                    "Failed initializing validator signer from {}",
                    validator_file.display()
                );
                validation_errors.push_validator_key_file_error(error_message);
                None
            }
        }
    } else {
        None
    };

    let node_key_path = dir.join(&config.node_key_file);
    let network_signer_result = NodeKeyFile::from_file(&node_key_path);
    let network_signer = match network_signer_result {
        Ok(node_key_file) => Some(node_key_file),
        Err(_) => {
            let error_message =
                format!("Failed reading node key file from {}", node_key_path.display());
            validation_errors.push_node_key_file_error(error_message);
            None
        }
    };

    let genesis_file = dir.join(&config.genesis_file);
    let genesis_result = match &config.genesis_records_file {
        // only load Genesis from file. Skip test for now.
        // this allows us to know the chain_id in order to check tracked_shards even if semantics checks fail.
        Some(records_file) => Genesis::from_files(
            &genesis_file,
            &dir.join(records_file),
            GenesisValidationMode::UnsafeFast,
        ),
        None => Genesis::from_file(&genesis_file, GenesisValidationMode::UnsafeFast),
    };

    let genesis = match genesis_result {
        Ok(genesis) => {
            if let Err(e) = genesis.validate(genesis_validation) {
                validation_errors.push_errors(e)
            };
            if validator_signer.is_some()
                && matches!(genesis.config.chain_id.as_ref(), MAINNET | TESTNET | BETANET)
                && config.tracked_shards.is_empty()
            {
                // Make sure validators tracks all shards, see
                // https://github.com/near/nearcore/issues/7388
                let error_message = format!("The `chain_id` field specified in genesis is among mainnet/betanet/testnet, so validator must track all shards. Please change `tracked_shards` field in config.json to be any non-empty vector");
                validation_errors.push_cross_file_semantics_error(error_message);
            }
            Some(genesis)
        }
        Err(error) => {
            validation_errors.push_errors(error);
            None
        }
    };

    validation_errors.return_ok_or_error()?;

    if genesis.is_none() || network_signer.is_none() {
        panic!("Genesis and network_signer should not be None by now.")
    }
    let near_config = NearConfig::new(
        config,
        genesis.unwrap(),
        network_signer.unwrap().into(),
        validator_signer,
    )?;
    Ok(near_config)
}

pub fn load_test_config(seed: &str, addr: tcp::ListenerAddr, genesis: Genesis) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = addr.to_string();
    config.set_rpc_addr(tcp::ListenerAddr::reserve_for_test());
    config.consensus.min_block_production_delay =
        Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
    config.consensus.max_block_production_delay =
        Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
    let (signer, validator_signer) = if seed.is_empty() {
        let signer =
            Arc::new(InMemorySigner::from_random("node".parse().unwrap(), KeyType::ED25519));
        (signer, None)
    } else {
        let signer =
            Arc::new(InMemorySigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed));
        let validator_signer = Arc::new(create_test_signer(seed)) as Arc<dyn ValidatorSigner>;
        (signer, Some(validator_signer))
    };
    NearConfig::new(config, genesis, signer.into(), validator_signer).unwrap()
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
/// Options for dumping state to S3.
pub struct StateSyncConfig {
    pub s3_bucket: String,
    pub s3_region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_state_of_dump: Option<Vec<ShardId>>,
}

#[test]
fn test_init_config_localnet() {
    // Check that we can initialize the config with multiple shards.
    let temp_dir = tempdir().unwrap();
    init_configs(
        &temp_dir.path(),
        Some("localnet".to_string()),
        None,
        Some("seed1"),
        3,
        false,
        None,
        false,
        None,
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();
    let genesis =
        Genesis::from_file(temp_dir.path().join("genesis.json"), GenesisValidationMode::UnsafeFast)
            .unwrap();
    assert_eq!(genesis.config.chain_id, "localnet");
    assert_eq!(genesis.config.shard_layout.num_shards(), 3);
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("shard0.test.near").unwrap(),
            &genesis.config.shard_layout
        ),
        0
    );
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("shard1.test.near").unwrap(),
            &genesis.config.shard_layout
        ),
        1
    );
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("foobar.near").unwrap(),
            &genesis.config.shard_layout
        ),
        2
    );
}

#[test]
// Tests that `init_configs()` works if both config and genesis file exists, but the node key and validator key files don't exist.
// Test does the following:
// * Initialize all config and key files
// * Check that the key files exist
// * Remove the key files
// * Run the initialization again
// * Check that the key files got created
fn test_init_config_localnet_keep_config_create_node_key() {
    let temp_dir = tempdir().unwrap();
    // Initialize all config and key files.
    init_configs(
        &temp_dir.path(),
        Some("localnet".to_string()),
        Some(AccountId::from_str("account.near").unwrap()),
        Some("seed1"),
        3,
        false,
        None,
        false,
        None,
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();

    // Check that the key files exist.
    let _genesis =
        Genesis::from_file(temp_dir.path().join("genesis.json"), GenesisValidationMode::Full)
            .unwrap();
    let config = Config::from_file(&temp_dir.path().join(CONFIG_FILENAME)).unwrap();
    let node_key_file = temp_dir.path().join(config.node_key_file);
    let validator_key_file = temp_dir.path().join(config.validator_key_file);
    assert!(node_key_file.exists());
    assert!(validator_key_file.exists());

    // Remove the key files.
    std::fs::remove_file(&node_key_file).unwrap();
    std::fs::remove_file(&validator_key_file).unwrap();

    // Run the initialization again.
    init_configs(
        &temp_dir.path(),
        Some("localnet".to_string()),
        Some(AccountId::from_str("account.near").unwrap()),
        Some("seed1"),
        3,
        false,
        None,
        false,
        None,
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();

    // Check that the key files got created.
    let _node_signer = InMemorySigner::from_file(&node_key_file).unwrap();
    let _validator_signer = InMemorySigner::from_file(&validator_key_file).unwrap();
}

/// Tests that loading a config.json file works and results in values being
/// correctly parsed and defaults being applied correctly applied.
/// We skip config validation since we only care about Config being correctly loaded from file.
#[test]
fn test_config_from_file_skip_validation() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));

    for (has_gc, path) in
        [(true, "res/example-config-gc.json"), (false, "res/example-config-no-gc.json")]
    {
        let path = base.join(path);
        let data = std::fs::read(path).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(&data).unwrap();

        let config = Config::from_file_skip_validation(&tmp.into_temp_path()).unwrap();

        // TODO(mina86): We might want to add more checks.  Looking at all
        // values is probably not worth it but there may be some other defaults
        // we want to ensure that they happen.
        let want_gc = if has_gc {
            GCConfig { gc_blocks_limit: 42, gc_fork_clean_step: 420, gc_num_epochs_to_keep: 24 }
        } else {
            GCConfig { gc_blocks_limit: 2, gc_fork_clean_step: 100, gc_num_epochs_to_keep: 5 }
        };
        assert_eq!(want_gc, config.gc);

        assert_eq!(
            vec!["https://explorer.mainnet.near.org/api/nodes".to_string()],
            config.telemetry.endpoints
        );
    }
}

#[test]
fn test_create_testnet_configs() {
    let num_shards = 4;
    let num_validator_seats = 4;
    let num_non_validator_seats = 8;
    let prefix = "node";
    let local_ports = true;

    // Set all supported options to true and verify config and genesis.

    let archive = true;
    let fixed_shards = true;
    let tracked_shards: Vec<u64> = vec![0, 1, 3];

    let (configs, _validator_signers, _network_signers, genesis, _shard_keys) =
        create_testnet_configs(
            num_shards,
            num_validator_seats,
            num_non_validator_seats,
            prefix,
            local_ports,
            archive,
            fixed_shards,
            tracked_shards.clone(),
        );

    assert_eq!(configs.len() as u64, num_validator_seats + num_non_validator_seats);

    for config in configs {
        assert_eq!(config.archive, true);
        assert_eq!(config.tracked_shards, tracked_shards);
    }

    assert_eq!(genesis.config.validators.len(), num_shards as usize);
    assert_eq!(genesis.config.shard_layout.num_shards(), num_shards);

    // Set all supported options to false and verify config and genesis.

    let archive = false;
    let fixed_shards = false;
    let tracked_shards: Vec<u64> = vec![];

    let (configs, _validator_signers, _network_signers, genesis, _shard_keys) =
        create_testnet_configs(
            num_shards,
            num_validator_seats,
            num_non_validator_seats,
            prefix,
            local_ports,
            archive,
            fixed_shards,
            tracked_shards.clone(),
        );
    assert_eq!(configs.len() as u64, num_validator_seats + num_non_validator_seats);

    for config in configs {
        assert_eq!(config.archive, false);
        assert_eq!(config.tracked_shards, tracked_shards);
    }

    assert_eq!(genesis.config.validators.len() as u64, num_shards);
    assert_eq!(genesis.config.shard_layout.num_shards(), num_shards);
}
