mod client_config;
mod genesis_config;
pub mod genesis_validate;
#[cfg(feature = "metrics")]
mod metrics;
pub mod test_genesis;
pub mod test_utils;
mod updateable_config;

pub use client_config::{
    default_enable_multiline_logging, default_epoch_sync_enabled,
    default_header_sync_expected_height_per_second, default_header_sync_initial_timeout,
    default_header_sync_progress_timeout, default_header_sync_stall_ban_timeout,
    default_log_summary_period, default_orphan_state_witness_max_size,
    default_orphan_state_witness_pool_size, default_produce_chunk_add_transactions_time_limit,
    default_state_sync, default_state_sync_enabled, default_state_sync_timeout,
    default_sync_check_period, default_sync_height_threshold, default_sync_step_period,
    default_transaction_pool_size_limit, default_trie_viewer_state_size_limit,
    default_tx_routing_height_horizon, default_view_client_threads,
    default_view_client_throttle_period, ChunkDistributionNetworkConfig, ChunkDistributionUris,
    ClientConfig, DumpConfig, ExternalStorageConfig, ExternalStorageLocation, GCConfig,
    LogSummaryStyle, ReshardingConfig, ReshardingHandle, StateSyncConfig, SyncConfig,
    DEFAULT_GC_NUM_EPOCHS_TO_KEEP, DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_EXTERNAL,
    DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_ON_CATCHUP_EXTERNAL, MIN_GC_NUM_EPOCHS_TO_KEEP,
    TEST_STATE_SYNC_TIMEOUT,
};
pub use genesis_config::{
    get_initial_supply, stream_records_from_file, Genesis, GenesisChangeConfig, GenesisConfig,
    GenesisContents, GenesisRecords, GenesisValidationMode, ProtocolConfig, ProtocolConfigView,
};
use near_primitives::types::{Balance, BlockHeightDelta, Gas, NumBlocks, NumSeats};
use num_rational::Rational32;
pub use updateable_config::{MutableConfigValue, MutableValidatorSigner, UpdateableClientConfig};

pub const GENESIS_CONFIG_FILENAME: &str = "genesis.json";

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

/// Protocol treasury account
pub const PROTOCOL_TREASURY_ACCOUNT: &str = "near";

/// Protocol upgrade stake threshold.
pub const PROTOCOL_UPGRADE_STAKE_THRESHOLD: Rational32 = Rational32::new_raw(4, 5);

/// Initial gas limit.
pub const INITIAL_GAS_LIMIT: Gas = 1_000_000_000_000_000;

/// Criterion for kicking out block producers.
pub const BLOCK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Criterion for kicking out chunk producers.
pub const CHUNK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Criterion for kicking out chunk validators.
pub const CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD: u8 = 80;

/// Fishermen stake threshold.
pub const FISHERMEN_THRESHOLD: Balance = 10 * NEAR_BASE;

/// The rate at which the gas price can be adjusted (alpha in the formula).
/// The formula is
/// gas_price_t = gas_price_{t-1} * (1 + (gas_used/gas_limit - 1/2) * alpha))
pub const GAS_PRICE_ADJUSTMENT_RATE: Rational32 = Rational32::new_raw(1, 100);

/// Protocol treasury reward
pub const PROTOCOL_REWARD_RATE: Rational32 = Rational32::new_raw(1, 10);

/// Expected block production time in ms.
pub const MIN_BLOCK_PRODUCTION_DELAY: i64 = 600;

/// Expected epoch length.
pub const EXPECTED_EPOCH_LENGTH: BlockHeightDelta =
    (5 * 60 * 1000) / MIN_BLOCK_PRODUCTION_DELAY as u64;

/// Maximum inflation rate per year
pub const MAX_INFLATION_RATE: Rational32 = Rational32::new_raw(1, 20);

/// Initial and minimum gas price.
pub const MIN_GAS_PRICE: Balance = 100_000_000;

/// Expected number of blocks per year
pub const NUM_BLOCKS_PER_YEAR: u64 = 365 * 24 * 60 * 60;

/// Number of blocks for which a given transaction is valid
pub const TRANSACTION_VALIDITY_PERIOD: NumBlocks = 100;

/// Number of seats for block producers
pub const NUM_BLOCK_PRODUCER_SEATS: NumSeats = 50;
