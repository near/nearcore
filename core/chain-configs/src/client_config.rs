//! Chain Client Configuration
use std::cmp::max;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use near_primitives::types::{AccountId, BlockHeightDelta, Gas, NumBlocks, NumSeats, ShardId};
use near_primitives::version::Version;

pub const TEST_STATE_SYNC_TIMEOUT: u64 = 5;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum LogSummaryStyle {
    #[serde(rename = "plain")]
    Plain,
    #[serde(rename = "colored")]
    Colored,
}

/// Minimum number of epochs for which we keep store data
pub const MIN_GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Default number of epochs for which we keep store data
pub const DEFAULT_GC_NUM_EPOCHS_TO_KEEP: u64 = 5;

/// Configuration for garbage collection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GCConfig {
    /// Maximum number of blocks to garbage collect at every garbage collection
    /// call.
    #[serde(default = "default_gc_blocks_limit")]
    pub gc_blocks_limit: NumBlocks,

    /// Maximum number of height to go through at each garbage collection step
    /// when cleaning forks during garbage collection.
    #[serde(default = "default_gc_fork_clean_step")]
    pub gc_fork_clean_step: u64,

    /// Number of epochs for which we keep store data.
    #[serde(default = "default_gc_num_epochs_to_keep")]
    pub gc_num_epochs_to_keep: u64,
}

impl Default for GCConfig {
    fn default() -> Self {
        Self {
            gc_blocks_limit: 2,
            gc_fork_clean_step: 100,
            gc_num_epochs_to_keep: DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
        }
    }
}

fn default_gc_blocks_limit() -> NumBlocks {
    GCConfig::default().gc_blocks_limit
}

fn default_gc_fork_clean_step() -> u64 {
    GCConfig::default().gc_fork_clean_step
}

fn default_gc_num_epochs_to_keep() -> u64 {
    GCConfig::default().gc_num_epochs_to_keep()
}

impl GCConfig {
    pub fn gc_num_epochs_to_keep(&self) -> u64 {
        max(MIN_GC_NUM_EPOCHS_TO_KEEP, self.gc_num_epochs_to_keep)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Version of the binary.
    pub version: Version,
    /// Chain id for status.
    pub chain_id: String,
    /// Listening rpc port for status.
    pub rpc_addr: Option<String>,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// Period between logging summary information.
    pub log_summary_period: Duration,
    /// Enable coloring of the logs
    pub log_summary_style: LogSummaryStyle,
    /// Epoch length.
    pub epoch_length: BlockHeightDelta,
    /// Number of block producer seats
    pub num_block_producer_seats: NumSeats,
    /// Maximum blocks ahead of us before becoming validators to announce account.
    pub announce_account_horizon: BlockHeightDelta,
    /// Time to persist Accounts Id in the router without removing them.
    pub ttl_account_id_router: Duration,
    /// Garbage collection configuration.
    pub gc: GCConfig,
    /// Accounts that this client tracks
    pub tracked_accounts: Vec<AccountId>,
    /// Shards that this client tracks
    pub tracked_shards: Vec<ShardId>,
    /// Not clear old data, set `true` for archive nodes.
    pub archive: bool,
    /// Number of threads for ViewClientActor pool.
    pub view_client_threads: usize,
    /// Run Epoch Sync on the start.
    pub epoch_sync_enabled: bool,
    /// Number of seconds between state requests for view client.
    pub view_client_throttle_period: Duration,
    /// Upper bound of the byte size of contract state that is still viewable. None is no limit
    pub trie_viewer_state_size_limit: Option<u64>,
    /// Max burnt gas per view method.  If present, overrides value stored in
    /// genesis file.  The value only affects the RPCs without influencing the
    /// protocol thus changing it per-node doesn’t affect the blockchain.
    pub max_gas_burnt_view: Option<Gas>,
    /// Re-export storage layer statistics as prometheus metrics.
    pub enable_statistics_export: bool,
    /// Number of threads to execute background migration work in client.
    pub client_background_migration_threads: usize,

    pub consensus: Arc<Mutex<Consensus>>,
}

impl ClientConfig {
    pub fn test(
        skip_sync_wait: bool,
        min_block_prod_time: u64,
        max_block_prod_time: u64,
        num_block_producer_seats: NumSeats,
        archive: bool,
        epoch_sync_enabled: bool,
    ) -> Self {
        ClientConfig {
            version: Default::default(),
            chain_id: "unittest".to_string(),
            rpc_addr: Some("0.0.0.0:3030".to_string()),
            skip_sync_wait,
            log_summary_period: Duration::from_secs(10),
            epoch_length: 10,
            num_block_producer_seats,
            announce_account_horizon: 5,
            ttl_account_id_router: Duration::from_secs(60 * 60),
            gc: GCConfig { gc_blocks_limit: 100, ..GCConfig::default() },
            tracked_accounts: vec![],
            tracked_shards: vec![],
            archive,
            log_summary_style: LogSummaryStyle::Colored,
            view_client_threads: 1,
            epoch_sync_enabled,
            view_client_throttle_period: Duration::from_secs(1),
            trie_viewer_state_size_limit: None,
            max_gas_burnt_view: None,
            enable_statistics_export: true,
            client_background_migration_threads: 1,
            consensus: Arc::new(Mutex::new(Consensus::test(
                min_block_prod_time,
                max_block_prod_time,
            ))),
        }
    }
}

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

fn default_doomslug_step_period() -> Duration {
    Duration::from_millis(100)
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

impl Consensus {
    fn test(min_block_prod_time: u64, max_block_prod_time: u64) -> Self {
        Consensus {
            min_num_peers: 1,
            block_production_tracking_delay: Duration::from_millis(std::cmp::max(
                10,
                min_block_prod_time / 5,
            )),
            min_block_production_delay: Duration::from_millis(min_block_prod_time),
            max_block_production_delay: Duration::from_millis(max_block_prod_time),
            max_block_wait_delay: Duration::from_millis(3 * min_block_prod_time),
            reduce_wait_for_missing_block: Duration::from_millis(0),
            produce_empty_blocks: true,
            block_fetch_horizon: 50,
            state_fetch_horizon: 5,
            block_header_fetch_horizon: 50,
            catchup_step_period: Duration::from_millis(1),
            chunk_request_retry_period: min(
                Duration::from_millis(100),
                Duration::from_millis(min_block_prod_time / 5),
            ),
            header_sync_initial_timeout: Duration::from_secs(10),
            header_sync_progress_timeout: Duration::from_secs(2),
            header_sync_stall_ban_timeout: Duration::from_secs(30),
            state_sync_timeout: Duration::from_secs(TEST_STATE_SYNC_TIMEOUT),
            header_sync_expected_height_per_second: 1,
            sync_check_period: Duration::from_millis(100),
            sync_step_period: Duration::from_millis(10),
            doomslug_step_period: Duration::from_millis(100),
            sync_height_threshold: 1,
        }
    }
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
