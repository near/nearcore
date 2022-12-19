//! Chain Client Configuration
use crate::Consensus;
use near_primitives::types::{AccountId, BlockHeightDelta, Gas, NumBlocks, NumSeats, ShardId};
use near_primitives::version::Version;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    /// Save trie changes. Must be set to true if either of the following is true
    /// - archive is false - non archival nodes need trie changes for garbage collection
    /// - the node will be migrated to split storage in the near future - split storage nodes need trie changes for hot storage garbage collection
    pub save_trie_changes: bool,
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

    /// Consensus config protected by a Mutex because it can be dynamically reloaded.
    pub consensus: Arc<Mutex<Consensus>>,
}

impl ClientConfig {
    pub fn test(
        skip_sync_wait: bool,
        min_block_prod_time: u64,
        max_block_prod_time: u64,
        num_block_producer_seats: NumSeats,
        archive: bool,
        save_trie_changes: bool,
        epoch_sync_enabled: bool,
    ) -> Self {
        assert!(
            archive || save_trie_changes,
            "Configuration with archive = false and save_trie_changes = false is not supported \
            because non-archival nodes must save trie changes in order to do do garbage collection."
        );

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
            save_trie_changes,
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
