#![doc = include_str!("../README.md")]

use near_chain_configs::Consensus;
use near_o11y::log_config::LogConfig;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DynConfig {
    /// Graceful shutdown at expected blockheight.
    pub expected_shutdown: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DynConfigs {
    pub dyn_config: Option<DynConfig>,
    pub log_config: Option<LogConfig>,
    pub consensus: Option<Consensus>,
}


/* fn default_client_config() -> ClientConfig {
    ClientConfig {
        version: Default::default(),
        chain_id: "".to_string(),
        rpc_addr: None,
        block_production_tracking_delay: Default::default(),
        min_block_production_delay: Default::default(),
        max_block_production_delay: Default::default(),
        max_block_wait_delay: Default::default(),
        reduce_wait_for_missing_block: Default::default(),
        skip_sync_wait: false,
        sync_check_period: Default::default(),
        sync_step_period: Default::default(),
        sync_height_threshold: 0,
        header_sync_initial_timeout: Default::default(),
        header_sync_progress_timeout: Default::default(),
        header_sync_stall_ban_timeout: Default::default(),
        header_sync_expected_height_per_second: 0,
        state_sync_timeout: Default::default(),
        min_num_peers: 0,
        log_summary_period: Default::default(),
        log_summary_style: LogSummaryStyle::Plain,
        produce_empty_blocks: false,
        epoch_length: 0,
        num_block_producer_seats: 0,
        announce_account_horizon: 0,
        ttl_account_id_router: Default::default(),
        block_fetch_horizon: 0,
        state_fetch_horizon: 0,
        catchup_step_period: Default::default(),
        chunk_request_retry_period: Default::default(),
        doosmslug_step_period: Default::default(),
        block_header_fetch_horizon: 0,
        gc: Default::default(),
        tracked_accounts: vec![],
        tracked_shards: vec![],
        archive: false,
        view_client_threads: 0,
        epoch_sync_enabled: false,
        view_client_throttle_period: Default::default(),
        trie_viewer_state_size_limit: None,
        max_gas_burnt_view: None,
        enable_statistics_export: false,
        client_background_migration_threads: 0,
    }
}
 */

#[derive(Default)]
pub struct DynConfigStore {
    dyn_configs: DynConfigs,
    callbacks: Vec<Box<dyn Fn(Option<&Consensus>) + Send + Sync + 'static>>,
}

impl DynConfigStore {
    pub fn reload(&mut self, dyn_configs: DynConfigs) {
        self.dyn_configs = dyn_configs;
        for f in &self.callbacks {
            f(self.client_config());
        }
    }

    pub fn new(dyn_configs: DynConfigs) -> Self {
        Self { dyn_configs, callbacks: vec![] }
    }

    pub fn dyn_config(&self) -> Option<&DynConfig> {
        self.dyn_configs.dyn_config.as_ref()
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.dyn_configs.log_config.as_ref()
    }

    pub fn client_config(&self) -> Option<&Consensus> {
        self.dyn_configs.consensus.as_ref()
    }

    pub fn register_update_callback(&mut self, f: Box<dyn Fn(Option<&Consensus>) + Send + Sync + 'static>) {
        self.callbacks.push(f);
    }
}
