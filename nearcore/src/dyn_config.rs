use crate::config::Config;
use near_chain_configs::{ClientConfig, LogSummaryStyle};
use near_dyn_configs::{DynConfig, DynConfigs};
use near_o11y::log_config::LogConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DynConfigsError {
    #[error("Failed to parse a dynamic config file {file:?}: {err:?}")]
    Parse { file: PathBuf, err: serde_json::Error },
    #[error("Can't open or read a dynamic config file {file:?}: {err:?}")]
    OpenAndRead { file: PathBuf, err: std::io::Error },
    #[error("Can't open or read the config file {file:?}: {err:?}")]
    ConfigFileError { file: PathBuf, err: anyhow::Error },
    #[error("One or multiple dynamic config files reload errors")]
    Errors(Vec<DynConfigsError>),
    #[error("No home dir set")]
    NoHomeDir(),
}

pub fn read_dyn_configs(home_dir: &Path) -> Result<DynConfigs, DynConfigsError> {
    let mut errs = vec![];
    let log_config = match read_log_config(home_dir) {
        Ok(config) => config,
        Err(err) => {
            errs.push(err);
            None
        }
    };
    let dyn_config = match read_dyn_config(home_dir) {
        Ok(config) => config,
        Err(err) => {
            errs.push(err);
            None
        }
    };
    let client_config = match Config::from_file(&home_dir.join(crate::config::CONFIG_FILENAME))
        .map(create_client_config)
    {
        Ok(config) => Some(config),
        Err(err) => {
            errs.push(DynConfigsError::ConfigFileError {
                file: PathBuf::from(crate::config::CONFIG_FILENAME),
                err,
            });
            None
        }
    };
    if errs.is_empty() {
        Ok(DynConfigs { log_config, dyn_config, client_config })
    } else {
        Err(DynConfigsError::Errors(errs))
    }
}

fn create_client_config(config: Config) -> ClientConfig {
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

fn read_log_config(home_dir: &Path) -> Result<Option<LogConfig>, DynConfigsError> {
    read_json_config::<LogConfig>(&home_dir.join("log_config.json"))
}

fn read_dyn_config(home_dir: &Path) -> Result<Option<DynConfig>, DynConfigsError> {
    read_json_config::<DynConfig>(&home_dir.join("dyn_config.json"))
}

fn read_json_config<T: std::fmt::Debug>(path: &Path) -> Result<Option<T>, DynConfigsError>
where
    for<'a> T: Deserialize<'a>,
{
    match std::fs::read_to_string(path) {
        Ok(config_str) => match serde_json::from_str::<T>(&config_str) {
            Ok(config) => {
                tracing::info!(target: "neard", config=?config, "Changing the config {path:?}.");
                return Ok(Some(config));
            }
            Err(err) => Err(DynConfigsError::Parse { file: path.to_path_buf(), err }),
        },
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                tracing::info!(target: "neard", ?err, "Reset the config {path:?} because the logging config file doesn't exist.");
                return Ok(None);
            }
            _ => Err(DynConfigsError::OpenAndRead { file: path.to_path_buf(), err }),
        },
    }
}
