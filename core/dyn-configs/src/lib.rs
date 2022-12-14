#![doc = include_str!("../README.md")]

use near_o11y::log_config::LogConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DynConfigsError {
    #[error("Failed to parse a dynamic config file {file:?}: {err:?}")]
    Parse { file: PathBuf, err: serde_json::Error },
    #[error("Can't open or read a dynamic config file {file:?}: {err:?}")]
    OpenAndRead { file: PathBuf, err: std::io::Error },
    #[error("One or multiple dynamic config files reload errors")]
    Errors(Vec<DynConfigsError>),
    #[error("No home dir set")]
    NoHomeDir(),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DynConfig {
    /// Graceful shutdown at expected blockheight.
    pub expected_shutdown: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DynConfigStore {
    home_dir: Option<PathBuf>,
    dyn_configs: Arc<Mutex<DynConfigs>>,
}

#[derive(Debug, Clone)]
pub struct DynConfigs {
    log_config: Option<LogConfig>,
    dyn_config: Option<DynConfig>,
}

impl DynConfigStore {
    pub fn config(&self) -> DynConfigs {
        let lock = self.dyn_configs.lock().unwrap();
        lock.clone()
    }

    pub fn reload(&mut self) -> Result<(), DynConfigsError> {
        if let Some(home_dir) = &self.home_dir {
            let dyn_configs = DynConfigs::new(home_dir)?;
            self.dyn_configs = Arc::new(Mutex::new(dyn_configs));
            Ok(())
        } else {
            Err(DynConfigsError::NoHomeDir())
        }
    }

    pub fn new(home_dir: &Path) -> Result<Self, DynConfigsError> {
        Ok(Self {
            home_dir: Some(home_dir.to_path_buf()),
            dyn_configs: Arc::new(Mutex::new(DynConfigs::new(home_dir)?)),
        })
    }

    pub fn new_empty() -> Self {
        Self { home_dir: None, dyn_configs: Arc::new(Mutex::new(DynConfigs::new_empty())) }
    }
}

impl DynConfigs {
    pub fn new_empty() -> Self {
        Self { log_config: None, dyn_config: None }
    }

    pub fn new(home_dir: &Path) -> Result<Self, DynConfigsError> {
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
        if errs.is_empty() {
            Ok(Self { log_config, dyn_config })
        } else {
            Err(DynConfigsError::Errors(errs))
        }
    }

    pub fn get_expected_shutdown_at(&self) -> Option<u64> {
        self.dyn_config.as_ref().map_or(None, |dyn_config| dyn_config.expected_shutdown)
    }

    pub fn log_config(&self) -> Option<&LogConfig> {
        self.log_config.as_ref()
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
