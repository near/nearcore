pub mod dyn_config_watcher;
pub mod log_config_watcher;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io;
use std::io::ErrorKind;
use std::path::PathBuf;
use tracing::{error, info};

pub(crate) enum UpdateBehavior {
    UpdateOrReset,
    UpdateOnlyIfExists,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub(crate) enum WatchConfigError {
    #[error("Failed to reload the watcher config")]
    Reload(String),
    #[error("Failed to reload the logging config")]
    Parse(#[source] serde_json::Error),
    #[error("Can't open or read the logging config file")]
    OpenAndRead(#[source] io::Error),
}

/// Watcher helps to `reload` the change of config
/// main thread will use `update` method to trigger config watchers to reload the config they watch
pub(crate) trait Watcher
where
    Self: Debug + for<'a> Deserialize<'a> + Serialize,
{
    fn reload(instance: Option<Self>) -> Result<(), WatchConfigError>;

    fn do_update(path: &PathBuf, update_behavior: &UpdateBehavior) -> Result<(), WatchConfigError> {
        match std::fs::read_to_string(path) {
            Ok(config_str) => match serde_json::from_str::<Self>(&config_str) {
                Ok(config) => {
                    info!(target: "neard", config=?config, "Changing the config {path:?}.");
                    return Self::reload(Some(config));
                }
                Err(e) => Err(WatchConfigError::Parse(e)),
            },
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {
                    if let UpdateBehavior::UpdateOrReset = update_behavior {
                        info!(target: "neard", ?err, "Reset the config {path:?} because the logging config file doesn't exist.");
                        return Self::reload(None);
                    }
                    Ok(())
                }
                _ => Err(err).map_err(WatchConfigError::OpenAndRead),
            },
        }
    }

    fn update(path: PathBuf, update_behavior: &UpdateBehavior) {
        if let Err(err) = Self::do_update(&path, update_behavior) {
            error!(target: "neard", "Failed to update {path:?}: {err:?}.");
        }
    }
}
