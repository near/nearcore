use crate::watchers::{WatchConfigError, Watcher};
use near_dyn_configs::reload;
use serde::{Deserialize, Serialize};

/// Configures logging.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct DynConfig {
    /// Graceful shutdonw at expected blockheight
    pub expected_shutdown: Option<u64>,
}

impl Watcher for DynConfig {
    fn reload(config: Option<Self>) -> Result<(), WatchConfigError> {
        if let Some(config) = config {
            reload(config.expected_shutdown);
            Ok(())
        } else {
            reload(None);
            Ok(())
        }
    }
}
