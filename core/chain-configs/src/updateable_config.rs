use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
/// A simple wrapper for a config value that can be updated while t he node is running.
pub struct MutableConfigValue<T>(Arc<Mutex<T>>);

impl<T: Copy> MutableConfigValue<T> {
    pub fn new(val: T) -> Self {
        Self(Arc::new(Mutex::new(val)))
    }

    pub fn get(&self) -> T {
        *self.0.lock().unwrap()
    }

    pub fn update(&self, val: T) {
        let mut lock = self.0.lock().unwrap();
        *lock = val;
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct UpdateableClientConfig {
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: Option<BlockHeight>,
    /// Duration to check for producing / skipping block.
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    pub max_block_wait_delay: Duration,
}
