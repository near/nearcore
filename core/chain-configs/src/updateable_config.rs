use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
/// A simple wrapper for a config value that can be updated while the node is running.
pub struct MutableConfigValue<T> {
    value: Arc<Mutex<T>>,
    field_name: String,
}

impl<T: Copy + PartialEq + Debug> MutableConfigValue<T> {
    pub fn new(val: T, field_name: &str) -> Self {
        Self { value: Arc::new(Mutex::new(val)), field_name: field_name.to_string() }
    }

    pub fn get(&self) -> T {
        *self.value.lock().unwrap()
    }

    pub fn update(&self, val: T) {
        let mut lock = self.value.lock().unwrap();
        tracing::info!(target: "config", "Updated config field '{}' from {:?} to {:?}", self.field_name, *lock, val);
        *lock = val;
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
/// A subset of Config that can be updated white the node is running.
pub struct UpdateableClientConfig {
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: Option<BlockHeight>,
}
