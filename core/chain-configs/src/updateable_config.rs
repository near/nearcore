use crate::metrics;
use near_primitives::time::Clock;
use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
/// A simple wrapper for a config value that can be updated while the node is running.
pub struct MutableConfigValue<T> {
    value: Arc<Mutex<T>>,
    field_name: String,
    last_update: i64,
}

impl<T: Copy + PartialEq + Debug> MutableConfigValue<T> {
    pub fn new(val: T, field_name: &str) -> Self {
        let res = Self {
            value: Arc::new(Mutex::new(val)),
            field_name: field_name.to_string(),
            last_update: Clock::utc().timestamp(),
        };
        res.set_metric_value(val, 1);
        res
    }

    pub fn get(&self) -> T {
        *self.value.lock().unwrap()
    }

    pub fn update(&self, val: T) {
        let mut lock = self.value.lock().unwrap();
        if *lock != val {
            tracing::info!(target: "config", "Updated config field '{}' from {:?} to {:?}", self.field_name, *lock, val);
            self.set_metric_value(*lock, 0);
            *lock = val;
            self.set_metric_value(val, 1);
        } else {
            tracing::info!(target: "config", "Mutable config field '{}' remains the same: {:?}", self.field_name, val);
        }
    }

    fn set_metric_value(&self, value: T, metric_value: i64) {
        metrics::CONFIG_MUTABLE_FIELD
            .with_label_values(&[
                &self.field_name,
                &self.last_update.to_string(),
                &format!("{:?}", value),
            ])
            .set(metric_value);
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
/// A subset of Config that can be updated white the node is running.
pub struct UpdateableClientConfig {
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: Option<BlockHeight>,
}
