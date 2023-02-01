use crate::metrics;
use chrono::{DateTime, Utc};
use near_primitives::time::Clock;
use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// A wrapper for a config value that can be updated while the node is running.
/// When initializing sub-objects (e.g. `ShardsManager`), please make sure to
/// pass this wrapper instead of passing a value from a single moment in time.
/// See `expected_shutdown` for an example how to use it.
/// TODO: custom implementation for Serialize and Deserialize s.t. only value is necessary(JIRA:ND-283)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutableConfigValue<T> {
    value: Arc<Mutex<T>>,
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    field_name: String,
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    last_update: DateTime<Utc>,
}

impl<T: Copy + PartialEq + Debug> MutableConfigValue<T> {
    /// Initializes a value.
    /// `field_name` is needed to export the config value as a prometheus metric.
    pub fn new(val: T, field_name: &str) -> Self {
        let res = Self {
            value: Arc::new(Mutex::new(val)),
            field_name: field_name.to_string(),
            last_update: Clock::utc(),
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
        // Use field_name as a label to tell different mutable config values apart.
        // Use timestamp as a label to give some idea to the node operator (or
        // people helping them debug their node) when exactly and what values
        // exactly were part of the config.
        // Use the config value as a label to make this work with config values
        // of any type: int, float, string or even a composite object.
        metrics::CONFIG_MUTABLE_FIELD
            .with_label_values(&[
                &self.field_name,
                &self.last_update.timestamp().to_string(),
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
