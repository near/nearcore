use near_primitives::types::BlockHeight;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// A wrapper for a config value that can be updated while the node is running.
/// When initializing sub-objects (e.g. `ShardsManager`), please make sure to
/// pass this wrapper instead of passing a value from a single moment in time.
/// See `expected_shutdown` for an example how to use it.
#[derive(Clone)]
pub struct MutableConfigValue<T> {
    value: Arc<Mutex<T>>,
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    field_name: String,
    #[cfg(feature = "metrics")]
    // For metrics.
    // Mutable config values are exported to prometheus with labels [field_name][last_update][value].
    last_update: chrono::DateTime<chrono::Utc>,
}

impl<T: Serialize> Serialize for MutableConfigValue<T> {
    /// Only include the value field of MutableConfigValue in serialized result
    /// since field_name and last_update are only relevant for internal monitoring
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let to_string_result = serde_json::to_string(&self.value);
        let value_str = to_string_result.unwrap_or("unable to serialize the value".to_string());
        serializer.serialize_str(&value_str)
    }
}

impl<T: Copy + PartialEq + Debug> MutableConfigValue<T> {
    /// Initializes a value.
    /// `field_name` is needed to export the config value as a prometheus metric.
    pub fn new(val: T, field_name: &str) -> Self {
        let res = Self {
            value: Arc::new(Mutex::new(val)),
            field_name: field_name.to_string(),
            #[cfg(feature = "metrics")]
            last_update: near_primitives::static_clock::StaticClock::utc(),
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

    #[cfg(feature = "metrics")]
    fn set_metric_value(&self, value: T, metric_value: i64) {
        // Use field_name as a label to tell different mutable config values apart.
        // Use timestamp as a label to give some idea to the node operator (or
        // people helping them debug their node) when exactly and what values
        // exactly were part of the config.
        // Use the config value as a label to make this work with config values
        // of any type: int, float, string or even a composite object.
        crate::metrics::CONFIG_MUTABLE_FIELD
            .with_label_values(&[
                &self.field_name,
                &self.last_update.timestamp().to_string(),
                &format!("{:?}", value),
            ])
            .set(metric_value);
    }

    #[cfg(not(feature = "metrics"))]
    fn set_metric_value(&self, _value: T, _metric_value: i64) {}
}

#[derive(Default, Clone, Serialize, Deserialize)]
/// A subset of Config that can be updated white the node is running.
pub struct UpdateableClientConfig {
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: Option<BlockHeight>,
}
