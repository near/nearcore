use near_config_utils::ValidationError;
use near_primitives::num_rational::Rational32;
use serde_json::{Serializer, Value};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use smart_default::SmartDefault;
use crate::genesis_config::GenesisConfigLoader;

#[derive(Clone, SmartDefault, serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainConfig {
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational32,
}

#[derive(Clone, SmartDefault, serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainConfigLoader {
    /// Protocol treasury rate
    pub protocol_reward_rate: Option<Rational32>,
}

impl ChainConfig {
    pub fn new(genesis_config_loader: GenesisConfigLoader) -> Self {
        Self { protocol_reward_rate: genesis_config_loader.protocol_reward_rate }
    }

    fn merge_jsons(base: Value, patch: Value) -> Value {
        let mut base_obj = base.clone().as_object().unwrap().clone();
        let patch_obj = patch.as_object().unwrap().clone();

        for (key, value) in patch_obj {
            if !value.is_null() {
                base_obj.insert(key, value);
            }
        }

        Value::Object(base_obj)
    }

    pub fn apply_loader(&self, patch: ChainConfigLoader) -> ChainConfig {
        let patch_fields = serde_json::to_value(&patch).expect("Failed to serialize struct");
        let config_fields = serde_json::to_value(self.clone()).unwrap();
        let merged_fields = Self::merge_jsons(config_fields, patch_fields);
        serde_json::from_value(merged_fields).unwrap()
    }
}

impl ChainConfigLoader {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ValidationError> {
        let mut file = File::open(&path).map_err(|_| ValidationError::GenesisFileError {
            error_message: format!(
                "Could not open chain config file at path {}.",
                &path.as_ref().display()
            ),
        })?;
        let mut json_str = String::new();
        file.read_to_string(&mut json_str).map_err(|_| ValidationError::GenesisFileError {
            error_message: "Failed to read chain config file to string. ".to_string(),
        })?;
        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)
            .map_err(|_| ValidationError::GenesisFileError {
                error_message: "Failed to strip comments from chain config file".to_string(),
            })?;
        let chain_config =
            serde_json::from_str::<ChainConfigLoader>(&json_str_without_comments).map_err(|_| {
                ValidationError::GenesisFileError {
                    error_message: "Failed to deserialize the chain config records.".to_string(),
                }
            })?;
        Ok(chain_config)
    }
}
