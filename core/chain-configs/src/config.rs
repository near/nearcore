use near_config_utils::ValidationError;
use near_primitives::num_rational::Rational32;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainConfig {
    /// Protocol treasury rate
    pub protocol_reward_rate: Option<Rational32>,
}

impl ChainConfig {
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
            serde_json::from_str::<ChainConfig>(&json_str_without_comments).map_err(|_| {
                ValidationError::GenesisFileError {
                    error_message: "Failed to deserialize the chain config records.".to_string(),
                }
            })?;
        Ok(chain_config)
    }
}
