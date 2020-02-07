//! Genesis Configuration
//!
//! NOTE: chain-configs is not the best place for `GenesisConfig` since it
//! contains `RuntimeConfig`, but we keep it here for now until we figure
//! out the better place.
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use near_primitives::serialize::u128_dec_format;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeightDelta, Gas, NumBlocks, NumSeats,
};
use near_runtime_configs::RuntimeConfig;

use crate::PROTOCOL_VERSION;

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, SmartDefault, Serialize, Deserialize)]
pub struct GenesisConfig {
    /// This is a version of a genesis config structure this version of binary works with.
    /// If the binary tries to load a JSON config with a different version it will panic.
    /// It's not a major protocol version, but used for automatic config migrations using scripts.
    pub config_version: u32,
    /// Protocol version that this genesis works with.
    pub protocol_version: u32,
    /// Official time of blockchain start.
    #[default(Utc::now())]
    pub genesis_time: DateTime<Utc>,
    /// ID of the blockchain. This must be unique for every blockchain.
    /// If your testnet blockchains do not have unique chain IDs, you will have a bad time.
    pub chain_id: String,
    /// Number of block producer seats at genesis.
    pub num_block_producer_seats: NumSeats,
    /// Defines number of shards and number of block producer seats per each shard at genesis.
    pub num_block_producer_seats_per_shard: Vec<NumSeats>,
    /// Expected number of hidden validators per shard.
    pub avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    /// Enable dynamic re-sharding.
    pub dynamic_resharding: bool,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    pub min_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: u8,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// Records in storage at genesis (get split into shards at genesis creation).
    pub records: Vec<StateRecord>,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
    /// Developer reward percentage (this is a number between 0 and 100)
    pub developer_reward_percentage: u8,
    /// Protocol treasury percentage (this is a number between 0 and 100)
    pub protocol_reward_percentage: u8,
    /// Maximum inflation on the total supply every epoch (this is a number between 0 and 100)
    pub max_inflation_rate: u8,
    /// Total supply of tokens at genesis.
    pub total_supply: u128,
    /// Expected number of blocks per year
    pub num_blocks_per_year: u64,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "u128_dec_format")]
    pub fishermen_threshold: Balance,
}

impl GenesisConfig {
    /// Init the computed values from the rest of the config.
    pub fn init(&mut self) {
        self.total_supply = get_initial_supply(&self.records);
    }

    /// Reads GenesisConfig from a file.
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open genesis config file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from genesis config file.");
        GenesisConfig::from(content.as_str())
    }

    /// Writes GenesisConfig to the file.
    pub fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a genesis config file.");
        let str =
            serde_json::to_string_pretty(self).expect("Error serializing the genesis config.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a genesis config file {}", err);
        }
    }
}

impl From<&str> for GenesisConfig {
    fn from(config: &str) -> Self {
        let mut config: GenesisConfig =
            serde_json::from_str(config).expect("Failed to deserialize the genesis config.");
        if config.protocol_version != PROTOCOL_VERSION {
            panic!(format!(
                "Incorrect version of genesis config {} expected {}",
                config.protocol_version, PROTOCOL_VERSION
            ));
        }
        config.init();
        config
    }
}

pub fn get_initial_supply(records: &[StateRecord]) -> Balance {
    let mut total_supply = 0;
    for record in records {
        if let StateRecord::Account { account, .. } = record {
            total_supply += account.amount + account.locked;
        }
    }
    total_supply
}
