//! Genesis Configuration
//!
//! NOTE: chain-configs is not the best place for `GenesisConfig` since it
//! contains `RuntimeConfig`, but we keep it here for now until we figure
//! out the better place.
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use num_rational::Rational;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use near_primitives::serialize::{u128_dec_format, u128_dec_format_compatible};
use near_primitives::state_record::StateRecord;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, Gas, NumBlocks, NumSeats,
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
    /// Height of genesis block.
    pub genesis_height: BlockHeight,
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
    #[serde(with = "u128_dec_format_compatible")]
    pub min_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Gas price adjustment rate
    #[default(Rational::from_integer(0))]
    pub gas_price_adjustment_rate: Rational,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
    /// Protocol treasury rate
    #[default(Rational::from_integer(0))]
    pub protocol_reward_rate: Rational,
    /// Maximum inflation on the total supply every epoch.
    #[default(Rational::from_integer(0))]
    pub max_inflation_rate: Rational,
    /// Total supply of tokens at genesis.
    #[serde(with = "u128_dec_format")]
    pub total_supply: Balance,
    /// Expected number of blocks per year
    pub num_blocks_per_year: NumBlocks,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "u128_dec_format")]
    pub fishermen_threshold: Balance,
}

/// Records in storage at genesis (get split into shards at genesis creation).
#[derive(
    Debug,
    Clone,
    SmartDefault,
    derive_more::AsRef,
    derive_more::AsMut,
    derive_more::From,
    Serialize,
    Deserialize,
)]
pub struct GenesisRecords(pub Vec<StateRecord>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Genesis {
    #[serde(flatten)]
    pub config: GenesisConfig,
    pub records: GenesisRecords,
    /// Using zero-size PhantomData is a Rust pattern preventing a structure being constructed
    /// without calling `new` method, which has some initialization routine.
    #[serde(skip)]
    phantom: PhantomData<()>,
}

impl AsRef<GenesisConfig> for Arc<Genesis> {
    fn as_ref(&self) -> &GenesisConfig {
        &self.config
    }
}

impl GenesisConfig {
    /// Parses GenesisConfig from a JSON string.
    ///
    /// It panics if the contents cannot be parsed from JSON to the GenesisConfig structure.
    pub fn from_json(value: &str) -> Self {
        let config: Self =
            serde_json::from_str(value).expect("Failed to deserialize the genesis config.");
        if config.protocol_version != PROTOCOL_VERSION {
            panic!(
                "Incorrect version of genesis config {} expected {}",
                config.protocol_version, PROTOCOL_VERSION
            );
        }
        config
    }

    /// Reads GenesisConfig from a JSON file.
    ///
    /// It panics if file cannot be open or read, or the contents cannot be parsed from JSON to the
    /// GenesisConfig structure.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        Self::from_json(
            &std::fs::read_to_string(path).expect("Could not read genesis config file."),
        )
    }

    /// Writes GenesisConfig to the file.
    pub fn to_file<P: AsRef<Path>>(&self, path: P) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(self).expect("Error serializing the genesis config."),
        )
        .expect("Failed to create / write a genesis config file.");
    }
}

impl GenesisRecords {
    /// Parses GenesisRecords from a JSON string.
    ///
    /// It panics if the contents cannot be parsed from JSON to the GenesisConfig structure.
    pub fn from_json(value: &str) -> Self {
        serde_json::from_str(value).expect("Failed to deserialize the genesis records.")
    }

    /// Reads GenesisRecords from a JSON file.
    ///
    /// It panics if file cannot be open or read, or the contents cannot be parsed from JSON to the
    /// GenesisConfig structure.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        Self::from_json(
            &std::fs::read_to_string(path).expect("Could not read genesis records file."),
        )
    }

    /// Writes GenesisRecords to the file.
    pub fn to_file<P: AsRef<Path>>(&self, path: P) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(self).expect("Error serializing the genesis records."),
        )
        .expect("Failed to create / write a genesis records file.");
    }
}

impl Genesis {
    pub fn new(config: GenesisConfig, records: GenesisRecords) -> Self {
        let mut genesis = Self { config, records, phantom: PhantomData };
        genesis.config.total_supply = get_initial_supply(&genesis.records.as_ref());
        genesis
    }

    /// Reads Genesis from a single file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        serde_json::from_str(&std::fs::read_to_string(path).expect("Could not read genesis file."))
            .expect("Failed to deserialize the genesis records.")
    }

    /// Reads Genesis from config and records files.
    pub fn from_files<P1, P2>(config_path: P1, records_path: P2) -> Self
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let config = GenesisConfig::from_file(config_path);
        let records = GenesisRecords::from_file(records_path);
        Self::new(config, records)
    }

    /// Writes Genesis to the file.
    pub fn to_file<P: AsRef<Path>>(&self, path: P) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(self).expect("Error serializing the genesis config."),
        )
        .expect("Failed to create / write a genesis config file.");
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
