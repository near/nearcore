//! Genesis Configuration
//!
//! NOTE: chain-configs is not the best place for `GenesisConfig` since it
//! contains `RuntimeConfig`, but we keep it here for now until we figure
//! out the better place.
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use num_rational::Rational;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Serializer;
use smart_default::SmartDefault;

use near_primitives::epoch_manager::EpochConfig;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::NumShards;
use near_primitives::{
    hash::CryptoHash,
    runtime::config::RuntimeConfig,
    serialize::{u128_dec_format, u128_dec_format_compatible},
    state_record::StateRecord,
    types::{
        AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, EpochHeight, Gas,
        NumBlocks, NumSeats,
    },
    version::ProtocolVersion,
};
use sha2::digest::Digest;
use std::convert::TryInto;

const MAX_GAS_PRICE: Balance = 10_000_000_000_000_000_000_000;

#[cfg(feature = "protocol_feature_evm")]
/// See https://github.com/ethereum-lists/chains/blob/master/_data/chains/eip155-1313161556.json
pub const BETANET_EVM_CHAIN_ID: u64 = 1313161556;

#[cfg(feature = "protocol_feature_evm")]
/// See https://github.com/ethereum-lists/chains/blob/master/_data/chains/eip155-1313161555.json
pub const TESTNET_EVM_CHAIN_ID: u64 = 1313161555;

#[cfg(feature = "protocol_feature_evm")]
/// See https://github.com/ethereum-lists/chains/blob/master/_data/chains/eip155-1313161554.json
pub const MAINNET_EVM_CHAIN_ID: u64 = 1313161554;

fn default_online_min_threshold() -> Rational {
    Rational::new(90, 100)
}

fn default_online_max_threshold() -> Rational {
    Rational::new(99, 100)
}

fn default_minimum_stake_divisor() -> u64 {
    10
}

fn default_protocol_upgrade_stake_threshold() -> Rational {
    Rational::new(8, 10)
}

#[derive(Debug, Clone, SmartDefault, Serialize, Deserialize)]
pub struct GenesisConfig {
    /// Protocol version that this genesis works with.
    pub protocol_version: ProtocolVersion,
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
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    #[serde(default = "default_protocol_upgrade_stake_threshold")]
    #[default(Rational::new(8, 10))]
    pub protocol_upgrade_stake_threshold: Rational,
    /// Number of epochs after stake threshold was achieved to start next prtocol version.
    pub protocol_upgrade_num_epochs: EpochHeight,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    #[serde(with = "u128_dec_format_compatible")]
    pub min_gas_price: Balance,
    #[serde(with = "u128_dec_format")]
    #[default(MAX_GAS_PRICE)]
    pub max_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    #[serde(default = "default_online_min_threshold")]
    #[default(Rational::new(90, 100))]
    pub online_min_threshold: Rational,
    /// Online maximum threshold above which validator gets full reward.
    #[serde(default = "default_online_max_threshold")]
    #[default(Rational::new(99, 100))]
    pub online_max_threshold: Rational,
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
    /// The minimum stake required for staking is last seat price divided by this number.
    #[serde(default = "default_minimum_stake_divisor")]
    #[default(10)]
    pub minimum_stake_divisor: u64,
}

impl From<&GenesisConfig> for EpochConfig {
    fn from(config: &GenesisConfig) -> Self {
        EpochConfig {
            epoch_length: config.epoch_length,
            num_shards: config.num_block_producer_seats_per_shard.len() as NumShards,
            num_block_producer_seats: config.num_block_producer_seats,
            num_block_producer_seats_per_shard: config.num_block_producer_seats_per_shard.clone(),
            avg_hidden_validator_seats_per_shard: config
                .avg_hidden_validator_seats_per_shard
                .clone(),
            block_producer_kickout_threshold: config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: config.chunk_producer_kickout_threshold,
            fishermen_threshold: config.fishermen_threshold,
            online_min_threshold: config.online_min_threshold,
            online_max_threshold: config.online_max_threshold,
            protocol_upgrade_num_epochs: config.protocol_upgrade_num_epochs,
            protocol_upgrade_stake_threshold: config.protocol_upgrade_stake_threshold,
            minimum_stake_divisor: config.minimum_stake_divisor,
        }
    }
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

#[derive(Debug, Copy, Clone)]
pub enum GenesisRecordsFileType {
    FullGenesis,
    RecordsArray,
}

#[derive(Debug, Clone)]
pub struct GenesisRecordsFile {
    pub path: PathBuf,
    pub file_type: GenesisRecordsFileType,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Genesis {
    #[serde(flatten)]
    pub config: GenesisConfig,
    pub records: GenesisRecords,
    /// Genesis object may not contain records.
    /// In this case records can be found in records_file.
    /// The idea is that all records consume too much memory,
    /// so they should be processed in streaming fashion with stream_records_with_callback.
    #[serde(skip)]
    pub records_file: GenesisRecordsFile,
    /// Using zero-size PhantomData is a Rust pattern preventing a structure being constructed
    /// without calling `new` method, which has some initialization routine.
    #[serde(skip)]
    phantom: PhantomData<()>,
}

impl AsRef<GenesisConfig> for &Genesis {
    fn as_ref(&self) -> &GenesisConfig {
        &self.config
    }
}

impl GenesisConfig {
    /// Parses GenesisConfig from a JSON string.
    ///
    /// It panics if the contents cannot be parsed from JSON to the GenesisConfig structure.
    pub fn from_json(value: &str) -> Self {
        serde_json::from_str(value).expect("Failed to deserialize the genesis config.")
    }

    /// Reads GenesisConfig from a JSON file.
    ///
    /// It panics if file cannot be open or read, or the contents cannot be parsed from JSON to the
    /// GenesisConfig structure.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        let reader = BufReader::new(File::open(path).expect("Could not open genesis config file."));
        let genesis_config: GenesisConfig =
            serde_json::from_reader(reader).expect("Failed to deserialize the genesis records.");
        genesis_config
    }

    /// Writes GenesisConfig to the file.
    pub fn to_file<P: AsRef<Path>>(&self, path: P) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(self).expect("Error serializing the genesis config."),
        )
        .expect("Failed to create / write a genesis config file.");
    }

    /// Get validators from genesis config
    pub fn validators(&self) -> Vec<ValidatorStake> {
        self.validators
            .iter()
            .map(|account_info| {
                ValidatorStake::new(
                    account_info.account_id.clone(),
                    account_info
                        .public_key
                        .clone()
                        .try_into()
                        .expect("Failed to deserialize validator public key"),
                    account_info.amount,
                )
            })
            .collect()
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
        let reader = BufReader::new(File::open(path).expect("Could not open genesis config file."));
        serde_json::from_reader(reader).expect("Failed to deserialize the genesis records.")
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

impl Default for GenesisRecordsFile {
    fn default() -> Self {
        GenesisRecordsFile { path: PathBuf::new(), file_type: GenesisRecordsFileType::FullGenesis }
    }
}

/// Visitor for records
/// Reads records one by one and passes them to sink
struct RecordsProcessor<F> {
    sink: F,
}

impl<'de, F: FnMut(StateRecord)> Visitor<'de> for RecordsProcessor<&'_ mut F> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("array of StateRecord")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(record) = seq.next_element::<StateRecord>()? {
            (self.sink)(record)
        }
        Ok(())
    }
}

impl<'de, F: FnMut(StateRecord)> DeserializeSeed<'de> for RecordsProcessor<&'_ mut F> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

/// Visitor for Genesis
/// Processes records field with records_processor
/// IGNORES OTHER FIELDS
struct GenesisRecordsProcessor<F> {
    records_processor: RecordsProcessor<F>,
}

impl<'de, F: FnMut(StateRecord)> Visitor<'de> for GenesisRecordsProcessor<&'_ mut F> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("map with records field which is array of StateRecord")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "records" => {
                    map.next_value_seed(self.records_processor)?;
                    return Ok(());
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Err(de::Error::custom("missing field: records"))
    }
}

pub fn stream_records_from_records_file(
    reader: impl Read,
    mut callback: impl FnMut(StateRecord),
) -> serde_json::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let records_processor = RecordsProcessor { sink: &mut callback };
    deserializer.deserialize_seq(records_processor)
}

pub fn stream_records_from_genesis_file(
    reader: impl Read,
    mut callback: impl FnMut(StateRecord),
) -> serde_json::Result<()> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let genesis_processor =
        GenesisRecordsProcessor { records_processor: RecordsProcessor { sink: &mut callback } };
    deserializer.deserialize_map(genesis_processor)
}

pub struct GenesisJsonHasher {
    digest: sha2::Sha256,
}

impl GenesisJsonHasher {
    pub fn new() -> Self {
        Self { digest: sha2::Sha256::new() }
    }

    pub fn process_config(&mut self, config: &GenesisConfig) {
        let mut ser = Serializer::pretty(&mut self.digest);
        config.serialize(&mut ser).expect("Error serializing the genesis config.");
    }

    pub fn process_record(&mut self, record: &StateRecord) {
        let mut ser = Serializer::pretty(&mut self.digest);
        record.serialize(&mut ser).expect("Error serializing the genesis record.");
    }

    pub fn process_genesis(&mut self, genesis: &Genesis) {
        self.process_config(&genesis.config);
        genesis.for_each_record(|record: &StateRecord| {
            self.process_record(record);
        });
    }

    pub fn finalize(self) -> CryptoHash {
        CryptoHash(self.digest.finalize().into())
    }
}

impl Genesis {
    pub fn new(config: GenesisConfig, records: GenesisRecords) -> Self {
        let mut genesis = Self {
            config,
            records,
            records_file: GenesisRecordsFile::default(),
            phantom: PhantomData,
        };
        genesis.config.total_supply = get_initial_supply(&genesis.records.as_ref());
        genesis
    }

    pub fn new_as_is(
        config: GenesisConfig,
        records: GenesisRecords,
        records_file: GenesisRecordsFile,
    ) -> Self {
        Self { config, records, records_file, phantom: PhantomData }
    }

    /// Reads Genesis from a single file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        let reader = BufReader::new(File::open(path).expect("Could not open genesis config file."));
        serde_json::from_reader(reader).expect("Failed to deserialize the genesis records.")
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

    /// Hash of the json-serialized input.
    /// DEVNOTE: the representation is not unique, and could change on upgrade.
    pub fn json_hash(&self) -> CryptoHash {
        let mut hasher = GenesisJsonHasher::new();
        hasher.process_genesis(self);
        hasher.finalize()
    }

    fn stream_records_with_callback(
        &self,
        callback: impl FnMut(StateRecord),
    ) -> std::io::Result<()> {
        let reader = BufReader::new(File::open(&self.records_file.path)?);
        match &self.records_file.file_type {
            GenesisRecordsFileType::FullGenesis => {
                stream_records_from_genesis_file(reader, callback).map_err(std::io::Error::from)
            }
            GenesisRecordsFileType::RecordsArray => {
                stream_records_from_records_file(reader, callback).map_err(std::io::Error::from)
            }
        }
    }

    pub fn for_each_record(&self, mut callback: impl FnMut(&StateRecord)) {
        if self.records.as_ref().is_empty() {
            let callback_move = |record: StateRecord| {
                callback(&record);
            };
            self.stream_records_with_callback(callback_move)
                .expect("error while streaming records");
        } else {
            for record in self.records.as_ref() {
                callback(record);
            }
        }
    }
}

pub fn get_initial_supply(records: &[StateRecord]) -> Balance {
    let mut total_supply = 0;
    for record in records {
        if let StateRecord::Account { account, .. } = record {
            total_supply += account.amount() + account.locked();
        }
    }
    total_supply
}

// Note: this type cannot be placed in primitives/src/view.rs because of `RuntimeConfig` dependency issues.
// Ideally we should create `RuntimeConfigView`, but given the deeply nested nature and the number of fields inside
// `RuntimeConfig`, it should be its own endeavor.
#[derive(Serialize, Deserialize, Debug)]
pub struct ProtocolConfigView {
    /// Current Protocol Version
    pub protocol_version: ProtocolVersion,
    /// Official time of blockchain start.
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
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    #[serde(with = "u128_dec_format_compatible")]
    pub min_gas_price: Balance,
    /// Maximum gas price.
    #[serde(with = "u128_dec_format")]
    pub max_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: Rational,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfig,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational,
    /// Maximum inflation on the total supply every epoch.
    pub max_inflation_rate: Rational,
    /// Expected number of blocks per year
    pub num_blocks_per_year: NumBlocks,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "u128_dec_format")]
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
}

// This may be subject to change
pub type ProtocolConfig = GenesisConfig;

impl From<ProtocolConfig> for ProtocolConfigView {
    fn from(config: ProtocolConfig) -> Self {
        ProtocolConfigView {
            protocol_version: config.protocol_version,
            genesis_time: config.genesis_time,
            chain_id: config.chain_id,
            genesis_height: config.genesis_height,
            num_block_producer_seats: config.num_block_producer_seats,
            num_block_producer_seats_per_shard: config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: config.avg_hidden_validator_seats_per_shard,
            dynamic_resharding: config.dynamic_resharding,
            protocol_upgrade_stake_threshold: config.protocol_upgrade_stake_threshold,
            epoch_length: config.epoch_length,
            gas_limit: config.gas_limit,
            min_gas_price: config.min_gas_price,
            max_gas_price: config.max_gas_price,
            block_producer_kickout_threshold: config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: config.chunk_producer_kickout_threshold,
            online_min_threshold: config.online_min_threshold,
            online_max_threshold: config.online_max_threshold,
            gas_price_adjustment_rate: config.gas_price_adjustment_rate,
            runtime_config: config.runtime_config,
            transaction_validity_period: config.transaction_validity_period,
            protocol_reward_rate: config.protocol_reward_rate,
            max_inflation_rate: config.max_inflation_rate,
            num_blocks_per_year: config.num_blocks_per_year,
            protocol_treasury_account: config.protocol_treasury_account,
            fishermen_threshold: config.fishermen_threshold,
            minimum_stake_divisor: config.minimum_stake_divisor,
        }
    }
}
