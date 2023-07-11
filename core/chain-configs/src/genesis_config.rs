//! Genesis Configuration
//!
//! NOTE: chain-configs is not the best place for `GenesisConfig` since it
//! contains `RuntimeConfig`, but we keep it here for now until we figure
//! out the better place.
use crate::genesis_validate::validate_genesis;
use anyhow::Context;
use chrono::{DateTime, Utc};
use near_config_utils::ValidationError;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::StateRoot;
use near_primitives::views::RuntimeConfigView;
use near_primitives::{
    hash::CryptoHash,
    runtime::config::RuntimeConfig,
    serialize::dec_format,
    state_record::StateRecord,
    types::{
        AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, Gas, NumBlocks, NumSeats,
    },
    version::ProtocolVersion,
};
use num_rational::Rational32;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Serializer;
use sha2::digest::Digest;
use smart_default::SmartDefault;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use tracing::warn;

const MAX_GAS_PRICE: Balance = 10_000_000_000_000_000_000_000;

fn default_online_min_threshold() -> Rational32 {
    Rational32::new(90, 100)
}

fn default_online_max_threshold() -> Rational32 {
    Rational32::new(99, 100)
}

fn default_minimum_stake_divisor() -> u64 {
    10
}

fn default_protocol_upgrade_stake_threshold() -> Rational32 {
    Rational32::new(8, 10)
}

fn default_shard_layout() -> ShardLayout {
    ShardLayout::v0_single_shard()
}

fn default_minimum_stake_ratio() -> Rational32 {
    Rational32::new(160, 1_000_000)
}

fn default_minimum_validators_per_shard() -> u64 {
    1
}

fn default_num_chunk_only_producer_seats() -> u64 {
    300
}

fn default_use_production_config() -> bool {
    false
}

fn default_max_kickout_stake_threshold() -> u8 {
    100
}

#[derive(Debug, Clone, SmartDefault, serde::Serialize, serde::Deserialize)]
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
    /// Note: not used with protocol_feature_chunk_only_producers -- replaced by minimum_validators_per_shard
    /// Note: not used before as all block producers produce chunks for all shards
    pub num_block_producer_seats_per_shard: Vec<NumSeats>,
    /// Expected number of hidden validators per shard.
    pub avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    /// Enable dynamic re-sharding.
    pub dynamic_resharding: bool,
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    #[serde(default = "default_protocol_upgrade_stake_threshold")]
    #[default(Rational32::new(8, 10))]
    pub protocol_upgrade_stake_threshold: Rational32,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    #[serde(with = "dec_format")]
    pub min_gas_price: Balance,
    #[serde(with = "dec_format")]
    #[default(MAX_GAS_PRICE)]
    pub max_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    #[serde(default = "default_online_min_threshold")]
    #[default(Rational32::new(90, 100))]
    pub online_min_threshold: Rational32,
    /// Online maximum threshold above which validator gets full reward.
    #[serde(default = "default_online_max_threshold")]
    #[default(Rational32::new(99, 100))]
    pub online_max_threshold: Rational32,
    /// Gas price adjustment rate
    #[default(Rational32::from_integer(0))]
    pub gas_price_adjustment_rate: Rational32,
    /// List of initial validators.
    pub validators: Vec<AccountInfo>,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
    /// Protocol treasury rate
    #[default(Rational32::from_integer(0))]
    pub protocol_reward_rate: Rational32,
    /// Maximum inflation on the total supply every epoch.
    #[default(Rational32::from_integer(0))]
    pub max_inflation_rate: Rational32,
    /// Total supply of tokens at genesis.
    #[serde(with = "dec_format")]
    pub total_supply: Balance,
    /// Expected number of blocks per year
    pub num_blocks_per_year: NumBlocks,
    /// Protocol treasury account
    #[default("near".parse().unwrap())]
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "dec_format")]
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    #[serde(default = "default_minimum_stake_divisor")]
    #[default(10)]
    pub minimum_stake_divisor: u64,
    /// Layout information regarding how to split accounts to shards
    #[serde(default = "default_shard_layout")]
    #[default(ShardLayout::v0_single_shard())]
    pub shard_layout: ShardLayout,
    #[serde(default = "default_num_chunk_only_producer_seats")]
    #[default(300)]
    pub num_chunk_only_producer_seats: NumSeats,
    /// The minimum number of validators each shard must have
    #[serde(default = "default_minimum_validators_per_shard")]
    #[default(1)]
    pub minimum_validators_per_shard: NumSeats,
    #[serde(default = "default_max_kickout_stake_threshold")]
    #[default(100)]
    /// Max stake percentage of the validators we will kick out.
    pub max_kickout_stake_perc: u8,
    /// The lowest ratio s/s_total any block producer can have.
    /// See <https://github.com/near/NEPs/pull/167> for details
    #[serde(default = "default_minimum_stake_ratio")]
    #[default(Rational32::new(160, 1_000_000))]
    pub minimum_stake_ratio: Rational32,
    #[serde(default = "default_use_production_config")]
    #[default(false)]
    /// This is only for test purposes. We hard code some configs for mainnet and testnet
    /// in AllEpochConfig, and we want to have a way to test that code path. This flag is for that.
    /// If set to true, the node will use the same config override path as mainnet and testnet.
    pub use_production_config: bool,
}

impl GenesisConfig {
    pub fn use_production_config(&self) -> bool {
        self.use_production_config || self.chain_id == "testnet" || self.chain_id == "mainnet"
    }
}

impl From<&GenesisConfig> for EpochConfig {
    fn from(config: &GenesisConfig) -> Self {
        EpochConfig {
            epoch_length: config.epoch_length,
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
            protocol_upgrade_stake_threshold: config.protocol_upgrade_stake_threshold,
            minimum_stake_divisor: config.minimum_stake_divisor,
            shard_layout: config.shard_layout.clone(),
            validator_selection_config: near_primitives::epoch_manager::ValidatorSelectionConfig {
                num_chunk_only_producer_seats: config.num_chunk_only_producer_seats,
                minimum_validators_per_shard: config.minimum_validators_per_shard,
                minimum_stake_ratio: config.minimum_stake_ratio,
            },
            validator_max_kickout_stake_perc: config.max_kickout_stake_perc,
        }
    }
}

impl From<&GenesisConfig> for AllEpochConfig {
    fn from(genesis_config: &GenesisConfig) -> Self {
        let initial_epoch_config = EpochConfig::from(genesis_config);
        let epoch_config = Self::new(genesis_config.use_production_config(), initial_epoch_config);
        epoch_config
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
    serde::Serialize,
    serde::Deserialize,
)]
pub struct GenesisRecords(pub Vec<StateRecord>);

/// custom deserializer that does *almost the same thing that #[serde(default)] would do --
/// if no value is provided in json, returns default value.
/// * Here if `null` is provided as value in JSON, default value will be returned,
/// while in serde default implementation that scenario wouldn't parse.
fn no_value_and_null_as_default<'de, D, T>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let opt = Option::<T>::deserialize(de)?;
    match opt {
        None => Ok(T::default()),
        Some(value) => Ok(value),
    }
}

#[derive(Debug, Clone, SmartDefault, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum GenesisContents {
    /// Records in storage at genesis (get split into shards at genesis creation).
    #[default]
    Records { records: GenesisRecords },
    /// Genesis object may not contain records.
    /// In this case records can be found in records_file.
    /// The idea is that all records consume too much memory,
    /// so they should be processed in streaming fashion with for_each_record.
    RecordsFile { records_file: PathBuf },
    /// Use records already in storage, represented by these state roots.
    /// Used only for mock network forking for testing purposes.
    /// WARNING: THIS IS USED FOR TESTING ONLY. IT IS **NOT CORRECT**, because
    /// it is impossible to compute the corresponding genesis hash in this form,
    /// such that the genesis hash is consistent with that of an equivalent
    /// genesis that spells out the records.
    StateRoots { state_roots: Vec<StateRoot> },
}

fn contents_are_from_record_file(contents: &GenesisContents) -> bool {
    match contents {
        GenesisContents::RecordsFile { .. } => true,
        _ => false,
    }
}

/// `Genesis` has an invariant that `total_supply` is equal to the supply seen in the records.
/// However, we can't enforce that invariant. All fields are public, but the clients are expected to
/// use the provided methods for instantiation, serialization and deserialization.
/// Refer to `test_loading_localnet_genesis` to see an example of serialized Genesis JSON.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Genesis {
    #[serde(flatten)]
    pub config: GenesisConfig,
    /// Custom deserializer used instead of serde(default),
    /// because serde(flatten) doesn't work with default for some reason
    /// The corresponding issue has been open since 2019, so any day now.
    #[serde(
        flatten,
        deserialize_with = "no_value_and_null_as_default",
        skip_serializing_if = "contents_are_from_record_file"
    )]
    pub contents: GenesisContents,
}

impl GenesisConfig {
    /// Parses GenesisConfig from a JSON string.
    /// The string can be a JSON with comments.
    /// It panics if the contents cannot be parsed from JSON to the GenesisConfig structure.
    pub fn from_json(value: &str) -> Self {
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&value.to_string())
                .expect("Failed to strip comments from genesis config.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the genesis config.")
    }

    /// Reads GenesisConfig from a JSON file.
    /// The file can be a JSON with comments.
    /// It panics if file cannot be open or read, or the contents cannot be parsed from JSON to the
    /// GenesisConfig structure.
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let mut file = File::open(path).with_context(|| "Could not open genesis config file.")?;
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)?;
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)?;
        let genesis_config: GenesisConfig = serde_json::from_str(&json_str_without_comments)
            .with_context(|| "Failed to deserialize the genesis config.")?;
        Ok(genesis_config)
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
                    account_info.public_key.clone(),
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
    /// The file can be a JSON with comments.
    /// It panics if file cannot be open or read, or the contents cannot be parsed from JSON to the
    /// GenesisConfig structure.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        let mut file = File::open(path).expect("Failed to open genesis config file.");
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .expect("Failed to read the genesis config file to string. ");
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_str)
                .expect("Failed to strip comments from Genesis config file.");
        serde_json::from_str(&json_str_without_comments)
            .expect("Failed to deserialize the genesis records.")
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

/// Visitor for records.
/// Reads records one by one and passes them to sink.
/// If full genesis file is passed, reads records from "records" field and
/// IGNORES OTHER FIELDS.
struct RecordsProcessor<F> {
    sink: F,
}

impl<'de, F: FnMut(StateRecord)> Visitor<'de> for RecordsProcessor<&'_ mut F> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(
            "either:\
        1. array of StateRecord\
        2. map with records field which is array of StateRecord",
        )
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

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut me = Some(self);
        let mut has_records_field = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "records" => {
                    let me =
                        me.take().ok_or_else(|| de::Error::custom("duplicate field: records"))?;
                    map.next_value_seed(me)?;
                    has_records_field = true;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if has_records_field {
            Ok(())
        } else {
            Err(de::Error::custom("missing field: records"))
        }
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

/// The file can be a JSON with comments
pub fn stream_records_from_file(
    reader: impl Read,
    mut callback: impl FnMut(StateRecord),
) -> serde_json::Result<()> {
    let reader_without_comments = near_config_utils::strip_comments_from_json_reader(reader);
    let mut deserializer = serde_json::Deserializer::from_reader(reader_without_comments);
    let records_processor = RecordsProcessor { sink: &mut callback };
    deserializer.deserialize_any(records_processor)
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

    pub fn process_state_roots(&mut self, state_roots: &[StateRoot]) {
        // WARNING: THIS IS INCORRECT, because it is impossible to compute the
        // genesis hash from the state root in a way that is consistent to that
        // of a genesis that spells out all the records.
        // THEREFORE, THIS IS ONLY USED FOR TESTING, and this logic is only
        // present at all because a genesis hash always has to at least exist.
        let mut ser = Serializer::pretty(&mut self.digest);
        state_roots.serialize(&mut ser).expect("Error serializing the state roots.");
    }

    pub fn process_genesis(&mut self, genesis: &Genesis) {
        self.process_config(&genesis.config);
        match &genesis.contents {
            GenesisContents::StateRoots { state_roots } => {
                self.process_state_roots(state_roots);
            }
            _ => {
                genesis.for_each_record(|record: &StateRecord| {
                    self.process_record(record);
                });
            }
        }
    }

    pub fn finalize(self) -> CryptoHash {
        CryptoHash(self.digest.finalize().into())
    }
}

pub enum GenesisValidationMode {
    Full,
    UnsafeFast,
}

impl Genesis {
    pub fn new(config: GenesisConfig, records: GenesisRecords) -> Result<Self, ValidationError> {
        Self::new_validated(config, records, GenesisValidationMode::Full)
    }

    pub fn new_with_path<P: AsRef<Path>>(
        config: GenesisConfig,
        records_file: P,
    ) -> Result<Self, ValidationError> {
        Self::new_with_path_validated(config, records_file, GenesisValidationMode::Full)
    }

    /// Reads Genesis from a single JSON file, the file can be JSON with comments
    /// This function will collect all errors regarding genesis.json and push them to validation_errors
    pub fn from_file<P: AsRef<Path>>(
        path: P,
        genesis_validation: GenesisValidationMode,
    ) -> Result<Self, ValidationError> {
        let mut file = File::open(&path).map_err(|_| ValidationError::GenesisFileError {
            error_message: format!(
                "Could not open genesis config file at path {}.",
                &path.as_ref().display()
            ),
        })?;

        let mut json_str = String::new();
        file.read_to_string(&mut json_str).map_err(|_| ValidationError::GenesisFileError {
            error_message: format!("Failed to read genesis config file to string. "),
        })?;

        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)
            .map_err(|_| ValidationError::GenesisFileError {
                error_message: format!("Failed to strip comments from genesis config file"),
            })?;

        let genesis =
            serde_json::from_str::<Genesis>(&json_str_without_comments).map_err(|_| {
                ValidationError::GenesisFileError {
                    error_message: format!("Failed to deserialize the genesis records."),
                }
            })?;

        genesis.validate(genesis_validation)?;
        Ok(genesis)
    }

    /// Reads Genesis from config and records files.
    pub fn from_files<P1, P2>(
        config_path: P1,
        records_path: P2,
        genesis_validation: GenesisValidationMode,
    ) -> Result<Self, ValidationError>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        let genesis_config = GenesisConfig::from_file(config_path).map_err(|error| {
            let error_message = error.to_string();
            ValidationError::GenesisFileError { error_message: error_message }
        })?;
        Self::new_with_path_validated(genesis_config, records_path, genesis_validation)
    }

    pub fn new_from_state_roots(config: GenesisConfig, state_roots: Vec<StateRoot>) -> Self {
        Self { config, contents: GenesisContents::StateRoots { state_roots } }
    }

    fn new_validated(
        config: GenesisConfig,
        records: GenesisRecords,
        genesis_validation: GenesisValidationMode,
    ) -> Result<Self, ValidationError> {
        let genesis = Self { config, contents: GenesisContents::Records { records } };
        genesis.validate(genesis_validation)?;
        Ok(genesis)
    }

    fn new_with_path_validated<P: AsRef<Path>>(
        config: GenesisConfig,
        records_file: P,
        genesis_validation: GenesisValidationMode,
    ) -> Result<Self, ValidationError> {
        let genesis = Self {
            config,
            contents: GenesisContents::RecordsFile {
                records_file: records_file.as_ref().to_path_buf(),
            },
        };
        genesis.validate(genesis_validation)?;
        Ok(genesis)
    }

    pub fn validate(
        &self,
        genesis_validation: GenesisValidationMode,
    ) -> Result<(), ValidationError> {
        match genesis_validation {
            GenesisValidationMode::Full => validate_genesis(self),
            GenesisValidationMode::UnsafeFast => {
                warn!(target: "genesis", "Skipped genesis validation");
                Ok(())
            }
        }
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

    /// If records vector is empty processes records stream from records_file.
    /// May panic if records_file is removed or is in wrong format.
    pub fn for_each_record(&self, mut callback: impl FnMut(&StateRecord)) {
        match &self.contents {
            GenesisContents::Records { records } => {
                for record in &records.0 {
                    callback(record);
                }
            }
            GenesisContents::RecordsFile { records_file } => {
                let callback_move = |record: StateRecord| {
                    callback(&record);
                };
                let reader = BufReader::new(
                    File::open(&records_file).expect("error while opening records file"),
                );
                stream_records_from_file(reader, callback_move)
                    .expect("error while streaming records");
            }
            GenesisContents::StateRoots { .. } => {
                unreachable!("Cannot iterate through records when genesis uses state roots");
            }
        }
    }

    /// Forces loading genesis records into memory.
    ///
    /// This is meant for **tests only**.  In production code you should be
    /// using [`Self::for_each_record`] instead to iterate over records.
    ///
    /// If the records are already loaded, simply returns mutable reference to
    /// them.  Otherwise, reads them from `records_file`, stores them in memory
    /// and then returns mutable reference to them.
    pub fn force_read_records(&mut self) -> &mut GenesisRecords {
        match &self.contents {
            GenesisContents::RecordsFile { records_file } => {
                self.contents =
                    GenesisContents::Records { records: GenesisRecords::from_file(records_file) };
            }
            GenesisContents::Records { .. } => {}
            GenesisContents::StateRoots { .. } => {
                unreachable!("Cannot iterate through records when genesis uses state roots");
            }
        }
        match &mut self.contents {
            GenesisContents::Records { records } => records,
            _ => {
                unreachable!("Records should have been set previously");
            }
        }
    }
}

/// Config for changes applied to state dump.
#[derive(Debug, Default)]
pub struct GenesisChangeConfig {
    pub select_account_ids: Option<Vec<AccountId>>,
    pub whitelist_validators: Option<HashSet<AccountId>>,
}

impl GenesisChangeConfig {
    pub fn with_select_account_ids(mut self, select_account_ids: Option<Vec<AccountId>>) -> Self {
        self.select_account_ids = select_account_ids;
        self
    }

    pub fn with_whitelist_validators(
        mut self,
        whitelist_validators: Option<Vec<AccountId>>,
    ) -> Self {
        self.whitelist_validators = match whitelist_validators {
            None => None,
            Some(whitelist) => Some(whitelist.into_iter().collect::<HashSet<AccountId>>()),
        };
        self
    }
}

// Note: this type cannot be placed in primitives/src/view.rs because of `RuntimeConfig` dependency issues.
// Ideally we should create `RuntimeConfigView`, but given the deeply nested nature and the number of fields inside
// `RuntimeConfig`, it should be its own endeavor.
// TODO: This has changed, there is now `RuntimeConfigView`. Reconsider if moving this is possible now.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
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
    pub protocol_upgrade_stake_threshold: Rational32,
    /// Epoch length counted in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Initial gas limit.
    pub gas_limit: Gas,
    /// Minimum gas price. It is also the initial gas price.
    #[serde(with = "dec_format")]
    pub min_gas_price: Balance,
    /// Maximum gas price.
    #[serde(with = "dec_format")]
    pub max_gas_price: Balance,
    /// Criterion for kicking out block producers (this is a number between 0 and 100)
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers (this is a number between 0 and 100)
    pub chunk_producer_kickout_threshold: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational32,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational32,
    /// Gas price adjustment rate
    pub gas_price_adjustment_rate: Rational32,
    /// Runtime configuration (mostly economics constants).
    pub runtime_config: RuntimeConfigView,
    /// Number of blocks for which a given transaction is valid
    pub transaction_validity_period: NumBlocks,
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational32,
    /// Maximum inflation on the total supply every epoch.
    pub max_inflation_rate: Rational32,
    /// Expected number of blocks per year
    pub num_blocks_per_year: NumBlocks,
    /// Protocol treasury account
    pub protocol_treasury_account: AccountId,
    /// Fishermen stake threshold.
    #[serde(with = "dec_format")]
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
}

pub struct ProtocolConfig {
    pub genesis_config: GenesisConfig,
    pub runtime_config: RuntimeConfig,
}

impl From<ProtocolConfig> for ProtocolConfigView {
    fn from(protocol_config: ProtocolConfig) -> Self {
        let ProtocolConfig { genesis_config, runtime_config } = protocol_config;

        ProtocolConfigView {
            protocol_version: genesis_config.protocol_version,
            genesis_time: genesis_config.genesis_time,
            chain_id: genesis_config.chain_id,
            genesis_height: genesis_config.genesis_height,
            num_block_producer_seats: genesis_config.num_block_producer_seats,
            num_block_producer_seats_per_shard: genesis_config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: genesis_config
                .avg_hidden_validator_seats_per_shard,
            dynamic_resharding: genesis_config.dynamic_resharding,
            protocol_upgrade_stake_threshold: genesis_config.protocol_upgrade_stake_threshold,
            epoch_length: genesis_config.epoch_length,
            gas_limit: genesis_config.gas_limit,
            min_gas_price: genesis_config.min_gas_price,
            max_gas_price: genesis_config.max_gas_price,
            block_producer_kickout_threshold: genesis_config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: genesis_config.chunk_producer_kickout_threshold,
            online_min_threshold: genesis_config.online_min_threshold,
            online_max_threshold: genesis_config.online_max_threshold,
            gas_price_adjustment_rate: genesis_config.gas_price_adjustment_rate,
            runtime_config: RuntimeConfigView::from(runtime_config),
            transaction_validity_period: genesis_config.transaction_validity_period,
            protocol_reward_rate: genesis_config.protocol_reward_rate,
            max_inflation_rate: genesis_config.max_inflation_rate,
            num_blocks_per_year: genesis_config.num_blocks_per_year,
            protocol_treasury_account: genesis_config.protocol_treasury_account,
            fishermen_threshold: genesis_config.fishermen_threshold,
            minimum_stake_divisor: genesis_config.minimum_stake_divisor,
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

#[cfg(test)]
mod test {
    use crate::genesis_config::RecordsProcessor;
    use crate::{Genesis, GenesisValidationMode};
    use near_primitives::state_record::StateRecord;
    use serde::Deserializer;

    fn stream_records_from_json_str(genesis: &str) -> serde_json::Result<()> {
        let mut deserializer = serde_json::Deserializer::from_reader(genesis.as_bytes());
        let records_processor = RecordsProcessor { sink: &mut |_record: StateRecord| {} };
        deserializer.deserialize_any(records_processor)
    }

    #[test]
    fn test_genesis_with_empty_records() {
        let genesis = r#"{
            "a": [1, 2],
            "b": "random",
            "records": []
        }"#;
        stream_records_from_json_str(genesis).expect("error reading empty records");
    }

    #[test]
    #[should_panic(expected = "missing field: records")]
    fn test_genesis_with_no_records() {
        let genesis = r#"{
            "a": [1, 2],
            "b": "random"
        }"#;
        stream_records_from_json_str(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "duplicate field: records")]
    fn test_genesis_with_several_records_fields() {
        let genesis = r#"{
            "a": [1, 2],
            "records": [{
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                }],
            "b": "random",
            "records": [{
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                }]
        }"#;
        stream_records_from_json_str(genesis).unwrap();
    }

    #[test]
    fn test_genesis_with_fields_after_records() {
        let genesis = r#"{
            "a": [1, 2],
            "b": "random",
            "records": [
                {
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                }
            ],
            "c": {
                "d": 1,
                "e": []
            }
        }"#;
        stream_records_from_json_str(genesis).expect("error reading records with a field after");
    }

    #[test]
    fn test_genesis_with_fields_before_records() {
        let genesis = r#"{
            "a": [1, 2],
            "b": "random",
            "c": {
                "d": 1,
                "e": []
            },
            "records": [
                {
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                }
            ]
        }"#;
        stream_records_from_json_str(genesis).expect("error reading records from genesis");
    }

    #[test]
    fn test_genesis_with_several_records() {
        let genesis = r#"{
            "a": [1, 2],
            "b": "random",
            "c": {
                "d": 1,
                "e": []
            },
            "records": [
                {
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                },
                {
                    "Account": {
                        "account_id": "01.near",
                        "account": {
                              "amount": "49999999958035075000000000",
                              "locked": "0",
                              "code_hash": "11111111111111111111111111111111",
                              "storage_usage": 264
                        }
                    }
                }
            ]
        }"#;
        stream_records_from_json_str(genesis).expect("error reading records from genesis");
    }

    #[test]
    fn test_loading_localnet_genesis() {
        let genesis_str = r#"{
              "protocol_version": 57,
              "genesis_time": "2022-11-15T05:17:59.578706Z",
              "chain_id": "localnet",
              "genesis_height": 0,
              "num_block_producer_seats": 50,
              "num_block_producer_seats_per_shard": [
                50
              ],
              "avg_hidden_validator_seats_per_shard": [
                0
              ],
              "dynamic_resharding": false,
              "protocol_upgrade_stake_threshold": [
                4,
                5
              ],
              "protocol_upgrade_num_epochs": 2,
              "epoch_length": 60,
              "gas_limit": 1000000000000000,
              "min_gas_price": "100000000",
              "max_gas_price": "10000000000000000000000",
              "block_producer_kickout_threshold": 90,
              "chunk_producer_kickout_threshold": 90,
              "online_min_threshold": [
                9,
                10
              ],
              "online_max_threshold": [
                99,
                100
              ],
              "gas_price_adjustment_rate": [
                1,
                100
              ],
              "validators": [
                {
                  "account_id": "test.near",
                  "public_key": "ed25519:Gc4yTakj3QVm5T9XpsFNooVKBxXcYnhnuQdGMXf5Hjcf",
                  "amount": "50000000000000000000000000000000"
                }
              ],
              "transaction_validity_period": 100,
              "protocol_reward_rate": [
                1,
                10
              ],
              "max_inflation_rate": [
                1,
                20
              ],
              "total_supply": "2050000000000000000000000000000000",
              "num_blocks_per_year": 31536000,
              "protocol_treasury_account": "test.near",
              "fishermen_threshold": "10000000000000000000000000",
              "minimum_stake_divisor": 10,
              "shard_layout": {
                "V0": {
                  "num_shards": 1,
                  "version": 0
                }
              },
              "num_chunk_only_producer_seats": 300,
              "minimum_validators_per_shard": 1,
              "max_kickout_stake_perc": 100,
              "minimum_stake_ratio": [
                1,
                6250
              ],
              "use_production_config": false,
              "records": [
                {
                  "Account": {
                    "account_id": "test.near",
                    "account": {
                      "amount": "1000000000000000000000000000000000",
                      "locked": "50000000000000000000000000000000",
                      "code_hash": "11111111111111111111111111111111",
                      "storage_usage": 0,
                      "version": "V1"
                    }
                  }
                },
                {
                  "AccessKey": {
                    "account_id": "test.near",
                    "public_key": "ed25519:Gc4yTakj3QVm5T9XpsFNooVKBxXcYnhnuQdGMXf5Hjcf",
                    "access_key": {
                      "nonce": 0,
                      "permission": "FullAccess"
                    }
                  }
                },
                {
                  "Account": {
                    "account_id": "near",
                    "account": {
                      "amount": "1000000000000000000000000000000000",
                      "locked": "0",
                      "code_hash": "11111111111111111111111111111111",
                      "storage_usage": 0,
                      "version": "V1"
                    }
                  }
                },
                {
                  "AccessKey": {
                    "account_id": "near",
                    "public_key": "ed25519:546XB2oHhj7PzUKHiH9Xve3Ze5q1JiW2WTh6abXFED3c",
                    "access_key": {
                      "nonce": 0,
                      "permission": "FullAccess"
                    }
                  }
                }
              ]
        }"#;
        let genesis =
            serde_json::from_str::<Genesis>(&genesis_str).expect("Failed to deserialize Genesis");
        genesis.validate(GenesisValidationMode::Full).expect("Failed to validate Genesis");
    }

    #[test]
    fn test_loading_localnet_genesis_without_records() {
        let genesis_str = r#"{
              "protocol_version": 57,
              "genesis_time": "2022-11-15T05:17:59.578706Z",
              "chain_id": "localnet",
              "genesis_height": 0,
              "num_block_producer_seats": 50,
              "num_block_producer_seats_per_shard": [
                50
              ],
              "avg_hidden_validator_seats_per_shard": [
                0
              ],
              "dynamic_resharding": false,
              "protocol_upgrade_stake_threshold": [
                4,
                5
              ],
              "protocol_upgrade_num_epochs": 2,
              "epoch_length": 60,
              "gas_limit": 1000000000000000,
              "min_gas_price": "100000000",
              "max_gas_price": "10000000000000000000000",
              "block_producer_kickout_threshold": 90,
              "chunk_producer_kickout_threshold": 90,
              "online_min_threshold": [
                9,
                10
              ],
              "online_max_threshold": [
                99,
                100
              ],
              "gas_price_adjustment_rate": [
                1,
                100
              ],
              "validators": [
                {
                  "account_id": "test.near",
                  "public_key": "ed25519:Gc4yTakj3QVm5T9XpsFNooVKBxXcYnhnuQdGMXf5Hjcf",
                  "amount": "50000000000000000000000000000000"
                }
              ],
              "transaction_validity_period": 100,
              "protocol_reward_rate": [
                1,
                10
              ],
              "max_inflation_rate": [
                1,
                20
              ],
              "total_supply": "2050000000000000000000000000000000",
              "num_blocks_per_year": 31536000,
              "protocol_treasury_account": "test.near",
              "fishermen_threshold": "10000000000000000000000000",
              "minimum_stake_divisor": 10,
              "shard_layout": {
                "V0": {
                  "num_shards": 1,
                  "version": 0
                }
              },
              "num_chunk_only_producer_seats": 300,
              "minimum_validators_per_shard": 1,
              "max_kickout_stake_perc": 100,
              "minimum_stake_ratio": [
                1,
                6250
              ],
              "use_production_config": false
        }"#;
        let _genesis =
            serde_json::from_str::<Genesis>(&genesis_str).expect("Failed to deserialize Genesis");
    }
}
