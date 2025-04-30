use crate::num_rational::Rational32;
use crate::shard_layout::ShardLayout;
use crate::types::validator_stake::ValidatorStake;
use crate::types::{
    AccountId, Balance, BlockChunkValidatorStats, BlockHeightDelta, NumSeats, ProtocolVersion,
    ValidatorKickoutReason,
};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::serialize::dec_format;
use near_primitives_core::version::PROTOCOL_VERSION;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const AGGREGATOR_KEY: &[u8] = b"AGGREGATOR";

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone, Eq, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EpochConfig {
    /// Epoch length in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Number of seats for block producers.
    pub num_block_producer_seats: NumSeats,
    /// Number of seats of block producers per each shard.
    pub num_block_producer_seats_per_shard: Vec<NumSeats>,
    /// Expected number of hidden validator seats per each shard.
    pub avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    /// Threshold for kicking out block producers.
    pub block_producer_kickout_threshold: u8,
    /// Threshold for kicking out chunk producers.
    pub chunk_producer_kickout_threshold: u8,
    /// Threshold for kicking out nodes which are only chunk validators.
    pub chunk_validator_only_kickout_threshold: u8,
    /// Number of target chunk validator mandates for each shard.
    pub target_validator_mandates_per_shard: NumSeats,
    /// Max ratio of validators that we can kick out in an epoch
    pub validator_max_kickout_stake_perc: u8,
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational32,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational32,
    /// Stake threshold for becoming a fisherman.
    #[serde(with = "dec_format")]
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational32,
    /// Shard layout of this epoch, may change from epoch to epoch
    pub shard_layout: ShardLayout,
    /// Additional configuration parameters for the new validator selection
    /// algorithm. See <https://github.com/near/NEPs/pull/167> for details.
    // #[default(100)]
    pub num_chunk_producer_seats: NumSeats,
    // #[default(300)]
    pub num_chunk_validator_seats: NumSeats,
    // TODO (#11267): deprecate after StatelessValidationV0 is in place.
    // Use 300 for older protocol versions.
    // #[default(300)]
    pub num_chunk_only_producer_seats: NumSeats,
    // #[default(1)]
    pub minimum_validators_per_shard: NumSeats,
    // #[default(Rational32::new(160, 1_000_000))]
    pub minimum_stake_ratio: Rational32,
    // #[default(5)]
    /// Limits the number of shard changes in chunk producer assignments,
    /// if algorithm is able to choose assignment with better balance of
    /// number of chunk producers for shards.
    pub chunk_producer_assignment_changes_limit: NumSeats,
    // #[default(false)]
    pub shuffle_shard_assignment_for_chunk_producers: bool,
}

impl EpochConfig {
    /// Total number of validator seats in the epoch since protocol version 69.
    pub fn num_validators(&self) -> NumSeats {
        self.num_block_producer_seats
            .max(self.num_chunk_producer_seats)
            .max(self.num_chunk_validator_seats)
    }
}

impl EpochConfig {
    // Create test-only epoch config.
    // Not depends on genesis!
    pub fn genesis_test(
        num_block_producer_seats: NumSeats,
        shard_layout: ShardLayout,
        epoch_length: BlockHeightDelta,
        block_producer_kickout_threshold: u8,
        chunk_producer_kickout_threshold: u8,
        chunk_validator_only_kickout_threshold: u8,
        protocol_upgrade_stake_threshold: Rational32,
        fishermen_threshold: Balance,
    ) -> Self {
        Self {
            epoch_length,
            num_block_producer_seats,
            num_block_producer_seats_per_shard: vec![
                num_block_producer_seats;
                shard_layout.shard_ids().count()
            ],
            avg_hidden_validator_seats_per_shard: vec![],
            target_validator_mandates_per_shard: 68,
            validator_max_kickout_stake_perc: 100,
            online_min_threshold: Rational32::new(90, 100),
            online_max_threshold: Rational32::new(99, 100),
            minimum_stake_divisor: 10,
            protocol_upgrade_stake_threshold,
            block_producer_kickout_threshold,
            chunk_producer_kickout_threshold,
            chunk_validator_only_kickout_threshold,
            fishermen_threshold,
            shard_layout,
            num_chunk_producer_seats: 100,
            num_chunk_validator_seats: 300,
            num_chunk_only_producer_seats: 300,
            minimum_validators_per_shard: 1,
            minimum_stake_ratio: Rational32::new(160i32, 1_000_000i32),
            chunk_producer_assignment_changes_limit: 5,
            shuffle_shard_assignment_for_chunk_producers: false,
        }
    }

    /// Minimal config for testing.
    pub fn minimal() -> Self {
        Self {
            epoch_length: 0,
            num_block_producer_seats: 0,
            num_block_producer_seats_per_shard: vec![],
            avg_hidden_validator_seats_per_shard: vec![],
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            chunk_validator_only_kickout_threshold: 0,
            target_validator_mandates_per_shard: 0,
            validator_max_kickout_stake_perc: 0,
            online_min_threshold: 0.into(),
            online_max_threshold: 0.into(),
            fishermen_threshold: 0,
            minimum_stake_divisor: 0,
            protocol_upgrade_stake_threshold: 0.into(),
            shard_layout: ShardLayout::get_simple_nightshade_layout(),
            num_chunk_producer_seats: 100,
            num_chunk_validator_seats: 300,
            num_chunk_only_producer_seats: 300,
            minimum_validators_per_shard: 1,
            minimum_stake_ratio: Rational32::new(160i32, 1_000_000i32),
            chunk_producer_assignment_changes_limit: 5,
            shuffle_shard_assignment_for_chunk_producers: false,
        }
    }

    pub fn mock(epoch_length: BlockHeightDelta, shard_layout: ShardLayout) -> Self {
        Self {
            epoch_length,
            num_block_producer_seats: 2,
            num_block_producer_seats_per_shard: vec![1, 1],
            avg_hidden_validator_seats_per_shard: vec![1, 1],
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            chunk_validator_only_kickout_threshold: 0,
            target_validator_mandates_per_shard: 1,
            validator_max_kickout_stake_perc: 0,
            online_min_threshold: Rational32::new(1i32, 4i32),
            online_max_threshold: Rational32::new(3i32, 4i32),
            fishermen_threshold: 1,
            minimum_stake_divisor: 1,
            protocol_upgrade_stake_threshold: Rational32::new(3i32, 4i32),
            shard_layout,
            num_chunk_producer_seats: 100,
            num_chunk_validator_seats: 300,
            num_chunk_only_producer_seats: 300,
            minimum_validators_per_shard: 1,
            minimum_stake_ratio: Rational32::new(160i32, 1_000_000i32),
            chunk_producer_assignment_changes_limit: 5,
            shuffle_shard_assignment_for_chunk_producers: false,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardConfig {
    pub num_block_producer_seats_per_shard: Vec<NumSeats>,
    pub avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    pub shard_layout: ShardLayout,
}

impl ShardConfig {
    pub fn new(epoch_config: EpochConfig) -> Self {
        Self {
            num_block_producer_seats_per_shard: epoch_config
                .num_block_producer_seats_per_shard
                .clone(),
            avg_hidden_validator_seats_per_shard: epoch_config
                .avg_hidden_validator_seats_per_shard
                .clone(),
            shard_layout: epoch_config.shard_layout,
        }
    }
}

/// Testing overrides to apply to the EpochConfig returned by the `for_protocol_version`.
/// All fields should be optional and the default should be a no-op.
#[derive(Clone, Debug, Default)]
pub struct AllEpochConfigTestOverrides {
    pub block_producer_kickout_threshold: Option<u8>,
    pub chunk_producer_kickout_threshold: Option<u8>,
}

/// AllEpochConfig manages protocol configs that might be changing throughout epochs (hence EpochConfig).
/// The main function in AllEpochConfig is ::for_protocol_version which takes a protocol version
/// and returns the EpochConfig that should be used for this protocol version.
#[derive(Debug, Clone)]
pub struct AllEpochConfig {
    /// Store for EpochConfigs, provides configs per protocol version.
    /// Initialized only for production, ie. when `use_protocol_version` is true.
    config_store: EpochConfigStore,
    /// Chain Id. Some parameters are specific to certain chains.
    chain_id: String,
    epoch_length: BlockHeightDelta,
}

impl AllEpochConfig {
    pub fn from_epoch_config_store(
        chain_id: &str,
        epoch_length: BlockHeightDelta,
        config_store: EpochConfigStore,
    ) -> Self {
        Self { config_store, chain_id: chain_id.to_string(), epoch_length }
    }

    pub fn for_protocol_version(&self, protocol_version: ProtocolVersion) -> EpochConfig {
        let mut config = self.config_store.get_config(protocol_version).as_ref().clone();
        // TODO(#11265): epoch length is overridden in many tests so we
        // need to support it here. Consider removing `epoch_length` from
        // EpochConfig.
        config.epoch_length = self.epoch_length;
        config
    }

    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }
}

#[derive(BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct EpochSummary {
    pub prev_epoch_last_block_hash: CryptoHash,
    /// Proposals from the epoch, only the latest one per account
    pub all_proposals: Vec<ValidatorStake>,
    /// Kickout set, includes slashed
    pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    /// Only for validators who met the threshold and didn't get slashed
    pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
    /// Protocol version for next next epoch, as summary of epoch T defines
    /// epoch T+2.
    pub next_next_epoch_version: ProtocolVersion,
}

macro_rules! include_config {
    ($chain:expr, $version:expr, $file:expr) => {
        (
            $chain,
            $version,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/res/epoch_configs/",
                $chain,
                "/",
                $file
            )),
        )
    };
}

/// List of (chain_id, version, JSON content) tuples used to initialize the EpochConfigStore.
static CONFIGS: &[(&str, ProtocolVersion, &str)] = &[
    // Epoch configs for mainnet (genesis protocol version is 29).
    include_config!("mainnet", 29, "29.json"),
    include_config!("mainnet", 48, "48.json"),
    include_config!("mainnet", 56, "56.json"),
    include_config!("mainnet", 64, "64.json"),
    include_config!("mainnet", 65, "65.json"),
    include_config!("mainnet", 69, "69.json"),
    include_config!("mainnet", 70, "70.json"),
    include_config!("mainnet", 71, "71.json"),
    include_config!("mainnet", 72, "72.json"),
    include_config!("mainnet", 75, "75.json"),
    include_config!("mainnet", 76, "76.json"),
    include_config!("mainnet", 78, "78.json"),
    include_config!("mainnet", 143, "143.json"),
    // Epoch configs for testnet (genesis protocol version is 29).
    include_config!("testnet", 29, "29.json"),
    include_config!("testnet", 48, "48.json"),
    include_config!("testnet", 56, "56.json"),
    include_config!("testnet", 64, "64.json"),
    include_config!("testnet", 65, "65.json"),
    include_config!("testnet", 69, "69.json"),
    include_config!("testnet", 70, "70.json"),
    include_config!("testnet", 71, "71.json"),
    include_config!("testnet", 72, "72.json"),
    include_config!("testnet", 75, "75.json"),
    include_config!("testnet", 76, "76.json"),
    include_config!("mainnet", 78, "78.json"),
    include_config!("testnet", 143, "143.json"),
];

/// Store for `[EpochConfig]` per protocol version.`
#[derive(Debug, Clone)]
pub struct EpochConfigStore {
    store: BTreeMap<ProtocolVersion, Arc<EpochConfig>>,
}

impl EpochConfigStore {
    /// Creates a config store to contain the EpochConfigs for the given chain parsed from the JSON files.
    /// If no configs are found for the given chain, try to load the configs from the file system.
    /// If there are no configs found, return None.
    pub fn for_chain_id(chain_id: &str, config_dir: Option<PathBuf>) -> Option<Self> {
        let mut store = Self::load_default_epoch_configs(chain_id);

        if !store.is_empty() {
            return Some(Self { store });
        }
        if let Some(config_dir) = config_dir {
            store = Self::load_epoch_config_from_file_system(config_dir.to_str().unwrap());
        }

        if store.is_empty() { None } else { Some(Self { store }) }
    }

    /// Loads the default epoch configs for the given chain from the CONFIGS array.
    fn load_default_epoch_configs(chain_id: &str) -> BTreeMap<ProtocolVersion, Arc<EpochConfig>> {
        let mut store = BTreeMap::new();
        for (chain, version, content) in CONFIGS {
            if *chain == chain_id {
                let config: EpochConfig = serde_json::from_str(*content).unwrap_or_else(|e| {
                    panic!(
                        "Failed to load epoch config files for chain {} and version {}: {:#}",
                        chain_id, version, e
                    )
                });
                store.insert(*version, Arc::new(config));
            }
        }
        store
    }

    /// Reads the json files from the epoch config directory.
    fn load_epoch_config_from_file_system(
        directory: &str,
    ) -> BTreeMap<ProtocolVersion, Arc<EpochConfig>> {
        fn get_epoch_config(
            dir_entry: fs::DirEntry,
        ) -> Option<(ProtocolVersion, Arc<EpochConfig>)> {
            let path = dir_entry.path();
            if !(path.extension()? == "json") {
                return None;
            }
            let file_name = path.file_stem()?.to_str()?.to_string();
            let protocol_version = file_name.parse().expect("Invalid protocol version");
            if protocol_version > PROTOCOL_VERSION {
                return None;
            }
            let contents = fs::read_to_string(&path).ok()?;
            let epoch_config = serde_json::from_str(&contents).unwrap_or_else(|_| {
                panic!("Failed to parse epoch config for version {}", protocol_version)
            });
            Some((protocol_version, epoch_config))
        }

        fs::read_dir(directory)
            .expect("Failed opening epoch config directory")
            .filter_map(Result::ok)
            .filter_map(get_epoch_config)
            .collect()
    }

    pub fn test(store: BTreeMap<ProtocolVersion, Arc<EpochConfig>>) -> Self {
        Self { store }
    }

    pub fn test_single_version(
        protocol_version: ProtocolVersion,
        epoch_config: EpochConfig,
    ) -> Self {
        Self::test(BTreeMap::from([(protocol_version, Arc::new(epoch_config))]))
    }

    /// Returns the EpochConfig for the given protocol version.
    /// This panics if no config is found for the given version, thus the initialization via `for_chain_id` should
    /// only be performed for chains with some configs stored in files.
    pub fn get_config(&self, protocol_version: ProtocolVersion) -> &Arc<EpochConfig> {
        self.store
            .range((Bound::Unbounded, Bound::Included(protocol_version)))
            .next_back()
            .unwrap_or_else(|| {
                panic!("Failed to find EpochConfig for protocol version {}", protocol_version)
            })
            .1
    }

    fn dump_epoch_config(directory: &Path, version: &ProtocolVersion, config: &Arc<EpochConfig>) {
        let content = serde_json::to_string_pretty(config.as_ref()).unwrap();
        let path = PathBuf::from(directory).join(format!("{}.json", version));
        fs::write(path, content).unwrap();
    }

    /// Dumps all the configs between the beginning and end protocol versions to the given directory.
    /// If the beginning version doesn't exist, the closest config to it will be dumped.
    pub fn dump_epoch_configs_between(
        &self,
        first_version: Option<&ProtocolVersion>,
        last_version: Option<&ProtocolVersion>,
        directory: impl AsRef<Path>,
    ) {
        // Dump all the configs between the beginning and end versions, inclusive.
        self.store
            .iter()
            .filter(|(version, _)| {
                first_version.is_none_or(|first_version| *version >= first_version)
            })
            .filter(|(version, _)| last_version.is_none_or(|last_version| *version <= last_version))
            .for_each(|(version, config)| {
                Self::dump_epoch_config(directory.as_ref(), version, config);
            });

        // Dump the closest config to the beginning version if it doesn't exist.
        if let Some(first_version) = first_version {
            if !self.store.contains_key(&first_version) {
                let config = self.get_config(*first_version);
                Self::dump_epoch_config(directory.as_ref(), first_version, config);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EpochConfigStore;
    use crate::epoch_manager::EpochConfig;
    use near_primitives_core::types::ProtocolVersion;
    use near_primitives_core::version::PROTOCOL_VERSION;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_dump_epoch_configs_mainnet() {
        let tmp_dir = tempfile::tempdir().unwrap();
        EpochConfigStore::for_chain_id("mainnet", None).unwrap().dump_epoch_configs_between(
            Some(&55),
            Some(&68),
            tmp_dir.path().to_str().unwrap(),
        );

        // Check if tmp dir contains the dumped files. 55, 64, 65.
        let dumped_files = fs::read_dir(tmp_dir.path()).unwrap();
        let dumped_files: Vec<_> =
            dumped_files.map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect();

        assert!(dumped_files.contains(&String::from("55.json")));
        assert!(dumped_files.contains(&String::from("64.json")));
        assert!(dumped_files.contains(&String::from("65.json")));

        // Check if 55.json is equal to 48.json from res/epoch_configs/mainnet.
        let contents_55 = fs::read_to_string(tmp_dir.path().join("55.json")).unwrap();
        let epoch_config_55: EpochConfig = serde_json::from_str(&contents_55).unwrap();
        let epoch_config_48 = parse_config_file("mainnet", 48).unwrap();
        assert_eq!(epoch_config_55, epoch_config_48);
    }

    #[test]
    fn test_dump_and_load_epoch_configs_mainnet() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let epoch_configs = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
        epoch_configs.dump_epoch_configs_between(
            Some(&55),
            Some(&68),
            tmp_dir.path().to_str().unwrap(),
        );

        let loaded_epoch_configs = EpochConfigStore::test(
            EpochConfigStore::load_epoch_config_from_file_system(tmp_dir.path().to_str().unwrap()),
        );

        // Insert a config for an newer protocol version. It should be ignored.
        EpochConfigStore::dump_epoch_config(
            tmp_dir.path(),
            &(PROTOCOL_VERSION + 1),
            &epoch_configs.get_config(PROTOCOL_VERSION),
        );

        let loaded_after_insert_epoch_configs = EpochConfigStore::test(
            EpochConfigStore::load_epoch_config_from_file_system(tmp_dir.path().to_str().unwrap()),
        );
        assert_eq!(loaded_epoch_configs.store, loaded_after_insert_epoch_configs.store);

        // Insert a config for an older protocol version. It should be loaded.
        EpochConfigStore::dump_epoch_config(
            tmp_dir.path(),
            &(PROTOCOL_VERSION - 22),
            &epoch_configs.get_config(PROTOCOL_VERSION),
        );

        let loaded_after_insert_epoch_configs = EpochConfigStore::test(
            EpochConfigStore::load_epoch_config_from_file_system(tmp_dir.path().to_str().unwrap()),
        );
        assert_ne!(loaded_epoch_configs.store, loaded_after_insert_epoch_configs.store);
    }

    fn parse_config_file(chain_id: &str, protocol_version: ProtocolVersion) -> Option<EpochConfig> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("res/epoch_configs")
            .join(chain_id)
            .join(format!("{}.json", protocol_version));
        if path.exists() {
            let content = fs::read_to_string(path).unwrap();
            let config: EpochConfig = serde_json::from_str(&content).unwrap();
            Some(config)
        } else {
            None
        }
    }
}
