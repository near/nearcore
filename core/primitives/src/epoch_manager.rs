use crate::challenge::SlashedValidator;
use crate::num_rational::Rational32;
use crate::shard_layout::ShardLayout;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeV1};
use crate::types::{
    AccountId, Balance, BlockChunkValidatorStats, BlockHeightDelta, EpochId, NumSeats,
    ProtocolVersion, ValidatorKickoutReason,
};
use crate::version::ProtocolFeature;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_schema_checker_lib::ProtocolSchema;
use smart_default::SmartDefault;
use std::collections::HashMap;

pub const AGGREGATOR_KEY: &[u8] = b"AGGREGATOR";

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone, Eq, Debug, PartialEq)]
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
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational32,
    /// Shard layout of this epoch, may change from epoch to epoch
    pub shard_layout: ShardLayout,
    /// Additional config for validator selection algorithm
    pub validator_selection_config: ValidatorSelectionConfig,
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
#[derive(Clone, Default)]
pub struct AllEpochConfigTestOverrides {
    pub block_producer_kickout_threshold: Option<u8>,
    pub chunk_producer_kickout_threshold: Option<u8>,
}

/// AllEpochConfig manages protocol configs that might be changing throughout epochs (hence EpochConfig).
/// The main function in AllEpochConfig is ::for_protocol_version which takes a protocol version
/// and returns the EpochConfig that should be used for this protocol version.
#[derive(Clone)]
pub struct AllEpochConfig {
    /// Whether this is for production (i.e., mainnet or testnet). This is a temporary implementation
    /// to allow us to change protocol config for mainnet and testnet without changing the genesis config
    use_production_config: bool,
    /// EpochConfig from genesis
    genesis_epoch_config: EpochConfig,
    /// Chain Id. Some parameters are specific to certain chains.
    chain_id: String,

    /// Testing overrides to apply to the EpochConfig returned by the `for_protocol_version`.
    test_overrides: AllEpochConfigTestOverrides,
}

impl AllEpochConfig {
    pub fn new(
        use_production_config: bool,
        genesis_epoch_config: EpochConfig,
        chain_id: &str,
    ) -> Self {
        Self {
            use_production_config,
            genesis_epoch_config,
            chain_id: chain_id.to_string(),
            test_overrides: AllEpochConfigTestOverrides::default(),
        }
    }

    pub fn new_with_test_overrides(
        use_production_config: bool,
        genesis_epoch_config: EpochConfig,
        chain_id: &str,
        test_overrides: Option<AllEpochConfigTestOverrides>,
    ) -> Self {
        Self {
            use_production_config,
            genesis_epoch_config,
            chain_id: chain_id.to_string(),
            test_overrides: test_overrides.unwrap_or_default(),
        }
    }

    pub fn for_protocol_version(&self, protocol_version: ProtocolVersion) -> EpochConfig {
        let mut config = self.genesis_epoch_config.clone();

        Self::config_mocknet(&mut config, &self.chain_id);

        if !self.use_production_config {
            return config;
        }

        Self::config_validator_selection(&mut config, protocol_version);

        Self::config_nightshade(&mut config, protocol_version);

        Self::config_chunk_only_producers(&mut config, &self.chain_id, protocol_version);

        Self::config_max_kickout_stake(&mut config, protocol_version);

        Self::config_test_overrides(&mut config, &self.test_overrides);

        config
    }

    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }

    /// Configures mocknet-specific features only.
    fn config_mocknet(config: &mut EpochConfig, chain_id: &str) {
        if chain_id != near_primitives_core::chains::MOCKNET {
            return;
        }
        // In production (mainnet/testnet) and nightly environments this setting is guarded by
        // ProtocolFeature::ShuffleShardAssignments. (see config_validator_selection function).
        // For pre-release environment such as mocknet, which uses features between production and nightly
        // (eg. stateless validation) we enable it by default with stateless validation in order to exercise
        // the codepaths for state sync more often.
        // TODO(#11201): When stabilizing "ShuffleShardAssignments" in mainnet,
        // also remove this temporary code and always rely on ShuffleShardAssignments.
        config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers = true;
    }

    /// Configures validator-selection related features.
    fn config_validator_selection(config: &mut EpochConfig, protocol_version: ProtocolVersion) {
        // Shuffle shard assignments every epoch, to trigger state sync more
        // frequently to exercise that code path.
        if checked_feature!("stable", ShuffleShardAssignments, protocol_version) {
            config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers = true;
        }
    }

    fn config_nightshade(config: &mut EpochConfig, protocol_version: ProtocolVersion) {
        // Unlike the other checks, this one is for strict equality. The testonly nightshade layout
        // is specifically used in resharding tests, not for any other protocol versions.
        #[cfg(feature = "nightly")]
        if protocol_version == ProtocolFeature::SimpleNightshadeTestonly.protocol_version() {
            Self::config_nightshade_impl(
                config,
                ShardLayout::get_simple_nightshade_layout_testonly(),
            );
            return;
        }

        if checked_feature!("stable", SimpleNightshadeV3, protocol_version) {
            Self::config_nightshade_impl(config, ShardLayout::get_simple_nightshade_layout_v3());
            return;
        }

        if checked_feature!("stable", SimpleNightshadeV2, protocol_version) {
            Self::config_nightshade_impl(config, ShardLayout::get_simple_nightshade_layout_v2());
            return;
        }

        if checked_feature!("stable", SimpleNightshade, protocol_version) {
            Self::config_nightshade_impl(config, ShardLayout::get_simple_nightshade_layout());
            return;
        }
    }

    fn config_nightshade_impl(config: &mut EpochConfig, shard_layout: ShardLayout) {
        let num_block_producer_seats = config.num_block_producer_seats;
        config.num_block_producer_seats_per_shard =
            shard_layout.shard_ids().map(|_| num_block_producer_seats).collect();
        config.avg_hidden_validator_seats_per_shard = shard_layout.shard_ids().map(|_| 0).collect();
        config.shard_layout = shard_layout;
    }

    fn config_chunk_only_producers(
        config: &mut EpochConfig,
        chain_id: &str,
        protocol_version: u32,
    ) {
        if checked_feature!("stable", ChunkOnlyProducers, protocol_version) {
            // On testnet, genesis config set num_block_producer_seats to 200
            // This is to bring it back to 100 to be the same as on mainnet
            config.num_block_producer_seats = 100;
            // Technically, after ChunkOnlyProducers is enabled, this field is no longer used
            // We still set it here just in case
            config.num_block_producer_seats_per_shard =
                config.shard_layout.shard_ids().map(|_| 100).collect();
            config.block_producer_kickout_threshold = 80;
            config.chunk_producer_kickout_threshold = 80;
            config.validator_selection_config.num_chunk_only_producer_seats = 200;
        }

        // Adjust the number of block and chunk producers for testnet, to make it easier to test the change.
        if chain_id == near_primitives_core::chains::TESTNET
            && checked_feature!("stable", TestnetFewerBlockProducers, protocol_version)
        {
            let shard_ids = config.shard_layout.shard_ids();
            // Decrease the number of block and chunk producers from 100 to 20.
            config.num_block_producer_seats = 20;
            // Checking feature NoChunkOnlyProducers in stateless validation
            if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
                config.validator_selection_config.num_chunk_producer_seats = 20;
            }
            config.num_block_producer_seats_per_shard =
                shard_ids.map(|_| config.num_block_producer_seats).collect();
            // Decrease the number of chunk producers.
            config.validator_selection_config.num_chunk_only_producer_seats = 100;
        }

        // Checking feature NoChunkOnlyProducers in stateless validation
        if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
            // Make sure there is no chunk only producer in stateless validation
            config.validator_selection_config.num_chunk_only_producer_seats = 0;
        }
    }

    fn config_max_kickout_stake(config: &mut EpochConfig, protocol_version: u32) {
        if checked_feature!("stable", MaxKickoutStake, protocol_version) {
            config.validator_max_kickout_stake_perc = 30;
        }
    }

    fn config_test_overrides(
        config: &mut EpochConfig,
        test_overrides: &AllEpochConfigTestOverrides,
    ) {
        if let Some(block_producer_kickout_threshold) =
            test_overrides.block_producer_kickout_threshold
        {
            config.block_producer_kickout_threshold = block_producer_kickout_threshold;
        }

        if let Some(chunk_producer_kickout_threshold) =
            test_overrides.chunk_producer_kickout_threshold
        {
            config.chunk_producer_kickout_threshold = chunk_producer_kickout_threshold;
        }
    }
}

/// Additional configuration parameters for the new validator selection
/// algorithm.  See <https://github.com/near/NEPs/pull/167> for details.
#[derive(Debug, Clone, SmartDefault, PartialEq, Eq)]
pub struct ValidatorSelectionConfig {
    #[default(100)]
    pub num_chunk_producer_seats: NumSeats,
    #[default(300)]
    pub num_chunk_validator_seats: NumSeats,
    // TODO (#11267): deprecate after StatelessValidationV0 is in place.
    // Use 300 for older protocol versions.
    #[default(300)]
    pub num_chunk_only_producer_seats: NumSeats,
    #[default(1)]
    pub minimum_validators_per_shard: NumSeats,
    #[default(Rational32::new(160, 1_000_000))]
    pub minimum_stake_ratio: Rational32,
    #[default(5)]
    /// Limits the number of shard changes in chunk producer assignments,
    /// if algorithm is able to choose assignment with better balance of
    /// number of chunk producers for shards.
    pub chunk_producer_assignment_changes_limit: NumSeats,
    #[default(false)]
    pub shuffle_shard_assignment_for_chunk_producers: bool,
}

pub mod block_info {
    use super::SlashState;
    use crate::challenge::SlashedValidator;
    use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
    use crate::types::EpochId;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{AccountId, Balance, BlockHeight, ProtocolVersion};
    use std::collections::HashMap;

    pub use super::BlockInfoV1;

    /// Information per each block.
    #[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Clone, Debug, serde::Serialize)]
    pub enum BlockInfo {
        V1(BlockInfoV1),
        V2(BlockInfoV2),
    }

    impl Default for BlockInfo {
        fn default() -> Self {
            Self::V2(BlockInfoV2::default())
        }
    }

    impl BlockInfo {
        pub fn new(
            hash: CryptoHash,
            height: BlockHeight,
            last_finalized_height: BlockHeight,
            last_final_block_hash: CryptoHash,
            prev_hash: CryptoHash,
            proposals: Vec<ValidatorStake>,
            validator_mask: Vec<bool>,
            slashed: Vec<SlashedValidator>,
            total_supply: Balance,
            latest_protocol_version: ProtocolVersion,
            timestamp_nanosec: u64,
        ) -> Self {
            Self::V2(BlockInfoV2 {
                hash,
                height,
                last_finalized_height,
                last_final_block_hash,
                prev_hash,
                proposals,
                chunk_mask: validator_mask,
                latest_protocol_version,
                slashed: slashed
                    .into_iter()
                    .map(|s| {
                        let slash_state = if s.is_double_sign {
                            SlashState::DoubleSign
                        } else {
                            SlashState::Other
                        };
                        (s.account_id, slash_state)
                    })
                    .collect(),
                total_supply,
                epoch_first_block: Default::default(),
                epoch_id: Default::default(),
                timestamp_nanosec,
            })
        }

        #[inline]
        pub fn proposals_iter(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.proposals),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.proposals),
            }
        }

        #[inline]
        pub fn hash(&self) -> &CryptoHash {
            match self {
                Self::V1(v1) => &v1.hash,
                Self::V2(v2) => &v2.hash,
            }
        }

        #[inline]
        pub fn height(&self) -> BlockHeight {
            match self {
                Self::V1(v1) => v1.height,
                Self::V2(v2) => v2.height,
            }
        }

        #[inline]
        pub fn last_finalized_height(&self) -> BlockHeight {
            match self {
                Self::V1(v1) => v1.last_finalized_height,
                Self::V2(v2) => v2.last_finalized_height,
            }
        }

        #[inline]
        pub fn last_final_block_hash(&self) -> &CryptoHash {
            match self {
                Self::V1(v1) => &v1.last_final_block_hash,
                Self::V2(v2) => &v2.last_final_block_hash,
            }
        }

        #[inline]
        pub fn prev_hash(&self) -> &CryptoHash {
            match self {
                Self::V1(v1) => &v1.prev_hash,
                Self::V2(v2) => &v2.prev_hash,
            }
        }

        #[inline]
        pub fn is_genesis(&self) -> bool {
            self.prev_hash() == &CryptoHash::default()
        }

        #[inline]
        pub fn epoch_first_block(&self) -> &CryptoHash {
            match self {
                Self::V1(v1) => &v1.epoch_first_block,
                Self::V2(v2) => &v2.epoch_first_block,
            }
        }

        #[inline]
        pub fn epoch_first_block_mut(&mut self) -> &mut CryptoHash {
            match self {
                Self::V1(v1) => &mut v1.epoch_first_block,
                Self::V2(v2) => &mut v2.epoch_first_block,
            }
        }

        #[inline]
        pub fn epoch_id(&self) -> &EpochId {
            match self {
                Self::V1(v1) => &v1.epoch_id,
                Self::V2(v2) => &v2.epoch_id,
            }
        }

        #[inline]
        pub fn epoch_id_mut(&mut self) -> &mut EpochId {
            match self {
                Self::V1(v1) => &mut v1.epoch_id,
                Self::V2(v2) => &mut v2.epoch_id,
            }
        }

        #[inline]
        pub fn chunk_mask(&self) -> &[bool] {
            match self {
                Self::V1(v1) => &v1.chunk_mask,
                Self::V2(v2) => &v2.chunk_mask,
            }
        }

        #[inline]
        pub fn latest_protocol_version(&self) -> &ProtocolVersion {
            match self {
                Self::V1(v1) => &v1.latest_protocol_version,
                Self::V2(v2) => &v2.latest_protocol_version,
            }
        }

        #[inline]
        pub fn slashed(&self) -> &HashMap<AccountId, SlashState> {
            match self {
                Self::V1(v1) => &v1.slashed,
                Self::V2(v2) => &v2.slashed,
            }
        }

        #[inline]
        pub fn slashed_mut(&mut self) -> &mut HashMap<AccountId, SlashState> {
            match self {
                Self::V1(v1) => &mut v1.slashed,
                Self::V2(v2) => &mut v2.slashed,
            }
        }

        #[inline]
        pub fn total_supply(&self) -> &Balance {
            match self {
                Self::V1(v1) => &v1.total_supply,
                Self::V2(v2) => &v2.total_supply,
            }
        }

        #[inline]
        pub fn timestamp_nanosec(&self) -> &u64 {
            match self {
                Self::V1(v1) => &v1.timestamp_nanosec,
                Self::V2(v2) => &v2.timestamp_nanosec,
            }
        }
    }

    // V1 -> V2: Use versioned ValidatorStake structure in proposals
    #[derive(
        Default, BorshSerialize, BorshDeserialize, Eq, PartialEq, Clone, Debug, serde::Serialize,
    )]
    pub struct BlockInfoV2 {
        pub hash: CryptoHash,
        pub height: BlockHeight,
        pub last_finalized_height: BlockHeight,
        pub last_final_block_hash: CryptoHash,
        pub prev_hash: CryptoHash,
        pub epoch_first_block: CryptoHash,
        pub epoch_id: EpochId,
        pub proposals: Vec<ValidatorStake>,
        pub chunk_mask: Vec<bool>,
        /// Latest protocol version this validator observes.
        pub latest_protocol_version: ProtocolVersion,
        /// Validators slashed since the start of epoch or in previous epoch.
        pub slashed: HashMap<AccountId, SlashState>,
        /// Total supply at this block.
        pub total_supply: Balance,
        pub timestamp_nanosec: u64,
    }
}

/// Information per each block.
#[derive(
    Default, BorshSerialize, BorshDeserialize, Eq, PartialEq, Clone, Debug, serde::Serialize,
)]
pub struct BlockInfoV1 {
    pub hash: CryptoHash,
    pub height: BlockHeight,
    pub last_finalized_height: BlockHeight,
    pub last_final_block_hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub epoch_first_block: CryptoHash,
    pub epoch_id: EpochId,
    pub proposals: Vec<ValidatorStakeV1>,
    pub chunk_mask: Vec<bool>,
    /// Latest protocol version this validator observes.
    pub latest_protocol_version: ProtocolVersion,
    /// Validators slashed since the start of epoch or in previous epoch.
    pub slashed: HashMap<AccountId, SlashState>,
    /// Total supply at this block.
    pub total_supply: Balance,
    pub timestamp_nanosec: u64,
}

impl BlockInfoV1 {
    pub fn new(
        hash: CryptoHash,
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        last_final_block_hash: CryptoHash,
        prev_hash: CryptoHash,
        proposals: Vec<ValidatorStakeV1>,
        validator_mask: Vec<bool>,
        slashed: Vec<SlashedValidator>,
        total_supply: Balance,
        latest_protocol_version: ProtocolVersion,
        timestamp_nanosec: u64,
    ) -> Self {
        Self {
            hash,
            height,
            last_finalized_height,
            last_final_block_hash,
            prev_hash,
            proposals,
            chunk_mask: validator_mask,
            latest_protocol_version,
            slashed: slashed
                .into_iter()
                .map(|s| {
                    let slash_state =
                        if s.is_double_sign { SlashState::DoubleSign } else { SlashState::Other };
                    (s.account_id, slash_state)
                })
                .collect(),
            total_supply,
            epoch_first_block: Default::default(),
            epoch_id: Default::default(),
            timestamp_nanosec,
        }
    }
}

/// State that a slashed validator can be in.
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq, ProtocolSchema,
)]
pub enum SlashState {
    /// Double Sign, will be partially slashed.
    DoubleSign,
    /// Malicious behavior but is already slashed (tokens taken away from account).
    AlreadySlashed,
    /// All other cases (tokens should be entirely slashed),
    Other,
}

#[cfg(feature = "new_epoch_sync")]
pub mod epoch_sync {
    use crate::block_header::BlockHeader;
    use crate::epoch_info::EpochInfo;
    use crate::epoch_manager::block_info::BlockInfo;
    use crate::errors::epoch_sync::{EpochSyncHashType, EpochSyncInfoError};
    use crate::types::EpochId;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use std::collections::{HashMap, HashSet};

    /// Struct to keep all the info that is transferred for one epoch during Epoch Sync.
    #[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug)]
    pub struct EpochSyncInfo {
        /// All block hashes of this epoch. In order of production.
        pub all_block_hashes: Vec<CryptoHash>,
        /// All headers relevant to epoch sync.
        /// Contains epoch headers that need to be saved + supporting headers needed for validation.
        /// Probably contains one header from the previous epoch.
        /// It refers to `last_final_block` of the first block of the epoch.
        /// Also contains first header from the next epoch.
        /// It refers to `next_epoch_first_hash`.
        pub headers: HashMap<CryptoHash, BlockHeader>,
        /// Hashes of headers that need to be validated and saved.
        pub headers_to_save: HashSet<CryptoHash>,
        /// Hash of the first block of the next epoch.
        /// Header of this block contains `epoch_sync_data_hash`.
        pub next_epoch_first_hash: CryptoHash,
        pub epoch_info: EpochInfo,
        pub next_epoch_info: EpochInfo,
        pub next_next_epoch_info: EpochInfo,
    }

    impl EpochSyncInfo {
        pub fn get_epoch_id(&self) -> Result<&EpochId, EpochSyncInfoError> {
            Ok(self.get_epoch_first_header()?.epoch_id())
        }

        pub fn get_next_epoch_id(&self) -> Result<&EpochId, EpochSyncInfoError> {
            Ok(self
                .get_header(self.next_epoch_first_hash, EpochSyncHashType::NextEpochFirstBlock)?
                .epoch_id())
        }

        pub fn get_next_next_epoch_id(&self) -> Result<EpochId, EpochSyncInfoError> {
            Ok(EpochId(*self.get_epoch_last_hash()?))
        }

        pub fn get_epoch_last_hash(&self) -> Result<&CryptoHash, EpochSyncInfoError> {
            let epoch_height = self.epoch_info.epoch_height();

            self.all_block_hashes.last().ok_or(EpochSyncInfoError::ShortEpoch { epoch_height })
        }

        pub fn get_epoch_last_header(&self) -> Result<&BlockHeader, EpochSyncInfoError> {
            self.get_header(*self.get_epoch_last_hash()?, EpochSyncHashType::LastEpochBlock)
        }

        pub fn get_epoch_last_finalised_hash(&self) -> Result<&CryptoHash, EpochSyncInfoError> {
            Ok(self.get_epoch_last_header()?.last_final_block())
        }

        pub fn get_epoch_last_finalised_header(&self) -> Result<&BlockHeader, EpochSyncInfoError> {
            self.get_header(
                *self.get_epoch_last_finalised_hash()?,
                EpochSyncHashType::LastFinalBlock,
            )
        }

        pub fn get_epoch_first_hash(&self) -> Result<&CryptoHash, EpochSyncInfoError> {
            let epoch_height = self.epoch_info.epoch_height();

            self.all_block_hashes.first().ok_or(EpochSyncInfoError::ShortEpoch { epoch_height })
        }

        pub fn get_epoch_first_header(&self) -> Result<&BlockHeader, EpochSyncInfoError> {
            self.get_header(*self.get_epoch_first_hash()?, EpochSyncHashType::FirstEpochBlock)
        }

        /// Reconstruct BlockInfo for `hash` from information in EpochSyncInfo.
        pub fn get_block_info(&self, hash: &CryptoHash) -> Result<BlockInfo, EpochSyncInfoError> {
            let epoch_first_header = self.get_epoch_first_header()?;
            let header = self.get_header(*hash, EpochSyncHashType::Other)?;

            if epoch_first_header.epoch_id() != header.epoch_id() {
                let msg = "We can only correctly reconstruct headers from this epoch";
                debug_assert!(false, "{}", msg);
                tracing::error!(message = msg);
            }

            let last_finalized_height = if *header.last_final_block() == CryptoHash::default() {
                0
            } else {
                let last_finalized_header =
                    self.get_header(*header.last_final_block(), EpochSyncHashType::LastFinalBlock)?;
                last_finalized_header.height()
            };
            let mut block_info = BlockInfo::new(
                *header.hash(),
                header.height(),
                last_finalized_height,
                *header.last_final_block(),
                *header.prev_hash(),
                header.prev_validator_proposals().collect(),
                header.chunk_mask().to_vec(),
                vec![],
                header.total_supply(),
                header.latest_protocol_version(),
                header.raw_timestamp(),
            );

            *block_info.epoch_id_mut() = *epoch_first_header.epoch_id();
            *block_info.epoch_first_block_mut() = *epoch_first_header.hash();
            Ok(block_info)
        }

        /// Reconstruct legacy `epoch_sync_data_hash` from `EpochSyncInfo`.
        /// `epoch_sync_data_hash` was introduced in `BlockHeaderInnerRestV3`.
        /// Using this hash we can verify that `EpochInfo` data provided in `EpochSyncInfo` is correct.
        pub fn calculate_epoch_sync_data_hash(&self) -> Result<CryptoHash, EpochSyncInfoError> {
            let epoch_height = self.epoch_info.epoch_height();

            if self.all_block_hashes.len() < 2 {
                return Err(EpochSyncInfoError::ShortEpoch { epoch_height });
            }
            let epoch_first_block = self.all_block_hashes[0];
            let epoch_prev_last_block = self.all_block_hashes[self.all_block_hashes.len() - 2];
            let epoch_last_block = self.all_block_hashes[self.all_block_hashes.len() - 1];

            Ok(CryptoHash::hash_borsh(&(
                self.get_block_info(&epoch_first_block)?,
                self.get_block_info(&epoch_prev_last_block)?,
                self.get_block_info(&epoch_last_block)?,
                &self.epoch_info,
                &self.next_epoch_info,
                &self.next_next_epoch_info,
            )))
        }

        /// Read legacy `epoch_sync_data_hash` from next epoch first header.
        /// `epoch_sync_data_hash` was introduced in `BlockHeaderInnerRestV3`.
        /// Using this hash we can verify that `EpochInfo` data provided in `EpochSyncInfo` is correct.
        pub fn get_epoch_sync_data_hash(&self) -> Result<Option<CryptoHash>, EpochSyncInfoError> {
            let next_epoch_first_header =
                self.get_header(self.next_epoch_first_hash, EpochSyncHashType::Other)?;
            Ok(next_epoch_first_header.epoch_sync_data_hash())
        }

        pub fn get_header(
            &self,
            hash: CryptoHash,
            hash_type: EpochSyncHashType,
        ) -> Result<&BlockHeader, EpochSyncInfoError> {
            self.headers.get(&hash).ok_or(EpochSyncInfoError::HashNotFound {
                hash,
                hash_type,
                epoch_height: self.epoch_info.epoch_height(),
            })
        }
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
