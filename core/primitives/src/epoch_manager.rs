use crate::challenge::SlashedValidator;
use crate::num_rational::Rational32;
use crate::shard_layout::ShardLayout;
use crate::types::validator_stake::ValidatorStakeV1;
use crate::types::{
    AccountId, Balance, BlockHeightDelta, EpochHeight, EpochId, NumSeats, ProtocolVersion,
    ValidatorId, ValidatorKickoutReason,
};
use crate::version::PROTOCOL_VERSION;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use smart_default::SmartDefault;
use std::collections::{BTreeMap, HashMap};

pub type RngSeed = [u8; 32];

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
    /// Criterion for kicking out block producers.
    pub block_producer_kickout_threshold: u8,
    /// Criterion for kicking out chunk producers.
    pub chunk_producer_kickout_threshold: u8,
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
        if !self.use_production_config {
            return config;
        }

        Self::config_nightshade(&mut config, protocol_version);

        Self::config_chunk_only_producers(&mut config, &self.chain_id, protocol_version);

        Self::config_max_kickout_stake(&mut config, protocol_version);

        Self::config_test_overrides(&mut config, &self.test_overrides);

        config
    }

    fn config_nightshade(config: &mut EpochConfig, protocol_version: ProtocolVersion) {
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
        let num_shards = shard_layout.num_shards() as usize;
        let num_block_producer_seats = config.num_block_producer_seats;
        config.shard_layout = shard_layout;
        config.num_block_producer_seats_per_shard = vec![num_block_producer_seats; num_shards];
        config.avg_hidden_validator_seats_per_shard = vec![0; num_shards];
    }

    fn config_chunk_only_producers(
        config: &mut EpochConfig,
        chain_id: &str,
        protocol_version: u32,
    ) {
        if checked_feature!("stable", ChunkOnlyProducers, protocol_version) {
            let num_shards = config.shard_layout.num_shards() as usize;
            // On testnet, genesis config set num_block_producer_seats to 200
            // This is to bring it back to 100 to be the same as on mainnet
            config.num_block_producer_seats = 100;
            // Technically, after ChunkOnlyProducers is enabled, this field is no longer used
            // We still set it here just in case
            config.num_block_producer_seats_per_shard = vec![100; num_shards];
            config.block_producer_kickout_threshold = 80;
            config.chunk_producer_kickout_threshold = 80;
            config.validator_selection_config.num_chunk_only_producer_seats = 200;
        }

        // Adjust the number of block and chunk producers for all chains except
        // mainnet, to make it easier to test the change.
        if chain_id != crate::chains::MAINNET
            && checked_feature!("stable", TestnetFewerBlockProducers, protocol_version)
        {
            let num_shards = config.shard_layout.num_shards() as usize;
            // Decrease the number of block producers from 100 to 20.
            config.num_block_producer_seats = 20;
            config.num_block_producer_seats_per_shard =
                vec![config.num_block_producer_seats; num_shards];
            // Decrease the number of chunk producers.
            config.validator_selection_config.num_chunk_only_producer_seats = 100;
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
    #[default(300)]
    pub num_chunk_only_producer_seats: NumSeats,
    #[default(1)]
    pub minimum_validators_per_shard: NumSeats,
    #[default(Rational32::new(160, 1_000_000))]
    pub minimum_stake_ratio: Rational32,
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

#[derive(
    Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, serde::Serialize,
)]
pub struct ValidatorWeight(ValidatorId, u64);

pub mod epoch_info {
    use crate::epoch_manager::ValidatorWeight;
    use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
    use crate::types::{BlockChunkValidatorStats, ValidatorKickoutReason};
    use crate::validator_mandates::{ValidatorMandates, ValidatorMandatesAssignment};
    use crate::version::PROTOCOL_VERSION;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{
        AccountId, Balance, EpochHeight, ProtocolVersion, ValidatorId,
    };
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;
    use smart_default::SmartDefault;
    use std::collections::{BTreeMap, HashMap};

    use crate::types::validator_stake::ValidatorStakeV1;
    use crate::{epoch_manager::RngSeed, rand::WeightedIndex};
    use near_primitives_core::{
        checked_feature,
        hash::hash,
        types::{BlockHeight, ShardId},
    };

    pub use super::EpochInfoV1;

    /// Information per epoch.
    #[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, serde::Serialize)]
    pub enum EpochInfo {
        V1(EpochInfoV1),
        V2(EpochInfoV2),
        V3(EpochInfoV3),
        V4(EpochInfoV4),
    }

    impl Default for EpochInfo {
        fn default() -> Self {
            Self::V2(EpochInfoV2::default())
        }
    }

    // V1 -> V2: Use versioned ValidatorStake structure in validators and fishermen
    #[derive(
        SmartDefault,
        BorshSerialize,
        BorshDeserialize,
        Clone,
        Debug,
        PartialEq,
        Eq,
        serde::Serialize,
    )]
    pub struct EpochInfoV2 {
        /// Ordinal of given epoch from genesis.
        /// There can be multiple epochs with the same ordinal in case of long forks.
        pub epoch_height: EpochHeight,
        /// List of current validators.
        pub validators: Vec<ValidatorStake>,
        /// Validator account id to index in proposals.
        pub validator_to_index: HashMap<AccountId, ValidatorId>,
        /// Settlement of validators responsible for block production.
        pub block_producers_settlement: Vec<ValidatorId>,
        /// Per each shard, settlement validators that are responsible.
        pub chunk_producers_settlement: Vec<Vec<ValidatorId>>,
        /// Settlement of hidden validators with weights used to determine how many shards they will validate.
        pub hidden_validators_settlement: Vec<ValidatorWeight>,
        /// List of current fishermen.
        pub fishermen: Vec<ValidatorStake>,
        /// Fisherman account id to index of proposal.
        pub fishermen_to_index: HashMap<AccountId, ValidatorId>,
        /// New stake for validators.
        pub stake_change: BTreeMap<AccountId, Balance>,
        /// Validator reward for the epoch.
        pub validator_reward: HashMap<AccountId, Balance>,
        /// Validators who are kicked out in this epoch.
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        /// Total minted tokens in the epoch.
        pub minted_amount: Balance,
        /// Seat price of this epoch.
        pub seat_price: Balance,
        /// Current protocol version during this epoch.
        #[default(PROTOCOL_VERSION)]
        pub protocol_version: ProtocolVersion,
    }

    // V2 -> V3: Structures for randomly selecting validators at each height based on new
    // block producer and chunk producer selection algorithm.
    #[derive(
        SmartDefault,
        BorshSerialize,
        BorshDeserialize,
        Clone,
        Debug,
        PartialEq,
        Eq,
        serde::Serialize,
    )]
    pub struct EpochInfoV3 {
        pub epoch_height: EpochHeight,
        pub validators: Vec<ValidatorStake>,
        pub validator_to_index: HashMap<AccountId, ValidatorId>,
        pub block_producers_settlement: Vec<ValidatorId>,
        pub chunk_producers_settlement: Vec<Vec<ValidatorId>>,
        pub hidden_validators_settlement: Vec<ValidatorWeight>,
        pub fishermen: Vec<ValidatorStake>,
        pub fishermen_to_index: HashMap<AccountId, ValidatorId>,
        pub stake_change: BTreeMap<AccountId, Balance>,
        pub validator_reward: HashMap<AccountId, Balance>,
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        pub minted_amount: Balance,
        pub seat_price: Balance,
        #[default(PROTOCOL_VERSION)]
        pub protocol_version: ProtocolVersion,
        // stuff for selecting validators at each height
        rng_seed: RngSeed,
        block_producers_sampler: WeightedIndex,
        chunk_producers_sampler: Vec<WeightedIndex>,
    }

    // V3 -> V4: Add structures and methods for stateless validator assignment.
    #[derive(
        SmartDefault,
        BorshSerialize,
        BorshDeserialize,
        Clone,
        Debug,
        PartialEq,
        Eq,
        serde::Serialize,
    )]
    pub struct EpochInfoV4 {
        pub epoch_height: EpochHeight,
        pub validators: Vec<ValidatorStake>,
        pub validator_to_index: HashMap<AccountId, ValidatorId>,
        pub block_producers_settlement: Vec<ValidatorId>,
        pub chunk_producers_settlement: Vec<Vec<ValidatorId>>,
        pub hidden_validators_settlement: Vec<ValidatorWeight>,
        pub fishermen: Vec<ValidatorStake>,
        pub fishermen_to_index: HashMap<AccountId, ValidatorId>,
        pub stake_change: BTreeMap<AccountId, Balance>,
        pub validator_reward: HashMap<AccountId, Balance>,
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        pub minted_amount: Balance,
        pub seat_price: Balance,
        #[default(PROTOCOL_VERSION)]
        pub protocol_version: ProtocolVersion,
        // stuff for selecting validators at each height
        rng_seed: RngSeed,
        block_producers_sampler: WeightedIndex,
        chunk_producers_sampler: Vec<WeightedIndex>,
        /// Contains the epoch's validator mandates. Used to sample chunk validators.
        validator_mandates: ValidatorMandates,
    }

    impl EpochInfo {
        pub fn new(
            epoch_height: EpochHeight,
            validators: Vec<ValidatorStake>,
            validator_to_index: HashMap<AccountId, ValidatorId>,
            block_producers_settlement: Vec<ValidatorId>,
            chunk_producers_settlement: Vec<Vec<ValidatorId>>,
            hidden_validators_settlement: Vec<ValidatorWeight>,
            fishermen: Vec<ValidatorStake>,
            fishermen_to_index: HashMap<AccountId, ValidatorId>,
            stake_change: BTreeMap<AccountId, Balance>,
            validator_reward: HashMap<AccountId, Balance>,
            validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
            minted_amount: Balance,
            seat_price: Balance,
            protocol_version: ProtocolVersion,
            rng_seed: RngSeed,
            #[cfg(feature = "protocol_feature_chunk_validation")]
            validator_mandates: ValidatorMandates,
        ) -> Self {
            if checked_feature!("stable", AliasValidatorSelectionAlgorithm, protocol_version) {
                let stake_weights = |ids: &[ValidatorId]| -> WeightedIndex {
                    WeightedIndex::new(
                        ids.iter()
                            .copied()
                            .map(|validator_id| validators[validator_id as usize].stake())
                            .collect(),
                    )
                };
                let block_producers_sampler = stake_weights(&block_producers_settlement);
                let chunk_producers_sampler =
                    chunk_producers_settlement.iter().map(|vs| stake_weights(vs)).collect();
                if checked_feature!(
                    "protocol_feature_chunk_validation",
                    ChunkValidation,
                    protocol_version
                ) {
                    // This block is entered only if feature `protocol_feature_chunk_validation` is
                    // enabled. In that case `validator_mandates` is a parameter of the function and
                    // the variable shadowing below is not included. Still, the following
                    // declaration of `validator_mandates` is needed to satisfy the compiler.
                    #[cfg(not(feature = "protocol_feature_chunk_validation"))]
                    let validator_mandates = Default::default();
                    Self::V4(EpochInfoV4 {
                        epoch_height,
                        validators,
                        fishermen,
                        validator_to_index,
                        block_producers_settlement,
                        chunk_producers_settlement,
                        hidden_validators_settlement,
                        stake_change,
                        validator_reward,
                        validator_kickout,
                        fishermen_to_index,
                        minted_amount,
                        seat_price,
                        protocol_version,
                        rng_seed,
                        block_producers_sampler,
                        chunk_producers_sampler,
                        validator_mandates,
                    })
                } else {
                    Self::V3(EpochInfoV3 {
                        epoch_height,
                        validators,
                        fishermen,
                        validator_to_index,
                        block_producers_settlement,
                        chunk_producers_settlement,
                        hidden_validators_settlement,
                        stake_change,
                        validator_reward,
                        validator_kickout,
                        fishermen_to_index,
                        minted_amount,
                        seat_price,
                        protocol_version,
                        rng_seed,
                        block_producers_sampler,
                        chunk_producers_sampler,
                    })
                }
            } else {
                Self::V2(EpochInfoV2 {
                    epoch_height,
                    validators,
                    fishermen,
                    validator_to_index,
                    block_producers_settlement,
                    chunk_producers_settlement,
                    hidden_validators_settlement,
                    stake_change,
                    validator_reward,
                    validator_kickout,
                    fishermen_to_index,
                    minted_amount,
                    seat_price,
                    protocol_version,
                })
            }
        }

        pub fn v1_test() -> Self {
            Self::V1(EpochInfoV1 {
                epoch_height: 10,
                validators: vec![
                    ValidatorStakeV1 {
                        account_id: "test".parse().unwrap(),
                        public_key: "ed25519:6E8sCci9badyRkXb3JoRpBj5p8C6Tw41ELDZoiihKEtp"
                            .parse()
                            .unwrap(),
                        stake: 0,
                    },
                    ValidatorStakeV1 {
                        account_id: "validator".parse().unwrap(),
                        public_key: "ed25519:9E8sCci9badyRkXb3JoRpBj5p8C6Tw41ELDZoiihKEtp"
                            .parse()
                            .unwrap(),
                        stake: 0,
                    },
                ],
                validator_to_index: HashMap::new(),
                block_producers_settlement: vec![0u64, 1u64],
                chunk_producers_settlement: vec![vec![0u64, 1u64]],
                hidden_validators_settlement: vec![],
                fishermen: vec![],
                fishermen_to_index: HashMap::new(),
                stake_change: BTreeMap::new(),
                validator_reward: HashMap::new(),
                validator_kickout: HashMap::new(),
                minted_amount: 1,
                seat_price: 1,
                protocol_version: 1,
            })
        }

        #[inline]
        pub fn epoch_height_mut(&mut self) -> &mut EpochHeight {
            match self {
                Self::V1(v1) => &mut v1.epoch_height,
                Self::V2(v2) => &mut v2.epoch_height,
                Self::V3(v3) => &mut v3.epoch_height,
                Self::V4(v4) => &mut v4.epoch_height,
            }
        }

        #[inline]
        pub fn epoch_height(&self) -> EpochHeight {
            match self {
                Self::V1(v1) => v1.epoch_height,
                Self::V2(v2) => v2.epoch_height,
                Self::V3(v3) => v3.epoch_height,
                Self::V4(v4) => v4.epoch_height,
            }
        }

        #[inline]
        pub fn seat_price(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.seat_price,
                Self::V2(v2) => v2.seat_price,
                Self::V3(v3) => v3.seat_price,
                Self::V4(v4) => v4.seat_price,
            }
        }

        #[inline]
        pub fn minted_amount(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.minted_amount,
                Self::V2(v2) => v2.minted_amount,
                Self::V3(v3) => v3.minted_amount,
                Self::V4(v4) => v4.minted_amount,
            }
        }

        #[inline]
        pub fn block_producers_settlement(&self) -> &[ValidatorId] {
            match self {
                Self::V1(v1) => &v1.block_producers_settlement,
                Self::V2(v2) => &v2.block_producers_settlement,
                Self::V3(v3) => &v3.block_producers_settlement,
                Self::V4(v4) => &v4.block_producers_settlement,
            }
        }

        #[inline]
        pub fn chunk_producers_settlement(&self) -> &[Vec<ValidatorId>] {
            match self {
                Self::V1(v1) => &v1.chunk_producers_settlement,
                Self::V2(v2) => &v2.chunk_producers_settlement,
                Self::V3(v3) => &v3.chunk_producers_settlement,
                Self::V4(v4) => &v4.chunk_producers_settlement,
            }
        }

        #[inline]
        pub fn validator_kickout(&self) -> &HashMap<AccountId, ValidatorKickoutReason> {
            match self {
                Self::V1(v1) => &v1.validator_kickout,
                Self::V2(v2) => &v2.validator_kickout,
                Self::V3(v3) => &v3.validator_kickout,
                Self::V4(v4) => &v4.validator_kickout,
            }
        }

        #[inline]
        pub fn protocol_version(&self) -> ProtocolVersion {
            match self {
                Self::V1(v1) => v1.protocol_version,
                Self::V2(v2) => v2.protocol_version,
                Self::V3(v3) => v3.protocol_version,
                Self::V4(v4) => v4.protocol_version,
            }
        }

        #[inline]
        pub fn stake_change(&self) -> &BTreeMap<AccountId, Balance> {
            match self {
                Self::V1(v1) => &v1.stake_change,
                Self::V2(v2) => &v2.stake_change,
                Self::V3(v3) => &v3.stake_change,
                Self::V4(v4) => &v4.stake_change,
            }
        }

        #[inline]
        pub fn validator_reward(&self) -> &HashMap<AccountId, Balance> {
            match self {
                Self::V1(v1) => &v1.validator_reward,
                Self::V2(v2) => &v2.validator_reward,
                Self::V3(v3) => &v3.validator_reward,
                Self::V4(v4) => &v4.validator_reward,
            }
        }

        #[inline]
        pub fn validators_iter(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.validators),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.validators),
                Self::V3(v3) => ValidatorStakeIter::new(&v3.validators),
                Self::V4(v4) => ValidatorStakeIter::new(&v4.validators),
            }
        }

        #[inline]
        pub fn fishermen_iter(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.fishermen),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.fishermen),
                Self::V3(v3) => ValidatorStakeIter::new(&v3.fishermen),
                Self::V4(v4) => ValidatorStakeIter::new(&v4.fishermen),
            }
        }

        #[inline]
        pub fn validator_stake(&self, validator_id: u64) -> Balance {
            match self {
                Self::V1(v1) => v1.validators[validator_id as usize].stake,
                Self::V2(v2) => v2.validators[validator_id as usize].stake(),
                Self::V3(v3) => v3.validators[validator_id as usize].stake(),
                Self::V4(v4) => v4.validators[validator_id as usize].stake(),
            }
        }

        #[inline]
        pub fn validator_account_id(&self, validator_id: u64) -> &AccountId {
            match self {
                Self::V1(v1) => &v1.validators[validator_id as usize].account_id,
                Self::V2(v2) => v2.validators[validator_id as usize].account_id(),
                Self::V3(v3) => v3.validators[validator_id as usize].account_id(),
                Self::V4(v4) => v4.validators[validator_id as usize].account_id(),
            }
        }

        #[inline]
        pub fn account_is_validator(&self, account_id: &AccountId) -> bool {
            match self {
                Self::V1(v1) => v1.validator_to_index.contains_key(account_id),
                Self::V2(v2) => v2.validator_to_index.contains_key(account_id),
                Self::V3(v3) => v3.validator_to_index.contains_key(account_id),
                Self::V4(v4) => v4.validator_to_index.contains_key(account_id),
            }
        }

        pub fn get_validator_id(&self, account_id: &AccountId) -> Option<&ValidatorId> {
            match self {
                Self::V1(v1) => v1.validator_to_index.get(account_id),
                Self::V2(v2) => v2.validator_to_index.get(account_id),
                Self::V3(v3) => v3.validator_to_index.get(account_id),
                Self::V4(v4) => v4.validator_to_index.get(account_id),
            }
        }

        pub fn get_validator_by_account(&self, account_id: &AccountId) -> Option<ValidatorStake> {
            match self {
                Self::V1(v1) => v1.validator_to_index.get(account_id).map(|validator_id| {
                    ValidatorStake::V1(v1.validators[*validator_id as usize].clone())
                }),
                Self::V2(v2) => v2
                    .validator_to_index
                    .get(account_id)
                    .map(|validator_id| v2.validators[*validator_id as usize].clone()),
                Self::V3(v3) => v3
                    .validator_to_index
                    .get(account_id)
                    .map(|validator_id| v3.validators[*validator_id as usize].clone()),
                Self::V4(v4) => v4
                    .validator_to_index
                    .get(account_id)
                    .map(|validator_id| v4.validators[*validator_id as usize].clone()),
            }
        }

        #[inline]
        pub fn get_validator(&self, validator_id: u64) -> ValidatorStake {
            match self {
                Self::V1(v1) => ValidatorStake::V1(v1.validators[validator_id as usize].clone()),
                Self::V2(v2) => v2.validators[validator_id as usize].clone(),
                Self::V3(v3) => v3.validators[validator_id as usize].clone(),
                Self::V4(v4) => v4.validators[validator_id as usize].clone(),
            }
        }

        #[inline]
        pub fn account_is_fisherman(&self, account_id: &AccountId) -> bool {
            match self {
                Self::V1(v1) => v1.fishermen_to_index.contains_key(account_id),
                Self::V2(v2) => v2.fishermen_to_index.contains_key(account_id),
                Self::V3(v3) => v3.fishermen_to_index.contains_key(account_id),
                Self::V4(v4) => v4.fishermen_to_index.contains_key(account_id),
            }
        }

        pub fn get_fisherman_by_account(&self, account_id: &AccountId) -> Option<ValidatorStake> {
            match self {
                Self::V1(v1) => v1.fishermen_to_index.get(account_id).map(|validator_id| {
                    ValidatorStake::V1(v1.fishermen[*validator_id as usize].clone())
                }),
                Self::V2(v2) => v2
                    .fishermen_to_index
                    .get(account_id)
                    .map(|validator_id| v2.fishermen[*validator_id as usize].clone()),
                Self::V3(v3) => v3
                    .fishermen_to_index
                    .get(account_id)
                    .map(|validator_id| v3.fishermen[*validator_id as usize].clone()),
                Self::V4(v4) => v4
                    .fishermen_to_index
                    .get(account_id)
                    .map(|validator_id| v4.fishermen[*validator_id as usize].clone()),
            }
        }

        #[inline]
        pub fn get_fisherman(&self, fisherman_id: u64) -> ValidatorStake {
            match self {
                Self::V1(v1) => ValidatorStake::V1(v1.fishermen[fisherman_id as usize].clone()),
                Self::V2(v2) => v2.fishermen[fisherman_id as usize].clone(),
                Self::V3(v3) => v3.fishermen[fisherman_id as usize].clone(),
                Self::V4(v4) => v4.fishermen[fisherman_id as usize].clone(),
            }
        }

        #[inline]
        pub fn validators_len(&self) -> usize {
            match self {
                Self::V1(v1) => v1.validators.len(),
                Self::V2(v2) => v2.validators.len(),
                Self::V3(v3) => v3.validators.len(),
                Self::V4(v4) => v4.validators.len(),
            }
        }

        pub fn sample_block_producer(&self, height: BlockHeight) -> ValidatorId {
            match &self {
                Self::V1(v1) => {
                    let bp_settlement = &v1.block_producers_settlement;
                    bp_settlement[(height % (bp_settlement.len() as u64)) as usize]
                }
                Self::V2(v2) => {
                    let bp_settlement = &v2.block_producers_settlement;
                    bp_settlement[(height % (bp_settlement.len() as u64)) as usize]
                }
                Self::V3(v3) => {
                    let seed = Self::block_produce_seed(height, &v3.rng_seed);
                    v3.block_producers_settlement[v3.block_producers_sampler.sample(seed)]
                }
                Self::V4(v4) => {
                    let seed = Self::block_produce_seed(height, &v4.rng_seed);
                    v4.block_producers_settlement[v4.block_producers_sampler.sample(seed)]
                }
            }
        }

        pub fn sample_chunk_producer(&self, height: BlockHeight, shard_id: ShardId) -> ValidatorId {
            match &self {
                Self::V1(v1) => {
                    let cp_settlement = &v1.chunk_producers_settlement;
                    let shard_cps = &cp_settlement[shard_id as usize];
                    shard_cps[(height as u64 % (shard_cps.len() as u64)) as usize]
                }
                Self::V2(v2) => {
                    let cp_settlement = &v2.chunk_producers_settlement;
                    let shard_cps = &cp_settlement[shard_id as usize];
                    shard_cps[(height as u64 % (shard_cps.len() as u64)) as usize]
                }
                Self::V3(v3) => {
                    let protocol_version = self.protocol_version();
                    let seed =
                        Self::chunk_produce_seed(protocol_version, &v3.rng_seed, height, shard_id);
                    let shard_id = shard_id as usize;
                    let sample = v3.chunk_producers_sampler[shard_id].sample(seed);
                    v3.chunk_producers_settlement[shard_id][sample]
                }
                Self::V4(v4) => {
                    let protocol_version = self.protocol_version();
                    let seed =
                        Self::chunk_produce_seed(protocol_version, &v4.rng_seed, height, shard_id);
                    let shard_id = shard_id as usize;
                    let sample = v4.chunk_producers_sampler[shard_id].sample(seed);
                    v4.chunk_producers_settlement[shard_id][sample]
                }
            }
        }

        pub fn sample_chunk_validators(&self, height: BlockHeight) -> ValidatorMandatesAssignment {
            // Chunk validator assignment was introduced with `V4`.
            match &self {
                Self::V1(_) => Default::default(),
                Self::V2(_) => Default::default(),
                Self::V3(_) => Default::default(),
                Self::V4(v4) => {
                    let mut rng = Self::chunk_validate_rng(&v4.rng_seed, height);
                    v4.validator_mandates.sample(&mut rng)
                }
            }
        }

        /// 32 bytes from epoch_seed, 8 bytes from height
        fn block_produce_seed(height: BlockHeight, seed: &RngSeed) -> [u8; 32] {
            let mut buffer = [0u8; 40];
            buffer[0..32].copy_from_slice(seed);
            buffer[32..40].copy_from_slice(&height.to_le_bytes());
            hash(&buffer).0
        }

        fn chunk_produce_seed(
            protocol_version: ProtocolVersion,
            seed: &RngSeed,
            height: BlockHeight,
            shard_id: ShardId,
        ) -> [u8; 32] {
            if checked_feature!("stable", SynchronizeBlockChunkProduction, protocol_version)
                && !checked_feature!("stable", ChunkOnlyProducers, protocol_version)
            {
                // This is same seed that used for determining block
                // producer. This seed does not contain the shard id
                // so all shards will be produced by the same
                // validator.
                Self::block_produce_seed(height, seed)
            } else {
                // 32 bytes from epoch_seed, 8 bytes from height, 8 bytes from shard_id
                let mut buffer = [0u8; 48];
                buffer[0..32].copy_from_slice(seed);
                buffer[32..40].copy_from_slice(&height.to_le_bytes());
                buffer[40..48].copy_from_slice(&shard_id.to_le_bytes());
                hash(&buffer).0
            }
        }

        /// Returns a new RNG obtained from combining the provided `seed` and `height`.
        ///
        /// The returned RNG can be used to shuffle slices via [`rand::seq::SliceRandom`].
        fn chunk_validate_rng(seed: &RngSeed, height: BlockHeight) -> ChaCha20Rng {
            let mut buffer = [0u8; 40];
            buffer[0..32].copy_from_slice(seed);
            buffer[32..40].copy_from_slice(&height.to_le_bytes());

            // The recommended seed for cryptographic RNG's is `[u8; 32]` and some required traits
            // are not implemented for larger seeds, see
            // https://docs.rs/rand_core/0.6.2/rand_core/trait.SeedableRng.html#associated-types
            // Therefore `buffer` is hashed to obtain a `[u8; 32]`.
            let seed = hash(&buffer);
            SeedableRng::from_seed(seed.0)
        }
    }

    #[derive(BorshSerialize, BorshDeserialize)]
    pub struct EpochSummary {
        pub prev_epoch_last_block_hash: CryptoHash,
        /// Proposals from the epoch, only the latest one per account
        pub all_proposals: Vec<ValidatorStake>,
        /// Kickout set, includes slashed
        pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        /// Only for validators who met the threshold and didn't get slashed
        pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        /// Protocol version for next epoch.
        pub next_version: ProtocolVersion,
    }
}

/// Information per epoch.
#[derive(
    SmartDefault, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, serde::Serialize,
)]
pub struct EpochInfoV1 {
    /// Ordinal of given epoch from genesis.
    /// There can be multiple epochs with the same ordinal in case of long forks.
    pub epoch_height: EpochHeight,
    /// List of current validators.
    pub validators: Vec<ValidatorStakeV1>,
    /// Validator account id to index in proposals.
    pub validator_to_index: HashMap<AccountId, ValidatorId>,
    /// Settlement of validators responsible for block production.
    pub block_producers_settlement: Vec<ValidatorId>,
    /// Per each shard, settlement validators that are responsible.
    pub chunk_producers_settlement: Vec<Vec<ValidatorId>>,
    /// Settlement of hidden validators with weights used to determine how many shards they will validate.
    pub hidden_validators_settlement: Vec<ValidatorWeight>,
    /// List of current fishermen.
    pub fishermen: Vec<ValidatorStakeV1>,
    /// Fisherman account id to index of proposal.
    pub fishermen_to_index: HashMap<AccountId, ValidatorId>,
    /// New stake for validators.
    pub stake_change: BTreeMap<AccountId, Balance>,
    /// Validator reward for the epoch.
    pub validator_reward: HashMap<AccountId, Balance>,
    /// Validators who are kicked out in this epoch.
    pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    /// Total minted tokens in the epoch.
    pub minted_amount: Balance,
    /// Seat price of this epoch.
    pub seat_price: Balance,
    /// Current protocol version during this epoch.
    #[default(PROTOCOL_VERSION)]
    pub protocol_version: ProtocolVersion,
}

/// State that a slashed validator can be in.
#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
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
    use crate::epoch_manager::block_info::BlockInfo;
    use crate::epoch_manager::epoch_info::EpochInfo;
    use crate::errors::epoch_sync::{EpochSyncHashType, EpochSyncInfoError};
    use crate::types::EpochId;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_o11y::log_assert;
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

            log_assert!(
                epoch_first_header.epoch_id() == header.epoch_id(),
                "We can only correctly reconstruct headers from this epoch"
            );

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

            *block_info.epoch_id_mut() = epoch_first_header.epoch_id().clone();
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
