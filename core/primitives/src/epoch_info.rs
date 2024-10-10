use borsh::{BorshDeserialize, BorshSerialize};
use smart_default::SmartDefault;
use std::collections::{BTreeMap, HashMap};

use crate::rand::WeightedIndex;
use crate::shard_layout::ShardLayout;
use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
use crate::types::{AccountId, ValidatorKickoutReason, ValidatorStakeV1};
use crate::validator_mandates::ValidatorMandates;
use crate::version::PROTOCOL_VERSION;
use near_primitives_core::types::{Balance, EpochHeight, ProtocolVersion, ValidatorId};
use near_primitives_core::version::ProtocolFeature;
use near_primitives_core::{
    checked_feature,
    hash::hash,
    types::{BlockHeight, ShardId},
};
use near_schema_checker_lib::ProtocolSchema;

/// Information per epoch.
#[derive(
    BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, serde::Serialize, ProtocolSchema,
)]
pub enum EpochInfo {
    V1(EpochInfoV1),
    V2(EpochInfoV2),
    V3(EpochInfoV3),
    V4(EpochInfoV4),
}

pub type RngSeed = [u8; 32];

#[derive(
    Default,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    ProtocolSchema,
)]
pub struct ValidatorWeight(ValidatorId, u64);

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
    ProtocolSchema,
)]
pub struct EpochInfoV4 {
    pub epoch_height: EpochHeight,
    pub validators: Vec<ValidatorStake>,
    pub validator_to_index: HashMap<AccountId, ValidatorId>,
    pub block_producers_settlement: Vec<ValidatorId>,
    pub chunk_producers_settlement: Vec<Vec<ValidatorId>>,
    /// Deprecated.
    pub _hidden_validators_settlement: Vec<ValidatorWeight>,
    /// Deprecated.
    pub _fishermen: Vec<crate::types::validator_stake::ValidatorStake>,
    /// Deprecated.
    pub _fishermen_to_index: HashMap<AccountId, ValidatorId>,
    pub stake_change: BTreeMap<AccountId, Balance>,
    pub validator_reward: HashMap<AccountId, Balance>,
    pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    pub minted_amount: Balance,
    pub seat_price: Balance,
    #[default(PROTOCOL_VERSION)]
    pub protocol_version: ProtocolVersion,
    // stuff for selecting validators at each height
    rng_seed: RngSeed,
    block_producers_sampler: crate::rand::WeightedIndex,
    chunk_producers_sampler: Vec<crate::rand::WeightedIndex>,
    /// Contains the epoch's validator mandates. Used to sample chunk validators.
    validator_mandates: crate::validator_mandates::ValidatorMandates,
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
    ProtocolSchema,
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
    ProtocolSchema,
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

impl EpochInfo {
    pub fn new(
        epoch_height: EpochHeight,
        validators: Vec<ValidatorStake>,
        validator_to_index: HashMap<AccountId, ValidatorId>,
        block_producers_settlement: Vec<ValidatorId>,
        chunk_producers_settlement: Vec<Vec<ValidatorId>>,
        stake_change: BTreeMap<AccountId, Balance>,
        validator_reward: HashMap<AccountId, Balance>,
        validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
        minted_amount: Balance,
        seat_price: Balance,
        protocol_version: ProtocolVersion,
        rng_seed: RngSeed,
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
            if ProtocolFeature::StatelessValidation.enabled(protocol_version) {
                Self::V4(EpochInfoV4 {
                    epoch_height,
                    validators,
                    _fishermen: Default::default(),
                    validator_to_index,
                    block_producers_settlement,
                    chunk_producers_settlement,
                    _hidden_validators_settlement: Default::default(),
                    stake_change,
                    validator_reward,
                    validator_kickout,
                    _fishermen_to_index: Default::default(),
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
                    fishermen: Default::default(),
                    validator_to_index,
                    block_producers_settlement,
                    chunk_producers_settlement,
                    hidden_validators_settlement: Default::default(),
                    stake_change,
                    validator_reward,
                    validator_kickout,
                    fishermen_to_index: Default::default(),
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
                fishermen: Default::default(),
                validator_to_index,
                block_producers_settlement,
                chunk_producers_settlement,
                hidden_validators_settlement: Default::default(),
                stake_change,
                validator_reward,
                validator_kickout,
                fishermen_to_index: Default::default(),
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
    pub fn chunk_producers_settlement_mut(&mut self) -> &mut Vec<Vec<ValidatorId>> {
        match self {
            Self::V1(v1) => &mut v1.chunk_producers_settlement,
            Self::V2(v2) => &mut v2.chunk_producers_settlement,
            Self::V3(v3) => &mut v3.chunk_producers_settlement,
            Self::V4(v4) => &mut v4.chunk_producers_settlement,
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
            Self::V4(v4) => ValidatorStakeIter::new(&v4._fishermen),
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

    pub fn get_validator_stake(&self, account_id: &AccountId) -> Option<Balance> {
        match self {
            Self::V1(v1) => v1
                .validator_to_index
                .get(account_id)
                .map(|validator_id| v1.validators[*validator_id as usize].stake),
            Self::V2(v2) => v2
                .validator_to_index
                .get(account_id)
                .map(|validator_id| v2.validators[*validator_id as usize].stake()),
            Self::V3(v3) => v3
                .validator_to_index
                .get(account_id)
                .map(|validator_id| v3.validators[*validator_id as usize].stake()),
            Self::V4(v4) => v4
                .validator_to_index
                .get(account_id)
                .map(|validator_id| v4.validators[*validator_id as usize].stake()),
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
            Self::V4(v4) => v4._fishermen_to_index.contains_key(account_id),
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
                ._fishermen_to_index
                .get(account_id)
                .map(|validator_id| v4._fishermen[*validator_id as usize].clone()),
        }
    }

    #[inline]
    pub fn get_fisherman(&self, fisherman_id: u64) -> ValidatorStake {
        match self {
            Self::V1(v1) => ValidatorStake::V1(v1.fishermen[fisherman_id as usize].clone()),
            Self::V2(v2) => v2.fishermen[fisherman_id as usize].clone(),
            Self::V3(v3) => v3.fishermen[fisherman_id as usize].clone(),
            Self::V4(v4) => v4._fishermen[fisherman_id as usize].clone(),
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

    #[inline]
    pub fn rng_seed(&self) -> RngSeed {
        match self {
            Self::V1(_) | Self::V2(_) => Default::default(),
            Self::V3(v3) => v3.rng_seed,
            Self::V4(v4) => v4.rng_seed,
        }
    }

    #[inline]
    pub fn validator_mandates(&self) -> ValidatorMandates {
        match self {
            Self::V1(_) | Self::V2(_) | Self::V3(_) => Default::default(),
            Self::V4(v4) => v4.validator_mandates.clone(),
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

    pub fn sample_chunk_producer(
        &self,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        height: BlockHeight,
    ) -> Option<ValidatorId> {
        let shard_index = shard_layout.get_shard_index(shard_id);
        match &self {
            Self::V1(v1) => {
                let cp_settlement = &v1.chunk_producers_settlement;
                let shard_cps = cp_settlement.get(shard_index)?;
                shard_cps.get((height as u64 % (shard_cps.len() as u64)) as usize).copied()
            }
            Self::V2(v2) => {
                let cp_settlement = &v2.chunk_producers_settlement;
                let shard_cps = cp_settlement.get(shard_index)?;
                shard_cps.get((height as u64 % (shard_cps.len() as u64)) as usize).copied()
            }
            Self::V3(v3) => {
                let protocol_version = self.protocol_version();
                let seed =
                    Self::chunk_produce_seed(protocol_version, &v3.rng_seed, height, shard_id);
                let sample = v3.chunk_producers_sampler.get(shard_index)?.sample(seed);
                v3.chunk_producers_settlement.get(shard_index)?.get(sample).copied()
            }
            Self::V4(v4) => {
                let protocol_version = self.protocol_version();
                let seed =
                    Self::chunk_produce_seed(protocol_version, &v4.rng_seed, height, shard_id);
                let sample = v4.chunk_producers_sampler.get(shard_index)?.sample(seed);
                v4.chunk_producers_settlement.get(shard_index)?.get(sample).copied()
            }
        }
    }

    #[cfg(feature = "rand")]
    pub fn sample_chunk_validators(
        &self,
        height: BlockHeight,
    ) -> crate::validator_mandates::ChunkValidatorStakeAssignment {
        // Chunk validator assignment was introduced with `V4`.
        match &self {
            Self::V1(_) | Self::V2(_) | Self::V3(_) => Default::default(),
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
}

#[cfg(feature = "rand")]
impl EpochInfo {
    /// Returns a new RNG obtained from combining the provided `seed` and `height`.
    ///
    /// The returned RNG can be used to shuffle slices via [`rand::seq::SliceRandom`].
    fn chunk_validate_rng(seed: &RngSeed, height: BlockHeight) -> rand_chacha::ChaCha20Rng {
        // A deterministic seed is produces using the block height and the provided seed.
        // This is important as all nodes need to agree on the set and order of chunk_validators
        let mut buffer = [0u8; 40];
        buffer[0..32].copy_from_slice(seed);
        buffer[32..40].copy_from_slice(&height.to_le_bytes());

        // The recommended seed for cryptographic RNG's is `[u8; 32]` and some required traits
        // are not implemented for larger seeds, see
        // https://docs.rs/rand_core/0.6.2/rand_core/trait.SeedableRng.html#associated-types
        // Therefore `buffer` is hashed to obtain a `[u8; 32]`.
        let seed = hash(&buffer);
        rand::SeedableRng::from_seed(seed.0)
    }

    /// Returns a new RNG used for random chunk producer modifications
    /// during shard assignments.
    pub fn shard_assignment_rng(seed: &RngSeed) -> rand_chacha::ChaCha20Rng {
        let mut buffer = [0u8; 62];
        buffer[0..32].copy_from_slice(seed);
        // Do this to avoid any possibility of colliding with any other rng.
        buffer[32..62].copy_from_slice(b"shard_assignment_shuffling_rng");
        let seed = hash(&buffer);
        rand::SeedableRng::from_seed(seed.0)
    }
}

/// Information per epoch.
#[derive(
    SmartDefault,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    ProtocolSchema,
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
