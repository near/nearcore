use std::collections::{BTreeMap, HashMap};

use borsh::{BorshDeserialize, BorshSerialize};
use num_rational::Rational;
use serde::Serialize;
use smart_default::SmartDefault;

use crate::challenge::SlashedValidator;
use crate::hash::CryptoHash;
use crate::types::{AccountId, Balance, BlockChunkValidatorStats, BlockHeight, BlockHeightDelta, EpochHeight, EpochId, NumSeats, NumShards, ValidatorId, ValidatorKickoutReason, ValidatorStake, ValidatorStakeV1, ValidatorStakeIter};
use crate::version::{ProtocolVersion, PROTOCOL_VERSION};

pub type RngSeed = [u8; 32];

pub const AGGREGATOR_KEY: &[u8] = b"AGGREGATOR";

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone)]
pub struct EpochConfig {
    /// Epoch length in block heights.
    pub epoch_length: BlockHeightDelta,
    /// Number of shards currently.
    pub num_shards: NumShards,
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
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational,
    /// Stake threshold for becoming a fisherman.
    pub fishermen_threshold: Balance,
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational,
    /// Number of epochs after stake threshold was achieved to start next prtocol version.
    pub protocol_upgrade_num_epochs: EpochHeight,
}

/// Information per each block.
#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Eq, PartialEq, Clone, Debug)]
pub struct BlockInfo {
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
    #[cfg(feature = "protocol_feature_rectify_inflation")]
    pub timestamp_nanosec: u64,
}

impl BlockInfo {
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
        #[cfg(feature = "protocol_feature_rectify_inflation")] timestamp_nanosec: u64,
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
            #[cfg(feature = "protocol_feature_rectify_inflation")]
            timestamp_nanosec,
        }
    }

    pub fn proposals_iter(&self) -> ValidatorStakeIter {
        ValidatorStakeIter::v1(&self.proposals)
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ValidatorWeight(ValidatorId, u64);

/// Information per epoch.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub enum EpochInfo {
    V1(EpochInfoV1),
    V2(EpochInfoV2),
}

impl Default for EpochInfo {
    fn default() -> Self {
        Self::V2(EpochInfoV2::default())
    }
}

#[derive(SmartDefault, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(SmartDefault, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
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
    ) -> Self {
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

    #[inline]
    pub fn epoch_height_mut(&mut self) -> &mut EpochHeight {
        match self {
            Self::V1(v1) => &mut v1.epoch_height,
            Self::V2(v2) => &mut v2.epoch_height,
        }
    }

    #[inline]
    pub fn epoch_height(&self) -> EpochHeight {
        match self {
            Self::V1(v1) => v1.epoch_height,
            Self::V2(v2) => v2.epoch_height,
        }
    }

    #[inline]
    pub fn seat_price(&self) -> Balance {
        match self {
            Self::V1(v1) => v1.seat_price,
            Self::V2(v2) => v2.seat_price,
        }
    }

    #[inline]
    pub fn minted_amount(&self) -> Balance {
        match self {
            Self::V1(v1) => v1.minted_amount,
            Self::V2(v2) => v2.minted_amount,
        }
    }

    #[inline]
    pub fn block_producers_settlement(&self) -> &[ValidatorId] {
        match self {
            Self::V1(v1) => &v1.block_producers_settlement,
            Self::V2(v2) => &v2.block_producers_settlement,
        }
    }

    #[inline]
    pub fn chunk_producers_settlement(&self) -> &[Vec<ValidatorId>] {
        match self {
            Self::V1(v1) => &v1.chunk_producers_settlement,
            Self::V2(v2) => &v2.chunk_producers_settlement,
        }
    }

    #[inline]
    pub fn validator_kickout(&self) -> &HashMap<AccountId, ValidatorKickoutReason> {
        match self {
            Self::V1(v1) => &v1.validator_kickout,
            Self::V2(v2) => &v2.validator_kickout,
        }
    }

    #[inline]
    pub fn protocol_version(&self) -> ProtocolVersion {
        match self {
            Self::V1(v1) => v1.protocol_version,
            Self::V2(v2) => v2.protocol_version,
        }
    }

    #[inline]
    pub fn stake_change(&self) -> &BTreeMap<AccountId, Balance> {
        match self {
            Self::V1(v1) => &v1.stake_change,
            Self::V2(v2) => &v2.stake_change,
        }
    }

    #[inline]
    pub fn validator_reward(&self) -> &HashMap<AccountId, Balance> {
        match self {
            Self::V1(v1) => &v1.validator_reward,
            Self::V2(v2) => &v2.validator_reward,
        }
    }

    pub fn validators_iter(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(v1) => ValidatorStakeIter::v1(&v1.validators),
            Self::V2(v2) => ValidatorStakeIter::new(&v2.validators),
        }
    }

    pub fn fishermen_iter(&self) -> ValidatorStakeIter {
        match self {
            Self::V1(v1) => ValidatorStakeIter::v1(&v1.fishermen),
            Self::V2(v2) => ValidatorStakeIter::new(&v2.fishermen),
        }
    }

    pub fn validator_stake(&self, validator_id: u64) -> Balance {
        match self {
            Self::V1(v1) => v1.validators[validator_id as usize].stake,
            Self::V2(v2) => v2.validators[validator_id as usize].stake(),
        }
    }

    pub fn validator_account_id(&self, validator_id: u64) -> &AccountId {
        match self {
            Self::V1(v1) => &v1.validators[validator_id as usize].account_id,
            Self::V2(v2) => v2.validators[validator_id as usize].account_id(),
        }
    }

    pub fn account_is_validator(&self, account_id: &str) -> bool {
        match self {
            Self::V1(v1) => v1.validator_to_index.contains_key(account_id),
            Self::V2(v2) => v2.validator_to_index.contains_key(account_id),
        }
    }

    pub fn get_validator_id(&self, account_id: &AccountId) -> Option<&ValidatorId> {
        match self {
            Self::V1(v1) => v1.validator_to_index.get(account_id),
            Self::V2(v2) => v2.validator_to_index.get(account_id),
        }
    }

    pub fn get_validator_by_account(&self, account_id: &AccountId) -> Option<ValidatorStake> {
        match self {
            Self::V1(v1) => v1.validator_to_index.get(account_id).map(|validator_id| ValidatorStake::lift(v1.validators[*validator_id as usize].clone())),
            Self::V2(v2) => v2.validator_to_index.get(account_id).map(|validator_id| v2.validators[*validator_id as usize].clone()),
        }
    }

    pub fn get_validator(&self, validator_id: u64) -> ValidatorStake {
        match self {
            Self::V1(v1) => ValidatorStake::lift(v1.validators[validator_id as usize].clone()),
            Self::V2(v2) => v2.validators[validator_id as usize].clone(),
        }
    }

    pub fn account_is_fisherman(&self, account_id: &AccountId) -> bool {
        match self {
            Self::V1(v1) => v1.fishermen_to_index.contains_key(account_id),
            Self::V2(v2) => v2.fishermen_to_index.contains_key(account_id),
        }
    }

    pub fn get_fisherman_by_account(&self, account_id: &AccountId) -> Option<ValidatorStake> {
        match self {
            Self::V1(v1) => v1.fishermen_to_index.get(account_id).map(|validator_id| ValidatorStake::lift(v1.fishermen[*validator_id as usize].clone())),
            Self::V2(v2) => v2.fishermen_to_index.get(account_id).map(|validator_id| v2.fishermen[*validator_id as usize].clone()),
        }
    }

    pub fn get_fisherman(&self, fisherman_id: u64) -> ValidatorStake {
        match self {
            Self::V1(v1) => ValidatorStake::lift(v1.fishermen[fisherman_id as usize].clone()),
            Self::V2(v2) => v2.fishermen[fisherman_id as usize].clone(),
        }
    }

    pub fn validators_len(&self) -> usize {
        match self {
            Self::V1(v1) => v1.validators.len(),
            Self::V2(v2) => v2.validators.len(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct EpochSummary {
    pub prev_epoch_last_block_hash: CryptoHash,
    /// Proposals from the epoch, only the latest one per account
    pub all_proposals: Vec<ValidatorStakeV1>,
    /// Kickout set, includes slashed
    pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    /// Only for validators who met the threshold and didn't get slashed
    pub validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
    /// Protocol version for next epoch.
    pub next_version: ProtocolVersion,
}

impl EpochSummary {
    pub fn proposals_iter(&self) -> ValidatorStakeIter {
        ValidatorStakeIter::v1(&self.all_proposals)
    }
}

/// State that a slashed validator can be in.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum SlashState {
    /// Double Sign, will be partially slashed.
    DoubleSign,
    /// Malicious behavior but is already slashed (tokens taken away from account).
    AlreadySlashed,
    /// All other cases (tokens should be entirely slashed),
    Other,
}
