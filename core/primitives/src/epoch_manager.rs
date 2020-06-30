use std::collections::{BTreeMap, HashMap};

use borsh::{BorshDeserialize, BorshSerialize};
use num_rational::Rational;
use serde::Serialize;
use smart_default::SmartDefault;

use crate::challenge::SlashedValidator;
use crate::hash::CryptoHash;
use crate::types::{
    AccountId, Balance, BlockChunkValidatorStats, BlockHeight, BlockHeightDelta, EpochHeight,
    EpochId, NumSeats, NumShards, ValidatorId, ValidatorKickoutReason, ValidatorStake,
};
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
#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug)]
pub struct BlockInfo {
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
}

impl BlockInfo {
    pub fn new(
        height: BlockHeight,
        last_finalized_height: BlockHeight,
        last_final_block_hash: CryptoHash,
        prev_hash: CryptoHash,
        proposals: Vec<ValidatorStake>,
        validator_mask: Vec<bool>,
        slashed: Vec<SlashedValidator>,
        total_supply: Balance,
        latest_protocol_version: ProtocolVersion,
    ) -> Self {
        Self {
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
        }
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct ValidatorWeight(ValidatorId, u64);

/// Information per epoch.
#[derive(SmartDefault, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct EpochInfo {
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
