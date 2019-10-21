use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, ShardId, ValidatorId, ValidatorStake,
};

pub type RngSeed = [u8; 32];

/// Epoch config, determines validator assignment for given epoch.
/// Can change from epoch to epoch depending on the sharding and other parameters, etc.
#[derive(Clone)]
pub struct EpochConfig {
    /// Epoch length in blocks.
    pub epoch_length: BlockIndex,
    /// Number of shards currently.
    pub num_shards: ShardId,
    /// Number of block producers.
    pub num_block_producers: ValidatorId,
    /// Number of block producers per each shard.
    pub block_producers_per_shard: Vec<ValidatorId>,
    /// Expected number of fisherman per each shard.
    pub avg_fisherman_per_shard: Vec<ValidatorId>,
    /// Criterion for kicking out validators
    pub validator_kickout_threshold: u8,
}

#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct ValidatorWeight(ValidatorId, u64);

/// Information per epoch.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct EpochInfo {
    /// List of current validators.
    pub validators: Vec<ValidatorStake>,
    /// Validator account id to index in proposals.
    pub validator_to_index: HashMap<AccountId, ValidatorId>,
    /// Weights for each of the validators responsible for block production.
    pub block_producers: Vec<ValidatorId>,
    /// Per each shard, ids and seats of validators that are responsible.
    pub chunk_producers: Vec<Vec<ValidatorId>>,
    /// Weight of given validator used to determine how many shards they will validate.
    pub fishermen: Vec<ValidatorWeight>,
    /// New stake for validators
    pub stake_change: BTreeMap<AccountId, Balance>,
    /// Validator reward for the epoch
    pub validator_reward: HashMap<AccountId, Balance>,
    /// Total inflation in the epoch
    pub inflation: Balance,
}

/// Information per each block.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct BlockInfo {
    pub index: BlockIndex,
    pub prev_hash: CryptoHash,
    pub epoch_first_block: CryptoHash,
    pub epoch_id: EpochId,

    pub proposals: Vec<ValidatorStake>,
    pub chunk_mask: Vec<bool>,
    pub slashed: HashSet<AccountId>,
    /// Total rent paid in this block.
    pub rent_paid: Balance,
    /// Total validator reward in this block.
    pub validator_reward: Balance,
    /// Total balance burnt in this block.
    pub balance_burnt: Balance,
    /// Total supply at this block.
    pub total_supply: Balance,
}

impl BlockInfo {
    pub fn new(
        index: BlockIndex,
        prev_hash: CryptoHash,
        proposals: Vec<ValidatorStake>,
        validator_mask: Vec<bool>,
        slashed: HashSet<AccountId>,
        rent_paid: Balance,
        validator_reward: Balance,
        balance_burnt: Balance,
        total_supply: Balance,
    ) -> Self {
        Self {
            index,
            prev_hash,
            proposals,
            chunk_mask: validator_mask,
            slashed,
            rent_paid,
            validator_reward,
            balance_burnt,
            total_supply,
            // These values are not set. This code is suboptimal
            epoch_first_block: CryptoHash::default(),
            epoch_id: EpochId::default(),
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum EpochError {
    /// Error calculating threshold from given stakes for given number of seats.
    /// Only should happened if calling code doesn't check for integer value of stake > number of seats.
    ThresholdError(Balance, u64),
    /// Requesting validators for an epoch that wasn't computed yet.
    EpochOutOfBounds,
    /// Number of selected seats doesn't match requested.
    SelectedSeatsMismatch(u64, ValidatorId),
    /// Missing block hash in the storage (means there is some structural issue).
    MissingBlock(CryptoHash),
    /// Other error.
    Other(String),
}

impl std::error::Error for EpochError {}

impl fmt::Debug for EpochError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EpochError::ThresholdError(stakes_sum, num_seats) => write!(
                f,
                "Total stake {} must be higher than the number of seats {}",
                stakes_sum, num_seats
            ),
            EpochError::EpochOutOfBounds => write!(f, "Epoch out of bounds"),
            EpochError::SelectedSeatsMismatch(selected, required) => write!(
                f,
                "Number of selected seats {} < total number of seats {}",
                selected, required
            ),
            EpochError::MissingBlock(hash) => write!(f, "Missing block {}", hash),
            EpochError::Other(err) => write!(f, "Other: {}", err),
        }
    }
}

impl fmt::Display for EpochError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EpochError::ThresholdError(stake, num_seats) => {
                write!(f, "ThresholdError({}, {})", stake, num_seats)
            }
            EpochError::EpochOutOfBounds => write!(f, "EpochOutOfBounds"),
            EpochError::SelectedSeatsMismatch(num_seats, validator) => {
                write!(f, "SelectedSeatsMismatch({}, {})", num_seats, validator)
            }
            EpochError::MissingBlock(hash) => write!(f, "MissingBlock({})", hash),
            EpochError::Other(err) => write!(f, "Other({})", err),
        }
    }
}

impl From<std::io::Error> for EpochError {
    fn from(error: std::io::Error) -> Self {
        EpochError::Other(error.to_string())
    }
}

impl From<EpochError> for near_chain::Error {
    fn from(error: EpochError) -> Self {
        match error {
            EpochError::EpochOutOfBounds => near_chain::ErrorKind::EpochOutOfBounds,
            err => near_chain::ErrorKind::ValidatorError(err.to_string()),
        }
        .into()
    }
}

pub struct EpochSummary {
    pub last_block_hash: CryptoHash,
    pub all_proposals: Vec<ValidatorStake>,
    pub validator_kickout: HashSet<AccountId>,
    pub validator_online_ratio: HashMap<AccountId, (u64, u64)>,
    pub total_storage_rent: Balance,
    pub total_validator_reward: Balance,
    pub total_balance_burnt: Balance,
}
