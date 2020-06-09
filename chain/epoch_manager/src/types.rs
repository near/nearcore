use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::challenge::SlashedValidator;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockChunkValidatorStats, BlockHeight, BlockHeightDelta, EpochHeight,
    EpochId, NumSeats, NumShards, ShardId, ValidatorId, ValidatorKickoutReason, ValidatorStake,
    ValidatorStats,
};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use num_rational::Rational;
use serde::Serialize;
use smart_default::SmartDefault;
use std::collections::{BTreeMap, HashMap};

use crate::EpochManager;

pub type RngSeed = [u8; 32];

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
    /// Threshold of stake that needs to indicate that they ready for upgrade.
    pub protocol_upgrade_stake_threshold: Rational,
    /// Number of epochs after stake threshold was achieved to start next prtocol version.
    pub protocol_upgrade_num_epochs: EpochHeight,
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
    /// Current protocol version during this epoch.
    #[default(PROTOCOL_VERSION)]
    pub protocol_version: ProtocolVersion,
}

/// Information per each block.
#[derive(Default, BorshSerialize, BorshDeserialize, Serialize, Clone, Debug)]
pub struct BlockInfo {
    pub height: BlockHeight,
    pub last_finalized_height: BlockHeight,
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
    /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
    pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
    /// For each shard, a map of validator id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
    pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
    /// All proposals in this epoch up to this block.
    pub all_proposals: Vec<ValidatorStake>,
    /// Protocol versions per validator.
    pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
}

impl BlockInfo {
    pub fn new(
        height: BlockHeight,
        last_finalized_height: BlockHeight,
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
            prev_hash,
            proposals,
            chunk_mask: validator_mask,
            slashed: slashed
                .into_iter()
                .map(|s| {
                    let slash_state =
                        if s.is_double_sign { SlashState::DoubleSign } else { SlashState::Other };
                    (s.account_id, slash_state)
                })
                .collect(),
            total_supply,
            latest_protocol_version,
            // TODO(2610): These values are "tip" and maintain latest in the current chain.
            // Current implementation is suboptimal and should be improved.
            epoch_first_block: CryptoHash::default(),
            epoch_id: EpochId::default(),
            block_tracker: HashMap::default(),
            shard_tracker: HashMap::default(),
            all_proposals: vec![],
            version_tracker: HashMap::default(),
        }
    }

    /// Updates block tracker given previous block tracker and current epoch info.
    pub fn update_block_tracker(
        &mut self,
        epoch_info: &EpochInfo,
        prev_block_height: BlockHeight,
        mut prev_block_tracker: HashMap<ValidatorId, ValidatorStats>,
    ) {
        let block_producer_id = epoch_info.block_producers_settlement
            [(self.height as u64 % (epoch_info.block_producers_settlement.len() as u64)) as usize];
        prev_block_tracker
            .entry(block_producer_id)
            .and_modify(|validator_stats| {
                validator_stats.produced += 1;
                validator_stats.expected += 1;
            })
            .or_insert(ValidatorStats { produced: 1, expected: 1 });
        // Iterate over all skipped blocks and increase the number of expected blocks.
        for height in prev_block_height + 1..self.height {
            let block_producer_id = epoch_info.block_producers_settlement
                [(height as u64 % (epoch_info.block_producers_settlement.len() as u64)) as usize];
            prev_block_tracker
                .entry(block_producer_id)
                .and_modify(|validator_stats| {
                    validator_stats.expected += 1;
                })
                .or_insert(ValidatorStats { produced: 0, expected: 1 });
        }
        self.block_tracker = prev_block_tracker;
    }

    pub fn update_shard_tracker(
        &mut self,
        epoch_info: &EpochInfo,
        prev_block_height: BlockHeight,
        mut prev_shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
    ) {
        for (i, mask) in self.chunk_mask.iter().enumerate() {
            let chunk_validator_id = EpochManager::chunk_producer_from_info(
                epoch_info,
                prev_block_height + 1,
                i as ShardId,
            );
            let tracker = prev_shard_tracker.entry(i as ShardId).or_insert_with(HashMap::new);
            tracker
                .entry(chunk_validator_id)
                .and_modify(|stats| {
                    if *mask {
                        stats.produced += 1;
                    }
                    stats.expected += 1;
                })
                .or_insert(ValidatorStats { produced: u64::from(*mask), expected: 1 });
        }
        self.shard_tracker = prev_shard_tracker;
    }

    pub fn update_version_tracker(
        &mut self,
        epoch_info: &EpochInfo,
        mut prev_version_tracker: HashMap<ValidatorId, ProtocolVersion>,
    ) {
        let block_producer_id = EpochManager::block_producer_from_info(epoch_info, self.height);
        prev_version_tracker.insert(block_producer_id, self.latest_protocol_version);
        self.version_tracker = prev_version_tracker;
    }
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
