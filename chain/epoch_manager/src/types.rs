use borsh::{BorshDeserialize, BorshSerialize};
use log::error;
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
    /// The minimum stake required for staking is last seat price divided by this number.
    pub minimum_stake_divisor: u64,
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
    /// Seat price of this epoch.
    pub seat_price: Balance,
    /// Current protocol version during this epoch.
    #[default(PROTOCOL_VERSION)]
    pub protocol_version: ProtocolVersion,
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

/// Aggregator of information needed for validator computation at the end of the epoch.
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct EpochInfoAggregator {
    /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
    pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
    /// For each shard, a map of validator id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
    pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
    /// Latest protocol version that each validator supports.
    pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
    /// All proposals in this epoch up to this block.
    pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
    /// Id of the epoch that this aggregator is in.
    pub epoch_id: EpochId,
    /// Last block hash recorded.
    pub last_block_hash: CryptoHash,
}

impl EpochInfoAggregator {
    pub fn new(epoch_id: EpochId, last_block_hash: CryptoHash) -> Self {
        Self {
            block_tracker: Default::default(),
            shard_tracker: Default::default(),
            version_tracker: Default::default(),
            all_proposals: BTreeMap::default(),
            epoch_id,
            last_block_hash,
        }
    }

    pub fn update(
        &mut self,
        block_info: &BlockInfo,
        epoch_info: &EpochInfo,
        prev_block_height: BlockHeight,
    ) {
        // Step 1: update block tracer
        for height in prev_block_height + 1..=block_info.height {
            let block_producer_id = epoch_info.block_producers_settlement
                [(height as u64 % (epoch_info.block_producers_settlement.len() as u64)) as usize];
            let entry = self.block_tracker.entry(block_producer_id);
            if height == block_info.height {
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.produced += 1;
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 1, expected: 1 });
            } else {
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 0, expected: 1 });
            }
        }

        // Step 2: update shard tracker
        for (i, mask) in block_info.chunk_mask.iter().enumerate() {
            let chunk_validator_id = EpochManager::chunk_producer_from_info(
                epoch_info,
                prev_block_height + 1,
                i as ShardId,
            );
            let tracker = self.shard_tracker.entry(i as ShardId).or_insert_with(HashMap::new);
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

        // Step 3: update version tracker
        let block_producer_id =
            EpochManager::block_producer_from_info(epoch_info, block_info.height);
        self.version_tracker
            .entry(block_producer_id)
            .or_insert_with(|| block_info.latest_protocol_version);

        // Step 4: update proposals
        for proposal in block_info.proposals.iter() {
            self.all_proposals
                .entry(proposal.account_id.clone())
                .or_insert_with(|| proposal.clone());
        }
    }

    pub fn merge(&mut self, new_aggregator: EpochInfoAggregator, overwrite: bool) {
        if self.epoch_id != new_aggregator.epoch_id {
            debug_assert!(false);
            error!(target: "epoch_manager", "Trying to merge an aggregator with epoch id {:?}, but our epoch id is {:?}", new_aggregator.epoch_id, self.epoch_id);
            return;
        }
        if overwrite {
            *self = new_aggregator;
        } else {
            // merge block tracker
            for (block_producer_id, stats) in new_aggregator.block_tracker {
                self.block_tracker
                    .entry(block_producer_id)
                    .and_modify(|e| {
                        e.expected += stats.expected;
                        e.produced += stats.produced
                    })
                    .or_insert_with(|| stats);
            }
            // merge shard tracker
            for (shard_id, stats) in new_aggregator.shard_tracker {
                self.shard_tracker
                    .entry(shard_id)
                    .and_modify(|e| {
                        for (chunk_producer_id, stat) in stats.iter() {
                            e.entry(*chunk_producer_id)
                                .and_modify(|entry| {
                                    entry.expected += stat.expected;
                                    entry.produced += stat.produced;
                                })
                                .or_insert_with(|| stat.clone());
                        }
                    })
                    .or_insert_with(|| stats);
            }
            // merge version tracker
            self.version_tracker.extend(new_aggregator.version_tracker.into_iter());
            // merge proposals
            self.all_proposals.extend(new_aggregator.all_proposals.into_iter());
            self.last_block_hash = new_aggregator.last_block_hash;
        }
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
