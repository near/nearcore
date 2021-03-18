use std::collections::{BTreeMap, HashMap};

use borsh::{BorshDeserialize, BorshSerialize};
use log::error;

use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, BlockHeight, EpochId, ShardId, ValidatorId, ValidatorStats,
};
use near_primitives::version::ProtocolVersion;

use crate::EpochManager;

pub type RngSeed = [u8; 32];

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
        let block_info_height = *block_info.height();
        for height in prev_block_height + 1..=block_info_height {
            let block_producers_settlement = epoch_info.block_producers_settlement();
            let block_producer_id = block_producers_settlement
                [(height as u64 % (block_producers_settlement.len() as u64)) as usize];
            let entry = self.block_tracker.entry(block_producer_id);
            if height == block_info_height {
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
        for (i, mask) in block_info.chunk_mask().iter().enumerate() {
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
            EpochManager::block_producer_from_info(epoch_info, block_info_height);
        self.version_tracker
            .entry(block_producer_id)
            .or_insert_with(|| *block_info.latest_protocol_version());

        // Step 4: update proposals
        for proposal in block_info.proposals_iter() {
            self.all_proposals.entry(proposal.account_id().clone()).or_insert(proposal);
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
