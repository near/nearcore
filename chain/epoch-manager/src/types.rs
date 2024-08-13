use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives::block_header::BlockHeader;
use near_primitives::challenge::SlashedValidator;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, ChunkStats, EpochId, ShardId, ValidatorId, ValidatorStats,
};
use near_primitives::version::ProtocolVersion;
use std::collections::{BTreeMap, HashMap};
use tracing::{debug, debug_span};

use crate::EpochManager;

pub type RngSeed = [u8; 32];

/// Compressed information about block.
/// Useful for epoch manager.
#[derive(Default, Clone, Debug)]
pub struct BlockHeaderInfo {
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub height: BlockHeight,
    pub random_value: CryptoHash,
    pub last_finalized_height: BlockHeight,
    pub last_finalized_block_hash: CryptoHash,
    pub proposals: Vec<ValidatorStake>,
    pub slashed_validators: Vec<SlashedValidator>,
    pub chunk_mask: Vec<bool>,
    pub total_supply: Balance,
    pub latest_protocol_version: ProtocolVersion,
    pub timestamp_nanosec: u64,
    pub chunk_endorsements: Option<ChunkEndorsementsBitmap>,
}

impl BlockHeaderInfo {
    pub fn new(header: &BlockHeader, last_finalized_height: u64) -> Self {
        Self {
            hash: *header.hash(),
            prev_hash: *header.prev_hash(),
            height: header.height(),
            random_value: *header.random_value(),
            last_finalized_height,
            last_finalized_block_hash: *header.last_final_block(),
            proposals: header.prev_validator_proposals().collect(),
            slashed_validators: vec![],
            chunk_mask: header.chunk_mask().to_vec(),
            total_supply: header.total_supply(),
            latest_protocol_version: header.latest_protocol_version(),
            timestamp_nanosec: header.raw_timestamp(),
            chunk_endorsements: header.chunk_endorsements().cloned(),
        }
    }
}

/// Aggregator of information needed for validator computation at the end of the epoch.
#[derive(Clone, BorshSerialize, BorshDeserialize, Debug, Default)]
pub struct EpochInfoAggregator {
    /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
    pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
    /// For each shard, a map of validator id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
    pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, ChunkStats>>,
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

    /// Aggregates data from a block which directly precede the first block this
    /// aggregator has statistic on.
    ///
    /// For example, this method can be used in the following situation (where
    /// A through G are blocks ordered in increasing height and arrows denote
    /// ‘is parent’ relationship and B is start of a new epoch):
    ///
    /// ```text
    ///     ┌─ block_info
    /// A ← B ← C ← D ← E ← F ← G ← H ← I
    ///         │←── self ─→│
    /// ```
    ///
    /// Note that there is no method which allows adding information about G,
    /// H or I blocks into the aggregator.  The expected usage is to create
    /// a new aggregator starting from I, add H and G into it (using this
    /// method) and then [merge][`Self::merge`] it into `self`.
    pub fn update_tail(
        &mut self,
        block_info: &BlockInfo,
        epoch_info: &EpochInfo,
        prev_block_height: BlockHeight,
    ) {
        let _span =
            debug_span!(target: "epoch_tracker", "update_tail", prev_block_height).entered();

        // Step 1: update block tracer (block-production stats)

        let block_info_height = block_info.height();
        for height in prev_block_height + 1..=block_info_height {
            let block_producer_id = EpochManager::block_producer_from_info(epoch_info, height);
            let entry = self.block_tracker.entry(block_producer_id);
            if height == block_info_height {
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.produced += 1;
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 1, expected: 1 });
            } else {
                debug!(
                    target: "epoch_tracker",
                    block_producer = ?epoch_info.validator_account_id(block_producer_id),
                    block_height = height, "Missed block");
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 0, expected: 1 });
            }
        }

        // Step 2: update shard tracker (chunk production/endorsement stats)

        // TODO(#11900): Call EpochManager::get_chunk_validator_assignments to access the cached validator assignments.
        let chunk_validator_assignment = epoch_info.sample_chunk_validators(prev_block_height + 1);

        for (i, mask) in block_info.chunk_mask().iter().enumerate() {
            let shard_id: ShardId = i as ShardId;
            let chunk_producer_id = EpochManager::chunk_producer_from_info(
                epoch_info,
                prev_block_height + 1,
                i as ShardId,
            )
            .unwrap();
            let tracker = self.shard_tracker.entry(shard_id).or_insert_with(HashMap::new);
            tracker
                .entry(chunk_producer_id)
                .and_modify(|stats| {
                    if *mask {
                        *stats.produced_mut() += 1;
                    } else {
                        debug!(
                            target: "epoch_tracker",
                            chunk_validator = ?epoch_info.validator_account_id(chunk_producer_id),
                            shard_id = i,
                            block_height = prev_block_height + 1,
                            "Missed chunk");
                    }
                    *stats.expected_mut() += 1;
                })
                .or_insert_with(|| ChunkStats::new_with_production(u64::from(*mask), 1));

            let chunk_validators = chunk_validator_assignment
                .get(i)
                .map_or::<&[(u64, u128)], _>(&[], Vec::as_slice)
                .iter()
                .map(|(id, _)| *id)
                .collect_vec();
            let chunk_endorsements =
                if let Some(chunk_endorsements) = block_info.chunk_endorsements() {
                    chunk_endorsements.iter(shard_id)
                } else {
                    Box::new(std::iter::repeat(*mask).take(chunk_validators.len()))
                };
            for (chunk_validator_id, endorsement_produced) in
                chunk_validators.iter().zip(chunk_endorsements)
            {
                tracker
                    .entry(*chunk_validator_id)
                    .and_modify(|stats| {
                        let endorsement_stats = stats.endorsement_stats_mut();
                        if endorsement_produced {
                            endorsement_stats.produced += 1;
                        }
                        endorsement_stats.expected += 1;
                    })
                    .or_insert_with(|| {
                        ChunkStats::new_with_endorsement(u64::from(endorsement_produced), 1)
                    });
            }
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

    /// Merges information from `other` aggregator into `self`.
    ///
    /// The `other` aggregator must hold statistics from blocks which **follow**
    /// the ones this aggregator has.  Both aggregators have to be for the same
    /// epoch (the function panics if they aren’t).
    ///
    /// For example, this method can be used in the following situation (where
    /// A through J are blocks ordered in increasing height, arrows denote ‘is
    /// parent’ relationship and B is start of a new epoch):
    ///
    /// ```text
    ///     │←─── self ────→│   │←─ other ─→│
    /// A ← B ← C ← D ← E ← F ← G ← H ← I ← J
    ///     └── new epoch ──→
    /// ```
    ///
    /// Once the method finishes `self` will hold statistics for blocks from
    /// B till J.
    pub fn merge(&mut self, other: EpochInfoAggregator) {
        self.merge_common(&other);

        // merge version tracker
        self.version_tracker.extend(other.version_tracker);
        // merge proposals
        self.all_proposals.extend(other.all_proposals);

        self.last_block_hash = other.last_block_hash;
    }

    /// Merges information from `other` aggregator into `self`.
    ///
    /// The `other` aggregator must hold statistics from blocks which
    /// **precede** the ones this aggregator has.  Both aggregators have to be
    /// for the same epoch (the function panics if they aren’t).
    ///
    /// For example, this method can be used in the following situation (where
    /// A through J are blocks ordered in increasing height, arrows denote ‘is
    /// parent’ relationship and B is start of a new epoch):
    ///
    /// ```text
    ///     │←─── other ───→│   │←─ self ──→│
    /// A ← B ← C ← D ← E ← F ← G ← H ← I ← J
    ///     └── new epoch ──→
    /// ```
    ///
    /// Once the method finishes `self` will hold statistics for blocks from
    /// B till J.
    ///
    /// The method is a bit like doing `other.merge(self)` except that `other`
    /// is not changed.
    pub fn merge_prefix(&mut self, other: &EpochInfoAggregator) {
        self.merge_common(&other);

        // merge version tracker
        self.version_tracker.reserve(other.version_tracker.len());
        // TODO(mina86): Use try_insert once map_try_insert is stabilised.
        for (k, v) in other.version_tracker.iter() {
            self.version_tracker.entry(*k).or_insert_with(|| *v);
        }

        // merge proposals
        // TODO(mina86): Use try_insert once map_try_insert is stabilised.
        for (k, v) in other.all_proposals.iter() {
            self.all_proposals.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    /// Merges block and shard trackers from `other` into `self`.
    ///
    /// See [`Self::merge`] and [`Self::merge_prefix`] method for description of
    /// merging.
    fn merge_common(&mut self, other: &EpochInfoAggregator) {
        assert_eq!(self.epoch_id, other.epoch_id);

        // merge block tracker
        for (block_producer_id, stats) in other.block_tracker.iter() {
            self.block_tracker
                .entry(*block_producer_id)
                .and_modify(|e| {
                    e.expected += stats.expected;
                    e.produced += stats.produced
                })
                .or_insert_with(|| stats.clone());
        }
        // merge shard tracker
        for (shard_id, stats) in other.shard_tracker.iter() {
            self.shard_tracker
                .entry(*shard_id)
                .and_modify(|e| {
                    for (chunk_producer_id, stat) in stats.iter() {
                        e.entry(*chunk_producer_id)
                            .and_modify(|entry| {
                                *entry.expected_mut() += stat.expected();
                                *entry.produced_mut() += stat.produced();
                                entry.endorsement_stats_mut().expected +=
                                    stat.endorsement_stats().expected;
                                entry.endorsement_stats_mut().produced +=
                                    stat.endorsement_stats().produced;
                            })
                            .or_insert_with(|| stat.clone());
                    }
                })
                .or_insert_with(|| stats.clone());
        }
    }
}
