use std::collections::{BTreeMap, HashMap, HashSet};

use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, ChunkStats, EpochId, ShardId, ValidatorId, ValidatorStats,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_schema_checker_lib::ProtocolSchema;

/// Aggregator of information needed for validator computation at the end of the epoch.
#[derive(
    Clone, BorshSerialize, BorshDeserialize, Debug, Default, serde::Serialize, ProtocolSchema,
)]
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
    /// Blacklist of chunk producers.
    pub excluded_chunk_producers: HashMap<ShardId, HashSet<ValidatorId>>,
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
            excluded_chunk_producers: Default::default(),
        }
    }

    /// Returns a set of validators that have missed more than the threshold number of chunks
    /// in the current epoch.
    pub fn get_excluded_chunk_producers_for_shard(
        &self,
        shard_id: &ShardId,
    ) -> HashSet<ValidatorId> {
        self.excluded_chunk_producers.get(shard_id).cloned().unwrap_or_default()
    }

    pub fn blacklist_chunk_producer(
        &mut self,
        shard_id: ShardId,
        chunk_producer_id: ValidatorId,
        producers_count: usize,
    ) {
        let excluded = self.excluded_chunk_producers.entry(shard_id).or_insert_with(HashSet::new);
        if excluded.contains(&chunk_producer_id) {
            return;
        }

        excluded.insert(chunk_producer_id);

        // If all producers are excluded, unban one
        if excluded.len() == producers_count {
            // TODO: find a fairer way to unban a producer
            let unbanned = excluded.iter().next().cloned().unwrap();
            excluded.remove(&unbanned);
        }
    }

    pub fn should_ban_chunk_producer(
        &self,
        shard_id: ShardId,
        chunk_producer_id: ValidatorId,
    ) -> bool {
        let Some(shard_info) = self.shard_tracker.get(&shard_id) else { return false };
        let Some(producer_stats) = shard_info.get(&chunk_producer_id) else { return false };

        let produced = producer_stats.produced();
        let expected = producer_stats.expected();

        expected - produced >= 20 && (produced as f64 / expected as f64) < 0.95
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
        shard_layout: &ShardLayout,
        prev_block_height: BlockHeight,
    ) {
        let _span = tracing::debug_span!(target: "epoch_tracker", "update_tail", prev_block_height)
            .entered();

        // Step 1: update block tracer (block-production stats)

        let block_info_height = block_info.height();
        for height in prev_block_height + 1..=block_info_height {
            let block_producer_id = epoch_info.sample_block_producer(height);
            let entry = self.block_tracker.entry(block_producer_id);
            if height == block_info_height {
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.produced += 1;
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 1, expected: 1 });
            } else {
                tracing::debug!(
                    target: "epoch_tracker",
                    block_producer = ?epoch_info.validator_account_id(block_producer_id),
                    block_height = height, "missed block");
                entry
                    .and_modify(|validator_stats| {
                        validator_stats.expected += 1;
                    })
                    .or_insert(ValidatorStats { produced: 0, expected: 1 });
            }
        }

        // Step 2: update shard tracker (chunk production/endorsement stats)
        let chunk_validator_assignment = epoch_info.sample_chunk_validators(prev_block_height + 1);

        for (shard_index, mask) in block_info.chunk_mask().iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            let blacklist = self.get_excluded_chunk_producers_for_shard(&shard_id);
            let chunk_producer_id = epoch_info
                .sample_chunk_producer(shard_layout, shard_id, prev_block_height + 1, blacklist)
                .unwrap();
            let tracker = self.shard_tracker.entry(shard_id).or_insert_with(HashMap::new);
            tracker
                .entry(chunk_producer_id)
                .and_modify(|stats| {
                    if *mask {
                        *stats.produced_mut() += 1;
                    } else {
                        tracing::debug!(
                            target: "epoch_tracker",
                            chunk_validator = ?epoch_info.validator_account_id(chunk_producer_id),
                            %shard_id,
                            block_height = prev_block_height + 1,
                            "missed chunk");
                    }
                    *stats.expected_mut() += 1;
                })
                .or_insert_with(|| ChunkStats::new_with_production(u64::from(*mask), 1));

            // With spice we have no chunk endorsements for chunks by design.
            if ProtocolFeature::Spice.enabled(epoch_info.protocol_version()) {
                continue;
            }

            let chunk_validators = chunk_validator_assignment
                .get(shard_index)
                .map_or::<&[(u64, Balance)], _>(&[], Vec::as_slice)
                .iter()
                .map(|(id, _)| *id)
                .collect_vec();
            // The following iterates over the chunk validator assignments and yields true if the endorsement from the validator
            // assigned the respective position was received or false if the endorsement was missed.
            // NOTE:(#11900): If the chunk endorsements received from the chunk validators are not recorded in the block header,
            // we use the chunk production stats as the endorsements stats, ie. if the chunk is produced then we assume that
            // the endorsements from all the chunk validators assigned to that chunk are received (hence the `else` branch below).
            let chunk_endorsements = if let Some(chunk_endorsements) =
                block_info.chunk_endorsements()
            {
                // For old chunks, we optimize the block and its header by not including the chunk endorsements and
                // corresponding bitmaps. Thus, we expect that the bitmap is non-empty for new chunks only.
                if *mask {
                    debug_assert!(
                        chunk_endorsements.len(shard_index).unwrap()
                            == chunk_validators.len().div_ceil(8) * 8,
                        "Chunk endorsement bitmap length is inconsistent with number of chunk validators. Bitmap length={}, num validators={}, shard_index={}",
                        chunk_endorsements.len(shard_index).unwrap(),
                        chunk_validators.len(),
                        shard_index
                    );
                    chunk_endorsements.iter(shard_index)
                } else {
                    debug_assert_eq!(
                        chunk_endorsements.len(shard_index).unwrap(),
                        0,
                        "Chunk endorsement bitmap must be empty for missing chunk. Bitmap length={}, shard_index={}",
                        chunk_endorsements.len(shard_index).unwrap(),
                        shard_index
                    );
                    Box::new(std::iter::repeat(false).take(chunk_validators.len()))
                }
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

            // Check if the chunk producers should be banned
            if self.should_ban_chunk_producer(shard_id, chunk_producer_id) {
                let producers_count =
                    epoch_info.chunk_producers_settlement().get(shard_index).unwrap().len();
                self.blacklist_chunk_producer(shard_id, chunk_producer_id, producers_count);
            }
        }

        // Step 3: update version tracker
        let block_producer_id = epoch_info.sample_block_producer(block_info_height);
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
        // TODO(mina86): Use try_insert once map_try_insert is stabilized.
        for (k, v) in &other.version_tracker {
            self.version_tracker.entry(*k).or_insert_with(|| *v);
        }

        // merge proposals
        // TODO(mina86): Use try_insert once map_try_insert is stabilized.
        for (k, v) in &other.all_proposals {
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
        for (block_producer_id, stats) in &other.block_tracker {
            self.block_tracker
                .entry(*block_producer_id)
                .and_modify(|e| {
                    e.expected += stats.expected;
                    e.produced += stats.produced
                })
                .or_insert_with(|| stats.clone());
        }
        // merge shard tracker
        for (shard_id, stats) in &other.shard_tracker {
            self.shard_tracker
                .entry(*shard_id)
                .and_modify(|e| {
                    for (chunk_producer_id, stat) in stats {
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

        // merge excluded chunk producers
        for (shard_id, excluded) in &other.excluded_chunk_producers {
            self.excluded_chunk_producers
                .entry(*shard_id)
                .and_modify(|entry| {
                    entry.extend(excluded.iter().copied());
                })
                .or_insert_with(|| excluded.clone());
        }

        // Re-check bans after merging chunk stats.
        let mut to_ban: Vec<(ShardId, ValidatorId, usize)> = Vec::new();
        for (shard_id, stats) in &self.shard_tracker {
            let producers_count = stats.len();
            for (chunk_producer_id, _) in stats {
                if self.should_ban_chunk_producer(*shard_id, *chunk_producer_id) {
                    to_ban.push((*shard_id, *chunk_producer_id, producers_count));
                }
            }
        }
        for (shard_id, chunk_producer_id, producers_count) in to_ban {
            self.blacklist_chunk_producer(shard_id, chunk_producer_id, producers_count);
        }
    }
}
