use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochId, ShardId, ValidatorId, ValidatorStats,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{BTreeMap, HashMap};

/// Aggregator of information needed for validator computation at the end of the epoch.
#[derive(
    Clone, BorshSerialize, BorshDeserialize, Debug, Default, serde::Serialize, ProtocolSchema,
)]
pub struct EpochInfoAggregator {
    /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
    pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
    /// For each shard, a map of chunk producer id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
    pub chunk_production: HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
    /// Map from chunk validator id to (num_endorsements_recorded, num_endorsements_expected) so
    /// far in the given epoch, summed over all shards. `produced` counts the endorsements that
    /// the blocks record.
    pub chunk_endorsement: HashMap<ValidatorId, ValidatorStats>,
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
            chunk_production: Default::default(),
            chunk_endorsement: Default::default(),
            version_tracker: Default::default(),
            all_proposals: BTreeMap::default(),
            epoch_id,
            last_block_hash,
        }
    }

    /// Adds the stats of `block_info` to this aggregator.
    ///
    /// Block recording calls it on an empty aggregator for one block, then
    /// [merges][`Self::merge`] the result into the parent's aggregator.
    /// `chunk_producers` carries the per-shard producers resolved via the
    /// grandparent anchor (EarlyKickout); `None` falls back to the legacy
    /// height-based sampler.
    pub fn update_tail(
        &mut self,
        block_info: &BlockInfo,
        epoch_info: &EpochInfo,
        shard_layout: &ShardLayout,
        prev_block_height: BlockHeight,
        chunk_producers: Option<&HashMap<ShardId, ValidatorId>>,
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

        // Step 2: update chunk production and endorsement stats
        let chunk_validator_assignment = epoch_info.sample_chunk_validators(prev_block_height + 1);

        for (shard_index, mask) in block_info.chunk_mask().iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            let chunk_producer_id = match chunk_producers.and_then(|cp| cp.get(&shard_id)) {
                Some(chunk_producer_id) => *chunk_producer_id,
                None => epoch_info
                    .sample_chunk_producer(shard_layout, shard_id, prev_block_height + 1)
                    .unwrap(),
            };
            let production_stats = self
                .chunk_production
                .entry(shard_id)
                .or_default()
                .entry(chunk_producer_id)
                .or_default();
            if *mask {
                production_stats.produced += 1;
            } else {
                tracing::debug!(
                    target: "epoch_tracker",
                    chunk_producer = ?epoch_info.validator_account_id(chunk_producer_id),
                    %shard_id,
                    block_height = prev_block_height + 1,
                    "missed chunk");
            }
            production_stats.expected += 1;

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
                let endorsement_stats =
                    self.chunk_endorsement.entry(*chunk_validator_id).or_default();
                if endorsement_produced {
                    endorsement_stats.produced += 1;
                }
                endorsement_stats.expected += 1;
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

    /// Merges block, chunk production and chunk endorsement stats from `other` into `self`.
    ///
    /// See [`Self::merge`] for a description of merging.
    fn merge_common(&mut self, other: &EpochInfoAggregator) {
        assert_eq!(self.epoch_id, other.epoch_id);

        add_stats(&mut self.block_tracker, &other.block_tracker);
        for (shard_id, stats) in &other.chunk_production {
            add_stats(self.chunk_production.entry(*shard_id).or_default(), stats);
        }
        add_stats(&mut self.chunk_endorsement, &other.chunk_endorsement);
    }
}

fn add_stats(
    target: &mut HashMap<ValidatorId, ValidatorStats>,
    source: &HashMap<ValidatorId, ValidatorStats>,
) {
    for (validator_id, stats) in source {
        let entry = target.entry(*validator_id).or_default();
        entry.produced += stats.produced;
        entry.expected += stats.expected;
    }
}
