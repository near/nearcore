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
use std::collections::{BTreeMap, HashMap, HashSet};

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

    /// Aggregates data from `block_info` into the running aggregator.
    ///
    /// Called in increasing-height order over an epoch's blocks: when this is
    /// invoked for a block, `self` already holds the stats for the epoch prefix
    /// `[epoch_start..block_info.prev]`. `chunk_producer_blacklist` is the blacklist
    /// computed from that prefix, so chunk attribution credits the producer the write
    /// path actually assigned. The blacklist is empty off-feature.
    pub fn update_tail(
        &mut self,
        block_info: &BlockInfo,
        epoch_info: &EpochInfo,
        shard_layout: &ShardLayout,
        prev_block_height: BlockHeight,
        chunk_producer_blacklist: &HashMap<ShardId, HashSet<ValidatorId>>,
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
        let empty = HashSet::new();

        for (shard_index, mask) in block_info.chunk_mask().iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            // Attribute the chunk to the producer the write path actually assigned
            // (the blacklist reassigns kicked-out slots). Off-feature the blacklist is
            // empty and this delegates to `sample_chunk_producer`, so attribution is
            // byte-identical to the pre-blacklist behavior.
            let chunk_producer_id = epoch_info
                .sample_chunk_producer_excluding(
                    shard_layout,
                    shard_id,
                    prev_block_height + 1,
                    chunk_producer_blacklist.get(&shard_id).unwrap_or(&empty),
                )
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
        }

        // Step 3: update version tracker
        // `version_tracker` and `all_proposals` are last-value, not additive. The walk
        // visits blocks in increasing height, so the last write wins — overwrite (not
        // `or_insert`) keeps the highest-height value, matching the previous backward
        // first-seen + merge behavior.
        let block_producer_id = epoch_info.sample_block_producer(block_info_height);
        self.version_tracker.insert(block_producer_id, *block_info.latest_protocol_version());

        // Step 4: update proposals
        for proposal in block_info.proposals_iter() {
            self.all_proposals.insert(proposal.account_id().clone(), proposal);
        }
    }
}
