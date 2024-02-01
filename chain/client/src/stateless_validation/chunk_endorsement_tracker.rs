use near_cache::SyncLruCache;
use std::collections::HashMap;
use std::sync::Arc;

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_body::ChunkEndorsementSignatures;
use near_primitives::checked_feature;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::stateless_validation::ChunkEndorsement;
use near_primitives::types::{AccountId, ShardId};

use crate::{metrics, Client};

// This is the number of unique chunks for which we would track the chunk endorsements.
// Ideally, we should not be processing more than num_shards chunks at a time.
const NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE: usize = 100;

/// Module to track chunk endorsements received from chunk validators.
pub struct ChunkEndorsementTracker {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// We store the validated chunk endorsements received from chunk validators
    /// This is keyed on chunk_hash and account_id of validator to avoid duplicates.
    /// Chunk endorsements would later be used as a part of block production.
    chunk_endorsements: SyncLruCache<ChunkHash, HashMap<AccountId, ChunkEndorsement>>,
}

impl Client {
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        let chunk_header = self.chain.get_chunk(endorsement.chunk_hash())?.cloned_header();
        self.chunk_endorsement_tracker.process_chunk_endorsement(&chunk_header, endorsement)
    }
}

impl ChunkEndorsementTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self {
            epoch_manager,
            chunk_endorsements: SyncLruCache::new(NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE),
        }
    }

    /// Function to process an incoming chunk endorsement from chunk validators.
    /// We first verify the chunk endorsement and then store it in a cache.
    /// We would later include the endorsements in the block production.
    pub(crate) fn process_chunk_endorsement(
        &self,
        chunk_header: &ShardChunkHeader,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        let chunk_hash = endorsement.chunk_hash();
        let account_id = &endorsement.account_id;

        // If we have already processed this chunk endorsement, return early.
        if self
            .chunk_endorsements
            .get(chunk_hash)
            .is_some_and(|existing_endorsements| existing_endorsements.get(account_id).is_some())
        {
            tracing::debug!(target: "stateless_validation", ?endorsement, "Already received chunk endorsement.");
            return Ok(());
        }

        if !self.epoch_manager.verify_chunk_endorsement(chunk_header, &endorsement)? {
            tracing::error!(target: "stateless_validation", ?endorsement, "Invalid chunk endorsement.");
            return Err(Error::InvalidChunkEndorsement);
        }

        // If we are the current block producer, we store the chunk endorsement for each chunk which
        // would later be used during block production to check whether to include the chunk or not.
        // TODO(stateless_validation): It's possible for a malicious validator to send endorsements
        // for 100 unique chunks thus pushing out current valid endorsements from our cache.
        // Maybe add check to ensure we don't accept endorsements from chunks already included in some block?
        // Maybe add check to ensure we don't accept endorsements from chunks that have too old height_created?
        tracing::debug!(target: "stateless_validation", ?endorsement, "Received and saved chunk endorsement.");
        let mut guard = self.chunk_endorsements.lock();
        guard.get_or_insert(chunk_hash.clone(), || HashMap::new());
        let chunk_endorsements = guard.get_mut(chunk_hash).unwrap();
        chunk_endorsements.insert(account_id.clone(), endorsement);

        Ok(())
    }

    /// Called by block producer.
    /// Returns Some(signatures) if node has enough signed stake for the chunk represented by chunk_header.
    /// Signatures have the same order as ordered_chunk_validators, thus ready to be included in a block as is.
    /// Returns None if chunk doesn't have enough stake.
    /// For older protocol version, we return an empty array of chunk endorsements.
    pub fn get_chunk_endorsement_signatures(
        &self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<Option<ChunkEndorsementSignatures>, Error> {
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        if !checked_feature!("stable", StatelessValidationV0, protocol_version) {
            // Return an empty array of chunk endorsements for older protocol versions.
            return Ok(Some(vec![]));
        }

        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        // Get the chunk_endorsements for the chunk from our cache.
        // Note that these chunk endorsements are already validated as part of process_chunk_endorsement.
        // We can safely rely on the the following details
        //    1. The chunk endorsements are from valid chunk_validator for this chunk.
        //    2. The chunk endorsements signatures are valid.
        let Some(chunk_endorsements) = self.chunk_endorsements.get(&chunk_header.chunk_hash())
        else {
            // Early return if no chunk_enforsements found in our cache.
            record_endorsement_metrics(
                chunk_header.shard_id(),
                0.0,
                chunk_validator_assignments.assignments().len(),
            );
            return Ok(None);
        };

        let endorsement_stats = chunk_validator_assignments
            .compute_endorsement_stats(&chunk_endorsements.keys().collect());
        record_endorsement_metrics(
            chunk_header.shard_id(),
            endorsement_stats.endorsed_stake as f64 / endorsement_stats.total_stake as f64,
            endorsement_stats
                .total_validators_count
                .saturating_sub(endorsement_stats.endorsed_validators_count),
        );

        // Check whether the current set of chunk_validators have enough stake to include chunk in block.
        if !endorsement_stats.has_enough_stake() {
            return Ok(None);
        }

        // We've already verified the chunk_endorsements are valid, collect signatures.
        let signatures = chunk_validator_assignments
            .ordered_chunk_validators()
            .iter()
            .map(|account_id| {
                // map Option<ChunkEndorsement> to Option<Box<Signature>>
                chunk_endorsements
                    .get(account_id)
                    .map(|endorsement| Box::new(endorsement.signature.clone()))
            })
            .collect();

        Ok(Some(signatures))
    }
}

fn record_endorsement_metrics(
    shard_id: ShardId,
    endorsed_stake_ratio: f64,
    missing_endorsement_count: usize,
) {
    let shard_label = shard_id.to_string();
    let label_values = &[shard_label.as_ref()];
    metrics::BLOCK_PRODUCER_ENDORSED_STAKE_RATIO
        .with_label_values(label_values)
        .observe(endorsed_stake_ratio);
    metrics::BLOCK_PRODUCER_MISSING_ENDORSEMENT_COUNT
        .with_label_values(label_values)
        .observe(missing_endorsement_count as f64);
}
