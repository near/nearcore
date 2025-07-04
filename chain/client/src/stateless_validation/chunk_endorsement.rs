use super::validate::{ChunkRelevance, validate_chunk_endorsement};
use crate::metrics;
use crate::spice_core::CoreStatementsProcessor;
use near_cache::SyncLruCache;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::validator_assignment::ChunkEndorsementsState;
use near_primitives::types::AccountId;
use near_store::Store;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

// This is the number of unique chunks for which we would track the chunk endorsements.
// Ideally, we should not be processing more than num_shards chunks at a time.
const NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE: usize = 100;

/// Module to track chunk endorsements received from chunk validators.
pub struct ChunkEndorsementTracker {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Used to find the chain HEAD when validating partial witnesses.
    store: Store,
    /// We store the validated chunk endorsements received from chunk validators.
    chunk_endorsements:
        SyncLruCache<ChunkProductionKey, HashMap<AccountId, (ChunkHash, Signature)>>,
    /// With spice records endorsements as core statements.
    spice_core_processor: CoreStatementsProcessor,
}

impl ChunkEndorsementTracker {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        store: Store,
        spice_core_processor: CoreStatementsProcessor,
    ) -> Self {
        Self {
            epoch_manager,
            store,
            spice_core_processor,
            chunk_endorsements: SyncLruCache::new(
                NonZeroUsize::new(NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE).unwrap().into(),
            ),
        }
    }

    // Validate the chunk endorsement and store it in the cache.
    pub fn process_chunk_endorsement(&self, endorsement: ChunkEndorsement) -> Result<(), Error> {
        // Check if we have already received chunk endorsement from this validator.
        let key = endorsement.chunk_production_key();
        let account_id = endorsement.account_id();

        {
            let cache = self.chunk_endorsements.lock();
            if cache.peek(&key).is_some_and(|entry| entry.contains_key(account_id)) {
                tracing::debug!(target: "client", ?endorsement, "Already received chunk endorsement.");
                return Ok(());
            }
        }

        // Validate the chunk endorsement and store it in the cache.
        match validate_chunk_endorsement(self.epoch_manager.as_ref(), &endorsement, &self.store)? {
            ChunkRelevance::Relevant => {
                let mut cache = self.chunk_endorsements.lock();
                cache.get_or_insert_mut(key, || HashMap::new()).insert(
                    account_id.clone(),
                    (endorsement.chunk_hash(), endorsement.signature()),
                );
                let shard_id = endorsement.shard_id();
                if cfg!(feature = "protocol_feature_spice") {
                    self.spice_core_processor.record_chunk_endorsement(endorsement);
                }
                metrics::CHUNK_ENDORSEMENTS_ACCEPTED
                    .with_label_values(&[&shard_id.to_string()])
                    .inc();
            }
            irrelevant => {
                metrics::CHUNK_ENDORSEMENTS_REJECTED
                    .with_label_values(&[&endorsement.shard_id().to_string(), irrelevant.into()])
                    .inc();
            }
        };
        Ok(())
    }

    /// This function is called by block producer potentially multiple times if there's not enough stake.
    pub fn collect_chunk_endorsements(
        &self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkEndorsementsState, Error> {
        let shard_id = chunk_header.shard_id();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;

        let height_created = chunk_header.height_created();
        let key = ChunkProductionKey { shard_id, epoch_id, height_created };

        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;

        // Get the chunk_endorsements for the chunk from our cache.
        // Note that these chunk endorsements are already validated as part of process_chunk_endorsement.
        // We can safely rely on the following details
        //    1. The chunk endorsements are from valid chunk_validator for this chunk.
        //    2. The chunk endorsements signatures are valid.
        //    3. We still need to validate if the chunk_hash matches the chunk_header.chunk_hash()
        let mut cache = self.chunk_endorsements.lock();
        let entry = cache.get_or_insert(key, || HashMap::new());
        let validator_signatures = entry
            .into_iter()
            .filter(|(_, (chunk_hash, _))| chunk_hash == chunk_header.chunk_hash())
            .map(|(account_id, (_, signature))| (account_id, signature.clone()))
            .collect();

        // [perf] The lock here is held over the `compute_endorsement_state` call. If that constitutes an
        // expensive call AND we get into lock contention here, one needs to investigate into
        // cloning the account_id or using the RwLock.
        Ok(chunk_validator_assignments.compute_endorsement_state(validator_signatures))
    }
}
