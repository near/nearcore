use lru::LruCache;
use near_chain::ChainStoreAccess;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_body::ChunkEndorsementSignatures;
use near_primitives::checked_feature;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::stateless_validation::{ChunkEndorsement, EndorsementStats};
use near_primitives::types::AccountId;

use crate::Client;

// This is the number of unique chunks for which we would track the chunk endorsements.
// Ideally, we should not be processing more than num_shards chunks at a time.
const NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE: usize = 100;

pub enum ChunkEndorsementsState {
    Endorsed(Option<EndorsementStats>, ChunkEndorsementSignatures),
    NotEnoughStake(Option<EndorsementStats>),
}

impl ChunkEndorsementsState {
    pub fn stats(&self) -> Option<&EndorsementStats> {
        match self {
            Self::Endorsed(stats, _) => stats.as_ref(),
            Self::NotEnoughStake(stats) => stats.as_ref(),
        }
    }
}

/// Module to track chunk endorsements received from chunk validators.
pub struct ChunkEndorsementTracker {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    inner: Mutex<ChunkEndorsementTrackerInner>,
}

struct ChunkEndorsementTrackerInner {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// We store the validated chunk endorsements received from chunk validators
    /// This is keyed on chunk_hash and account_id of validator to avoid duplicates.
    /// Chunk endorsements would later be used as a part of block production.
    chunk_endorsements:
        LruCache<ChunkHash, (ShardChunkHeader, HashMap<AccountId, ChunkEndorsement>)>,
    /// We store chunk endorsements to be processed later because we did not have
    /// chunks ready at the time we received that endorsements from validators.
    /// This is keyed on chunk_hash and account_id of validator to avoid duplicates.
    pending_chunk_endorsements: LruCache<ChunkHash, HashMap<AccountId, ChunkEndorsement>>,
}

impl Client {
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        // We need the chunk header in order to process the chunk endorsement.
        // If we don't have the header, then queue it up for when we do have the header.
        // We must use the partial chunk (as opposed to the full chunk) in order to get
        // the chunk header, because we may not be tracking that shard.
        match self.chain.chain_store().get_partial_chunk(endorsement.chunk_hash()) {
            Ok(chunk) => self
                .chunk_endorsement_tracker
                .process_chunk_endorsement(&chunk.cloned_header(), endorsement),
            Err(Error::ChunkMissing(_)) => {
                tracing::debug!(target: "client", ?endorsement, "Endorsement arrived before chunk.");
                self.chunk_endorsement_tracker.add_chunk_endorsement_to_pending_cache(endorsement)
            }
            Err(error) => return Err(error),
        }
    }
}

impl ChunkEndorsementTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self {
            epoch_manager: epoch_manager.clone(),
            inner: Mutex::new(ChunkEndorsementTrackerInner {
                epoch_manager,
                chunk_endorsements: LruCache::new(
                    NonZeroUsize::new(NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE).unwrap(),
                ),
                // We can use a different cache size if needed, it does not have to be the same as for `chunk_endorsements`.
                pending_chunk_endorsements: LruCache::new(
                    NonZeroUsize::new(NUM_CHUNKS_IN_CHUNK_ENDORSEMENTS_CACHE).unwrap(),
                ),
            }),
        }
    }

    /// Process pending endorsements for the given chunk header.
    /// It removes these endorsements from the `pending_chunk_endorsements` cache.
    pub fn process_pending_endorsements(&self, chunk_header: &ShardChunkHeader) {
        self.inner.lock().unwrap().process_pending_endorsements(chunk_header);
    }

    /// Add the chunk endorsement to a cache of pending chunk endorsements (if not yet there).
    pub(crate) fn add_chunk_endorsement_to_pending_cache(
        &self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        self.inner.lock().unwrap().process_chunk_endorsement_impl(endorsement, None, false)
    }

    /// Function to process an incoming chunk endorsement from chunk validators.
    /// We first verify the chunk endorsement and then store it in a cache.
    /// We would later include the endorsements in the block production.
    pub(crate) fn process_chunk_endorsement(
        &self,
        chunk_header: &ShardChunkHeader,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "client", "process_chunk_endorsement", chunk_hash=?chunk_header.chunk_hash(), shard_id=chunk_header.shard_id()).entered();

        // Validate the endorsement before locking the mutex to improve performance.
        if !self.epoch_manager.verify_chunk_endorsement(&chunk_header, &endorsement)? {
            tracing::error!(target: "client", ?endorsement, "Invalid chunk endorsement.");
            return Err(Error::InvalidChunkEndorsement);
        }
        self.inner.lock().unwrap().process_chunk_endorsement_impl(
            endorsement,
            Some(chunk_header),
            true,
        )
    }

    /// Called by block producer.
    /// Returns ChunkEndorsementsState::Endorsed if node has enough signed stake for the chunk
    /// represented by chunk_header.
    /// Signatures have the same order as ordered_chunk_validators, thus ready to be included in a block as is.
    /// Returns ChunkEndorsementsState::NotEnoughStake if chunk doesn't have enough stake.
    /// For older protocol version, we return ChunkEndorsementsState::Endorsed with an empty array of
    /// chunk endorsements.
    pub fn compute_chunk_endorsements(
        &self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkEndorsementsState, Error> {
        self.inner.lock().unwrap().compute_chunk_endorsements_impl(chunk_header)
    }
}

impl ChunkEndorsementTrackerInner {
    /// Process pending endorsements for the given chunk header.
    /// It removes these endorsements from the `pending_chunk_endorsements` cache.
    pub fn process_pending_endorsements(&mut self, chunk_header: &ShardChunkHeader) {
        let chunk_hash = &chunk_header.chunk_hash();
        let chunk_endorsements = self.pending_chunk_endorsements.pop(chunk_hash);
        let Some(chunk_endorsements) = chunk_endorsements else {
            return;
        };
        tracing::debug!(target: "client", ?chunk_hash, "Processing pending chunk endorsements.");
        for endorsement in chunk_endorsements.values() {
            if let Err(error) =
                self.process_chunk_endorsement_impl(endorsement.clone(), Some(chunk_header), false)
            {
                tracing::debug!(target: "client", ?endorsement, ?error, "Error processing pending chunk endorsement");
            }
        }
    }

    /// If the chunk header is available, we will verify the chunk endorsement and then store it in a cache.
    /// Otherwise, we store the endorsement in a separate cache of endorsements to be processed when the chunk is ready.
    fn process_chunk_endorsement_impl(
        &mut self,
        endorsement: ChunkEndorsement,
        chunk_header: Option<&ShardChunkHeader>,
        already_validated: bool,
    ) -> Result<(), Error> {
        let chunk_hash = endorsement.chunk_hash();
        let account_id = &endorsement.account_id;

        let existing_entry = self.chunk_endorsements.peek(chunk_hash);

        // If we have already processed this chunk endorsement, return early.
        if existing_entry.is_some_and(|(_, existing_endorsements)| {
            existing_endorsements.contains_key(account_id)
        }) {
            tracing::debug!(target: "client", ?endorsement, "Already received chunk endorsement.");
            return Ok(());
        }

        // If we are the current block producer, we store the chunk endorsement for each chunk which
        // would later be used during block production to check whether to include the chunk or not.
        // TODO(stateless_validation): It's possible for a malicious validator to send endorsements
        // for 100 unique chunks thus pushing out current valid endorsements from our cache.
        // Maybe add check to ensure we don't accept endorsements from chunks already included in some block?
        // Maybe add check to ensure we don't accept endorsements from chunks that have too old height_created?
        tracing::debug!(target: "client", ?endorsement, "Received and saved chunk endorsement.");

        // The header might be available in the endorsement cache, even if it isn't provided.
        // In such case it should be treated as a non-pending endorsement.
        let header = chunk_header.or_else(|| existing_entry.map(|(header, _)| header));

        if let Some(chunk_header) = header {
            if !already_validated
                && !self.epoch_manager.verify_chunk_endorsement(&chunk_header, &endorsement)?
            {
                tracing::error!(target: "client", ?endorsement, "Invalid chunk endorsement.");
                return Err(Error::InvalidChunkEndorsement);
            }

            if self.chunk_endorsements.peek(chunk_hash).is_none() {
                self.chunk_endorsements
                    .put(chunk_hash.clone(), (chunk_header.clone(), HashMap::new()));
            }
            self.chunk_endorsements
                .get_mut(chunk_hash)
                .unwrap()
                .1
                .insert(account_id.clone(), endorsement);
        } else {
            // Chunk header is not available, store the endorsement in the pending cache.
            self.pending_chunk_endorsements.get_or_insert(chunk_hash.clone(), || HashMap::new());
            self.pending_chunk_endorsements
                .get_mut(chunk_hash)
                .unwrap()
                .insert(account_id.clone(), endorsement);
        }

        Ok(())
    }

    pub fn compute_chunk_endorsements_impl(
        &mut self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkEndorsementsState, Error> {
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        if !checked_feature!("stable", StatelessValidationV0, protocol_version) {
            // Return an empty array of chunk endorsements for older protocol versions.
            return Ok(ChunkEndorsementsState::Endorsed(None, vec![]));
        }

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
        let Some((_header, chunk_endorsements)) =
            self.chunk_endorsements.get(&chunk_header.chunk_hash())
        else {
            // Early return if no chunk_endorsements found in our cache.
            return Ok(ChunkEndorsementsState::NotEnoughStake(None));
        };

        let endorsement_stats = chunk_validator_assignments
            .compute_endorsement_stats(&chunk_endorsements.keys().collect());

        // Check whether the current set of chunk_validators have enough stake to include chunk in block.
        if !endorsement_stats.has_enough_stake() {
            return Ok(ChunkEndorsementsState::NotEnoughStake(Some(endorsement_stats)));
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

        Ok(ChunkEndorsementsState::Endorsed(Some(endorsement_stats), signatures))
    }
}
