use chrono::{DateTime, Utc};
use itertools::Itertools;
use lru::LruCache;
use near_primitives::checked_feature;
use near_primitives::version::ProtocolVersion;
use std::collections::HashMap;
use std::sync::Arc;

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_body::ChunkEndorsementSignatures;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{AccountId, EpochId, ShardId};

use crate::chunk_validation::ChunkValidator;
use crate::metrics;

const CHUNK_HEADERS_FOR_INCLUSION_CACHE_SIZE: usize = 2048;
const NUM_EPOCH_CHUNK_PRODUCERS_TO_KEEP_IN_BLOCKLIST: usize = 1000;

struct ChunkInfo {
    // chunk_header, received_time and chunk_producer are populated when we call mark_chunk_header_ready_for_inclusion
    pub chunk_header: ShardChunkHeader,
    pub received_time: DateTime<Utc>,
    pub chunk_producer: AccountId,
    // The signatures are populated later during call to chunk_validator.get_chunk_endorsement_signature
    // This is a sort of cache to store previously fetched signatures.
    pub signatures: Option<ChunkEndorsementSignatures>,
}

pub struct ChunkInclusionTracker {
    // Track chunks that are ready to be included in a block.
    // Key is the previous_block_hash as the chunk is created based on this block. It's possible that
    // the block included isn't of height previous_block_height + 1 in cases of skipped blocks etc.
    // We store the map of chunks from [shard_id] to chunk_hash
    prev_block_to_chunk_hash_ready: LruCache<CryptoHash, HashMap<ShardId, ChunkHash>>,

    // Map from chunk_hash to chunk_info.
    // ChunkInfo stores the chunk_header, received_time, chunk_producer and chunk endorsements.
    // Cleaning up of chunk_hash_to_chunk_info is handled during cache eviction from prev_block_to_chunk_hash_ready.
    chunk_hash_to_chunk_info: HashMap<ChunkHash, ChunkInfo>,

    // Track banned chunk producers for a given epoch. We filter out chunks produced by them.
    banned_chunk_producers: LruCache<(EpochId, AccountId), ()>,

    // Epoch manager to get the protocol version for a given epoch.
    epoch_manager: Arc<dyn EpochManagerAdapter>,
}

impl ChunkInclusionTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self {
            prev_block_to_chunk_hash_ready: LruCache::new(CHUNK_HEADERS_FOR_INCLUSION_CACHE_SIZE),
            chunk_hash_to_chunk_info: HashMap::new(),
            banned_chunk_producers: LruCache::new(NUM_EPOCH_CHUNK_PRODUCERS_TO_KEEP_IN_BLOCKLIST),
            epoch_manager,
        }
    }

    /// Call this function once we've collected all encoded chunk body and we are ready to include the chunk in block.
    pub fn mark_chunk_header_ready_for_inclusion(
        &mut self,
        chunk_header: ShardChunkHeader,
        chunk_producer: AccountId,
    ) {
        let prev_block_hash = chunk_header.prev_block_hash();
        if let Some(entry) = self.prev_block_to_chunk_hash_ready.get_mut(prev_block_hash) {
            // If prev_block_hash entry exists, add the new chunk to the entry.
            entry.insert(chunk_header.shard_id(), chunk_header.chunk_hash());
        } else {
            let new_entry = HashMap::from([(chunk_header.shard_id(), chunk_header.chunk_hash())]);
            // Call to prev_block_to_chunk_hash_ready.push might evict an entry from LRU cache.
            // In case of an eviction, cleanup entries in chunk_hash_to_chunk_info
            let maybe_evicted_entry =
                self.prev_block_to_chunk_hash_ready.push(prev_block_hash.clone(), new_entry);
            if let Some((_, evicted_entry)) = maybe_evicted_entry {
                self.process_evicted_entry(evicted_entry);
            }
        }
        // Insert chunk info in chunk_hash_to_chunk_info. This would be cleaned up later during eviction
        let chunk_hash = chunk_header.chunk_hash();
        let chunk_info =
            ChunkInfo { chunk_header, received_time: Utc::now(), chunk_producer, signatures: None };
        self.chunk_hash_to_chunk_info.insert(chunk_hash, chunk_info);
    }

    // once a set of ChunkHash is evicted from prev_block_to_chunk_hash_ready, cleanup chunk_hash_to_chunk_info
    fn process_evicted_entry(&mut self, evicted_entry: HashMap<ShardId, ChunkHash>) {
        for (_, chunk_hash) in evicted_entry.into_iter() {
            self.chunk_hash_to_chunk_info.remove(&chunk_hash);
        }
    }

    /// Add account_id to the list of banned chunk producers for the given epoch.
    /// This would typically happen for cases when a validator has produced an invalid chunk.
    pub fn ban_chunk_producer(&mut self, epoch_id: EpochId, account_id: AccountId) {
        self.banned_chunk_producers.put((epoch_id, account_id), ());
    }

    /// Function to return the chunks that are ready to be included in a block.
    /// We filter out the chunks that are produced by banned chunk producers or have insufficient
    /// chunk validator endorsements.
    /// Return HashMap from [shard_id] -> chunk_hash
    /// We can later use the chunk_hash to fetch chunk_header, chunk_endorsements, chunk_producer_and_received_time
    pub fn get_chunk_headers_ready_for_inclusion(
        &mut self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
        chunk_validator: &mut ChunkValidator,
    ) -> Result<HashMap<ShardId, ChunkHash>, Error> {
        let mut chunk_headers_ready_for_inclusion = HashMap::new();
        if let Some(entry) = self.prev_block_to_chunk_hash_ready.get_mut(prev_block_hash) {
            let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
            for (shard_id, chunk_hash) in entry.iter_mut() {
                let chunk_info = self.chunk_hash_to_chunk_info.get_mut(chunk_hash).unwrap();
                if filter_banned_chunk_producers(
                    &self.banned_chunk_producers,
                    epoch_id,
                    &chunk_info,
                ) && filter_insufficient_chunk_endorsements(
                    chunk_info,
                    chunk_validator,
                    protocol_version,
                )? {
                    // only add to chunk_headers_ready_for_inclusion if chunk passes the banned_chunk_producers and
                    // insufficient_chunk_endorsements checks
                    chunk_headers_ready_for_inclusion.insert(*shard_id, chunk_hash.clone());
                }
            }
        }
        Ok(chunk_headers_ready_for_inclusion)
    }

    pub fn num_chunk_headers_ready_for_inclusion(
        &mut self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
        chunk_validator: &mut ChunkValidator,
    ) -> usize {
        self.get_chunk_headers_ready_for_inclusion(epoch_id, prev_block_hash, chunk_validator)
            .map_or(0, |chunks| chunks.len())
    }

    pub fn get_banned_chunk_producers(&self) -> Vec<(EpochId, Vec<AccountId>)> {
        let mut banned_chunk_producers: HashMap<EpochId, Vec<_>> = HashMap::new();
        for ((epoch_id, account_id), _) in self.banned_chunk_producers.iter() {
            banned_chunk_producers.entry(epoch_id.clone()).or_default().push(account_id.clone());
        }
        banned_chunk_producers.into_iter().collect_vec()
    }

    fn get_chunk_info(&self, chunk_hash: &ChunkHash) -> Result<&ChunkInfo, Error> {
        // It should never happen that we are missing the key in chunk_hash_to_chunk_info
        self.chunk_hash_to_chunk_info
            .get(chunk_hash)
            .ok_or(Error::Other(format!("missing key {:?} in ChunkInclusionTracker", chunk_hash)))
    }

    pub fn chunk_header(&self, chunk_hash: &ChunkHash) -> Result<&ShardChunkHeader, Error> {
        Ok(&self.get_chunk_info(chunk_hash)?.chunk_header)
    }

    pub fn chunk_endorsements(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<ChunkEndorsementSignatures, Error> {
        Ok(self.get_chunk_info(chunk_hash)?.signatures.clone().unwrap_or_default())
    }

    pub fn chunk_producer_and_received_time(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<(AccountId, DateTime<Utc>), Error> {
        let chunk_info = self.get_chunk_info(chunk_hash)?;
        Ok((chunk_info.chunk_producer.clone(), chunk_info.received_time))
    }
}

fn filter_banned_chunk_producers(
    banned_chunk_producers: &LruCache<(EpochId, AccountId), ()>,
    epoch_id: &EpochId,
    chunk_info: &ChunkInfo,
) -> bool {
    let banned =
        banned_chunk_producers.contains(&(epoch_id.clone(), chunk_info.chunk_producer.clone()));
    if banned {
        tracing::warn!(
            target: "client",
            chunk_hash = ?chunk_info.chunk_header.chunk_hash(),
            chunk_producer = ?chunk_info.chunk_producer,
            "Not including chunk from a banned validator");
        metrics::CHUNK_DROPPED_BECAUSE_OF_BANNED_CHUNK_PRODUCER.inc();
    }
    !banned
}

fn filter_insufficient_chunk_endorsements(
    chunk_info: &mut ChunkInfo,
    chunk_validator: &mut ChunkValidator,
    protocol_version: ProtocolVersion,
) -> Result<bool, Error> {
    if !checked_feature!("stable", ChunkValidation, protocol_version) {
        return Ok(true);
    }

    // We cache the signatures in chunk_info.signatures. If it's None, we need to fetch it from chunk_validator.
    if chunk_info.signatures.is_some() {
        return Ok(true);
    }

    // Update chunk_info.signatures with the new signatures and return.
    Ok(chunk_validator
        .get_chunk_endorsement_signature(&chunk_info.chunk_header)?
        .map(|signatures| chunk_info.signatures = Some(signatures))
        .is_some())
}
