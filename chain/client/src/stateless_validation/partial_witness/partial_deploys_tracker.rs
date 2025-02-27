use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::metrics;
use lru::LruCache;
use near_async::time::Instant;
use near_chain::Error;
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonPartsTracker,
};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractDeploys, PartialEncodedContractDeploys, PartialEncodedContractDeploysPart,
};
use time::ext::InstantExt as _;

const DEPLOY_PARTS_CACHE_SIZE: usize = 20;
const PROCESSED_DEPLOYS_CACHE_SIZE: usize = 50;

struct CacheEntry {
    parts: ReedSolomonPartsTracker<ChunkContractDeploys>,
    created_at: Instant,
}

impl CacheEntry {
    fn new(encoder: Arc<ReedSolomonEncoder>, encoded_length: usize) -> Self {
        Self {
            parts: ReedSolomonPartsTracker::new(encoder, encoded_length),
            created_at: Instant::now(),
        }
    }

    fn process_part(
        &mut self,
        key: &ChunkProductionKey,
        part: PartialEncodedContractDeploysPart,
    ) -> Option<std::io::Result<ChunkContractDeploys>> {
        let part_ord = part.part_ord;
        if self.parts.encoded_length() != part.encoded_length {
            tracing::warn!(
                target: "client",
                expected = self.parts.encoded_length(),
                actual = part.encoded_length,
                part_ord,
                "Partial encoded contract deploys encoded_length field doesn't match",
            );
            return None;
        }
        match self.parts.insert_part(part_ord, part.data) {
            InsertPartResult::Accepted => None,
            InsertPartResult::PartAlreadyAvailable => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "Received duplicate or redundant contract deploy part"
                );
                None
            }
            InsertPartResult::InvalidPartOrd => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "Received invalid contract deploys part ord"
                );
                None
            }
            InsertPartResult::Decoded(decode_result) => Some(decode_result),
        }
    }
}

pub struct PartialEncodedContractDeploysTracker {
    parts_cache: LruCache<ChunkProductionKey, CacheEntry>,
    processed_deploys: LruCache<ChunkProductionKey, ()>,
}

impl PartialEncodedContractDeploysTracker {
    pub fn new() -> Self {
        Self {
            parts_cache: LruCache::new(NonZeroUsize::new(DEPLOY_PARTS_CACHE_SIZE).unwrap()),
            processed_deploys: LruCache::new(
                NonZeroUsize::new(PROCESSED_DEPLOYS_CACHE_SIZE).unwrap(),
            ),
        }
    }

    pub fn already_processed(&self, partial_deploys: &PartialEncodedContractDeploys) -> bool {
        let key = partial_deploys.chunk_production_key();
        if self.processed_deploys.contains(key) {
            return true;
        }
        if self
            .parts_cache
            .peek(key)
            .is_some_and(|entry| entry.parts.has_part(partial_deploys.part().part_ord))
        {
            tracing::warn!(
                target: "client",
                ?key,
                part = ?partial_deploys.part(),
                "Received already processed partial deploys part"
            );
            return true;
        }
        false
    }

    pub fn store_partial_encoded_contract_deploys(
        &mut self,
        partial_deploys: PartialEncodedContractDeploys,
        encoder: Arc<ReedSolomonEncoder>,
    ) -> Result<Option<ChunkContractDeploys>, Error> {
        let (key, part) = partial_deploys.into();
        if !self.parts_cache.contains(&key) {
            let new_entry = CacheEntry::new(encoder, part.encoded_length);
            if let Some((evicted_key, evicted_entry)) =
                self.parts_cache.push(key.clone(), new_entry)
            {
                tracing::warn!(
                    target: "client",
                    ?evicted_key,
                    data_parts_present = ?evicted_entry.parts.data_parts_present(),
                    data_parts_required = ?evicted_entry.parts.data_parts_required(),
                    "Evicted unprocessed contract deploys"
                );
            }
        }
        let entry = self.parts_cache.get_mut(&key).unwrap();
        if let Some(decode_result) = entry.process_part(&key, part) {
            let time_to_last_part = Instant::now().signed_duration_since(entry.created_at);
            metrics::PARTIAL_CONTRACT_DEPLOYS_TIME_TO_LAST_PART
                .with_label_values(&[key.shard_id.to_string().as_str()])
                .observe(time_to_last_part.as_seconds_f64());
            self.parts_cache.pop(&key);
            self.processed_deploys.push(key.clone(), ());
            let deploys = match decode_result {
                Ok(deploys) => deploys,
                Err(err) => {
                    tracing::warn!(
                        target: "client",
                        ?err,
                        ?key,
                        "Failed to reed solomon decode deployed contracts"
                    );
                    return Ok(None);
                }
            };
            return Ok(Some(deploys));
        }
        Ok(None)
    }
}
