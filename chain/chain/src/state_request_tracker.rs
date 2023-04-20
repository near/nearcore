use crate::metrics;
use crate::Chain;
use lru::LruCache;
use near_primitives::{
    hash::CryptoHash,
    types::ShardId,
    views::{PartElapsedTimeView, RequestedStatePartsView},
};
use std::collections::HashMap;

const REQUESTED_STATE_PARTS_CACHE_SIZE: usize = 4;

//Track the state parts that are already requested for debugging
//This struct provides methods to save and retrieve the information
//along with time elapsed to create state part.
//See https://github.com/near/nearcore/issues/7991 for more details
#[derive(Debug)]
pub(crate) struct StateRequestTracker {
    requested_state_parts: LruCache<CryptoHash, HashMap<ShardId, Vec<PartElapsedTimeView>>>,
}

impl StateRequestTracker {
    pub(crate) fn new() -> Self {
        StateRequestTracker {
            requested_state_parts: LruCache::new(REQUESTED_STATE_PARTS_CACHE_SIZE),
        }
    }

    pub(crate) fn save_state_part_elapsed(
        &mut self,
        crypto_hash: &CryptoHash,
        shard_id: &ShardId,
        part_id: &u64,
        elapsed_ms: u128,
    ) {
        self.requested_state_parts.get_or_insert(*crypto_hash, || HashMap::new());
        let parts_per_shard = self.requested_state_parts.get_mut(crypto_hash).unwrap();
        let elapsed = parts_per_shard.entry(*shard_id).or_insert_with(|| vec![]);
        elapsed.push(PartElapsedTimeView::new(part_id, elapsed_ms));
        metrics::STATE_PART_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .observe(elapsed_ms as f64 / 1000.);
    }
}

impl Chain {
    pub fn get_requested_state_parts(&self) -> Vec<RequestedStatePartsView> {
        let result: Vec<RequestedStatePartsView> = self
            .requested_state_parts
            .requested_state_parts
            .iter()
            .map(|(crypto_hash, parts_per_shard)| RequestedStatePartsView {
                block_hash: *crypto_hash,
                shard_requested_parts: parts_per_shard.clone(),
            })
            .collect();
        return result;
    }
}
