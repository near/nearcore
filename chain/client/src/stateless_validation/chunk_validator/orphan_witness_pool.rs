use std::collections::{HashMap, HashSet};

use lru::LruCache;
use near_chain_configs::default_orphan_state_witness_pool_size;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::{BlockHeight, ShardId};

use metrics_tracker::OrphanWitnessMetricsTracker;

/// `OrphanStateWitnessPool` is used to keep orphaned ChunkStateWitnesses until it's possible to process them.
/// To process a ChunkStateWitness we need to have the previous block, but it might happen that a ChunkStateWitness
/// shows up before the block is available. In such cases the witness is put in `OrphanStateWitnessPool` until the
/// required block arrives and the witness can be processed.
pub struct OrphanStateWitnessPool {
    witness_cache: LruCache<(ShardId, BlockHeight), CacheEntry>,
    /// List of orphaned witnesses that wait for this block to appear.
    /// Maps block hash to entries in `witness_cache`.
    /// Must be kept in sync with `witness_cache`.
    waiting_for_block: HashMap<CryptoHash, HashSet<(ShardId, BlockHeight)>>,
}

struct CacheEntry {
    witness: ChunkStateWitness,
    // cargo complains that metrics_tracker is never read, but it's ok,
    // as metrics_tracker does all of its work during initalization and destruction.
    #[allow(dead_code)]
    metrics_tracker: OrphanWitnessMetricsTracker,
}

impl OrphanStateWitnessPool {
    /// Create a new `OrphanStateWitnessPool` with a capacity of `cache_capacity` witnesses.
    /// The `Default` trait implementation provides reasonable defaults.
    pub fn new(cache_capacity: usize) -> Self {
        OrphanStateWitnessPool {
            witness_cache: LruCache::new(cache_capacity),
            waiting_for_block: HashMap::new(),
        }
    }

    /// Add an orphaned chunk state witness to the pool. The witness will be put in a cache and it'll
    /// wait there for the block that's required to process it.
    /// It's expected that this `ChunkStateWitness` has gone through basic validation - including signature,
    /// shard_id, size and distance from the tip. The pool would still work without it, but without validation
    /// it'd be possible to fill the whole cache with spam.
    /// `witness_size` is only used for metrics, it's okay to pass 0 if you don't care about the metrics.
    pub fn add_orphan_state_witness(&mut self, witness: ChunkStateWitness, witness_size: usize) {
        if self.witness_cache.cap() == 0 {
            // A cache with 0 capacity doesn't keep anything.
            return;
        }

        // Insert the new ChunkStateWitness into the cache
        let chunk_header = &witness.inner.chunk_header;
        let prev_block_hash = *chunk_header.prev_block_hash();
        let cache_key = (chunk_header.shard_id(), chunk_header.height_created());
        let metrics_tracker = OrphanWitnessMetricsTracker::new(&witness, witness_size);
        let cache_entry = CacheEntry { witness, metrics_tracker };
        if let Some((_, ejected_entry)) = self.witness_cache.push(cache_key, cache_entry) {
            // If another witness has been ejected from the cache due to capacity limit,
            // then remove the ejected witness from `waiting_for_block` to keep them in sync
            let header = &ejected_entry.witness.inner.chunk_header;
            tracing::debug!(
                target: "client",
                witness_height = header.height_created(),
                witness_shard = header.shard_id(),
                witness_chunk = ?header.chunk_hash(),
                witness_prev_block = ?header.prev_block_hash(),
                "Ejecting an orphaned ChunkStateWitness from the cache due to capacity limit. It will not be processed."
            );
            self.remove_from_waiting_for_block(&ejected_entry.witness);
        }

        // Add the new orphaned state witness to `waiting_for_block`
        self.waiting_for_block
            .entry(prev_block_hash)
            .or_insert_with(|| HashSet::new())
            .insert(cache_key);
    }

    fn remove_from_waiting_for_block(&mut self, witness: &ChunkStateWitness) {
        let chunk_header = &witness.inner.chunk_header;
        let waiting_set = self
            .waiting_for_block
            .get_mut(chunk_header.prev_block_hash())
            .expect("Every ejected witness must have a corresponding entry in waiting_for_block.");
        waiting_set.remove(&(chunk_header.shard_id(), chunk_header.height_created()));
        if waiting_set.is_empty() {
            self.waiting_for_block.remove(chunk_header.prev_block_hash());
        }
    }

    /// Find all orphaned witnesses that were waiting for this block and remove them from the pool.
    /// The block has arrived, so they can be now processed, they're no longer orphans.
    pub fn take_state_witnesses_waiting_for_block(
        &mut self,
        prev_block: &CryptoHash,
    ) -> Vec<ChunkStateWitness> {
        let Some(waiting) = self.waiting_for_block.remove(prev_block) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for (shard_id, height) in waiting {
            // Remove this witness from `witness_cache` to keep them in sync
            let entry = self.witness_cache.pop(&(shard_id, height)).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache",
            );

            result.push(entry.witness);
        }
        result
    }
}

impl Default for OrphanStateWitnessPool {
    fn default() -> OrphanStateWitnessPool {
        OrphanStateWitnessPool::new(default_orphan_state_witness_pool_size())
    }
}

mod metrics_tracker {
    use near_primitives::stateless_validation::ChunkStateWitness;

    use crate::metrics;

    /// OrphanWitnessMetricsTracker is a helper struct which leverages RAII to update
    /// the metrics about witnesses in the orphan pool when they're added and removed.
    /// Its constructor adds the witness to the metrics, and later its destructor
    /// removes the witness from metrics.
    /// Using this struct is much less error-prone than adjusting the metrics by hand.
    pub struct OrphanWitnessMetricsTracker {
        shard_id: String,
        witness_size: usize,
    }

    impl OrphanWitnessMetricsTracker {
        pub fn new(
            witness: &ChunkStateWitness,
            witness_size: usize,
        ) -> OrphanWitnessMetricsTracker {
            let shard_id = witness.inner.chunk_header.shard_id().to_string();
            metrics::ORPHAN_CHUNK_STATE_WITNESSES_TOTAL_COUNT
                .with_label_values(&[shard_id.as_str()])
                .inc();
            metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_SIZE
                .with_label_values(&[shard_id.as_str()])
                .inc();
            metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_MEMORY_USED
                .with_label_values(&[shard_id.as_str()])
                .add(witness_size_to_i64(witness_size));

            OrphanWitnessMetricsTracker { shard_id, witness_size }
        }
    }

    impl Drop for OrphanWitnessMetricsTracker {
        fn drop(&mut self) {
            metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_SIZE
                .with_label_values(&[self.shard_id.as_str()])
                .dec();
            metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_MEMORY_USED
                .with_label_values(&[self.shard_id.as_str()])
                .sub(witness_size_to_i64(self.witness_size));
        }
    }

    fn witness_size_to_i64(witness_size: usize) -> i64 {
        witness_size.try_into().expect(
            "Orphaned ChunkStateWitness size can't be converted to i64. \
    This should be impossible, is it over one exabyte in size?",
        )
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderInner};
    use near_primitives::stateless_validation::ChunkStateWitness;
    use near_primitives::types::{BlockHeight, ShardId};

    use super::OrphanStateWitnessPool;

    /// Make a dummy witness for testing
    /// encoded_length is used to differentiate between witnesses with the same main parameters.
    fn make_witness(
        height: BlockHeight,
        shard_id: ShardId,
        prev_block_hash: CryptoHash,
        encoded_length: u64,
    ) -> ChunkStateWitness {
        let mut witness = ChunkStateWitness::new_dummy(height, shard_id, prev_block_hash);
        match &mut witness.inner.chunk_header {
            ShardChunkHeader::V3(header) => match &mut header.inner {
                ShardChunkHeaderInner::V2(inner) => inner.encoded_length = encoded_length,
                _ => panic!(),
            },
            _ => panic!(),
        }
        witness
    }

    /// Generate fake block hash based on height
    fn block(height: BlockHeight) -> CryptoHash {
        hash(&height.to_be_bytes())
    }

    /// Assert that both Vecs are equal after sorting. It's order-independent, unlike the standard assert_eq!
    fn assert_same(mut observed: Vec<ChunkStateWitness>, mut expected: Vec<ChunkStateWitness>) {
        let sort_comparator = |witness1: &ChunkStateWitness, witness2: &ChunkStateWitness| {
            let bytes1 = borsh::to_vec(witness1).unwrap();
            let bytes2 = borsh::to_vec(witness2).unwrap();
            bytes1.cmp(&bytes2)
        };
        observed.sort_by(sort_comparator);
        expected.sort_by(sort_comparator);
        if observed != expected {
            let print_witness_info = |witness: &ChunkStateWitness| {
                let header = &witness.inner.chunk_header;
                eprintln!(
                    "- height = {}, shard_id = {}, encoded_length: {} prev_block: {}",
                    header.height_created(),
                    header.shard_id(),
                    header.encoded_length(),
                    header.prev_block_hash()
                );
            };
            eprintln!("Mismatch!");
            eprintln!("Expected {} witnesses:", expected.len());
            for witness in expected {
                print_witness_info(&witness);
            }
            eprintln!("Observed {} witnesse:", observed.len());
            for witness in observed {
                print_witness_info(&witness);
            }
            eprintln!("==================");
            panic!("assert_same failed");
        }
    }

    // Check that the pool is empty, all witnesses have been removed from both fields
    fn assert_empty(pool: &OrphanStateWitnessPool) {
        assert!(pool.witness_cache.is_empty());
        assert!(pool.waiting_for_block.is_empty());
    }

    /// Basic functionality - inserting witnesses and fetching them works as expected
    #[test]
    fn basic() {
        let mut pool = OrphanStateWitnessPool::new(10);

        let witness1 = make_witness(100, 1, block(99), 0);
        let witness2 = make_witness(100, 2, block(99), 0);
        let witness3 = make_witness(101, 1, block(100), 0);
        let witness4 = make_witness(101, 2, block(100), 0);

        pool.add_orphan_state_witness(witness1.clone(), 0);
        pool.add_orphan_state_witness(witness2.clone(), 0);
        pool.add_orphan_state_witness(witness3.clone(), 0);
        pool.add_orphan_state_witness(witness4.clone(), 0);

        let waiting_for_99 = pool.take_state_witnesses_waiting_for_block(&block(99));
        assert_same(waiting_for_99, vec![witness1, witness2]);

        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_same(waiting_for_100, vec![witness3, witness4]);

        assert_empty(&pool);
    }

    /// When a new witness is inserted with the same (shard_id, height) as an existing witness, the new witness
    /// should replace the old one. The old one should be ejected from the pool.
    #[test]
    fn replacing() {
        let mut pool = OrphanStateWitnessPool::new(10);

        // The old witness is replaced when the awaited block is the same
        {
            let witness1 = make_witness(100, 1, block(99), 0);
            let witness2 = make_witness(100, 1, block(99), 1);
            pool.add_orphan_state_witness(witness1, 0);
            pool.add_orphan_state_witness(witness2.clone(), 0);

            let waiting_for_99 = pool.take_state_witnesses_waiting_for_block(&block(99));
            assert_same(waiting_for_99, vec![witness2]);
        }

        // The old witness is replaced when the awaited block is different, waiting_for_block is cleaned as expected
        {
            let witness3 = make_witness(102, 1, block(100), 0);
            let witness4 = make_witness(102, 1, block(101), 0);
            pool.add_orphan_state_witness(witness3, 0);
            pool.add_orphan_state_witness(witness4.clone(), 0);

            let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
            assert_same(waiting_for_101, vec![witness4]);

            let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
            assert_same(waiting_for_100, vec![]);
        }

        assert_empty(&pool);
    }

    /// The pool has limited capacity. Once it hits the capacity, the least-recently used witness will be ejected.
    #[test]
    fn limited_capacity() {
        let mut pool = OrphanStateWitnessPool::new(2);

        let witness1 = make_witness(102, 1, block(101), 0);
        let witness2 = make_witness(101, 1, block(100), 0);
        let witness3 = make_witness(101, 2, block(100), 0);

        pool.add_orphan_state_witness(witness1, 0);
        pool.add_orphan_state_witness(witness2.clone(), 0);

        // Inserting the third witness causes the pool to go over capacity, so witness1 should be ejected.
        pool.add_orphan_state_witness(witness3.clone(), 0);

        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_same(waiting_for_100, vec![witness2, witness3]);

        // witness1 should be ejected, no one is waiting for block 101
        let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
        assert_same(waiting_for_101, vec![]);

        assert_empty(&pool);
    }

    /// When a witness is ejected from the cache, it should also be removed from the corresponding waiting_for_block set.
    /// But it's not enough to remove it from the set, if the set has become empty we must also remove the set itself
    /// from `waiting_for_block`. Otherwise `waiting_for_block` would have a constantly increasing amount of empty sets,
    /// which would cause a memory leak.
    #[test]
    fn empty_waiting_sets_cleared_on_ejection() {
        let mut pool = OrphanStateWitnessPool::new(1);

        let witness1 = make_witness(100, 1, block(99), 0);
        pool.add_orphan_state_witness(witness1, 0);

        // When witness2 is added to the pool witness1 gets ejected from the cache (capacity is set to 1).
        // When this happens the set of witness waiting for block 99 will become empty. Then this empty set should
        // be removed from `waiting_for_block`, otherwise there'd be a memory leak.
        let witness2 = make_witness(100, 2, block(100), 0);
        pool.add_orphan_state_witness(witness2.clone(), 0);

        // waiting_for_block contains only one entry, list of witnesses waiting for block 100.
        assert_eq!(pool.waiting_for_block.len(), 1);

        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_same(waiting_for_100, vec![witness2]);

        assert_empty(&pool);
    }

    /// OrphanStateWitnessPool can handle large shard ids without any problems, it doesn't keep a Vec indexed by shard_id
    #[test]
    fn large_shard_id() {
        let mut pool = OrphanStateWitnessPool::new(10);

        let large_shard_id = ShardId::MAX;
        let witness = make_witness(101, large_shard_id, block(99), 0);
        pool.add_orphan_state_witness(witness.clone(), 0);

        let waiting_for_99 = pool.take_state_witnesses_waiting_for_block(&block(99));
        assert_same(waiting_for_99, vec![witness]);

        assert_empty(&pool);
    }

    /// An OrphanStateWitnessPool with 0 capacity shouldn't crash, it should just ignore all witnesses
    #[test]
    fn zero_capacity() {
        let mut pool = OrphanStateWitnessPool::new(0);

        pool.add_orphan_state_witness(make_witness(100, 1, block(99), 0), 0);
        pool.add_orphan_state_witness(make_witness(100, 1, block(99), 0), 1);
        pool.add_orphan_state_witness(make_witness(100, 2, block(99), 0), 0);
        pool.add_orphan_state_witness(make_witness(101, 0, block(100), 0), 0);

        let waiting = pool.take_state_witnesses_waiting_for_block(&block(99));
        assert_same(waiting, vec![]);

        assert_empty(&pool);
    }

    /// OrphanStateWitnessPool has a Drop implementation which clears the metrics.
    /// It's hard to test it because metrics are global and it could interfere with other tests,
    /// but we can at least test that it doesn't crash. That's always something.
    #[test]
    fn destructor_doesnt_crash() {
        let mut pool = OrphanStateWitnessPool::new(10);
        pool.add_orphan_state_witness(make_witness(100, 0, block(99), 0), 0);
        pool.add_orphan_state_witness(make_witness(100, 2, block(99), 0), 0);
        pool.add_orphan_state_witness(make_witness(100, 2, block(99), 0), 1);
        pool.add_orphan_state_witness(make_witness(101, 0, block(100), 0), 0);
        std::mem::drop(pool);
        println!("Ok!");
    }

    /// A longer test scenario
    #[test]
    fn scenario() {
        let mut pool = OrphanStateWitnessPool::new(5);

        // Witnesses for shards 0, 1, 2, 3 at height 1000, looking for block 99
        let witness0 = make_witness(100, 0, block(99), 0);
        let witness1 = make_witness(100, 1, block(99), 0);
        let witness2 = make_witness(100, 2, block(99), 0);
        let witness3 = make_witness(100, 3, block(99), 0);
        pool.add_orphan_state_witness(witness0, 0);
        pool.add_orphan_state_witness(witness1, 0);
        pool.add_orphan_state_witness(witness2, 0);
        pool.add_orphan_state_witness(witness3, 0);

        // Another witness on shard 1, height 100. Should replace witness1
        let witness5 = make_witness(100, 1, block(99), 1);
        pool.add_orphan_state_witness(witness5.clone(), 0);

        // Witnesses for shards 0, 1, 2, 3 at height 101, looking for block 100
        let witness6 = make_witness(101, 0, block(100), 0);
        let witness7 = make_witness(101, 1, block(100), 0);
        let witness8 = make_witness(101, 2, block(100), 0);
        let witness9 = make_witness(101, 3, block(100), 0);
        pool.add_orphan_state_witness(witness6, 0);
        pool.add_orphan_state_witness(witness7.clone(), 0);
        pool.add_orphan_state_witness(witness8.clone(), 0);
        pool.add_orphan_state_witness(witness9.clone(), 0);

        // Pool capacity is 5, so three witnesses at height 100 should be ejected.
        // The only surviving witness should be witness5, which was the freshest one among them
        let looking_for_99 = pool.take_state_witnesses_waiting_for_block(&block(99));
        assert_same(looking_for_99, vec![witness5]);

        // Let's add a few more witnesses
        let witness10 = make_witness(102, 1, block(101), 0);
        let witness11 = make_witness(102, 4, block(100), 0);
        let witness12 = make_witness(102, 1, block(77), 0);
        pool.add_orphan_state_witness(witness10, 0);
        pool.add_orphan_state_witness(witness11.clone(), 0);
        pool.add_orphan_state_witness(witness12.clone(), 0);

        // Check that witnesses waiting for block 100 are correct
        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_same(waiting_for_100, vec![witness7, witness8, witness9, witness11]);

        // At this point the pool contains only witness12, no one should be waiting for block 101.
        let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
        assert_same(waiting_for_101, vec![]);

        let waiting_for_77 = pool.take_state_witnesses_waiting_for_block(&block(77));
        assert_same(waiting_for_77, vec![witness12]);

        assert_empty(&pool);
    }
}
