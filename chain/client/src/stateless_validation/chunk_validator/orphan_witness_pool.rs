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
}

struct CacheEntry {
    witness: ChunkStateWitness,
    _metrics_tracker: OrphanWitnessMetricsTracker,
}

impl OrphanStateWitnessPool {
    /// Create a new `OrphanStateWitnessPool` with a capacity of `cache_capacity` witnesses.
    /// The `Default` trait implementation provides reasonable defaults.
    pub fn new(cache_capacity: usize) -> Self {
        if cache_capacity > 128 {
            tracing::warn!(
                target: "client",
                "OrphanStateWitnessPool capacity is set to {}, which is larger than expected. \
                OrphanStateWitnessPool uses a naive algorithm, using a large capacity might lead \
                to performance problems.", cache_capacity);
        }

        OrphanStateWitnessPool { witness_cache: LruCache::new(cache_capacity) }
    }

    /// Add an orphaned chunk state witness to the pool. The witness will be put in a cache and it'll
    /// wait there for the block that's required to process it.
    /// It's expected that this `ChunkStateWitness` has gone through basic validation - including signature,
    /// shard_id, size and distance from the tip. The pool would still work without it, but without validation
    /// it'd be possible to fill the whole cache with spam.
    /// `witness_size` is only used for metrics, it's okay to pass 0 if you don't care about the metrics.
    pub fn add_orphan_state_witness(&mut self, witness: ChunkStateWitness, witness_size: usize) {
        // Insert the new ChunkStateWitness into the cache
        let chunk_header = &witness.chunk_header;
        let cache_key = (chunk_header.shard_id(), chunk_header.height_created());
        let metrics_tracker = OrphanWitnessMetricsTracker::new(&witness, witness_size);
        let cache_entry = CacheEntry { witness, _metrics_tracker: metrics_tracker };
        if let Some((_, ejected_entry)) = self.witness_cache.push(cache_key, cache_entry) {
            // Another witness has been ejected from the cache due to capacity limit
            let header = &ejected_entry.witness.chunk_header;
            tracing::debug!(
                target: "client",
                ejected_witness_height = header.height_created(),
                ejected_witness_shard = header.shard_id(),
                ejected_witness_chunk = ?header.chunk_hash(),
                ejected_witness_prev_block = ?header.prev_block_hash(),
                "Ejecting an orphaned ChunkStateWitness from the cache due to capacity limit. It will not be processed."
            );
        }
    }

    /// Find all orphaned witnesses that were waiting for this block and remove them from the pool.
    /// The block has arrived, so they can be now processed, they're no longer orphans.
    pub fn take_state_witnesses_waiting_for_block(
        &mut self,
        prev_block: &CryptoHash,
    ) -> Vec<ChunkStateWitness> {
        let mut to_remove: Vec<(ShardId, BlockHeight)> = Vec::new();
        for (cache_key, cache_entry) in self.witness_cache.iter() {
            if cache_entry.witness.chunk_header.prev_block_hash() == prev_block {
                to_remove.push(*cache_key);
            }
        }
        let mut result = Vec::new();
        for cache_key in to_remove {
            let ready_witness = self
                .witness_cache
                .pop(&cache_key)
                .expect("The cache contains this entry, a moment ago it was iterated over");
            result.push(ready_witness.witness);
        }
        result
    }

    /// Remove all witnesses below the given height from the pool.
    /// Orphan witnesses below the final height of the chain won't be needed anymore,
    /// so they can be removed from the pool to free up memory.
    pub fn remove_witnesses_below_final_height(&mut self, final_height: BlockHeight) {
        let mut to_remove: Vec<(ShardId, BlockHeight)> = Vec::new();
        for ((witness_shard, witness_height), cache_entry) in self.witness_cache.iter() {
            if *witness_height <= final_height {
                to_remove.push((*witness_shard, *witness_height));
                let header = &cache_entry.witness.chunk_header;
                tracing::debug!(
                    target: "client",
                    final_height,
                    ejected_witness_height = *witness_height,
                    ejected_witness_shard = *witness_shard,
                    ejected_witness_chunk = ?header.chunk_hash(),
                    ejected_witness_prev_block = ?header.prev_block_hash(),
                    "Ejecting an orphaned ChunkStateWitness from the cache because it's below \
                    the final height of the chain. It will not be processed.");
            }
        }
        for cache_key in to_remove {
            let popped = self.witness_cache.pop(&cache_key);
            debug_assert!(popped.is_some());
        }
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
            let shard_id = witness.chunk_header.shard_id().to_string();
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
        match &mut witness.chunk_header {
            ShardChunkHeader::V3(header) => match &mut header.inner {
                ShardChunkHeaderInner::V2(inner) => inner.encoded_length = encoded_length,
                ShardChunkHeaderInner::V3(inner) => inner.encoded_length = encoded_length,
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
        witness
    }

    /// Generate fake block hash based on height
    fn block(height: BlockHeight) -> CryptoHash {
        hash(&height.to_be_bytes())
    }

    /// Assert that both Vecs are equal after sorting. It's order-independent, unlike the standard assert_eq!
    fn assert_contents(mut observed: Vec<ChunkStateWitness>, mut expected: Vec<ChunkStateWitness>) {
        let sort_comparator = |witness1: &ChunkStateWitness, witness2: &ChunkStateWitness| {
            let bytes1 = borsh::to_vec(witness1).unwrap();
            let bytes2 = borsh::to_vec(witness2).unwrap();
            bytes1.cmp(&bytes2)
        };
        observed.sort_by(sort_comparator);
        expected.sort_by(sort_comparator);
        if observed != expected {
            let print_witness_info = |witness: &ChunkStateWitness| {
                let header = &witness.chunk_header;
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
            eprintln!("Observed {} witnesses:", observed.len());
            for witness in observed {
                print_witness_info(&witness);
            }
            eprintln!("==================");
            panic!("assert_contents failed");
        }
    }

    // Check that the pool is empty, all witnesses have been removed
    fn assert_empty(pool: &OrphanStateWitnessPool) {
        assert_eq!(pool.witness_cache.len(), 0);
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
        assert_contents(waiting_for_99, vec![witness1, witness2]);

        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_contents(waiting_for_100, vec![witness3, witness4]);

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
            assert_contents(waiting_for_99, vec![witness2]);
        }

        // The old witness is replaced when the awaited block is different, waiting_for_block is cleaned as expected
        {
            let witness3 = make_witness(102, 1, block(100), 0);
            let witness4 = make_witness(102, 1, block(101), 0);
            pool.add_orphan_state_witness(witness3, 0);
            pool.add_orphan_state_witness(witness4.clone(), 0);

            let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
            assert_contents(waiting_for_101, vec![witness4]);

            let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
            assert_contents(waiting_for_100, vec![]);
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
        assert_contents(waiting_for_100, vec![witness2, witness3]);

        // witness1 should be ejected, no one is waiting for block 101
        let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
        assert_contents(waiting_for_101, vec![]);

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
        assert_contents(waiting_for_99, vec![witness]);

        assert_empty(&pool);
    }

    /// Test that remove_witnesses_below_final_height() works correctly
    #[test]
    fn remove_below_height() {
        let mut pool = OrphanStateWitnessPool::new(10);

        let witness1 = make_witness(100, 1, block(99), 0);
        let witness2 = make_witness(101, 1, block(100), 0);
        let witness3 = make_witness(102, 1, block(101), 0);
        let witness4 = make_witness(103, 1, block(102), 0);

        pool.add_orphan_state_witness(witness1, 0);
        pool.add_orphan_state_witness(witness2.clone(), 0);
        pool.add_orphan_state_witness(witness3, 0);
        pool.add_orphan_state_witness(witness4.clone(), 0);

        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_contents(waiting_for_100, vec![witness2]);

        pool.remove_witnesses_below_final_height(102);

        let waiting_for_99 = pool.take_state_witnesses_waiting_for_block(&block(99));
        assert_contents(waiting_for_99, vec![]);

        let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
        assert_contents(waiting_for_101, vec![]);

        let waiting_for_102 = pool.take_state_witnesses_waiting_for_block(&block(102));
        assert_contents(waiting_for_102, vec![witness4]);
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
        assert_contents(waiting, vec![]);

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
        assert_contents(looking_for_99, vec![witness5]);

        // Let's add a few more witnesses
        let witness10 = make_witness(102, 1, block(101), 0);
        let witness11 = make_witness(102, 4, block(100), 0);
        let witness12 = make_witness(102, 1, block(77), 0);
        pool.add_orphan_state_witness(witness10, 0);
        pool.add_orphan_state_witness(witness11.clone(), 0);
        pool.add_orphan_state_witness(witness12.clone(), 0);

        // Check that witnesses waiting for block 100 are correct
        let waiting_for_100 = pool.take_state_witnesses_waiting_for_block(&block(100));
        assert_contents(waiting_for_100, vec![witness7, witness8, witness9, witness11]);

        // At this point the pool contains only witness12, no one should be waiting for block 101.
        let waiting_for_101 = pool.take_state_witnesses_waiting_for_block(&block(101));
        assert_contents(waiting_for_101, vec![]);

        let waiting_for_77 = pool.take_state_witnesses_waiting_for_block(&block(77));
        assert_contents(waiting_for_77, vec![witness12]);

        assert_empty(&pool);
    }
}
