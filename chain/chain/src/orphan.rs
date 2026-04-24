use crate::chain::ApplyChunksDoneSender;
use crate::missing_chunks::BlockLike;
use crate::{BlockProcessingArtifact, Chain, Provenance, metrics};
use lru::LruCache;
use near_async::time::{Duration, Instant};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::MaybeValidated;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::debug_span;

/// Maximum number of orphans chain can store.
const MAX_ORPHAN_SIZE: usize = 1024;

/// Maximum age of orphan to store in the chain.
const MAX_ORPHAN_AGE_SECS: u64 = 300;

/// Maximum number of optimistic blocks to store in the cache.
const MAX_OPTIMISTIC_ORPHANS: usize = 10;

// Number of orphan ancestors should be checked to request chunks
// Orphans for which we will request for missing chunks must satisfy,
// its NUM_ORPHAN_ANCESTORS_CHECK'th ancestor has been accepted
pub const NUM_ORPHAN_ANCESTORS_CHECK: u64 = 3;

// Maximum number of orphans that we can request missing chunks
// Note that if there are no forks, the maximum number of orphans we would
// request missing chunks will not exceed NUM_ORPHAN_ANCESTORS_CHECK,
// this number only adds another restriction when there are multiple forks.
// It should almost never be hit
const MAX_ORPHAN_MISSING_CHUNKS: usize = 5;

/// Orphan is a block whose previous block is not accepted (in store) yet.
/// Therefore, they are not ready to be processed yet.
/// We save these blocks in an in-memory orphan pool to be processed later
/// after their previous block is accepted.
pub struct Orphan {
    pub(crate) block: MaybeValidated<Arc<Block>>,
    pub(crate) provenance: Provenance,
    pub(crate) added: Instant,
}

impl BlockLike for Orphan {
    fn hash(&self) -> CryptoHash {
        *self.block.hash()
    }

    fn height(&self) -> u64 {
        self.block.header().height()
    }
}

impl Orphan {
    fn prev_hash(&self) -> &CryptoHash {
        self.block.header().prev_hash()
    }
}

/// OrphanBlockPool stores information of all orphans that are waiting to be processed
/// A block is added to the orphan pool when process_block failed because the block is an orphan
/// A block is removed from the pool if
/// 1) it is ready to be processed
/// or
/// 2) size of the pool exceeds MAX_ORPHAN_SIZE and the orphan was added a long time ago
///    or the height is high
pub struct OrphanBlockPool {
    /// A map from block hash to an orphan block
    orphans: HashMap<CryptoHash, Orphan>,
    /// LRU cache mapping previous blocks to the orphaned optimistic blocks
    /// based on them.
    optimistic_orphans: LruCache<CryptoHash, OptimisticBlock>,
    /// A set that contains all orphans for which we have requested missing chunks for them
    /// An orphan can be added to this set when it was first added to the pool, or later
    /// when certain requirements are satisfied (see check_orphans)
    /// It can only be removed from this set when the orphan is removed from the pool
    orphans_requested_missing_chunks: HashSet<CryptoHash>,
    /// A map from block heights to orphan blocks at the height
    /// It's used to evict orphans when the pool is saturated
    height_idx: HashMap<BlockHeight, Vec<CryptoHash>>,
    /// A map from block hashes to orphan blocks whose prev block is the block
    /// It's used to check which orphan blocks are ready to be processed when a block is accepted
    prev_hash_idx: HashMap<CryptoHash, Vec<CryptoHash>>,
    /// number of orphans that were evicted
    evicted: usize,
}

impl OrphanBlockPool {
    pub fn new() -> OrphanBlockPool {
        OrphanBlockPool {
            orphans: HashMap::default(),
            optimistic_orphans: LruCache::new(NonZeroUsize::new(MAX_OPTIMISTIC_ORPHANS).unwrap()),
            orphans_requested_missing_chunks: HashSet::default(),
            height_idx: HashMap::default(),
            prev_hash_idx: HashMap::default(),
            evicted: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.orphans.len()
    }

    fn len_evicted(&self) -> usize {
        self.evicted
    }

    /// Add a block to the orphan pool
    /// `requested_missing_chunks`: whether missing chunks has been requested for the orphan
    fn add(&mut self, orphan: Orphan, requested_missing_chunks: bool) {
        let block_hash = *orphan.block.hash();
        let height_hashes = self.height_idx.entry(orphan.block.header().height()).or_default();
        height_hashes.push(*orphan.block.hash());
        let prev_hash_entries =
            self.prev_hash_idx.entry(*orphan.block.header().prev_hash()).or_default();
        prev_hash_entries.push(block_hash);
        self.orphans.insert(block_hash, orphan);
        if requested_missing_chunks {
            self.orphans_requested_missing_chunks.insert(block_hash);
        }

        if self.orphans.len() > MAX_ORPHAN_SIZE {
            let old_len = self.orphans.len();

            let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
            self.orphans.retain(|_, ref mut x| {
                let keep = x.added.elapsed() < Duration::seconds(MAX_ORPHAN_AGE_SECS as i64);
                if !keep {
                    removed_hashes.insert(*x.block.hash());
                }
                keep
            });
            let mut heights = self.height_idx.keys().cloned().collect::<Vec<u64>>();
            heights.sort_unstable();
            for h in heights.iter().rev() {
                if let Some(hash) = self.height_idx.remove(h) {
                    for h in hash {
                        let _ = self.orphans.remove(&h);
                        removed_hashes.insert(h);
                    }
                }
                if self.orphans.len() < MAX_ORPHAN_SIZE {
                    break;
                }
            }
            self.prune_side_indexes(&removed_hashes);

            self.evicted += old_len - self.orphans.len();
        }
        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
    }

    /// Prune `removed` hashes from every side-index and drop empty buckets.
    /// Precondition: `self.orphans` no longer contains any of these hashes.
    fn prune_side_indexes(&mut self, removed: &HashSet<CryptoHash>) {
        self.height_idx.retain(|_, xs| {
            xs.retain(|x| !removed.contains(x));
            !xs.is_empty()
        });
        self.prev_hash_idx.retain(|_, xs| {
            xs.retain(|x| !removed.contains(x));
            !xs.is_empty()
        });
        self.orphans_requested_missing_chunks.retain(|x| !removed.contains(x));
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }

    pub fn get(&self, hash: &CryptoHash) -> Option<&Orphan> {
        self.orphans.get(hash)
    }

    pub fn add_optimistic(&mut self, block: OptimisticBlock) {
        self.optimistic_orphans.push(*block.prev_block_hash(), block);
        metrics::NUM_OPTIMISTIC_ORPHANS.set(self.optimistic_orphans.len() as i64);
    }

    // // Iterates over existing orphans.
    // pub fn map(&self, orphan_fn: &mut dyn FnMut(&CryptoHash, &Block, &Instant)) {
    //     self.orphans
    //         .iter()
    //         .map(|it| orphan_fn(it.0, it.1.block.get_inner(), &it.1.added))
    //         .collect_vec();
    // }

    /// Remove all orphans in the pool that can be "adopted" by block `prev_hash`, i.e., children
    /// of `prev_hash` and return the list.
    /// This function is called when `prev_hash` is accepted, thus its children can be removed
    /// from the orphan pool and be processed.
    pub fn remove_by_prev_hash(&mut self, prev_hash: CryptoHash) -> Option<Vec<Orphan>> {
        let mut removed_hashes: HashSet<CryptoHash> = HashSet::default();
        let ret = self.prev_hash_idx.remove(&prev_hash).map(|hs| {
            hs.iter()
                .filter_map(|h| {
                    removed_hashes.insert(*h);
                    self.orphans_requested_missing_chunks.remove(h);
                    self.orphans.remove(h)
                })
                .collect()
        });

        self.prune_side_indexes(&removed_hashes);

        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
        ret
    }

    pub fn remove_optimistic(&mut self, prev_hash: &CryptoHash) -> Option<OptimisticBlock> {
        let block = self.optimistic_orphans.pop(prev_hash);
        metrics::NUM_OPTIMISTIC_ORPHANS.set(self.optimistic_orphans.len() as i64);
        block
    }

    /// Return a list of orphans that are among the `target_depth` immediate descendants of
    /// the block `parent_hash`
    pub fn get_orphans_within_depth(
        &self,
        parent_hash: CryptoHash,
        target_depth: u64,
    ) -> Vec<CryptoHash> {
        let mut _visited = HashSet::new();

        let mut res = vec![];
        let mut queue = vec![(parent_hash, 0)];
        while let Some((prev_hash, depth)) = queue.pop() {
            if depth == target_depth {
                // Don't process the children of this block, their depth will be above target_depth.
                continue;
            }
            if let Some(block_hashes) = self.prev_hash_idx.get(&prev_hash) {
                for hash in block_hashes {
                    queue.push((*hash, depth + 1));
                    res.push(*hash);
                    // there should be no loop
                    debug_assert!(_visited.insert(*hash));
                }
            }
        }
        res
    }

    /// Returns true if the block has not been requested yet and the number of orphans
    /// for which we have requested missing chunks have not exceeded MAX_ORPHAN_MISSING_CHUNKS
    fn can_request_missing_chunks_for_orphan(&self, block_hash: &CryptoHash) -> bool {
        self.orphans_requested_missing_chunks.len() < MAX_ORPHAN_MISSING_CHUNKS
            && !self.orphans_requested_missing_chunks.contains(block_hash)
    }

    fn mark_missing_chunks_requested_for_orphan(&mut self, block_hash: CryptoHash) {
        self.orphans_requested_missing_chunks.insert(block_hash);
    }
}

/// Contains information needed to request chunks for orphans
/// Fields will be used as arguments for `request_chunks_for_orphan`
pub struct OrphanMissingChunks {
    pub missing_chunks: Vec<ShardChunkHeader>,
    /// epoch id for the block that has missing chunks
    pub epoch_id: EpochId,
    /// hash of an ancestor block of the block that has missing chunks
    /// this is used as an argument for `request_chunks_for_orphan`
    /// see comments in `request_chunks_for_orphan` for what `ancestor_hash` is used for
    pub ancestor_hash: CryptoHash,
}

impl Debug for OrphanMissingChunks {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrphanMissingChunks")
            .field("epoch_id", &self.epoch_id)
            .field("ancestor_hash", &self.ancestor_hash)
            .field("num_missing_chunks", &self.missing_chunks.len())
            .finish()
    }
}

impl Chain {
    pub fn save_orphan(
        &mut self,
        block: MaybeValidated<Arc<Block>>,
        provenance: Provenance,
        requested_missing_chunks: bool,
    ) {
        let block_hash = *block.hash();
        if !self.orphans.contains(block.hash()) {
            self.orphans.add(
                Orphan { block, provenance, added: self.clock.now() },
                requested_missing_chunks,
            );
        }

        tracing::debug!(
            target: "chain",
            ?block_hash,
            orphans_count = %self.orphans.len(),
            evicted_count = %self.orphans.len_evicted(),
            "process block: orphan"
        );
    }

    pub fn save_optimistic_orphan(&mut self, block: OptimisticBlock) {
        self.orphans.add_optimistic(block);
    }

    /// Check if we can request chunks for this orphan. Conditions are
    /// 1) Orphans that with outstanding missing chunks request has not exceed `MAX_ORPHAN_MISSING_CHUNKS`
    /// 2) we haven't already requested missing chunks for the orphan
    /// 3) All the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block are either accepted,
    ///    or orphans or in `blocks_with_missing_chunks`
    /// 4) Among the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block at least one is
    ///    accepted(is in store), call it `ancestor`
    /// 5) The next block of `ancestor` has the same epoch_id as the orphan block
    ///    (This is because when requesting chunks, we will use `ancestor` hash instead of the
    ///     previous block hash of the orphan to decide epoch id)
    /// 6) The orphan has missing chunks
    pub fn should_request_chunks_for_orphan(
        &mut self,
        orphan: &Block,
    ) -> Option<OrphanMissingChunks> {
        // 1) Orphans that with outstanding missing chunks request has not exceed `MAX_ORPHAN_MISSING_CHUNKS`
        // 2) we haven't already requested missing chunks for the orphan
        if !self.orphans.can_request_missing_chunks_for_orphan(orphan.hash()) {
            return None;
        }
        let mut block_hash = *orphan.header().prev_hash();
        for _ in 0..NUM_ORPHAN_ANCESTORS_CHECK {
            // 3) All the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block are either accepted,
            //    or orphans or in `blocks_with_missing_chunks`
            if let Some(block) = self.blocks_with_missing_chunks.get(&block_hash) {
                block_hash = *block.prev_hash();
                continue;
            }
            if let Some(orphan) = self.orphans.get(&block_hash) {
                block_hash = *orphan.prev_hash();
                continue;
            }
            // 4) Among the `NUM_ORPHAN_ANCESTORS_CHECK` immediate parents of the block at least one is
            //    accepted(is in store), call it `ancestor`
            if self.get_block(&block_hash).is_ok() {
                if let Ok(epoch_id) = self.epoch_manager.get_epoch_id_from_prev_block(&block_hash) {
                    // 5) The next block of `ancestor` has the same epoch_id as the orphan block
                    if &epoch_id == orphan.header().epoch_id() {
                        // 6) The orphan has missing chunks
                        if let Err(e) = self.ping_missing_chunks(block_hash, orphan) {
                            return match e {
                                Error::ChunksMissing(missing_chunks) => {
                                    let missing_chunks_list = missing_chunks
                                        .iter()
                                        .map(|chunk| (chunk.shard_id(), chunk.chunk_hash()))
                                        .collect::<Vec<_>>();
                                    tracing::debug!(target: "chain", orphan_hash = ?orphan.hash(), ?missing_chunks_list, "request missing chunks for orphan");
                                    Some(OrphanMissingChunks {
                                        missing_chunks,
                                        epoch_id,
                                        ancestor_hash: block_hash,
                                    })
                                }
                                _ => None,
                            };
                        }
                    }
                }
                return None;
            }
            return None;
        }
        None
    }

    /// only used for test
    pub fn check_orphan_partial_chunks_requested(&self, block_hash: &CryptoHash) -> bool {
        self.orphans.orphans_requested_missing_chunks.contains(block_hash)
    }

    /// Check for orphans that are ready to be processed or request missing chunks, process these blocks.
    /// `prev_hash`: hash of the block that is just accepted
    /// `block_accepted`: callback to be called when an orphan is accepted
    /// `block_misses_chunks`: callback to be called when an orphan is added to the pool of blocks
    ///                        that have missing chunks
    /// `orphan_misses_chunks`: callback to be called when it is ready to request missing chunks for
    ///                         an orphan
    /// `on_challenge`: callback to be called when an orphan should be challenged
    pub fn check_orphans(
        &mut self,
        prev_hash: CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<ApplyChunksDoneSender>,
    ) {
        let _span = debug_span!(
            target: "chain",
            "check_orphans",
            ?prev_hash,
            num_orphans = self.orphans.len())
        .entered();
        // Check if there are orphans we can process.
        // check within the descendants of `prev_hash` to see if there are orphans there that
        // are ready to request missing chunks for
        let orphans_to_check =
            self.orphans.get_orphans_within_depth(prev_hash, NUM_ORPHAN_ANCESTORS_CHECK);
        for orphan_hash in orphans_to_check {
            let Some(orphan) = self.orphans.get(&orphan_hash) else {
                continue;
            };
            let orphan = orphan.block.clone();
            if let Some(orphan_missing_chunks) = self.should_request_chunks_for_orphan(&orphan) {
                block_processing_artifacts.orphans_missing_chunks.push(orphan_missing_chunks);
                self.orphans.mark_missing_chunks_requested_for_orphan(orphan_hash);
            }
        }
        if let Some(orphans) = self.orphans.remove_by_prev_hash(prev_hash) {
            tracing::debug!(target: "chain", found_orphans = orphans.len(), "check orphans");
            for orphan in orphans {
                let block_hash = orphan.hash();
                self.blocks_delay_tracker.mark_block_unorphaned(&block_hash);
                let res = self.start_process_block_async(
                    orphan.block,
                    orphan.provenance,
                    block_processing_artifacts,
                    apply_chunks_done_sender.clone(),
                );
                if let Err(err) = res {
                    tracing::debug!(target: "chain", ?block_hash, ?err, "orphan declined");
                }
            }
            tracing::debug!(
                target: "chain",
                remaining_orphans=self.orphans.len(),
                "check orphans",
            );
        }
        if let Some(optimistic_block) = self.orphans.remove_optimistic(&prev_hash) {
            tracing::debug!(target: "chain", ?optimistic_block, "check optimistic orphan");
            self.preprocess_optimistic_block(optimistic_block, apply_chunks_done_sender);
        }
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn orphans_len(&self) -> usize {
        self.orphans.len()
    }

    /// Check if hash is for a known orphan.
    #[inline]
    pub fn is_orphan(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_async::time::{Clock, FakeClock, Utc};
    use near_primitives::genesis::genesis_block;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::Balance;
    use near_primitives::validator_signer::ValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;

    fn make_orphan(clock: &Clock, block: Arc<Block>) -> Orphan {
        Orphan {
            block: MaybeValidated::from_validated(block),
            provenance: crate::Provenance::NONE,
            added: clock.now(),
        }
    }

    /// Clone a block, override its height and prev_hash, then re-sign.
    fn make_block_at(
        base: &Arc<Block>,
        height: BlockHeight,
        prev_hash: CryptoHash,
        signer: &ValidatorSigner,
    ) -> Arc<Block> {
        let mut block = base.clone();
        let m = Arc::make_mut(&mut block);
        m.mut_header().set_prev_hash(prev_hash);
        m.mut_header().set_height(height);
        m.recompute_fields_derived_from_chunks();
        m.mut_header().resign(signer);
        block
    }

    /// Assert that every hash in every side-index vector exists in
    /// `self.orphans`, and vice versa.
    fn assert_pool_consistency(pool: &OrphanBlockPool) {
        // Forward: every side-index entry must be live in orphans.
        for (prev_hash, hashes) in &pool.prev_hash_idx {
            for hash in hashes {
                assert!(
                    pool.orphans.contains_key(hash),
                    "stale hash {hash} in prev_hash_idx[{prev_hash}]"
                );
            }
        }
        for (height, hashes) in &pool.height_idx {
            for hash in hashes {
                assert!(
                    pool.orphans.contains_key(hash),
                    "stale hash {hash} in height_idx[{height}]"
                );
            }
        }
        // Reverse: every orphan must appear in both side-indexes.
        for (hash, orphan) in &pool.orphans {
            let prev = orphan.block.header().prev_hash();
            assert!(
                pool.prev_hash_idx.get(prev).map_or(false, |v| v.contains(hash)),
                "orphan {hash} not found in prev_hash_idx[{prev}]"
            );
            let height = orphan.block.header().height();
            assert!(
                pool.height_idx.get(&height).map_or(false, |v| v.contains(hash)),
                "orphan {hash} not found in height_idx[{height}]"
            );
        }
        // get_orphans_within_depth must only return live hashes.
        for prev_hash in pool.prev_hash_idx.keys() {
            for hash in pool.get_orphans_within_depth(*prev_hash, 1) {
                assert!(
                    pool.orphans.contains_key(&hash),
                    "get_orphans_within_depth({prev_hash}) returned stale hash {hash}"
                );
            }
        }
    }

    fn make_genesis() -> Arc<Block> {
        Arc::new(genesis_block(
            PROTOCOL_VERSION,
            vec![],
            Utc::now_utc(),
            0,
            Balance::ZERO,
            Balance::ZERO,
            &vec![],
        ))
    }

    /// Overflow via height-based eviction must not leave stale hashes in
    /// prev_hash_idx.  All orphans share one parent; the highest-height
    /// ones are evicted from `self.orphans` and their hashes must be
    /// pruned from the side-index vectors too.
    #[test]
    fn test_height_eviction_overflow_preserves_side_index_consistency() {
        let clock = Clock::real();
        let signer = Arc::new(create_test_signer("test"));

        let genesis = make_genesis();
        let parent =
            TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone()).build();
        let parent_hash = *parent.hash();
        let base_height = parent.header().height();

        let mut pool = OrphanBlockPool::new();

        // 1025 orphans, all children of P, at distinct heights.
        // Height-based eviction removes the highest ones first.
        for i in 0..=(MAX_ORPHAN_SIZE as u64) {
            let child = make_block_at(&parent, base_height + 1 + i, parent_hash, &signer);
            pool.add(make_orphan(&clock, child), false);
        }

        assert!(pool.len() < MAX_ORPHAN_SIZE);
        assert_pool_consistency(&pool);

        // get_orphans_within_depth should return exactly the live children.
        let returned: HashSet<CryptoHash> =
            pool.get_orphans_within_depth(parent_hash, 1).into_iter().collect();
        let live: HashSet<CryptoHash> = pool.orphans.keys().copied().collect();
        assert_eq!(returned, live);

        // remove_by_prev_hash should return all of them with no silent drops.
        let removed = pool.remove_by_prev_hash(parent_hash).unwrap();
        assert_eq!(removed.len(), live.len());
    }

    /// Overflow via age-based eviction must not leave stale hashes in
    /// height_idx.  Old orphans share a height with newer siblings;
    /// the old ones are evicted by age, but the surviving siblings keep
    /// the height_idx bucket alive — stale hashes must be pruned from
    /// inside the surviving bucket.
    #[test]
    fn test_age_eviction_overflow_preserves_side_index_consistency() {
        let fake_clock = FakeClock::default();
        let clock = fake_clock.clock();
        let signer = Arc::new(create_test_signer("test"));

        let genesis = make_genesis();
        let parent =
            TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone()).build();
        let parent_hash = *parent.hash();
        let genesis_hash = *genesis.hash();
        let base_height = parent.header().height();

        let mut pool = OrphanBlockPool::new();

        // Phase 1: "old" orphans at heights base+1..base+10.
        let num_old = 10u64;
        for i in 1..=num_old {
            let child = make_block_at(&parent, base_height + i, parent_hash, &signer);
            pool.add(make_orphan(&clock, child), false);
        }

        // Age them past MAX_ORPHAN_AGE_SECS.
        fake_clock.advance(Duration::seconds(MAX_ORPHAN_AGE_SECS as i64 + 1));

        // Phase 2: "new" orphans at the SAME heights (different prev_hash
        // so they get distinct block hashes).  These survive age cleanup
        // and keep the height_idx buckets alive.
        for i in 1..=num_old {
            let child = make_block_at(&parent, base_height + i, genesis_hash, &signer);
            pool.add(make_orphan(&clock, child), false);
        }

        // Phase 3: fill up to overflow.
        let remaining = MAX_ORPHAN_SIZE as u64 + 1 - num_old * 2;
        for i in 0..remaining {
            let child = make_block_at(&parent, base_height + num_old + 1 + i, parent_hash, &signer);
            pool.add(make_orphan(&clock, child), false);
        }

        assert!(pool.len() < MAX_ORPHAN_SIZE);
        assert_pool_consistency(&pool);
    }

    /// remove_by_prev_hash must prune stale hashes from height_idx when the
    /// removed orphans share heights with orphans under a different parent.
    /// The surviving siblings keep the bucket alive, so stale hashes must be
    /// pruned from inside the bucket, not only from fully-empty buckets.
    #[test]
    fn test_remove_by_prev_hash_preserves_side_index_consistency() {
        let clock = Clock::real();
        let signer = Arc::new(create_test_signer("test"));

        let genesis = make_genesis();
        let parent_p =
            TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone()).build();
        let parent_p_hash = *parent_p.hash();
        let parent_q_hash = *genesis.hash();
        let base_height = parent_p.header().height();

        let mut pool = OrphanBlockPool::new();

        // Children of P and Q share the same three heights.
        for i in 1..=3u64 {
            let p_child = make_block_at(&parent_p, base_height + i, parent_p_hash, &signer);
            let q_child = make_block_at(&parent_p, base_height + i, parent_q_hash, &signer);
            pool.add(make_orphan(&clock, p_child), false);
            pool.add(make_orphan(&clock, q_child), false);
        }
        assert_eq!(pool.len(), 6);

        // Remove children of P. Children of Q keep the height buckets alive,
        // so the P-children hashes would remain stale without inner pruning.
        let removed = pool.remove_by_prev_hash(parent_p_hash).unwrap();
        assert_eq!(removed.len(), 3);
        assert_eq!(pool.len(), 3);
        assert_pool_consistency(&pool);
    }

    /// Test that `get_orphans_within_depth` collects all orphans within the
    /// target depth when there are multiple forks.
    ///
    /// Tree structure:
    ///   genesis -> block_a (depth 0) -> block_b (depth 1) -> block_d (depth 2) -> block_f (depth 3)
    ///                               \-> block_c (depth 1) -> block_e (depth 2) -> block_g (depth 3)
    ///
    /// With target_depth=2, all 4 orphans (B, C, D, E) must be returned.
    #[test]
    fn test_get_orphans_within_depth_with_forks() {
        let clock = Clock::real();
        let signer = Arc::new(create_test_signer("test"));

        // Create genesis block.
        let genesis = Arc::new(genesis_block(
            PROTOCOL_VERSION,
            vec![],
            Utc::now_utc(),
            0,
            Balance::ZERO,
            Balance::ZERO,
            &vec![],
        ));

        // Create block_a - the parent of the fork
        let block_a =
            TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone()).build();

        // Create two children of block_a
        let block_b =
            TestBlockBuilder::from_prev_block(clock.clone(), &block_a, signer.clone()).build();
        let block_c =
            TestBlockBuilder::from_prev_block(clock.clone(), &block_a, signer.clone()).build();

        // Create children of B and C
        let block_d =
            TestBlockBuilder::from_prev_block(clock.clone(), &block_b, signer.clone()).build();
        let block_e =
            TestBlockBuilder::from_prev_block(clock.clone(), &block_c, signer.clone()).build();

        // Create children of D and E
        let block_f =
            TestBlockBuilder::from_prev_block(clock.clone(), &block_d, signer.clone()).build();
        let block_g = TestBlockBuilder::from_prev_block(clock.clone(), &block_e, signer).build();

        // Add all 6 blocks as orphans.
        let mut pool = OrphanBlockPool::new();
        pool.add(make_orphan(&clock, block_b.clone()), false);
        pool.add(make_orphan(&clock, block_c.clone()), false);
        pool.add(make_orphan(&clock, block_d.clone()), false);
        pool.add(make_orphan(&clock, block_e.clone()), false);
        pool.add(make_orphan(&clock, block_f), false);
        pool.add(make_orphan(&clock, block_g), false);

        // Query for all orphans within depth 2 from block_a.
        let result = pool.get_orphans_within_depth(*block_a.hash(), 2);
        let result_set: HashSet<CryptoHash> = result.into_iter().collect();

        assert!(result_set.contains(block_b.hash()), "missing block B (depth 1)");
        assert!(result_set.contains(block_c.hash()), "missing block C (depth 1)");
        assert!(result_set.contains(block_d.hash()), "missing block D (depth 2)");
        assert!(result_set.contains(block_e.hash()), "missing block E (depth 2)");
        assert_eq!(result_set.len(), 4);
    }
}
