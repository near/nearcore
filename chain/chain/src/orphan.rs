use crate::chain::ApplyChunksDoneMessage;
use near_async::messaging::Sender;
use near_async::time::{Duration, Instant};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_primitives::utils::MaybeValidated;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use tracing::{debug, debug_span};

use crate::missing_chunks::BlockLike;
use crate::{metrics, BlockProcessingArtifact, Chain, Provenance};

/// Maximum number of orphans chain can store.
const MAX_ORPHAN_SIZE: usize = 1024;

/// Maximum age of orphan to store in the chain.
const MAX_ORPHAN_AGE_SECS: u64 = 300;

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
    pub(crate) block: MaybeValidated<Block>,
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
    /// A map from block hash to a orphan block
    orphans: HashMap<CryptoHash, Orphan>,
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
            self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));
            self.prev_hash_idx
                .retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));
            self.orphans_requested_missing_chunks.retain(|x| !removed_hashes.contains(x));

            self.evicted += old_len - self.orphans.len();
        }
        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains_key(hash)
    }

    pub fn get(&self, hash: &CryptoHash) -> Option<&Orphan> {
        self.orphans.get(hash)
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

        self.height_idx.retain(|_, ref mut xs| xs.iter().any(|x| !removed_hashes.contains(x)));

        metrics::NUM_ORPHANS.set(self.orphans.len() as i64);
        ret
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
                break;
            }
            if let Some(block_hashes) = self.prev_hash_idx.get(&prev_hash) {
                for hash in block_hashes {
                    queue.push((*hash, depth + 1));
                    res.push(*hash);
                    // there should be no loop
                    debug_assert!(_visited.insert(*hash));
                }
            }

            // probably something serious went wrong here because there shouldn't be so many forks
            assert!(
                res.len() <= 100 * target_depth as usize,
                "found too many orphans {:?}, probably something is wrong with the chain",
                res
            );
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
        block: MaybeValidated<Block>,
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

        debug!(
            target: "chain",
            "Process block: orphan: {:?}, # orphans {}{}",
            block_hash,
            self.orphans.len(),
            if self.orphans.len_evicted() > 0 {
                format!(", # evicted {}", self.orphans.len_evicted())
            } else {
                String::new()
            },
        );
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
        me: &Option<AccountId>,
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
                        if let Err(e) = self.ping_missing_chunks(me, block_hash, orphan) {
                            return match e {
                                Error::ChunksMissing(missing_chunks) => {
                                    debug!(target:"chain", "Request missing chunks for orphan {:?} {:?}", orphan.hash(), missing_chunks.iter().map(|chunk|{(chunk.shard_id(), chunk.chunk_hash())}).collect::<Vec<_>>());
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
        me: &Option<AccountId>,
        prev_hash: CryptoHash,
        block_processing_artifacts: &mut BlockProcessingArtifact,
        apply_chunks_done_sender: Option<Sender<ApplyChunksDoneMessage>>,
    ) {
        let _span = debug_span!(
            target: "chain",
            "check_orphans",
            ?prev_hash,
            num_orphans = self.orphans.len())
        .entered();
        // Check if there are orphans we can process.
        // check within the descendents of `prev_hash` to see if there are orphans there that
        // are ready to request missing chunks for
        let orphans_to_check =
            self.orphans.get_orphans_within_depth(prev_hash, NUM_ORPHAN_ANCESTORS_CHECK);
        for orphan_hash in orphans_to_check {
            let orphan = self.orphans.get(&orphan_hash).unwrap().block.clone();
            if let Some(orphan_missing_chunks) = self.should_request_chunks_for_orphan(me, &orphan)
            {
                block_processing_artifacts.orphans_missing_chunks.push(orphan_missing_chunks);
                self.orphans.mark_missing_chunks_requested_for_orphan(orphan_hash);
            }
        }
        if let Some(orphans) = self.orphans.remove_by_prev_hash(prev_hash) {
            debug!(target: "chain", found_orphans = orphans.len(), "Check orphans");
            for orphan in orphans.into_iter() {
                let block_hash = orphan.hash();
                self.blocks_delay_tracker.mark_block_unorphaned(&block_hash);
                let res = self.start_process_block_async(
                    me,
                    orphan.block,
                    orphan.provenance,
                    block_processing_artifacts,
                    apply_chunks_done_sender.clone(),
                );
                if let Err(err) = res {
                    debug!(target: "chain", "Orphan {:?} declined, error: {:?}", block_hash, err);
                }
            }
            debug!(
                target: "chain",
                remaining_orphans=self.orphans.len(),
                "Check orphans",
            );
        }
    }

    /// Returns number of orphans currently in the orphan pool.
    #[inline]
    pub fn orphans_len(&self) -> usize {
        self.orphans.len()
    }

    /// Returns number of evicted orphans.
    #[inline]
    pub fn orphans_evicted_len(&self) -> usize {
        self.orphans.len_evicted()
    }

    /// Check if hash is for a known orphan.
    #[inline]
    pub fn is_orphan(&self, hash: &CryptoHash) -> bool {
        self.orphans.contains(hash)
    }
}
