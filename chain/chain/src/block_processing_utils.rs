use crate::chain::ApplyChunkResult;
use near_primitives::challenge::ChallengesResult;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ReceiptProof, StateSyncInfo};
use near_primitives::types::ShardId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Max number of blocks that can be in the pool at once.
/// This number will likely never be hit unless there are many forks in the chain.
const MAX_PROCESSING_BLOCKS: usize = 5;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Contains information from preprocessing a block
#[derive(Default)]
pub(crate) struct BlockPreprocessInfo {
    pub(crate) is_caught_up: bool,
    pub(crate) state_dl_info: Option<StateSyncInfo>,
    pub(crate) incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
    pub(crate) challenges_result: ChallengesResult,
    pub(crate) challenged_blocks: Vec<CryptoHash>,
}

pub(crate) trait BlockLike {
    fn hash(&self) -> &CryptoHash;
}

/// BlockProcessingPool is used to keep track of blocks have been preprocessed but
/// haven't been processed fully yet. It is needed because the applying of blocks
/// (processing transactions and receipts) are done in a separate thread from the
/// ClientActor thread, where blocks are preprocessed and changes from applying blocks
/// are stored. This struct is thread-safe.
/// The reason to use the BlockLike trait is to make testing easier.
#[derive(Clone)]
pub(crate) struct BlockProcessingPool<Block: BlockLike>(
    Arc<RwLock<BlockProcessingPoolImpl<Block>>>,
);

struct BlockProcessingPoolImpl<Block: BlockLike> {
    preprocessed_blocks: HashMap<CryptoHash, (Block, BlockPreprocessInfo)>,
    applied_blocks:
        HashMap<CryptoHash, Vec<Result<ApplyChunkResult, near_chain_primitives::Error>>>,
}

#[derive(Debug)]
pub(crate) enum Error {
    ExceedingPoolSize,
    BlockNotPreprocessed,
}

impl<Block: BlockLike> BlockProcessingPool<Block> {
    pub(crate) fn new() -> Self {
        Self(Arc::new(RwLock::new(BlockProcessingPoolImpl {
            preprocessed_blocks: HashMap::new(),
            applied_blocks: HashMap::new(),
        })))
    }

    /// Add a preprocessed block to the pool. Return Error::ExceedingPoolSize if the pool already
    /// reaches its max size.
    pub(crate) fn add_preprocessed_block(
        &self,
        block: Block,
        preprocess_info: BlockPreprocessInfo,
    ) -> Result<(), Error> {
        let mut pool = self.0.write().expect(POISONED_LOCK_ERR);
        if pool.preprocessed_blocks.len() >= MAX_PROCESSING_BLOCKS {
            return Err(Error::ExceedingPoolSize);
        }

        pool.preprocessed_blocks.insert(*block.hash(), (block, preprocess_info));
        Ok(())
    }

    /// Add a applied block to the pool. Return Error::BlockNotPreprocessed if the block has not been
    /// added as preprocessed.
    pub(crate) fn add_applied_block(
        &self,
        block_hash: CryptoHash,
        apply_results: Vec<Result<ApplyChunkResult, near_chain_primitives::Error>>,
    ) -> Result<(), Error> {
        let mut pool = self.0.write().expect(POISONED_LOCK_ERR);
        if !pool.preprocessed_blocks.contains_key(&block_hash) {
            return Err(Error::BlockNotPreprocessed);
        }

        pool.applied_blocks.insert(block_hash, apply_results);
        Ok(())
    }

    /// Take all applied blocks from the pool.
    pub(crate) fn take_applied_blocks(
        &self,
    ) -> Vec<(
        Block,
        BlockPreprocessInfo,
        Vec<Result<ApplyChunkResult, near_chain_primitives::Error>>,
    )> {
        if self.0.read().expect(POISONED_LOCK_ERR).applied_blocks.is_empty() {
            return vec![];
        }

        let mut pool = self.0.write().expect(POISONED_LOCK_ERR);

        let mut res = vec![];
        let applied_blocks = pool.applied_blocks.drain().collect::<Vec<_>>();
        for (block_hash, apply_result) in applied_blocks {
            let (block, preprocess_result) = pool.preprocessed_blocks.remove(&block_hash).expect(
                &format!("block {:?} in applied_blocks but not in preprocessed_blocks", block_hash),
            );
            res.push((block, preprocess_result, apply_result));
        }

        res
    }

    /// Returns whether the block has been added as preprocessed
    pub(crate) fn is_block_preprocessed(&self, block_hash: &CryptoHash) -> bool {
        self.0.read().expect(POISONED_LOCK_ERR).preprocessed_blocks.contains_key(block_hash)
    }

    /// Returns whether the pool is full
    pub(crate) fn is_full(&self) -> bool {
        self.0.read().expect(POISONED_LOCK_ERR).preprocessed_blocks.len() >= MAX_PROCESSING_BLOCKS
    }
}

#[cfg(test)]
mod test {
    use crate::block_processing_utils::{BlockLike, BlockPreprocessInfo, BlockProcessingPool};
    use actix::clock::{sleep, timeout};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::time::Duration;
    use std::collections::HashSet;

    #[derive(Clone)]
    struct MockBlock {
        hash: CryptoHash,
    }
    impl BlockLike for MockBlock {
        fn hash(&self) -> &CryptoHash {
            &self.hash
        }
    }

    #[test]
    // This test creates a scenario similar to how BlockProcessingPool will be used in chain.
    // One thread adds preprocessed blocks to the pool and schedule other threads to add applied
    // blocks to the pool. Then it waits and keeps checking if there are applied blocks in the pool
    fn test_block_processing_pool_basic() {
        actix::System::new().block_on(async {
            timeout(Duration::from_secs(5), async {
                let pool = BlockProcessingPool::new();
                let mut hashes = HashSet::new();
                for i in 0..10 {
                    let hash = hash(&[i]);
                    let res = pool.add_preprocessed_block(
                        MockBlock { hash: hash.clone() },
                        BlockPreprocessInfo::default(),
                    );
                    if res.is_ok() {
                        hashes.insert(hash);
                        let pool1 = pool.clone();
                        let hash1 = hash.clone();
                        // spwan another thread to add applied blocks later
                        actix::spawn(async move {
                            assert!(pool1.is_block_preprocessed(&hash1));
                            sleep(Duration::from_secs(1)).await;
                            pool1.add_applied_block(hash, vec![]).unwrap();
                        });
                    } else {
                        assert!(!pool.is_block_preprocessed(&hash));
                    }
                }

                assert!(pool.is_full());
                for hash in hashes.iter() {
                    assert!(pool.is_block_preprocessed(hash));
                }

                // check applied blocks in the pool
                while !hashes.is_empty() {
                    sleep(Duration::from_millis(100)).await;
                    for (block, _, _) in pool.take_applied_blocks() {
                        assert!(hashes.remove(block.hash()));
                    }
                }
            })
            .await
            .unwrap()
        })
    }
}
