use parking_lot::{RwLock, RwLockWriteGuard};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::Sender;
use near_chain::Block;
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::types::{ChunkExecutionResult, ShardId};

use crate::chunk_executor_actor::ExecutorExecutionResultEndorsed;

#[derive(Clone)]
pub struct CoreStatementsProcessor(Arc<RwLock<CoreStatementsTracker>>);

impl CoreStatementsProcessor {
    pub fn new(chunk_executor_sender: Sender<ExecutorExecutionResultEndorsed>) -> Self {
        Self(Arc::new(RwLock::new(CoreStatementsTracker::new(chunk_executor_sender))))
    }

    fn write(&self) -> RwLockWriteGuard<CoreStatementsTracker> {
        self.0.write()
    }
}

/// Tracks core statements that node knows about. This includes core statements that correspond to
/// core state as well as additional core statements that may not be included in the core state.
/// Core state is based purely on core statement we include in blocks. However, we need to make
/// decisions based on endorsements before blocks are producers to allow execution to catch up to
/// consensus.
struct CoreStatementsTracker {
    chunk_executor_sender: Sender<ExecutorExecutionResultEndorsed>,
    // TODO(spice): persist endorsements and execution results in db, making sure relevant chunks
    // exist (to avoid storing garbage that cannot be garbage collected).
    /// Endorsements keyed by the chunk's hash execution of which they endorse.
    endorsements: LruCache<ChunkHash, Vec<ChunkEndorsement>>,
    /// Execution results of the chunk keyed by the chunk's hash.
    execution_results: LruCache<ChunkHash, ChunkExecutionResult>,
}

impl CoreStatementsTracker {
    fn new(chunk_executor_sender: Sender<ExecutorExecutionResultEndorsed>) -> Self {
        const CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
        Self {
            chunk_executor_sender,
            endorsements: LruCache::new(CACHE_SIZE),
            execution_results: LruCache::new(CACHE_SIZE),
        }
    }
}

impl CoreStatementsProcessor {
    /// Records endorsement and execution result contained within. Endorsement should be already
    /// validated.
    pub fn record_chunk_endorsement(&self, mut endorsement: ChunkEndorsement) {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let chunk_hash = endorsement.chunk_hash();
        let block_hash = endorsement
            .block_hash()
            .expect("with spice endorsements should always contain block hash");
        let execution_result = endorsement
            .take_execution_result()
            .expect("with spice endorsements should always contain execution results");

        let mut tracker = self.write();
        tracker.endorsements.get_or_insert_mut(chunk_hash.clone(), || Vec::new()).push(endorsement);
        // TODO(spice): Wait for enough endorsements for chunk, checking that they endorse the same
        // execution result, before recording execution result.
        tracker.execution_results.get_or_insert(chunk_hash.clone(), || execution_result);
        // TODO(spice): Send this only once all chunks from the block have their execution results
        // endorsed and don't send chunk_hash.
        tracker
            .chunk_executor_sender
            .send(ExecutorExecutionResultEndorsed { block_hash, chunk_hash });
    }

    pub fn get_execution_results(&self, block: &Block) -> HashMap<ShardId, ChunkExecutionResult> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut tracker = self.write();
        let mut results = HashMap::new();
        for chunk in block.chunks().iter_raw() {
            let Some(result) = tracker.execution_results.get(&chunk.chunk_hash()) else {
                continue;
            };
            let shard_id = chunk.shard_id();
            results.insert(shard_id, result.clone());
        }
        results
    }

    pub fn all_execution_results_exist(&self, block: &Block) -> bool {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut tracker = self.write();
        for chunk in block.chunks().iter_raw() {
            if tracker.execution_results.get(&chunk.chunk_hash()).is_none() {
                return false;
            }
        }
        true
    }
}
