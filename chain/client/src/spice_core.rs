use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::Sender;
use near_chain::{Block, Error};
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::types::{
    AccountId, BlockExecutionResults, ChunkExecutionResult, ChunkExecutionResultHash, ShardId,
};

use crate::chunk_executor_actor::ExecutionResultEndorsed;

#[derive(Clone)]
pub struct CoreStatementsProcessor(Arc<RwLock<CoreStatementsTracker>>);

impl CoreStatementsProcessor {
    pub fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chunk_executor_sender: Sender<ExecutionResultEndorsed>,
        spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    ) -> Self {
        Self(Arc::new(RwLock::new(CoreStatementsTracker::new(
            chain_store,
            epoch_manager,
            chunk_executor_sender,
            spice_chunk_validator_sender,
        ))))
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
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    chunk_executor_sender: Sender<ExecutionResultEndorsed>,
    spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    // TODO(spice): persist endorsements and execution results in db, making sure relevant chunks
    // exist (to avoid storing garbage that cannot be garbage collected).
    /// Endorsements keyed by the chunk's hash execution of which they endorse.
    endorsements: LruCache<ChunkHash, Vec<(AccountId, ChunkExecutionResultHash, Signature)>>,
    /// Execution results of the chunk keyed by the chunk's hash.
    execution_results: LruCache<ChunkHash, ChunkExecutionResult>,
}

impl CoreStatementsTracker {
    fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chunk_executor_sender: Sender<ExecutionResultEndorsed>,
        spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    ) -> Self {
        const CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
        Self {
            chain_store,
            epoch_manager,
            chunk_executor_sender,
            spice_chunk_validator_sender,
            endorsements: LruCache::new(CACHE_SIZE),
            execution_results: LruCache::new(CACHE_SIZE),
        }
    }

    fn all_execution_results_exist(&mut self, block: &Block) -> bool {
        for chunk in block.chunks().iter_raw() {
            if self.execution_results.get(&chunk.chunk_hash()).is_none() {
                return false;
            }
        }
        true
    }
}

impl CoreStatementsProcessor {
    /// Records endorsement and execution result contained within. Endorsement should be already
    /// validated.
    pub fn record_chunk_endorsement(&self, mut endorsement: ChunkEndorsement) -> Result<(), Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let chunk_production_key = endorsement.chunk_production_key();
        let chunk_hash = endorsement.chunk_hash();
        let block_hash = endorsement
            .block_hash()
            .expect("with spice endorsements should always contain block hash");
        let execution_result = endorsement
            .take_execution_result()
            .expect("with spice endorsements should always contain execution results");
        let chunk_execution_result_hash = execution_result.compute_hash();

        let mut tracker = self.write();

        let chunk_validator_assignments = tracker.epoch_manager.get_chunk_validator_assignments(
            &chunk_production_key.epoch_id,
            chunk_production_key.shard_id,
            chunk_production_key.height_created,
        )?;

        let endorsements =
            tracker.endorsements.get_or_insert_mut(chunk_hash.clone(), || Vec::new());
        endorsements.push((
            endorsement.account_id().clone(),
            chunk_execution_result_hash.clone(),
            endorsement.signature(),
        ));

        let validator_signatures = endorsements
            .iter()
            // TODO(spice): Make sure that block hash in all endorsements is the same as well.
            .filter(|(_, result_hash, _)| &chunk_execution_result_hash == result_hash)
            .map(|(account_id, _, signature)| (account_id, signature.clone()))
            .collect();
        let endorsement_state =
            chunk_validator_assignments.compute_endorsement_state(validator_signatures);

        if !endorsement_state.is_endorsed {
            return Ok(());
        }

        tracker.execution_results.get_or_insert(chunk_hash, || execution_result);

        let block = match tracker.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(err) => return Err(err),
        };

        if tracker.all_execution_results_exist(&block) {
            let result_endorsed_message =
                ExecutionResultEndorsed { block_hash: *block.header().hash() };
            tracker.chunk_executor_sender.send(result_endorsed_message.clone());
            tracker.spice_chunk_validator_sender.send(result_endorsed_message);
        }

        Ok(())
    }

    pub fn get_execution_results_by_shard_id(
        &self,
        block: &Block,
    ) -> HashMap<ShardId, ChunkExecutionResult> {
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

    /// Returns ChunkExecutionResult for all chunks or None if at least one execution result is
    /// missing;
    pub fn get_block_execution_results(&self, block: &Block) -> Option<BlockExecutionResults> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        if block.header().is_genesis() {
            return Some(BlockExecutionResults(HashMap::new()));
        }

        let mut tracker = self.write();
        let mut results = HashMap::new();
        for chunk in block.chunks().iter_raw() {
            let Some(result) = tracker.execution_results.get(&chunk.chunk_hash()) else {
                return None;
            };
            results.insert(chunk.chunk_hash().clone(), result.clone());
        }
        Some(BlockExecutionResults(results))
    }

    pub fn all_execution_results_exist(&self, block: &Block) -> bool {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut tracker = self.write();
        tracker.all_execution_results_exist(block)
    }
}
