use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_store::adapter::chain_store::ChainStoreAdapter;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::{IntoSender as _, Sender, noop};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::{
    ChunkEndorsement, SpiceEndorsementSignedInner,
};
use near_primitives::types::{
    AccountId, BlockExecutionResults, ChunkExecutionResult, ChunkExecutionResultHash, ShardId,
};

/// Message that should be sent once executions results for all chunks in a block are endorsed.
#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct ExecutionResultEndorsed {
    pub block_hash: CryptoHash,
}

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

    pub fn new_with_noop_senders(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self::new(chain_store, epoch_manager, noop().into_sender(), noop().into_sender())
    }

    fn write(&self) -> RwLockWriteGuard<CoreStatementsTracker> {
        self.0.write()
    }
}

#[derive(Clone)]
struct SpiceUncertifiedChunkInfo {
    chunk_production_key: ChunkProductionKey,
    missing_endorsements: Vec<AccountId>,
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
    // TODO(spice): persist endorsements, execution results and uncertified chunks in db, making
    // sure we don't store anything that cannot be gc-ed.
    /// Endorsements keyed by the chunk's hash execution of which they endorse.
    endorsements:
        LruCache<ChunkProductionKey, HashMap<AccountId, (SpiceEndorsementSignedInner, Signature)>>,
    /// Execution results of the chunk keyed by the chunk's hash.
    execution_results: LruCache<ChunkProductionKey, ChunkExecutionResult>,
    /// Uncertified chunks for this block and all it's ancestry.
    uncertified_chunks: LruCache<CryptoHash, Vec<SpiceUncertifiedChunkInfo>>,
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
            uncertified_chunks: LruCache::new(CACHE_SIZE),
        }
    }

    fn all_execution_results_exist(&mut self, block: &Block) -> bool {
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            if self.execution_results.get(&key).is_none() {
                return false;
            }
        }
        true
    }

    fn save_endorsement(
        &mut self,
        chunk_production_key: ChunkProductionKey,
        account_id: AccountId,
        signed_inner: SpiceEndorsementSignedInner,
        signature: Signature,
    ) -> &HashMap<AccountId, (SpiceEndorsementSignedInner, Signature)> {
        let endorsements =
            self.endorsements.get_or_insert_mut(chunk_production_key, || HashMap::new());
        endorsements.insert(account_id, (signed_inner, signature));
        endorsements
    }

    fn get_uncertified_chunks(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
        let header = self.chain_store.get_block_header(block_hash)?;

        if header.is_genesis() {
            Ok(vec![])
        } else {
            // TODO(spice): Don't unwrap after adding persistence.
            Ok(self.uncertified_chunks.get(block_hash).unwrap().clone())
        }
    }

    fn try_sending_execution_result_endorsed(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(err) => return Err(err),
        };

        if self.all_execution_results_exist(&block) {
            let result_endorsed_message =
                ExecutionResultEndorsed { block_hash: *block.header().hash() };
            self.chunk_executor_sender.send(result_endorsed_message.clone());
            self.spice_chunk_validator_sender.send(result_endorsed_message);
        }
        Ok(())
    }

    fn record_uncertified_chunks(
        &mut self,
        block: &Block,
        endorsements: &HashSet<(&ChunkProductionKey, &AccountId)>,
        block_execution_results: &HashMap<&ChunkProductionKey, ChunkExecutionResultHash>,
    ) -> Result<(), Error> {
        let prev_hash = block.header().prev_hash();
        let mut uncertified_chunks = self.get_uncertified_chunks(prev_hash)?;
        uncertified_chunks.retain(|chunk_info| {
            !block_execution_results.contains_key(&chunk_info.chunk_production_key)
        });
        for chunk_info in &mut uncertified_chunks {
            chunk_info.missing_endorsements.retain(|account_id| {
                !endorsements.contains(&(&chunk_info.chunk_production_key, account_id))
            });
            assert!(
                !chunk_info.missing_endorsements.is_empty(),
                "when there are no missing endorsements execution result should be present"
            );
        }
        let epoch_id = block.header().epoch_id();
        let chunks = block.chunks();
        uncertified_chunks.reserve_exact(chunks.len());
        for chunk in chunks.iter_raw() {
            let shard_id = chunk.shard_id();
            let height_created = chunk.height_created();
            let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                &epoch_id,
                shard_id,
                height_created,
            )?;

            let missing_endorsements = chunk_validator_assignments
                .assignments()
                .iter()
                .map(|(account_id, _)| account_id)
                .cloned()
                .collect();
            uncertified_chunks.push(SpiceUncertifiedChunkInfo {
                chunk_production_key: ChunkProductionKey {
                    epoch_id: *epoch_id,
                    shard_id,
                    height_created,
                },
                missing_endorsements,
            });
        }
        self.uncertified_chunks.push(*block.header().hash(), uncertified_chunks);
        Ok(())
    }
}

impl CoreStatementsProcessor {
    /// Records endorsement and execution result contained within. Endorsement should be already
    /// validated.
    pub fn record_chunk_endorsement(&self, endorsement: ChunkEndorsement) -> Result<(), Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let Some((
            chunk_production_key,
            endorsement_account_id,
            endorsement_signed_inner,
            execution_result,
            endorsement_signature,
        )) = endorsement.spice_destructure()
        else {
            return Ok(());
        };

        let mut tracker = self.write();

        let chunk_validator_assignments = tracker.epoch_manager.get_chunk_validator_assignments(
            &chunk_production_key.epoch_id,
            chunk_production_key.shard_id,
            chunk_production_key.height_created,
        )?;

        let endorsements = tracker.save_endorsement(
            chunk_production_key.clone(),
            endorsement_account_id,
            endorsement_signed_inner.clone(),
            endorsement_signature,
        );

        let validator_signatures = endorsements
            .iter()
            .filter(|(_, (signed_inner, _))| signed_inner == &endorsement_signed_inner)
            .map(|(account_id, (_, signature))| (account_id, signature.clone()))
            .collect();
        let endorsement_state =
            chunk_validator_assignments.compute_endorsement_state(validator_signatures);

        if !endorsement_state.is_endorsed {
            return Ok(());
        }

        tracker.execution_results.get_or_insert(chunk_production_key, || execution_result);

        let block_hash = endorsement_signed_inner.block_hash;
        tracker.try_sending_execution_result_endorsed(&block_hash)
    }

    pub fn get_execution_results_by_shard_id(
        &self,
        block: &Block,
    ) -> HashMap<ShardId, ChunkExecutionResult> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut tracker = self.write();
        let mut results = HashMap::new();
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            let Some(result) = tracker.execution_results.get(&key) else {
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
            let key = make_chunk_production_key(block, chunk);
            let Some(result) = tracker.execution_results.get(&key) else {
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

    pub fn core_statement_for_next_block(
        &self,
        block_header: &BlockHeader,
    ) -> Result<Vec<SpiceCoreStatement>, Error> {
        if block_header.is_genesis() {
            return Ok(vec![]);
        }
        let block_hash = block_header.hash();

        let mut tracker = self.write();
        let uncertified_chunks =
            tracker.uncertified_chunks.get(block_hash).cloned().unwrap_or_default();

        let mut core_statements = Vec::new();
        for chunk_info in uncertified_chunks {
            let Some(known_endorsements) =
                tracker.endorsements.get(&chunk_info.chunk_production_key)
            else {
                continue;
            };
            for account_id in chunk_info.missing_endorsements {
                if let Some((signed_inner, signature)) = known_endorsements.get(&account_id) {
                    core_statements.push(SpiceCoreStatement::Endorsement {
                        chunk_production_key: chunk_info.chunk_production_key.clone(),
                        account_id,
                        signed_inner: signed_inner.clone(),
                        signature: signature.clone(),
                    });
                }
            }

            let Some(execution_result) =
                tracker.execution_results.get(&chunk_info.chunk_production_key)
            else {
                continue;
            };
            // Execution results are stored only for endorsed chunks.
            core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_production_key: chunk_info.chunk_production_key,
                execution_result: execution_result.clone(),
            });
        }
        Ok(core_statements)
    }

    pub fn record_core_statements_from_block(&mut self, block: &Block) -> Result<(), Error> {
        let mut tracker = self.write();
        let mut block_execution_results = HashMap::new();
        let mut execution_result_original_block = HashMap::new();
        let mut endorsements = HashSet::new();
        for core_statement in block.spice_core_statements() {
            match core_statement {
                SpiceCoreStatement::Endorsement {
                    account_id,
                    signature,
                    chunk_production_key,
                    signed_inner,
                } => {
                    tracker.save_endorsement(
                        chunk_production_key.clone(),
                        account_id.clone(),
                        signed_inner.clone(),
                        signature.clone(),
                    );
                    endorsements.insert((chunk_production_key, account_id));
                    execution_result_original_block
                        .insert(&signed_inner.execution_result_hash, signed_inner.block_hash);
                }
                SpiceCoreStatement::ChunkExecutionResult {
                    chunk_production_key,
                    execution_result,
                } => {
                    tracker
                        .execution_results
                        .get_or_insert(chunk_production_key.clone(), || execution_result.clone());

                    block_execution_results
                        .insert(chunk_production_key, execution_result.compute_hash());
                }
            };
        }

        for (_, execution_result_hash) in &block_execution_results {
            let block_hash = execution_result_original_block.get(execution_result_hash).expect(
                "block validation should make sure that each block contains at least one endorsement for each execution_result");

            if let Err(err) = tracker.try_sending_execution_result_endorsed(block_hash) {
                // If we don't have a relevant previous block we can not be waiting for it's
                // execution results.
                if !matches!(err, Error::DBNotFoundErr(_)) {
                    return Err(err);
                }
            }
        }

        tracker.record_uncertified_chunks(block, &endorsements, &block_execution_results)
    }

    pub fn validate_core_statements_in_block(
        &self,
        block: &Block,
    ) -> Result<(), InvalidSpiceCoreStatementsError> {
        use InvalidSpiceCoreStatementsError::*;

        let mut tracker = self.write();

        let prev_uncertified_chunks = tracker
            .get_uncertified_chunks(block.header().prev_hash())
            .map_err(|err| {
                tracing::debug!(target: "spice_core", prev_hash=?block.header().prev_hash(), ?err, "failed getting uncertified_chunks");
                NoPrevUncertifiedChunks
            })?;
        let waiting_on_endorsements: HashSet<_> = prev_uncertified_chunks
            .iter()
            .flat_map(|info| {
                info.missing_endorsements
                    .iter()
                    .map(|account_id| (&info.chunk_production_key, account_id))
            })
            .collect();

        let mut in_block_endorsements: HashMap<
            &ChunkProductionKey,
            HashMap<&SpiceEndorsementSignedInner, HashMap<&AccountId, Signature>>,
        > = HashMap::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    signature,
                    signed_inner,
                } => {
                    let validator_info = tracker
                        .epoch_manager
                        .get_validator_by_account_id(&chunk_production_key.epoch_id, account_id)
                        .map_err(|error| NoValidatorForAccountId { index, error })?;

                    if !signed_inner.verify(validator_info.public_key(), signature) {
                        return Err(InvalidCoreStatement { index, reason: "invalid signature" });
                    }
                    // Checking that waiting_on_endorsements contains chunk_production_key makes
                    // sure that chunk_production_key is valid.
                    if !waiting_on_endorsements.contains(&(chunk_production_key, account_id)) {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "endorsement already included in one of the previous blocks",
                        });
                    }

                    if in_block_endorsements
                        .entry(chunk_production_key)
                        .or_default()
                        .entry(signed_inner)
                        .or_default()
                        .insert(account_id, signature.clone())
                        .is_some()
                    {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate endorsement",
                        });
                    }
                }
                SpiceCoreStatement::ChunkExecutionResult {
                    chunk_production_key,
                    execution_result,
                } => {
                    if block_execution_results
                        .insert(chunk_production_key, (execution_result, index))
                        .is_some()
                    {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate execution_result",
                        });
                    }

                    let max_endorsed_height = max_endorsed_height_created
                        .entry(chunk_production_key.shard_id)
                        .or_insert(chunk_production_key.height_created);
                    *max_endorsed_height =
                        chunk_production_key.height_created.max(*max_endorsed_height);
                }
            };
        }

        for (chunk_production_key, _) in &waiting_on_endorsements {
            if block_execution_results.contains_key(chunk_production_key) {
                continue;
            }
            let Some(max_endorsed_height_created) =
                max_endorsed_height_created.get(&chunk_production_key.shard_id)
            else {
                continue;
            };
            if chunk_production_key.height_created < *max_endorsed_height_created {
                // We cannot be waiting on an endorsement for chunk created at height that is less
                // than maximum endorsed height for the chunk as that would mean that child is
                // endorsed before parent.
                return Err(SkippedExecutionResult {
                    epoch_id: chunk_production_key.epoch_id,
                    shard_id: chunk_production_key.shard_id,
                    height_created: chunk_production_key.height_created,
                });
            }
        }

        for (chunk_production_key, mut on_chain_endorsements) in in_block_endorsements {
            let chunk_validator_assignments = tracker
                .epoch_manager
                .get_chunk_validator_assignments(
                    &chunk_production_key.epoch_id,
                    chunk_production_key.shard_id,
                    chunk_production_key.height_created,
                )
                .map_err(|error| NoValidatorAssignments {
                    epoch_id: chunk_production_key.epoch_id,
                    shard_id: chunk_production_key.shard_id,
                    height_created: chunk_production_key.height_created,
                    error,
                })?;
            let known_endorsements =
                tracker.endorsements.get(chunk_production_key).cloned().unwrap_or_default();
            for (account_id, _) in chunk_validator_assignments.assignments() {
                // It's not enough to only look at the known endorsements since the block
                // and it's ancestry may not yet contain enough signatures for certification of
                // chunk.
                if !waiting_on_endorsements.contains(&(chunk_production_key, account_id)) {
                    let (signed_inner, signature) = known_endorsements.get(account_id).expect(
                        "if we aren't waiting for endorsement in this block it should be in ancestry and known"
                    );
                    on_chain_endorsements
                        .entry(signed_inner)
                        .or_default()
                        .insert(account_id, signature.clone());
                }
            }
            for (signed_inner, validator_signatures) in on_chain_endorsements {
                let endorsement_state =
                    chunk_validator_assignments.compute_endorsement_state(validator_signatures);
                if !endorsement_state.is_endorsed {
                    continue;
                }

                let Some((execution_result, index)) =
                    block_execution_results.remove(chunk_production_key)
                else {
                    return Err(NoExecutionResultForEndorsedChunk {
                        epoch_id: chunk_production_key.epoch_id,
                        shard_id: chunk_production_key.shard_id,
                        height_created: chunk_production_key.height_created,
                    });
                };
                if execution_result.compute_hash() != signed_inner.execution_result_hash {
                    return Err(InvalidCoreStatement {
                        index,
                        reason: "endorsed execution result is different from execution result in block",
                    });
                }
            }
        }

        if !block_execution_results.is_empty() {
            let (_, index) = block_execution_results.into_values().next().unwrap();
            return Err(InvalidCoreStatement {
                index,
                reason: "execution results included without corresponding endorsement",
            });
        }
        Ok(())
    }
}

fn make_chunk_production_key(block: &Block, chunk: &ShardChunkHeader) -> ChunkProductionKey {
    ChunkProductionKey {
        shard_id: chunk.shard_id(),
        epoch_id: *block.header().epoch_id(),
        height_created: chunk.height_created(),
    }
}
