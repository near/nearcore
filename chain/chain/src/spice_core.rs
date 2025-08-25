use near_async::messaging::{IntoSender as _, Sender, noop};
use near_cache::SyncLruCache;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::{
    ChunkEndorsement, SpiceEndorsementSignedInner, SpiceEndorsementWithSignature,
};
use near_primitives::types::{
    AccountId, BlockExecutionResults, ChunkExecutionResult, ChunkExecutionResultHash, ShardId,
    SpiceUncertifiedChunkInfo,
};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, StoreUpdate};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Message that should be sent once executions results for all chunks in a block are endorsed.
#[derive(actix::Message, Debug, Clone, PartialEq)]
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

    fn read(&self) -> RwLockReadGuard<CoreStatementsTracker> {
        self.0.read()
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
    // Endorsements that arrived before the relevant block (so cannot be verified to be related to
    // the block yet).
    pending_endorsements: SyncLruCache<ChunkProductionKey, HashMap<AccountId, ChunkEndorsement>>,
}

impl CoreStatementsTracker {
    fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chunk_executor_sender: Sender<ExecutionResultEndorsed>,
        spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    ) -> Self {
        const PENDING_ENDORSEMENT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
        Self {
            chain_store,
            epoch_manager,
            chunk_executor_sender,
            spice_chunk_validator_sender,
            pending_endorsements: SyncLruCache::new(PENDING_ENDORSEMENT_CACHE_SIZE.into()),
        }
    }

    fn all_execution_results_exist(&self, block: &Block) -> Result<bool, std::io::Error> {
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            if self.get_execution_result(&key)?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn save_endorsement(
        &self,
        chunk_production_key: &ChunkProductionKey,
        account_id: &AccountId,
        endorsement: &SpiceEndorsementWithSignature,
    ) -> Result<StoreUpdate, std::io::Error> {
        let key = get_endorsements_key(chunk_production_key, account_id);
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::endorsements(), &key, endorsement)?;
        Ok(store_update)
    }

    fn get_endorsement(
        &self,
        chunk_production_key: &ChunkProductionKey,
        account_id: &AccountId,
    ) -> Result<Option<SpiceEndorsementWithSignature>, std::io::Error> {
        self.chain_store.store().get_ser(
            DBCol::endorsements(),
            &get_endorsements_key(&chunk_production_key, &account_id),
        )
    }

    fn save_execution_result(
        &self,
        chunk_production_key: &ChunkProductionKey,
        execution_result: &ChunkExecutionResult,
    ) -> Result<StoreUpdate, std::io::Error> {
        let key = get_execution_results_key(chunk_production_key);
        let mut store_update = self.chain_store.store().store_update();
        store_update.insert_ser(DBCol::execution_results(), &key, &execution_result)?;
        Ok(store_update)
    }

    fn get_execution_result(
        &self,
        chunk_production_key: &ChunkProductionKey,
    ) -> Result<Option<Arc<ChunkExecutionResult>>, std::io::Error> {
        let key = get_execution_results_key(chunk_production_key);
        self.chain_store.store().caching_get_ser(DBCol::execution_results(), &key)
    }

    fn get_uncertified_chunks(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
        let block = self.chain_store.get_block(block_hash)?;

        if block.header().is_genesis() || !block.is_spice_block() {
            Ok(vec![])
        } else {
            let Some(uncertified_chunks) = self
                .chain_store
                .store()
                .get_ser(DBCol::uncertified_chunks(), block_hash.as_ref())?
            else {
                debug_assert!(
                    false,
                    "spice blocks in store should always have uncertified_chunks present"
                );
                return Err(Error::Other(format!("missing uncertified chunks for {}", block_hash)));
            };

            Ok(uncertified_chunks)
        }
    }

    fn try_sending_execution_result_endorsed(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(err) => return Err(err),
        };

        if self.all_execution_results_exist(&block)? {
            let result_endorsed_message =
                ExecutionResultEndorsed { block_hash: *block.header().hash() };
            self.chunk_executor_sender.send(result_endorsed_message.clone());
            self.spice_chunk_validator_sender.send(result_endorsed_message);
        }
        Ok(())
    }

    fn record_uncertified_chunks(
        &self,
        block: &Block,
        endorsements: &HashSet<(&ChunkProductionKey, &AccountId)>,
        block_execution_results: &HashMap<&ChunkProductionKey, ChunkExecutionResultHash>,
    ) -> Result<StoreUpdate, Error> {
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

        let mut store_update = self.chain_store.store().store_update();
        store_update.insert_ser(
            DBCol::uncertified_chunks(),
            block.header().hash().as_ref(),
            &uncertified_chunks,
        )?;
        Ok(store_update)
    }

    fn record_chunk_endorsements_with_block(
        &self,
        chunk_production_key: &ChunkProductionKey,
        endorsements: Vec<ChunkEndorsement>,
        block: &Block,
    ) -> Result<StoreUpdate, Error> {
        // We have to make sure that endorsement is for a valid chunk since otherwise it may not be
        // garbage collected.
        if !block.chunks().iter_raw().any(|chunk| {
            let key = make_chunk_production_key(block, chunk);
            &key == chunk_production_key
        }) {
            tracing::error!(target: "spice_core", block_hash=?block.hash(), ?chunk_production_key, "endorsement's key is invalid: missing from related block");
            return Err(Error::InvalidChunkEndorsement);
        }

        let mut store_update = self.chain_store.store().store_update();
        let mut endorsements_by_inner: HashMap<
            SpiceEndorsementSignedInner,
            (ChunkExecutionResult, HashMap<AccountId, Signature>),
        > = HashMap::new();

        for endorsement in endorsements {
            let Some((
                endorsement_chunk_production_key,
                endorsement_account_id,
                endorsement_signed_inner,
                execution_result,
                endorsement_signature,
            )) = endorsement.spice_destructure()
            else {
                continue;
            };
            let block_hash = endorsement_signed_inner.block_hash;
            assert_eq!(&block_hash, block.header().hash());
            assert_eq!(chunk_production_key, &endorsement_chunk_production_key);

            store_update.merge(self.save_endorsement(
                &chunk_production_key,
                &endorsement_account_id,
                &SpiceEndorsementWithSignature {
                    inner: endorsement_signed_inner.clone(),
                    signature: endorsement_signature.clone(),
                },
            )?);
            endorsements_by_inner
                .entry(endorsement_signed_inner)
                .or_insert_with(|| (execution_result, HashMap::new()))
                .1
                .insert(endorsement_account_id, endorsement_signature);
        }

        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &chunk_production_key.epoch_id,
            chunk_production_key.shard_id,
            chunk_production_key.height_created,
        )?;

        for (signed_inner, (execution_result, signatures)) in endorsements_by_inner {
            let mut signatures: HashMap<&AccountId, Signature> = signatures
                .iter()
                .map(|(account_id, signature)| (account_id, signature.clone()))
                .collect();
            for (account_id, _) in chunk_validator_assignments.assignments() {
                let Some(endorsement) = self.get_endorsement(&chunk_production_key, &account_id)?
                else {
                    continue;
                };
                if endorsement.inner != signed_inner {
                    continue;
                }
                signatures.insert(account_id, endorsement.signature);
            }

            let endorsement_state =
                chunk_validator_assignments.compute_endorsement_state(signatures);

            if !endorsement_state.is_endorsed {
                continue;
            }

            store_update
                .merge(self.save_execution_result(&chunk_production_key, &execution_result)?);
        }

        return Ok(store_update);
    }
}

impl CoreStatementsProcessor {
    /// Records endorsement and execution result contained within. Endorsement should be already
    /// validated.
    pub fn record_chunk_endorsement(&self, endorsement: ChunkEndorsement) -> Result<(), Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let tracker = self.read();

        let chunk_production_key = endorsement.chunk_production_key();

        let Some(&block_hash) = endorsement.block_hash() else {
            return Ok(());
        };

        let block = match tracker.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => {
                let final_head = tracker.chain_store.final_head()?;
                if chunk_production_key.height_created <= final_head.height {
                    tracing::debug!(
                        target: "spice_core",
                        %block_hash,
                        ?chunk_production_key,
                        ?final_head,
                        "invalid endorsement: missing block with height < final head height"
                    );
                    return Err(Error::InvalidChunkEndorsement);
                }
                tracing::debug!(
                    target: "spice_core",
                    %block_hash,
                    ?chunk_production_key,
                    "not processing endorsement immediately since haven't received relevant block yet"
                );
                tracker
                    .pending_endorsements
                    .lock()
                    .get_or_insert_mut(chunk_production_key, HashMap::new)
                    .insert(endorsement.account_id().clone(), endorsement);
                return Ok(());
            }
            Err(err) => {
                return Err(err);
            }
        };

        let execution_result_is_known =
            tracker.get_execution_result(&chunk_production_key)?.is_some();

        let store_update = tracker.record_chunk_endorsements_with_block(
            &chunk_production_key,
            vec![endorsement],
            &block,
        )?;
        store_update.commit()?;
        // We record endorsement even when execution result is known to allow it being included on
        // chain when execution result isn't endorsed on chain yet.
        // However since we already know about execution result for this endorsement there should
        // be no need to send duplicate execution result endorsed message.
        if !execution_result_is_known {
            tracker.try_sending_execution_result_endorsed(&block_hash)?;
        }
        Ok(())
    }

    pub fn get_execution_results_by_shard_id(
        &self,
        block: &Block,
    ) -> Result<HashMap<ShardId, Arc<ChunkExecutionResult>>, std::io::Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let tracker = self.read();
        let mut results = HashMap::new();
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            let Some(result) = tracker.get_execution_result(&key)? else {
                continue;
            };
            let shard_id = chunk.shard_id();
            results.insert(shard_id, result.clone());
        }
        Ok(results)
    }

    /// Returns ChunkExecutionResult for all chunks or None if at least one execution result is
    /// missing;
    pub fn get_block_execution_results(
        &self,
        block: &Block,
    ) -> Result<Option<BlockExecutionResults>, std::io::Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        if block.header().is_genesis() {
            return Ok(Some(BlockExecutionResults(HashMap::new())));
        }

        let tracker = self.read();
        let mut results = HashMap::new();
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            let Some(result) = tracker.get_execution_result(&key)? else {
                return Ok(None);
            };
            results.insert(chunk.chunk_hash().clone(), result.clone());
        }
        Ok(Some(BlockExecutionResults(results)))
    }

    pub fn all_execution_results_exist(&self, block: &Block) -> Result<bool, std::io::Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let tracker = self.read();
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

        let tracker = self.read();
        let uncertified_chunks = tracker.get_uncertified_chunks(block_hash)?;

        let mut core_statements = Vec::new();
        for chunk_info in uncertified_chunks {
            for account_id in chunk_info.missing_endorsements {
                if let Some(endorsement) =
                    tracker.get_endorsement(&chunk_info.chunk_production_key, &account_id)?
                {
                    core_statements.push(SpiceCoreStatement::Endorsement {
                        chunk_production_key: chunk_info.chunk_production_key.clone(),
                        account_id,
                        endorsement,
                    });
                }
            }

            let Some(execution_result) =
                tracker.get_execution_result(&chunk_info.chunk_production_key)?
            else {
                continue;
            };
            // Execution results are stored only for endorsed chunks.
            core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_production_key: chunk_info.chunk_production_key,
                execution_result: Arc::unwrap_or_clone(execution_result),
            });
        }
        Ok(core_statements)
    }

    /// Sends notifications if an accepted block contains execution result endorsements.
    pub fn send_execution_result_endorsements(&self, block: &Block) {
        let mut execution_result_hashes = HashSet::new();
        let mut execution_result_original_block = HashMap::new();

        for core_statement in block.spice_core_statements() {
            match core_statement {
                SpiceCoreStatement::Endorsement { endorsement, .. } => {
                    execution_result_original_block.insert(
                        &endorsement.inner.execution_result_hash,
                        endorsement.inner.block_hash,
                    );
                }
                SpiceCoreStatement::ChunkExecutionResult { execution_result, .. } => {
                    execution_result_hashes.insert(execution_result.compute_hash());
                }
            };
        }

        let blocks_with_execution_results: HashSet<_> = execution_result_hashes.iter().map(|execution_result_hash| {
            // TODO(spice): to avoid this expect unite endorsements and execution results into a
            // single struct with chunk production key, non-empty vector of endorsements and
            // optional execution results.
            let block_hash = execution_result_original_block.get(execution_result_hash).expect(
                "block validation should make sure that each block contains at least one endorsement for each execution_result");
            block_hash
        }).collect();

        let tracker = self.read();
        for block_hash in blocks_with_execution_results {
            tracker
                .try_sending_execution_result_endorsed(block_hash)
                .expect("should fail only if failing to access store");
        }
    }

    pub fn record_block(&self, block: &Block) -> Result<StoreUpdate, Error> {
        let tracker = self.read();
        let mut block_execution_results = HashMap::new();
        let mut endorsements = HashSet::new();
        let mut store_update = tracker.chain_store.store().store_update();
        for core_statement in block.spice_core_statements() {
            match core_statement {
                SpiceCoreStatement::Endorsement {
                    account_id,
                    chunk_production_key,
                    endorsement,
                } => {
                    store_update.merge(tracker.save_endorsement(
                        chunk_production_key,
                        account_id,
                        endorsement,
                    )?);
                    endorsements.insert((chunk_production_key, account_id));
                }
                SpiceCoreStatement::ChunkExecutionResult {
                    chunk_production_key,
                    execution_result,
                } => {
                    store_update.merge(
                        tracker.save_execution_result(chunk_production_key, execution_result)?,
                    );
                    block_execution_results
                        .insert(chunk_production_key, execution_result.compute_hash());
                }
            };
        }

        store_update.merge(tracker.record_uncertified_chunks(
            block,
            &endorsements,
            &block_execution_results,
        )?);
        for chunk in block.chunks().iter_raw() {
            let key = make_chunk_production_key(block, chunk);
            let Some(endorsements) = tracker.pending_endorsements.lock().pop(&key) else {
                continue;
            };
            match tracker.record_chunk_endorsements_with_block(
                &key,
                endorsements.into_values().collect(),
                block,
            ) {
                Ok(update) => store_update.merge(update),
                Err(Error::InvalidChunkEndorsement) => continue,
                Err(err) => return Err(err),
            };
        }
        Ok(store_update)
    }

    pub fn validate_core_statements_in_block(
        &self,
        block: &Block,
    ) -> Result<(), InvalidSpiceCoreStatementsError> {
        use InvalidSpiceCoreStatementsError::*;

        let tracker = self.read();

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
            HashMap<SpiceEndorsementSignedInner, HashMap<&AccountId, Signature>>,
        > = HashMap::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement,
                } => {
                    let validator_info = tracker
                        .epoch_manager
                        .get_validator_by_account_id(&chunk_production_key.epoch_id, account_id)
                        .map_err(|error| NoValidatorForAccountId { index, error })?;

                    if !endorsement
                        .inner
                        .verify(validator_info.public_key(), &endorsement.signature)
                    {
                        return Err(InvalidCoreStatement { index, reason: "invalid signature" });
                    }
                    // Checking that waiting_on_endorsements contains chunk_production_key makes
                    // sure that chunk_production_key is valid.
                    if !waiting_on_endorsements.contains(&(chunk_production_key, account_id)) {
                        return Err(InvalidCoreStatement {
                            index,
                            // It can either be already included in the ancestry or be for a block
                            // outside of ancestry.
                            reason: "endorsement is irrelevant",
                        });
                    }

                    if in_block_endorsements
                        .entry(chunk_production_key)
                        .or_default()
                        .entry(endorsement.inner.clone())
                        .or_default()
                        .insert(account_id, endorsement.signature.clone())
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
                            reason: "duplicate execution result",
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

        // TODO(spice): Add validation that endorsements for blocks are included only when previous
        // block is fully endorsed (as part of block we are validating or it's ancestry).
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
                .expect(
                    "since we are waiting for endorsement we should know it's validator assignments",
                );
            for (account_id, _) in chunk_validator_assignments.assignments() {
                // It's not enough to only look at the known endorsements since the block
                // and it's ancestry may not yet contain enough signatures for certification of
                // chunk.
                if !waiting_on_endorsements.contains(&(chunk_production_key, account_id)) {
                    let endorsement = tracker.get_endorsement(chunk_production_key, account_id)
                        .expect("we cannot recover from io error")
                        .expect(
                        "if we aren't waiting for endorsement in this block it should be in ancestry and known"
                    );
                    on_chain_endorsements
                        .entry(endorsement.inner)
                        .or_default()
                        .insert(account_id, endorsement.signature.clone());
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
                reason: "execution results included without enough corresponding endorsement",
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use assert_matches::assert_matches;
    use itertools::Itertools as _;
    use near_async::time::Clock;
    use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::chunk_extra::ChunkExtra;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

    use crate::test_utils::{
        get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
    };
    use crate::{BlockProcessingArtifact, Chain, Provenance};

    use super::*;

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_non_spice_endorsement() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let epoch_id = block.header().epoch_id();
        let signer = create_test_signer(&test_validators()[0]);
        let endorsement = ChunkEndorsement::new(*epoch_id, chunk_header, &signer);
        assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_with_execution_result_already_present() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert!(execution_results.contains_key(&chunk_header.shard_id()));

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_with_unknown_old_block() {
        let (mut chain, core_processor) = setup();

        let genesis = chain.genesis_block();
        let old_block = build_block(&chain, &genesis, vec![]);

        let mut prev_block = genesis;
        while chain.chain_store().final_head().unwrap().height < old_block.header().height() {
            let block = build_block(&mut chain, &prev_block, vec![]);
            process_block(&mut chain, block.clone());
            prev_block = block;
        }
        assert_eq!(old_block.header().height(), chain.chain_store().final_head().unwrap().height);

        let chunks = old_block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &old_block, chunk_header);
        assert_matches!(
            core_processor.record_chunk_endorsement(endorsement),
            Err(Error::InvalidChunkEndorsement)
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_with_unknown_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_with_irrelevant_chunk_production_key() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let genesis_chunks = genesis.chunks();
        let genesis_chunk_header = genesis_chunks.iter_raw().next().unwrap();
        let endorsement =
            test_chunk_endorsement(&test_validators()[0], &block, genesis_chunk_header);
        assert_matches!(
            core_processor.record_chunk_endorsement(endorsement),
            Err(Error::InvalidChunkEndorsement)
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_does_not_record_result_without_enough_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();

        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert!(!execution_results.contains_key(&chunk_header.shard_id()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_does_not_record_result_with_enough_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }

        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert!(execution_results.contains_key(&chunk_header.shard_id()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_chunk_endorsement_results_endorsed_notification_is_sent() {
        let (executor_sc, mut executor_rc) = unbounded_channel();
        let (validator_sc, mut validator_rc) = unbounded_channel();
        let (mut chain, core_processor) =
            setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();

        for chunk_header in chunks.iter_raw() {
            assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));
            assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
            for validator in test_validators() {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                core_processor.record_chunk_endorsement(endorsement).unwrap();
            }
        }

        assert_eq!(
            executor_rc.try_recv().unwrap(),
            ExecutionResultEndorsed { block_hash: *block.hash() }
        );
        assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(
            validator_rc.try_recv().unwrap(),
            ExecutionResultEndorsed { block_hash: *block.hash() }
        );
        assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_execution_by_shard_id_with_no_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert_eq!(execution_results, HashMap::new());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_execution_by_shard_id_with_some_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert_eq!(execution_results.len(), 1);
        assert!(execution_results.contains_key(&chunk_header.shard_id()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_execution_by_shard_id_with_all_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();

        for chunk_header in chunks.iter_raw() {
            for validator in test_validators() {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                core_processor.record_chunk_endorsement(endorsement).unwrap();
            }
        }
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        for chunk_header in chunks.iter_raw() {
            assert!(execution_results.contains_key(&chunk_header.shard_id()));
        }
        assert_eq!(execution_results.len(), chunks.len());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_block_execution_results_for_genesis() {
        let (chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let execution_results = core_processor.get_block_execution_results(&genesis).unwrap();
        assert!(execution_results.is_some());
        assert_eq!(execution_results.unwrap().0, HashMap::new());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_block_execution_results_with_no_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let execution_results = core_processor.get_block_execution_results(&block).unwrap();
        assert!(execution_results.is_none())
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_block_execution_results_with_some_execution_results_missing() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let execution_results = core_processor.get_block_execution_results(&block).unwrap();
        assert!(execution_results.is_none())
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_get_block_execution_results_with_all_execution_results_present() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();

        for chunk_header in chunks.iter_raw() {
            for validator in test_validators() {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                core_processor.record_chunk_endorsement(endorsement).unwrap();
            }
        }
        let execution_results = core_processor.get_block_execution_results(&block).unwrap();
        assert!(execution_results.is_some());
        let execution_results = execution_results.unwrap();
        for chunk_header in chunks.iter_raw() {
            assert!(execution_results.0.contains_key(&chunk_header.chunk_hash()));
        }
        assert_eq!(execution_results.0.len(), chunks.len());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_all_execution_results_exist_when_all_exist() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();

        for chunk_header in chunks.iter_raw() {
            for validator in test_validators() {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                core_processor.record_chunk_endorsement(endorsement).unwrap();
            }
        }
        assert!(core_processor.all_execution_results_exist(&block).unwrap());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_all_execution_results_exist_when_some_are_missing() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        assert!(!core_processor.all_execution_results_exist(&block).unwrap());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_for_genesis() {
        let (chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        assert_eq!(core_processor.core_statement_for_next_block(genesis.header()).unwrap(), vec![]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_for_non_spice_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        process_block(&mut chain, block.clone());
        assert_eq!(core_processor.core_statement_for_next_block(block.header()).unwrap(), vec![]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_when_block_is_not_recorded() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        assert!(core_processor.core_statement_for_next_block(block.header()).is_err());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_no_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert_eq!(core_statements, vec![]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_new_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert_eq!(core_statements.len(), 1);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        assert_eq!(
            core_statements[0],
            SpiceCoreStatement::Endorsement {
                chunk_production_key,
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature }
            }
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_no_endorsements_for_fork_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        // We create fork of length 2 because the way we build test chunks they would be equivalent
        // in fork_block and block when fork is of length 1, so endorsements would be considered
        // valid.
        let fork_block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, fork_block.clone());
        let fork_block = build_block(&mut chain, &fork_block, vec![]);
        process_block(&mut chain, fork_block.clone());

        let fork_chunks = fork_block.chunks();
        let fork_chunk_header = fork_chunks.iter_raw().next().unwrap();
        let endorsement =
            test_chunk_endorsement(&test_validators()[0], &fork_block, fork_chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert_eq!(core_statements.len(), 0);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_new_endorsement() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert_eq!(core_statements.len(), 1);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        assert_eq!(
            core_statements[0],
            SpiceCoreStatement::Endorsement {
                chunk_production_key,
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature }
            }
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_new_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let validators = test_validators();
        for validator in &validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
        }

        let execution_result = test_execution_result_for_chunk(&chunk_header);
        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert!(core_statements.contains(&SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result,
        }));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_with_endorsements_creates_valid_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        let next_block = build_block(&chain, &block, core_statements);
        assert!(core_processor.validate_core_statements_in_block(&next_block).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_with_execution_results_creates_valid_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let validators = test_validators();
        for validator in &validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
        }

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        let next_block = build_block(&chain, &block, core_statements);
        assert!(core_processor.validate_core_statements_in_block(&next_block).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_no_already_included_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let validators = test_validators();
        for validator in &validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
        }

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        let next_block = build_block(&chain, &block, core_statements);
        process_block(&mut chain, next_block.clone());

        assert_eq!(
            core_processor.core_statement_for_next_block(next_block.header()).unwrap(),
            vec![]
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_no_already_included_endorsement() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        let next_block = build_block(&chain, &block, core_statements);
        process_block(&mut chain, next_block.clone());

        assert_eq!(
            core_processor.core_statement_for_next_block(next_block.header()).unwrap(),
            vec![]
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_no_endorsements_for_included_execution_result() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let all_validators = test_validators();
        let (last_validator, validators) = all_validators.split_last().unwrap();
        for validator in validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
        }

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        let next_block = build_block(&chain, &block, core_statements);
        process_block(&mut chain, next_block.clone());

        let endorsement = test_chunk_endorsement(&last_validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();

        assert_eq!(
            core_processor.core_statement_for_next_block(next_block.header()).unwrap(),
            vec![]
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_statement_for_next_block_contains_all_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();
        let all_validators = test_validators();
        let mut all_endorsements = Vec::new();
        for validator in all_validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            all_endorsements.push(SpiceCoreStatement::Endorsement {
                chunk_production_key,
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            });
        }

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        for endorsement in &all_endorsements {
            assert!(core_statements.contains(endorsement));
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_for_non_spice_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        assert!(core_processor.record_block(&block).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_for_block_without_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        assert!(core_processor.record_block(&block).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_for_block_with_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let (in_block_validators, in_core_validators) =
            all_validators.split_at(all_validators.len() / 2);
        assert!(in_block_validators.len() >= in_core_validators.len());

        let block_core_statements = in_block_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();

        let next_block = build_block(&mut chain, &genesis, block_core_statements);
        let store_update = core_processor.record_block(&next_block).unwrap();
        store_update.commit().unwrap();

        assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
        for validator in in_core_validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert!(execution_results.contains_key(&chunk_header.shard_id()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_for_block_with_execution_results() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let mut block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        let execution_result = test_execution_result_for_chunk(&chunk_header);
        block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: execution_result.clone(),
        });

        let next_block = build_block(&mut chain, &genesis, block_core_statements);
        let store_update = core_processor.record_block(&next_block).unwrap();
        store_update.commit().unwrap();
        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert_eq!(
            execution_results.get(&chunk_header.shard_id()),
            Some(&Arc::new(execution_result))
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_processes_pending_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }

        assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
        let store_update = core_processor.record_block(&block).unwrap();
        store_update.commit().unwrap();

        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert_eq!(
            execution_results.get(&chunk_header.shard_id()),
            Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_block_processes_pending_endorsements_with_invalid_endorsement() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let (last_validator, validators) = all_validators.split_last().unwrap();
        for validator in validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let invalid_endorsement = invalid_chunk_endorsement(&last_validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(invalid_endorsement).unwrap();

        assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
        let store_update = core_processor.record_block(&block).unwrap();
        store_update.commit().unwrap();

        let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
        assert_eq!(
            execution_results.get(&chunk_header.shard_id()),
            Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_endorsements_from_forks_can_be_used_in_other_forks() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        };

        let fork_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
        let store_update = core_processor.record_block(&fork_block).unwrap();
        store_update.commit().unwrap();

        let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
        assert_eq!(core_statements, vec![core_endorsement]);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_with_non_spice_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        assert!(core_processor.validate_core_statements_in_block(&block).is_ok());
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_parent_not_recorded() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        let next_block = build_block(&mut chain, &block, vec![]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&next_block),
            Err(InvalidSpiceCoreStatementsError::NoPrevUncertifiedChunks)
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_endorsement_with_invalid_account_id() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let invalid_validator = "invalid-validator";
        let endorsement = test_chunk_endorsement(invalid_validator, &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        };

        let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&next_block),
            Err(InvalidSpiceCoreStatementsError::NoValidatorForAccountId { index: 0, .. })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_endorsement_with_invalid_signature() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, _signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature {
                inner: SpiceEndorsementSignedInner {
                    block_hash: *block.hash(),
                    execution_result_hash: ChunkExecutionResultHash(CryptoHash::default()),
                },
                signature,
            },
        };

        let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&next_block),
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "invalid signature",
                index: 0
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_endorsement_already_included() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        };

        let next_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
        process_block(&mut chain, next_block.clone());

        let next_next_block = build_block(&mut chain, &next_block, vec![core_endorsement]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&next_next_block),
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "endorsement is irrelevant",
                index: 0
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_endorsement_for_unknown_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let next_block = build_block(&mut chain, &block, vec![]);
        let next_block_chunks = next_block.chunks();
        let next_block_chunk_header = next_block_chunks.iter_raw().next().unwrap();

        let endorsement =
            test_chunk_endorsement(&test_validators()[0], &next_block, next_block_chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        };

        let fork_block = build_block(&mut chain, &block, vec![core_endorsement]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&fork_block),
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "endorsement is irrelevant",
                index: 0
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_duplicate_endorsement() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let core_endorsement = SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        };
        let next_block =
            build_block(&mut chain, &block, vec![core_endorsement.clone(), core_endorsement]);
        assert_matches!(
            core_processor.validate_core_statements_in_block(&next_block),
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "duplicate endorsement",
                index: 1
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_duplicate_execution_result() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let mut block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        let execution_result = SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        };
        block_core_statements.push(execution_result.clone());
        block_core_statements.push(execution_result);
        let duplicate_index = block_core_statements.len() - 1;

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "duplicate execution result",
                index: _,
            })
        );
        let Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement { index, .. }) = result
        else {
            panic!()
        };
        assert_eq!(index, duplicate_index);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_child_execution_result_included_before_parent() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let parent_block = build_block(&mut chain, &genesis, vec![]);
        let parent_chunks = parent_block.chunks();
        let parent_chunk_header = parent_chunks.iter_raw().next().unwrap();
        process_block(&mut chain, parent_block.clone());
        let block = build_block(&mut chain, &parent_block, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let mut block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        let execution_result = SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        };
        block_core_statements.push(execution_result);

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::SkippedExecutionResult { .. })
        );

        let Err(InvalidSpiceCoreStatementsError::SkippedExecutionResult {
            shard_id,
            epoch_id,
            height_created,
        }) = result
        else {
            panic!();
        };
        let parent_key = make_chunk_production_key(&parent_block, &parent_chunk_header);
        assert_eq!(parent_key, ChunkProductionKey { shard_id, epoch_id, height_created });
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_execution_result_included_without_enough_endorsements()
     {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let block_core_statements = vec![
            SpiceCoreStatement::Endorsement {
                chunk_production_key,
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            },
            SpiceCoreStatement::ChunkExecutionResult {
                chunk_production_key: make_chunk_production_key(&block, chunk_header),
                execution_result: test_execution_result_for_chunk(&chunk_header),
            },
        ];

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                index: 1,
                reason: "execution results included without enough corresponding endorsement",
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_execution_result_included_without_endorsements()
    {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let block_core_statements = vec![SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        }];

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                index: 0,
                reason: "execution results included without enough corresponding endorsement",
            })
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_enough_endorsements_but_no_execution_result() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::NoExecutionResultForEndorsedChunk { .. })
        );

        let Err(InvalidSpiceCoreStatementsError::NoExecutionResultForEndorsedChunk {
            shard_id,
            epoch_id,
            height_created,
        }) = result
        else {
            panic!();
        };
        let key = make_chunk_production_key(&block, &chunk_header);
        assert_eq!(key, ChunkProductionKey { shard_id, epoch_id, height_created });
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_with_execution_result_different_from_endorsed_one() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let mut block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: invalid_execution_result_for_chunk(&chunk_header),
        });
        let execution_result_index = block_core_statements.len() - 1;

        let next_block = build_block(&mut chain, &block, block_core_statements);
        let result = core_processor.validate_core_statements_in_block(&next_block);
        assert_matches!(
            result,
            Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
                reason: "endorsed execution result is different from execution result in block",
                index: _,
            })
        );
        let Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement { index, .. }) = result
        else {
            panic!();
        };
        assert_eq!(index, execution_result_index);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_with_no_core_statements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let next_block = build_block(&mut chain, &block, vec![]);
        assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_with_not_enough_on_chain_endorsements_for_execution_result()
     {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let (in_block_validators, in_core_validators) =
            all_validators.split_at(all_validators.len() / 2);
        assert!(in_block_validators.len() >= in_core_validators.len());

        let block_core_statements = in_block_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();

        for validator in in_core_validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
        let next_block = build_block(&mut chain, &block, block_core_statements);
        assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_endorsements_without_execution_result() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let (left_validators, right_validators) = all_validators.split_at(all_validators.len() / 2);
        assert!(left_validators.len() <= right_validators.len());
        let block_core_statements = left_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();

        let next_block = build_block(&mut chain, &block, block_core_statements);
        assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_endorsements_with_execution_result() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let mut block_core_statements = all_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        });

        let next_block = build_block(&mut chain, &block, block_core_statements);
        assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_validate_core_statements_in_block_valid_execution_result_with_ancestral_endorsements() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let all_validators = test_validators();
        let (left_validators, right_validators) = all_validators.split_at(all_validators.len() / 2);
        assert!(left_validators.len() <= right_validators.len());
        let next_block_core_statements = left_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        let next_block = build_block(&chain, &block, next_block_core_statements);
        process_block(&mut chain, next_block.clone());

        let mut next_next_block_core_statements = right_validators
            .iter()
            .map(|validator| {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                }
            })
            .collect_vec();
        next_next_block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_production_key: make_chunk_production_key(&block, chunk_header),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        });

        let next_next_block = build_block(&mut chain, &next_block, next_next_block_core_statements);
        assert_matches!(core_processor.validate_core_statements_in_block(&next_next_block), Ok(()));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_send_execution_result_endorsements_with_endorsements_but_without_execution_result() {
        let (executor_sc, mut executor_rc) = unbounded_channel();
        let (validator_sc, mut validator_rc) = unbounded_channel();
        let (mut chain, core_processor) =
            setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());
        let chunks = block.chunks();
        let chunk_header = chunks.iter_raw().next().unwrap();

        let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        let block_core_statements = vec![SpiceCoreStatement::Endorsement {
            chunk_production_key,
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        }];
        let next_block = build_block(&mut chain, &block, block_core_statements);
        process_block(&mut chain, next_block.clone());
        core_processor.send_execution_result_endorsements(&next_block);
        assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_send_execution_result_endorsements_with_non_spice_block() {
        let (mut chain, core_processor) = setup();
        let genesis = chain.genesis_block();
        let block = build_non_spice_block(&mut chain, &genesis);
        // We just want to make sure we don't panic.
        core_processor.send_execution_result_endorsements(&block);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_send_execution_result_endorsements_with_endorsements_and_execution_results() {
        let (executor_sc, mut executor_rc) = unbounded_channel();
        let (validator_sc, mut validator_rc) = unbounded_channel();
        let (mut chain, core_processor) =
            setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
        let genesis = chain.genesis_block();
        let block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, block.clone());

        let mut block_core_statements = Vec::new();

        let all_validators = test_validators();
        for chunk_header in block.chunks().iter_raw() {
            for validator in &all_validators {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                block_core_statements.push(SpiceCoreStatement::Endorsement {
                    chunk_production_key,
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                });
            }
            block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_production_key: make_chunk_production_key(&block, chunk_header),
                execution_result: test_execution_result_for_chunk(&chunk_header),
            });
        }
        let next_block = build_block(&mut chain, &block, block_core_statements);
        process_block(&mut chain, next_block.clone());
        core_processor.send_execution_result_endorsements(&next_block);

        let mut executor_notifications = Vec::new();
        while let Ok(event) = executor_rc.try_recv() {
            executor_notifications.push(event);
        }
        assert_eq!(
            executor_notifications,
            vec![ExecutionResultEndorsed { block_hash: *block.hash() }]
        );

        let mut validator_notifications = Vec::new();
        while let Ok(event) = validator_rc.try_recv() {
            validator_notifications.push(event);
        }
        assert_eq!(
            validator_notifications,
            vec![ExecutionResultEndorsed { block_hash: *block.hash() }]
        );
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_send_execution_result_endorsements_with_execution_results_for_several_blocks() {
        let (executor_sc, mut executor_rc) = unbounded_channel();
        let (validator_sc, mut validator_rc) = unbounded_channel();
        let (mut chain, core_processor) =
            setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
        let genesis = chain.genesis_block();
        let parent_block = build_block(&mut chain, &genesis, vec![]);
        process_block(&mut chain, parent_block.clone());
        let current_block = build_block(&mut chain, &parent_block, vec![]);
        process_block(&mut chain, current_block.clone());

        let all_validators = test_validators();

        let mut core_statements = Vec::new();

        for block in [&parent_block, &current_block] {
            for chunk_header in block.chunks().iter_raw() {
                for validator in &all_validators {
                    let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                    let (
                        chunk_production_key,
                        account_id,
                        signed_inner,
                        _execution_result,
                        signature,
                    ) = endorsement.spice_destructure().unwrap();
                    core_statements.push(SpiceCoreStatement::Endorsement {
                        chunk_production_key,
                        account_id,
                        endorsement: SpiceEndorsementWithSignature {
                            inner: signed_inner,
                            signature,
                        },
                    });
                }
                core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                    chunk_production_key: make_chunk_production_key(&block, chunk_header),
                    execution_result: test_execution_result_for_chunk(&chunk_header),
                });
            }
        }

        let next_block = build_block(&mut chain, &current_block, core_statements);
        process_block(&mut chain, next_block.clone());
        core_processor.send_execution_result_endorsements(&next_block);

        let current_block_notification =
            ExecutionResultEndorsed { block_hash: *current_block.hash() };
        let parent_block_notification =
            ExecutionResultEndorsed { block_hash: *parent_block.hash() };

        let mut executor_notifications = Vec::new();
        while let Ok(event) = executor_rc.try_recv() {
            executor_notifications.push(event);
        }

        let mut validator_notifications = Vec::new();
        while let Ok(event) = validator_rc.try_recv() {
            validator_notifications.push(event);
        }

        assert_eq!(executor_notifications.len(), 2);
        assert!(executor_notifications.contains(&current_block_notification));
        assert!(executor_notifications.contains(&parent_block_notification));

        assert_eq!(validator_notifications.len(), 2);
        assert!(validator_notifications.contains(&current_block_notification));
        assert!(validator_notifications.contains(&parent_block_notification));
    }

    fn block_builder(chain: &Chain, prev_block: &Block) -> TestBlockBuilder {
        let block_producer = chain
            .epoch_manager
            .get_block_producer_info(
                prev_block.header().epoch_id(),
                prev_block.header().height() + 1,
            )
            .unwrap();
        let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
        TestBlockBuilder::new(Clock::real(), prev_block, signer)
            .chunks(get_fake_next_block_chunk_headers(&prev_block, chain.epoch_manager.as_ref()))
    }

    fn build_non_spice_block(chain: &Chain, prev_block: &Block) -> Arc<Block> {
        block_builder(chain, prev_block).build()
    }

    fn build_block(
        chain: &Chain,
        prev_block: &Block,
        spice_core_statements: Vec<SpiceCoreStatement>,
    ) -> Arc<Block> {
        block_builder(chain, prev_block).spice_core_statements(spice_core_statements).build()
    }

    fn process_block(chain: &mut Chain, block: Arc<Block>) {
        process_block_sync(
            chain,
            block.into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
    }

    fn test_validators() -> Vec<String> {
        (0..4).map(|i| format!("test{i}")).collect()
    }

    fn sender_from_channel<T: Send>(sc: UnboundedSender<T>) -> Sender<T> {
        Sender::from_fn(move |event| {
            sc.send(event).unwrap();
        })
    }

    fn setup() -> (Chain, CoreStatementsProcessor) {
        setup_with_senders(noop().into_sender(), noop().into_sender())
    }

    fn setup_with_senders(
        chunk_executor_sender: Sender<ExecutionResultEndorsed>,
        spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    ) -> (Chain, CoreStatementsProcessor) {
        init_test_logger();

        let num_shards = 3;

        let shard_layout = ShardLayout::multi_shard(num_shards, 0);

        let validators = test_validators();
        let validators_spec = ValidatorsSpec::desired_roles(
            &validators.iter().map(|v| v.as_str()).collect_vec(),
            &[],
        );

        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&Clock::real())
            .shard_layout(shard_layout)
            .validators_spec(validators_spec)
            .build();

        let mut chain = get_chain_with_genesis(Clock::real(), genesis);
        let core_processor = CoreStatementsProcessor::new(
            chain.chain_store().chain_store(),
            chain.epoch_manager.clone(),
            chunk_executor_sender,
            spice_chunk_validator_sender,
        );
        chain.spice_core_processor = core_processor.clone();
        (chain, core_processor)
    }

    fn test_execution_result_for_chunk(chunk_header: &ShardChunkHeader) -> ChunkExecutionResult {
        ChunkExecutionResult {
            // Using chunk_hash makes sure that each chunk has a different execution result.
            chunk_extra: ChunkExtra::new_with_only_state_root(&chunk_header.chunk_hash().0),
            outgoing_receipts_root: CryptoHash::default(),
        }
    }

    fn invalid_execution_result_for_chunk(chunk_header: &ShardChunkHeader) -> ChunkExecutionResult {
        let mut execution_result = test_execution_result_for_chunk(chunk_header);
        execution_result.outgoing_receipts_root =
            CryptoHash::from_str("32222222222233333333334444444444445555555777").unwrap();
        execution_result
    }

    fn invalid_chunk_endorsement(
        validator: &str,
        block: &Block,
        chunk_header: &ShardChunkHeader,
    ) -> ChunkEndorsement {
        let execution_result = invalid_execution_result_for_chunk(chunk_header);
        let epoch_id = block.header().epoch_id();
        let signer = create_test_signer(&validator);
        ChunkEndorsement::new_with_execution_result(
            *epoch_id,
            execution_result,
            *block.hash(),
            chunk_header,
            &signer,
        )
    }

    fn test_chunk_endorsement(
        validator: &str,
        block: &Block,
        chunk_header: &ShardChunkHeader,
    ) -> ChunkEndorsement {
        let execution_result = test_execution_result_for_chunk(chunk_header);
        let epoch_id = block.header().epoch_id();
        let signer = create_test_signer(&validator);
        ChunkEndorsement::new_with_execution_result(
            *epoch_id,
            execution_result,
            *block.hash(),
            chunk_header,
            &signer,
        )
    }
}
