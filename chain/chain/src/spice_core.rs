use near_async::Message;
use near_async::messaging::{IntoSender as _, Sender, noop};
use near_cache::SyncLruCache;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::chunk_endorsement::{
    ChunkEndorsement, SpiceEndorsementSignedInner, SpiceEndorsementWithSignature,
};
use near_primitives::types::{
    AccountId, BlockExecutionResults, ChunkExecutionResult, ShardId, SpiceChunkId,
    SpiceUncertifiedChunkInfo,
};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, Store, StoreUpdate};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Message that should be sent once executions results for all chunks in a block are endorsed.
#[derive(Message, Debug, Clone, PartialEq)]
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
    pending_endorsements: SyncLruCache<SpiceChunkId, HashMap<AccountId, ChunkEndorsement>>,
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

    fn all_execution_results_exist(&self, block: &Block) -> Result<bool, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            if self.get_execution_result(block.hash(), shard_id)?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn save_endorsement(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
        endorsement: &SpiceEndorsementWithSignature,
    ) -> Result<StoreUpdate, std::io::Error> {
        debug_assert_eq!(block_hash, &endorsement.inner.block_hash);
        let key = get_endorsements_key(block_hash, shard_id, account_id);
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::endorsements(), &key, endorsement)?;
        Ok(store_update)
    }

    fn endorsement_exists(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
    ) -> Result<bool, std::io::Error> {
        self.chain_store
            .store()
            .exists(DBCol::endorsements(), &get_endorsements_key(block_hash, shard_id, account_id))
    }

    fn get_endorsement(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
    ) -> Result<Option<SpiceEndorsementWithSignature>, std::io::Error> {
        self.chain_store.store().get_ser(
            DBCol::endorsements(),
            &get_endorsements_key(block_hash, shard_id, &account_id),
        )
    }

    fn save_execution_result(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        execution_result: &ChunkExecutionResult,
    ) -> Result<StoreUpdate, std::io::Error> {
        let key = get_execution_results_key(block_hash, shard_id);
        let mut store_update = self.chain_store.store().store_update();
        store_update.insert_ser(DBCol::execution_results(), &key, &execution_result)?;
        Ok(store_update)
    }

    fn get_execution_result(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<Arc<ChunkExecutionResult>>, std::io::Error> {
        let key = get_execution_results_key(block_hash, shard_id);
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
        endorsements: &HashSet<(&SpiceChunkId, &AccountId)>,
        block_execution_results: &HashMap<&SpiceChunkId, &ChunkExecutionResult>,
    ) -> Result<StoreUpdate, Error> {
        let prev_hash = block.header().prev_hash();
        let mut uncertified_chunks = self.get_uncertified_chunks(prev_hash)?;
        uncertified_chunks
            .retain(|chunk_info| !block_execution_results.contains_key(&chunk_info.chunk_id));
        for chunk_info in &mut uncertified_chunks {
            chunk_info
                .missing_endorsements
                .retain(|account_id| !endorsements.contains(&(&chunk_info.chunk_id, account_id)));
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
                chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id },
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
        endorsements: Vec<ChunkEndorsement>,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<StoreUpdate, Error> {
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
            assert_eq!(shard_id, endorsement_chunk_production_key.shard_id);

            store_update.merge(self.save_endorsement(
                &block_hash,
                shard_id,
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
            &block.header().epoch_id(),
            shard_id,
            block.header().height(),
        )?;

        for (signed_inner, (execution_result, signatures)) in endorsements_by_inner {
            let mut signatures: HashMap<&AccountId, Signature> = signatures
                .iter()
                .map(|(account_id, signature)| (account_id, signature.clone()))
                .collect();
            for (account_id, _) in chunk_validator_assignments.assignments() {
                let Some(endorsement) =
                    self.get_endorsement(block.hash(), shard_id, &account_id)?
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

            store_update.merge(self.save_execution_result(
                block.hash(),
                shard_id,
                &execution_result,
            )?);
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
                    .get_or_insert_mut(
                        SpiceChunkId { block_hash, shard_id: chunk_production_key.shard_id },
                        HashMap::new,
                    )
                    .insert(endorsement.account_id().clone(), endorsement);
                return Ok(());
            }
            Err(err) => {
                return Err(err);
            }
        };

        let execution_result_is_known =
            tracker.get_execution_result(block.hash(), chunk_production_key.shard_id)?.is_some();

        // We have to make sure that endorsement is for a valid chunk since otherwise it may not be
        // garbage collected.
        let shard_layout = tracker.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        if !shard_layout.shard_ids().any(|shard_id| shard_id == chunk_production_key.shard_id) {
            tracing::error!(
                target: "spice_core",
                block_hash=?block.hash(),
                ?chunk_production_key,
                "endorsement's key is invalid: missing from related block",
            );
            return Err(Error::InvalidChunkEndorsement);
        }

        let store_update = tracker.record_chunk_endorsements_with_block(
            vec![endorsement],
            &block,
            chunk_production_key.shard_id,
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
    ) -> Result<HashMap<ShardId, Arc<ChunkExecutionResult>>, Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let mut results = HashMap::new();

        let tracker = self.read();
        let shard_layout = tracker.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let Some(result) = tracker.get_execution_result(block.hash(), shard_id)? else {
                continue;
            };
            results.insert(shard_id, result.clone());
        }
        Ok(results)
    }

    /// Returns ChunkExecutionResult for all chunks or None if at least one execution result is
    /// missing;
    pub fn get_block_execution_results(
        &self,
        block: &Block,
    ) -> Result<Option<BlockExecutionResults>, Error> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        if block.header().is_genesis() {
            return Ok(Some(BlockExecutionResults(HashMap::new())));
        }

        let mut results = HashMap::new();

        let tracker = self.read();
        let shard_layout = tracker.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let Some(result) = tracker.get_execution_result(block.hash(), shard_id)? else {
                return Ok(None);
            };
            results.insert(shard_id, result.clone());
        }
        Ok(Some(BlockExecutionResults(results)))
    }

    pub fn all_execution_results_exist(&self, block: &Block) -> Result<bool, Error> {
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
                if let Some(endorsement) = tracker.get_endorsement(
                    &chunk_info.chunk_id.block_hash,
                    chunk_info.chunk_id.shard_id,
                    &account_id,
                )? {
                    core_statements.push(SpiceCoreStatement::Endorsement {
                        account_id,
                        endorsement,
                        chunk_id: chunk_info.chunk_id.clone(),
                    });
                }
            }

            let Some(execution_result) = tracker.get_execution_result(
                &chunk_info.chunk_id.block_hash,
                chunk_info.chunk_id.shard_id,
            )?
            else {
                continue;
            };
            // Execution results are stored only for endorsed chunks.
            core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_id: chunk_info.chunk_id,
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
                SpiceCoreStatement::Endorsement { account_id, endorsement, chunk_id } => {
                    store_update.merge(tracker.save_endorsement(
                        &chunk_id.block_hash,
                        chunk_id.shard_id,
                        account_id,
                        endorsement,
                    )?);
                    endorsements.insert((chunk_id, account_id));
                }
                SpiceCoreStatement::ChunkExecutionResult { execution_result, chunk_id } => {
                    block_execution_results.insert(chunk_id, execution_result);
                    store_update.merge(tracker.save_execution_result(
                        &chunk_id.block_hash,
                        chunk_id.shard_id,
                        execution_result,
                    )?);
                }
            };
        }

        store_update.merge(tracker.record_uncertified_chunks(
            block,
            &endorsements,
            &block_execution_results,
        )?);

        let shard_layout = tracker.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let key = SpiceChunkId { block_hash: *block.hash(), shard_id };
            let Some(endorsements) = tracker.pending_endorsements.lock().pop(&key) else {
                continue;
            };
            match tracker.record_chunk_endorsements_with_block(
                endorsements.into_values().collect(),
                block,
                shard_id,
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

        fn get_block(
            store: &Store,
            block_hash: &CryptoHash,
        ) -> Result<Arc<Block>, InvalidSpiceCoreStatementsError> {
            store
                .caching_get_ser(DBCol::Block, block_hash.as_ref())
                .map_err(|error| IoError { error })?
                .ok_or(UnknownBlock { block_hash: *block_hash })
        }

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
                info.missing_endorsements.iter().map(|account_id| (&info.chunk_id, account_id))
            })
            .collect();

        let mut in_block_endorsements: HashMap<
            &SpiceChunkId,
            HashMap<SpiceEndorsementSignedInner, HashMap<&AccountId, Signature>>,
        > = HashMap::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement { chunk_id, account_id, endorsement } => {
                    // Checking contents of waiting_on_endorsements makes sure that
                    // chunk_id and account_id are valid.
                    if !waiting_on_endorsements.contains(&(chunk_id, account_id)) {
                        return Err(InvalidCoreStatement {
                            index,
                            // It can either be already included in the ancestry or be for a block
                            // outside of ancestry.
                            reason: "endorsement is irrelevant",
                        });
                    }

                    // TODO(spice): remove block hash from endorsement.inner
                    debug_assert_eq!(chunk_id.block_hash, endorsement.inner.block_hash);

                    let block = get_block(tracker.chain_store.store_ref(), &chunk_id.block_hash)?;

                    let validator_info = tracker
                        .epoch_manager
                        .get_validator_by_account_id(block.header().epoch_id(), account_id)
                        .expect("we are waiting on endorsement for this account so relevant validator has to exist");

                    if !endorsement
                        .inner
                        .verify(validator_info.public_key(), &endorsement.signature)
                    {
                        return Err(InvalidCoreStatement { index, reason: "invalid signature" });
                    }

                    if in_block_endorsements
                        .entry(chunk_id)
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
                SpiceCoreStatement::ChunkExecutionResult { chunk_id, execution_result } => {
                    if block_execution_results.insert(chunk_id, (execution_result, index)).is_some()
                    {
                        return Err(InvalidCoreStatement {
                            index,
                            reason: "duplicate execution result",
                        });
                    }

                    let block = get_block(tracker.chain_store.store_ref(), &chunk_id.block_hash)?;
                    let height = block.header().height();

                    let max_endorsed_height =
                        max_endorsed_height_created.entry(chunk_id.shard_id).or_insert(height);
                    *max_endorsed_height = height.max(*max_endorsed_height);
                }
            };
        }

        // TODO(spice): Add validation that endorsements for blocks are included only when previous
        // block is fully endorsed (as part of block we are validating or it's ancestry).
        for (chunk_id, _) in &waiting_on_endorsements {
            if block_execution_results.contains_key(chunk_id) {
                continue;
            }
            let Some(max_endorsed_height_created) =
                max_endorsed_height_created.get(&chunk_id.shard_id)
            else {
                continue;
            };
            let block = get_block(tracker.chain_store.store_ref(), &chunk_id.block_hash)?;
            let height = block.header().height();
            if height < *max_endorsed_height_created {
                // We cannot be waiting on an endorsement for chunk created at height that is less
                // than maximum endorsed height for the chunk as that would mean that child is
                // endorsed before parent.
                return Err(SkippedExecutionResult { chunk_id: (*chunk_id).clone() });
            }
        }

        for (chunk_id, mut on_chain_endorsements) in in_block_endorsements {
            let block = get_block(tracker.chain_store.store_ref(), &chunk_id.block_hash)?;
            let chunk_validator_assignments = tracker
                .epoch_manager
                .get_chunk_validator_assignments(
                    &block.header().epoch_id(),
                    chunk_id.shard_id,
                    block.header().height(),
                )
                .expect(
                    "since we are waiting for endorsement we should know it's validator assignments",
                );
            for (account_id, _) in chunk_validator_assignments.assignments() {
                // It's not enough to only look at the known endorsements since the block
                // and it's ancestry may not yet contain enough signatures for certification of
                // chunk.
                if !waiting_on_endorsements.contains(&(chunk_id, account_id)) {
                    let endorsement = tracker.get_endorsement(&chunk_id.block_hash, chunk_id.shard_id, account_id)
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

                let Some((execution_result, index)) = block_execution_results.remove(chunk_id)
                else {
                    return Err(NoExecutionResultForEndorsedChunk { chunk_id: chunk_id.clone() });
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

    pub fn endorsement_exists(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        account_id: &AccountId,
    ) -> Result<bool, std::io::Error> {
        let tracker = self.read();
        tracker.endorsement_exists(block_hash, shard_id, account_id)
    }
}
