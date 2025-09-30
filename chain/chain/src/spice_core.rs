use itertools::Itertools;
use near_async::Message;
use near_async::messaging::{IntoSender as _, Sender, noop};
use near_cache::SyncLruCache;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::{EpochError, InvalidSpiceCoreStatementsError};
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::spice_chunk_endorsement::{
    SpiceChunkEndorsement, SpiceStoredVerifiedEndorsement, SpiceVerifiedEndorsement,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, BlockExecutionResults, ChunkExecutionResult, ChunkExecutionResultHash, EpochId,
    ShardId, SpiceChunkId, SpiceUncertifiedChunkInfo,
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
    // Endorsements that arrived before the relevant block, so cannot be fully validated yet.
    pending_endorsements: SyncLruCache<SpiceChunkId, HashMap<AccountId, SpiceVerifiedEndorsement>>,
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
        endorsement: &SpiceStoredVerifiedEndorsement,
    ) -> Result<StoreUpdate, std::io::Error> {
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
    ) -> Result<Option<SpiceStoredVerifiedEndorsement>, std::io::Error> {
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
        block: &Block,
        endorsements: Vec<SpiceVerifiedEndorsement>,
    ) -> Result<StoreUpdate, Error> {
        let mut store_update = self.chain_store.store().store_update();
        let mut endorsements_by_unique_result: HashMap<
            (&SpiceChunkId, ChunkExecutionResultHash),
            HashMap<&AccountId, &SpiceVerifiedEndorsement>,
        > = HashMap::new();

        for endorsement in &endorsements {
            let chunk_id = endorsement.chunk_id();
            assert_eq!(&chunk_id.block_hash, block.header().hash());

            store_update.merge(self.save_endorsement(
                &chunk_id.block_hash,
                chunk_id.shard_id,
                endorsement.account_id(),
                &endorsement.to_stored(),
            )?);
            endorsements_by_unique_result
                .entry((chunk_id, endorsement.execution_result().compute_hash()))
                .or_default()
                .insert(endorsement.account_id(), &endorsement);
        }

        for ((chunk_id, chunk_execution_result_hash), endorsements) in endorsements_by_unique_result
        {
            let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                &block.header().epoch_id(),
                chunk_id.shard_id,
                block.header().height(),
            )?;

            let execution_result = endorsements
                .values()
                .next()
                .expect("at least one endorsement should always be there")
                .execution_result();
            let mut signatures: HashMap<&AccountId, Signature> = endorsements
                .iter()
                .map(|(account_id, endorsement)| (*account_id, endorsement.signature().clone()))
                .collect();
            for (account_id, _) in chunk_validator_assignments.assignments() {
                let Some(stored_endorsement) =
                    self.get_endorsement(&chunk_id.block_hash, chunk_id.shard_id, &account_id)?
                else {
                    continue;
                };
                if stored_endorsement.execution_result_hash != chunk_execution_result_hash {
                    continue;
                }
                signatures.insert(account_id, stored_endorsement.signature);
            }

            let endorsement_state =
                chunk_validator_assignments.compute_endorsement_state(signatures);

            if !endorsement_state.is_endorsed {
                continue;
            }

            assert_eq!(&chunk_id.block_hash, block.header().hash());
            store_update.merge(self.save_execution_result(
                &chunk_id.block_hash,
                chunk_id.shard_id,
                execution_result,
            )?);
        }

        return Ok(store_update);
    }

    fn validate_verified_endorsement_with_block(
        &self,
        endorsement: &SpiceVerifiedEndorsement,
        block: &Block,
    ) -> Result<(), InvalidSpiceEndorsementError> {
        assert_eq!(block.hash(), &endorsement.chunk_id().block_hash);
        let shard_layout = self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        if !shard_layout.shard_ids().contains(&endorsement.chunk_id().shard_id) {
            return Err(InvalidSpiceEndorsementError::InvalidShardId);
        }

        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &block.header().epoch_id(),
            endorsement.chunk_id().shard_id,
            block.header().height(),
        )?;

        if !chunk_validator_assignments.contains(endorsement.account_id()) {
            return Err(InvalidSpiceEndorsementError::EndorsementIsNotRelevant);
        }

        Ok(())
    }

    fn validate_endorsement_with_block(
        &self,
        endorsement: SpiceChunkEndorsement,
        block: &Block,
    ) -> Result<SpiceVerifiedEndorsement, InvalidSpiceEndorsementError> {
        assert_eq!(block.hash(), endorsement.block_hash());
        let account_id = endorsement.account_id();
        let validator = self
            .epoch_manager
            .get_validator_by_account_id(block.header().epoch_id(), account_id)?;
        let Some(endorsement) = endorsement.into_verified(validator.public_key()) else {
            return Err(InvalidSpiceEndorsementError::InvalidSignature);
        };
        self.validate_verified_endorsement_with_block(&endorsement, block)?;
        Ok(endorsement)
    }

    fn validate_endorsement_without_block(
        &self,
        endorsement: SpiceChunkEndorsement,
    ) -> Result<SpiceVerifiedEndorsement, InvalidSpiceEndorsementError> {
        use InvalidSpiceEndorsementError::*;

        let account_id = endorsement.account_id();
        // Since block is unknown it should be in the future, i.e. either same epoch as head or the
        // next epoch.
        let final_head = self.chain_store.final_head().map_err(NearChainError)?;
        let possible_epoch_ids = [final_head.epoch_id, final_head.next_epoch_id];
        let validator =
            self.get_validator_from_possible_epoch_id(&possible_epoch_ids, account_id)?;
        let Some(endorsement) = endorsement.into_verified(validator.public_key()) else {
            return Err(InvalidSignature);
        };

        let mut feasible_shard_id = false;
        for epoch_id in &possible_epoch_ids {
            let shard_layout = self.epoch_manager.get_shard_layout(epoch_id).map_err(EpochError)?;
            if shard_layout.shard_ids().contains(&endorsement.chunk_id().shard_id) {
                feasible_shard_id = true;
            }
        }
        if !feasible_shard_id {
            return Err(InvalidShardId);
        }

        // We cannot check that account is validator for relevant chunk before we know block height.
        Ok(endorsement)
    }

    fn get_validator_from_possible_epoch_id(
        &self,
        possible_epoch_ids: &[EpochId],
        account_id: &AccountId,
    ) -> Result<ValidatorStake, InvalidSpiceEndorsementError> {
        for epoch_id in possible_epoch_ids {
            if let Ok(validator) =
                self.epoch_manager.get_validator_by_account_id(&epoch_id, account_id)
            {
                return Ok(validator);
            }
        }
        Err(InvalidSpiceEndorsementError::AccountIsNotValidator)
    }

    fn pop_pending_endorsement_for_block(
        &self,
        block: &Block,
    ) -> Result<Vec<SpiceVerifiedEndorsement>, Error> {
        let mut endorsements = Vec::new();
        let shard_layout = self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
        for shard_id in shard_layout.shard_ids() {
            let Some(endorsements_map) = self
                .pending_endorsements
                .lock()
                .pop(&SpiceChunkId { block_hash: *block.hash(), shard_id })
            else {
                continue;
            };
            endorsements.extend(endorsements_map.into_values().filter(|endorsement| {
                match self.validate_verified_endorsement_with_block(endorsement, block) {
                    Ok(()) => true,
                    Err(err) => {
                        tracing::info!(
                            target: "spice_core",
                            chunk_id = ?endorsement.chunk_id(),
                            ?err,
                            "encountered invalid pending endorsement"
                        );
                        false
                    }
                }
            }));
        }
        Ok(endorsements)
    }

    fn add_pending_endorsement(
        &self,
        endorsement: SpiceChunkEndorsement,
    ) -> Result<(), InvalidSpiceEndorsementError> {
        let endorsement = self.validate_endorsement_without_block(endorsement)?;
        self.pending_endorsements
            .lock()
            .get_or_insert_mut(endorsement.chunk_id().clone(), HashMap::new)
            .insert(endorsement.account_id().clone(), endorsement);
        Ok(())
    }
}

impl CoreStatementsProcessor {
    /// Validates and records endorsement and execution result contained within. Endorsement should
    /// be already validated.
    pub fn process_chunk_endorsement(
        &self,
        endorsement: SpiceChunkEndorsement,
    ) -> Result<(), ProcessChunkError> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let tracker = self.read();

        let block = match tracker.chain_store.get_block(endorsement.block_hash()) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => {
                tracing::debug!(
                    target: "spice_core",
                    block_hash = ?endorsement.block_hash(),
                    "not processing endorsement immediately since haven't received relevant block yet"
                );
                return tracker
                    .add_pending_endorsement(endorsement)
                    .map_err(ProcessChunkError::InvalidPendingEndorsement);
            }
            Err(err) => {
                return Err(ProcessChunkError::GetBlock(err));
            }
        };

        let endorsement = tracker
            .validate_endorsement_with_block(endorsement, &block)
            .map_err(ProcessChunkError::InvalidEndorsement)?;
        let chunk_id = endorsement.chunk_id();

        let execution_result_is_known =
            tracker.get_execution_result(&chunk_id.block_hash, chunk_id.shard_id)?.is_some();

        let block_hash = chunk_id.block_hash;
        let store_update = tracker
            .record_chunk_endorsements_with_block(&block, vec![endorsement])
            .map_err(ProcessChunkError::RecordWithBlock)?;
        store_update.commit()?;
        // We record endorsement even when execution result is known to allow it being included on
        // chain when execution result isn't endorsed on chain yet.
        // However since we already know about execution result for this endorsement there should
        // be no need to send duplicate execution result endorsed message.
        if !execution_result_is_known {
            tracker
                .try_sending_execution_result_endorsed(&block_hash)
                .map_err(ProcessChunkError::SendingExecutionResultsEndorsed)?;
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
                    core_statements.push(
                        endorsement.into_core_statement(chunk_info.chunk_id.clone(), account_id),
                    );
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
        let tracker = self.read();
        let mut block_hashes = HashSet::new();
        for core_statement in block.spice_core_statements() {
            let SpiceCoreStatement::ChunkExecutionResult { execution_result: _, chunk_id } =
                core_statement
            else {
                continue;
            };
            block_hashes.insert(&chunk_id.block_hash);
        }
        for block_hash in block_hashes {
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
                SpiceCoreStatement::Endorsement(endorsement) => {
                    let chunk_id = endorsement.chunk_id();
                    // Core statement is validated as part of block validation.
                    let stored_endorsement = endorsement.unchecked_to_stored();
                    let account_id = endorsement.account_id();
                    store_update.merge(tracker.save_endorsement(
                        &chunk_id.block_hash,
                        chunk_id.shard_id,
                        account_id,
                        &stored_endorsement,
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

        let pending_endorsements = tracker.pop_pending_endorsement_for_block(&block)?;
        store_update
            .merge(tracker.record_chunk_endorsements_with_block(block, pending_endorsements)?);
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
            HashMap<ChunkExecutionResultHash, HashMap<&AccountId, Signature>>,
        > = HashMap::new();
        let mut block_execution_results = HashMap::new();
        let mut max_endorsed_height_created = HashMap::new();

        for (index, core_statement) in block.spice_core_statements().iter().enumerate() {
            match core_statement {
                SpiceCoreStatement::Endorsement(endorsement) => {
                    let chunk_id = endorsement.chunk_id();
                    let account_id = endorsement.account_id();
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

                    let block = get_block(tracker.chain_store.store_ref(), &chunk_id.block_hash)?;

                    let validator_info = tracker
                        .epoch_manager
                        .get_validator_by_account_id(block.header().epoch_id(), account_id)
                        .expect("we are waiting on endorsement for this account so relevant validator has to exist");

                    let Some((signed_data, signature)) =
                        endorsement.verified_signed_data(validator_info.public_key())
                    else {
                        return Err(InvalidCoreStatement { index, reason: "invalid signature" });
                    };

                    if in_block_endorsements
                        .entry(chunk_id)
                        .or_default()
                        .entry(signed_data.execution_result_hash.clone())
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
                        .entry(endorsement.execution_result_hash)
                        .or_default()
                        .insert(account_id, endorsement.signature);
                }
            }
            for (execution_result_hash, validator_signatures) in on_chain_endorsements {
                let endorsement_state =
                    chunk_validator_assignments.compute_endorsement_state(validator_signatures);
                if !endorsement_state.is_endorsed {
                    continue;
                }

                let Some((execution_result, index)) = block_execution_results.remove(chunk_id)
                else {
                    return Err(NoExecutionResultForEndorsedChunk { chunk_id: chunk_id.clone() });
                };
                if execution_result.compute_hash() != execution_result_hash {
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

#[derive(Debug, thiserror::Error)]
pub enum InvalidSpiceEndorsementError {
    #[error("account is not validator")]
    AccountIsNotValidator,
    #[error("endorsement from account is not relevant")]
    EndorsementIsNotRelevant,
    #[error("data doesn't match signature")]
    InvalidSignature,
    #[error("shard id is invalid")]
    InvalidShardId,
    #[error("failed to evaluate validity due to epoch error")]
    EpochError(EpochError),
    #[error("failed to evaluate validity due to near chain error")]
    NearChainError(Error),
}

impl From<EpochError> for InvalidSpiceEndorsementError {
    fn from(err: EpochError) -> Self {
        match err {
            EpochError::NotAValidator(..) => Self::AccountIsNotValidator,
            _ => Self::EpochError(err),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessChunkError {
    #[error("invalid spice endorsement")]
    InvalidEndorsement(InvalidSpiceEndorsementError),
    #[error("invalid spice endorsement though no relevant block is available yet")]
    InvalidPendingEndorsement(InvalidSpiceEndorsementError),
    #[error("failed when recording endorsements with block")]
    RecordWithBlock(Error),
    #[error("failed when sending execution results endorsed message")]
    SendingExecutionResultsEndorsed(Error),
    #[error("failed trying to get block for endorsement")]
    GetBlock(Error),
    #[error("io error")]
    IoError(#[from] std::io::Error),
}
