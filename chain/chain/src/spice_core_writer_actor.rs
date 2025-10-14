use itertools::Itertools;
use near_async::Message;
use near_async::messaging::{Handler, Sender};
use near_cache::SyncLruCache;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::spice_chunk_endorsement::{
    SpiceChunkEndorsement, SpiceStoredVerifiedEndorsement, SpiceVerifiedEndorsement,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ChunkExecutionResult, ChunkExecutionResultHash, EpochId, ShardId, SpiceChunkId,
};
use near_primitives::utils::{get_endorsements_key, get_execution_results_key};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, StoreUpdate};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::spice_core::SpiceCoreReader;

/// Message that should be sent once executions results for all chunks in a block are endorsed.
#[derive(Message, Debug, Clone, PartialEq)]
pub struct ExecutionResultEndorsed {
    pub block_hash: CryptoHash,
}

/// Message that should be sent once block is processed.
#[derive(Message, Debug)]
pub struct ProcessedBlock {
    pub block_hash: CryptoHash,
}

// SpiceCoreWriterActor is the only actor that should be allowed to change spice core state to
// avoid any related race conditions.
pub struct SpiceCoreWriterActor {
    pub(crate) core_reader: SpiceCoreReader,

    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    chunk_executor_sender: Sender<ExecutionResultEndorsed>,
    spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    // Endorsements that arrived before the relevant block, so cannot be fully validated yet.
    pending_endorsements: SyncLruCache<SpiceChunkId, HashMap<AccountId, SpiceVerifiedEndorsement>>,
}

impl near_async::messaging::Actor for SpiceCoreWriterActor {}

impl Handler<ProcessedBlock> for SpiceCoreWriterActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.handle_processed_block(block_hash) {
            tracing::error!(target: "spice_core_writer", ?err, "Error handling processed block");
        }
    }
}

impl Handler<SpiceChunkEndorsementMessage> for SpiceCoreWriterActor {
    fn handle(&mut self, msg: SpiceChunkEndorsementMessage) {
        if let Err(err) = self.process_chunk_endorsement(msg.0) {
            tracing::error!(target: "spice_core_writer", ?err, "Error processing spice chunk endorsement");
        }
    }
}

impl SpiceCoreWriterActor {
    pub fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chunk_executor_sender: Sender<ExecutionResultEndorsed>,
        spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
    ) -> Self {
        const PENDING_ENDORSEMENT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
        Self {
            core_reader: SpiceCoreReader::new(chain_store.clone(), epoch_manager.clone()),
            chain_store,
            epoch_manager,
            chunk_executor_sender,
            spice_chunk_validator_sender,
            pending_endorsements: SyncLruCache::new(PENDING_ENDORSEMENT_CACHE_SIZE.into()),
        }
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

    fn try_sending_execution_result_endorsed(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(err) => return Err(err),
        };

        if self.core_reader.all_execution_results_exist(&block)? {
            let result_endorsed_message =
                ExecutionResultEndorsed { block_hash: *block.header().hash() };
            self.chunk_executor_sender.send(result_endorsed_message.clone());
            self.spice_chunk_validator_sender.send(result_endorsed_message);
        }
        Ok(())
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

    fn get_execution_result_from_store(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<Arc<ChunkExecutionResult>>, std::io::Error> {
        let key = get_execution_results_key(block_hash, shard_id);
        self.chain_store.store().caching_get_ser(DBCol::execution_results(), &key)
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
                            target: "spice_core_writer",
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

    /// Validates and records endorsement and execution result contained within.
    pub(crate) fn process_chunk_endorsement(
        &self,
        endorsement: SpiceChunkEndorsement,
    ) -> Result<(), ProcessChunkError> {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let block = match self.chain_store.get_block(endorsement.block_hash()) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(_)) => {
                tracing::debug!(
                    target: "spice_core_writer",
                    block_hash = ?endorsement.block_hash(),
                    "not processing endorsement immediately since haven't received relevant block yet"
                );
                return self
                    .add_pending_endorsement(endorsement)
                    .map_err(ProcessChunkError::InvalidPendingEndorsement);
            }
            Err(err) => {
                return Err(ProcessChunkError::GetBlock(err));
            }
        };

        let endorsement = self
            .validate_endorsement_with_block(endorsement, &block)
            .map_err(ProcessChunkError::InvalidEndorsement)?;
        let chunk_id = endorsement.chunk_id();

        let execution_result_is_known = self
            .get_execution_result_from_store(&chunk_id.block_hash, chunk_id.shard_id)?
            .is_some();

        let block_hash = chunk_id.block_hash;
        let store_update = self
            .record_chunk_endorsements_with_block(&block, vec![endorsement])
            .map_err(ProcessChunkError::RecordWithBlock)?;
        store_update.commit()?;
        // We record endorsement even when execution result is known to allow it being included on
        // chain when execution result isn't endorsed on chain yet.
        // However since we already know about execution result for this endorsement there should
        // be no need to send duplicate execution result endorsed message.
        if !execution_result_is_known {
            self.try_sending_execution_result_endorsed(&block_hash)
                .map_err(ProcessChunkError::SendingExecutionResultsEndorsed)?;
        }
        Ok(())
    }

    /// Sends notifications if an accepted block contains execution result endorsements.
    fn send_execution_result_endorsements(&self, block: &Block) {
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
            self.try_sending_execution_result_endorsed(block_hash)
                .expect("should fail only if failing to access store");
        }
    }

    fn record_block_core_statements(&self, block: &Block) -> Result<StoreUpdate, Error> {
        let mut store_update = self.chain_store.store().store_update();
        for core_statement in block.spice_core_statements() {
            match core_statement {
                SpiceCoreStatement::Endorsement(endorsement) => {
                    let chunk_id = endorsement.chunk_id();
                    // Core statement is validated as part of block validation.
                    let stored_endorsement = endorsement.unchecked_to_stored();
                    let account_id = endorsement.account_id();
                    store_update.merge(self.save_endorsement(
                        &chunk_id.block_hash,
                        chunk_id.shard_id,
                        account_id,
                        &stored_endorsement,
                    )?);
                }
                SpiceCoreStatement::ChunkExecutionResult { execution_result, chunk_id } => {
                    store_update.merge(self.save_execution_result(
                        &chunk_id.block_hash,
                        chunk_id.shard_id,
                        execution_result,
                    )?);
                }
            };
        }

        let pending_endorsements = self.pop_pending_endorsement_for_block(&block)?;
        store_update.merge(self.record_chunk_endorsements_with_block(block, pending_endorsements)?);
        Ok(store_update)
    }

    pub(crate) fn handle_processed_block(&self, block_hash: CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(&block_hash).unwrap();
        // Since block was already processed we know it's valid so can record it in core state.
        let store_update = self.record_block_core_statements(&block)?;
        store_update.commit()?;
        self.send_execution_result_endorsements(&block);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvalidSpiceEndorsementError {
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
pub(crate) enum ProcessChunkError {
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
