pub mod orphan_witness_handling;
pub mod orphan_witness_pool;

use super::processing_tracker::ProcessingDoneTracker;
use crate::stateless_validation::chunk_endorsement_tracker::ChunkEndorsementTracker;
use crate::{metrics, Client};
use itertools::Itertools;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Sender};
use near_chain::chain::{
    apply_new_chunk, apply_old_chunk, NewChunkData, NewChunkResult, OldChunkData, OldChunkResult,
    ShardContext, StorageContext,
};
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, PrepareTransactionsChunkContext,
    PreparedTransactions, RuntimeAdapter, RuntimeStorageConfig, StorageDataSource,
};
use near_chain::validate::{
    validate_chunk_with_chunk_extra, validate_chunk_with_chunk_extra_and_receipts_root,
};
use near_chain::{Block, Chain, ChainStoreAccess};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_pool::TransactionGroupIteratorWrapper;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader};
use near_primitives::stateless_validation::{
    ChunkEndorsement, ChunkStateWitness, ChunkStateWitnessAck, ChunkStateWitnessSize,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::ShardId;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::{PartialStorage, ShardUId};
use near_vm_runner::logic::ProtocolVersion;
use orphan_witness_pool::OrphanStateWitnessPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// After validating a chunk state witness, we ideally need to send the chunk endorsement
// to just the next block producer at height h. However, it's possible that blocks at height
// h may be skipped and block producer at height h+1 picks up the chunk. We need to ensure
// that these later block producers also receive the chunk endorsement.
// Keeping a threshold of 5 block producers should be sufficient for most scenarios.
const NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT: u64 = 5;

/// The number of state witness validation results to cache per shard.
/// This number needs to be small because result contains outgoing receipts, which can be large.
const NUM_WITNESS_RESULT_CACHE_ENTRIES: usize = 20;

#[derive(Clone)]
pub struct ChunkStateWitnessValidationResult {
    pub chunk_extra: ChunkExtra,
    pub outgoing_receipts: Vec<Receipt>,
}

pub type MainStateTransitionCache =
    Arc<Mutex<HashMap<ShardUId, LruCache<CryptoHash, ChunkStateWitnessValidationResult>>>>;

/// A module that handles chunk validation logic. Chunk validation refers to a
/// critical process of stateless validation, where chunk validators (certain
/// validators selected to validate the chunk) verify that the chunk's state
/// witness is correct, and then send chunk endorsements to the block producer
/// so that the chunk can be included in the block.
pub struct ChunkValidator {
    /// The signer for our own node, if we are a validator. If not, this is None.
    my_signer: Option<Arc<dyn ValidatorSigner>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_sender: Sender<PeerManagerMessageRequest>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
    orphan_witness_pool: OrphanStateWitnessPool,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
    main_state_transition_result_cache: MainStateTransitionCache,
    /// If true, a chunk-witness validation error will lead to a panic.
    /// This is used for non-production environments, eg. mocknet and localnet,
    /// to quickly detect issues in validation code, and must NOT be set to true
    /// for mainnet and testnet.
    panic_on_validation_error: bool,
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
        orphan_witness_pool_size: usize,
        validation_spawner: Arc<dyn AsyncComputationSpawner>,
        panic_on_validation_error: bool,
    ) -> Self {
        Self {
            my_signer,
            epoch_manager,
            network_sender,
            runtime_adapter,
            chunk_endorsement_tracker,
            orphan_witness_pool: OrphanStateWitnessPool::new(orphan_witness_pool_size),
            validation_spawner,
            main_state_transition_result_cache: MainStateTransitionCache::default(),
            panic_on_validation_error,
        }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    /// The chunk is validated asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        chain: &Chain,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let prev_block_hash = state_witness.chunk_header.prev_block_hash();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        if epoch_id != state_witness.epoch_id {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Invalid EpochId {:?} for previous block {}, expected {:?}",
                state_witness.epoch_id, prev_block_hash, epoch_id
            )));
        }

        let pre_validation_result = pre_validate_chunk_state_witness(
            &state_witness,
            chain,
            self.epoch_manager.as_ref(),
            self.runtime_adapter.as_ref(),
        )?;

        let chunk_header = state_witness.chunk_header.clone();
        let network_sender = self.network_sender.clone();
        let signer = self.my_signer.as_ref().ok_or(Error::NotAValidator)?.clone();
        let chunk_endorsement_tracker = self.chunk_endorsement_tracker.clone();
        let epoch_manager = self.epoch_manager.clone();
        // If we have the chunk extra for the previous block, we can validate the chunk without state witness.
        // This usually happens because we are a chunk producer and
        // therefore have the chunk extra for the previous block saved on disk.
        // We can also skip validating the chunk state witness in this case.
        let prev_block = chain.get_block(prev_block_hash)?;
        let last_header = Chain::get_prev_chunk_header(
            epoch_manager.as_ref(),
            &prev_block,
            chunk_header.shard_id(),
        )?;
        let shard_uid = epoch_manager.shard_id_to_uid(last_header.shard_id(), &epoch_id)?;
        let panic_on_validation_error = self.panic_on_validation_error;

        if let Ok(prev_chunk_extra) = chain.get_chunk_extra(prev_block_hash, &shard_uid) {
            match validate_chunk_with_chunk_extra(
                chain.chain_store(),
                self.epoch_manager.as_ref(),
                prev_block_hash,
                &prev_chunk_extra,
                last_header.height_included(),
                &chunk_header,
            ) {
                Ok(()) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        epoch_manager.as_ref(),
                        signer.as_ref(),
                        &network_sender,
                        chunk_endorsement_tracker.as_ref(),
                    );
                    return Ok(());
                }
                Err(err) => {
                    if panic_on_validation_error {
                        panic!("Failed to validate chunk using existing chunk extra: {:?}", err);
                    } else {
                        tracing::error!(
                            "Failed to validate chunk using existing chunk extra: {:?}",
                            err
                        );
                    }
                    return Err(err);
                }
            }
        }

        let runtime_adapter = self.runtime_adapter.clone();
        let cache = self.main_state_transition_result_cache.clone();
        self.validation_spawner.spawn("stateless_validation", move || {
            // processing_done_tracker must survive until the processing is finished.
            let _processing_done_tracker_capture: Option<ProcessingDoneTracker> =
                processing_done_tracker;

            match validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &cache,
            ) {
                Ok(()) => {
                    send_chunk_endorsement_to_block_producers(
                        &chunk_header,
                        epoch_manager.as_ref(),
                        signer.as_ref(),
                        &network_sender,
                        chunk_endorsement_tracker.as_ref(),
                    );
                }
                Err(err) => {
                    if panic_on_validation_error {
                        panic!("Failed to validate chunk: {:?}", err);
                    } else {
                        tracing::error!("Failed to validate chunk: {:?}", err);
                    }
                }
            }
        });
        Ok(())
    }

    /// TESTING ONLY: Used to override the value of panic_on_validation_error, for example,
    /// when the chunks validation errors are expected when testing adversarial behavior and
    /// the test should not panic for the invalid chunks witnesses.
    #[cfg(feature = "test_features")]
    pub fn set_should_panic_on_validation_error(&mut self, value: bool) {
        self.panic_on_validation_error = value;
    }
}

/// Checks that proposed `transactions` are valid for a chunk with `chunk_header`.
/// Uses `storage_config` to possibly record reads or use recorded storage.
pub(crate) fn validate_prepared_transactions(
    chain: &Chain,
    runtime_adapter: &dyn RuntimeAdapter,
    chunk_header: &ShardChunkHeader,
    storage_config: RuntimeStorageConfig,
    transactions: &[SignedTransaction],
    last_chunk_transactions: &[SignedTransaction],
) -> Result<PreparedTransactions, Error> {
    let parent_block = chain.chain_store().get_block(chunk_header.prev_block_hash())?;
    let last_chunk_transactions_size = borsh::to_vec(last_chunk_transactions)?.len();
    runtime_adapter.prepare_transactions(
        storage_config,
        PrepareTransactionsChunkContext {
            shard_id: chunk_header.shard_id(),
            gas_limit: chunk_header.gas_limit(),
            last_chunk_transactions_size,
        },
        (&parent_block).into(),
        &mut TransactionGroupIteratorWrapper::new(transactions),
        &mut chain.transaction_validity_check(parent_block.header().clone()),
        None,
    )
}

/// Pre-validates the chunk's receipts and transactions against the chain.
/// We do this before handing off the computationally intensive part to a
/// validation thread.
pub(crate) fn pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    chain: &Chain,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<PreValidationOutput, Error> {
    let store = chain.chain_store();
    let shard_id = state_witness.chunk_header.shard_id();

    // First, go back through the blockchain history to locate the last new chunk
    // and last last new chunk for the shard.

    // Blocks from the last new chunk (exclusive) to the parent block (inclusive).
    let mut blocks_after_last_chunk = Vec::new();
    // Blocks from the last last new chunk (exclusive) to the last new chunk (inclusive).
    let mut blocks_after_last_last_chunk = Vec::new();

    {
        let mut block_hash = *state_witness.chunk_header.prev_block_hash();
        let mut prev_chunks_seen = 0;
        loop {
            let block = store.get_block(&block_hash)?;
            let chunks = block.chunks();
            let Some(chunk) = chunks.get(shard_id as usize) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Shard {} does not exist in block {:?}",
                    shard_id, block_hash
                )));
            };
            let is_new_chunk = chunk.is_new_chunk(block.header().height());
            let is_genesis = block.header().is_genesis();
            block_hash = *block.header().prev_hash();
            if is_new_chunk {
                prev_chunks_seen += 1;
            }
            if prev_chunks_seen == 0 {
                blocks_after_last_chunk.push(block);
            } else if prev_chunks_seen == 1 {
                blocks_after_last_last_chunk.push(block);
            }
            if prev_chunks_seen == 2 || is_genesis {
                break;
            }
        }
    }

    let receipts_to_apply = validate_source_receipt_proofs(
        &state_witness.source_receipt_proofs,
        &blocks_after_last_last_chunk,
        shard_id,
    )?;
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if applied_receipts_hash != state_witness.applied_receipts_hash {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash, state_witness.applied_receipts_hash
        )));
    }
    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions);
    let last_chunk_block = blocks_after_last_last_chunk.first().ok_or_else(|| {
        Error::Other("blocks_after_last_last_chunk is empty, this should be impossible!".into())
    })?;
    let last_new_chunk_tx_root =
        last_chunk_block.chunks().get(shard_id as usize).unwrap().tx_root();
    if last_new_chunk_tx_root != tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness, last_new_chunk_tx_root
        )));
    }

    // Verify that all proposed transactions are valid.
    let new_transactions = &state_witness.new_transactions;
    if !new_transactions.is_empty() {
        let transactions_validation_storage_config = RuntimeStorageConfig {
            state_root: state_witness.chunk_header.prev_state_root(),
            use_flat_storage: true,
            source: StorageDataSource::Recorded(PartialStorage {
                nodes: state_witness.new_transactions_validation_state.clone(),
            }),
            state_patch: Default::default(),
        };

        match validate_prepared_transactions(
            chain,
            runtime_adapter,
            &state_witness.chunk_header,
            transactions_validation_storage_config,
            &new_transactions,
            &state_witness.transactions,
        ) {
            Ok(result) => {
                if result.transactions.len() != new_transactions.len() {
                    return Err(Error::InvalidChunkStateWitness(format!(
                        "New transactions validation failed. {} transactions out of {} proposed transactions were valid.",
                        result.transactions.len(),
                        new_transactions.len(),
                    )));
                }
            }
            Err(error) => {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "New transactions validation failed: {}",
                    error,
                )));
            }
        };
    }

    let main_transition_params = if last_chunk_block.header().is_genesis() {
        let epoch_id = last_chunk_block.header().epoch_id();
        let congestion_info = last_chunk_block
            .shards_congestion_info()
            .get(&shard_id)
            .map(|info| info.congestion_info);
        let genesis_protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let chunk_extra =
            chain.genesis_chunk_extra(shard_id, genesis_protocol_version, congestion_info)?;
        MainTransition::Genesis { chunk_extra, block_hash: *last_chunk_block.hash(), shard_id }
    } else {
        MainTransition::NewChunk(NewChunkData {
            chunk_header: last_chunk_block.chunks().get(shard_id as usize).unwrap().clone(),
            transactions: state_witness.transactions.clone(),
            receipts: receipts_to_apply,
            resharding_state_roots: None,
            block: Chain::get_apply_chunk_block_context(
                epoch_manager,
                last_chunk_block,
                &store.get_block_header(last_chunk_block.header().prev_hash())?,
                true,
            )?,
            is_first_block_with_chunk_of_version: false,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: state_witness.main_state_transition.base_state.clone(),
                }),
                state_patch: Default::default(),
            },
        })
    };

    Ok(PreValidationOutput {
        main_transition_params,
        implicit_transition_params: blocks_after_last_chunk
            .into_iter()
            .rev()
            .map(|block| -> Result<_, Error> {
                Ok(Chain::get_apply_chunk_block_context(
                    epoch_manager,
                    &block,
                    &store.get_block_header(block.header().prev_hash())?,
                    false,
                )?)
            })
            .collect::<Result<_, _>>()?,
    })
}

/// Validate that receipt proofs contain the receipts that should be applied during the
/// transition proven by ChunkStateWitness. The receipts are extracted from the proofs
/// and arranged in the order in which they should be applied during the transition.
/// TODO(resharding): Handle resharding properly. If the receipts were sent from before
/// a resharding boundary, we should first validate the proof using the pre-resharding
/// target_shard_id and then extract the receipts that are targeted at this half of a split shard.
fn validate_source_receipt_proofs(
    source_receipt_proofs: &HashMap<ChunkHash, ReceiptProof>,
    receipt_source_blocks: &[Block],
    target_chunk_shard_id: ShardId,
) -> Result<Vec<Receipt>, Error> {
    if receipt_source_blocks.iter().any(|block| block.header().is_genesis()) {
        if receipt_source_blocks.len() != 1 {
            return Err(Error::Other(
                "Invalid chain state: receipt_source_blocks should not have any blocks alongside genesis".to_owned()
            ));
        }
        if !source_receipt_proofs.is_empty() {
            return Err(Error::InvalidChunkStateWitness(format!(
                "genesis source_receipt_proofs should be empty, actual len is {}",
                source_receipt_proofs.len()
            )));
        }
        return Ok(vec![]);
    }

    let mut receipts_to_apply = Vec::new();
    let mut expected_proofs_len = 0;

    // Iterate over blocks between last_chunk_block (inclusive) and last_last_chunk_block (exclusive),
    // from the newest blocks to the oldest.
    for block in receipt_source_blocks {
        // Collect all receipts coming from this block.
        let mut block_receipt_proofs = Vec::new();

        for chunk in block.chunks().iter() {
            if !chunk.is_new_chunk(block.header().height()) {
                continue;
            }

            // Collect receipts coming from this chunk and validate that they are correct.
            let Some(receipt_proof) = source_receipt_proofs.get(&chunk.chunk_hash()) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Missing source receipt proof for chunk {:?}",
                    chunk.chunk_hash()
                )));
            };
            validate_receipt_proof(receipt_proof, chunk, target_chunk_shard_id)?;

            expected_proofs_len += 1;
            block_receipt_proofs.push(receipt_proof);
        }

        // Arrange the receipts in the order in which they should be applied.
        shuffle_receipt_proofs(&mut block_receipt_proofs, block.hash());
        for proof in block_receipt_proofs {
            receipts_to_apply.extend(proof.0.iter().cloned());
        }
    }

    // Check that there are no extraneous proofs in source_receipt_proofs.
    if source_receipt_proofs.len() != expected_proofs_len {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains too many proofs. Expected {} proofs, found {}",
            expected_proofs_len,
            source_receipt_proofs.len()
        )));
    }
    Ok(receipts_to_apply)
}

fn validate_receipt_proof(
    receipt_proof: &ReceiptProof,
    from_chunk: &ShardChunkHeader,
    target_chunk_shard_id: ShardId,
) -> Result<(), Error> {
    // Validate that from_shard_id is correct. The receipts must match the outgoing receipt root
    // for this shard, so it's impossible to fake it.
    if receipt_proof.1.from_shard_id != from_chunk.shard_id() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof for chunk {:?} is from shard {}, expected shard {}",
            from_chunk.chunk_hash(),
            receipt_proof.1.from_shard_id,
            from_chunk.shard_id(),
        )));
    }
    // Validate that to_shard_id is correct. to_shard_id is also encoded in the merkle tree,
    // so it's impossible to fake it.
    if receipt_proof.1.to_shard_id != target_chunk_shard_id {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof for chunk {:?} is for shard {}, expected shard {}",
            from_chunk.chunk_hash(),
            receipt_proof.1.to_shard_id,
            target_chunk_shard_id
        )));
    }
    // Verify that (receipts, to_shard_id) belongs to the merkle tree of outgoing receipts in from_chunk.
    if !receipt_proof.verify_against_receipt_root(from_chunk.prev_outgoing_receipts_root()) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipt proof for chunk {:?} has invalid merkle path, doesn't match outgoing receipts root",
            from_chunk.chunk_hash()
        )));
    }
    Ok(())
}

#[allow(clippy::large_enum_variant)]
enum MainTransition {
    Genesis { chunk_extra: ChunkExtra, block_hash: CryptoHash, shard_id: ShardId },
    NewChunk(NewChunkData),
}

impl MainTransition {
    fn block_hash(&self) -> CryptoHash {
        match self {
            Self::Genesis { block_hash, .. } => *block_hash,
            Self::NewChunk(data) => data.block.block_hash,
        }
    }

    fn shard_id(&self) -> ShardId {
        match self {
            Self::Genesis { shard_id, .. } => *shard_id,
            Self::NewChunk(data) => data.chunk_header.shard_id(),
        }
    }
}

pub(crate) struct PreValidationOutput {
    main_transition_params: MainTransition,
    implicit_transition_params: Vec<ApplyChunkBlockContext>,
}

pub(crate) fn validate_chunk_state_witness(
    state_witness: ChunkStateWitness,
    pre_validation_output: PreValidationOutput,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
    main_state_transition_cache: &MainStateTransitionCache,
) -> Result<(), Error> {
    let _timer = metrics::CHUNK_STATE_WITNESS_VALIDATION_TIME
        .with_label_values(&[&state_witness.chunk_header.shard_id().to_string()])
        .start_timer();
    let span = tracing::debug_span!(target: "client", "validate_chunk_state_witness").entered();
    let block_hash = pre_validation_output.main_transition_params.block_hash();
    let epoch_id = epoch_manager.get_epoch_id(&block_hash)?;
    let shard_uid = epoch_manager
        .shard_id_to_uid(pre_validation_output.main_transition_params.shard_id(), &epoch_id)?;
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    let cache_result = {
        let mut shard_cache = main_state_transition_cache.lock().unwrap();
        shard_cache.get_mut(&shard_uid).and_then(|cache| cache.get(&block_hash).cloned())
    };
    let (mut chunk_extra, outgoing_receipts) =
        match (pre_validation_output.main_transition_params, cache_result) {
            (MainTransition::Genesis { chunk_extra, .. }, _) => (chunk_extra, vec![]),
            (MainTransition::NewChunk(new_chunk_data), None) => {
                let chunk_header = new_chunk_data.chunk_header.clone();
                let NewChunkResult { apply_result: mut main_apply_result, .. } = apply_new_chunk(
                    ApplyChunkReason::ValidateChunkStateWitness,
                    &span,
                    new_chunk_data,
                    ShardContext {
                        shard_uid,
                        cares_about_shard_this_epoch: true,
                        will_shard_layout_change: false,
                        should_apply_chunk: true,
                        need_to_reshard: false,
                    },
                    runtime_adapter,
                    epoch_manager,
                )?;
                let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
                let chunk_extra =
                    apply_result_to_chunk_extra(protocol_version, main_apply_result, &chunk_header);

                (chunk_extra, outgoing_receipts)
            }
            (_, Some(result)) => (result.chunk_extra, result.outgoing_receipts),
        };
    if chunk_extra.state_root() != &state_witness.main_state_transition.post_state_root {
        // This is an early check, it's not for correctness, only for better
        // error reporting in case of an invalid state witness due to a bug.
        // Only the final state root check against the chunk header is required.
        return Err(Error::InvalidChunkStateWitness(format!(
            "Post state root {:?} for main transition does not match expected post state root {:?}",
            chunk_extra.state_root(),
            state_witness.main_state_transition.post_state_root,
        )));
    }

    // Compute receipt hashes here to avoid copying receipts
    let outgoing_receipts_hashes = {
        let shard_layout = epoch_manager
            .get_shard_layout_from_prev_block(state_witness.chunk_header.prev_block_hash())?;
        Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)
    };
    // Save main state transition result to cache.
    {
        let mut shard_cache = main_state_transition_cache.lock().unwrap();
        let cache = shard_cache
            .entry(shard_uid)
            .or_insert_with(|| LruCache::new(NUM_WITNESS_RESULT_CACHE_ENTRIES));
        cache.put(
            block_hash,
            ChunkStateWitnessValidationResult {
                chunk_extra: chunk_extra.clone(),
                outgoing_receipts: outgoing_receipts,
            },
        );
    }

    for (block, transition) in pre_validation_output
        .implicit_transition_params
        .into_iter()
        .zip(state_witness.implicit_transitions.into_iter())
    {
        let block_hash = block.block_hash;
        let old_chunk_data = OldChunkData {
            prev_chunk_extra: chunk_extra.clone(),
            resharding_state_roots: None,
            block,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: transition.base_state,
                }),
                state_patch: Default::default(),
            },
        };
        let OldChunkResult { apply_result, .. } = apply_old_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &span,
            old_chunk_data,
            ShardContext {
                // Consider other shard uid in case of resharding.
                shard_uid,
                cares_about_shard_this_epoch: true,
                will_shard_layout_change: false,
                should_apply_chunk: false,
                need_to_reshard: false,
            },
            runtime_adapter,
            epoch_manager,
        )?;
        *chunk_extra.state_root_mut() = apply_result.new_root;
        if chunk_extra.state_root() != &transition.post_state_root {
            // This is an early check, it's not for correctness, only for better
            // error reporting in case of an invalid state witness due to a bug.
            // Only the final state root check against the chunk header is required.
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at block {:?}, does not match expected state root {:?}",
                chunk_extra.state_root(), block_hash, transition.post_state_root
            )));
        }
    }

    // Finally, verify that the newly proposed chunk matches everything we have computed.
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);
    validate_chunk_with_chunk_extra_and_receipts_root(
        &chunk_extra,
        &state_witness.chunk_header,
        &outgoing_receipts_root,
        protocol_version,
    )?;

    Ok(())
}

fn apply_result_to_chunk_extra(
    protocol_version: ProtocolVersion,
    apply_result: ApplyChunkResult,
    chunk: &ShardChunkHeader,
) -> ChunkExtra {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    ChunkExtra::new(
        protocol_version,
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        chunk.gas_limit(),
        apply_result.total_balance_burnt,
        apply_result.congestion_info,
    )
}

pub(crate) fn send_chunk_endorsement_to_block_producers(
    chunk_header: &ShardChunkHeader,
    epoch_manager: &dyn EpochManagerAdapter,
    signer: &dyn ValidatorSigner,
    network_sender: &Sender<PeerManagerMessageRequest>,
    chunk_endorsement_tracker: &ChunkEndorsementTracker,
) {
    let epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash()).unwrap();

    // Send the chunk endorsement to the next NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT block producers.
    // It's possible we may reach the end of the epoch, in which case, ignore the error from get_block_producer.
    let block_height = chunk_header.height_created();
    let block_producers = (0..NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT)
        .map_while(|i| epoch_manager.get_block_producer(&epoch_id, block_height + i).ok())
        .collect_vec();
    assert!(!block_producers.is_empty());

    let chunk_hash = chunk_header.chunk_hash();
    tracing::debug!(
        target: "client",
        chunk_hash=?chunk_hash,
        ?block_producers,
        "send_chunk_endorsement",
    );

    let endorsement = ChunkEndorsement::new(chunk_header.chunk_hash(), signer);
    for block_producer in block_producers {
        if signer.validator_id() == &block_producer {
            // Unwrap here as we always expect our own endorsements to be valid
            chunk_endorsement_tracker
                .process_chunk_endorsement(chunk_header, endorsement.clone())
                .unwrap();
        } else {
            network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
            ));
        }
    }
}

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    /// State witness is processed asynchronously, if you want to wait for the processing to finish
    /// you can use the `processing_done_tracker` argument (but it's optional, it's safe to pass None there).
    pub fn process_chunk_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        raw_witness_size: ChunkStateWitnessSize,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        tracing::debug!(
            target: "client",
            chunk_hash=?witness.chunk_header.chunk_hash(),
            shard_id=witness.chunk_header.shard_id(),
            "process_chunk_state_witness",
        );

        // Send the acknowledgement for the state witness back to the chunk producer.
        // This is currently used for network roundtrip time measurement, so we do not need to
        // wait for validation to finish.
        self.send_state_witness_ack(&witness);

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        match self.chain.get_block(witness.chunk_header.prev_block_hash()) {
            Ok(block) => self.process_chunk_state_witness_with_prev_block(
                witness,
                &block,
                processing_done_tracker,
            ),
            Err(Error::DBNotFoundErr(_)) => {
                // Previous block isn't available at the moment, add this witness to the orphan pool.
                self.handle_orphan_state_witness(witness, raw_witness_size)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn send_state_witness_ack(&self, witness: &ChunkStateWitness) {
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitnessAck(
                witness.chunk_producer.clone(),
                ChunkStateWitnessAck::new(witness),
            ),
        ));
    }

    pub fn process_chunk_state_witness_with_prev_block(
        &mut self,
        witness: ChunkStateWitness,
        prev_block: &Block,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        if witness.chunk_header.prev_block_hash() != prev_block.hash() {
            return Err(Error::Other(format!(
                "process_chunk_state_witness_with_prev_block - prev_block doesn't match ({} != {})",
                witness.chunk_header.prev_block_hash(),
                prev_block.hash()
            )));
        }

        self.chunk_validator.start_validating_chunk(witness, &self.chain, processing_done_tracker)
    }
}
