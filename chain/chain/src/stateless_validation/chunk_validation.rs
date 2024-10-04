use crate::chain::{
    apply_new_chunk, apply_old_chunk, NewChunkData, NewChunkResult, OldChunkData, OldChunkResult,
    ShardContext, StorageContext,
};
use crate::rayon_spawner::RayonAsyncComputationSpawner;
use crate::sharding::shuffle_receipt_proofs;
use crate::stateless_validation::processing_tracker::ProcessingDoneTracker;
use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, PreparedTransactions, RuntimeAdapter,
    RuntimeStorageConfig, StorageDataSource,
};
use crate::validate::validate_chunk_with_chunk_extra_and_receipts_root;
use crate::{Chain, ChainStoreAccess};
use lru::LruCache;
use near_async::futures::AsyncComputationSpawnerExt;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_pool::TransactionGroupIteratorWrapper;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::Block;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader};
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, EncodedChunkStateWitness,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ProtocolVersion, ShardId};
use near_primitives::utils::compression::CompressedData;
use near_store::PartialStorage;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[allow(clippy::large_enum_variant)]
pub enum MainTransition {
    Genesis { chunk_extra: ChunkExtra, block_hash: CryptoHash, shard_id: ShardId },
    NewChunk(NewChunkData),
}

impl MainTransition {
    pub fn block_hash(&self) -> CryptoHash {
        match self {
            Self::Genesis { block_hash, .. } => *block_hash,
            Self::NewChunk(data) => data.block.block_hash,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            Self::Genesis { shard_id, .. } => *shard_id,
            Self::NewChunk(data) => data.chunk_header.shard_id(),
        }
    }
}

pub struct PreValidationOutput {
    pub main_transition_params: MainTransition,
    pub implicit_transition_params: Vec<ApplyChunkBlockContext>,
}

#[derive(Clone)]
pub struct ChunkStateWitnessValidationResult {
    pub chunk_extra: ChunkExtra,
    pub outgoing_receipts: Vec<Receipt>,
}

pub type MainStateTransitionCache =
    Arc<Mutex<HashMap<ShardUId, LruCache<CryptoHash, ChunkStateWitnessValidationResult>>>>;

/// The number of state witness validation results to cache per shard.
/// This number needs to be small because result contains outgoing receipts, which can be large.
const NUM_WITNESS_RESULT_CACHE_ENTRIES: usize = 20;

/// Checks that proposed `transactions` are valid for a chunk with `chunk_header`.
/// Uses `storage_config` to possibly record reads or use recorded storage.
pub fn validate_prepared_transactions(
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
        crate::types::PrepareTransactionsChunkContext {
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
pub fn pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    chain: &Chain,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<PreValidationOutput, Error> {
    let store = chain.chain_store();
    let epoch_id = state_witness.epoch_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;

    let shard_id = state_witness.chunk_header.shard_id();
    let shard_index = shard_layout.get_shard_index(shard_id);

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
            let Some(chunk) = chunks.get(shard_index) else {
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
    let last_new_chunk_tx_root = last_chunk_block.chunks().get(shard_index).unwrap().tx_root();
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
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let congestion_info = last_chunk_block
            .block_congestion_info()
            .get(&shard_id)
            .map(|info| info.congestion_info);
        let genesis_protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let chunk_extra = chain.genesis_chunk_extra(
            &shard_layout,
            shard_id,
            genesis_protocol_version,
            congestion_info,
        )?;
        MainTransition::Genesis { chunk_extra, block_hash: *last_chunk_block.hash(), shard_id }
    } else {
        MainTransition::NewChunk(NewChunkData {
            chunk_header: last_chunk_block.chunks().get(shard_index).unwrap().clone(),
            transactions: state_witness.transactions.clone(),
            receipts: receipts_to_apply,
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

pub fn validate_chunk_state_witness(
    state_witness: ChunkStateWitness,
    pre_validation_output: PreValidationOutput,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
    main_state_transition_cache: &MainStateTransitionCache,
) -> Result<(), Error> {
    let _timer = crate::stateless_validation::metrics::CHUNK_STATE_WITNESS_VALIDATION_TIME
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
                    },
                    runtime_adapter,
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
        let cache = shard_cache.entry(shard_uid).or_insert_with(|| {
            LruCache::new(NonZeroUsize::new(NUM_WITNESS_RESULT_CACHE_ENTRIES).unwrap())
        });
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
            },
            runtime_adapter,
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
    )?;

    Ok(())
}

pub fn apply_result_to_chunk_extra(
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

impl Chain {
    pub fn shadow_validate_state_witness(
        &self,
        witness: ChunkStateWitness,
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let shard_id = witness.chunk_header.shard_id();
        let height_created = witness.chunk_header.height_created();
        let chunk_hash = witness.chunk_header.chunk_hash();
        let parent_span = tracing::debug_span!(
            target: "chain", "shadow_validate", ?shard_id, height_created);
        let (encoded_witness, raw_witness_size) = {
            let shard_id_label = shard_id.to_string();
            let encode_timer =
                crate::stateless_validation::metrics::CHUNK_STATE_WITNESS_ENCODE_TIME
                    .with_label_values(&[shard_id_label.as_str()])
                    .start_timer();
            let (encoded_witness, raw_witness_size) = EncodedChunkStateWitness::encode(&witness)?;
            encode_timer.observe_duration();
            crate::stateless_validation::metrics::record_witness_size_metrics(
                raw_witness_size,
                encoded_witness.size_bytes(),
                &witness,
            );
            let decode_timer =
                crate::stateless_validation::metrics::CHUNK_STATE_WITNESS_DECODE_TIME
                    .with_label_values(&[shard_id_label.as_str()])
                    .start_timer();
            encoded_witness.decode()?;
            decode_timer.observe_duration();
            (encoded_witness, raw_witness_size)
        };
        let pre_validation_start = Instant::now();
        let pre_validation_result =
            pre_validate_chunk_state_witness(&witness, &self, epoch_manager, runtime_adapter)?;
        tracing::debug!(
            parent: &parent_span,
            ?shard_id,
            ?chunk_hash,
            witness_size = encoded_witness.size_bytes(),
            raw_witness_size,
            pre_validation_elapsed = ?pre_validation_start.elapsed(),
            "completed shadow chunk pre-validation"
        );
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        Arc::new(RayonAsyncComputationSpawner).spawn("shadow_validate", move || {
            // processing_done_tracker must survive until the processing is finished.
            let _processing_done_tracker_capture: Option<ProcessingDoneTracker> =
                processing_done_tracker;

            let validation_start = Instant::now();

            match validate_chunk_state_witness(
                witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &MainStateTransitionCache::default(),
            ) {
                Ok(()) => {
                    tracing::debug!(
                        parent: &parent_span,
                        ?shard_id,
                        ?chunk_hash,
                        validation_elapsed = ?validation_start.elapsed(),
                        "completed shadow chunk validation"
                    );
                }
                Err(err) => {
                    crate::stateless_validation::metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL
                        .inc();
                    tracing::error!(
                        parent: &parent_span,
                        ?err,
                        ?shard_id,
                        ?chunk_hash,
                        "shadow chunk validation failed"
                    );
                }
            }
        });
        Ok(())
    }
}
