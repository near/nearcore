use crate::chain::{
    NewChunkData, NewChunkResult, OldChunkData, OldChunkResult, ShardContext, StorageContext,
    apply_new_chunk, apply_old_chunk,
};
use crate::rayon_spawner::RayonAsyncComputationSpawner;
use crate::resharding::event_type::ReshardingEventType;
use crate::resharding::manager::ReshardingManager;
use crate::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use crate::stateless_validation::processing_tracker::ProcessingDoneTracker;
use crate::store::filter_incoming_receipts_for_shard;
use crate::types::{ApplyChunkBlockContext, ApplyChunkResult, RuntimeAdapter, StorageDataSource};
use crate::validate::validate_chunk_with_chunk_extra_and_receipts_root;
use crate::{Chain, ChainStore, ChainStoreAccess};
use lru::LruCache;
use near_async::futures::AsyncComputationSpawnerExt;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessV1, EncodedChunkStateWitness,
};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ShardId, ShardIndex};
use near_primitives::utils::compression::CompressedData;
use near_primitives::version::ProtocolFeature;
use near_store::flat::BlockInfo;
use near_store::trie::ops::resharding::RetainMode;
use near_store::{PartialStorage, Trie};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
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
            // It is ok to use the shard id from the header because it is a new
            // chunk. An old chunk may have the shard id from the parent shard.
            Self::NewChunk(data) => data.chunk_header.shard_id(),
        }
    }
}

pub struct PreValidationOutput {
    pub main_transition_params: MainTransition,
    pub implicit_transition_params: Vec<ImplicitTransitionParams>,
}

#[derive(Clone)]
pub struct ChunkStateWitnessValidationResult {
    pub chunk_extra: ChunkExtra,
    pub outgoing_receipts: Vec<Receipt>,
}

// TODO: key should be a pair (chunk_shard_uid, witness_shard_uid) for shard merging
pub type MainStateTransitionCache =
    Arc<Mutex<HashMap<ShardUId, LruCache<CryptoHash, ChunkStateWitnessValidationResult>>>>;

/// The number of state witness validation results to cache per shard.
/// This number needs to be small because result contains outgoing receipts, which can be large.
const NUM_WITNESS_RESULT_CACHE_ENTRIES: usize = 20;

/// Parameters of implicit state transition, which is not resulted by
/// application of new chunk.
pub enum ImplicitTransitionParams {
    /// Transition resulted from application of an old chunk. Defined by block
    /// of that chunk and its shard.
    ApplyOldChunk(ApplyChunkBlockContext, ShardUId),
    /// Transition resulted from resharding. Defined by boundary account, mode
    /// saying which of child shards to retain, and parent shard uid.
    Resharding(AccountId, RetainMode, ShardUId),
}

struct StateWitnessBlockRange {
    /// Transition parameters **after** the last chunk, corresponding to all
    /// state transitions from the last new chunk (exclusive) to the parent
    /// block (inclusive).
    implicit_transition_params: Vec<ImplicitTransitionParams>,
    /// Blocks from the last last new chunk (exclusive) to the last new chunk
    /// (inclusive). Note they are in **reverse** order, from the newest to the
    /// oldest. They are needed to validate the chunk's source receipt proofs.
    blocks_after_last_last_chunk: Vec<Block>,
    /// Shard layout for the last chunk before the chunk being validated.
    last_chunk_shard_layout: ShardLayout,
    /// Shard id of the last chunk before the chunk being validated.
    last_chunk_shard_id: ShardId,
}

/// Checks if a block has a new chunk with `shard_index`.
fn block_has_new_chunk(block: &Block, shard_index: ShardIndex) -> Result<bool, Error> {
    let chunks = block.chunks();
    let chunk = chunks.get(shard_index).ok_or_else(|| {
        Error::InvalidChunkStateWitness(format!(
            "Shard {} does not exist in block {}",
            shard_index,
            block.hash()
        ))
    })?;

    Ok(chunk.is_new_chunk(block.header().height()))
}

/// Gets ranges of blocks that are needed to validate a chunk state witness.
/// Iterates backwards through the chain, from the chunk being validated to
/// the second last chunk, if it exists.
fn get_state_witness_block_range(
    store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
    state_witness: &ChunkStateWitness,
) -> Result<StateWitnessBlockRange, Error> {
    let mut implicit_transition_params = Vec::new();
    let mut blocks_after_last_last_chunk = Vec::new();

    /// Position in the chain while traversing the blocks backwards.
    struct TraversalPosition {
        /// Shard ID of chunk, needed to validate state transitions, in the
        /// currently observed block.
        shard_id: ShardId,
        /// Previous block.
        prev_block: Block,
        /// Number of new chunks seen during traversal.
        num_new_chunks_seen: u32,
        /// Current candidate shard layout of last chunk before the chunk being
        /// validated.
        last_chunk_shard_layout: ShardLayout,
        /// Current candidate shard id of last chunk before the chunk being
        /// validated.
        last_chunk_shard_id: ShardId,
    }

    let initial_prev_hash = *state_witness.chunk_header().prev_block_hash();
    let initial_prev_block = store.get_block(&initial_prev_hash)?;
    let initial_shard_layout =
        epoch_manager.get_shard_layout_from_prev_block(&initial_prev_hash)?;
    let initial_shard_id = state_witness.chunk_header().shard_id();
    // Check that shard id is present in current epoch.
    // TODO: consider more proper way to validate this.
    let _ = initial_shard_layout.get_shard_index(initial_shard_id)?;

    let mut position = TraversalPosition {
        shard_id: initial_shard_id,
        prev_block: initial_prev_block,
        num_new_chunks_seen: 0,
        last_chunk_shard_layout: initial_shard_layout,
        last_chunk_shard_id: initial_shard_id,
    };

    loop {
        let prev_hash = position.prev_block.hash();
        let prev_prev_hash = position.prev_block.header().prev_hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash)?;
        let shard_uid = shard_id_to_uid(epoch_manager, position.shard_id, &epoch_id)?;

        if let Some(transition) = get_resharding_transition(
            epoch_manager,
            position.prev_block.header(),
            shard_uid,
            position.num_new_chunks_seen,
        )? {
            implicit_transition_params.push(transition);
        }
        let (prev_shard_layout, prev_shard_id, prev_shard_index) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_hash, position.shard_id)?;

        let new_chunk_seen = block_has_new_chunk(&position.prev_block, prev_shard_index)?;
        let new_chunks_seen_update =
            position.num_new_chunks_seen + if new_chunk_seen { 1 } else { 0 };

        match new_chunks_seen_update {
            // If we have seen 0 chunks, the block contributes to implicit
            // state transition.
            0 => {
                let block_context = Chain::get_apply_chunk_block_context(
                    &position.prev_block,
                    &store.get_block_header(&prev_prev_hash)?,
                    false,
                )?;

                implicit_transition_params
                    .push(ImplicitTransitionParams::ApplyOldChunk(block_context, shard_uid));
            }
            // If we have seen 1 chunk, the block contributes to source receipt
            // proofs.
            1 => blocks_after_last_last_chunk.push(position.prev_block.clone()),
            // If we have seen the 2nd chunk, we are done.
            2 => break,
            _ => unreachable!("chunks_seen should never exceed 2"),
        }

        if position.prev_block.header().is_genesis() {
            break;
        }

        let prev_prev_block = store.get_block(&prev_prev_hash)?;
        // If we have not seen chunks, switch to previous shard id, but
        // once we just saw the first chunk, start keeping its shard id.
        let (last_chunk_shard_layout, last_chunk_shard_id) = if position.num_new_chunks_seen == 0 {
            (prev_shard_layout, prev_shard_id)
        } else {
            (position.last_chunk_shard_layout, position.last_chunk_shard_id)
        };
        position = TraversalPosition {
            shard_id: prev_shard_id,
            prev_block: prev_prev_block,
            num_new_chunks_seen: new_chunks_seen_update,
            last_chunk_shard_layout,
            last_chunk_shard_id,
        };
    }

    implicit_transition_params.reverse();
    Ok(StateWitnessBlockRange {
        implicit_transition_params,
        blocks_after_last_last_chunk,
        last_chunk_shard_layout: position.last_chunk_shard_layout,
        last_chunk_shard_id: position.last_chunk_shard_id,
    })
}

/// Checks if chunk validation requires a transition to new shard layout in the
/// block with `prev_hash`, with a split resulting in the `shard_uid`, and if
/// so, returns the corresponding resharding transition parameters.
fn get_resharding_transition(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_header: &BlockHeader,
    shard_uid: ShardUId,
    num_new_chunks_seen: u32,
) -> Result<Option<ImplicitTransitionParams>, Error> {
    // If we have already seen a new chunk, we don't need to validate
    // resharding transition.
    if num_new_chunks_seen > 0 {
        return Ok(None);
    }

    let shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_header.hash())?;
    let prev_epoch_id = epoch_manager.get_prev_epoch_id_from_prev_block(prev_header.hash())?;
    let prev_shard_layout = epoch_manager.get_shard_layout(&prev_epoch_id)?;
    let block_has_new_shard_layout = epoch_manager.is_next_block_epoch_start(prev_header.hash())?
        && shard_layout != prev_shard_layout;

    if !block_has_new_shard_layout {
        return Ok(None);
    }

    let block_info = BlockInfo {
        hash: *prev_header.hash(),
        height: prev_header.height(),
        prev_hash: *prev_header.prev_hash(),
    };
    let params = match ReshardingEventType::from_shard_layout(&shard_layout, block_info)? {
        Some(ReshardingEventType::SplitShard(params)) => params,
        None => return Ok(None),
    };

    if params.left_child_shard == shard_uid {
        Ok(Some(ImplicitTransitionParams::Resharding(
            params.boundary_account,
            RetainMode::Left,
            shard_uid,
        )))
    } else if params.right_child_shard == shard_uid {
        Ok(Some(ImplicitTransitionParams::Resharding(
            params.boundary_account,
            RetainMode::Right,
            shard_uid,
        )))
    } else {
        Ok(None)
    }
}

/// Pre-validates the chunk's receipts and transactions against the chain.
/// We do this before handing off the computationally intensive part to a
/// validation thread.
pub fn pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    chain: &Chain,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<PreValidationOutput, Error> {
    let store = chain.chain_store();

    // Ensure that the chunk header version is supported in this protocol version
    let ChunkProductionKey { epoch_id, .. } = state_witness.chunk_production_key();
    let protocol_version = epoch_manager.get_epoch_info(&epoch_id)?.protocol_version();
    state_witness.chunk_header().validate_version(protocol_version)?;

    // First, go back through the blockchain history to locate the last new chunk
    // and last last new chunk for the shard.
    let StateWitnessBlockRange {
        implicit_transition_params,
        blocks_after_last_last_chunk,
        last_chunk_shard_layout,
        last_chunk_shard_id,
    } = get_state_witness_block_range(store, epoch_manager, state_witness)?;
    let last_chunk_shard_index = last_chunk_shard_layout.get_shard_index(last_chunk_shard_id)?;

    let receipts_to_apply = validate_source_receipt_proofs(
        epoch_manager,
        &state_witness.source_receipt_proofs(),
        &blocks_after_last_last_chunk,
        last_chunk_shard_layout,
        last_chunk_shard_id,
    )?;
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if &applied_receipts_hash != state_witness.applied_receipts_hash() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash,
            state_witness.applied_receipts_hash()
        )));
    }
    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions());
    let last_chunk_block = blocks_after_last_last_chunk.first().ok_or_else(|| {
        Error::Other("blocks_after_last_last_chunk is empty, this should be impossible!".into())
    })?;
    let last_new_chunk_tx_root =
        last_chunk_block.chunks().get(last_chunk_shard_index).unwrap().tx_root();
    if last_new_chunk_tx_root != tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness, last_new_chunk_tx_root
        )));
    }

    let transaction_validity_check_results = {
        if last_chunk_block.header().is_genesis() {
            vec![true; state_witness.transactions().len()]
        } else {
            let prev_block_header =
                store.get_block_header(last_chunk_block.header().prev_hash())?;
            let check = chain.transaction_validity_check(prev_block_header);
            state_witness.transactions().iter().map(|t| check(t)).collect::<Vec<_>>()
        }
    };

    let main_transition_params = if last_chunk_block.header().is_genesis() {
        let epoch_id = last_chunk_block.header().epoch_id();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let congestion_info = last_chunk_block
            .block_congestion_info()
            .get(&last_chunk_shard_id)
            .map(|info| info.congestion_info);
        let chunk_extra =
            chain.genesis_chunk_extra(&shard_layout, last_chunk_shard_id, congestion_info)?;
        MainTransition::Genesis {
            chunk_extra,
            block_hash: *last_chunk_block.hash(),
            shard_id: last_chunk_shard_id,
        }
    } else {
        MainTransition::NewChunk(NewChunkData {
            chunk_header: last_chunk_block.chunks().get(last_chunk_shard_index).unwrap().clone(),
            transactions: state_witness.transactions().clone(),
            transaction_validity_check_results,
            receipts: receipts_to_apply,
            block: Chain::get_apply_chunk_block_context(
                last_chunk_block,
                &store.get_block_header(last_chunk_block.header().prev_hash())?,
                true,
            )?,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: state_witness.main_state_transition().base_state.clone(),
                }),
                state_patch: Default::default(),
            },
        })
    };

    Ok(PreValidationOutput { main_transition_params, implicit_transition_params })
}

/// Validate that receipt proofs contain the receipts that should be applied during the
/// transition proven by ChunkStateWitness. The receipts are extracted from the proofs
/// and arranged in the order in which they should be applied during the transition.
fn validate_source_receipt_proofs(
    epoch_manager: &dyn EpochManagerAdapter,
    source_receipt_proofs: &HashMap<ChunkHash, ReceiptProof>,
    receipt_source_blocks: &[Block],
    target_shard_layout: ShardLayout,
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
    let mut current_target_shard_id = target_chunk_shard_id;

    // Iterate over blocks between last_chunk_block (inclusive) and last_last_chunk_block (exclusive),
    // from the newest blocks to the oldest.
    for block in receipt_source_blocks {
        // Collect all receipts coming from this block.
        let mut block_receipt_proofs = Vec::new();

        for chunk in block.chunks().iter_deprecated() {
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

            validate_receipt_proof(receipt_proof, chunk, current_target_shard_id)?;

            expected_proofs_len += 1;
            block_receipt_proofs.push(receipt_proof.clone());
        }

        block_receipt_proofs = filter_incoming_receipts_for_shard(
            &target_shard_layout,
            target_chunk_shard_id,
            Arc::new(block_receipt_proofs),
        )?;

        // Arrange the receipts in the order in which they should be applied.
        let receipts_shuffle_salt = get_receipts_shuffle_salt(epoch_manager, block)?;
        shuffle_receipt_proofs(&mut block_receipt_proofs, receipts_shuffle_salt);
        for proof in block_receipt_proofs {
            receipts_to_apply.extend(proof.0.iter().cloned());
        }

        current_target_shard_id = epoch_manager
            .get_prev_shard_id_from_prev_hash(block.header().prev_hash(), current_target_shard_id)?
            .1;
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
    let ChunkProductionKey { epoch_id, shard_id, .. } = state_witness.chunk_production_key();
    let _timer = crate::stateless_validation::metrics::CHUNK_STATE_WITNESS_VALIDATION_TIME
        .with_label_values(&[&shard_id.to_string()])
        .start_timer();
    let span = tracing::debug_span!(target: "client", "validate_chunk_state_witness").entered();
    let witness_shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let witness_chunk_shard_id = shard_id;
    let witness_chunk_shard_uid =
        shard_id_to_uid(epoch_manager, witness_chunk_shard_id, &epoch_id)?;
    let block_hash = pre_validation_output.main_transition_params.block_hash();
    let epoch_id = epoch_manager.get_epoch_id(&block_hash)?;
    let shard_id = pre_validation_output.main_transition_params.shard_id();
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &epoch_id)?;
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    let cache_result = {
        let mut shard_cache = main_state_transition_cache.lock();
        shard_cache
            .get_mut(&witness_chunk_shard_uid)
            .and_then(|cache| cache.get(&block_hash).cloned())
    };
    let (mut chunk_extra, mut outgoing_receipts) =
        match (pre_validation_output.main_transition_params, cache_result) {
            (MainTransition::Genesis { chunk_extra, .. }, _) => (chunk_extra, vec![]),
            (MainTransition::NewChunk(new_chunk_data), None) => {
                let chunk_header = new_chunk_data.chunk_header.clone();
                let NewChunkResult { apply_result: mut main_apply_result, .. } = apply_new_chunk(
                    ApplyChunkReason::ValidateChunkStateWitness,
                    &span,
                    new_chunk_data,
                    ShardContext { shard_uid, should_apply_chunk: true },
                    runtime_adapter,
                )?;
                let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
                let chunk_extra = apply_result_to_chunk_extra(main_apply_result, &chunk_header);

                (chunk_extra, outgoing_receipts)
            }
            (_, Some(result)) => (result.chunk_extra, result.outgoing_receipts),
        };
    if chunk_extra.state_root() != &state_witness.main_state_transition().post_state_root {
        // This is an early check, it's not for correctness, only for better
        // error reporting in case of an invalid state witness due to a bug.
        // Only the final state root check against the chunk header is required.
        return Err(Error::InvalidChunkStateWitness(format!(
            "Post state root {:?} for main transition does not match expected post state root {:?}",
            chunk_extra.state_root(),
            state_witness.main_state_transition().post_state_root,
        )));
    }

    // Compute receipt hashes here to avoid copying receipts
    let outgoing_receipts_hashes = {
        let chunk_epoch_id = epoch_manager.get_epoch_id(&block_hash)?;
        let chunk_shard_layout = epoch_manager.get_shard_layout(&chunk_epoch_id)?;
        if chunk_shard_layout != witness_shard_layout {
            ChainStore::reassign_outgoing_receipts_for_resharding(
                &mut outgoing_receipts,
                protocol_version,
                &witness_shard_layout,
                state_witness.chunk_header().shard_id(),
                shard_id,
            )?;
        }
        Chain::build_receipts_hashes(&outgoing_receipts, &witness_shard_layout)?
    };
    // Save main state transition result to cache.
    {
        let mut shard_cache = main_state_transition_cache.lock();
        let cache = shard_cache.entry(witness_chunk_shard_uid).or_insert_with(|| {
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

    if pre_validation_output.implicit_transition_params.len()
        != state_witness.implicit_transitions().len()
    {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Implicit transitions count mismatch. Expected {}, found {}",
            pre_validation_output.implicit_transition_params.len(),
            state_witness.implicit_transitions().len(),
        )));
    }

    for (implicit_transition_params, transition) in pre_validation_output
        .implicit_transition_params
        .into_iter()
        .zip(state_witness.implicit_transitions().into_iter())
    {
        let (shard_uid, new_state_root, new_congestion_info) = match implicit_transition_params {
            ImplicitTransitionParams::ApplyOldChunk(block, shard_uid) => {
                let shard_context = ShardContext { shard_uid, should_apply_chunk: false };
                let old_chunk_data = OldChunkData {
                    prev_chunk_extra: chunk_extra.clone(),
                    block,
                    storage_context: StorageContext {
                        storage_data_source: StorageDataSource::Recorded(PartialStorage {
                            nodes: transition.base_state.clone(),
                        }),
                        state_patch: Default::default(),
                    },
                };
                let OldChunkResult { apply_result, .. } = apply_old_chunk(
                    ApplyChunkReason::ValidateChunkStateWitness,
                    &span,
                    old_chunk_data,
                    shard_context,
                    runtime_adapter,
                )?;
                let congestion_info = chunk_extra.congestion_info();
                (shard_uid, apply_result.new_root, congestion_info)
            }
            ImplicitTransitionParams::Resharding(
                boundary_account,
                retain_mode,
                child_shard_uid,
            ) => {
                let old_root = *chunk_extra.state_root();
                let partial_storage = PartialStorage { nodes: transition.base_state.clone() };
                let parent_trie = Trie::from_recorded_storage(partial_storage, old_root, true);

                // Update the congestion info based on the parent shard. It's
                // important to do this step before the `retain_split_shard`
                // because only the parent trie has the needed information.
                let epoch_id = epoch_manager.get_epoch_id(&block_hash)?;
                let parent_shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
                let parent_congestion_info = chunk_extra.congestion_info();

                let child_epoch_id = epoch_manager.get_next_epoch_id(&block_hash)?;
                let child_shard_layout = epoch_manager.get_shard_layout(&child_epoch_id)?;
                let child_congestion_info = ReshardingManager::get_child_congestion_info(
                    &parent_trie,
                    &parent_shard_layout,
                    parent_congestion_info,
                    &child_shard_layout,
                    &child_shard_uid,
                    retain_mode,
                )?;

                let trie_changes =
                    parent_trie.retain_split_shard(&boundary_account, retain_mode)?;

                (child_shard_uid, trie_changes.new_root, child_congestion_info)
            }
        };

        *chunk_extra.state_root_mut() = new_state_root;
        *chunk_extra.congestion_info_mut() = new_congestion_info;
        if chunk_extra.state_root() != &transition.post_state_root {
            // This is an early check, it's not for correctness, only for better
            // error reporting in case of an invalid state witness due to a bug.
            // Only the final state root check against the chunk header is required.
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at block {:?} to shard {:?}, does not match expected state root {:?}",
                chunk_extra.state_root(),
                transition.block_hash,
                shard_uid,
                transition.post_state_root
            )));
        }
    }

    // Finally, verify that the newly proposed chunk matches everything we have computed.
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);
    validate_chunk_with_chunk_extra_and_receipts_root(
        &chunk_extra,
        &state_witness.chunk_header(),
        &outgoing_receipts_root,
    )?;

    Ok(())
}

pub fn apply_result_to_chunk_extra(
    apply_result: ApplyChunkResult,
    chunk: &ShardChunkHeader,
) -> ChunkExtra {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        chunk.gas_limit(),
        apply_result.total_balance_burnt,
        apply_result.congestion_info,
        apply_result.bandwidth_requests,
    )
}

impl Chain {
    pub fn shadow_validate_state_witness(
        &self,
        witness: ChunkStateWitness,
        epoch_manager: &dyn EpochManagerAdapter,
        processing_done_tracker: Option<ProcessingDoneTracker>,
    ) -> Result<(), Error> {
        let shard_id = witness.chunk_header().shard_id();
        let height_created = witness.chunk_header().height_created();
        let chunk_hash = witness.chunk_header().chunk_hash();
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

            let protocol_version = self
                .epoch_manager
                .get_epoch_protocol_version(&witness.chunk_production_key().epoch_id)?;
            if ProtocolFeature::VersionedStateWitness.enabled(protocol_version) {
                let _witness: ChunkStateWitness = encoded_witness.decode()?.0;
            } else {
                let _witness: ChunkStateWitnessV1 = encoded_witness.decode()?.0;
            };
            decode_timer.observe_duration();
            (encoded_witness, raw_witness_size)
        };
        let pre_validation_start = Instant::now();
        let pre_validation_result =
            pre_validate_chunk_state_witness(&witness, &self, epoch_manager)?;
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
                    crate::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
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
