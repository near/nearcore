use crate::crypto_hash_timer::CryptoHashTimer;
use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, ApplyResultForResharding,
    ReshardingResults, RuntimeAdapter, RuntimeStorageConfig, StorageDataSource,
};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ShardChunk;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, Gas, StateChangesForResharding, StateRoot};
use std::collections::HashMap;

/// Result of updating a shard for some block when it has a new chunk for this
/// shard.
#[derive(Debug)]
pub struct NewChunkResult {
    pub(crate) shard_uid: ShardUId,
    pub(crate) gas_limit: Gas,
    pub(crate) apply_result: ApplyChunkResult,
    pub(crate) resharding_results: Option<ReshardingResults>,
}

/// Result of updating a shard for some block when it doesn't have a new chunk
/// for this shard, so previous chunk header is copied.
#[derive(Debug)]
pub struct OldChunkResult {
    pub(crate) shard_uid: ShardUId,
    /// Note that despite the naming, no transactions are applied in this case.
    /// TODO(logunov): exclude receipts/txs context from all related types.
    pub(crate) apply_result: ApplyChunkResult,
    pub(crate) resharding_results: Option<ReshardingResults>,
}

/// Result of updating a shard for some block when we apply only resharding
/// changes due to resharding.
#[derive(Debug)]
pub struct ReshardingResult {
    // parent shard of the
    pub(crate) shard_uid: ShardUId,
    pub(crate) results: Vec<ApplyResultForResharding>,
}

/// Result of processing shard update, covering both stateful and stateless scenarios.
#[derive(Debug)]
pub enum ShardUpdateResult {
    /// Stateful scenario - processed update for a single block.
    Stateful(ShardBlockUpdateResult),
    /// Stateless scenario - processed update based on state witness in a chunk.
    /// Contains `ChunkExtra`s - results for processing updates corresponding
    /// to state witness.
    Stateless(Vec<(CryptoHash, ShardUId, ChunkExtra)>),
}

/// Result for a shard update for a single block.
#[derive(Debug)]
pub enum ShardBlockUpdateResult {
    NewChunk(NewChunkResult),
    OldChunk(OldChunkResult),
    Resharding(ReshardingResult),
}

/// State roots of children shards which are ready.
type ReshardingStateRoots = HashMap<ShardUId, StateRoot>;

pub(crate) struct NewChunkData {
    pub chunk: ShardChunk,
    pub receipts: Vec<Receipt>,
    pub resharding_state_roots: Option<ReshardingStateRoots>,
    pub block: ApplyChunkBlockContext,
    pub is_first_block_with_chunk_of_version: bool,
    pub storage_context: StorageContext,
}

pub(crate) struct OldChunkData {
    pub prev_chunk_extra: ChunkExtra,
    pub resharding_state_roots: Option<ReshardingStateRoots>,
    pub block: ApplyChunkBlockContext,
    pub storage_context: StorageContext,
}

pub(crate) struct ReshardingData {
    pub resharding_state_roots: ReshardingStateRoots,
    pub state_changes: StateChangesForResharding,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
}

/// Reason to update a shard when new block appears on chain.
/// All types include state roots for children shards in case of resharding.
#[allow(clippy::large_enum_variant)]
pub(crate) enum ShardUpdateReason {
    /// Block has a new chunk for the shard.
    /// Contains chunk itself and all new incoming receipts to the shard.
    NewChunk(NewChunkData),
    /// Block doesn't have a new chunk for the shard.
    /// Instead, previous chunk header is copied.
    /// Contains result of shard update for previous block.
    OldChunk(OldChunkData),
    /// See comment to `resharding_state_roots` in `Chain::get_update_shard_job`.
    /// Process only state changes caused by resharding.
    Resharding(ReshardingData),
}

/// Information about shard to update.
pub(crate) struct ShardContext {
    pub shard_uid: ShardUId,
    /// Whether node cares about shard in this epoch.
    pub cares_about_shard_this_epoch: bool,
    /// Whether shard layout changes in the next epoch.
    pub will_shard_layout_change: bool,
    /// Whether transactions should be applied.
    pub should_apply_chunk: bool,
    /// See comment in `get_update_shard_job`.
    pub need_to_reshard: bool,
}

/// Information about storage used for applying txs and receipts.
pub(crate) struct StorageContext {
    /// Data source used for processing shard update.
    pub storage_data_source: StorageDataSource,
    pub state_patch: SandboxStatePatch,
}

/// Processes shard update with given block and shard.
/// Doesn't modify chain, only produces result to be applied later.
pub(crate) fn process_shard_update(
    parent_span: &tracing::Span,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_update_reason: ShardUpdateReason,
    shard_context: ShardContext,
) -> Result<ShardBlockUpdateResult, Error> {
    Ok(match shard_update_reason {
        ShardUpdateReason::NewChunk(data) => ShardBlockUpdateResult::NewChunk(apply_new_chunk(
            parent_span,
            data,
            shard_context,
            runtime,
            epoch_manager,
        )?),
        ShardUpdateReason::OldChunk(data) => ShardBlockUpdateResult::OldChunk(apply_old_chunk(
            parent_span,
            data,
            shard_context,
            runtime,
            epoch_manager,
        )?),
        ShardUpdateReason::Resharding(data) => ShardBlockUpdateResult::Resharding(
            apply_resharding(parent_span, data, shard_context.shard_uid, runtime, epoch_manager)?,
        ),
    })
}

/// Processes shard updates for the execution contexts range which must
/// correspond to missing chunks for some shard.
/// `current_chunk_extra` must correspond to `ChunkExtra` just before
/// execution; in the end it will correspond to the latest execution
/// result.
pub(crate) fn process_missing_chunks_range(
    parent_span: &tracing::Span,
    mut current_chunk_extra: ChunkExtra,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    execution_contexts: Vec<(ApplyChunkBlockContext, ShardContext)>,
) -> Result<Vec<(CryptoHash, ShardUId, ChunkExtra)>, Error> {
    let mut result = vec![];
    for (block_context, shard_context) in execution_contexts {
        let OldChunkResult { shard_uid, apply_result, resharding_results: _ } = apply_old_chunk(
            parent_span,
            OldChunkData {
                block: block_context.clone(),
                resharding_state_roots: None,
                prev_chunk_extra: current_chunk_extra.clone(),
                storage_context: StorageContext {
                    storage_data_source: StorageDataSource::DbTrieOnly,
                    state_patch: Default::default(),
                },
            },
            shard_context,
            runtime,
            epoch_manager,
        )?;
        *current_chunk_extra.state_root_mut() = apply_result.new_root;
        result.push((block_context.block_hash, shard_uid, current_chunk_extra.clone()));
    }
    Ok(result)
}
/// Applies new chunk, which includes applying transactions from chunk and
/// receipts filtered from outgoing receipts from previous chunks.
pub(crate) fn apply_new_chunk(
    parent_span: &tracing::Span,
    data: NewChunkData,
    shard_context: ShardContext,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<NewChunkResult, Error> {
    let NewChunkData {
        block,
        chunk,
        receipts,
        resharding_state_roots,
        is_first_block_with_chunk_of_version,
        storage_context,
    } = data;
    let shard_id = shard_context.shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "new_chunk",
        shard_id)
    .entered();
    let chunk_inner = chunk.cloned_header().take_inner();
    let gas_limit = chunk_inner.gas_limit();

    let _timer = CryptoHashTimer::new(chunk.chunk_hash().0);
    let storage_config = RuntimeStorageConfig {
        state_root: *chunk_inner.prev_state_root(),
        use_flat_storage: true,
        source: storage_context.storage_data_source,
        state_patch: storage_context.state_patch,
        record_storage: false,
    };
    match runtime.apply_chunk(
        storage_config,
        ApplyChunkShardContext {
            shard_id,
            last_validator_proposals: chunk_inner.prev_validator_proposals(),
            gas_limit,
            is_new_chunk: true,
            is_first_block_with_chunk_of_version,
        },
        block.clone(),
        &receipts,
        chunk.transactions(),
    ) {
        Ok(apply_result) => {
            let apply_split_result_or_state_changes = if shard_context.will_shard_layout_change {
                Some(apply_resharding_state_changes(
                    epoch_manager,
                    runtime,
                    block,
                    &apply_result,
                    resharding_state_roots,
                )?)
            } else {
                None
            };
            Ok(NewChunkResult {
                gas_limit,
                shard_uid: shard_context.shard_uid,
                apply_result,
                resharding_results: apply_split_result_or_state_changes,
            })
        }
        Err(err) => Err(err),
    }
}

/// Applies shard update corresponding to missing chunk.
/// (logunov) From what I know, the state update may include only validator
/// accounts update on epoch start.
fn apply_old_chunk(
    parent_span: &tracing::Span,
    data: OldChunkData,
    shard_context: ShardContext,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<OldChunkResult, Error> {
    let OldChunkData { prev_chunk_extra, resharding_state_roots, block, storage_context } = data;
    let shard_id = shard_context.shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "existing_chunk",
        shard_id)
    .entered();

    let storage_config = RuntimeStorageConfig {
        state_root: *prev_chunk_extra.state_root(),
        use_flat_storage: true,
        source: storage_context.storage_data_source,
        state_patch: storage_context.state_patch,
        record_storage: false,
    };
    match runtime.apply_chunk(
        storage_config,
        ApplyChunkShardContext {
            shard_id,
            last_validator_proposals: prev_chunk_extra.validator_proposals(),
            gas_limit: prev_chunk_extra.gas_limit(),
            is_new_chunk: false,
            is_first_block_with_chunk_of_version: false,
        },
        block.clone(),
        &[],
        &[],
    ) {
        Ok(apply_result) => {
            let apply_split_result_or_state_changes = if shard_context.will_shard_layout_change {
                Some(apply_resharding_state_changes(
                    epoch_manager,
                    runtime,
                    block,
                    &apply_result,
                    resharding_state_roots,
                )?)
            } else {
                None
            };
            Ok(OldChunkResult {
                shard_uid: shard_context.shard_uid,
                apply_result,
                resharding_results: apply_split_result_or_state_changes,
            })
        }
        Err(err) => Err(err),
    }
}

/// Applies only resharding changes but not applies any transactions.
fn apply_resharding(
    parent_span: &tracing::Span,
    data: ReshardingData,
    shard_uid: ShardUId,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ReshardingResult, Error> {
    let ReshardingData { resharding_state_roots, state_changes, block_height: height, block_hash } =
        data;
    let shard_id = shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "resharding",
        shard_id,
        ?shard_uid)
    .entered();
    let next_epoch_id = epoch_manager.get_next_epoch_id(&block_hash)?;
    let next_epoch_shard_layout = epoch_manager.get_shard_layout(&next_epoch_id)?;
    let results = runtime.apply_update_to_children_states(
        &block_hash,
        height,
        resharding_state_roots,
        &next_epoch_shard_layout,
        state_changes,
    )?;
    Ok(ReshardingResult { shard_uid, results })
}

/// Process ApplyChunkResult to apply changes to children shards. When
/// shards will change next epoch,
///  - if `resharding_state_roots` is not None, that means states for the
///    children shards are ready this function updates these states and returns
///    apply results for these states
///  - otherwise, this function returns state changes needed to be applied to
///    children shards. These state changes will be stored in the database by
///    `process_resharding_results`
fn apply_resharding_state_changes(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
    block: ApplyChunkBlockContext,
    apply_result: &ApplyChunkResult,
    resharding_state_roots: Option<ReshardingStateRoots>,
) -> Result<ReshardingResults, Error> {
    let state_changes = StateChangesForResharding::from_raw_state_changes(
        apply_result.trie_changes.state_changes(),
        apply_result.processed_delayed_receipts.clone(),
    );
    let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(&block.prev_block_hash)?;
    let next_shard_layout = epoch_manager.get_shard_layout(&next_epoch_id)?;
    if let Some(state_roots) = resharding_state_roots {
        // children states are ready, apply update to them now
        let resharding_results = runtime_adapter.apply_update_to_children_states(
            &block.block_hash,
            block.height,
            state_roots,
            &next_shard_layout,
            state_changes,
        )?;
        Ok(ReshardingResults::ApplyReshardingResults(resharding_results))
    } else {
        // children states are not ready yet, store state changes in consolidated_state_changes
        Ok(ReshardingResults::StoreReshardingResults(state_changes))
    }
}
