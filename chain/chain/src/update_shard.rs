use crate::crypto_hash_timer::CryptoHashTimer;
use crate::types::{
    ApplySplitStateResult, ApplySplitStateResultOrStateChanges, ApplyTransactionResult,
    ApplyTransactionsBlockContext, ApplyTransactionsChunkContext, RuntimeAdapter,
    RuntimeStorageConfig, StorageDataSource,
};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ShardChunk;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, Gas, StateChangesForSplitStates, StateRoot};
use std::collections::HashMap;

/// Result of updating a shard for some block when it has a new chunk for this
/// shard.
#[derive(Debug)]
pub struct NewChunkResult {
    pub(crate) shard_uid: ShardUId,
    pub(crate) gas_limit: Gas,
    pub(crate) apply_result: ApplyTransactionResult,
    pub(crate) apply_split_result_or_state_changes: Option<ApplySplitStateResultOrStateChanges>,
}

/// Result of updating a shard for some block when it doesn't have a new chunk
/// for this shard, so previous chunk header is copied.
#[derive(Debug)]
pub struct OldChunkResult {
    pub(crate) shard_uid: ShardUId,
    /// Note that despite the naming, no transactions are applied in this case.
    /// TODO(logunov): exclude receipts/txs context from all related types.
    pub(crate) apply_result: ApplyTransactionResult,
    pub(crate) apply_split_result_or_state_changes: Option<ApplySplitStateResultOrStateChanges>,
}

/// Result of updating a shard for some block when we apply only split state
/// changes due to resharding.
#[derive(Debug)]
pub struct StateSplitResult {
    // parent shard of the split states
    pub(crate) shard_uid: ShardUId,
    pub(crate) results: Vec<ApplySplitStateResult>,
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
    StateSplit(StateSplitResult),
}

/// State roots of split shards which are ready.
type SplitStateRoots = HashMap<ShardUId, StateRoot>;

pub(crate) struct NewChunkData {
    pub chunk: ShardChunk,
    pub receipts: Vec<Receipt>,
    pub split_state_roots: Option<SplitStateRoots>,
    pub block: ApplyTransactionsBlockContext,
    pub is_first_block_with_chunk_of_version: bool,
    pub storage_context: StorageContext,
}

pub(crate) struct OldChunkData {
    pub prev_chunk_extra: ChunkExtra,
    pub split_state_roots: Option<SplitStateRoots>,
    pub block: ApplyTransactionsBlockContext,
    pub storage_context: StorageContext,
}

pub(crate) struct StateSplitData {
    pub split_state_roots: SplitStateRoots,
    pub state_changes: StateChangesForSplitStates,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
}

/// Reason to update a shard when new block appears on chain.
/// All types include state roots for split shards in case of resharding.
pub(crate) enum ShardUpdateReason {
    /// Block has a new chunk for the shard.
    /// Contains chunk itself and all new incoming receipts to the shard.
    NewChunk(NewChunkData),
    /// Block doesn't have a new chunk for the shard.
    /// Instead, previous chunk header is copied.
    /// Contains result of shard update for previous block.
    OldChunk(OldChunkData),
    /// See comment to `split_state_roots` in `Chain::get_update_shard_job`.
    /// Process only state changes caused by resharding.
    StateSplit(StateSplitData),
}

/// Information about shard to update.
pub(crate) struct ShardContext {
    pub shard_uid: ShardUId,
    /// Whether node cares about shard in this epoch.
    pub cares_about_shard_this_epoch: bool,
    /// Whether shard layout changes in the next epoch.
    pub will_shard_layout_change: bool,
    /// Whether transactions should be applied.
    pub should_apply_transactions: bool,
    /// See comment in `get_update_shard_job`.
    pub need_to_split_states: bool,
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
    match shard_update_reason {
        ShardUpdateReason::NewChunk(data) => {
            apply_new_chunk(parent_span, data, shard_context, runtime, epoch_manager)
        }
        ShardUpdateReason::OldChunk(data) => {
            apply_old_chunk(parent_span, data, shard_context, runtime, epoch_manager)
        }
        ShardUpdateReason::StateSplit(data) => {
            apply_state_split(parent_span, data, shard_context.shard_uid, runtime, epoch_manager)
        }
    }
}

/// Applies new chunk, which includes applying transactions from chunk and
/// receipts filtered from outgoing receipts from previous chunks.
fn apply_new_chunk(
    parent_span: &tracing::Span,
    data: NewChunkData,
    shard_context: ShardContext,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ShardBlockUpdateResult, Error> {
    let NewChunkData {
        block,
        chunk,
        receipts,
        split_state_roots,
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
    match runtime.apply_transactions(
        storage_config,
        ApplyTransactionsChunkContext {
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
                Some(apply_split_state_changes(
                    epoch_manager,
                    runtime,
                    block,
                    &apply_result,
                    split_state_roots,
                )?)
            } else {
                None
            };
            Ok(ShardBlockUpdateResult::NewChunk(NewChunkResult {
                gas_limit,
                shard_uid: shard_context.shard_uid,
                apply_result,
                apply_split_result_or_state_changes,
            }))
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
) -> Result<ShardBlockUpdateResult, Error> {
    let OldChunkData { prev_chunk_extra, split_state_roots, block, storage_context } = data;
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
    match runtime.apply_transactions(
        storage_config,
        ApplyTransactionsChunkContext {
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
                Some(apply_split_state_changes(
                    epoch_manager,
                    runtime,
                    block,
                    &apply_result,
                    split_state_roots,
                )?)
            } else {
                None
            };
            Ok(ShardBlockUpdateResult::OldChunk(OldChunkResult {
                shard_uid: shard_context.shard_uid,
                apply_result,
                apply_split_result_or_state_changes,
            }))
        }
        Err(err) => Err(err),
    }
}

/// Applies only split state changes but not applies any transactions.
fn apply_state_split(
    parent_span: &tracing::Span,
    data: StateSplitData,
    shard_uid: ShardUId,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ShardBlockUpdateResult, Error> {
    let StateSplitData { split_state_roots, state_changes, block_height: height, block_hash } =
        data;
    let shard_id = shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "split_state",
        shard_id,
        ?shard_uid)
    .entered();
    let next_epoch_id = epoch_manager.get_next_epoch_id(&block_hash)?;
    let next_epoch_shard_layout = epoch_manager.get_shard_layout(&next_epoch_id)?;
    let results = runtime.apply_update_to_split_states(
        &block_hash,
        height,
        split_state_roots,
        &next_epoch_shard_layout,
        state_changes,
    )?;
    Ok(ShardBlockUpdateResult::StateSplit(StateSplitResult { shard_uid, results }))
}

/// Process ApplyTransactionResult to apply changes to split states
/// When shards will change next epoch,
///    if `split_state_roots` is not None, that means states for the split shards are ready
///    this function updates these states and return apply results for these states
///    otherwise, this function returns state changes needed to be applied to split
///    states. These state changes will be stored in the database by `process_split_state`
fn apply_split_state_changes(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
    block: ApplyTransactionsBlockContext,
    apply_result: &ApplyTransactionResult,
    split_state_roots: Option<SplitStateRoots>,
) -> Result<ApplySplitStateResultOrStateChanges, Error> {
    let state_changes = StateChangesForSplitStates::from_raw_state_changes(
        apply_result.trie_changes.state_changes(),
        apply_result.processed_delayed_receipts.clone(),
    );
    let next_epoch_shard_layout = {
        let next_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(&block.prev_block_hash)?;
        epoch_manager.get_shard_layout(&next_epoch_id)?
    };
    // split states are ready, apply update to them now
    if let Some(state_roots) = split_state_roots {
        let split_state_results = runtime_adapter.apply_update_to_split_states(
            &block.block_hash,
            block.height,
            state_roots,
            &next_epoch_shard_layout,
            state_changes,
        )?;
        Ok(ApplySplitStateResultOrStateChanges::ApplySplitStateResults(split_state_results))
    } else {
        // split states are not ready yet, store state changes in consolidated_state_changes
        Ok(ApplySplitStateResultOrStateChanges::StateChangesForSplitStates(state_changes))
    }
}
