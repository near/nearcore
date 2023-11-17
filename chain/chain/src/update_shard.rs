use crate::crypto_hash_timer::CryptoHashTimer;
use crate::types::{
    ApplySplitStateResult, ApplySplitStateResultOrStateChanges, ApplyTransactionResult,
    RuntimeAdapter, RuntimeStorageConfig,
};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::challenge::ChallengesResult;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ShardChunk;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{Balance, BlockHeight, Gas, StateChangesForSplitStates, StateRoot};
use std::collections::HashMap;

/// Information about block for which shard is updated.
/// Use cases include:
/// - queries to epoch manager
/// - allowing contracts to get current chain data
#[derive(Clone, Debug)]
pub(crate) struct BlockContext {
    pub block_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub challenges_result: ChallengesResult,
    pub block_timestamp: u64,
    pub gas_price: Balance,
    pub height: BlockHeight,
    pub random_seed: CryptoHash,
    pub is_first_block_with_chunk_of_version: bool,
}

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

#[derive(Debug)]
pub enum ApplyChunkResult {
    NewChunk(NewChunkResult),
    OldChunk(OldChunkResult),
    StateSplit(StateSplitResult),
}

/// State roots of split shards which are ready.
type SplitStateRoots = HashMap<ShardUId, StateRoot>;

/// Reason to update a shard when new block appears on chain.
/// All types include state roots for split shards in case of resharding.
pub(crate) enum ShardUpdateReason {
    /// Block has a new chunk for the shard.
    /// Contains chunk itself and all new incoming receipts to the shard.
    NewChunk(ShardChunk, Vec<Receipt>, Option<SplitStateRoots>),
    /// Block doesn't have a new chunk for the shard.
    /// Instead, previous chunk header is copied.
    /// Contains result of shard update for previous block.
    OldChunk(ChunkExtra, Option<SplitStateRoots>),
    /// See comment to `split_state_roots` in `Chain::get_update_shard_job`.
    /// Process only state changes caused by resharding.
    StateSplit(SplitStateRoots, StateChangesForSplitStates),
}

/// Information about shard to update.
pub(crate) struct ShardContext {
    pub shard_uid: ShardUId,
    /// Whether shard layout changes in the next epoch.
    pub will_shard_layout_change: bool,
}

/// Processes shard update with given block and shard.
/// Doesn't modify chain, only produces result to be applied later.
pub(crate) fn process_shard_update(
    parent_span: &tracing::Span,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_update_reason: ShardUpdateReason,
    block_context: BlockContext,
    shard_context: ShardContext,
    state_patch: SandboxStatePatch,
) -> Result<ApplyChunkResult, Error> {
    match shard_update_reason {
        ShardUpdateReason::NewChunk(chunk, receipts, split_state_roots) => apply_new_chunk(
            parent_span,
            block_context,
            chunk,
            shard_context,
            receipts,
            state_patch,
            runtime,
            epoch_manager,
            split_state_roots,
        ),
        ShardUpdateReason::OldChunk(prev_chunk_extra, split_state_roots) => apply_old_chunk(
            parent_span,
            block_context,
            &prev_chunk_extra,
            shard_context,
            state_patch,
            runtime,
            epoch_manager,
            split_state_roots,
        ),
        ShardUpdateReason::StateSplit(split_state_roots, state_changes) => apply_state_split(
            parent_span,
            block_context,
            shard_context.shard_uid,
            runtime,
            epoch_manager,
            split_state_roots,
            state_changes,
        ),
    }
}

/// Applies new chunk, which includes applying transactions from chunk and
/// receipts filtered from outgoing receipts from previous chunks.
fn apply_new_chunk(
    parent_span: &tracing::Span,
    block_context: BlockContext,
    chunk: ShardChunk,
    shard_info: ShardContext,
    receipts: Vec<Receipt>,
    state_patch: SandboxStatePatch,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    split_state_roots: Option<SplitStateRoots>,
) -> Result<ApplyChunkResult, Error> {
    let shard_id = shard_info.shard_uid.shard_id();
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
        source: crate::types::StorageDataSource::Db,
        state_patch,
        record_storage: false,
    };
    match runtime.apply_transactions(
        shard_id,
        storage_config,
        block_context.height,
        block_context.block_timestamp,
        &block_context.prev_block_hash,
        &block_context.block_hash,
        &receipts,
        chunk.transactions(),
        chunk_inner.prev_validator_proposals(),
        block_context.gas_price,
        gas_limit,
        &block_context.challenges_result,
        block_context.random_seed,
        true,
        block_context.is_first_block_with_chunk_of_version,
    ) {
        Ok(apply_result) => {
            let apply_split_result_or_state_changes = if shard_info.will_shard_layout_change {
                Some(apply_split_state_changes(
                    epoch_manager,
                    runtime,
                    block_context,
                    &apply_result,
                    split_state_roots,
                )?)
            } else {
                None
            };
            Ok(ApplyChunkResult::NewChunk(NewChunkResult {
                gas_limit,
                shard_uid: shard_info.shard_uid,
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
    block_context: BlockContext,
    prev_chunk_extra: &ChunkExtra,
    shard_info: ShardContext,
    state_patch: SandboxStatePatch,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    split_state_roots: Option<SplitStateRoots>,
) -> Result<ApplyChunkResult, Error> {
    let shard_id = shard_info.shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "existing_chunk",
        shard_id)
    .entered();

    let storage_config = RuntimeStorageConfig {
        state_root: *prev_chunk_extra.state_root(),
        use_flat_storage: true,
        source: crate::types::StorageDataSource::Db,
        state_patch,
        record_storage: false,
    };
    match runtime.apply_transactions(
        shard_id,
        storage_config,
        block_context.height,
        block_context.block_timestamp,
        &block_context.prev_block_hash,
        &block_context.block_hash,
        &[],
        &[],
        prev_chunk_extra.validator_proposals(),
        block_context.gas_price,
        prev_chunk_extra.gas_limit(),
        &block_context.challenges_result,
        block_context.random_seed,
        false,
        false,
    ) {
        Ok(apply_result) => {
            let apply_split_result_or_state_changes = if shard_info.will_shard_layout_change {
                Some(apply_split_state_changes(
                    epoch_manager,
                    runtime,
                    block_context,
                    &apply_result,
                    split_state_roots,
                )?)
            } else {
                None
            };
            Ok(ApplyChunkResult::OldChunk(OldChunkResult {
                shard_uid: shard_info.shard_uid,
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
    block_context: BlockContext,
    shard_uid: ShardUId,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    split_state_roots: SplitStateRoots,
    state_changes: StateChangesForSplitStates,
) -> Result<ApplyChunkResult, Error> {
    let shard_id = shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "split_state",
        shard_id,
        ?shard_uid)
    .entered();
    let next_epoch_id = epoch_manager.get_next_epoch_id(&block_context.block_hash)?;
    let next_epoch_shard_layout = epoch_manager.get_shard_layout(&next_epoch_id)?;
    let results = runtime.apply_update_to_split_states(
        &block_context.block_hash,
        block_context.height,
        split_state_roots,
        &next_epoch_shard_layout,
        state_changes,
    )?;
    Ok(ApplyChunkResult::StateSplit(StateSplitResult { shard_uid, results }))
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
    block_context: BlockContext,
    apply_result: &ApplyTransactionResult,
    split_state_roots: Option<SplitStateRoots>,
) -> Result<ApplySplitStateResultOrStateChanges, Error> {
    let state_changes = StateChangesForSplitStates::from_raw_state_changes(
        apply_result.trie_changes.state_changes(),
        apply_result.processed_delayed_receipts.clone(),
    );
    let next_epoch_shard_layout = {
        let next_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(&block_context.prev_block_hash)?;
        epoch_manager.get_shard_layout(&next_epoch_id)?
    };
    // split states are ready, apply update to them now
    if let Some(state_roots) = split_state_roots {
        let split_state_results = runtime_adapter.apply_update_to_split_states(
            &block_context.block_hash,
            block_context.height,
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
