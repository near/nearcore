use crate::crypto_hash_timer::CryptoHashTimer;
use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext, RuntimeAdapter,
    RuntimeStorageConfig, StorageDataSource,
};
use near_async::time::Clock;
use near_chain_primitives::Error;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::receipt::Receipt;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::Gas;

/// Result of updating a shard for some block when it has a new chunk for this
/// shard.
#[derive(Debug)]
pub struct NewChunkResult {
    pub shard_uid: ShardUId,
    pub gas_limit: Gas,
    pub apply_result: ApplyChunkResult,
}

/// Result of updating a shard for some block when it doesn't have a new chunk
/// for this shard, so previous chunk header is copied.
#[derive(Debug)]
pub struct OldChunkResult {
    pub shard_uid: ShardUId,
    /// Note that despite the naming, no transactions are applied in this case.
    /// TODO(logunov): exclude receipts/txs context from all related types.
    pub apply_result: ApplyChunkResult,
}

/// Result for a shard update for a single block.
#[derive(Debug)]
pub enum ShardUpdateResult {
    NewChunk(NewChunkResult),
    OldChunk(OldChunkResult),
}

pub struct NewChunkData {
    pub chunk_header: ShardChunkHeader,
    pub transactions: Vec<SignedTransaction>,
    pub receipts: Vec<Receipt>,
    pub block: ApplyChunkBlockContext,
    pub is_first_block_with_chunk_of_version: bool,
    pub storage_context: StorageContext,
}

pub struct OldChunkData {
    pub prev_chunk_extra: ChunkExtra,
    pub block: ApplyChunkBlockContext,
    pub storage_context: StorageContext,
}

/// Reason to update a shard when new block appears on chain.
#[allow(clippy::large_enum_variant)]
pub enum ShardUpdateReason {
    /// Block has a new chunk for the shard.
    /// Contains chunk itself and all new incoming receipts to the shard.
    NewChunk(NewChunkData),
    /// Block doesn't have a new chunk for the shard.
    /// Instead, previous chunk header is copied.
    /// Contains result of shard update for previous block.
    OldChunk(OldChunkData),
}

/// Information about shard to update.
pub struct ShardContext {
    pub shard_uid: ShardUId,
    /// Whether node cares about shard in this epoch.
    pub cares_about_shard_this_epoch: bool,
    /// Whether shard layout changes in the next epoch.
    pub will_shard_layout_change: bool,
    /// Whether transactions should be applied.
    pub should_apply_chunk: bool,
}

/// Information about storage used for applying txs and receipts.
pub struct StorageContext {
    /// Data source used for processing shard update.
    pub storage_data_source: StorageDataSource,
    pub state_patch: SandboxStatePatch,
}

/// Processes shard update with given block and shard.
/// Doesn't modify chain, only produces result to be applied later.
pub fn process_shard_update(
    parent_span: &tracing::Span,
    runtime: &dyn RuntimeAdapter,
    shard_update_reason: ShardUpdateReason,
    shard_context: ShardContext,
) -> Result<ShardUpdateResult, Error> {
    Ok(match shard_update_reason {
        ShardUpdateReason::NewChunk(data) => ShardUpdateResult::NewChunk(apply_new_chunk(
            ApplyChunkReason::UpdateTrackedShard,
            parent_span,
            data,
            shard_context,
            runtime,
        )?),
        ShardUpdateReason::OldChunk(data) => ShardUpdateResult::OldChunk(apply_old_chunk(
            ApplyChunkReason::UpdateTrackedShard,
            parent_span,
            data,
            shard_context,
            runtime,
        )?),
    })
}

/// Applies new chunk, which includes applying transactions from chunk and
/// receipts filtered from outgoing receipts from previous chunks.
pub fn apply_new_chunk(
    apply_reason: ApplyChunkReason,
    parent_span: &tracing::Span,
    data: NewChunkData,
    shard_context: ShardContext,
    runtime: &dyn RuntimeAdapter,
) -> Result<NewChunkResult, Error> {
    let NewChunkData {
        chunk_header,
        transactions,
        block,
        receipts,
        is_first_block_with_chunk_of_version,
        storage_context,
    } = data;
    let shard_id = shard_context.shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "apply_new_chunk",
        shard_id,
        ?apply_reason)
    .entered();
    let gas_limit = chunk_header.gas_limit();

    let _timer = CryptoHashTimer::new(Clock::real(), chunk_header.chunk_hash().0);
    let storage_config = RuntimeStorageConfig {
        state_root: chunk_header.prev_state_root(),
        use_flat_storage: true,
        source: storage_context.storage_data_source,
        state_patch: storage_context.state_patch,
    };
    match runtime.apply_chunk(
        storage_config,
        apply_reason,
        ApplyChunkShardContext {
            shard_id,
            last_validator_proposals: chunk_header.prev_validator_proposals(),
            gas_limit,
            is_new_chunk: true,
            is_first_block_with_chunk_of_version,
        },
        block,
        &receipts,
        &transactions,
    ) {
        Ok(apply_result) => {
            Ok(NewChunkResult { gas_limit, shard_uid: shard_context.shard_uid, apply_result })
        }
        Err(err) => Err(err),
    }
}

/// Applies shard update corresponding to missing chunk.
/// (logunov) From what I know, the state update may include only validator
/// accounts update on epoch start.
pub fn apply_old_chunk(
    apply_reason: ApplyChunkReason,
    parent_span: &tracing::Span,
    data: OldChunkData,
    shard_context: ShardContext,
    runtime: &dyn RuntimeAdapter,
) -> Result<OldChunkResult, Error> {
    let OldChunkData { prev_chunk_extra, block, storage_context } = data;
    let shard_id = shard_context.shard_uid.shard_id();
    let _span = tracing::debug_span!(
        target: "chain",
        parent: parent_span,
        "apply_old_chunk",
        shard_id,
        ?apply_reason)
    .entered();

    let storage_config = RuntimeStorageConfig {
        state_root: *prev_chunk_extra.state_root(),
        use_flat_storage: true,
        source: storage_context.storage_data_source,
        state_patch: storage_context.state_patch,
    };
    match runtime.apply_chunk(
        storage_config,
        apply_reason,
        ApplyChunkShardContext {
            shard_id,
            last_validator_proposals: prev_chunk_extra.validator_proposals(),
            gas_limit: prev_chunk_extra.gas_limit(),
            is_new_chunk: false,
            is_first_block_with_chunk_of_version: false,
        },
        block,
        &[],
        &[],
    ) {
        Ok(apply_result) => Ok(OldChunkResult { shard_uid: shard_context.shard_uid, apply_result }),
        Err(err) => Err(err),
    }
}
