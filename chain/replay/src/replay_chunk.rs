use near_chain::chain::collect_receipts_from_response;
use near_chain::types::{ApplyChunkResult, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{
    NewChunkData, NewChunkResult, OldChunkData, OldChunkResult, ShardContext, ShardUpdateReason,
    ShardUpdateResult, StorageContext, process_shard_update,
};
use near_chain::{
    Chain, ChainStore, ChainStoreAccess, Error, ReceiptFilter, get_incoming_receipts_for_shard,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_index;
use near_primitives::hash::CryptoHash;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::ShardUId;
use node_runtime::SignedValidPeriodTransactions;

/// Result of replaying a single chunk. Contains both the expected ChunkExtra
/// (read from the database) and the actual ChunkExtra (produced by replay).
/// The caller decides how to interpret differences.
pub struct ChunkReplayResult {
    pub expected: ChunkExtra,
    pub actual: ChunkExtra,
    pub apply_result: ApplyChunkResult,
}

impl ChunkReplayResult {
    /// Returns `Ok(())` if the actual ChunkExtra matches the expected one,
    /// or an error describing the mismatch otherwise.
    pub fn verify(&self) -> Result<(), Error> {
        if self.expected != self.actual {
            return Err(Error::Other(format!(
                "chunk extra mismatch:\nexpected: {:#?}\nactual:   {:#?}",
                self.expected, self.actual,
            )));
        }
        Ok(())
    }
}

/// Replays a single chunk and returns the expected and actual ChunkExtras.
///
/// All data needed for replay (block, prev block, chunk, receipts, etc.)
/// is fetched from the chain store. The caller only needs to provide the
/// block hash and shard UID identifying which chunk to replay.
#[tracing::instrument(
    target = "replay",
    level = "debug",
    skip_all,
    fields(%block_hash, %shard_uid),
)]
pub fn replay_chunk(
    chain_store: &ChainStore,
    runtime: &dyn RuntimeAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
    shard_uid: ShardUId,
    storage_data_source: StorageDataSource,
) -> Result<ChunkReplayResult, Error> {
    let span = tracing::Span::current();

    let block = chain_store.get_block(block_hash)?;
    let block_header = block.header();
    let height = block_header.height();

    let prev_block = chain_store.get_block(block_header.prev_hash())?;

    let shard_id = shard_uid.shard_id();
    let epoch_id = block_header.epoch_id();
    let shard_index = shard_id_to_index(epoch_manager, shard_id, epoch_id)?;

    let chunk_header = block.chunks()[shard_index].clone();
    let prev_chunk_header = epoch_manager.get_prev_chunk_header(&prev_block, shard_id)?;

    let is_new_chunk = chunk_header.is_new_chunk(height);

    let prev_chunk_extra = chain_store.get_chunk_extra(block_header.prev_hash(), &shard_uid)?;

    let shard_context = ShardContext { shard_uid, should_apply_chunk: true };
    let storage_context = StorageContext { storage_data_source, state_patch: Default::default() };

    let block_context =
        Chain::get_apply_chunk_block_context(&block, prev_block.header(), is_new_chunk);

    let update_reason = if is_new_chunk {
        let chunk = chain_store.get_chunk(&chunk_header.chunk_hash())?;

        let shard_layout =
            epoch_manager.get_shard_layout_from_prev_block(block_header.prev_hash())?;
        let receipt_response = get_incoming_receipts_for_shard(
            chain_store,
            epoch_manager,
            shard_id,
            &shard_layout,
            *block_header.hash(),
            prev_chunk_header.height_included(),
            ReceiptFilter::TargetShard,
        )?;
        let receipts = collect_receipts_from_response(&receipt_response);

        let transactions = SignedValidPeriodTransactions::new(
            chunk.to_transactions().to_vec(),
            vec![true; chunk.to_transactions().len()],
        );

        ShardUpdateReason::NewChunk(NewChunkData {
            gas_limit: chunk_header.gas_limit(),
            prev_state_root: chunk_header.prev_state_root(),
            prev_validator_proposals: chunk_header.prev_validator_proposals().collect(),
            chunk_hash: Some(chunk_header.chunk_hash().clone()),
            transactions,
            receipts,
            block: block_context,
            storage_context,
        })
    } else {
        ShardUpdateReason::OldChunk(OldChunkData {
            block: block_context,
            prev_chunk_extra: ChunkExtra::clone(prev_chunk_extra.as_ref()),
            storage_context,
        })
    };

    let shard_update_result =
        process_shard_update(&span, runtime, update_reason, shard_context, None)?;

    let (actual_chunk_extra, apply_result) = match shard_update_result {
        ShardUpdateResult::NewChunk(NewChunkResult { apply_result, .. }) => {
            let chunk_extra = apply_result.to_chunk_extra(chunk_header.gas_limit());
            (chunk_extra, apply_result)
        }
        ShardUpdateResult::OldChunk(OldChunkResult { apply_result, .. }) => {
            let mut chunk_extra = ChunkExtra::clone(prev_chunk_extra.as_ref());
            *chunk_extra.state_root_mut() = apply_result.new_root;
            (chunk_extra, apply_result)
        }
    };

    let expected_chunk_extra = chain_store.get_chunk_extra(block_header.hash(), &shard_uid)?;

    Ok(ChunkReplayResult {
        expected: ChunkExtra::clone(&expected_chunk_extra),
        actual: actual_chunk_extra,
        apply_result,
    })
}
