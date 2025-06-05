use near_chain::ChainStoreAccess;
use near_chain::{BlockHeader, Chain, ChainStore, types::EpochManagerAdapter};
use near_chunks_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::sharding::ShardChunkWithEncoding;
use near_primitives::{
    errors::EpochError,
    hash::CryptoHash,
    merkle::{MerklePath, merklize},
    receipt::Receipt,
    sharding::{
        PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1, PartialEncodedChunkV2,
        ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof,
    },
    types::{AccountId, ShardId},
};
use std::sync::Arc;
use tracing::debug_span;

pub fn need_receipt(
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
    me: Option<&AccountId>,
    shard_tracker: &ShardTracker,
) -> bool {
    shard_tracker.cares_about_shard_this_or_next_epoch(me, prev_block_hash, shard_id, true)
}

/// Returns true if we need this part to sign the block.
pub fn need_part(
    prev_block_hash: &CryptoHash,
    part_ord: u64,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<bool, EpochError> {
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
    Ok(Some(&epoch_manager.get_part_owner(&epoch_id, part_ord)?) == me)
}

pub fn get_shards_cares_about_this_or_next_epoch(
    account_id: Option<&AccountId>,
    is_me: bool,
    block_header: &BlockHeader,
    shard_tracker: &ShardTracker,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Vec<ShardId> {
    epoch_manager
        .shard_ids(&block_header.epoch_id())
        .unwrap()
        .into_iter()
        .filter(|&shard_id| {
            shard_tracker.cares_about_shard_this_or_next_epoch(
                account_id,
                block_header.prev_hash(),
                shard_id,
                is_me,
            )
        })
        .collect()
}

pub fn chunk_needs_to_be_fetched_from_archival(
    chunk_prev_block_hash: &CryptoHash,
    header_head: &CryptoHash,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<bool, EpochError> {
    let head_epoch_id = epoch_manager.get_epoch_id(header_head)?;
    let head_next_epoch_id = epoch_manager.get_next_epoch_id(header_head)?;
    let chunk_epoch_id = epoch_manager.get_epoch_id_from_prev_block(chunk_prev_block_hash)?;
    let chunk_next_epoch_id =
        epoch_manager.get_next_epoch_id_from_prev_block(chunk_prev_block_hash)?;

    // `chunk_epoch_id != head_epoch_id && chunk_next_epoch_id != head_epoch_id` covers the
    // common case: the chunk is in the current epoch, or in the previous epoch, relative to the
    // header head. The third condition (`chunk_epoch_id != head_next_epoch_id`) covers a
    // corner case, in which the `header_head` is the last block of an epoch, and the chunk is
    // for the next block. In this case the `chunk_epoch_id` will be one epoch ahead of the
    // `header_head`.
    Ok(chunk_epoch_id != head_epoch_id
        && chunk_next_epoch_id != head_epoch_id
        && chunk_epoch_id != head_next_epoch_id)
}

/// Constructs receipt proofs for specified chunk and returns them in an
/// iterator.
pub fn make_outgoing_receipts_proofs(
    chunk_header: &ShardChunkHeader,
    outgoing_receipts: Vec<Receipt>,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<Vec<ReceiptProof>, EpochError> {
    let shard_id = chunk_header.shard_id();
    let shard_layout =
        epoch_manager.get_shard_layout_from_prev_block(chunk_header.prev_block_hash())?;

    let hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)?;
    let (root, proofs) = merklize(&hashes);
    assert_eq!(chunk_header.prev_outgoing_receipts_root(), root);

    let mut receipts_by_shard = Chain::group_receipts_by_shard(outgoing_receipts, &shard_layout)?;
    let mut result = vec![];
    for (proof_shard_index, proof) in proofs.into_iter().enumerate() {
        let proof_shard_id = shard_layout.get_shard_id(proof_shard_index)?;
        let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
        let shard_proof =
            ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
        result.push(ReceiptProof(receipts, shard_proof));
    }
    Ok(result)
}

pub fn make_partial_encoded_chunk_from_owned_parts_and_needed_receipts(
    header: ShardChunkHeader,
    parts: impl Iterator<Item = PartialEncodedChunkPart>,
    receipts: impl Iterator<Item = ReceiptProof>,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
) -> PartialEncodedChunk {
    let prev_block_hash = header.prev_block_hash();
    let cares_about_shard = shard_tracker.cares_about_shard_this_or_next_epoch(
        me,
        prev_block_hash,
        header.shard_id(),
        true,
    );
    let parts = parts
        .filter(|entry| {
            cares_about_shard
                || need_part(prev_block_hash, entry.part_ord, me, epoch_manager).unwrap_or(false)
        })
        .collect();
    let mut prev_outgoing_receipts = receipts
        .filter(|receipt| {
            cares_about_shard
                || need_receipt(prev_block_hash, receipt.1.to_shard_id, me, shard_tracker)
        })
        .collect::<Vec<_>>();
    // Make sure the receipts are in a deterministic order.
    prev_outgoing_receipts.sort();
    match header {
        ShardChunkHeader::V1(header) => {
            PartialEncodedChunk::V1(PartialEncodedChunkV1 { header, parts, prev_outgoing_receipts })
        }
        header => {
            PartialEncodedChunk::V2(PartialEncodedChunkV2 { header, parts, prev_outgoing_receipts })
        }
    }
}

pub fn create_partial_chunk(
    chunk: &ShardChunkWithEncoding,
    merkle_paths: Vec<MerklePath>,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
) -> Result<PartialEncodedChunk, Error> {
    let encoded_chunk = chunk.to_encoded_shard_chunk();
    let header = encoded_chunk.cloned_header();

    let span = debug_span!(
        target: "chunks",
        "create_partial_chunk",
        height_included = header.height_included(),
        shard_id = ?header.shard_id(),
        encoded_length = tracing::field::Empty,
        num_tx = tracing::field::Empty,
        me = me.map(tracing::field::debug),
        hash = ?encoded_chunk.chunk_hash())
    .entered();

    let shard_chunk = chunk.to_shard_chunk();
    let outgoing_receipts = shard_chunk.prev_outgoing_receipts().to_vec();
    let prev_outgoing_receipts =
        make_outgoing_receipts_proofs(&header, outgoing_receipts, epoch_manager)?;

    span.record("encoded_length", encoded_chunk.encoded_length());
    span.record("num_tx", shard_chunk.to_transactions().len());

    let partial_chunk = PartialEncodedChunkV2 {
        header,
        parts: encoded_chunk
            .content()
            .parts
            .clone()
            .into_iter()
            .zip(merkle_paths)
            .enumerate()
            .map(|(part_ord, (part, merkle_proof))| {
                let part_ord = part_ord as u64;
                let part = part.unwrap();
                PartialEncodedChunkPart { part_ord, part, merkle_proof }
            })
            .collect(),
        prev_outgoing_receipts,
    };

    Ok(make_partial_encoded_chunk_from_owned_parts_and_needed_receipts(
        partial_chunk.header,
        partial_chunk.parts.into_iter(),
        partial_chunk.prev_outgoing_receipts.into_iter(),
        me,
        epoch_manager,
        shard_tracker,
    ))
}

pub fn persist_chunk(
    partial_chunk: Arc<PartialEncodedChunk>,
    shard_chunk: Option<ShardChunk>,
    store: &mut ChainStore,
) -> Result<(), Error> {
    let mut update = store.store_update();
    if update.get_partial_chunk(&partial_chunk.chunk_hash()).is_err() {
        update.save_partial_chunk(partial_chunk);
    }
    if let Some(shard_chunk) = shard_chunk {
        if !update.chunk_exists(&shard_chunk.chunk_hash())? {
            update.save_chunk(shard_chunk);
        }
    }
    update.commit().map_err(Error::from)
}
