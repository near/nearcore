use near_chain::{types::EpochManagerAdapter, validate::validate_chunk_proofs, Chain, ChainStore};
use near_chunks_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::{
    errors::EpochError,
    hash::CryptoHash,
    merkle::{merklize, MerklePath},
    receipt::Receipt,
    sharding::{
        EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart, PartialEncodedChunkV1,
        PartialEncodedChunkV2, ReceiptProof, ShardChunk, ShardChunkHeader, ShardProof,
    },
    types::{AccountId, ShardId},
};
use tracing::log::{debug, error};

pub fn need_receipt(
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
    me: Option<&AccountId>,
    shard_tracker: &ShardTracker,
) -> bool {
    cares_about_shard_this_or_next_epoch(me, prev_block_hash, shard_id, true, shard_tracker)
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

pub fn cares_about_shard_this_or_next_epoch(
    account_id: Option<&AccountId>,
    parent_hash: &CryptoHash,
    shard_id: ShardId,
    is_me: bool,
    shard_tracker: &ShardTracker,
) -> bool {
    // TODO(robin-near): I think we only need the shard_tracker if is_me is false.
    shard_tracker.care_about_shard(account_id, parent_hash, shard_id, is_me)
        || shard_tracker.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
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
    outgoing_receipts: &[Receipt],
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<impl Iterator<Item = ReceiptProof>, EpochError> {
    let shard_id = chunk_header.shard_id();
    let shard_layout =
        epoch_manager.get_shard_layout_from_prev_block(chunk_header.prev_block_hash())?;

    let hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout);
    let (root, proofs) = merklize(&hashes);
    assert_eq!(chunk_header.outgoing_receipts_root(), root);

    let mut receipts_by_shard =
        Chain::group_receipts_by_shard(outgoing_receipts.to_vec(), &shard_layout);
    let it = proofs.into_iter().enumerate().map(move |(proof_shard_id, proof)| {
        let proof_shard_id = proof_shard_id as u64;
        let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
        let shard_proof =
            ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
        ReceiptProof(receipts, shard_proof)
    });
    Ok(it)
}

pub fn make_partial_encoded_chunk_from_owned_parts_and_needed_receipts<'a>(
    header: &'a ShardChunkHeader,
    parts: impl Iterator<Item = &'a PartialEncodedChunkPart>,
    receipts: impl Iterator<Item = &'a ReceiptProof>,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
) -> PartialEncodedChunk {
    let prev_block_hash = header.prev_block_hash();
    let cares_about_shard = cares_about_shard_this_or_next_epoch(
        me,
        prev_block_hash,
        header.shard_id(),
        true,
        shard_tracker,
    );
    let parts = parts
        .filter(|entry| {
            cares_about_shard
                || need_part(prev_block_hash, entry.part_ord, me, epoch_manager).unwrap_or(false)
        })
        .cloned()
        .collect();
    let receipts = receipts
        .filter(|receipt| {
            cares_about_shard
                || need_receipt(prev_block_hash, receipt.1.to_shard_id, me, shard_tracker)
        })
        .cloned()
        .collect();
    match header.clone() {
        ShardChunkHeader::V1(header) => {
            PartialEncodedChunk::V1(PartialEncodedChunkV1 { header, parts, receipts })
        }
        header => PartialEncodedChunk::V2(PartialEncodedChunkV2 { header, parts, receipts }),
    }
}

pub fn decode_encoded_chunk(
    encoded_chunk: &EncodedShardChunk,
    merkle_paths: Vec<MerklePath>,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
) -> Result<(ShardChunk, PartialEncodedChunk), Error> {
    let chunk_hash = encoded_chunk.chunk_hash();

    if let Ok(shard_chunk) = encoded_chunk
        .decode_chunk(epoch_manager.num_data_parts())
        .map_err(|err| Error::from(err))
        .and_then(|shard_chunk| {
            if !validate_chunk_proofs(&shard_chunk, epoch_manager)? {
                return Err(Error::InvalidChunk);
            }
            Ok(shard_chunk)
        })
    {
        debug!(target: "chunks", "Reconstructed and decoded chunk {}, encoded length was {}, num txs: {}, I'm {:?}", chunk_hash.0, encoded_chunk.encoded_length(), shard_chunk.transactions().len(), me);

        let partial_chunk = create_partial_chunk(
            encoded_chunk,
            merkle_paths,
            shard_chunk.receipts().to_vec(),
            me,
            epoch_manager,
            shard_tracker,
        )
        .map_err(|err| Error::ChainError(err.into()))?;

        return Ok((shard_chunk, partial_chunk));
    } else {
        // Can't decode chunk or has invalid proofs, ignore it
        error!(target: "chunks", "Reconstructed, but failed to decoded chunk {}, I'm {:?}", chunk_hash.0, me);
        return Err(Error::InvalidChunk);
    }
}

fn create_partial_chunk(
    encoded_chunk: &EncodedShardChunk,
    merkle_paths: Vec<MerklePath>,
    outgoing_receipts: Vec<Receipt>,
    me: Option<&AccountId>,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
) -> Result<PartialEncodedChunk, EpochError> {
    let header = encoded_chunk.cloned_header();
    let receipts =
        make_outgoing_receipts_proofs(&header, &outgoing_receipts, epoch_manager)?.collect();
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
        receipts,
    };

    Ok(make_partial_encoded_chunk_from_owned_parts_and_needed_receipts(
        &partial_chunk.header,
        partial_chunk.parts.iter(),
        partial_chunk.receipts.iter(),
        me,
        epoch_manager,
        shard_tracker,
    ))
}

pub fn persist_chunk(
    partial_chunk: PartialEncodedChunk,
    shard_chunk: Option<ShardChunk>,
    store: &mut ChainStore,
) -> Result<(), Error> {
    let mut update = store.store_update();
    update.save_partial_chunk(partial_chunk);
    if let Some(shard_chunk) = shard_chunk {
        update.save_chunk(shard_chunk);
    }
    update.commit()?;
    Ok(())
}
