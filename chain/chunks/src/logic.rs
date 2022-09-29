use near_chain::{validate::validate_chunk_proofs, Chain, ChainStore, RuntimeAdapter};
use near_chunks_primitives::Error;
use near_primitives::{
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
    runtime_adapter: &dyn RuntimeAdapter,
) -> bool {
    cares_about_shard_this_or_next_epoch(me, prev_block_hash, shard_id, true, runtime_adapter)
}

/// Returns true if we need this part to sign the block.
pub fn need_part(
    prev_block_hash: &CryptoHash,
    part_ord: u64,
    me: Option<&AccountId>,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<bool, Error> {
    let epoch_id = runtime_adapter.get_epoch_id_from_prev_block(prev_block_hash)?;
    Ok(Some(&runtime_adapter.get_part_owner(&epoch_id, part_ord)?) == me)
}

pub fn cares_about_shard_this_or_next_epoch(
    account_id: Option<&AccountId>,
    parent_hash: &CryptoHash,
    shard_id: ShardId,
    is_me: bool,
    runtime_adapter: &dyn RuntimeAdapter,
) -> bool {
    runtime_adapter.cares_about_shard(account_id, parent_hash, shard_id, is_me)
        || runtime_adapter.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
}

/// Constructs receipt proofs for specified chunk and returns them in an
/// iterator.
pub fn make_outgoing_receipts_proofs(
    chunk_header: &ShardChunkHeader,
    outgoing_receipts: &[Receipt],
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<impl Iterator<Item = ReceiptProof>, near_chunks_primitives::Error> {
    let shard_id = chunk_header.shard_id();
    let shard_layout =
        runtime_adapter.get_shard_layout_from_prev_block(chunk_header.prev_block_hash())?;

    let hashes = Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout);
    let (root, proofs) = merklize(&hashes);
    assert_eq!(chunk_header.outgoing_receipts_root(), root);

    let mut receipts_by_shard =
        Chain::group_receipts_by_shard(outgoing_receipts.to_vec(), &shard_layout);
    let it = proofs.into_iter().enumerate().map(move |(proof_shard_id, proof)| {
        let proof_shard_id = proof_shard_id as u64;
        let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
        let shard_proof =
            ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof: proof };
        ReceiptProof(receipts, shard_proof)
    });
    Ok(it)
}

pub fn make_partial_encoded_chunk_from_owned_parts_and_needed_receipts<'a>(
    header: &'a ShardChunkHeader,
    parts: impl Iterator<Item = &'a PartialEncodedChunkPart>,
    receipts: impl Iterator<Item = &'a ReceiptProof>,
    me: Option<&AccountId>,
    runtime_adapter: &dyn RuntimeAdapter,
) -> PartialEncodedChunk {
    let prev_block_hash = header.prev_block_hash();
    let cares_about_shard = cares_about_shard_this_or_next_epoch(
        me,
        prev_block_hash,
        header.shard_id(),
        true,
        runtime_adapter,
    );
    let parts = parts
        .filter(|entry| {
            cares_about_shard
                || need_part(prev_block_hash, entry.part_ord, me, runtime_adapter).unwrap_or(false)
        })
        .cloned()
        .collect();
    let receipts = receipts
        .filter(|receipt| {
            cares_about_shard
                || need_receipt(prev_block_hash, receipt.1.to_shard_id, me, runtime_adapter)
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
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<(ShardChunk, PartialEncodedChunk), Error> {
    let chunk_hash = encoded_chunk.chunk_hash();

    if let Ok(shard_chunk) = encoded_chunk
        .decode_chunk(runtime_adapter.num_data_parts())
        .map_err(|err| Error::from(err))
        .and_then(|shard_chunk| {
            if !validate_chunk_proofs(&shard_chunk, runtime_adapter)? {
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
            runtime_adapter,
        )?;

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
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<PartialEncodedChunk, Error> {
    let header = encoded_chunk.cloned_header();
    let receipts =
        make_outgoing_receipts_proofs(&header, &outgoing_receipts, runtime_adapter)?.collect();
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
        runtime_adapter,
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
