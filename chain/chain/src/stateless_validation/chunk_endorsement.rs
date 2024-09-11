use std::collections::HashSet;

use itertools::Itertools;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;

/// This function validates the chunk_endorsements present in the block body. Validation does the following:
///    - Match number of chunks/shards with number of chunk endorsements vector.
///    - For each chunk/shard, do the following:
///       - Match number of chunk validators with number of chunk endorsement signatures.
///       - Verify that the signature are valid for given chunk_validator index.
///         Essentially, signature[i] should correspond with ordered_chunk_validators[i].
///       - Verify that the chunk has enough stake from chunk_validators to be endorsed.
/// Note that while getting the chunk_validator_assignments, we use the chunk_header.height_created field.
/// This is because chunk producer sends state witness to chunk validators assignment for `height_created`.
/// We expect the endorsements to come from these chunk validators only.
/// The chunk can later be included in block at any height which is recorded in `height_included` field.
pub fn validate_chunk_endorsements_in_block(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    // Number of chunks and chunk endorsements must match and must be equal to number of shards
    if block.chunks().len() != block.chunk_endorsements().len() {
        tracing::error!(
            target: "chain",
            num_chunks = block.chunks().len(),
            num_chunk_endorsement_shards = block.chunk_endorsements().len(),
            "Number of chunks and chunk endorsements does not match",
        );
        return Err(Error::InvalidChunkEndorsement);
    }

    // Validate the chunk endorsements bitmap (if present) in the block header against the endorsement signatures in the body.
    let endorsements_bitmap = block.header().chunk_endorsements();
    if let Some(endorsements_bitmap) = endorsements_bitmap {
        if endorsements_bitmap.num_shards() != block.chunk_endorsements().len() {
            return Err(Error::InvalidChunkEndorsementBitmap(
                format!("Number of shards in bitmap and chunk endorsement signatures do not match: shards={}, signatures={}",
                    endorsements_bitmap.num_shards(), block.chunk_endorsements().len())));
        }
    }

    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(block.header().prev_hash())?;
    for (chunk_header, signatures) in block.chunks().iter().zip(block.chunk_endorsements()) {
        let shard_id = chunk_header.shard_id();
        // For old chunks, we optimize the block by not including the chunk endorsements.
        if chunk_header.height_included() != block.header().height() {
            if !signatures.is_empty() {
                tracing::error!(
                    target: "chain",
                    chunk_header_height_included = chunk_header.height_included(),
                    block_header_height = block.header().height(),
                    "Expected chunk endorsements to be empty for old chunks in current block",
                );
                return Err(Error::InvalidChunkEndorsement);
            }
            continue;
        }
        // Validation for chunks in each shard
        // The signatures from chunk validators for each shard must match the ordered_chunk_validators
        let chunk_validator_assignments = epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            shard_id,
            chunk_header.height_created(),
        )?;
        let ordered_chunk_validators = chunk_validator_assignments.ordered_chunk_validators();
        if ordered_chunk_validators.len() != signatures.len() {
            tracing::error!(
                target: "chain",
                num_ordered_chunk_validators = ordered_chunk_validators.len(),
                num_chunk_endorsement_signatures = signatures.len(),
                "Number of ordered chunk validators and chunk endorsement signatures does not match",
            );
            return Err(Error::InvalidChunkEndorsement);
        }

        // Verify that the signature in block body are valid for given chunk_validator.
        // Signature can be either None, or Some(signature).
        // We calculate the stake of the chunk_validators for who we have the signature present.
        let mut endorsed_chunk_validators = HashSet::new();
        for (account_id, signature) in ordered_chunk_validators.iter().zip(signatures) {
            let Some(signature) = signature else { continue };
            let (validator, _) = epoch_manager.get_validator_by_account_id(
                &epoch_id,
                block.header().prev_hash(),
                account_id,
            )?;

            // Block should not be produced with an invalid signature.
            if !ChunkEndorsement::validate_signature(
                chunk_header.chunk_hash(),
                signature,
                validator.public_key(),
            ) {
                tracing::error!(
                    target: "chain",
                    "Invalid chunk endorsement signature for chunk {:?} and validator {:?}",
                    chunk_header.chunk_hash(),
                    validator.account_id(),
                );
                return Err(Error::InvalidChunkEndorsement);
            }

            // Add validators with signature in endorsed_chunk_validators. We later use this to check stake.
            endorsed_chunk_validators.insert(account_id);
        }

        let endorsement_stats =
            chunk_validator_assignments.compute_endorsement_stats(&endorsed_chunk_validators);
        if !endorsement_stats.has_enough_stake() {
            tracing::error!(target: "chain", ?endorsement_stats, "Chunk does not have enough stake to be endorsed");
            return Err(Error::InvalidChunkEndorsement);
        }

        // Validate the chunk endorsements bitmap (if present) in the block header against the endorsement signatures in the body.
        if let Some(endorsements_bitmap) = endorsements_bitmap {
            // Bitmap's length must be equal to the min bytes needed to encode one bit per validator assignment.
            if endorsements_bitmap.len(shard_id).unwrap() != signatures.len().div_ceil(8) * 8 {
                return Err(Error::InvalidChunkEndorsementBitmap(format!(
                    "Bitmap's length {} is inconsistent with the number of signatures {} for shard {} ",
                    endorsements_bitmap.len(shard_id).unwrap(), signatures.len(), shard_id,
                )));
            }
            // Bits in the bitmap must match the existence of signature for the corresponding validator in the body.
            for (bit, signature) in endorsements_bitmap.iter(shard_id).zip(signatures.iter()) {
                if bit != signature.is_some() {
                    return Err(Error::InvalidChunkEndorsementBitmap(
                        format!("Chunk endorsement bit in header does not match endorsement in body. shard={}, bit={}, signature={}",
                        shard_id, bit, signature.is_some())));
                }
            }
            // All extra positions after the assignments must be left as false.
            for value in endorsements_bitmap.iter(shard_id).skip(signatures.len()) {
                if value {
                    return Err(Error::InvalidChunkEndorsementBitmap(
                        format!("Extra positions in the bitmap after {} validator assignments are not all false for shard {}",
                        signatures.len(), shard_id)));
                }
            }
        }
    }
    Ok(())
}

/// Validates the [`ChunkEndorsementBitmap`] in the [`BlockHeader`] if it is present, otherwise returns an error.
/// This function must be called only if ChunkEndorsementInBlockHeader feature is enabled.
pub fn validate_chunk_endorsements_in_header(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<(), Error> {
    let Some(chunk_endorsements) = header.chunk_endorsements() else {
        return Err(Error::InvalidChunkEndorsementBitmap(format!(
            "Expected chunk endorsements bitmap but found None at height {}",
            header.height()
        )));
    };
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header.prev_hash())?;
    let shard_ids = epoch_manager.get_shard_layout(&epoch_id)?.shard_ids().collect_vec();
    if chunk_endorsements.num_shards() != shard_ids.len() {
        return Err(Error::InvalidChunkEndorsementBitmap(
            format!("Number of shards in bitmap and in epoch do not match: shards in bitmap={}, shards in epoch={}",
                chunk_endorsements.num_shards(), shard_ids.len())));
    }
    let chunk_mask = header.chunk_mask();
    for shard_id in shard_ids.into_iter() {
        // For old chunks, we optimize the block and its header by not including the chunk endorsements and
        // corresponding bitmaps. Thus, we expect that the bitmap is empty for shard with no new chunk.
        if chunk_mask[shard_id as usize] != (chunk_endorsements.len(shard_id).unwrap() > 0) {
            return Err(Error::InvalidChunkEndorsementBitmap(format!(
                "Bitmap must be non-empty iff shard {} has new chunk in the block. Chunk mask={}, Bitmap length={}",
                shard_id, chunk_mask[shard_id as usize], chunk_endorsements.len(shard_id).unwrap(),
            )));
        }
    }
    Ok(())
}
