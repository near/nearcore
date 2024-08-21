use super::partial_witness::witness_part_length;
use itertools::Itertools;
use near_chain::types::Tip;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsementV2;
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitness, MAX_COMPRESSED_STATE_WITNESS_SIZE,
};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::{AccountId, BlockHeightDelta};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::{DBCol, Store, FINAL_HEAD_KEY, HEAD_KEY};

/// This is taken to be the same value as near_chunks::chunk_cache::MAX_HEIGHTS_AHEAD, and we
/// reject partial witnesses with height more than this value above the height of our current HEAD
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

/// Function to validate the partial encoded state witness. In addition of ChunkProductionKey, we check the following:
/// - part_ord is valid and within range of the number of expected parts for this chunk
/// - partial_witness signature is valid and from the expected chunk_producer
/// TODO(stateless_validation): Include checks from handle_orphan_state_witness in orphan_witness_handling.rs
/// These include checks based on epoch_id validity, witness size, height_created, distance from chain head, etc.
pub fn validate_partial_encoded_state_witness(
    epoch_manager: &dyn EpochManagerAdapter,
    partial_witness: &PartialEncodedStateWitness,
    signer: &ValidatorSigner,
    store: &Store,
) -> Result<bool, Error> {
    let ChunkProductionKey { shard_id, epoch_id, height_created } =
        partial_witness.chunk_production_key();
    let num_parts =
        epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?.len();
    if partial_witness.part_ord() >= num_parts {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "Invalid part_ord in PartialEncodedStateWitness: {}",
            partial_witness.part_ord()
        )));
    }

    let max_part_len =
        witness_part_length(MAX_COMPRESSED_STATE_WITNESS_SIZE.as_u64() as usize, num_parts);
    if partial_witness.part_size() > max_part_len {
        return Err(Error::InvalidPartialChunkStateWitness(format!(
            "Part size {} exceed limit of {} (total parts: {})",
            partial_witness.part_size(),
            max_part_len,
            num_parts
        )));
    }

    if !validate_chunk_production_key(
        epoch_manager,
        partial_witness.chunk_production_key(),
        signer.validator_id(),
        store,
    )? {
        return Ok(false);
    }

    if !epoch_manager.verify_partial_witness_signature(&partial_witness)? {
        return Err(Error::InvalidPartialChunkStateWitness("Invalid signature".to_string()));
    }

    Ok(true)
}

/// Function to validate the chunk endorsement. In addition of ChunkProductionKey, we check the following:
/// - signature of endorsement and metadata is valid
pub fn validate_chunk_endorsement(
    epoch_manager: &dyn EpochManagerAdapter,
    endorsement: &ChunkEndorsementV2,
    store: &Store,
) -> Result<bool, Error> {
    if !validate_chunk_production_key(
        epoch_manager,
        endorsement.chunk_production_key(),
        endorsement.account_id(),
        store,
    )? {
        return Ok(false);
    }

    if !epoch_manager.verify_chunk_endorsement_signature(endorsement)? {
        return Err(Error::InvalidChunkEndorsement);
    }

    Ok(true)
}

/// Function to validate ChunkProductionKey. We check the following:
/// - shard_id is valid
/// - account_id is one of the validators for the chunk
/// - height_created is in (last_final_height..chain_head_height + MAX_HEIGHTS_AHEAD] range
/// - epoch_id is within epoch_manager's possible_epochs_of_height_around_tip
/// Returns:
/// - Ok(true) if ChunkProductionKey is valid and we should process it.
/// - Ok(false) if ChunkProductionKey is potentially valid, but at this point we should not
///   process it. One example of that is if the witness is too old.
/// - Err if ChunkProductionKey is invalid which most probably indicates malicious behavior.
fn validate_chunk_production_key(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_production_key: ChunkProductionKey,
    account_id: &AccountId,
    store: &Store,
) -> Result<bool, Error> {
    let shard_id = chunk_production_key.shard_id;
    let epoch_id = chunk_production_key.epoch_id;
    let height_created = chunk_production_key.height_created;

    if !epoch_manager.get_shard_layout(&epoch_id)?.shard_ids().contains(&shard_id) {
        tracing::error!(
            target: "stateless_validation",
            ?chunk_production_key,
            "ShardId is not in the shard layout of the epoch",
        );
        return Err(Error::InvalidShardId(shard_id));
    }

    // Reject witnesses/endorsements for chunks for which the account_id isn't a validator.
    // It's an error, as chunk producer shouldn't send the witness/endorsement to/from a non-validator node.
    let chunk_validator_assignments =
        epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?;
    if !chunk_validator_assignments.contains(account_id) {
        return Err(Error::NotAChunkValidator);
    }

    // TODO(https://github.com/near/nearcore/issues/11301): replace these direct DB accesses with messages
    // sent to the client actor. for a draft, see https://github.com/near/nearcore/commit/e186dc7c0b467294034c60758fe555c78a31ef2d
    let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?;
    let final_head = store.get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?;

    // Avoid processing state witness for old chunks.
    // In particular it is impossible for a chunk created at a height
    // that doesn't exceed the height of the current final block to be
    // included in the chain. This addresses both network-delayed messages
    // as well as malicious behavior of a chunk producer.
    if let Some(final_head) = final_head {
        if height_created <= final_head.height {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                final_head_height = final_head.height,
                "Skipping because height created is not greater than final head height",
            );
            return Ok(false);
        }
    }
    if let Some(head) = head {
        if height_created > head.height + MAX_HEIGHTS_AHEAD {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                head_height = head.height,
                "Skipping because height created is more than {} blocks ahead of head height",
                MAX_HEIGHTS_AHEAD
            );
            return Ok(false);
        }

        // Try to find the EpochId to which this witness will belong based on its height.
        // It's not always possible to determine the exact epoch_id because the exact
        // starting height of the next epoch isn't known until it actually starts,
        // so things can get unclear around epoch boundaries.
        // Let's collect the epoch_ids in which the witness might possibly be.
        let possible_epochs =
            epoch_manager.possible_epochs_of_height_around_tip(&head, height_created)?;
        if !possible_epochs.contains(&epoch_id) {
            tracing::debug!(
                target: "stateless_validation",
                ?chunk_production_key,
                ?possible_epochs,
                "Skipping because EpochId is not in the possible list of epochs",
            );
            return Ok(false);
        }
    }

    Ok(true)
}
