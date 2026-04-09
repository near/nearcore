use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    block::BlockHeader,
    hash::CryptoHash,
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::{BlockHeight, EpochId, ShardId, validator_stake::ValidatorStake},
};

pub fn verify_block_vrf(
    validator: ValidatorStake,
    prev_random_value: &CryptoHash,
    vrf_value: &near_crypto::vrf::Value,
    vrf_proof: &near_crypto::vrf::Proof,
) -> Result<(), Error> {
    let public_key =
        near_crypto::key_conversion::convert_public_key(validator.public_key().unwrap_as_ed25519())
            .unwrap();

    if !public_key.is_vrf_valid(&prev_random_value.as_ref(), vrf_value, vrf_proof) {
        return Err(Error::InvalidRandomnessBeaconOutput);
    }
    Ok(())
}

/// Verify chunk header signature.
/// return false if the header signature does not match the key for the assigned chunk producer
/// for this chunk, or if the chunk producer has been slashed
/// return `EpochError::NotAValidator` if cannot find chunk producer info for this chunk
/// `header`: chunk header
/// `epoch_id`: epoch_id that the chunk header belongs to
/// `last_known_hash`: used to determine the list of chunk producers that are slashed
pub fn verify_chunk_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
    epoch_id: EpochId,
) -> Result<bool, Error> {
    verify_chunk_header_signature_with_epoch_manager_and_parts(
        epoch_manager,
        &chunk_header.chunk_hash(),
        chunk_header.signature(),
        epoch_id,
        chunk_header.height_created(),
        chunk_header.shard_id(),
    )
}

// TODO(early-kickout): migrate to get_chunk_producer_info_db
// once the ShardsManager's validate_chunk_header is refactored to separate
// preliminary (epoch_id-based) and full (prev_block_hash-based) validation.
pub fn verify_chunk_header_signature_with_epoch_manager_and_parts(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_hash: &ChunkHash,
    signature: &Signature,
    epoch_id: EpochId,
    height_created: BlockHeight,
    shard_id: ShardId,
) -> Result<bool, Error> {
    let key = ChunkProductionKey { epoch_id, height_created, shard_id };
    let chunk_producer = epoch_manager.get_chunk_producer_info(&key)?;
    Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
}

/// Verify chunk header signature using hash-based chunk producer lookup.
/// Uses get_chunk_producer_info_db(prev_block_hash, shard_id) which reads from
/// the ChunkProducers DB column when EarlyKickout is enabled, and errors on
/// a missing DB entry.
pub fn verify_chunk_header_signature_by_hash(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
) -> Result<bool, Error> {
    verify_chunk_header_signature_by_hash_and_parts(
        epoch_manager,
        &chunk_header.chunk_hash(),
        chunk_header.signature(),
        chunk_header.prev_block_hash(),
        chunk_header.shard_id(),
    )
}

pub fn verify_chunk_header_signature_by_hash_and_parts(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_hash: &ChunkHash,
    signature: &Signature,
    prev_block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<bool, Error> {
    let chunk_producer = epoch_manager.get_chunk_producer_info_db(prev_block_hash, shard_id)?;
    Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
}

pub fn verify_block_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let block_producer =
        epoch_manager.get_block_producer_info(header.epoch_id(), header.height())?;
    Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
}
