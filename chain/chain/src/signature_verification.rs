use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    block::BlockHeader,
    epoch_block_info::BlockInfo,
    errors::EpochError,
    hash::CryptoHash,
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::validator_stake::ValidatorStake,
};
use std::sync::Arc;

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
pub fn verify_chunk_header_signature(
    chunk_hash: &ChunkHash,
    signature: &Signature,
    chunk_producer: ValidatorStake,
    block_info: Arc<BlockInfo>,
) -> Result<bool, Error> {
    if block_info.slashed().contains_key(chunk_producer.account_id()) {
        return Ok(false);
    }
    Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
}

pub fn verify_chunk_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    chunk_header: &ShardChunkHeader,
    parent_hash: &CryptoHash,
) -> Result<bool, Error> {
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
    let key = ChunkProductionKey {
        epoch_id,
        height_created: chunk_header.height_created(),
        shard_id: chunk_header.shard_id(),
    };
    let chunk_producer = epoch_manager.get_chunk_producer_info(&key)?;
    let block_info = epoch_manager.get_block_info(&parent_hash)?;
    verify_chunk_header_signature(
        &chunk_header.chunk_hash(),
        chunk_header.signature(),
        chunk_producer,
        block_info,
    )
}

/// This function requires that the previous block of `header` has been processed.
/// If not, it returns EpochError::MissingBlock.
fn verify_header_signature_with_epoch_manager(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<bool, Error> {
    let block_producer =
        epoch_manager.get_block_producer_info(header.epoch_id(), header.height())?;
    match epoch_manager.get_block_info(header.prev_hash()) {
        Ok(block_info) => Ok(verify_header_signature(header, block_producer, block_info)),
        Err(_) => return Err(EpochError::MissingBlock(*header.prev_hash()).into()),
    }
}

fn verify_header_signature(
    header: &BlockHeader,
    block_producer: ValidatorStake,
    block_info: Arc<BlockInfo>,
) -> bool {
    if block_info.slashed().contains_key(block_producer.account_id()) {
        return false;
    }
    header.signature().verify(header.hash().as_ref(), block_producer.public_key())
}
