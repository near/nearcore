use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    epoch_block_info::BlockInfo,
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::validator_stake::ValidatorStake,
};
use std::sync::Arc;

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
) -> Result<bool, Error> {
    let parent_hash = chunk_header.prev_block_hash();
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
