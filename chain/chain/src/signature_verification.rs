use crate::metrics::ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_epoch_manager::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter};
use near_primitives::{
    block::BlockHeader,
    errors::EpochError,
    hash::CryptoHash,
    sharding::{ChunkHash, ShardChunkHeader},
    stateless_validation::ChunkProductionKey,
    types::{BlockHeight, EpochId, ShardId, validator_stake::ValidatorStake},
};
use near_store::{Store, get_genesis_height};

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

/// Verify chunk header signature using the anchored chunk producer lookup.
/// Under EarlyKickout the producer is read from the ChunkProducers DB column
/// keyed by the chunk's grandparent anchor; cross-epoch and low-height chunks,
/// and the feature-off path, fall back to the canonical height sampler.
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
    let chunk_producer =
        epoch_manager.get_chunk_producer_info_from_prev_block(prev_block_hash, shard_id)?;
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

fn verify_anchored_chunk_key(
    epoch_manager: &dyn EpochManagerAdapter,
    epoch_id: &EpochId,
    height_created: BlockHeight,
    prev_block_hash: &CryptoHash,
    prev_prev_block_hash: &CryptoHash,
    store: &Store,
    msg_label: &str,
) -> Result<(), Error> {
    match epoch_manager.get_block_info(prev_block_hash) {
        Ok(parent_info) => {
            let expected_epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
            let expected_height =
                parent_info.height().checked_add(1).expect("block height overflow");
            if parent_info.prev_hash() != prev_prev_block_hash
                || expected_height != height_created
                || &expected_epoch_id != epoch_id
            {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "V2 {msg_label} chunk key mismatch: signed (epoch_id={:?}, height={}, \
                     prev_prev={:?}) does not match prev_block_hash-implied \
                     (epoch_id={:?}, height={}, prev_prev={:?})",
                    epoch_id,
                    height_created,
                    prev_prev_block_hash,
                    expected_epoch_id,
                    expected_height,
                    parent_info.prev_hash(),
                )));
            }
        }
        Err(EpochError::MissingBlock(_)) => {
            if prev_prev_block_hash != &CryptoHash::default() {
                // Parent not here yet, so only the anchor is known.
                let anchor_height = epoch_manager.get_block_info(prev_prev_block_hash)?.height();
                let min_height = anchor_height
                    .checked_add(CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET)
                    .expect("block height overflow");
                if height_created < min_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} height {height_created} below \
                         anchor-implied minimum height {min_height}"
                    )));
                }
            } else {
                // Default (genesis) anchor with no parent: nothing pins the height. A real
                // default anchor only happens at genesis or genesis + 1, so reject higher
                // to avoid an any-height hole.
                let genesis_height = get_genesis_height(store)
                    .ok_or_else(|| Error::Other("genesis height not found".to_owned()))?;
                // Overflow is impossible: `genesis_height` is a small configured constant.
                let max_height = genesis_height.checked_add(1).expect("block height overflow");
                if height_created > max_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} with default anchor at height {height_created} \
                         above genesis + 1 ({max_height})"
                    )));
                }
            }
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn resolve_anchored_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_prev_block_hash: &CryptoHash,
    epoch_id: &EpochId,
    height_created: BlockHeight,
    shard_id: ShardId,
    message_type: &str,
) -> Result<ValidatorStake, EpochError> {
    let result = epoch_manager.get_chunk_producer_info_anchored(
        Some(prev_prev_block_hash),
        epoch_id,
        height_created,
        shard_id,
    );
    let label = match &result {
        Ok(_) => "hit",
        // Anchor block not processed yet: this node is two or more blocks behind.
        Err(EpochError::MissingBlock(_)) => "miss_anchor_block",
        // Anchor is processed but has no `DBCol::ChunkProducers` row. Should be ~0
        // normally; if it persists, something that writes that row has a bug.
        Err(EpochError::ChunkProducerNotInDB(_, _)) => "miss_db_entry",
        Err(_) => "error",
    };
    ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL
        .with_label_values(&[shard_id.to_string().as_str(), message_type, label])
        .inc();
    result
}

pub fn resolve_and_verify_anchored_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    key: &ChunkProductionKey,
    prev_block_hash: &CryptoHash,
    prev_prev_block_hash: &CryptoHash,
    store: &Store,
    msg_label: &str,
) -> Result<ValidatorStake, Error> {
    let producer = resolve_anchored_producer(
        epoch_manager,
        prev_prev_block_hash,
        &key.epoch_id,
        key.height_created,
        key.shard_id,
        msg_label,
    )?;
    verify_anchored_chunk_key(
        epoch_manager,
        &key.epoch_id,
        key.height_created,
        prev_block_hash,
        prev_prev_block_hash,
        store,
        msg_label,
    )?;
    Ok(producer)
}

#[cfg(test)]
mod tests {
    use super::verify_anchored_chunk_key;
    use crate::ChainStoreAccess;
    use crate::test_utils::setup;
    use assert_matches::assert_matches;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_chain_primitives::Error;
    use near_epoch_manager::{CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET, EpochManagerAdapter};
    use near_primitives::hash::CryptoHash;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::EpochId;

    /// Exercises the parent-absent branch of `verify_anchored_chunk_key`: when the chunk's
    /// parent is not processed yet only the grandparent anchor is known, so the height can
    /// only be bounded from below (`>= anchor + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET`).
    #[test]
    fn test_anchored_key_parent_absent_accepts_skipped_height() {
        let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
        let (mut chain, epoch_manager, _runtime, signer) = setup(clock.clock());

        // Build a few real blocks so the anchor is a processed, non-genesis block.
        let mut prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        for _ in 0..3 {
            clock.advance(Duration::milliseconds(1));
            let block =
                TestBlockBuilder::from_prev_block(clock.clock(), &prev, signer.clone()).build();
            chain.process_block_test(block.clone()).unwrap();
            prev = block;
        }
        let anchor_hash = *prev.hash();
        let anchor_height = epoch_manager.get_block_info(&anchor_hash).unwrap().height();

        // A hash that is not a processed block, so `get_block_info` returns `MissingBlock`,
        // selecting the parent-absent branch under test.
        let missing_parent = CryptoHash::hash_bytes(&[42]);
        assert!(
            epoch_manager.get_block_info(&missing_parent).is_err(),
            "prev_block_hash must be unprocessed to exercise the parent-absent branch"
        );

        let store = chain.chain_store().store();
        // `epoch_id` is only consulted in the parent-known branch, so any value works here.
        let epoch_id = EpochId::default();
        let min_height = anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;

        let check = |height_created| {
            verify_anchored_chunk_key(
                epoch_manager.as_ref(),
                &epoch_id,
                height_created,
                &missing_parent,
                &anchor_hash,
                &store,
                "chunk",
            )
        };

        // Skipped-height chunk well above the anchor-implied minimum: accepted only because
        // the bound is a lower bound, not an exact match.
        assert_matches!(check(min_height + 3), Ok(()));
        // Boundary: exactly the minimum is accepted.
        assert_matches!(check(min_height), Ok(()));
        // Below the minimum is impossible for a valid chunk and must be rejected.
        assert_matches!(check(min_height - 1), Err(Error::InvalidPartialChunkStateWitness(_)));
    }
}
