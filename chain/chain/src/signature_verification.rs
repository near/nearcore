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

use crate::metrics::ANCHORED_CHUNK_PRODUCER_LOOKUP_TOTAL;

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

/// Cross-check the signed (epoch, height) against the signed anchor before trusting the
/// producer resolved from it. The anchored lookup is keyed by anchor and shard only, not
/// height, so without this a producer could sign under any height and we would store or
/// forward under a forged key.
///
/// Parent known: must match the parent exactly. Parent absent, real anchor: height must be
/// anchor + 2. Parent absent, default anchor: only valid at genesis or genesis + 1.
/// `msg_label` names the caller in the error.
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
            if parent_info.prev_hash() != prev_prev_block_hash
                || parent_info.height() + 1 != height_created
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
                    parent_info.height() + 1,
                    parent_info.prev_hash(),
                )));
            }
        }
        Err(EpochError::MissingBlock(_)) => {
            if prev_prev_block_hash != &CryptoHash::default() {
                // Parent not here yet, so only the anchor is known. Requiring height ==
                // anchor + 2 ties one anchor to one chunk key per shard, so a producer
                // cannot reuse it across many cache slots. (Cross-epoch anchors fall back
                // to the resolver.) A skipped slot with a missing parent is dropped here;
                // that is fine, best-effort.
                let anchor_height = epoch_manager.get_block_info(prev_prev_block_hash)?.height();
                let expected_height = anchor_height + CHUNK_GRANDPARENT_ANCHOR_HEIGHT_OFFSET;
                if height_created != expected_height {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} height {height_created} does not match \
                         anchor-implied height {expected_height}"
                    )));
                }
                // TODO(verified-chunk-cache): also bound `epoch_id` to this anchor's candidate
                // epochs ({ get_epoch_id(anchor), next epoch }; the chunk crosses at most one
                // epoch boundary). Here the epoch is disciplined only by the signature, so a
                // validator assigned as producer in an unrelated *known* epoch can self-sign a
                // header at this height and transiently poison the cache (racing out the real
                // chunk via the (height, shard) dedup) until the parent lands. Shared with the
                // witness path.
            } else {
                // Default (genesis) anchor with no parent: nothing pins the height. A real
                // default anchor only happens at genesis or genesis + 1, so reject higher
                // to avoid an any-height hole.
                let genesis_height = get_genesis_height(store)
                    .ok_or_else(|| Error::Other("genesis height not found".to_owned()))?;
                if height_created > genesis_height + 1 {
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "V2 {msg_label} with default anchor at height {height_created} \
                         above genesis + 1 ({})",
                        genesis_height + 1
                    )));
                }
            }
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

/// Look up the producer from the anchor and record the lookup-outcome metric (hit, anchor
/// block missing, or `DBCol::ChunkProducers` row missing). Error returned unchanged;
/// callers turn the node-behind cases (`MissingBlock`, `ChunkProducerNotInDB`) into a
/// quiet drop.
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

/// Resolve a V2 message's producer from the grandparent anchor, then cross-check the signed
/// chunk key against the anchor. Shared by the witness, contract-accesses and contract-deploys
/// V2 paths; `msg_label` names the caller in the metric and errors. Resolve runs before the
/// cross-check so the lookup metric is recorded even when the key check later rejects.
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
