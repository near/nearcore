//! Synthesis of spice execution artifacts from a pre-spice chunk application, for
//! the chunks at the spice activation boundary.

use crate::stateless_validation::chunk_validation::validate_receipt_proof;
use crate::{Chain, ChainStore, byzantine_assert};
use near_chain_primitives::{ApplyChunksMode, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ChunkExecutionResult, ShardId, SpiceChunkId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::sync::Arc;

/// The `ChunkExecutionResult` of shard `shard_id` of the last pre-spice block `block`,
/// synthesized from artifacts its pre-spice apply committed.
fn execution_result_from_pre_spice_apply(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<ChunkExecutionResult, Error> {
    Ok(execution_result_and_receipt_proofs_from_pre_spice_apply(
        chain_store,
        epoch_manager,
        block,
        shard_id,
    )?
    .0)
}

/// Same as [`execution_result_from_pre_spice_apply`], also returning the receipt proofs the
/// result's receipts root commits to, for persisting at the boundary.
pub fn execution_result_and_receipt_proofs_from_pre_spice_apply(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<(ChunkExecutionResult, Vec<ReceiptProof>), Error> {
    let epoch_id = block.header().epoch_id();
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id)?;
    let chunk_extra = chain_store.chunk_store().get_chunk_extra(block.hash(), &shard_uid)?;

    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunks = block.chunks();
    let chunk_header = chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
    let mut inclusion_header = Arc::new(block.header().clone());
    while inclusion_header.height() > chunk_header.height_included() {
        inclusion_header = chain_store.get_block_header(inclusion_header.prev_hash())?;
    }
    assert_eq!(
        inclusion_header.height(),
        chunk_header.height_included(),
        "chunk inclusion height is not on the ancestry of its block"
    );
    let mut outgoing_receipts =
        chain_store.get_outgoing_receipts(inclusion_header.hash(), shard_id)?.to_vec();
    outgoing_receipts.extend(get_undelivered_receipts(
        chain_store,
        epoch_manager,
        block,
        shard_id,
    )?);

    let next_shard_layout = epoch_manager.get_shard_layout_from_prev_block(block.hash())?;
    let (outgoing_receipts_root, receipt_proofs) =
        Chain::create_receipts_proofs_from_outgoing_receipts(
            &next_shard_layout,
            shard_id,
            outgoing_receipts,
        )?;
    Ok((
        ChunkExecutionResult { chunk_extra: chunk_extra.as_ref().clone(), outgoing_receipts_root },
        receipt_proofs,
    ))
}

/// A chunk of a shard that carried receipts some target shards never applied pre-spice.
pub struct UndeliveredReceiptCarrier {
    /// Commits, through its `prev_outgoing_receipts_root`, to the carried receipts: those
    /// its shard's previous chunk produced.
    pub chunk_header: ShardChunkHeader,
    /// The hash of the block before the one including the chunk.
    pub prev_block_hash: CryptoHash,
    /// The targets whose last pre-spice chunk precedes the chunk, in the order of the
    /// boundary shard layout.
    pub target_shard_ids: Vec<ShardId>,
}

/// The chunks of shard `shard_id` whose carried receipts some target has not applied by
/// the end of the last pre-spice `block`, newest first.
///
/// Pre-spice, a chunk carries the receipts its shard's previous chunk produced, and a
/// target's next chunk applies the receipts carried in every block since its previous
/// chunk. A target whose chunks are missing since before a carrying chunk never
/// applied what that chunk carried; under spice its first chunk applies them, as
/// incoming receipts from the boundary execution result of `shard_id`.
pub fn get_undelivered_receipt_carriers(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<Vec<UndeliveredReceiptCarrier>, Error> {
    // TODO(spice-resharding): the targets are the boundary block's shards; a resharding
    // between a carrying chunk and the boundary changes them.
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;
    let chunks = block.chunks();
    let mut target_last_included_heights = Vec::new();
    for target_shard_id in shard_layout.shard_ids() {
        let target_shard_index = shard_layout.get_shard_index(target_shard_id)?;
        let chunk_header =
            chunks.get(target_shard_index).ok_or(Error::InvalidShardId(target_shard_id))?;
        target_last_included_heights.push((target_shard_id, chunk_header.height_included()));
    }
    let Some(lowest_last_included_height) =
        target_last_included_heights.iter().map(|(_, height)| *height).min()
    else {
        return Ok(Vec::new());
    };

    let mut carriers = Vec::new();
    let mut current = chain_store.get_block(block.hash())?;
    while current.header().height() > lowest_last_included_height {
        let height = current.header().height();
        let current_shard_layout = epoch_manager.get_shard_layout(current.header().epoch_id())?;
        let current_chunks = current.chunks();
        let chunk_header = current_chunks
            .get(current_shard_layout.get_shard_index(shard_id)?)
            .ok_or(Error::InvalidShardId(shard_id))?;
        if chunk_header.is_new_chunk(height) {
            carriers.push(UndeliveredReceiptCarrier {
                chunk_header: chunk_header.clone(),
                prev_block_hash: *current.header().prev_hash(),
                target_shard_ids: target_last_included_heights
                    .iter()
                    .filter(|(_, last_included_height)| *last_included_height < height)
                    .map(|(target_shard_id, _)| *target_shard_id)
                    .collect(),
            });
        }
        let prev_hash = *current.header().prev_hash();
        current = chain_store.get_block(&prev_hash)?;
    }
    Ok(carriers)
}

/// Proofs of the receipts [`get_undelivered_receipt_carriers`] carried to the targets
/// that missed them, from this node's store, as a boundary witness of `block` carries
/// them.
pub fn get_undelivered_receipt_proofs(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<HashMap<ChunkHash, Vec<ReceiptProof>>, Error> {
    let mut undelivered_receipt_proofs = HashMap::new();
    for carrier in get_undelivered_receipt_carriers(chain_store, epoch_manager, block, shard_id)? {
        let prev_block = chain_store.get_block(&carrier.prev_block_hash)?;
        let prev_shard_layout = epoch_manager.get_shard_layout(prev_block.header().epoch_id())?;
        let prev_chunks = prev_block.chunks();
        let prev_last_included_height = prev_chunks
            .get(prev_shard_layout.get_shard_index(shard_id)?)
            .ok_or(Error::InvalidShardId(shard_id))?
            .height_included();
        let carried_receipts = ChainStore::get_outgoing_receipts_for_shard_from_store(
            chain_store,
            epoch_manager,
            carrier.prev_block_hash,
            shard_id,
            prev_last_included_height,
        )?;
        let carrier_shard_layout =
            epoch_manager.get_shard_layout_from_prev_block(&carrier.prev_block_hash)?;
        let (carried_receipts_root, carried_receipt_proofs) =
            Chain::create_receipts_proofs_from_outgoing_receipts(
                &carrier_shard_layout,
                shard_id,
                carried_receipts,
            )?;
        if &carried_receipts_root != carrier.chunk_header.prev_outgoing_receipts_root() {
            return Err(Error::Other(format!(
                "stored receipts carried by chunk {:?} do not match its receipts root",
                carrier.chunk_header.chunk_hash()
            )));
        }
        let mut proofs_by_target: HashMap<ShardId, ReceiptProof> =
            carried_receipt_proofs.into_iter().map(|proof| (proof.1.to_shard_id, proof)).collect();
        let proofs = carrier
            .target_shard_ids
            .iter()
            .map(|target_shard_id| {
                proofs_by_target
                    .remove(target_shard_id)
                    .ok_or(Error::InvalidShardId(*target_shard_id))
            })
            .collect::<Result<_, Error>>()?;
        undelivered_receipt_proofs.insert(carrier.chunk_header.chunk_hash().clone(), proofs);
    }
    Ok(undelivered_receipt_proofs)
}

/// Validates `proofs` as [`get_undelivered_receipt_proofs`] of `block` for shard
/// `shard_id`, returning the receipts in the order the boundary execution result
/// commits to them: newest carrying chunk first, by target within a chunk.
pub fn validate_undelivered_receipt_proofs(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
    proofs: &HashMap<ChunkHash, Vec<ReceiptProof>>,
) -> Result<Vec<Receipt>, Error> {
    let carriers = get_undelivered_receipt_carriers(chain_store, epoch_manager, block, shard_id)?;
    if proofs.len() != carriers.len() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "undelivered receipt proofs for {} chunks, expected {}",
            proofs.len(),
            carriers.len()
        )));
    }
    let mut receipts = Vec::new();
    for carrier in &carriers {
        let chunk_hash = carrier.chunk_header.chunk_hash();
        let Some(chunk_proofs) = proofs.get(chunk_hash) else {
            return Err(Error::InvalidChunkStateWitness(format!(
                "missing undelivered receipt proofs for chunk {:?}",
                chunk_hash
            )));
        };
        if chunk_proofs.len() != carrier.target_shard_ids.len() {
            return Err(Error::InvalidChunkStateWitness(format!(
                "{} undelivered receipt proofs for chunk {:?}, expected {}",
                chunk_proofs.len(),
                chunk_hash,
                carrier.target_shard_ids.len()
            )));
        }
        for (target_shard_id, proof) in carrier.target_shard_ids.iter().zip(chunk_proofs) {
            validate_receipt_proof(
                proof,
                &carrier.chunk_header,
                *target_shard_id,
                *carrier.chunk_header.prev_outgoing_receipts_root(),
            )?;
            receipts.extend(proof.0.iter().cloned());
        }
    }
    Ok(receipts)
}

/// The receipts shard `shard_id` sent that their targets have not applied by the end
/// of the last pre-spice `block`, beyond those of its last chunk's apply; read from
/// this node's store, in the order [`validate_undelivered_receipt_proofs`] yields them.
pub fn get_undelivered_receipts(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<Vec<Receipt>, Error> {
    let proofs = get_undelivered_receipt_proofs(chain_store, epoch_manager, block, shard_id)?;
    validate_undelivered_receipt_proofs(chain_store, epoch_manager, block, shard_id, &proofs)
}

/// The execution result of shard `shard_id`'s previous chunk, read off the chunk
/// header the pre-spice `block` carries for that shard.
pub fn execution_result_from_pre_spice_child(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<Option<ChunkExecutionResult>, Error> {
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunks = block.chunks();
    let chunk_header = chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
    if !chunk_header.is_new_chunk(block.header().height()) {
        return Ok(None);
    }
    // The unprefixed fields (gas limit, congestion info, bandwidth requests, proposed split)
    // describe the previous chunk's result too: the pre-spice chunk producer copies them
    // from that chunk's extra into the header unchanged.
    let chunk_extra = ChunkExtra::new(
        &chunk_header.prev_state_root(),
        *chunk_header.prev_outcome_root(),
        chunk_header.prev_validator_proposals().collect(),
        chunk_header.prev_gas_used(),
        chunk_header.gas_limit(),
        chunk_header.prev_balance_burnt(),
        Some(chunk_header.congestion_info()),
        chunk_header.bandwidth_requests().cloned().unwrap_or_else(BandwidthRequests::empty),
        chunk_header.proposed_split().cloned(),
    );
    Ok(Some(ChunkExecutionResult {
        chunk_extra,
        outgoing_receipts_root: *chunk_header.prev_outgoing_receipts_root(),
    }))
}

/// The blocks a boundary witness of `block` for shard `shard_id` covers.
pub struct PreSpiceChunkApplyBlocks {
    /// The block carrying the shard's chunk the witness applies; `block` itself when it
    /// includes one.
    pub last_new_chunk_block: Arc<Block>,
    /// The blocks after it, oldest first, whose old-chunk applications the witness
    /// replays.
    pub old_chunk_blocks: Vec<Arc<Block>>,
}

pub fn get_last_new_chunk_block_and_old_chunk_blocks(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<PreSpiceChunkApplyBlocks, Error> {
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunks = block.chunks();
    let height_included =
        chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?.height_included();

    let mut old_chunk_blocks = Vec::new();
    let mut last_new_chunk_block = chain_store.get_block(block.hash())?;
    while last_new_chunk_block.header().height() > height_included {
        let prev_hash = *last_new_chunk_block.header().prev_hash();
        old_chunk_blocks.push(last_new_chunk_block);
        last_new_chunk_block = chain_store.get_block(&prev_hash)?;
    }
    assert_eq!(
        last_new_chunk_block.header().height(),
        height_included,
        "chunk inclusion height is not on the ancestry of its block"
    );
    old_chunk_blocks.reverse();
    Ok(PreSpiceChunkApplyBlocks { last_new_chunk_block, old_chunk_blocks })
}

/// The blocks whose incoming receipts the chunk of `shard_id` at
/// `last_new_chunk_block` consumed: `last_new_chunk_block` itself and the blocks back
/// to, excluding, the one that included the shard's previous chunk. Newest first.
pub fn get_incoming_receipt_blocks_for_shard(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    last_new_chunk_block: &Block,
    shard_id: ShardId,
) -> Result<Vec<Arc<Block>>, Error> {
    let prev_block = chain_store.get_block(last_new_chunk_block.header().prev_hash())?;
    let prev_shard_layout = epoch_manager.get_shard_layout(prev_block.header().epoch_id())?;
    let prev_shard_index = prev_shard_layout.get_shard_index(shard_id)?;
    let prev_chunks = prev_block.chunks();
    let previous_inclusion_height =
        prev_chunks.get(prev_shard_index).ok_or(Error::InvalidShardId(shard_id))?.height_included();

    let mut source_blocks = Vec::new();
    let mut block = chain_store.get_block(last_new_chunk_block.hash())?;
    while block.header().height() > previous_inclusion_height {
        let prev_hash = *block.header().prev_hash();
        source_blocks.push(block);
        block = chain_store.get_block(&prev_hash)?;
    }
    assert_eq!(
        block.header().height(),
        previous_inclusion_height,
        "chunk inclusion height is not on the ancestry of its block"
    );
    Ok(source_blocks)
}

/// Consistency check between the two sources of truth at the boundary: a certified
/// execution result of a pre-spice chunk must match what this node synthesizes from
/// its own pre-spice apply.
///
/// Only a chunk of a shard this node applied with the block has local artifacts to check
/// against; for any other shard, learning the result from certification is the point.
pub fn check_pre_spice_execution_result(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    chunk_id: &SpiceChunkId,
    execution_result: &ChunkExecutionResult,
) -> Result<(), Error> {
    // A block this node does not hold is nothing to check against.
    let block = match chain_store.get_block(&chunk_id.block_hash) {
        Ok(block) => block,
        Err(Error::DBNotFoundErr(_)) => return Ok(()),
        Err(err) => return Err(err),
    };
    if block.is_spice_block() {
        return Ok(());
    }
    // The shards applied with the block whatever its catch-up status: `NotCaughtUp` is
    // the lower bound of the apply modes. A shard the node only tracks next epoch and
    // is still catching up is applied later, so its artifacts cannot be required here.
    if !shard_tracker.should_apply_chunk(
        ApplyChunksMode::NotCaughtUp,
        block.header().prev_hash(),
        chunk_id.shard_id,
    ) {
        return Ok(());
    }
    let synthesized = execution_result_from_pre_spice_apply(
        chain_store,
        epoch_manager,
        &block,
        chunk_id.shard_id,
    )?;
    if &synthesized != execution_result {
        byzantine_assert!(false);
        return Err(Error::Other(format!(
            "certified execution result for pre-spice chunk {:?} does not match local synthesis",
            chunk_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        check_pre_spice_execution_result, execution_result_from_pre_spice_apply,
        execution_result_from_pre_spice_child, get_incoming_receipt_blocks_for_shard,
        get_last_new_chunk_block_and_old_chunk_blocks,
    };
    use crate::Chain;
    use crate::spice::tests::pre_spice::{add_pre_spice_block, setup_pre_spice_chain};
    use near_async::time::Clock;
    use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
    use near_chain_primitives::Error;
    use near_crypto::{KeyType, SecretKey};
    use near_epoch_manager::shard_tracker::ShardTracker;
    use near_primitives::bandwidth_scheduler::BandwidthRequests;
    use near_primitives::block::Block;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::gas::Gas;
    use near_primitives::hash::CryptoHash;
    use near_primitives::merkle::merklize;
    use near_primitives::receipt::Receipt;
    use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
    };
    use near_primitives::types::Balance;
    use near_primitives::types::SpiceChunkId;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_store::adapter::StoreAdapter;
    use std::sync::Arc;

    /// The synthesized result must be the chunk extra the pre-spice apply wrote plus
    /// the receipts root a producer of the next block's chunk would compute.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_execution_result_from_pre_spice_apply_with_included_and_missing_chunk() {
        let mut chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let all_shards: Vec<_> = shard_layout.shard_ids().collect();

        // The first block includes a new chunk; the second copies the first's chunk
        // headers, so its chunk is missing (last included height stays at the first).
        let block_with_chunk = add_pre_spice_block(&mut chain, &genesis_block, &all_shards);
        let block_missing_chunk = add_pre_spice_block(&mut chain, &block_with_chunk, &[]);
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let shard_id = shard_uid.shard_id();

        let receipts =
            vec![Receipt::new_balance_refund(&"user".parse().unwrap(), Balance::from_near(1))];
        let extra_with_chunk = ChunkExtra::new_with_only_state_root(&CryptoHash::hash_bytes(b"a"));
        let extra_missing_chunk =
            ChunkExtra::new_with_only_state_root(&CryptoHash::hash_bytes(b"b"));
        let mut store_update = chain.chain_store.store_update();
        store_update.save_outgoing_receipt(block_with_chunk.hash(), shard_id, receipts.clone());
        store_update.save_chunk_extra(
            block_with_chunk.hash(),
            &shard_uid,
            extra_with_chunk.clone().into(),
        );
        store_update.save_chunk_extra(
            block_missing_chunk.hash(),
            &shard_uid,
            extra_missing_chunk.clone().into(),
        );
        store_update.commit().unwrap();

        let expected_root =
            merklize(&Chain::build_receipts_hashes(&receipts, &shard_layout).unwrap()).0;
        let empty_root = merklize(&Chain::build_receipts_hashes(&[], &shard_layout).unwrap()).0;
        assert_ne!(expected_root, empty_root, "the roots must discriminate the read block");

        let chain_store = chain.chain_store.store().chain_store();
        let result = execution_result_from_pre_spice_apply(
            &chain_store,
            epoch_manager.as_ref(),
            &block_with_chunk,
            shard_id,
        )
        .unwrap();
        assert_eq!(result.chunk_extra, extra_with_chunk);
        assert_eq!(result.outgoing_receipts_root, expected_root);

        // The missing chunk produced no receipts row of its own: the root still covers
        // the last included chunk's receipts, while the chunk extra is the block's own
        // (applying a missed chunk writes one).
        let result = execution_result_from_pre_spice_apply(
            &chain_store,
            epoch_manager.as_ref(),
            &block_missing_chunk,
            shard_id,
        )
        .unwrap();
        assert_eq!(result.chunk_extra, extra_missing_chunk);
        assert_eq!(result.outgoing_receipts_root, expected_root);
    }

    /// The consistency check accepts a certified pre-spice result equal to the local synthesis,
    /// rejects one that differs, skips a shard this node does not track, and fails on a
    /// tracked shard it has no artifacts for.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_pre_spice_execution_result_consistency_check() {
        let mut chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let all_shards: Vec<_> = shard_layout.shard_ids().collect();

        // Applied by this node: chunk extra and receipts on disk, synthesis possible.
        let applied_block = add_pre_spice_block(&mut chain, &genesis_block, &all_shards);
        // Never applied by this node: no chunk extra, synthesis impossible.
        let unapplied_block = add_pre_spice_block(&mut chain, &applied_block, &[]);
        let shard_uid = shard_layout.shard_uids().next().unwrap();
        let shard_id = shard_uid.shard_id();
        let mut store_update = chain.chain_store.store_update();
        store_update.save_outgoing_receipt(applied_block.hash(), shard_id, vec![]);
        store_update.save_chunk_extra(
            applied_block.hash(),
            &shard_uid,
            ChunkExtra::new_with_only_state_root(&CryptoHash::hash_bytes(b"a")).into(),
        );
        store_update.commit().unwrap();

        let chain_store = chain.chain_store.store().chain_store();
        let tracking_all = ShardTracker::new(
            TrackedShardsConfig::AllShards,
            epoch_manager.clone(),
            MutableConfigValue::new(None, "validator_signer"),
        );
        let tracking_none = ShardTracker::new_empty(epoch_manager.clone());
        let chunk_id = SpiceChunkId { block_hash: *applied_block.hash(), shard_id };
        let synthesized = execution_result_from_pre_spice_apply(
            &chain_store,
            epoch_manager.as_ref(),
            &applied_block,
            shard_id,
        )
        .unwrap();

        check_pre_spice_execution_result(
            &chain_store,
            epoch_manager.as_ref(),
            &tracking_all,
            &chunk_id,
            &synthesized,
        )
        .unwrap();

        let mut forged = synthesized;
        forged.outgoing_receipts_root = CryptoHash::hash_bytes(b"forged root");
        let err = check_pre_spice_execution_result(
            &chain_store,
            epoch_manager.as_ref(),
            &tracking_all,
            &chunk_id,
            &forged,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not match local synthesis"), "{err}");

        // A forged result for an untracked shard passes: nothing local to check
        // against, and learning untracked results from certification is the point.
        check_pre_spice_execution_result(
            &chain_store,
            epoch_manager.as_ref(),
            &tracking_none,
            &chunk_id,
            &forged,
        )
        .unwrap();

        // A tracked shard whose chunk this node never applied is a local error.
        let unapplied_chunk_id = SpiceChunkId { block_hash: *unapplied_block.hash(), shard_id };
        let err = check_pre_spice_execution_result(
            &chain_store,
            epoch_manager.as_ref(),
            &tracking_all,
            &unapplied_chunk_id,
            &forged,
        )
        .unwrap_err();
        assert!(matches!(err, Error::DBNotFoundErr(_)), "{err}");
    }

    /// Every field of the reconstructed result must come from the corresponding
    /// prev_* header field, and a missing chunk must yield `None` rather than the older chunk's stale fields.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_execution_result_from_pre_spice_child() {
        let signer = Arc::new(create_test_signer("test1"));
        let chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        // A header with every prev_* field distinct, so a swapped mapping cannot pass.
        let proposals = vec![ValidatorStake::new(
            "test1".parse().unwrap(),
            SecretKey::from_seed(KeyType::ED25519, "test1").public_key(),
            Balance::from_yoctonear(17),
        )];
        let congestion_info = CongestionInfo::default();
        let mut chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
            *genesis_block.hash(),
            CryptoHash::hash_bytes(b"state root"),
            CryptoHash::hash_bytes(b"outcome root"),
            CryptoHash::default(),
            0,
            1,
            shard_id,
            Gas::from_gas(7),
            Gas::from_gas(1_000_000),
            Balance::from_yoctonear(42),
            CryptoHash::hash_bytes(b"receipts root"),
            CryptoHash::default(),
            proposals.clone(),
            congestion_info,
            BandwidthRequests::empty(),
            None,
            &signer,
            pre_spice_protocol_version(),
        ));
        *chunk_header.height_included_mut() = 1;
        let block_with_chunk = TestBlockBuilder::from_prev_block(
            Clock::real(),
            genesis_block.as_ref(),
            signer.clone(),
        )
        .chunks(vec![chunk_header])
        .protocol_version(pre_spice_protocol_version())
        .build();

        let result = execution_result_from_pre_spice_child(
            epoch_manager.as_ref(),
            &block_with_chunk,
            shard_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.chunk_extra.state_root(), &CryptoHash::hash_bytes(b"state root"));
        assert_eq!(result.chunk_extra.outcome_root(), &CryptoHash::hash_bytes(b"outcome root"));
        assert_eq!(result.chunk_extra.validator_proposals().collect::<Vec<_>>(), proposals,);
        assert_eq!(result.chunk_extra.gas_used(), Gas::from_gas(7));
        assert_eq!(result.chunk_extra.gas_limit(), Gas::from_gas(1_000_000));
        assert_eq!(result.chunk_extra.balance_burnt(), Balance::from_yoctonear(42));
        assert_eq!(result.chunk_extra.congestion_info(), congestion_info);
        assert_eq!(result.chunk_extra.bandwidth_requests(), Some(&BandwidthRequests::empty()));
        assert_eq!(result.chunk_extra.proposed_split(), None);
        assert_eq!(result.outgoing_receipts_root, CryptoHash::hash_bytes(b"receipts root"));

        // The next block carries the same header (chunk missing): no result.
        let block_missing_chunk =
            TestBlockBuilder::from_prev_block(Clock::real(), block_with_chunk.as_ref(), signer)
                .protocol_version(pre_spice_protocol_version())
                .build();
        assert_eq!(
            execution_result_from_pre_spice_child(
                epoch_manager.as_ref(),
                &block_missing_chunk,
                shard_id,
            )
            .unwrap(),
            None,
        );
    }

    /// The block walks a boundary witness covers: the old-chunk blocks after the last
    /// new chunk's block, oldest first, and the incoming receipt sources back to the
    /// previous chunk's block, newest first.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_last_new_chunk_block_and_incoming_receipt_blocks() {
        let mut chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let all_shards: Vec<_> = shard_layout.shard_ids().collect();
        let shard_id = shard_layout.shard_ids().next().unwrap();

        // A new chunk, two missing ones, then a new chunk again.
        let new_chunk_block = add_pre_spice_block(&mut chain, &genesis_block, &all_shards);
        let missing_1 = add_pre_spice_block(&mut chain, &new_chunk_block, &[]);
        let missing_2 = add_pre_spice_block(&mut chain, &missing_1, &[]);
        let next_new_chunk_block = add_pre_spice_block(&mut chain, &missing_2, &all_shards);
        let chain_store = chain.chain_store.store().chain_store();
        let hashes =
            |blocks: &[Arc<Block>]| blocks.iter().map(|block| *block.hash()).collect::<Vec<_>>();

        // A block carrying its own chunk replays nothing.
        let blocks = get_last_new_chunk_block_and_old_chunk_blocks(
            &chain_store,
            epoch_manager.as_ref(),
            &new_chunk_block,
            shard_id,
        )
        .unwrap();
        assert_eq!(blocks.last_new_chunk_block.hash(), new_chunk_block.hash());
        assert!(blocks.old_chunk_blocks.is_empty());

        // The second missing chunk's witness applies the last new chunk and replays both
        // missing-chunk blocks after it, oldest first.
        let blocks = get_last_new_chunk_block_and_old_chunk_blocks(
            &chain_store,
            epoch_manager.as_ref(),
            &missing_2,
            shard_id,
        )
        .unwrap();
        assert_eq!(blocks.last_new_chunk_block.hash(), new_chunk_block.hash());
        assert_eq!(hashes(&blocks.old_chunk_blocks), vec![*missing_1.hash(), *missing_2.hash()]);

        // The first chunk after genesis consumed the receipts of its own block only.
        let sources = get_incoming_receipt_blocks_for_shard(
            &chain_store,
            epoch_manager.as_ref(),
            &new_chunk_block,
            shard_id,
        )
        .unwrap();
        assert_eq!(hashes(&sources), vec![*new_chunk_block.hash()]);

        // The chunk after the gap consumed the receipts of every block since the
        // previous chunk's, newest first.
        let sources = get_incoming_receipt_blocks_for_shard(
            &chain_store,
            epoch_manager.as_ref(),
            &next_new_chunk_block,
            shard_id,
        )
        .unwrap();
        assert_eq!(
            hashes(&sources),
            vec![*next_new_chunk_block.hash(), *missing_2.hash(), *missing_1.hash()]
        );
    }
}
