use crate::Chain;
use crate::store::ChainStore;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{ChunkExecutionResult, ShardId};
use near_primitives::version::ProtocolFeature;
use near_store::StoreUpdate;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};

/// Whether `block_hash` is a spice activation parent: a last block of the last
/// pre-spice epoch, so every child of it is a first spice block.
///
/// Epoch-manager-backed on purpose: the next epoch's protocol version is fixed by the
/// time `block_hash` exists, so the answer is stable regardless of the caller's head.
pub fn is_spice_activation_parent(
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<bool, Error> {
    if !epoch_manager.is_next_block_epoch_start(block_hash)? {
        return Ok(false);
    }
    let epoch_id = epoch_manager.get_epoch_id(block_hash)?;
    let epoch_protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
    if ProtocolFeature::Spice.enabled(epoch_protocol_version) {
        return Ok(false);
    }
    let next_epoch_protocol_version = epoch_manager.get_next_epoch_protocol_version(block_hash)?;
    Ok(ProtocolFeature::Spice.enabled(next_epoch_protocol_version))
}

/// Seeds the spice execution heads when `block` is a first spice block, i.e. when
/// `parent_header` is still pre-spice; a no-op otherwise.
pub fn seed_execution_heads_at_activation(
    store_update: &mut StoreUpdate,
    block: &Block,
    parent_header: &BlockHeader,
) -> Result<(), Error> {
    if parent_header.is_spice() {
        return Ok(());
    }
    let mut adapter = store_update.chain_store_update();
    adapter.set_spice_execution_head(&Tip::from_header(parent_header))?;
    adapter.update_spice_final_execution_head(block)?;
    Ok(())
}

/// Synthesizes the `ChunkExecutionResult` of shard `shard_id` of the activation parent
/// `block` from artifacts its pre-spice apply committed.
pub fn synthesize_execution_result(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    shard_id: ShardId,
) -> Result<ChunkExecutionResult, Error> {
    let epoch_id = block.header().epoch_id();
    let shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id)?;
    let chunk_extra = chain_store.chunk_store().get_chunk_extra(block.hash(), &shard_uid)?;

    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunks = block.chunks();
    let chunk_header = chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
    let outgoing_receipts = ChainStore::get_outgoing_receipts_for_shard_from_store(
        chain_store,
        epoch_manager,
        *block.hash(),
        shard_id,
        chunk_header.height_included(),
    )?;

    let next_shard_layout = epoch_manager.get_shard_layout_from_prev_block(block.hash())?;
    let (outgoing_receipts_root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
        &next_shard_layout,
        shard_id,
        outgoing_receipts,
    )?;
    Ok(ChunkExecutionResult { chunk_extra: chunk_extra.as_ref().clone(), outgoing_receipts_root })
}

#[cfg(test)]
mod tests {
    use super::synthesize_execution_result;
    use crate::test_utils::{get_chain_with_genesis, get_fake_next_block_chunk_headers};
    use crate::{Block, Chain};
    use near_async::time::Clock;
    use near_chain_configs::Genesis;
    use near_primitives::epoch_block_info::BlockInfo;
    use near_primitives::hash::CryptoHash;
    use near_primitives::merkle::merklize;
    use near_primitives::receipt::Receipt;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
    };
    use near_primitives::types::Balance;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_store::adapter::StoreAdapter;
    use std::sync::Arc;

    /// Saves the block and records it in the epoch manager, the way block
    /// postprocessing does, so epoch lookups keyed on its hash resolve.
    fn save_and_record_block(chain: &mut Chain, block: &Arc<Block>) {
        let mut store_update = chain.chain_store.store_update();
        store_update.save_block(block.clone());
        store_update.save_block_header(block.header().clone()).unwrap();
        let block_info = BlockInfo::from_header(block.header(), 0, pre_spice_protocol_version());
        let epoch_manager_update = chain
            .epoch_manager
            .add_validator_proposals(block_info, *block.header().random_value())
            .unwrap();
        store_update.merge(epoch_manager_update.into());
        store_update.commit().unwrap();
    }

    /// The synthesized result must be the chunk extra the pre-spice apply wrote plus
    /// the receipts root a producer of the next block's chunk would compute: the
    /// receipts of the shard's last included chunk, whether that chunk was included
    /// in the block itself or the block missed it.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_synthesize_execution_result_with_included_and_missing_chunk() {
        let signer = Arc::new(create_test_signer("test1"));
        let mut genesis =
            Genesis::test_sharded(Clock::real(), vec!["test1".parse().unwrap()], 1, 1);
        genesis.config.protocol_version = pre_spice_protocol_version();
        let mut chain = get_chain_with_genesis(Clock::real(), genesis);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();

        // The first block includes a new chunk; the second copies the first's chunk
        // headers, so its chunk is missing (last included height stays at the first).
        let block_with_chunk = TestBlockBuilder::from_prev_block(
            Clock::real(),
            genesis_block.as_ref(),
            signer.clone(),
        )
        .chunks(get_fake_next_block_chunk_headers(&genesis_block, epoch_manager.as_ref()))
        .protocol_version(pre_spice_protocol_version())
        .build();
        let block_missing_chunk = TestBlockBuilder::from_prev_block(
            Clock::real(),
            block_with_chunk.as_ref(),
            signer.clone(),
        )
        .protocol_version(pre_spice_protocol_version())
        .build();
        save_and_record_block(&mut chain, &block_with_chunk);
        save_and_record_block(&mut chain, &block_missing_chunk);

        let shard_layout =
            epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();
        let shard_uid = shard_layout.shard_uids().next().unwrap();

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
        let result = synthesize_execution_result(
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
        let result = synthesize_execution_result(
            &chain_store,
            epoch_manager.as_ref(),
            &block_missing_chunk,
            shard_id,
        )
        .unwrap();
        assert_eq!(result.chunk_extra, extra_missing_chunk);
        assert_eq!(result.outgoing_receipts_root, expected_root);
    }
}
