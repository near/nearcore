use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{EpochId, SpiceChunkId, SpiceUncertifiedChunkInfo};
use near_primitives::version::ProtocolFeature;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{DBCol, StoreUpdate};
use std::sync::Arc;

/// Whether `block_hash` is a last pre-spice block: a last block of the last
/// pre-spice epoch, so every child of it is a first spice block.
pub fn is_last_pre_spice_block(
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

/// The last pre-spice block in the ancestry of `block_hash`, `block_hash` itself when
/// it is pre-spice. Errors on a chain that is spice from genesis.
pub fn last_pre_spice_block_header(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<Arc<BlockHeader>, Error> {
    let mut header = chain_store.get_block_header(block_hash)?;
    while header.is_spice() {
        let epoch_first_block = *epoch_manager.get_block_info(header.hash())?.epoch_first_block();
        let epoch_first_header = chain_store.get_block_header(&epoch_first_block)?;
        header = chain_store.get_block_header(epoch_first_header.prev_hash())?;
    }
    Ok(header)
}

/// Seeds what the activation boundary needs when `block` is a last pre-spice block
/// or a first spice block; a no-op otherwise.
pub fn seed_activation_boundary(
    store_update: &mut StoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
    prev_header: &BlockHeader,
) -> Result<(), Error> {
    seed_boundary_uncertified_chunks(store_update, epoch_manager, block)?;
    seed_execution_heads_at_activation(store_update, block, prev_header)
}

/// Seeds the spice execution heads when `block` is a first spice block, i.e. when
/// `prev_header` is still pre-spice; a no-op otherwise.
pub fn seed_execution_heads_at_activation(
    store_update: &mut StoreUpdate,
    block: &Block,
    prev_header: &BlockHeader,
) -> Result<(), Error> {
    if !block.is_spice_block() || prev_header.is_spice() {
        return Ok(());
    }
    let mut adapter = store_update.chain_store_update();
    adapter.set_spice_execution_head(&Tip::from_header(prev_header))?;
    adapter.update_spice_final_execution_head(block)?;
    Ok(())
}

/// The uncertified-chunks row for the last pre-spice block `block`, one entry per shard
/// of its layout, with every designated endorsement missing.
fn boundary_uncertified_chunks(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
    let epoch_id = block.header().epoch_id();
    let height = block.header().height();
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let mut uncertified_chunks = Vec::with_capacity(shard_layout.num_shards() as usize);
    for shard_id in shard_layout.shard_ids() {
        let chunk_validator_assignments =
            epoch_manager.get_chunk_validator_assignments(epoch_id, shard_id, height)?;
        let missing_endorsements = chunk_validator_assignments
            .assignments()
            .iter()
            .map(|(account_id, _)| account_id)
            .cloned()
            .collect();
        uncertified_chunks.push(SpiceUncertifiedChunkInfo {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id },
            missing_endorsements,
            present_endorsements: Vec::new(),
            present_fallback_endorsements: Vec::new(),
            // The block before the last pre-spice block is pre-spice, certified by
            // definition, so the designated validators can act right away.
            certifiable_since_height: Some(height),
        });
    }
    Ok(uncertified_chunks)
}

/// Seeds `DBCol::uncertified_chunks` for `block` when it is a last pre-spice block; a
/// no-op otherwise.
fn seed_boundary_uncertified_chunks(
    store_update: &mut StoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    if !cfg!(feature = "protocol_feature_spice") {
        return Ok(());
    }
    if !is_last_pre_spice_block(epoch_manager, block.hash())? {
        return Ok(());
    }
    let uncertified_chunks = boundary_uncertified_chunks(epoch_manager, block)?;
    store_update.insert_ser(
        DBCol::uncertified_chunks(),
        block.hash().as_ref(),
        &uncertified_chunks,
    );
    Ok(())
}

/// The seeded uncertified-chunks row of the pre-spice `block_hash`: present only for
/// a last pre-spice block, empty otherwise.
pub(crate) fn seeded_uncertified_chunks(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Vec<SpiceUncertifiedChunkInfo> {
    if !cfg!(feature = "protocol_feature_spice") {
        return vec![];
    }
    chain_store
        .store_ref()
        .get_ser(DBCol::uncertified_chunks(), block_hash.as_ref())
        .unwrap_or_default()
}

/// The epoch whose chunk producers produce the spice data of `block_hash`: its own,
/// or for a last pre-spice block the next one, whose producers run the boundary
/// bootstrap.
pub fn spice_producers_epoch_id(
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<EpochId, Error> {
    if is_last_pre_spice_block(epoch_manager, block_hash)? {
        Ok(epoch_manager.get_epoch_id_from_prev_block(block_hash)?)
    } else {
        Ok(epoch_manager.get_epoch_id(block_hash)?)
    }
}

/// The prev hash shard tracking of `block`'s spice applications is keyed on: `block`
/// itself for a last pre-spice block, whose chunks are bootstrapped by the shards
/// tracked in the first spice epoch.
pub fn spice_tracking_prev_hash(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<CryptoHash, Error> {
    if is_last_pre_spice_block(epoch_manager, block.hash())? {
        Ok(*block.hash())
    } else {
        Ok(*block.header().prev_hash())
    }
}

#[cfg(test)]
mod tests {
    use super::boundary_uncertified_chunks;
    use crate::spice::tests::{add_pre_spice_block, setup_pre_spice_chain};
    use near_store::DBCol;
    use near_store::adapter::StoreAdapter;

    /// A row seeded for a pre-spice block is read back by the core reader, while a
    /// pre-spice block without one keeps reading as "nothing to certify".
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_core_reader_returns_seeded_uncertified_chunks_of_pre_spice_block() {
        let mut chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let block = add_pre_spice_block(&mut chain, &genesis_block, &[]);

        let core_reader = chain.spice_core_reader.clone();
        assert_eq!(core_reader.get_uncertified_chunks(block.hash()).unwrap(), vec![]);

        let uncertified_chunks =
            boundary_uncertified_chunks(epoch_manager.as_ref(), &block).unwrap();
        assert!(!uncertified_chunks.is_empty());
        let mut store_update = chain.chain_store.store().store_update();
        store_update.insert_ser(
            DBCol::uncertified_chunks(),
            block.hash().as_ref(),
            &uncertified_chunks,
        );
        store_update.commit();

        assert_eq!(core_reader.get_uncertified_chunks(block.hash()).unwrap(), uncertified_chunks);
    }
}
