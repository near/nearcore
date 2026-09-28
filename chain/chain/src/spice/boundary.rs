//! Seeding for the spice activation boundary: the last pre-spice block, whose chunks
//! are the first to be certified under spice.

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, Tip};
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{SpiceChunkId, SpiceUncertifiedChunkInfo};
use near_primitives::version::ProtocolFeature;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{DBCol, StoreUpdate};
use std::sync::Arc;

/// Whether `block_hash` is a last pre-spice block: a last block of a pre-spice epoch
/// whose next epoch is spice, so every child of it is a first spice block. The
/// predicate is per block, not per chain: concurrent forks can each hold one.
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
    if block.is_spice_block() {
        seed_execution_heads_at_activation(store_update, block, prev_header)
    } else if is_last_pre_spice_block(epoch_manager, block.hash())? {
        write_boundary_uncertified_chunks(store_update, epoch_manager, block.header())
    } else {
        Ok(())
    }
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

/// The uncertified-chunks row for the last pre-spice block `header`, one entry per
/// shard of its layout, with every designated endorsement missing.
fn boundary_uncertified_chunks(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<Vec<SpiceUncertifiedChunkInfo>, Error> {
    let epoch_id = header.epoch_id();
    let height = header.height();
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
            chunk_id: SpiceChunkId { block_hash: *header.hash(), shard_id },
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

/// Writes the `DBCol::uncertified_chunks` row of the last pre-spice block `header`.
fn write_boundary_uncertified_chunks(
    store_update: &mut StoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> Result<(), Error> {
    if !cfg!(feature = "protocol_feature_spice") {
        return Ok(());
    }
    let uncertified_chunks = boundary_uncertified_chunks(epoch_manager, header)?;
    store_update.insert_ser(
        DBCol::uncertified_chunks(),
        header.hash().as_ref(),
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
    let uncertified_chunks: Vec<SpiceUncertifiedChunkInfo> = chain_store
        .store_ref()
        .get_ser(DBCol::uncertified_chunks(), block_hash.as_ref())
        .unwrap_or_default();
    debug_assert!(
        uncertified_chunks.iter().all(|chunk| &chunk.chunk_id.block_hash == block_hash),
        "seeded uncertified chunks of {block_hash} reference another block"
    );
    uncertified_chunks
}

#[cfg(test)]
mod tests {
    use super::{boundary_uncertified_chunks, seed_activation_boundary, seeded_uncertified_chunks};
    use crate::Chain;
    use crate::spice::core::record_uncertified_chunks_for_block;
    use crate::spice::tests::pre_spice::{
        add_pre_spice_block, grow_to_last_pre_spice_block, setup_pre_spice_chain,
        setup_pre_spice_chain_with_epoch_length,
    };
    use near_async::time::Clock;
    use near_chain_primitives::Error;
    use near_primitives::block::Block;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::SpiceUncertifiedChunkInfo;
    use near_primitives::version::ProtocolFeature;
    use near_store::DBCol;
    use near_store::adapter::StoreAdapter;
    use std::sync::Arc;

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
            boundary_uncertified_chunks(epoch_manager.as_ref(), block.header()).unwrap();
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

    /// Postprocessing the last pre-spice block seeds its row; postprocessing its parent,
    /// an ordinary pre-spice block, seeds nothing.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_seeding_writes_the_row_of_the_last_pre_spice_block_only() {
        let mut chain = setup_pre_spice_chain_with_epoch_length(2, 5);
        let epoch_manager = chain.epoch_manager.clone();
        let (last_pre_spice, parent) = grow_to_last_pre_spice_block(&mut chain);
        let grandparent = chain.get_block_header(parent.header().prev_hash()).unwrap();
        let chain_store = chain.chain_store.store().chain_store();

        let mut store_update = chain.chain_store.store().store_update();
        seed_activation_boundary(&mut store_update, epoch_manager.as_ref(), &parent, &grandparent)
            .unwrap();
        seed_activation_boundary(
            &mut store_update,
            epoch_manager.as_ref(),
            &last_pre_spice,
            parent.header(),
        )
        .unwrap();
        store_update.commit();

        assert_eq!(seeded_uncertified_chunks(&chain_store, parent.hash()), vec![]);
        assert_eq!(
            seeded_uncertified_chunks(&chain_store, last_pre_spice.hash()),
            boundary_uncertified_chunks(epoch_manager.as_ref(), last_pre_spice.header()).unwrap()
        );
    }

    /// A first spice block seeds the execution heads and nothing else: its parent's row
    /// is the parent's own postprocessing's to write.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_seeding_at_the_first_spice_block_writes_the_execution_heads_only() {
        let mut chain = setup_pre_spice_chain(1);
        let epoch_manager = chain.epoch_manager.clone();
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let parent = add_pre_spice_block(&mut chain, &genesis_block, &[]);
        let first_spice = TestBlockBuilder::from_prev_block(
            Clock::real(),
            &parent,
            Arc::new(create_test_signer("test1")),
        )
        .protocol_version(ProtocolFeature::Spice.protocol_version())
        .build();
        let chain_store = chain.chain_store.store().chain_store();
        assert!(chain_store.spice_execution_head().is_err());

        let mut store_update = chain.chain_store.store().store_update();
        seed_activation_boundary(
            &mut store_update,
            epoch_manager.as_ref(),
            &first_spice,
            parent.header(),
        )
        .unwrap();
        store_update.commit();

        assert_eq!(seeded_uncertified_chunks(&chain_store, parent.hash()), vec![]);
        assert_eq!(&chain_store.spice_execution_head().unwrap().last_block_hash, parent.hash());
    }

    /// Recording the first spice block's row in the order of block postprocessing, with
    /// its parent's row committed by the parent's own postprocessing, carries the
    /// parent's seeded chunks over unchanged alongside the block's own.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_recording_the_first_spice_block_carries_seeded_boundary_chunks() {
        let mut chain = setup_pre_spice_chain_with_epoch_length(2, 5);
        let epoch_manager = chain.epoch_manager.clone();
        let (last_pre_spice, parent) = grow_to_last_pre_spice_block(&mut chain);
        let first_spice = build_first_spice_block(&chain, &last_pre_spice);

        // The last pre-spice block's postprocessing.
        let mut store_update = chain.chain_store.store().store_update();
        seed_activation_boundary(
            &mut store_update,
            epoch_manager.as_ref(),
            &last_pre_spice,
            parent.header(),
        )
        .unwrap();
        store_update.commit();

        // The first spice block's postprocessing: seeding, then recording, one commit.
        let mut boundary_update = chain.chain_store.store().store_update();
        let mut chain_store_update = chain.chain_store.store_update();
        seed_activation_boundary(
            &mut boundary_update,
            epoch_manager.as_ref(),
            &first_spice,
            last_pre_spice.header(),
        )
        .unwrap();
        chain_store_update.merge(boundary_update);
        record_uncertified_chunks_for_block(
            &mut chain_store_update,
            epoch_manager.as_ref(),
            &first_spice,
        )
        .unwrap();
        chain_store_update.commit().unwrap();

        let seeded =
            boundary_uncertified_chunks(epoch_manager.as_ref(), last_pre_spice.header()).unwrap();
        let recorded: Vec<SpiceUncertifiedChunkInfo> = chain
            .chain_store
            .store()
            .get_ser(DBCol::uncertified_chunks(), first_spice.hash().as_ref())
            .unwrap();
        let shard_layout = epoch_manager.get_shard_layout(first_spice.header().epoch_id()).unwrap();
        assert_eq!(recorded.len(), seeded.len() + shard_layout.num_shards() as usize);
        assert_eq!(&recorded[..seeded.len()], &seeded[..]);
        assert!(
            recorded[seeded.len()..]
                .iter()
                .all(|chunk| &chunk.chunk_id.block_hash == first_spice.hash())
        );
    }

    /// Recording the first spice block's row without its parent's seeded row is an
    /// error: continuing would drop the boundary chunks from certification for good.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_record_first_spice_block_errors_without_seeded_parent_row() {
        let mut chain = setup_pre_spice_chain_with_epoch_length(2, 5);
        let epoch_manager = chain.epoch_manager.clone();
        let (last_pre_spice, _parent) = grow_to_last_pre_spice_block(&mut chain);
        let first_spice = build_first_spice_block(&chain, &last_pre_spice);

        let mut chain_store_update = chain.chain_store.store_update();
        let result = record_uncertified_chunks_for_block(
            &mut chain_store_update,
            epoch_manager.as_ref(),
            &first_spice,
        );
        let Err(Error::Other(message)) = result else {
            panic!("expected a missing seeded row error, got {result:?}");
        };
        assert!(
            message.contains("missing seeded uncertified chunks of last pre-spice block"),
            "{message}"
        );
    }

    /// Fabricates the first spice block on top of `last_pre_spice` without saving it.
    fn build_first_spice_block(chain: &Chain, last_pre_spice: &Block) -> Arc<Block> {
        let epoch_manager = chain.epoch_manager.as_ref();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let next_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        TestBlockBuilder::from_prev_block(
            Clock::real(),
            last_pre_spice,
            Arc::new(create_test_signer("test1")),
        )
        .epoch_id(epoch_id)
        .next_epoch_id(next_epoch_id)
        .protocol_version(ProtocolFeature::Spice.protocol_version())
        .build()
    }
}
