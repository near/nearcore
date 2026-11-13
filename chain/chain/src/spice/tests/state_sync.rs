use super::core::{
    build_block, endorsement_into_core_statement, process_block,
    run_record_uncertified_chunks_for_block, setup, test_chunk_endorsement,
    test_execution_result_for_chunk, test_validators,
};
use crate::spice::boundary::{boundary_uncertified_chunks, write_boundary_uncertified_chunks};
use crate::spice::core::{
    CertificationWindow, certification_window,
    record_uncertified_chunks_by_replaying_certification_window,
};
use crate::spice::tests::pre_spice::{
    grow_to_last_pre_spice_block, save_and_record_block, setup_pre_spice_chain_with_epoch_length,
};
use crate::{BlockProcessingArtifact, Chain, Error};
use assert_matches::assert_matches;
use near_async::time::Clock;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::{ShardId, SpiceChunkId, SpiceUncertifiedChunkInfo};
use near_primitives::version::ProtocolFeature;
use near_store::DBCol;
use near_store::adapter::StoreAdapter as _;
use std::sync::Arc;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_certification_window_reaches_past_a_partly_certified_block() {
    let (mut chain, _core_reader) = setup();
    let blocks = build_chain_with_staggered_certification(&mut chain);
    let num_blocks = blocks.len();

    let window = certification_window(
        &chain.chain_store.store().chain_store(),
        chain.epoch_manager.as_ref(),
        blocks[num_blocks - 1].hash(),
    );

    // The head certifies its parent's fast chunk but not its slow ones, so the window goes on to
    // the parent. The block before it is certified whole.
    assert_matches!(
        window,
        Ok(CertificationWindow::Complete(hashes))
            if hashes == vec![*blocks[num_blocks - 1].hash(), *blocks[num_blocks - 2].hash()]
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_state_sync_rebuilds_uncertified_chunks_under_staggered_certification() {
    let (mut chain, _core_reader) = setup();
    let blocks = build_chain_with_staggered_certification(&mut chain);
    let sync_block = build_block_with_staggered_certification(&chain, &blocks);
    // A syncing node has the sync block's header from header sync.
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block_header(sync_block.header().clone()).unwrap();
    store_update.commit().unwrap();

    for row in assert_state_sync_rebuilds_uncertified_chunks(&mut chain, &sync_block) {
        assert!(row.iter().any(|chunk| !chunk.present_endorsements.is_empty()), "{row:?}");
    }
    process_block(&mut chain, sync_block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_state_sync_rebuilds_uncertified_chunks_down_to_the_last_pre_spice_block() {
    let mut chain = setup_pre_spice_chain_with_epoch_length(2, 5);
    let (last_pre_spice, _) = grow_to_last_pre_spice_block(&mut chain);
    // The last pre-spice block's processing seeds its row.
    let mut store_update = chain.chain_store.store().store_update();
    write_boundary_uncertified_chunks(
        &mut store_update,
        chain.epoch_manager.as_ref(),
        last_pre_spice.header(),
    )
    .unwrap();
    store_update.commit();
    let first_spice = save_spice_block(&mut chain, &last_pre_spice);
    run_record_uncertified_chunks_for_block(&mut chain, &first_spice);
    let parent = save_spice_block(&mut chain, &first_spice);
    run_record_uncertified_chunks_for_block(&mut chain, &parent);
    let sync_block = save_spice_block(&mut chain, &parent);

    let window = certification_window(
        &chain.chain_store.store().chain_store(),
        chain.epoch_manager.as_ref(),
        first_spice.hash(),
    );

    assert_matches!(
        window,
        Ok(CertificationWindow::Complete(hashes)) if hashes == vec![*first_spice.hash()]
    );
    assert_state_sync_rebuilds_uncertified_chunks(&mut chain, &sync_block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_state_sync_to_the_first_spice_block_seeds_the_last_pre_spice_block() {
    let mut chain = setup_pre_spice_chain_with_epoch_length(2, 5);
    let (last_pre_spice, _) = grow_to_last_pre_spice_block(&mut chain);
    let sync_block = save_spice_block(&mut chain, &last_pre_spice);
    assert_eq!(recorded_uncertified_chunks(&chain, &last_pre_spice), None);

    chain
        .reset_heads_post_state_sync(
            *sync_block.hash(),
            &mut BlockProcessingArtifact::default(),
            None,
        )
        .unwrap();

    let seeded =
        boundary_uncertified_chunks(chain.epoch_manager.as_ref(), last_pre_spice.header()).unwrap();
    assert_eq!(recorded_uncertified_chunks(&chain, &last_pre_spice), Some(seeded));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_certification_window_stops_at_a_missing_body() {
    let (mut chain, _core_reader) = setup();
    let blocks = build_chain_with_staggered_certification(&mut chain);
    let num_blocks = blocks.len();
    let mut store_update = chain.chain_store.store().store_update();
    store_update.delete(DBCol::Block, blocks[num_blocks - 2].hash().as_ref());
    store_update.delete(DBCol::uncertified_chunks(), blocks[num_blocks - 1].hash().as_ref());
    store_update.commit();
    let chain_store = chain.chain_store.store().chain_store();
    let epoch_manager = chain.epoch_manager.as_ref();

    assert_matches!(
        certification_window(&chain_store, epoch_manager, blocks[num_blocks - 1].hash()),
        Ok(CertificationWindow::MissingBody { known, missing })
            if known == vec![*blocks[num_blocks - 1].hash()]
                && &missing == blocks[num_blocks - 2].hash()
    );
    assert_matches!(
        record_uncertified_chunks_by_replaying_certification_window(
            &chain_store,
            epoch_manager,
            blocks[num_blocks - 1].hash(),
        ),
        Err(Error::Other(message)) if message.contains("missing the body of block")
    );
}

/// Every block is fully certified by its grandchild.
fn build_chain_with_staggered_certification(chain: &mut Chain) -> Vec<Arc<Block>> {
    const NUM_BLOCKS: usize = 4;

    let mut blocks = vec![chain.genesis_block()];
    for _ in 0..NUM_BLOCKS {
        let block = build_block_with_staggered_certification(chain, &blocks);
        process_block(chain, block.clone());
        blocks.push(block);
    }
    blocks
}

fn build_block_with_staggered_certification(chain: &Chain, blocks: &[Arc<Block>]) -> Arc<Block> {
    // A block's chunks in these shards are certified by its child.
    const FAST_SHARDS: [ShardId; 1] = [ShardId::new(0)];
    // A block's chunks in these shards are endorsed below the threshold by its child and certified
    // by its grandchild.
    const SLOW_SHARDS: [ShardId; 2] = [ShardId::new(1), ShardId::new(2)];

    let validators = test_validators();
    let (partial_endorsers, other_endorsers) = validators.split_at(1);
    let endorsements = |block: &Block, shard_ids: &[ShardId], endorsers: &[String]| {
        let chunks = block.chunks();
        chunks
            .iter_raw()
            .filter(|chunk| shard_ids.contains(&chunk.shard_id()))
            .flat_map(|chunk| {
                endorsers.iter().map(move |endorser| {
                    endorsement_into_core_statement(test_chunk_endorsement(endorser, block, chunk))
                })
            })
            .collect::<Vec<_>>()
    };
    let execution_results = |block: &Block, shard_ids: &[ShardId]| {
        let chunks = block.chunks();
        chunks
            .iter_raw()
            .filter(|chunk| shard_ids.contains(&chunk.shard_id()))
            .map(|chunk| SpiceCoreStatement::ChunkExecutionResult {
                chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk.shard_id() },
                execution_result: test_execution_result_for_chunk(chunk),
            })
            .collect::<Vec<_>>()
    };

    let mut ancestors = blocks.iter().rev();
    let parent = ancestors.next().unwrap();
    let mut core_statements = Vec::new();
    if !parent.header().is_genesis() {
        core_statements.extend(endorsements(parent, &FAST_SHARDS, &validators));
        core_statements.extend(execution_results(parent, &FAST_SHARDS));
        core_statements.extend(endorsements(parent, &SLOW_SHARDS, partial_endorsers));
    }
    if let Some(grandparent) = ancestors.next().filter(|block| !block.header().is_genesis()) {
        core_statements.extend(endorsements(grandparent, &SLOW_SHARDS, other_endorsers));
        core_statements.extend(execution_results(grandparent, &SLOW_SHARDS));
    }
    build_block(chain, parent, core_statements)
}

fn assert_state_sync_rebuilds_uncertified_chunks(
    chain: &mut Chain,
    sync_block: &Block,
) -> [Vec<SpiceUncertifiedChunkInfo>; 2] {
    let parent = chain.get_block(sync_block.header().prev_hash()).unwrap();
    let parent_row = recorded_uncertified_chunks(chain, &parent).unwrap();
    let grandparent = chain.get_block(parent.header().prev_hash()).unwrap();
    let grandparent_row = recorded_uncertified_chunks(chain, &grandparent).unwrap();
    let mut store_update = chain.chain_store.store().store_update();
    store_update.delete(DBCol::uncertified_chunks(), grandparent.hash().as_ref());
    store_update.delete(DBCol::uncertified_chunks(), parent.hash().as_ref());
    store_update.commit();

    chain
        .reset_heads_post_state_sync(
            *sync_block.hash(),
            &mut BlockProcessingArtifact::default(),
            None,
        )
        .unwrap();

    assert_eq!(recorded_uncertified_chunks(chain, &grandparent).as_ref(), Some(&grandparent_row));
    assert_eq!(recorded_uncertified_chunks(chain, &parent).as_ref(), Some(&parent_row));

    [grandparent_row, parent_row]
}

fn recorded_uncertified_chunks(
    chain: &Chain,
    block: &Block,
) -> Option<Vec<SpiceUncertifiedChunkInfo>> {
    chain.chain_store.store().get_ser(DBCol::uncertified_chunks(), block.hash().as_ref())
}

fn save_spice_block(chain: &mut Chain, prev_block: &Block) -> Arc<Block> {
    let protocol_version = ProtocolFeature::Spice.protocol_version();
    let epoch_manager = chain.epoch_manager.as_ref();
    let block = TestBlockBuilder::from_prev_block(
        Clock::real(),
        prev_block,
        Arc::new(create_test_signer("test1")),
    )
    .epoch_id(epoch_manager.get_epoch_id_from_prev_block(prev_block.hash()).unwrap())
    .next_epoch_id(epoch_manager.get_next_epoch_id_from_prev_block(prev_block.hash()).unwrap())
    .protocol_version(protocol_version)
    .build();
    save_and_record_block(chain, &block, protocol_version);
    block
}
