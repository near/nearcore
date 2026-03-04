use crate::store::ChainStoreAccess;
use crate::test_utils::setup;
use near_async::time::Clock;
use near_o11y::testonly::init_test_logger;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::TestBlockBuilder;

#[test]
fn chain_sync_headers() {
    init_test_logger();
    let (mut chain, _, _, bls_signer) = setup(Clock::real());
    assert_eq!(chain.header_head().unwrap().height, 0);
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..4 {
        blocks.push(
            TestBlockBuilder::from_prev_block(Clock::real(), &blocks[i], bls_signer.clone())
                .block_merkle_tree(&mut block_merkle_tree)
                .build(),
        )
    }

    chain
        .sync_block_headers(blocks.drain(1..).map(|block| block.header().clone().into()).collect())
        .unwrap();
    assert_eq!(chain.header_head().unwrap().height, 4);
}

/// Verifies that `sync_block_headers` populates the `ChunkProducers` DB column
/// and that `get_chunk_producer_info` reads from it consistently.
#[test]
fn chain_sync_headers_populates_chunk_producers_column() {
    use near_epoch_manager::EpochManagerAdapter;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    init_test_logger();
    let (mut chain, epoch_manager, _, bls_signer) = setup(Clock::real());
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..4 {
        blocks.push(
            TestBlockBuilder::from_prev_block(Clock::real(), &blocks[i], bls_signer.clone())
                .block_merkle_tree(&mut block_merkle_tree)
                .build(),
        );
    }

    chain
        .sync_block_headers(blocks[1..].iter().map(|block| block.header().clone().into()).collect())
        .unwrap();

    // Verify the ChunkProducers column was populated for each synced header.
    let store = chain.chain_store().store();
    for block in &blocks {
        let prev_block_hash = block.header().hash();
        let epoch_id = match epoch_manager.get_epoch_id_from_prev_block(prev_block_hash) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let key = get_block_shard_id(prev_block_hash, shard_id);
            let db_entry = store
                .get_ser::<near_primitives::types::validator_stake::ValidatorStake>(
                    DBCol::ChunkProducers,
                    &key,
                );
            assert!(
                db_entry.is_some(),
                "ChunkProducers column should be populated for block {} shard {}",
                prev_block_hash,
                shard_id,
            );

            // Verify consistency with get_chunk_producer_info.
            let from_lookup =
                epoch_manager.get_chunk_producer_info(prev_block_hash, shard_id).unwrap();
            assert_eq!(from_lookup, db_entry.unwrap());
        }
    }
}
