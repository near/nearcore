use crate::test_utils::setup;
use assert_matches::assert_matches;
use near_async::time::Clock;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::TestBlockBuilder;

#[test]
fn chain_sync_headers() {
    init_test_logger();
    let (mut chain, epoch_manager, _, bls_signer) = setup(Clock::real());
    assert_eq!(chain.header_head().unwrap().height, 0);
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..4 {
        // Only the first block after genesis is epoch-start; later prev blocks are
        // not yet in the epoch manager, so compute the hash only where it applies.
        let epoch_sync_data_hash = if blocks[i].header().is_genesis() {
            epoch_manager.compute_epoch_sync_data_hash(blocks[i].hash()).unwrap()
        } else {
            None
        };
        blocks.push(
            TestBlockBuilder::from_prev_block(Clock::real(), &blocks[i], bls_signer.clone())
                .block_merkle_tree(&mut block_merkle_tree)
                .epoch_sync_data_hash(epoch_sync_data_hash)
                .build(),
        )
    }

    chain
        .sync_block_headers(blocks.drain(1..).map(|block| block.header().clone().into()).collect())
        .unwrap();
    assert_eq!(chain.header_head().unwrap().height, 4);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn chain_sync_headers_records_spice_block_info() {
    init_test_logger();
    let (mut chain, epoch_manager, _, bls_signer) = setup(Clock::real());
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..3 {
        let epoch_sync_data_hash = if blocks[i].header().is_genesis() {
            epoch_manager.compute_epoch_sync_data_hash(blocks[i].hash()).unwrap()
        } else {
            None
        };
        blocks.push(
            TestBlockBuilder::from_prev_block(Clock::real(), &blocks[i], bls_signer.clone())
                .block_merkle_tree(&mut block_merkle_tree)
                .epoch_sync_data_hash(epoch_sync_data_hash)
                .build(),
        )
    }

    chain
        .sync_block_headers(blocks[1..].iter().map(|block| block.header().clone().into()).collect())
        .unwrap();

    for block in &blocks[1..] {
        let block_info = epoch_manager.get_block_info(block.hash()).unwrap();
        assert_eq!(
            block_info.last_certified_block_epoch(),
            block.header().prev_last_certified_block_epoch_id(),
        );
        assert_matches!(block_info.as_ref(), BlockInfo::V5(_));
    }
}
