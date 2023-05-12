use crate::test_utils::setup;
use near_o11y::testonly::init_test_logger;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::TestBlockBuilder;

#[test]
fn chain_sync_headers() {
    init_test_logger();
    let (mut chain, _, _, bls_signer) = setup();
    assert_eq!(chain.header_head().unwrap().height, 0);
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..4 {
        blocks.push(
            TestBlockBuilder::new(&blocks[i], bls_signer.clone())
                .block_merkle_tree(&mut block_merkle_tree)
                .build(),
        )
    }

    let mut challenges = vec![];
    chain
        .sync_block_headers(
            blocks.drain(1..).map(|block| block.header().clone()).collect(),
            &mut challenges,
        )
        .unwrap();
    assert_eq!(chain.header_head().unwrap().height, 4);
    assert!(challenges.is_empty());
}
