use near_chain::test_utils::setup;
use near_chain::Block;
use near_logger_utils::init_test_logger;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::validator_signer::ValidatorSigner;
use std::sync::{Arc, Mutex};

#[test]
fn chain_sync_headers() {
    init_test_logger();
    let (mut chain, _, bls_signer) = setup();
    assert_eq!(chain.sync_head().unwrap().height, 0);
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap().clone()];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for i in 0..4 {
        blocks.push(Block::empty_with_block_merkle_tree(
            &blocks[i],
            &*bls_signer,
            &mut block_merkle_tree,
        ));
    }
    chain
        .sync_block_headers(blocks.drain(1..).map(|block| block.header().clone()).collect(), |_| {
            panic!("Unexpected")
        })
        .unwrap();
    assert_eq!(chain.sync_head().unwrap().height, 4);
}

#[test]
fn test_received_blocks() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap().clone();
    let b2 = Block::empty_with_height(&genesis, 10, &*signer);
    chain.save_orphan(&b2);
    let received_blocks = chain.blocks_received().unwrap();
    assert_eq!(received_blocks, 1);
    let blocks_accepted = Arc::new(Mutex::new(vec![]));
    chain
        .check_orphans(
            &Some(signer.validator_id().to_string()),
            *genesis.hash(),
            |b| {
                blocks_accepted.lock().unwrap().push(b);
            },
            |_| {},
            |_| {},
        )
        .unwrap();
    assert_eq!(blocks_accepted.lock().unwrap().len(), 1);
    let received_blocks = chain.blocks_received().unwrap();
    assert_eq!(received_blocks, 1);
}
