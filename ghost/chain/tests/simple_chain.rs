use std::sync::Arc;

use near_chain::{Block, BlockHeader, Chain, ErrorKind, Provenance, RuntimeAdapter};
use near_chain::test_utils::setup;
use primitives::test_utils::init_test_logger;

#[test]
fn empty_chain() {
    init_test_logger();
    let (chain, _, _) = setup();
    assert_eq!(chain.store().head().unwrap().height, 0);
}

#[test]
fn build_chain() {
    init_test_logger();
    let (mut chain, runtime, signer) = setup();
    for i in 0..4 {
        let prev = chain.head_header().unwrap();
        let block = Block::empty(&prev, signer.clone());
        let tip = chain.process_block(block, Provenance::PRODUCED, |_, _, _| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.store().head().unwrap().height, 4);
}

#[test]
fn build_chain_with_orhpans() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let mut blocks = vec![chain.get_block(&chain.genesis().hash()).unwrap().clone()];
    for i in 1..4 {
        let block = Block::empty(&blocks[i - 1].header, signer.clone());
        blocks.push(block);
    }
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {});
    assert_eq!(res.unwrap().unwrap().height, 3);
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Unfit("already known in store".to_string())
    );
}
