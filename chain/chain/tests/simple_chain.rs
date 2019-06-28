use std::collections::HashMap;

use near_chain::test_utils::setup;
use near_chain::{Block, ErrorKind, Provenance, RuntimeAdapter};
use near_primitives::test_utils::init_test_logger;

#[test]
fn empty_chain() {
    init_test_logger();
    let (chain, _, _) = setup();
    assert_eq!(chain.head().unwrap().height, 0);
}

#[test]
fn build_chain() {
    init_test_logger();
    let (mut chain, runtime, signer) = setup();
    let num_shards = runtime.num_shards();
    for i in 0..4 {
        let prev = chain.head_header().unwrap();
        let block = Block::empty(&prev, signer.clone(), num_shards);
        let tip = chain.process_block(&None, block, Provenance::PRODUCED, |_, _, _| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.head().unwrap().height, 4);
}

#[test]
fn build_chain_with_orhpans() {
    init_test_logger();
    let (mut chain, runtime, signer) = setup();
    let num_shards = runtime.num_shards();
    let mut blocks = vec![chain.get_block(&chain.genesis().hash()).unwrap().clone()];
    for i in 1..4 {
        let block = Block::empty(&blocks[i - 1].header, signer.clone(), num_shards);
        blocks.push(block);
    }
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {});
    assert_eq!(res.unwrap().unwrap().height, 3);
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Unfit("already known in store".to_string())
    );
}

#[test]
fn build_chain_with_skips_and_forks() {
    init_test_logger();
    let (mut chain, runtime, signer) = setup();
    let num_shards = runtime.num_shards();
    let b1 = Block::empty(chain.genesis(), signer.clone(), num_shards);
    let b2 = Block::produce(
        chain.genesis(),
        2,
        Block::genesis_chunks(vec![Block::genesis_hash()], num_shards),
        vec![],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    let b3 = Block::empty(&b1.header, signer.clone(), num_shards);
    let b4 = Block::produce(
        &b2.header,
        4,
        Block::genesis_chunks(vec![Block::genesis_hash()], num_shards),
        vec![],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    let b5 = Block::empty(&b4.header, signer, num_shards);
    assert!(chain.process_block(&None, b1, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(&None, b2, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(&None, b3, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(&None, b4, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(&None, b5, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.get_header_by_height(1).is_err());
    assert_eq!(chain.get_header_by_height(5).unwrap().height, 5);
}
