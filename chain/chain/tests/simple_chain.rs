use near_chain::test_utils::{display_chain, setup};
use near_chain::{Block, ErrorKind, Provenance};
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
    let (mut chain, _, signer) = setup();
    for i in 0..4 {
        let prev_hash = chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = Block::empty(&prev, signer.clone());
        let tip =
            chain.process_block(&None, block, Provenance::PRODUCED, |_, _, _| {}, |_| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.head().unwrap().height, 4);
}

#[test]
fn build_chain_with_orhpans() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let mut blocks = vec![chain.get_block(&chain.genesis().hash()).unwrap().clone()];
    for i in 1..4 {
        let block = Block::empty(&blocks[i - 1], signer.clone());
        blocks.push(block);
    }
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {}, |_| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {}, |_| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(
        &None,
        blocks.pop().unwrap(),
        Provenance::PRODUCED,
        |_, _, _| {},
        |_| {},
    );
    assert_eq!(res.unwrap().unwrap().height, 3);
    assert_eq!(
        chain
            .process_block(&None, blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {}, |_| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Unfit("already known in store".to_string())
    );
}

#[test]
fn build_chain_with_skips_and_forks() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let genesis = chain.get_block(&chain.genesis().hash()).unwrap();
    let b1 = Block::empty(&genesis, signer.clone());
    let b2 = Block::empty_with_height(&genesis, 2, signer.clone());
    let b3 = Block::empty_with_height(&b1, 3, signer.clone());
    let b4 = Block::empty_with_height(&b2, 4, signer.clone());
    let b5 = Block::empty(&b4, signer);
    assert!(chain.process_block(&None, b1, Provenance::PRODUCED, |_, _, _| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b2, Provenance::PRODUCED, |_, _, _| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b3, Provenance::PRODUCED, |_, _, _| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b4, Provenance::PRODUCED, |_, _, _| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b5, Provenance::PRODUCED, |_, _, _| {}, |_| {}).is_ok());
    assert!(chain.get_header_by_height(1).is_err());
    assert_eq!(chain.get_header_by_height(5).unwrap().height, 5);
}
