use near_chain::test_utils::setup;
use near_chain::{Block, ChainStoreAccess, ErrorKind, Provenance};
use near_primitives::test_utils::init_test_logger;
use num_rational::Rational;

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
        let block = Block::empty(&prev, &*signer);
        let tip = chain
            .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
            .unwrap();
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
        let block = Block::empty(&blocks[i - 1], &*signer);
        blocks.push(block);
    }
    let last_block = &blocks[blocks.len() - 1];
    let block = Block::produce(
        &last_block.header,
        10,
        last_block.chunks.clone(),
        last_block.header.inner_lite.epoch_id.clone(),
        last_block.header.inner_lite.next_epoch_id.clone(),
        vec![],
        Rational::from_integer(0),
        0,
        Some(0),
        vec![],
        vec![],
        &*signer,
        last_block.header.inner_lite.next_bp_hash.clone(),
    );
    assert_eq!(
        chain
            .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(
                &None,
                blocks.pop().unwrap(),
                Provenance::PRODUCED,
                |_| {},
                |_| {},
                |_| {}
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(
                &None,
                blocks.pop().unwrap(),
                Provenance::PRODUCED,
                |_| {},
                |_| {},
                |_| {}
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(
        &None,
        blocks.pop().unwrap(),
        Provenance::PRODUCED,
        |_| {},
        |_| {},
        |_| {},
    );
    assert_eq!(res.unwrap().unwrap().height, 10);
    assert_eq!(
        chain
            .process_block(
                &None,
                blocks.pop().unwrap(),
                Provenance::PRODUCED,
                |_| {},
                |_| {},
                |_| {}
            )
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
    let b1 = Block::empty(&genesis, &*signer);
    let b2 = Block::empty_with_height(&genesis, 2, &*signer);
    let b3 = Block::empty_with_height(&b1, 3, &*signer);
    let b4 = Block::empty_with_height(&b2, 4, &*signer);
    let b5 = Block::empty(&b4, &*signer);
    assert!(chain.process_block(&None, b1, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b2, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b3, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b4, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b5, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.get_header_by_height(1).is_err());
    assert_eq!(chain.get_header_by_height(5).unwrap().inner_lite.height, 5);
}

/// Verifies that the block at height are updated correctly when blocks from different forks are
/// processed, especially when certain heights are skipped
#[test]
fn blocks_at_height() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let genesis = chain.get_block_by_height(0).unwrap();
    let b_1 = Block::empty_with_height(genesis, 1, &*signer);
    let b_2 = Block::empty_with_height(&b_1, 2, &*signer);
    let b_3 = Block::empty_with_height(&b_2, 3, &*signer);

    let c_1 = Block::empty_with_height(&genesis, 1, &*signer);
    let c_3 = Block::empty_with_height(&c_1, 3, &*signer);
    let c_4 = Block::empty_with_height(&c_3, 4, &*signer);
    let c_5 = Block::empty_with_height(&c_4, 5, &*signer);

    let d_3 = Block::empty_with_height(&b_2, 3, &*signer);
    let d_4 = Block::empty_with_height(&d_3, 4, &*signer);
    let d_6 = Block::empty_with_height(&d_4, 6, &*signer);

    let e_7 = Block::empty_with_height(&b_1, 7, &*signer);

    let b_1_hash = b_1.hash();
    let b_2_hash = b_2.hash();
    let b_3_hash = b_3.hash();

    let c_1_hash = c_1.hash();
    let c_3_hash = c_3.hash();
    let c_4_hash = c_4.hash();
    let c_5_hash = c_5.hash();

    let d_3_hash = d_3.hash();
    let d_4_hash = d_4.hash();
    let d_6_hash = d_6.hash();

    let e_7_hash = e_7.hash();

    assert_ne!(d_3_hash, b_3_hash);

    chain.process_block(&None, b_1, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, b_2, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, b_3, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 3);

    assert_eq!(chain.get_header_by_height(1).unwrap().hash(), b_1_hash);
    assert_eq!(chain.get_header_by_height(2).unwrap().hash(), b_2_hash);
    assert_eq!(chain.get_header_by_height(3).unwrap().hash(), b_3_hash);

    chain.process_block(&None, c_1, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, c_3, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, c_4, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, c_5, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 5);

    assert_eq!(chain.get_header_by_height(1).unwrap().hash(), c_1_hash);
    assert!(chain.get_header_by_height(2).is_err());
    assert_eq!(chain.get_header_by_height(3).unwrap().hash(), c_3_hash);
    assert_eq!(chain.get_header_by_height(4).unwrap().hash(), c_4_hash);
    assert_eq!(chain.get_header_by_height(5).unwrap().hash(), c_5_hash);

    chain.process_block(&None, d_3, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, d_4, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    chain.process_block(&None, d_6, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 6);

    assert_eq!(chain.get_header_by_height(1).unwrap().hash(), b_1_hash);
    assert_eq!(chain.get_header_by_height(2).unwrap().hash(), b_2_hash);
    assert_eq!(chain.get_header_by_height(3).unwrap().hash(), d_3_hash);
    assert_eq!(chain.get_header_by_height(4).unwrap().hash(), d_4_hash);
    assert!(chain.get_header_by_height(5).is_err());
    assert_eq!(chain.get_header_by_height(6).unwrap().hash(), d_6_hash);

    chain.process_block(&None, e_7, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).unwrap();

    assert_eq!(chain.get_header_by_height(1).unwrap().hash(), b_1_hash);
    for h in 2..=5 {
        assert!(chain.get_header_by_height(h).is_err());
    }
    assert_eq!(chain.get_header_by_height(7).unwrap().hash(), e_7_hash);
}

#[test]
fn next_blocks() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let genesis = chain.get_block(&chain.genesis().hash()).unwrap();
    let b1 = Block::empty(&genesis, &*signer);
    let b2 = Block::empty_with_height(&b1, 2, &*signer);
    let b3 = Block::empty_with_height(&b1, 3, &*signer);
    let b4 = Block::empty_with_height(&b3, 4, &*signer);
    let b1_hash = b1.hash();
    let b2_hash = b2.hash();
    let b3_hash = b3.hash();
    let b4_hash = b4.hash();
    assert!(chain.process_block(&None, b1, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b2, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert_eq!(chain.mut_store().get_next_block_hash(&b1_hash).unwrap(), &b2_hash);
    assert!(chain.process_block(&None, b3, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert!(chain.process_block(&None, b4, Provenance::PRODUCED, |_| {}, |_| {}, |_| {}).is_ok());
    assert_eq!(chain.mut_store().get_next_block_hash(&b1_hash).unwrap(), &b3_hash);
    assert_eq!(chain.mut_store().get_next_block_hash(&b3_hash).unwrap(), &b4_hash);
}
