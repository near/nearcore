use near_chain::test_utils::setup;
use near_chain::{Block, ErrorKind, Provenance};
use near_primitives::test_utils::init_test_logger;

#[test]
fn challenges_new_head_prev() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let mut hashes = vec![];
    for i in 0..5 {
        let prev_hash = chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = Block::empty(&prev, &*signer);
        hashes.push(block.hash());
        let tip = chain
            .process_block(&None, block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
            .unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }

    assert_eq!(chain.head().unwrap().height, 5);

    // The block to be added below after we invalidated fourth block.
    let last_block = Block::empty(&chain.get_block(&hashes[3]).unwrap(), &*signer);
    assert_eq!(last_block.header.inner_lite.height, 5);

    let prev = chain.get_block(&hashes[1]).unwrap();
    let challenger_block = Block::empty_with_height(&prev, 3, &*signer);
    let challenger_hash = challenger_block.hash();

    let _ = chain
        .process_block(&None, challenger_block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap();

    // At this point the challenger block is not on canonical chain
    assert_eq!(chain.head_header().unwrap().inner_lite.height, 5);

    // Challenge fourth block. The third block and the challenger block have the same height, the
    //   current logic will choose the third block.
    chain.mark_block_as_challenged(&hashes[3], &challenger_hash).unwrap();

    assert_eq!(chain.head_header().unwrap().hash(), hashes[2]);

    assert_eq!(chain.get_header_by_height(2).unwrap().hash(), hashes[1]);
    assert_eq!(chain.get_header_by_height(3).unwrap().hash(), hashes[2]);
    assert!(chain.get_header_by_height(4).is_err());

    // Try to add a block on top of the fifth block.

    if let Err(e) =
        chain.process_block(&None, last_block, Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
    {
        assert_eq!(e.kind(), ErrorKind::ChallengedBlockOnChain)
    } else {
        assert!(false);
    }
    assert_eq!(chain.head_header().unwrap().hash(), hashes[2]);

    // Add two more blocks
    let b3 = Block::empty(&chain.get_block(&hashes[2]).unwrap().clone(), &*signer);
    let _ = chain
        .process_block(&None, b3.clone(), Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap()
        .unwrap();

    let b4 = Block::empty(&b3, &*signer);
    let new_head = chain
        .process_block(&None, b4.clone(), Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap()
        .unwrap()
        .last_block_hash;

    assert_eq!(chain.head_header().unwrap().hash(), new_head);

    // Add two more blocks on an alternative chain
    let b3 = Block::empty(&chain.get_block(&hashes[2]).unwrap().clone(), &*signer);
    let _ = chain
        .process_block(&None, b3.clone(), Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap();

    let b4 = Block::empty(&b3, &*signer);
    let _ = chain
        .process_block(&None, b4.clone(), Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap();
    let challenger_hash = b4.hash();

    assert_eq!(chain.head_header().unwrap().hash(), new_head);

    chain.mark_block_as_challenged(&new_head, &challenger_hash).unwrap();

    assert_eq!(chain.head_header().unwrap().hash(), challenger_hash);
}

#[test]
fn test_no_challenge_on_same_header() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let prev_hash = chain.head_header().unwrap().hash();
    let prev = chain.get_block(&prev_hash).unwrap();
    let block = Block::empty(&prev, &*signer);
    let tip = chain
        .process_block(&None, block.clone(), Provenance::PRODUCED, |_| {}, |_| {}, |_| {})
        .unwrap();
    assert_eq!(tip.unwrap().height, 1);
    if let Err(e) = chain.process_block_header(&block.header, |_| panic!("Unexpected Challenge")) {
        match e.kind() {
            ErrorKind::Unfit(_) => {}
            _ => panic!("Wrong error kind {}", e),
        }
    } else {
        panic!("Process the same header twice should produce error");
    }
}
