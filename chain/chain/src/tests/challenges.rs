use crate::test_utils::setup;
use crate::{Error, Provenance};
use assert_matches::assert_matches;
use near_o11y::testonly::init_test_logger;
use near_primitives::static_clock::StaticClock;
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::utils::MaybeValidated;

/// Ignore the test because challenges are not enabled yet and the test not compatible with flat storage implementation.
#[test]
#[ignore]
fn challenges_new_head_prev() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup();
    let mut hashes = vec![];
    for i in 0..5 {
        let prev_hash = *chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = TestBlockBuilder::new(&prev, signer.clone()).build();
        hashes.push(*block.hash());
        chain.process_block_test(&None, block).unwrap();
        assert_eq!(chain.head().unwrap().height, i + 1);
    }

    assert_eq!(chain.head().unwrap().height, 5);

    // The block to be added below after we invalidated fourth block.
    let last_block =
        TestBlockBuilder::new(&chain.get_block(&hashes[3]).unwrap(), signer.clone()).build();
    assert_eq!(last_block.header().height(), 5);

    let prev = chain.get_block(&hashes[1]).unwrap();
    let challenger_block = TestBlockBuilder::new(&prev, signer.clone()).height(3).build();
    let challenger_hash = *challenger_block.hash();

    let _ = chain.process_block_test(&None, challenger_block).unwrap();

    // At this point the challenger block is not on canonical chain
    assert_eq!(chain.head_header().unwrap().height(), 5);

    // Challenge fourth block. The third block and the challenger block have the same height, the
    //   current logic will choose the third block.
    chain.mark_block_as_challenged(&hashes[3], &challenger_hash).unwrap();

    assert_eq!(chain.head_header().unwrap().hash(), &hashes[2]);

    assert_eq!(chain.get_block_header_by_height(2).unwrap().hash(), &hashes[1]);
    assert_eq!(chain.get_block_header_by_height(3).unwrap().hash(), &hashes[2]);
    assert!(chain.get_block_header_by_height(4).is_err());

    // Try to add a block on top of the fifth block.
    if let Err(e) = chain.preprocess_block(
        &None,
        &MaybeValidated::from(last_block),
        &Provenance::NONE,
        &mut vec![],
        &mut vec![],
        StaticClock::instant(),
        Default::default(),
    ) {
        assert_matches!(e, Error::ChallengedBlockOnChain)
    } else {
        assert!(false);
    }

    assert_eq!(chain.head_header().unwrap().hash(), &hashes[2]);

    // Add two more blocks
    let b3 = TestBlockBuilder::new(&chain.get_block(&hashes[2]).unwrap(), signer.clone()).build();
    let _ = chain.process_block_test(&None, b3.clone()).unwrap();
    assert_eq!(&chain.head().unwrap().last_block_hash, b3.hash());

    let b4 = TestBlockBuilder::new(&b3, signer.clone()).build();
    chain.process_block_test(&None, b4.clone()).unwrap();
    let new_head = chain.head().unwrap().last_block_hash;

    assert_eq!(&new_head, b4.hash());
    assert_eq!(chain.head_header().unwrap().hash(), &new_head);

    // Add two more blocks on an alternative chain
    let b3 = TestBlockBuilder::new(&chain.get_block(&hashes[2]).unwrap(), signer.clone()).build();
    let _ = chain.process_block_test(&None, b3.clone()).unwrap();

    let b4 = TestBlockBuilder::new(&b3, signer).build();
    let _ = chain.process_block_test(&None, b4.clone()).unwrap();
    let challenger_hash = b4.hash();

    assert_eq!(chain.head_header().unwrap().hash(), &new_head);

    chain.mark_block_as_challenged(&new_head, challenger_hash).unwrap();

    assert_eq!(chain.head_header().unwrap().hash(), challenger_hash);
}

#[test]
fn test_no_challenge_on_same_header() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup();
    let prev_hash = *chain.head_header().unwrap().hash();
    let prev = chain.get_block(&prev_hash).unwrap();
    let block = TestBlockBuilder::new(&prev, signer).build();
    chain.process_block_test(&None, block.clone()).unwrap();
    assert_eq!(chain.head().unwrap().height, 1);
    let mut challenges = vec![];
    if let Err(e) = chain.process_block_header(block.header(), &mut challenges) {
        match e {
            Error::BlockKnown(_) => {}
            _ => panic!("Wrong error kind {}", e),
        }
        assert!(challenges.is_empty());
    } else {
        panic!("Process the same header twice should produce error");
    }
}
