use crate::near_chain_primitives::error::BlockKnownError;
use crate::test_utils::{setup, wait_for_all_blocks_in_processing};
use crate::{Block, BlockProcessingArtifact, ChainStoreAccess, Error};
use assert_matches::assert_matches;
use near_async::time::{Clock, Duration, FakeClock, Utc};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::version::PROTOCOL_VERSION;
use num_rational::Ratio;

#[test]
fn build_chain() {
    init_test_logger();
    let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap()); // 2020-10-01 00:00:00
    clock.advance(Duration::milliseconds(3444));
    let (mut chain, _, _, signer) = setup(clock.clock());
    assert_eq!(chain.head().unwrap().height, 0);

    // The hashes here will have to be modified after changes to the protocol.
    // In particular if you update protocol version or add new protocol
    // features.  If this assert is failing without you adding any new or
    // stabilising any existing protocol features, this indicates bug in your
    // code which unexpectedly changes the protocol.
    //
    // To update the hashes you can use cargo-insta.  Note that you’ll need to
    // run the test twice: once with default features and once with
    // ‘nightly’ feature enabled:
    //
    //     cargo install cargo-insta
    //     cargo insta test --accept -p near-chain                    -- tests::simple_chain::build_chain
    //     cargo insta test --accept -p near-chain --features nightly -- tests::simple_chain::build_chain
    let hash = chain.head().unwrap().last_block_hash;
    if cfg!(feature = "nightly") {
        insta::assert_snapshot!(hash, @"Hc3bWEd7ikHf9BAe2SknvH2jAAakEtBRU1FBu6Udocm3");
    } else {
        insta::assert_snapshot!(hash, @"GpzRk39JJJZkXiPaXpe7tv1KkyeWYBPYEakZFP6hznGh");
    }

    for i in 1..5 {
        clock.advance(Duration::milliseconds(1));
        let prev_hash = *chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = TestBlockBuilder::new(clock.clock(), &prev, signer.clone()).build();
        chain.process_block_test(&None, block).unwrap();
        assert_eq!(chain.head().unwrap().height, i as u64);
    }

    assert_eq!(chain.head().unwrap().height, 4);

    let hash = chain.head().unwrap().last_block_hash;
    if cfg!(feature = "nightly") {
        insta::assert_snapshot!(hash, @"9Sf2SDepZ5GLjhbtt8iRDEnYAUkA4tMCf4r4pPcqgwo3");
    } else {
        insta::assert_snapshot!(hash, @"ANjdWVaMLmMKmXMLfkzVbWSWYri6Q5Mvc1qRogUTZQAK");
    }
}

#[test]
fn build_chain_with_orphans() {
    init_test_logger();
    let clock = Clock::real();
    let (mut chain, _, _, signer) = setup(clock.clone());
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    for i in 1..4 {
        let block = TestBlockBuilder::new(clock.clone(), &blocks[i - 1], signer.clone()).build();
        blocks.push(block);
    }
    let last_block = &blocks[blocks.len() - 1];
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        last_block.header(),
        10,
        last_block.header().block_ordinal() + 1,
        last_block.chunks().iter().cloned().collect(),
        vec![vec![]; last_block.chunks().len()],
        *last_block.header().epoch_id(),
        *last_block.header().next_epoch_id(),
        None,
        vec![],
        Ratio::from_integer(0),
        0,
        100,
        Some(0),
        vec![],
        vec![],
        &*signer,
        *last_block.header().next_bp_hash(),
        CryptoHash::default(),
        clock,
        None,
    );
    assert_matches!(chain.process_block_test(&None, block).unwrap_err(), Error::Orphan);
    assert_matches!(
        chain.process_block_test(&None, blocks.pop().unwrap()).unwrap_err(),
        Error::Orphan
    );
    assert_matches!(
        chain.process_block_test(&None, blocks.pop().unwrap()).unwrap_err(),
        Error::Orphan
    );
    chain.process_block_test(&None, blocks.pop().unwrap()).unwrap();
    while wait_for_all_blocks_in_processing(&mut chain) {
        chain.postprocess_ready_blocks(&None, &mut BlockProcessingArtifact::default(), None);
    }
    assert_eq!(chain.head().unwrap().height, 10);
    assert_matches!(
        chain.process_block_test(&None, blocks.pop().unwrap(),).unwrap_err(),
        Error::BlockKnown(BlockKnownError::KnownInStore)
    );
}

/// Checks that chain successfully processes blocks with skipped blocks and forks, but doesn't process block behind
/// final head.
#[test]
fn build_chain_with_skips_and_forks() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    let b2 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(2).build();
    let b3 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(3).build();
    let b4 = TestBlockBuilder::new(Clock::real(), &b2, signer.clone()).height(4).build();
    let b5 = TestBlockBuilder::new(Clock::real(), &b4, signer.clone()).build();
    let b6 = TestBlockBuilder::new(Clock::real(), &b5, signer.clone()).build();
    assert!(chain.process_block_test(&None, b1).is_ok());
    assert!(chain.process_block_test(&None, b2).is_ok());
    assert!(chain.process_block_test(&None, b3.clone()).is_ok());
    assert!(chain.process_block_test(&None, b4).is_ok());
    assert!(chain.process_block_test(&None, b5).is_ok());
    assert!(chain.process_block_test(&None, b6).is_ok());
    assert!(chain.get_block_header_by_height(1).is_err());
    assert_eq!(chain.get_block_header_by_height(5).unwrap().height(), 5);
    assert_eq!(chain.get_block_header_by_height(6).unwrap().height(), 6);

    let c4 = TestBlockBuilder::new(Clock::real(), &b3, signer).height(4).build();
    assert_eq!(chain.final_head().unwrap().height, 4);
    assert_matches!(chain.process_block_test(&None, c4), Err(Error::CannotBeFinalized));
}

/// Verifies that getting block by its height are updated correctly when blocks from different forks are
/// processed, especially when certain heights are skipped.
/// Chain looks as follows (variable name + height):
///
/// 0 -> b1 (c1) -> b2
///        |  \      \
///        |   \      -> d3 -------> d5 -> d6
///        |    \
///        |     ------> c3 -> c4
///        |
///        ------------------------------------> e7
///
/// Note that only block b1 is finalized, so all blocks here can be processed. But getting block by height should
/// return only blocks from the canonical chain.
#[test]
fn blocks_at_height() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block_by_height(0).unwrap();
    let b_1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(1).build();

    let b_2 = TestBlockBuilder::new(Clock::real(), &b_1, signer.clone()).height(2).build();

    let c_1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(1).build();
    let c_3 = TestBlockBuilder::new(Clock::real(), &c_1, signer.clone()).height(3).build();
    let c_4 = TestBlockBuilder::new(Clock::real(), &c_3, signer.clone()).height(4).build();

    let d_3 = TestBlockBuilder::new(Clock::real(), &b_2, signer.clone()).height(3).build();

    let d_5 = TestBlockBuilder::new(Clock::real(), &d_3, signer.clone()).height(5).build();
    let d_6 = TestBlockBuilder::new(Clock::real(), &d_5, signer.clone()).height(6).build();

    let e_7 = TestBlockBuilder::new(Clock::real(), &b_1, signer).height(7).build();

    let b_1_hash = *b_1.hash();
    let b_2_hash = *b_2.hash();

    let c_1_hash = *c_1.hash();
    let c_3_hash = *c_3.hash();
    let c_4_hash = *c_4.hash();

    let d_3_hash = *d_3.hash();
    let d_5_hash = *d_5.hash();
    let d_6_hash = *d_6.hash();

    let e_7_hash = *e_7.hash();

    assert_ne!(c_3_hash, d_3_hash);

    chain.process_block_test(&None, b_1).unwrap();
    chain.process_block_test(&None, b_2).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 2);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    assert_eq!(chain.get_block_header_by_height(2).unwrap().hash(), &b_2_hash);

    chain.process_block_test(&None, c_1).unwrap();
    chain.process_block_test(&None, c_3).unwrap();
    chain.process_block_test(&None, c_4).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 4);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &c_1_hash);
    assert!(chain.get_block_header_by_height(2).is_err());
    assert_eq!(chain.get_block_header_by_height(3).unwrap().hash(), &c_3_hash);
    assert_eq!(chain.get_block_header_by_height(4).unwrap().hash(), &c_4_hash);

    chain.process_block_test(&None, d_3).unwrap();
    chain.process_block_test(&None, d_5).unwrap();
    chain.process_block_test(&None, d_6).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 6);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    assert_eq!(chain.get_block_header_by_height(2).unwrap().hash(), &b_2_hash);
    assert_eq!(chain.get_block_header_by_height(3).unwrap().hash(), &d_3_hash);
    assert!(chain.get_block_header_by_height(4).is_err());
    assert_eq!(chain.get_block_header_by_height(5).unwrap().hash(), &d_5_hash);
    assert_eq!(chain.get_block_header_by_height(6).unwrap().hash(), &d_6_hash);

    chain.process_block_test(&None, e_7).unwrap();

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    for h in 2..=5 {
        assert!(chain.get_block_header_by_height(h).is_err());
    }
    assert_eq!(chain.get_block_header_by_height(7).unwrap().hash(), &e_7_hash);
}

#[test]
fn next_blocks() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    let b2 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(2).build();
    let b3 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(3).build();
    let b4 = TestBlockBuilder::new(Clock::real(), &b3, signer).height(4).build();
    let b1_hash = *b1.hash();
    let b2_hash = *b2.hash();
    let b3_hash = *b3.hash();
    let b4_hash = *b4.hash();
    assert!(chain.process_block_test(&None, b1).is_ok());
    assert!(chain.process_block_test(&None, b2).is_ok());
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b1_hash).unwrap(), b2_hash);
    assert!(chain.process_block_test(&None, b3).is_ok());
    assert!(chain.process_block_test(&None, b4).is_ok());
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b1_hash).unwrap(), b3_hash);
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b3_hash).unwrap(), b4_hash);
}
