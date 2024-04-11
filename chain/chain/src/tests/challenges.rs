use crate::test_utils::setup;
use crate::Error;
use near_async::time::Clock;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::TestBlockBuilder;

#[test]
fn test_no_challenge_on_same_header() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let prev_hash = *chain.head_header().unwrap().hash();
    let prev = chain.get_block(&prev_hash).unwrap();
    let block = TestBlockBuilder::new(Clock::real(), &prev, signer).build();
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
