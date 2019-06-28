use near_chain::test_utils::setup;
use near_chain::{Block, RuntimeAdapter};
use near_primitives::test_utils::init_test_logger;

#[test]
fn chain_sync_headers() {
    init_test_logger();
    let (mut chain, runtime, signer) = setup();
    let num_shards = runtime.num_shards();
    assert_eq!(chain.sync_head().unwrap().height, 0);
    let mut headers = vec![chain.genesis().clone()];
    for i in 0..4 {
        headers.push(Block::empty(&headers[i], signer.clone(), num_shards).header);
    }
    chain.sync_block_headers(headers.drain(1..).collect()).unwrap();
    assert_eq!(chain.sync_head().unwrap().height, 4);
}
