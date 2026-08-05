use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::utils::get_block_shard_id_rev;
use near_store::DBCol;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chunk_certifying_block_index_is_collected_with_its_block() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(1)
        .build();

    env.node_runner(0).run_until(|node| node.tail() >= 2 * epoch_length, Duration::seconds(30));

    let node = env.node(0);
    let chain_store = &node.client().chain.chain_store;
    let tail = node.tail();
    let mut indexed_chunks = 0;
    for (key, _) in node.store().iter(DBCol::chunk_certifying_block()) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&key).unwrap();
        let block = chain_store.get_block(&block_hash).unwrap_or_else(|_| {
            panic!(
                "chunk ({block_hash}, {shard_id}) is indexed but its block is collected, tail {tail}"
            )
        });
        assert!(block.header().height() >= tail);
        indexed_chunks += 1;
    }
    assert!(indexed_chunks > 0, "expected the index to hold chunks of retained blocks");
}
