use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients_with_rpc};
use near_async::time::Duration;
use near_primitives::utils::get_block_shard_id_rev;
use near_store::DBCol;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chunk_certifying_block_index_is_collected_with_its_block() {
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let epoch_length = 5;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .gc_num_epochs_to_keep(1)
        .epoch_config_store_from_genesis()
        .clients(clients)
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
