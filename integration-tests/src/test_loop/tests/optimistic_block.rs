use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    build_genesis_and_epoch_config_store, GenesisAndEpochConfigParams, ValidatorsSpec,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};

use crate::test_loop::builder::TestLoopBuilder;

#[test]
fn test_optimistic_block() {
    if !ProtocolFeature::ProduceOptimisticBlock.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let builder = TestLoopBuilder::new();

    let epoch_length = 100;
    let accounts =
        (0..3).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &accounts.iter().map(|account_id| account_id.as_str()).collect_vec(),
        &[],
    );
    let shard_layout = ShardLayout::multi_shard(3, 1);
    let num_shards = shard_layout.num_shards() as usize;

    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
            shard_layout,
            validators_spec,
            accounts: &accounts,
        },
        |genesis_builder| genesis_builder,
        |epoch_config_builder| epoch_config_builder,
    );
    let mut env =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();

    env.test_loop.run_for(Duration::seconds(10));

    {
        let chain =
            &env.test_loop.data.get(&env.datas[0].client_sender.actor_handle()).client.chain;
        // Under normal block processing, there can be only one optimistic
        // block waiting to be processed.
        assert!(chain.optimistic_block_chunks.num_blocks() <= 1);
        // Under normal block processing, number of waiting chunks can't exceed
        // delta between the highest block height and the final block height
        // (normally 3), multiplied by the number of shards.
        assert!(chain.optimistic_block_chunks.num_chunks() <= 3 * num_shards);
        // There should be at least one optimistic block result in the cache.
        assert!(chain.apply_chunk_results_cache.len() > 0);
        // Optimistic block result should be used at least once.
        assert!(chain.apply_chunk_results_cache.hits() > 0);
        // Because there is no optimistic block distribution yet, there should
        // be at least one miss for each shard.
        // TODO: after distribution is implemented, this may change.
        assert!(chain.apply_chunk_results_cache.misses() > 0);
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
