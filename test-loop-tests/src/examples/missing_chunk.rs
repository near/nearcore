use near_async::time::Duration;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{BlockHeight, NumShards};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

/// This test demonstrates how to trigger missing chunk at a certain height.
/// Requires "test_features" feature to be enabled.
#[test]
fn missing_chunk_example_test() {
    init_test_logger();
    let missing_chunk_heigh = 8;
    let shard_layout = ShardLayout::multi_shard(2, 1);
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let mut validator_runner = env.node_runner(0);
    // Note: waiting for height H results in chunk already produced for H+1.
    // That is why if we want to have missing chunk at H we do the following:
    // 1. Wait for H-2, chunk at H-1 is produced at this point
    // 2. Disable chunk production
    // 3. Wait for H-1, chunk is skipped at H
    // 4. Enable chunk production, chunks are produced again after H
    validator_runner.run_until_head_height(missing_chunk_heigh - 2);
    validator_runner.send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::StopProduce,
    ));
    validator_runner.run_until_head_height(missing_chunk_heigh - 1);
    validator_runner.send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::Valid,
    ));
    validator_runner.run_until_head_height(missing_chunk_heigh + 1);

    let validator_node = env.node(0);
    assert_eq!(get_chunk_mask(&validator_node, missing_chunk_heigh - 1), vec![true, true]);
    assert_eq!(get_chunk_mask(&validator_node, missing_chunk_heigh), vec![false, true]);
    assert_eq!(get_chunk_mask(&validator_node, missing_chunk_heigh + 1), vec![true, true]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn missing_chunk_window_example_test() {
    init_test_logger();
    let num_shards: NumShards = 2;
    let shard_layout = ShardLayout::multi_shard(num_shards, 1);
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let window_size = 5;
    let skip_length = 2;
    let window_start_height = 2 * window_size;
    env.node_runner(0).run_until_head_height(window_start_height - 2);

    let num_nodes = env.node_datas.len();
    for i in 0..num_nodes {
        env.node_runner(i).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::SkipWindow { window_size, skip_length },
        ));
    }

    env.node_runner(0).run_until_head_height(window_start_height + window_size - 1);

    #[derive(Clone)]
    struct ShardMissingChunkState {
        last_missing_height: Option<BlockHeight>,
        missing_count: usize,
    }
    let mut shard_missing_chunk_states =
        vec![
            ShardMissingChunkState { last_missing_height: None, missing_count: 0 };
            num_shards as usize
        ];

    let node = env.node(0);
    for height in window_start_height..window_start_height + window_size {
        let mask = get_chunk_mask(&node, height);
        for (shard_id, has_chunk) in mask.into_iter().enumerate() {
            if !has_chunk {
                let shard_state = &mut shard_missing_chunk_states[shard_id];
                shard_state.missing_count += 1;
                if let Some(last_height) = shard_state.last_missing_height {
                    assert_eq!(
                        height,
                        last_height + 1,
                        "expected to have consecutive missing chunks for shard {shard_id}"
                    )
                }
                shard_state.last_missing_height = Some(height);
            }
        }
    }

    for (shard_id, shard_state) in shard_missing_chunk_states.iter().enumerate() {
        assert_eq!(
            shard_state.missing_count, skip_length as usize,
            "expected number of missing chunks to be equal to skip_length for shard {shard_id}"
        );
    }

    // Note that this is not strictly true: probabilistically we can have the same
    // skipped height sequence for both shards, but the probability is pretty low
    // and we use deterministic rng so this should not cause any flakiness.
    assert_ne!(
        shard_missing_chunk_states[0].last_missing_height,
        shard_missing_chunk_states[1].last_missing_height,
        "expected to have different heights with missing chunk with high probability"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn get_chunk_mask(node: &TestLoopNode<'_>, block_height: BlockHeight) -> Vec<bool> {
    node.client().chain.get_block_by_height(block_height).unwrap().header().chunk_mask().to_vec()
}
