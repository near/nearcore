use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::{get_node_client, get_node_data, run_until_node_head_height};

/// This test demonstrates how to trigger missing chunk at a certain height.
/// Requires "test_features" feature to be enabled.
#[test]
fn missing_chunk_example_test() {
    let validators = ["validator0", "validator1"];
    let missing_chunk_heigh = 8;
    let shard_layout = ShardLayout::multi_shard(2, 1);
    let clients = validators.map(|acc| acc.parse::<AccountId>().unwrap()).to_vec();
    let target_client = &clients[0];
    let validators_spec = ValidatorsSpec::desired_roles(&validators, &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build()
        .warmup();

    // Note: waiting for height H results in chunk already produced for H+1.
    // That is why if we want to have missing chunk at H we do the following:
    // 1. Wait for H-2, chunk at H-1 is produced at this point
    // 2. Disable chunk production
    // 3. Wait for H-1, chunk is skipped at H
    // 4. Enable chunk production, chunks are produced again after H
    run_until_node_head_height(
        &mut env,
        target_client,
        missing_chunk_heigh - 2,
        Duration::seconds(10),
    );
    send_adv_produce_chunks(&env, target_client, AdvProduceChunksMode::StopProduce);
    run_until_node_head_height(
        &mut env,
        target_client,
        missing_chunk_heigh - 1,
        Duration::seconds(3),
    );
    send_adv_produce_chunks(&env, target_client, AdvProduceChunksMode::Valid);
    run_until_node_head_height(
        &mut env,
        target_client,
        missing_chunk_heigh + 1,
        Duration::seconds(3),
    );

    assert_eq!(get_chunk_mask(&env, target_client, missing_chunk_heigh - 1), vec![true, true]);
    assert_eq!(get_chunk_mask(&env, target_client, missing_chunk_heigh), vec![false, true]);
    assert_eq!(get_chunk_mask(&env, target_client, missing_chunk_heigh + 1), vec![true, true]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn send_adv_produce_chunks(
    env: &TestLoopEnv,
    client_account_id: &AccountId,
    mode: AdvProduceChunksMode,
) {
    let client_sender = get_node_data(&env.node_datas, client_account_id).client_sender.clone();
    env.test_loop.send_adhoc_event(format!("send {mode:?} to {client_account_id}"), move |_| {
        client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(mode));
    });
}

fn get_chunk_mask(
    env: &TestLoopEnv,
    client_account_id: &AccountId,
    block_height: BlockHeight,
) -> Vec<bool> {
    get_node_client(env, client_account_id)
        .chain
        .get_block_by_height(block_height)
        .unwrap()
        .header()
        .chunk_mask()
        .to_vec()
}
