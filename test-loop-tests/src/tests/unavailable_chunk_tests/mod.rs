#![cfg(feature = "test_features")] // required for adversarial behaviors

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::client_actor::{AdvProduceChunksMode, NetworkAdversarialMessage};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;

/// Test that verifies the chain continues processing when a malicious chunk producer
/// withholds chunk parts strategically to make the chunk unavailable.
/// 
/// This test simulates a scenario where a malicious chunk producer sends only a subset
/// of chunk parts, making the chunk unavailable for full reconstruction, but the chain
/// should continue processing blocks for shards that are not affected.
#[test]
fn test_producer_withholds_chunk_parts() {
    let accounts =
        (0..4).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let chunk_producer = accounts[0].as_str();
    let validators: Vec<_> = accounts[1..].iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &validators);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(ShardLayout::v1_test())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts.clone())
        .build()
        .warmup();
    let TestLoopEnv { test_loop, node_datas, .. } = &mut test_loop_env;

    // First, let's make the chunk producer malicious
    let chunk_producer = &node_datas[0];
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("set malicious chunk production".into(), move |_| {
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::WithholdChunks,
        ));
    });

    // Run the test for a while to see if the chain continues to make progress
    test_loop.run_until(
        |test_loop_data| {
            // Check if all validators except the malicious one continue to make progress
            for node in &node_datas[1..] {
                let c = &test_loop_data.get(&node.client_sender.actor_handle()).client;
                let h = c.chain.head().unwrap().height;
                if h <= 10025 {
                    return false;
                }
            }
            true
        },
        Duration::seconds(30),
    );

    // Verify that the chain made progress despite missing chunks
    for node in &node_datas[1..] {
        let client = &test_loop.data.get(&node.client_sender.actor_handle()).client;
        let head = client.chain.head().unwrap();
        assert!(
            head.height > 10025,
            "Chain should make progress despite missing chunks"
        );
    }

    test_loop_env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
