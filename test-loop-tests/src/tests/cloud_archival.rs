use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;

use crate::setup::builder::TestLoopBuilder;

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

struct TestCloudArchivalParameters {
    with_cold_store: bool,
}

fn test_cloud_archival_base(params: TestCloudArchivalParameters) {
    init_test_logger();

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let validator = "cp0";
    let validator_client: AccountId = validator.parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let archival_client: AccountId = "archival".parse().unwrap();
    let all_clients = vec![archival_client.clone(), validator_client];
    let archival_client_index = 0;
    assert_eq!(all_clients[archival_client_index], archival_client);
    let mut split_store_archival_clients = HashSet::<AccountId>::new();
    if params.with_cold_store {
        split_store_archival_clients.insert(archival_client.clone());
    }
    let cloud_archival_writers = [archival_client].into_iter().collect();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .split_store_archival_clients(split_store_archival_clients)
        .cloud_archival_writers(cloud_archival_writers)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    // Run the chain until it garbage collects blocks from the first epoch.
    let client_handle = env.node_datas[archival_client_index].client_sender.actor_handle();
    let target_height = EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 1);
    env.test_loop.run_until(
        |test_loop_data| {
            let chain = &test_loop_data.get(&client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(target_height as i64),
    );
    let client = env.test_loop.data.get(&client_handle);
    let cloud_archival_actor_handle = env.node_datas[archival_client_index]
        .cloud_archival_sender
        .as_ref()
        .unwrap()
        .actor_handle();
    let cloud_archival_actor = env.test_loop.data.get(&cloud_archival_actor_handle);

    let gc_tail = client.client.chain.chain_store().tail().unwrap();
    let cloud_head = cloud_archival_actor.get_cloud_head();
    // TODO(cloud_archival) With cloud archival paused, the assertion below would fail.
    assert!(cloud_head > gc_tail);
    assert!(gc_tail > EPOCH_LENGTH);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Verifies that the cloud archival writer does not crash and that `cloud_head` progresses.
#[test]
fn test_cloud_archival_basic() {
    let params = TestCloudArchivalParameters { with_cold_store: false };
    test_cloud_archival_base(params);
}

/// Verifies that the cloud archival writer does not crash and that `cloud_head` progresses if cold store is
/// enabled.
#[test]
fn test_cloud_archival_with_cold_store() {
    let params = TestCloudArchivalParameters { with_cold_store: true };
    test_cloud_archival_base(params);
}
