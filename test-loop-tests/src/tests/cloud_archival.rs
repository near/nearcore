use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_chain_configs::test_utils::test_cloud_archival_configs;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;

use crate::setup::builder::TestLoopBuilder;

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

#[test]
fn test_cloud_archival_base() {
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
    let archival_clients: HashSet<AccountId> = [archival_client].into_iter().collect();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .archival_clients(archival_clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .config_modifier(move |config, client_index| {
            if client_index != archival_client_index {
                return;
            }
            let (_, writer_config) = test_cloud_archival_configs("");
            config.cloud_archival_writer = Some(writer_config);
        })
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
    assert!(cloud_head > gc_tail);
    // TODO(cloud_archival) Test GC if cloud archival is enabled but cold store is not
    assert!(gc_tail > EPOCH_LENGTH);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn test_cloud_archival() {
    test_cloud_archival_base();
}
