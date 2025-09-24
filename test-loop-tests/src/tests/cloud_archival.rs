use std::collections::HashSet;

use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::types::Tip;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight};
use near_store::{COLD_HEAD_KEY, DBCol};

use crate::setup::builder::TestLoopBuilder;

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
const DEFAULT_NUM_EPOCHS_TO_WAIT: u64 = GC_NUM_EPOCHS_TO_KEEP + 1;

/// Parameters to control the behavior of cloud archival tests
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
struct TestCloudArchivalParameters {
    /// Run the cold store loop.
    enable_split_store: bool,
    #[allow(unused)]
    /// Delay the start of cloud archival node to the given height.
    delay_cloud_archival_start: Option<BlockHeight>,
    /// For how many epochs should the test be running.
    num_epochs_to_wait: u64,
}

impl TestCloudArchivalParametersBuilder {
    fn build(self) -> TestCloudArchivalParameters {
        TestCloudArchivalParameters {
            enable_split_store: self.enable_split_store.unwrap_or(false),
            delay_cloud_archival_start: self.delay_cloud_archival_start.unwrap_or_default(),
            num_epochs_to_wait: self.num_epochs_to_wait.unwrap_or(DEFAULT_NUM_EPOCHS_TO_WAIT),
        }
    }
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
    if params.enable_split_store {
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
    let target_height = params.num_epochs_to_wait * EPOCH_LENGTH;

    env.test_loop.run_until(
        |test_loop_data| {
            let chain = &test_loop_data.get(&client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(target_height as i64),
    );

    let client = env.test_loop.data.get(&client_handle);
    let client_store = client.client.chain.chain_store();

    let cloud_archival_actor_handle = env.node_datas[archival_client_index]
        .cloud_archival_sender
        .as_ref()
        .unwrap()
        .actor_handle();
    let cloud_archival_actor = env.test_loop.data.get(&cloud_archival_actor_handle);

    let gc_tail = client_store.tail().unwrap();
    let cloud_head = cloud_archival_actor.get_cloud_head();

    if params.enable_split_store {
        let cold_head = client_store.store().get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY);
        let cold_head_height = cold_head.unwrap().unwrap().height;
        assert!(cold_head_height > gc_tail);
    }

    // TODO(cloud_archival) With cloud archival paused, the assertion below would fail.
    assert!(cloud_head > gc_tail);
    assert!(gc_tail > EPOCH_LENGTH);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Verifies that the cloud archival writer does not crash and that `cloud_head` progresses.
#[test]
fn test_cloud_archival_basic() {
    test_cloud_archival_base(TestCloudArchivalParametersBuilder::default().build());
}

/// Verifies that the cloud archival writer and cold store loop progress when both are enabled.
#[test]
fn test_cloud_archival_with_split_store() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default().enable_split_store(true).build(),
    );
}
