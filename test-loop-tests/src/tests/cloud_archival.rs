use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::cloud_archival::{
    gc_and_heads_sanity_checks, get_cloud_writer, stop_and_restart_node,
};
use crate::utils::node::TestLoopNode;

const EPOCH_LENGTH: BlockHeightDelta = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
/// Minimum number of epochs to wait before GC can trigger.
const MINIMUM_NUM_EPOCHS_TO_WAIT: u64 = GC_NUM_EPOCHS_TO_KEEP + 1;

/// Parameters controlling the behavior of cloud archival tests.
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
struct TestCloudArchivalParameters {
    /// Number of epochs the test should run; must be at least
    /// `MINIMUM_NUM_EPOCHS_TO_WAIT`.
    num_epochs_to_wait: u64,
    /// Whether to run the cold-store loop.
    enable_split_store: bool,
    /// Height until which to delay the start of the cloud archival writer node.
    delay_writer_start: Option<BlockHeight>,
}

impl TestCloudArchivalParametersBuilder {
    fn build(self) -> TestCloudArchivalParameters {
        let num_epochs_to_wait = self.num_epochs_to_wait.unwrap_or(MINIMUM_NUM_EPOCHS_TO_WAIT);
        assert!(num_epochs_to_wait >= MINIMUM_NUM_EPOCHS_TO_WAIT);
        TestCloudArchivalParameters {
            enable_split_store: self.enable_split_store.unwrap_or(false),
            delay_writer_start: self.delay_writer_start.unwrap_or_default(),
            num_epochs_to_wait,
        }
    }
}

/// Base setup for sanity-checking cloud archival flow.
fn test_cloud_archival_base(params: TestCloudArchivalParameters) {
    init_test_logger();

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let validator_id: AccountId = "cp0".parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator_id.as_str()], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let archival_id: AccountId = "archival".parse().unwrap();
    let all_clients = vec![archival_id.clone(), validator_id];
    let mut split_store_archival_clients = HashSet::<AccountId>::new();
    if params.enable_split_store {
        split_store_archival_clients.insert(archival_id.clone());
    }
    let cloud_archival_writers = [archival_id.clone()].into_iter().collect();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .split_store_archival_clients(split_store_archival_clients)
        .cloud_archival_writers(cloud_archival_writers)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    if let Some(start_height) = params.delay_writer_start {
        get_cloud_writer(&env, &archival_id).get_handle().stop();
        let node_identifier = {
            let archival_node = TestLoopNode::for_account(&env.node_datas, &archival_id);
            archival_node.run_until_head_height(&mut env.test_loop, start_height);
            archival_node.data().identifier.clone()
        };
        get_cloud_writer(&env, &archival_id).get_handle().resume();
        stop_and_restart_node(&mut env, node_identifier.as_str());
        gc_and_heads_sanity_checks(&env, &archival_id, params.enable_split_store, None);
    }

    let archival_node = TestLoopNode::for_account(&env.node_datas, &archival_id);
    let target_height = params.num_epochs_to_wait * EPOCH_LENGTH;
    archival_node.run_until_head_height(&mut env.test_loop, target_height);

    println!("Final sanity checks");
    gc_and_heads_sanity_checks(&env, &archival_id, params.enable_split_store, Some(EPOCH_LENGTH));

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Verifies that `cloud_head` progresses without crashes.
#[test]
fn test_cloud_archival_basic() {
    test_cloud_archival_base(TestCloudArchivalParametersBuilder::default().build());
}

/// Verifies that both `cloud_head` and `cold_head` progress with split store enabled.
#[test]
fn test_cloud_archival_with_split_store() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default().enable_split_store(true).build(),
    );
}

/// Verifies that while the cloud writer is paused, GC stop never exceeds `cloud_head`;
/// after resuming, the writer catches up, and the cold-store loop keeps progressing
/// throughout.
#[test]
// TODO(cloud_archival): Enable once cloud head is persisted to external storage.
#[cfg(ignore)]
fn test_cloud_archival_delayed_start() {
    let gc_period_num_blocks = GC_NUM_EPOCHS_TO_KEEP * EPOCH_LENGTH;
    // Pause the cloud writer long enough that, if it were possible, GC could overtake
    // `cloud_head`.
    let start_writer_height = 2 * gc_period_num_blocks;
    // After resuming writer, wait one more GC window to expose potential crash.
    let num_epochs_to_wait = 3 * GC_NUM_EPOCHS_TO_KEEP;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .num_epochs_to_wait(num_epochs_to_wait)
            .enable_split_store(true)
            .delay_writer_start(Some(start_writer_height))
            .build(),
    );
}
