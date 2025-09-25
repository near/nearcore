use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::cloud_archival::{gc_and_heads_sanity_checks, get_cloud_writer, stop_and_restart_node};
use crate::utils::node::TestLoopNode;

const EPOCH_LENGTH: BlockHeightDelta = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
/// The minimum number of epochs to wait so that GC may kick in.
const MINIMUM_NUM_EPOCHS_TO_WAIT: u64 = GC_NUM_EPOCHS_TO_KEEP + 1;

/// Parameters to control the behavior of cloud archival tests
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
struct TestCloudArchivalParameters {
    /// Run the cold store loop.
    enable_split_store: bool,
    /// Delay the start of cloud archival writer node until the given height.
    delay_writer_start: Option<BlockHeight>,
    /// For how many epochs should the test be running. Wait for at least `MINIMUM_NUM_EPOCHS_TO_WAIT`. 
    num_epochs_to_wait: u64,
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
        gc_and_heads_sanity_checks(&mut env, &archival_id, params.enable_split_store, None);
    }

    let archival_node = TestLoopNode::for_account(&env.node_datas, &archival_id);
    let target_height = params.num_epochs_to_wait * EPOCH_LENGTH;
    archival_node.run_until_head_height(&mut env.test_loop, target_height);

    println!("Final sanity checks");
    gc_and_heads_sanity_checks(&mut env, &archival_id, params.enable_split_store, Some(EPOCH_LENGTH));

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// `cloud_head` progresses without crashing.
#[test]
fn test_cloud_archival_basic() {
    test_cloud_archival_base(TestCloudArchivalParametersBuilder::default().build());
}

/// `cloud_head` and `cold_head` progress when split store is enabled.
#[test]
fn test_cloud_archival_with_split_store() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default().enable_split_store(true).build(),
    );
}

/// Verifies that the cloud archival writer and cold store loop progress when both are enabled.
#[test]
fn test_cloud_archival_delayed_start() {
    let gc_period_num_blocks = GC_NUM_EPOCHS_TO_KEEP * EPOCH_LENGTH;
    let start_writer_height = 2 * gc_period_num_blocks;
    let num_epochs_to_wait = 3 * GC_NUM_EPOCHS_TO_KEEP;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .delay_writer_start(Some(start_writer_height))
            .num_epochs_to_wait(num_epochs_to_wait)
            .build(),
    );
}
