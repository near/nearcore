use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::cloud_archival::{
    gc_and_heads_sanity_checks, pause_and_resume_writer_with_sanity_checks, run_node_until,
    snapshots_sanity_check, test_view_client,
};

const MIN_GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
/// Minimum epoch length assumed in tests.
const MIN_EPOCH_LENGTH: BlockHeightDelta = 10;
/// Minimum number of epochs to wait before GC can trigger.
const MIN_NUM_EPOCHS_TO_WAIT: u64 = MIN_GC_NUM_EPOCHS_TO_KEEP + 1;

/// Parameters controlling the behavior of cloud archival tests.
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
struct TestCloudArchivalParameters {
    /// If true, disables the cloud archival writer for this test.
    disable_writer: bool,
    /// Number of epochs the test should run; must be at least
    /// `MINIMUM_NUM_EPOCHS_TO_WAIT`.
    num_epochs_to_wait: u64,
    /// Whether to run the cold-store loop.
    enable_cold_storage: bool,
    /// Height up to which the cloud archival writer should be paused.
    pause_writer_until_height: Option<BlockHeight>,
    /// If set, runs tests against the `view_client` at the given block height.
    test_view_client_at_height: Option<BlockHeight>,
}

impl TestCloudArchivalParametersBuilder {
    fn build(self) -> TestCloudArchivalParameters {
        let num_epochs_to_wait = self.num_epochs_to_wait.unwrap_or(MIN_NUM_EPOCHS_TO_WAIT);
        assert!(num_epochs_to_wait >= MIN_NUM_EPOCHS_TO_WAIT);
        let disable_writer = self.disable_writer.unwrap_or_default();
        let pause_writer_until_height = self.pause_writer_until_height.unwrap_or_default();
        if disable_writer {
            assert!(pause_writer_until_height.is_none());
        }
        TestCloudArchivalParameters {
            disable_writer,
            enable_cold_storage: self.enable_cold_storage.unwrap_or(false),
            pause_writer_until_height,
            num_epochs_to_wait,
            test_view_client_at_height: self.test_view_client_at_height.unwrap_or(None),
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
        .epoch_length(MIN_EPOCH_LENGTH)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let archival_id: AccountId = "archival".parse().unwrap();
    let all_clients = vec![archival_id.clone(), validator_id];
    let mut cold_storage_archival_clients = HashSet::<AccountId>::new();
    if params.enable_cold_storage {
        cold_storage_archival_clients.insert(archival_id.clone());
    }
    let cloud_storage_archival_clients = [archival_id.clone()].into_iter().collect();
    let archival_index = all_clients.iter().position(|id| id == &archival_id).unwrap();

    let mut builder = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .cold_storage_archival_clients(cold_storage_archival_clients)
        .cloud_storage_archival_clients(cloud_storage_archival_clients)
        .gc_num_epochs_to_keep(MIN_GC_NUM_EPOCHS_TO_KEEP);

    if !params.disable_writer {
        builder = builder.config_modifier(move |config, client_index| {
            if client_index != archival_index {
                return;
            }
            config.cloud_archival_writer = Some(Default::default());
        });
    }

    let mut env = builder.build().warmup();

    if let Some(resume_height) = params.pause_writer_until_height {
        pause_and_resume_writer_with_sanity_checks(
            &mut env,
            resume_height,
            MIN_EPOCH_LENGTH,
            &archival_id,
            params.enable_cold_storage,
        );
    }

    let target_height = params.num_epochs_to_wait * MIN_EPOCH_LENGTH;
    run_node_until(&mut env, &archival_id, target_height);

    println!("Final sanity checks");
    gc_and_heads_sanity_checks(
        &env,
        &archival_id,
        params.enable_cold_storage,
        Some(MIN_EPOCH_LENGTH),
    );

    if let Some(block_height) = params.test_view_client_at_height {
        test_view_client(&mut env, &archival_id, block_height);
    }
    snapshots_sanity_check(&mut env, &archival_id, params.num_epochs_to_wait);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Verifies that `cloud_head` progresses without crashes.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_basic() {
    test_cloud_archival_base(TestCloudArchivalParametersBuilder::default().build());
}

/// Verifies that both `cloud_head` and `cold_head` progress with cold DB enabled.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_with_cold() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default().enable_cold_storage(true).build(),
    );
}

/// Verifies that while the cloud writer is paused, GC stop never exceeds the first block
/// of the epoch containing `cloud_head` and the writer catches up after resuming.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_resume() {
    let gc_period_num_blocks = MIN_GC_NUM_EPOCHS_TO_KEEP * MIN_EPOCH_LENGTH;
    // Pause the cloud writer long enough so that, if it were possible, GC could overtake
    // `cloud_head`. Place `cloud_head` in the middle of the epoch so that the first block
    // of the epoch containing `cloud_head` could potentially be garbage collected.
    let resume_writer_height = Some(2 * gc_period_num_blocks + MIN_EPOCH_LENGTH / 2);
    // After resuming writer, wait one more GC window to expose potential crash.
    let num_epochs_to_wait = 3 * MIN_GC_NUM_EPOCHS_TO_KEEP;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .num_epochs_to_wait(num_epochs_to_wait)
            .pause_writer_until_height(resume_writer_height)
            .build(),
    );
}

/// Verifies that block data can be read from the cloud.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_read_block() {
    let block_height = Some(MIN_EPOCH_LENGTH / 2);
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .test_view_client_at_height(block_height)
            .build(),
    );
}
