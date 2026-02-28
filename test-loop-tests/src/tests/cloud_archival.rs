use std::collections::HashSet;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta, ShardId};
use near_store::ShardUId;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::cloud_archival::{
    WriterConfig, add_writer_node, apply_writer_settings, bootstrap_reader_at_height,
    check_account_balance, check_data_at_height, gc_and_heads_sanity_checks,
    pause_and_resume_writer_with_sanity_checks, run_node_until, simulate_lagging_shard,
    snapshots_sanity_check,
};

const MIN_GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
/// Minimum epoch length assumed in tests.
const MIN_EPOCH_LENGTH: BlockHeightDelta = 10;
/// Minimum number of epochs to wait before GC can trigger.
const MIN_NUM_EPOCHS_TO_WAIT: u64 = MIN_GC_NUM_EPOCHS_TO_KEEP + 1;

const TEST_USER_ACCOUNT: &str = "user_account";
const TEST_USER_BALANCE: Balance = Balance::from_near(42);

fn test_shard_layout() -> ShardLayout {
    ShardLayout::multi_shard(3, 3)
}

fn all_test_shard_uids() -> Vec<ShardUId> {
    test_shard_layout().shard_uids().collect()
}

fn all_test_shard_ids() -> Vec<ShardId> {
    test_shard_layout().shard_ids().collect()
}

/// Action to execute during the test.
enum TestAction {
    /// Pause the writer, run for the given number of blocks, then resume.
    PauseAndResume { pause_duration_blocks: BlockHeightDelta },
    /// Kill writer, set back one shard's external head, restart.
    LaggingShardCatchup { shard_id: ShardId, lag_blocks: BlockHeight },
    /// Add a new writer node mid-test.
    AddWriter { after_epochs: u64, config: WriterConfig },
    /// Add a reader node after the main run and bootstrap from cloud storage.
    BootstrapReader { at_height: BlockHeight },
}

/// Parameters controlling the behavior of cloud archival tests.
#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip))]
struct TestCloudArchivalParameters {
    /// Writer nodes to create at test start. Default: single writer with all shards.
    writers: Vec<WriterConfig>,
    /// Number of epochs the test should run; must be at least `MIN_NUM_EPOCHS_TO_WAIT`.
    num_epochs_to_wait: u64,
    /// Whether to run the cold-store loop.
    enable_cold_storage: bool,
    /// Optional action to execute during the test.
    test_action: Option<TestAction>,
    /// (height, expected_shards) pairs — verifies that block data and exactly
    /// the listed shards have data archived at each height.
    check_data_at_heights: Vec<(BlockHeight, Vec<ShardId>)>,
}

impl TestCloudArchivalParametersBuilder {
    fn build(self) -> TestCloudArchivalParameters {
        let num_epochs_to_wait = self.num_epochs_to_wait.unwrap_or(MIN_NUM_EPOCHS_TO_WAIT);
        assert!(num_epochs_to_wait >= MIN_NUM_EPOCHS_TO_WAIT);

        let writers = match self.writers {
            Some(writers) => {
                assert!(!writers.is_empty(), "writers list must not be empty");
                writers
            }
            None => vec![WriterConfig {
                id: "archival".parse().unwrap(),
                archive_block_data: true,
                tracked_shards: all_test_shard_uids(),
            }],
        };

        TestCloudArchivalParameters {
            writers,
            num_epochs_to_wait,
            enable_cold_storage: self.enable_cold_storage.unwrap_or(false),
            test_action: self.test_action.flatten(),
            check_data_at_heights: self.check_data_at_heights.unwrap_or_default(),
        }
    }
}

/// Base setup for sanity-checking cloud archival flow.
fn test_cloud_archival_base(params: TestCloudArchivalParameters) {
    init_test_logger();

    let shard_layout = test_shard_layout();
    let user_account: AccountId = TEST_USER_ACCOUNT.parse().unwrap();
    let validator_id: AccountId = "cp0".parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator_id.as_str()], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(MIN_EPOCH_LENGTH)
        .add_user_account_simple(user_account.clone(), TEST_USER_BALANCE)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    // Writers first (order must match writer_configs for config_modifier indexing),
    // validator last.
    let mut all_clients: Vec<AccountId> = params.writers.iter().map(|w| w.id.clone()).collect();
    let validator_index = all_clients.len();
    all_clients.push(validator_id.clone());
    assert_eq!(all_clients[validator_index], validator_id, "validator must be at validator_index");
    let cloud_archival_clients: HashSet<AccountId> =
        params.writers.iter().map(|w| w.id.clone()).collect();

    let cold_storage_archival_clients =
        if params.enable_cold_storage { cloud_archival_clients.clone() } else { HashSet::new() };

    // Capture writer configs for the config_modifier closure.
    let writer_configs: Vec<(bool, Vec<ShardUId>)> =
        params.writers.iter().map(|w| (w.archive_block_data, w.tracked_shards.clone())).collect();
    let builder = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .cold_storage_archival_clients(cold_storage_archival_clients)
        .cloud_storage_archival_clients(cloud_archival_clients)
        .gc_num_epochs_to_keep(MIN_GC_NUM_EPOCHS_TO_KEEP)
        .config_modifier(move |config, client_index| {
            if let Some((archive_block_data, tracked_shards)) = writer_configs.get(client_index) {
                apply_writer_settings(config, *archive_block_data, tracked_shards);
            }
            // Keep all blocks on the validator so new nodes can always block-sync.
            if client_index == validator_index {
                config.gc.gc_num_epochs_to_keep = 1000;
            }
        });

    let mut env = builder.build().warmup();

    let first_writer_id = params.writers[0].id.clone();

    let mut added_writer: Option<WriterConfig> = None;
    let mut reader_at_height: Option<BlockHeight> = None;
    if let Some(action) = params.test_action {
        match action {
            TestAction::PauseAndResume { pause_duration_blocks } => {
                pause_and_resume_writer_with_sanity_checks(
                    &mut env,
                    pause_duration_blocks,
                    MIN_EPOCH_LENGTH,
                    &params.writers[0],
                    params.enable_cold_storage,
                );
            }
            TestAction::LaggingShardCatchup { shard_id, lag_blocks } => {
                // Run for MIN_NUM_EPOCHS_TO_WAIT epochs before simulating the lag.
                // This ensures cloud archival has progressed far enough that GC is
                // active, so the lagging shard catchup exercises the real recovery
                // path (re-archiving from data that is about to be garbage-collected).
                let first_phase_height = MIN_NUM_EPOCHS_TO_WAIT * MIN_EPOCH_LENGTH;
                run_node_until(&mut env, &first_writer_id, first_phase_height);
                simulate_lagging_shard(&mut env, &first_writer_id, shard_id, lag_blocks);
            }
            TestAction::AddWriter { after_epochs, config } => {
                let add_at_height = after_epochs * MIN_EPOCH_LENGTH;
                run_node_until(&mut env, &first_writer_id, add_at_height);
                add_writer_node(&mut env, &config);
                added_writer = Some(config);
            }
            TestAction::BootstrapReader { at_height } => {
                reader_at_height = Some(at_height);
            }
        }
    }

    // Run to target height.
    let target_height = params.num_epochs_to_wait * MIN_EPOCH_LENGTH;
    run_node_until(&mut env, &first_writer_id, target_height);

    // Final sanity checks for all writers.
    println!("Final sanity checks");
    for writer in &params.writers {
        gc_and_heads_sanity_checks(
            &env,
            writer,
            params.enable_cold_storage,
            Some(MIN_EPOCH_LENGTH),
        );
    }
    if let Some(writer) = &added_writer {
        gc_and_heads_sanity_checks(&env, writer, false, Some(MIN_EPOCH_LENGTH));
    }

    for (height, expected_shards) in &params.check_data_at_heights {
        check_data_at_height(&env, &first_writer_id, *height, expected_shards);
    }
    snapshots_sanity_check(
        &env,
        &first_writer_id,
        params.num_epochs_to_wait,
        &params.writers[0].tracked_shards,
    );

    if let Some(target_block_height) = reader_at_height {
        let reader_id: AccountId = "reader".parse().unwrap();
        bootstrap_reader_at_height(&mut env, &reader_id, target_block_height);
        check_account_balance(&env, &reader_id, &user_account, TEST_USER_BALANCE);
        // Kill the reader node immediately after bootstrapping. We only want to
        // verify that state sync + delta application produces the correct state.
        env.kill_node(reader_id.as_ref());
    }

    env.test_loop.run_for(Duration::seconds(5));
    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Verifies that `cloud_head` progresses without crashes.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_basic() {
    test_cloud_archival_base(TestCloudArchivalParametersBuilder::default().build());
}

/// Verifies that a writer configured with no block archiving and no tracked shards
/// panics during initialization. The panic originates from the assertion in
/// `create_cloud_archival_writer`, which requires at least one tracked component.
#[test]
#[should_panic(expected = "cloud archival writer must track at least one component")]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_misconfigured_writer_panics() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .writers(vec![WriterConfig {
                id: "archival".parse().unwrap(),
                archive_block_data: false,
                tracked_shards: vec![],
            }])
            .build(),
    );
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
    let pause_duration_blocks = 2 * gc_period_num_blocks + MIN_EPOCH_LENGTH / 2;
    // After resuming writer, wait one more GC window to expose potential crash.
    let num_epochs_to_wait = 3 * MIN_GC_NUM_EPOCHS_TO_KEEP;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .num_epochs_to_wait(num_epochs_to_wait)
            .test_action(Some(TestAction::PauseAndResume { pause_duration_blocks }))
            .build(),
    );
}

/// Verifies that block data can be read from the cloud.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_read_data_at_height() {
    let all_shards = all_test_shard_ids();
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .check_data_at_heights(vec![
                (2, all_shards.clone()),
                (MIN_EPOCH_LENGTH / 2, all_shards.clone()),
                (MIN_EPOCH_LENGTH + 1, all_shards),
            ])
            .build(),
    );
}

/// Verifies that a reader node can bootstrap its state from cloud storage by
/// downloading a state snapshot and then applying per-block state deltas.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_use_snapshot() {
    let epochs_num = 3 + MIN_GC_NUM_EPOCHS_TO_KEEP;
    let at_height = MIN_EPOCH_LENGTH + MIN_EPOCH_LENGTH / 2;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .num_epochs_to_wait(epochs_num)
            .test_action(Some(TestAction::BootstrapReader { at_height }))
            .build(),
    );
}

/// Verifies that a writer recovers when one shard's external head lags behind.
/// The writer re-archives the lagging shard's data idempotently on restart.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_lagging_shard_catchup() {
    let shard_ids = all_test_shard_ids();
    let all_shards = shard_ids.clone();
    // Height at the end of the first phase (before simulating the lag). Same
    // calculation as in `test_cloud_archival_base` for `LaggingShardCatchup`.
    let first_phase_height = MIN_NUM_EPOCHS_TO_WAIT * MIN_EPOCH_LENGTH;
    let lag_blocks = 5;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .num_epochs_to_wait(2 * MIN_NUM_EPOCHS_TO_WAIT)
            .test_action(Some(TestAction::LaggingShardCatchup {
                shard_id: shard_ids[2],
                lag_blocks,
            }))
            .check_data_at_heights(vec![
                (first_phase_height - lag_blocks, all_shards.clone()),
                (first_phase_height - 1, all_shards.clone()),
                (first_phase_height + MIN_EPOCH_LENGTH, all_shards),
            ])
            .build(),
    );
}

/// Verifies that a second writer can join mid-operation.
///
/// Setup: writer_a tracks shards {0,1} from the start; writer_b joins at
/// epoch 2 with shards {1,2}. Shard 1 overlaps to exercise idempotent writes
/// to cloud storage.
///
/// On join, writer_b reads existing external heads (from writer_a) and
/// initializes its missing components (shard 2) at `hot_final_height`.
/// Writer_b only archives shard 2 from its join height onward — no backfill.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_joins_later() {
    let shard_uids = all_test_shard_uids();
    let shard_ids = all_test_shard_ids();
    let writer_a_shards = vec![shard_ids[0], shard_ids[1]];
    let join_epoch = 2;
    let join_height = join_epoch * MIN_EPOCH_LENGTH;
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .writers(vec![WriterConfig {
                id: "writer_a".parse().unwrap(),
                archive_block_data: true,
                tracked_shards: vec![shard_uids[0], shard_uids[1]],
            }])
            .num_epochs_to_wait(2 * MIN_NUM_EPOCHS_TO_WAIT)
            .test_action(Some(TestAction::AddWriter {
                after_epochs: join_epoch,
                config: WriterConfig {
                    id: "writer_b".parse().unwrap(),
                    archive_block_data: false,
                    tracked_shards: vec![shard_uids[1], shard_uids[2]],
                },
            }))
            .check_data_at_heights(vec![
                // Before writer_b joins: only writer_a's shards are archived.
                (MIN_EPOCH_LENGTH / 2, writer_a_shards.clone()),
                (MIN_EPOCH_LENGTH, writer_a_shards),
                // After writer_b catches up: all shards are archived.
                (join_height + MIN_EPOCH_LENGTH + 1, all_test_shard_ids()),
            ])
            .build(),
    );
}

/// Verifies that two writers archiving the same components do not conflict.
///
/// Both writers track all shards and archive block data. In production, writers
/// may run concurrently on different machines. Data uploads are idempotent:
/// both writers produce the same deterministic block/shard data for a given
/// height, so concurrent uploads are harmless.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_multi_writer_same_shards() {
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .writers(vec![
                WriterConfig {
                    id: "writer_a".parse().unwrap(),
                    archive_block_data: true,
                    tracked_shards: all_test_shard_uids(),
                },
                WriterConfig {
                    id: "writer_b".parse().unwrap(),
                    archive_block_data: true,
                    tracked_shards: all_test_shard_uids(),
                },
            ])
            .check_data_at_heights(vec![
                (2, all_test_shard_ids()),
                (MIN_EPOCH_LENGTH + 1, all_test_shard_ids()),
            ])
            .build(),
    );
}

/// Verifies that two writers with disjoint shard assignments produce
/// complete coverage: block data from writer A, shard data split across both.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_multi_writer_disjoint_shards() {
    let shard_uids = all_test_shard_uids();
    test_cloud_archival_base(
        TestCloudArchivalParametersBuilder::default()
            .writers(vec![
                WriterConfig {
                    id: "writer_a".parse().unwrap(),
                    archive_block_data: true,
                    tracked_shards: vec![shard_uids[0], shard_uids[1]],
                },
                WriterConfig {
                    id: "writer_b".parse().unwrap(),
                    archive_block_data: false,
                    tracked_shards: vec![shard_uids[2]],
                },
            ])
            .check_data_at_heights(vec![
                (2, all_test_shard_ids()),
                (MIN_EPOCH_LENGTH + 1, all_test_shard_ids()),
            ])
            .build(),
    );
}
