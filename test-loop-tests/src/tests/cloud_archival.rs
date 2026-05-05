use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::setup::env::TestLoopEnv;
use crate::utils::account::archival_account_id;
use crate::utils::cloud_archival::{
    WriterConfig, add_writer_node, apply_writer_settings, bootstrap_reader, check_account_balance,
    check_data_at_height_for_shards, gc_and_heads_sanity_checks, get_cloud_head, get_cloud_storage,
    get_writer_handle, run_node_until, simulate_lagging_shard, snapshots_sanity_check,
    stop_and_restart_node, verify_block_range,
};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::MIN_GC_NUM_EPOCHS_TO_KEEP;
use near_client::archive::cloud_archival_reader::find_snapshot_at_or_before;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta, ShardId};
use near_store::ShardUId;
use near_store::archive::cloud_storage::bucket_config::BucketConfig;

/// Test harness for cloud archival tests. Owns the `TestLoopEnv` and exposes
/// composable action and assertion methods so each test reads as an explicit
/// Arrange-Act-Assert sequence.
struct CloudArchiveHarness {
    env: TestLoopEnv,
    /// Account ID of the first cloud archival writer node.
    archival_id: AccountId,
    /// Epoch length in blocks.
    epoch_length: BlockHeightDelta,
    /// Whether cold (split) storage is enabled on the archival node.
    cold_storage_enabled: bool,
    /// Cadence of state snapshots, passed through to assertions.
    snapshot_every_n_epochs: u64,
    /// Account ID of the reader node, set after `bootstrap_reader()`.
    reader_id: Option<AccountId>,
}

struct CloudArchiveHarnessBuilder {
    cold_storage: bool,
    writer: WriterConfig,
}

impl CloudArchiveHarnessBuilder {
    fn cold_storage(mut self, enabled: bool) -> Self {
        self.cold_storage = enabled;
        self
    }

    fn archive_block_data(mut self, enabled: bool) -> Self {
        self.writer.archive_block_data = enabled;
        self
    }

    fn tracked_shards(mut self, shards: Vec<ShardUId>) -> Self {
        self.writer.tracked_shards = shards;
        self
    }

    fn snapshot_every_n_epochs(mut self, cadence: u64) -> Self {
        self.writer.snapshot_every_n_epochs = cadence;
        self
    }

    fn build(self) -> CloudArchiveHarness {
        let user_account: AccountId = CloudArchiveHarness::USER_ACCOUNT.parse().unwrap();
        let archival_kind =
            if self.cold_storage { ArchivalKind::ColdAndCloud } else { ArchivalKind::Cloud };
        let archival_id = self.writer.id.clone();
        let snapshot_every_n_epochs = self.writer.snapshot_every_n_epochs;
        let env = TestLoopBuilder::new()
            .shard_layout(CloudArchiveHarness::default_shard_layout())
            .epoch_length(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH)
            .add_user_account(&user_account, CloudArchiveHarness::USER_BALANCE)
            .enable_archival_node(archival_kind)
            .gc_num_epochs_to_keep(MIN_GC_NUM_EPOCHS_TO_KEEP)
            .bucket_config(BucketConfig::with_batch_size_for_test(
                CloudArchiveHarness::TEST_BATCH_SIZE,
            ))
            .config_modifier(move |config, _client_index| {
                if !config.archive {
                    return;
                }
                apply_writer_settings(
                    config,
                    self.writer.archive_block_data,
                    &self.writer.tracked_shards,
                    snapshot_every_n_epochs,
                );
            })
            .build();
        CloudArchiveHarness {
            env,
            archival_id,
            epoch_length: CloudArchiveHarness::DEFAULT_EPOCH_LENGTH,
            cold_storage_enabled: self.cold_storage,
            snapshot_every_n_epochs,
            reader_id: None,
        }
    }
}

impl CloudArchiveHarness {
    const DEFAULT_EPOCH_LENGTH: BlockHeightDelta = 10;
    const TEST_BATCH_SIZE: u32 = 4;
    const USER_ACCOUNT: &str = "user_account";
    const USER_BALANCE: Balance = Balance::from_near(42);

    fn builder() -> CloudArchiveHarnessBuilder {
        CloudArchiveHarnessBuilder {
            cold_storage: false,
            writer: WriterConfig {
                id: archival_account_id(),
                archive_block_data: true,
                tracked_shards: Self::all_shard_uids(),
                snapshot_every_n_epochs: 1,
            },
        }
    }

    fn run_until(&mut self, height: BlockHeight) {
        run_node_until(&mut self.env, &self.archival_id, height);
    }

    fn run_until_epoch(&mut self, num_epochs: u64) {
        self.run_until(num_epochs * self.epoch_length);
    }

    fn pause_writer(&self) {
        get_writer_handle(&self.env, &self.archival_id).0.stop();
    }

    fn resume_writer(&mut self) {
        get_writer_handle(&self.env, &self.archival_id).0.resume();
        let node_data = self.env.get_node_data_by_account_id(&self.archival_id);
        let node_identifier = node_data.identifier.clone();
        stop_and_restart_node(&mut self.env, node_identifier.as_str());
    }

    fn bootstrap_reader(&mut self, start_height: BlockHeight, target_height: BlockHeight) {
        let reader_id: AccountId = "reader".parse().unwrap();
        bootstrap_reader(&mut self.env, &reader_id, start_height, target_height);
        self.reader_id = Some(reader_id);
    }

    fn kill_reader(&mut self) {
        let reader_id = self.reader_id.take().expect("no reader to kill");
        self.env.kill_node(reader_id.as_ref());
    }

    /// Checks heads alignment and GC tail bounds. Use after a full run when
    /// GC has had time to trigger.
    fn assert_heads_and_gc_ok(&self) {
        gc_and_heads_sanity_checks(
            &self.env,
            &self.archival_id,
            self.cold_storage_enabled,
            Some(self.epoch_length),
        );
    }

    /// Checks heads alignment expecting gc_tail == 1 (no GC has happened yet).
    /// Use during pause scenarios before GC can advance past genesis.
    fn assert_heads_ok_before_gc(&self) {
        gc_and_heads_sanity_checks(&self.env, &self.archival_id, self.cold_storage_enabled, None);
    }

    /// Checks that each epoch (except the final one) has complete snapshots
    /// and epoch data uploaded. Derives `final_epoch_height` from the current
    /// chain head.
    fn assert_snapshots_ok(&self) {
        let head_height = self.env.archival_node().head().height;
        let final_epoch_height = head_height / self.epoch_length;
        snapshots_sanity_check(
            &self.env,
            &self.archival_id,
            final_epoch_height,
            self.snapshot_every_n_epochs,
        );
    }

    fn assert_reader_blocks(&self, start_height: BlockHeight, end_height: BlockHeight) {
        let reader_id = self.reader_id.as_ref().expect("no reader bootstrapped");
        let store = self.env.node_for_account(reader_id).client().chain.chain_store().store();
        verify_block_range(&store, start_height, end_height);
    }

    fn assert_reader_account_balance(&self, account: &AccountId, expected: Balance) {
        let reader_id = self.reader_id.as_ref().expect("no reader bootstrapped");
        check_account_balance(&self.env, reader_id, account, expected);
    }

    fn cloud_head(&self) -> BlockHeight {
        get_cloud_head(&self.env, &self.archival_id)
    }

    fn block_batch_exists_at(&self, block_height: BlockHeight) -> bool {
        get_cloud_storage(&self.env, &self.archival_id)
            .get_block_batch_for_height(block_height)
            .is_ok()
    }

    fn gc_tail(&self) -> BlockHeight {
        self.env.archival_node().client().chain.chain_store().tail()
    }

    fn simulate_lagging_shard(&mut self, shard_id: ShardId, target_height: BlockHeight) {
        simulate_lagging_shard(&mut self.env, &self.archival_id, shard_id, target_height);
    }

    fn add_writer_node(&mut self, config: &WriterConfig) {
        add_writer_node(&mut self.env, config);
    }

    /// For each (height, expected_shards) pair, asserts that exactly those
    /// shards have data at that height (and non-listed shards do NOT).
    fn check_data(&self, checks: &[(BlockHeight, &[ShardId])]) {
        for (height, expected_shards) in checks {
            check_data_at_height_for_shards(
                &self.env,
                &self.archival_id,
                *height,
                expected_shards,
                &Self::all_shard_ids(),
            );
        }
    }

    fn default_shard_layout() -> ShardLayout {
        ShardLayout::multi_shard(3, 3)
    }

    fn all_shard_ids() -> Vec<ShardId> {
        Self::default_shard_layout().shard_ids().collect()
    }

    fn all_shard_uids() -> Vec<ShardUId> {
        let layout = Self::default_shard_layout();
        layout.shard_ids().map(|id| ShardUId::from_shard_id_and_layout(id, &layout)).collect()
    }

    fn shutdown(mut self) {
        self.env.test_loop.run_for(Duration::seconds(5));
    }
}

/// Verifies that `cloud_head` progresses without crashes.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_basic() {
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 2);
    h.assert_heads_and_gc_ok();
    h.assert_snapshots_ok();
    h.shutdown();
}

/// Verifies that both `cloud_head` and `cold_head` progress with cold DB enabled.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_with_cold() {
    let mut h = CloudArchiveHarness::builder().cold_storage(true).build();
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 2);
    h.assert_heads_and_gc_ok();
    h.assert_snapshots_ok();
    h.shutdown();
}

/// Verifies that while the cloud writer is paused, GC stop never exceeds the first block
/// of the epoch containing `cloud_head` and the writer catches up after resuming.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_resume() {
    let mut h = CloudArchiveHarness::builder().build();

    // Cloud head advances within first epoch.
    h.run_until(h.epoch_length);
    let cloud_head = h.cloud_head();
    assert!(2 < cloud_head && cloud_head + 1 < h.epoch_length);

    // Pause writer and advance enough for GC to want to collect cloud_head's
    // epoch. cloud_head is in epoch 1; GC tries to collect it once the chain
    // is MIN_GC_NUM_EPOCHS_TO_KEEP + 1 epochs ahead.
    h.pause_writer();
    let resume_height = (MIN_GC_NUM_EPOCHS_TO_KEEP + 2) * h.epoch_length + h.epoch_length / 2;
    h.run_until(resume_height);
    h.assert_heads_ok_before_gc();

    // Resume and run far enough for GC to collect blocks up to resume_height.
    h.resume_writer();
    h.run_until_epoch(2 * MIN_GC_NUM_EPOCHS_TO_KEEP + 4);
    h.assert_heads_and_gc_ok();
    h.assert_snapshots_ok();
    h.shutdown();
}

/// Verifies that block data can be read from the cloud at multiple heights.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_read_data_at_height() {
    let all_shards = CloudArchiveHarness::all_shard_ids();
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 2);
    h.check_data(&[
        (2, &all_shards),
        (h.epoch_length / 2, &all_shards),
        (h.epoch_length + 1, &all_shards),
    ]);
    h.shutdown();
}

/// cloud_head is always at a batch boundary (last height of an archived batch).
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_batching_cloud_head_at_batch_boundary() {
    assert_eq!(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH, 10);
    assert_eq!(CloudArchiveHarness::TEST_BATCH_SIZE, 4);
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(3);
    // chain head is ~30, so cloud_head is at a batch boundary below it.
    let head = h.cloud_head();
    let batch_size = CloudArchiveHarness::TEST_BATCH_SIZE as u64;
    assert!(head <= 27 && (head + 1) % batch_size == 0, "cloud_head: {head}");
    h.shutdown();
}

/// Every batch up to cloud_head has been uploaded as its own blob, and the
/// batch past cloud_head has not.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_batching_blob_per_batch() {
    assert_eq!(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH, 10);
    assert_eq!(CloudArchiveHarness::TEST_BATCH_SIZE, 4);
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(3);
    let batch_size = CloudArchiveHarness::TEST_BATCH_SIZE as u64;
    let cloud_head = h.cloud_head();
    // Each archived batch has a blob; one batch past cloud_head does not.
    for id in (0..=cloud_head).step_by(batch_size as usize) {
        assert!(h.block_batch_exists_at(id), "batch at {id} should exist");
    }
    assert!(!h.block_batch_exists_at(cloud_head + 1));
    h.shutdown();
}

/// Verifies that a reader node can bootstrap from cloud storage using a
/// state snapshot and per-block state deltas.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_use_snapshot() {
    let mut h = CloudArchiveHarness::builder().build();
    // Run enough epochs for the target height (mid-epoch-2) to be gc-ed
    // locally, so the reader must bootstrap entirely from cloud.
    let epochs = 3 + MIN_GC_NUM_EPOCHS_TO_KEEP;
    h.run_until_epoch(epochs);
    h.assert_heads_and_gc_ok();
    h.assert_snapshots_ok();

    // Bootstrap reader from mid-epoch-1 to mid-epoch-2, spanning an epoch boundary.
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    assert!(h.gc_tail() > target, "target height should be gc-ed");
    h.bootstrap_reader(start, target);
    h.assert_reader_blocks(start, target);
    h.assert_reader_account_balance(
        &CloudArchiveHarness::USER_ACCOUNT.parse().unwrap(),
        CloudArchiveHarness::USER_BALANCE,
    );
    h.kill_reader();

    h.shutdown();
}

/// A writer with `archive_block_data: false` and no tracked shards is misconfigured
/// and must panic during initialization.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[should_panic(expected = "cloud archival writer must track at least one component")]
fn test_cloud_archival_misconfigured_writer_panics() {
    let mut h =
        CloudArchiveHarness::builder().archive_block_data(false).tracked_shards(vec![]).build();
    // One epoch is sufficient — the writer panics during initialization.
    h.run_until_epoch(1);
}

/// Verifies that a writer recovers when one shard's external head lags behind.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_lagging_shard_catchup() {
    let all_shards = CloudArchiveHarness::all_shard_ids();
    let mut h = CloudArchiveHarness::builder().build();
    let lag_at_height = (MIN_GC_NUM_EPOCHS_TO_KEEP + 1) * h.epoch_length;
    let lag_blocks = 5;
    h.run_until(lag_at_height);
    h.simulate_lagging_shard(all_shards[0], lag_at_height - lag_blocks);
    // Run enough for GC to advance past the lagging height.
    h.run_until_epoch(lag_at_height / h.epoch_length + MIN_GC_NUM_EPOCHS_TO_KEEP + 1);
    h.check_data(&[
        (lag_at_height - lag_blocks, &all_shards),
        (lag_at_height - 1, &all_shards),
        (lag_at_height + h.epoch_length, &all_shards),
    ]);
    h.assert_heads_and_gc_ok();
}

/// Verifies that the writer stops when a shard's external head is set back
/// far enough that the data has already been garbage collected.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_lagging_shard_beyond_gc() {
    let mut h = CloudArchiveHarness::builder().build();
    let lag_at_height = (MIN_GC_NUM_EPOCHS_TO_KEEP + 1) * h.epoch_length;
    h.run_until(lag_at_height);
    let cloud_head_before = h.cloud_head();
    // Lag to a height below gc_tail so the writer can't recover.
    let lagged_to = h.epoch_length / 2;
    assert!(lagged_to < h.gc_tail(), "lagged height should be below gc_tail");
    h.simulate_lagging_shard(CloudArchiveHarness::all_shard_ids()[0], lagged_to);
    // Advance testloop — the writer should stop (initialization fails),
    // so the cloud head should not advance.
    h.run_until(lag_at_height + h.epoch_length);
    assert_eq!(
        h.cloud_head(),
        cloud_head_before,
        "cloud head should not advance when writer stops due to lagging shard beyond GC"
    );
}

/// Verifies that a second writer joining mid-test catches up and covers
/// additional shards. Writer_b only archives from its join height onward.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_joins_later() {
    let all_shard_uids = CloudArchiveHarness::all_shard_uids();
    let all_shard_ids = CloudArchiveHarness::all_shard_ids();
    let writer_a_shards = vec![all_shard_ids[0], all_shard_ids[1]];
    let mut h = CloudArchiveHarness::builder()
        .tracked_shards(vec![all_shard_uids[0], all_shard_uids[1]])
        .build();
    let join_height = h.epoch_length * 2;
    h.run_until(join_height);
    // Add writer_b but immediately stop its cloud archival writer so it
    // doesn't archive anything while catching up. This makes the negative
    // check deterministic — writer_b will NOT archive pre-join heights.
    let writer_b_id: AccountId = "writer_b".parse().unwrap();
    h.add_writer_node(&WriterConfig {
        id: writer_b_id.clone(),
        archive_block_data: false,
        tracked_shards: vec![all_shard_uids[1], all_shard_uids[2]],
        snapshot_every_n_epochs: 1,
    });
    get_writer_handle(&h.env, &writer_b_id).0.stop();
    // Let writer_b catch up to join_height (hot store advances) while its
    // cloud archival writer is stopped.
    run_node_until(&mut h.env, &writer_b_id, join_height);
    // Restart writer_b: the hot store is preserved across restart, so the new
    // cloud archival writer initializes at hot_final_height ≈ join_height and
    // archives from there.
    {
        let node_data = h.env.get_node_data_by_account_id(&writer_b_id);
        let node_identifier = node_data.identifier.clone();
        stop_and_restart_node(&mut h.env, &node_identifier);
    }
    // Run enough epochs past join for writer_b to catch up and GC to trigger.
    let target_epochs = join_height / h.epoch_length + MIN_GC_NUM_EPOCHS_TO_KEEP + 2;
    h.run_until_epoch(target_epochs);
    h.check_data(&[
        // Before writer_b joins: only writer_a's shards are archived.
        (h.epoch_length / 2, &writer_a_shards),
        (h.epoch_length, &writer_a_shards),
        // After writer_b catches up: all shards are archived.
        (join_height + 1, &all_shard_ids),
    ]);
    h.assert_heads_and_gc_ok();
}

/// Verifies that two writers tracking all shards both produce valid data.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_multi_writer_same_shards() {
    let all_shard_uids = CloudArchiveHarness::all_shard_uids();
    let all_shard_ids = CloudArchiveHarness::all_shard_ids();
    let mut h = CloudArchiveHarness::builder().build();
    h.add_writer_node(&WriterConfig {
        id: "writer_b".parse().unwrap(),
        archive_block_data: true,
        tracked_shards: all_shard_uids,
        snapshot_every_n_epochs: 1,
    });
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 2);
    h.check_data(&[(2, &all_shard_ids), (h.epoch_length + 1, &all_shard_ids)]);
    h.assert_heads_and_gc_ok();
}

/// Verifies that two writers with disjoint shard assignments together cover
/// all shards.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_multi_writer_disjoint_shards() {
    let all_shard_uids = CloudArchiveHarness::all_shard_uids();
    let all_shard_ids = CloudArchiveHarness::all_shard_ids();
    // Primary writer only tracks shards 0,1.
    let mut h = CloudArchiveHarness::builder()
        .tracked_shards(vec![all_shard_uids[0], all_shard_uids[1]])
        .build();
    h.add_writer_node(&WriterConfig {
        id: "writer_b".parse().unwrap(),
        archive_block_data: false,
        tracked_shards: vec![all_shard_uids[2]],
        snapshot_every_n_epochs: 1,
    });
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 2);
    // Both writers start together: all shards are archived.
    h.check_data(&[(h.epoch_length / 2, &all_shard_ids), (h.epoch_length + 1, &all_shard_ids)]);
    h.assert_heads_and_gc_ok();
}

/// Verifies that a writer with `snapshot_every_n_epochs = 2` only snapshots even epochs
/// and that the discovery helper locates the nearest snapshot at or before a given height.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_custom_snapshot_cadence() {
    let mut h = CloudArchiveHarness::builder().snapshot_every_n_epochs(2).build();
    h.run_until_epoch(6);
    h.assert_snapshots_ok();

    let shard_id = CloudArchiveHarness::all_shard_ids()[0];
    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    // Read epoch ids from cloud so the lookup survives local GC of the block.
    let epoch_id_of =
        |height| *cloud_storage.get_block_data(height).unwrap().block().header().epoch_id();

    // Epoch 4 was snapshotted: first probe hits.
    let hit_at_45 = find_snapshot_at_or_before(&cloud_storage, 45, shard_id).unwrap();
    assert_eq!(hit_at_45, Some((4, epoch_id_of(45))));

    // Epoch 3 was skipped: probe misses, walks back to epoch 2.
    let hit_at_35 = find_snapshot_at_or_before(&cloud_storage, 35, shard_id).unwrap();
    assert_eq!(hit_at_35, Some((2, epoch_id_of(25))));

    h.shutdown();
}
