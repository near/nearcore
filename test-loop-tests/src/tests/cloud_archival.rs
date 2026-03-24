use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::setup::env::TestLoopEnv;
use crate::utils::account::archival_account_id;
use crate::utils::cloud_archival::{
    bootstrap_reader_at_height, check_account_balance, check_data_at_height,
    gc_and_heads_sanity_checks, get_cloud_head, get_writer_handle, run_node_until,
    snapshots_sanity_check, stop_and_restart_node,
};
use near_async::time::Duration;
use near_chain_configs::{CloudArchivalWriterConfig, MIN_GC_NUM_EPOCHS_TO_KEEP};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta};

/// Test harness for cloud archival tests. Owns the `TestLoopEnv` and exposes
/// composable action and assertion methods so each test reads as an explicit
/// Arrange-Act-Assert sequence.
struct CloudArchiveHarness {
    env: TestLoopEnv,
    /// Account ID of the cloud archival node that writes to cloud storage.
    archival_id: AccountId,
    /// Epoch length in blocks.
    epoch_length: BlockHeightDelta,
    /// Whether cold (split) storage is enabled on the archival node.
    cold_storage_enabled: bool,
    /// Account ID of the reader node, set after `bootstrap_reader()`.
    reader_id: Option<AccountId>,
}

struct CloudArchiveHarnessBuilder {
    cold_storage: bool,
}

impl CloudArchiveHarnessBuilder {
    fn cold_storage(mut self, enabled: bool) -> Self {
        self.cold_storage = enabled;
        self
    }

    fn build(self) -> CloudArchiveHarness {
        let archival_kind =
            if self.cold_storage { ArchivalKind::ColdAndCloud } else { ArchivalKind::Cloud };
        let user_account: AccountId = CloudArchiveHarness::USER_ACCOUNT.parse().unwrap();
        let archival_id = archival_account_id();

        let env = TestLoopBuilder::new()
            .shard_layout(ShardLayout::multi_shard(3, 3))
            .epoch_length(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH)
            .add_user_account(&user_account, CloudArchiveHarness::USER_BALANCE)
            .enable_archival_node(archival_kind)
            .gc_num_epochs_to_keep(MIN_GC_NUM_EPOCHS_TO_KEEP)
            .config_modifier(move |config, _client_index| {
                if !config.archive {
                    return;
                }
                config.cloud_archival_writer = Some(CloudArchivalWriterConfig {
                    archive_block_data: true,
                    ..Default::default()
                });
            })
            .build();

        CloudArchiveHarness {
            env,
            archival_id,
            epoch_length: CloudArchiveHarness::DEFAULT_EPOCH_LENGTH,
            cold_storage_enabled: self.cold_storage,
            reader_id: None,
        }
    }
}

impl CloudArchiveHarness {
    const DEFAULT_EPOCH_LENGTH: BlockHeightDelta = 10;
    const USER_ACCOUNT: &str = "user_account";
    const USER_BALANCE: Balance = Balance::from_near(42);

    fn builder() -> CloudArchiveHarnessBuilder {
        CloudArchiveHarnessBuilder { cold_storage: false }
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

    fn bootstrap_reader(&mut self, target_height: BlockHeight) {
        let reader_id: AccountId = "reader".parse().unwrap();
        bootstrap_reader_at_height(&mut self.env, &reader_id, target_height);
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

    fn assert_external_data_at_height(&self, height: BlockHeight) {
        check_data_at_height(&self.env, &self.archival_id, height);
    }

    /// Checks that each epoch (except the final one) has complete snapshots
    /// and epoch data uploaded. Derives `final_epoch_height` from the current
    /// chain head.
    fn assert_snapshots_ok(&self) {
        let head_height = self.env.archival_node().head().height;
        let final_epoch_height = head_height / self.epoch_length;
        snapshots_sanity_check(&self.env, &self.archival_id, final_epoch_height);
    }

    fn assert_reader_account_balance(&self, account: &AccountId, expected: Balance) {
        let reader_id = self.reader_id.as_ref().expect("no reader bootstrapped");
        check_account_balance(&self.env, reader_id, account, expected);
    }

    fn cloud_head(&self) -> BlockHeight {
        get_cloud_head(&self.env, &self.archival_id)
    }

    fn gc_tail(&self) -> BlockHeight {
        self.env.archival_node().client().chain.chain_store().tail()
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

/// Verifies that block data can be read from the cloud.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_read_data_at_height() {
    let mut h = CloudArchiveHarness::builder().build();
    // 2 epochs is enough for the writer to upload data for height 5.
    h.run_until_epoch(2);
    h.assert_external_data_at_height(h.epoch_length / 2);
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

    // Bootstrap reader at mid-epoch-2. Verify the target was gc-ed locally
    // so the reader must use cloud storage.
    let target = h.epoch_length + h.epoch_length / 2;
    assert!(h.gc_tail() > target, "target height should be gc-ed");
    h.bootstrap_reader(target);
    h.assert_reader_account_balance(
        &CloudArchiveHarness::USER_ACCOUNT.parse().unwrap(),
        CloudArchiveHarness::USER_BALANCE,
    );
    h.kill_reader();

    h.shutdown();
}
