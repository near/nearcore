use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::archival_account_id;
use crate::utils::cloud_archival::{
    ReshardingInfo, WriterConfig, add_writer_node, apply_writer_settings,
    assert_reader_writer_parity, assert_resharding_epoch_snapshot_forced,
    assert_writer_inverse_deltas, bootstrap_reader, build_shard_tries, check_account_balance,
    check_data_at_height_for_shards, gc_and_heads_sanity_checks, get_cloud_head, get_cloud_storage,
    get_writer_handle, has_state_root, run_node_until, run_until_one_epoch_after_resharding,
    simulate_lagging_shard, snapshots_sanity_check, stop_and_restart_node,
};
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use borsh::to_vec;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::MIN_GC_NUM_EPOCHS_TO_KEEP;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::archive::cloud_archival_reader::find_snapshot_at_or_before;
#[cfg(feature = "nightly")]
use near_client::archive::cloud_archival_reader::save_block_data;
use near_primitives::block::Block;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
#[cfg(feature = "nightly")]
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::receipt::{Receipt, ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid};
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta, ShardId};
#[cfg(feature = "nightly")]
use near_primitives::utils::get_block_shard_id_rev;
use near_primitives::utils::{get_block_shard_id, get_outcome_id_block_hash, index_to_bytes};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::bucket_config::BucketConfig;
use near_store::db::cloud_shard_head_key;
#[cfg(feature = "nightly")]
use near_store::test_utils::create_test_store;
use near_store::{DBCol, KeyForStateChanges, ShardUId, Store};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

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
    /// Post-resharding shard layout when `enable_resharding` was used.
    new_shard_layout: Option<ShardLayout>,
    /// Boundary account of the resharding split when `enable_resharding` was used.
    resharding_boundary: Option<AccountId>,
}

struct CloudArchiveHarnessBuilder {
    cold_storage: bool,
    writer: WriterConfig,
    num_validators: Option<usize>,
    gc_num_epochs_to_keep: u64,
    dropped_block_heights: HashSet<BlockHeight>,
    /// Per-shard chunk-production schedule applied every epoch.
    dropped_chunks_by_shard: HashMap<ShardId, Vec<bool>>,
    /// Whether to schedule a static resharding split.
    resharding_enabled: bool,
    /// Cloud archival batch size in blocks.
    batch_size: u32,
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

    /// Effectively disables GC so the writer retains the bootstrap range for
    /// reader-writer-parity assertions.
    fn disable_gc(mut self) -> Self {
        self.gc_num_epochs_to_keep = 1000;
        self
    }

    /// Sets the number of block-and-chunk-producer validators.
    fn validators(mut self, count: usize) -> Self {
        // `drop_blocks_at` / `drop_chunks` need count >= 4 to be observable on
        // the chain; see `block_dropper_by_height` in `test-loop-tests/src/utils/network.rs`.
        assert!(count > 0);
        self.num_validators = Some(count);
        self
    }

    fn drop_blocks_at(mut self, heights: &[BlockHeight]) -> Self {
        self.dropped_block_heights.extend(heights.iter().copied());
        self
    }

    /// `pattern[i] == false` drops the chunk at offset `i` of every epoch.
    fn drop_chunks(mut self, shard_id: ShardId, pattern: Vec<bool>) -> Self {
        self.dropped_chunks_by_shard.insert(shard_id, pattern);
        self
    }

    /// Schedules a static resharding split at the default boundary account in
    /// the successor protocol version.
    fn enable_resharding(mut self) -> Self {
        self.resharding_enabled = true;
        self
    }

    fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    fn build(self) -> CloudArchiveHarness {
        let user_account: AccountId = CloudArchiveHarness::USER_ACCOUNT.parse().unwrap();
        let archival_kind =
            if self.cold_storage { ArchivalKind::ColdAndCloud } else { ArchivalKind::Cloud };
        let archival_id = self.writer.id.clone();
        let snapshot_every_n_epochs = self.writer.snapshot_every_n_epochs;
        let has_drops =
            !self.dropped_block_heights.is_empty() || !self.dropped_chunks_by_shard.is_empty();
        let base_shard_layout = CloudArchiveHarness::default_shard_layout();
        let mut builder = TestLoopBuilder::new()
            .shard_layout(base_shard_layout.clone())
            .epoch_length(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH)
            .add_user_account(&user_account, CloudArchiveHarness::USER_BALANCE)
            .enable_archival_node(archival_kind)
            .gc_num_epochs_to_keep(self.gc_num_epochs_to_keep)
            .bucket_config(BucketConfig::with_batch_size_for_test(self.batch_size))
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
            });
        let mut new_shard_layout = None;
        let mut resharding_boundary = None;
        if self.resharding_enabled {
            let boundary: AccountId =
                CloudArchiveHarness::RESHARDING_BOUNDARY_ACCOUNT.parse().unwrap();
            assert!(
                self.num_validators.is_none(),
                "resharding tests use the default single validator; the explicit \
                 EpochConfigStore does not carry a custom validator seat count"
            );
            let base_epoch_config = TestEpochConfigBuilder::new()
                .shard_layout(base_shard_layout)
                .epoch_length(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH)
                .build();
            let (new_epoch_config, derived_layout) =
                derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary);
            new_shard_layout = Some(derived_layout);
            let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter([
                (PROTOCOL_VERSION - 1, Arc::new(base_epoch_config)),
                (PROTOCOL_VERSION, Arc::new(new_epoch_config)),
            ]));
            builder = builder
                .protocol_version(PROTOCOL_VERSION - 1)
                .epoch_config_store(epoch_config_store);
            resharding_boundary = Some(boundary);
        }
        if let Some(count) = self.num_validators {
            builder = builder.validators(count, 0);
        }
        // Drop conditions must be registered after build but before warmup;
        // delay_warmup splits build/warmup so we can call drop() in between.
        // No-drop tests keep the default auto-warmup.
        if has_drops {
            builder = builder.delay_warmup();
        }
        let mut env = builder.build();
        if !self.dropped_block_heights.is_empty() {
            env = env.drop(DropCondition::BlocksByHeight(self.dropped_block_heights));
        }
        if !self.dropped_chunks_by_shard.is_empty() {
            env = env.drop(DropCondition::ChunksProducedByHeight(self.dropped_chunks_by_shard));
        }
        if has_drops {
            env = env.warmup();
        }
        CloudArchiveHarness {
            env,
            archival_id,
            epoch_length: CloudArchiveHarness::DEFAULT_EPOCH_LENGTH,
            cold_storage_enabled: self.cold_storage,
            snapshot_every_n_epochs,
            reader_id: None,
            new_shard_layout,
            resharding_boundary,
        }
    }
}

impl CloudArchiveHarness {
    const DEFAULT_EPOCH_LENGTH: BlockHeightDelta = 10;
    const RESHARDING_BOUNDARY_ACCOUNT: &str = "boundary";
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
            num_validators: None,
            gc_num_epochs_to_keep: MIN_GC_NUM_EPOCHS_TO_KEEP,
            dropped_block_heights: HashSet::new(),
            dropped_chunks_by_shard: HashMap::new(),
            resharding_enabled: false,
            batch_size: Self::TEST_BATCH_SIZE,
        }
    }

    fn run_until(&mut self, height: BlockHeight) {
        run_node_until(&mut self.env, &self.archival_id, height);
    }

    fn run_until_epoch(&mut self, num_epochs: u64) {
        self.run_until(num_epochs * self.epoch_length);
    }

    /// Post-resharding shard layout. Requires `enable_resharding` on the builder.
    fn new_shard_layout(&self) -> &ShardLayout {
        self.new_shard_layout.as_ref().expect("enable_resharding required")
    }

    /// Runs the chain one epoch past the resharding.
    fn run_until_one_epoch_after_resharding(&mut self) -> ReshardingInfo {
        let new_layout = self.new_shard_layout().clone();
        let base_layout = Self::default_shard_layout();
        let boundary = self.resharding_boundary.clone().expect("enable_resharding required");
        run_until_one_epoch_after_resharding(
            &mut self.env,
            &self.archival_id,
            &base_layout,
            &new_layout,
            &boundary,
            self.epoch_length,
        )
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

    fn reader_store(&self) -> Store {
        let reader_id = self.reader_id.as_ref().expect("no reader bootstrapped");
        self.env.node_for_account(reader_id).client().chain.chain_store().store()
    }

    fn writer_store(&self) -> Store {
        self.env.archival_node().client().chain.chain_store().store()
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

    fn assert_reader_writer_parity(&self, start: BlockHeight, end: BlockHeight) {
        assert_reader_writer_parity(&self.reader_store(), &self.writer_store(), start, end);
    }

    fn assert_reader_account_balance(&self, account: &AccountId, expected: Balance) {
        let reader_id = self.reader_id.as_ref().expect("no reader bootstrapped");
        check_account_balance(&self.env, reader_id, account, expected);
    }

    /// Asserts the writer attached inverse state changes to the resharding gap
    /// blocks. Requires `enable_resharding`.
    fn assert_writer_inverse_deltas(&self, info: &ReshardingInfo) {
        assert_writer_inverse_deltas(&self.env, &self.archival_id, info);
    }

    /// Asserts the resharding epoch's snapshot fired despite being off-cadence.
    /// Requires `enable_resharding` and a cadence above 1.
    fn assert_resharding_epoch_snapshot_forced(&self, info: &ReshardingInfo) {
        assert_resharding_epoch_snapshot_forced(
            &self.env,
            &self.archival_id,
            info,
            self.snapshot_every_n_epochs,
        );
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
    // h=3 is the first height where every shard has a new chunk.
    h.check_data(&[(3, &all_shards), (h.epoch_length + 1, &all_shards)]);
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
    assert!(head <= 27 && (head + 1).is_multiple_of(batch_size), "cloud_head: {head}");
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
    // Probe at batch ends so the partial first batch (which starts above
    // its grid position) doesn't fail the "height in batch" check.
    for batch_end in (batch_size - 1..=cloud_head).step_by(batch_size as usize) {
        assert!(h.block_batch_exists_at(batch_end), "batch ending at {batch_end} should exist");
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
    // Reader still uses cloud: it's a fresh node with no local data.
    let mut h = CloudArchiveHarness::builder().disable_gc().build();
    h.run_until_epoch(3);
    h.assert_heads_ok_before_gc();
    h.assert_snapshots_ok();

    // Bootstrap reader from mid-epoch-1 to mid-epoch-2, spanning an epoch boundary.
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);
    h.assert_reader_writer_parity(start, target);
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

    h.shutdown();
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

    h.shutdown();
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

    h.shutdown();
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
    h.check_data(&[(3, &all_shard_ids), (h.epoch_length + 1, &all_shard_ids)]);
    h.assert_heads_and_gc_ok();

    h.shutdown();
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

    h.shutdown();
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
    let epoch_id_of = |height| {
        *cloud_storage.get_block_data(height).unwrap().unwrap().block().header().epoch_id()
    };

    // Epoch 4 was snapshotted: first probe hits.
    let hit_at_45 = find_snapshot_at_or_before(&cloud_storage, 45, shard_id).unwrap();
    assert_eq!(hit_at_45, (4, epoch_id_of(45)));

    // Epoch 3 was skipped: probe misses, walks back to epoch 2.
    let hit_at_35 = find_snapshot_at_or_before(&cloud_storage, 35, shard_id).unwrap();
    assert_eq!(hit_at_35, (2, epoch_id_of(25)));

    h.shutdown();
}

/// `find_snapshot_at_or_before` must skip a missing slot at `epoch_start - 1`
/// when stepping to the previous epoch. The test drops the block at that
/// height; the function still locates the earlier snapshot.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_find_snapshot_with_missing_epoch_boundary() {
    // With `epoch_length = 10`, height 29 sits at `epoch_start_3 - 1`, the
    // first height the walk-back from epoch 3 probes.
    assert_eq!(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH, 10);
    let dropped_height: BlockHeight = 29;
    let mut h = CloudArchiveHarness::builder()
        .validators(4)
        .snapshot_every_n_epochs(2)
        .drop_blocks_at(&[dropped_height])
        .build();
    h.run_until_epoch(MIN_GC_NUM_EPOCHS_TO_KEEP + 3);
    h.assert_snapshots_ok();

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let batch = cloud_storage.get_block_batch_for_height(dropped_height).unwrap();
    assert!(
        batch.get_block_at_height(dropped_height).is_none(),
        "block at h={dropped_height} must be None in cloud"
    );

    let shard_id = CloudArchiveHarness::all_shard_ids()[0];
    let epoch_id_of = |height| {
        *cloud_storage.get_block_data(height).unwrap().unwrap().block().header().epoch_id()
    };
    // Probe at height 35 (epoch 3, no snapshot). The walk-back lands on
    // `epoch_start_3 - 1 = 29` which is the dropped block; the function must
    // walk further down to a present block in epoch 2 and find its snapshot.
    let hit = find_snapshot_at_or_before(&cloud_storage, 35, shard_id).unwrap();
    assert_eq!(hit, (2, epoch_id_of(25)));

    h.shutdown();
}

/// A block at one height is lost; the batch entry there is `None` and
/// `cloud_head` advances past it. The dropped slot pushes the epoch's sync block
/// past a mid-epoch bootstrap target, so the reader reconstructs from the
/// previous epoch's snapshot and applies deltas across the gap.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_single_skipped_slot() {
    let dropped_height: BlockHeight = 13;
    let mut h = CloudArchiveHarness::builder()
        .validators(4)
        .drop_blocks_at(&[dropped_height])
        .disable_gc()
        .build();
    h.run_until_epoch(3);

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let batch = cloud_storage.get_block_batch_for_height(dropped_height).unwrap();
    assert!(batch.get_block_at_height(dropped_height).is_none());
    assert!(h.cloud_head() > dropped_height);
    h.assert_heads_ok_before_gc();

    let (start, target) = (5, 15);
    let epoch_of = |height| {
        *cloud_storage.get_block_data(height).unwrap().unwrap().block().header().epoch_id()
    };
    // start and target straddle an epoch boundary, so reconstruction crosses it.
    assert_ne!(epoch_of(start), epoch_of(target), "start and target must be in different epochs");
    let target_epoch_data = cloud_storage.get_epoch_data(epoch_of(target)).unwrap();
    let sync_block_height = target_epoch_data.sync_block_height();
    // The dropped slot is in the target epoch within the bootstrap range; it
    // pushes that epoch's sync block past the target, so the reader cannot use
    // the target epoch's own snapshot and must walk back to an earlier one.
    assert!(
        target_epoch_data.epoch_start_height() <= dropped_height && dropped_height < target,
        "dropped slot {dropped_height} must be in the target epoch and the bootstrap range"
    );
    assert!(
        sync_block_height > target,
        "sync block {sync_block_height} must be past target {target}"
    );

    h.bootstrap_reader(start, target);
    h.assert_reader_writer_parity(start, target);
    h.kill_reader();

    h.shutdown();
}

/// At cadence 2, `start` (epoch 3) has no snapshot, so the reader resolves the
/// snapshot to an earlier epoch whose sync block is below the downloaded block
/// range. Reconstruction loads that snapshot from cloud state parts, so it works
/// without the sync block being local.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_bootstrap_snapshot_in_earlier_epoch() {
    let mut h = CloudArchiveHarness::builder().snapshot_every_n_epochs(2).build();
    h.run_until_epoch(5);

    // Pin the scenario: the resolved snapshot is in an earlier epoch than start.
    let (start, target) = (35, 38);
    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let shard_id = CloudArchiveHarness::all_shard_ids()[0];
    let (_, snapshot_epoch_id) =
        find_snapshot_at_or_before(&cloud_storage, start, shard_id).unwrap();
    let start_epoch_id =
        *cloud_storage.get_block_data(start).unwrap().unwrap().block().header().epoch_id();
    assert_ne!(snapshot_epoch_id, start_epoch_id, "snapshot must resolve to an earlier epoch");

    h.bootstrap_reader(start, target);
    h.kill_reader();
    h.shutdown();
}

/// Every block in a batch window is lost; the batch is uploaded with `None`
/// at every height and `cloud_head` advances past the gap.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_fully_skipped_batch() {
    // Drops every height of one batch. `[12, 13, 14, 15]` assumes batch_size 4.
    assert_eq!(CloudArchiveHarness::TEST_BATCH_SIZE, 4);
    let dropped_heights: Vec<BlockHeight> = vec![12, 13, 14, 15];
    // TODO(cloud_archival): drop validator count once `block_dropper_by_height`
    // also intercepts `BlockRequest` responses.
    let mut h = CloudArchiveHarness::builder()
        .validators(12)
        .drop_blocks_at(&dropped_heights)
        .disable_gc()
        .build();
    h.run_until_epoch(3);

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let batch = cloud_storage.get_block_batch_for_height(12).unwrap();
    for h_drop in &dropped_heights {
        assert!(
            batch.get_block_at_height(*h_drop).is_none(),
            "batch entry at h={h_drop} must be None"
        );
    }
    assert!(h.cloud_head() > 15, "cloud_head must advance past the gap");
    h.assert_heads_ok_before_gc();

    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);
    h.assert_reader_writer_parity(start, target);
    h.kill_reader();

    h.shutdown();
}

/// Bootstrap a reader over a range whose start and end heights are both
/// skipped slots, with one shard's chunks also dropped mid-range. Exercises
/// the missing-block handling in `bootstrap_range` and the carried-over-chunk
/// path during state apply.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_bootstrap_with_missing_blocks_and_chunks() {
    assert_eq!(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH, 10);
    // Drop blocks at the bootstrap range's start and end heights. The end
    // height is chosen near the end of an epoch so the target's epoch's
    // sync block (computed near the start of that epoch) is comfortably
    // inside the range, even when block/chunk drops delay finalization.
    let start = 14;
    let target = 29;
    // Drop one shard's chunks at offset 5 of every epoch (heights 16, 26, ...),
    // placing a carried-over chunk inside the range.
    let dropped_shard = CloudArchiveHarness::all_shard_ids()[0];
    let mut chunk_pattern = vec![true; 10];
    chunk_pattern[5] = false;

    let mut h = CloudArchiveHarness::builder()
        .validators(4)
        .drop_blocks_at(&[start, target])
        .drop_chunks(dropped_shard, chunk_pattern)
        .disable_gc()
        .build();
    h.run_until_epoch(target / h.epoch_length + 2);
    h.assert_heads_ok_before_gc();
    h.assert_snapshots_ok();

    // Confirm the drops actually landed in cloud storage before bootstrap.
    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    for &dropped in &[start, target] {
        let batch = cloud_storage.get_block_batch_for_height(dropped).unwrap();
        assert!(
            batch.get_block_at_height(dropped).is_none(),
            "block at h={dropped} must be None in cloud"
        );
    }
    // The chunk-drop pattern is applied per epoch; assert that offset 5 of
    // the target's epoch is missing in cloud for the dropped shard.
    let target_epoch_id =
        *cloud_storage.get_block_data(25).unwrap().unwrap().block().header().epoch_id();
    let target_epoch_start =
        cloud_storage.get_epoch_data(target_epoch_id).unwrap().epoch_start_height();
    let chunk_drop_height = target_epoch_start + 5;
    assert!(
        cloud_storage
            .get_shard_data(chunk_drop_height, dropped_shard)
            .unwrap()
            .unwrap()
            .chunk()
            .is_none(),
        "carried chunk at h={chunk_drop_height} must be archived with chunk=None"
    );

    h.bootstrap_reader(start, target);
    h.assert_reader_writer_parity(start, target);
    // A correct balance proves bootstrap completed without panic, the trie
    // was reconstructed up to the last present block at or below the target,
    // and the carried-over chunk path was traversed during state apply.
    h.assert_reader_account_balance(
        &CloudArchiveHarness::USER_ACCOUNT.parse().unwrap(),
        CloudArchiveHarness::USER_BALANCE,
    );
    h.kill_reader();

    h.shutdown();
}

/// One shard's chunk producer is offline at some heights; the offline
/// shard's batch entry must be `None` at those heights while other shards
/// remain `Some`.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_missing_chunks_one_shard() {
    // Drop chunks at offset 0 (epoch start), 5 (mid-epoch), and 9 (epoch end)
    // of every epoch. The hard-coded offsets assume epoch length 10.
    assert_eq!(CloudArchiveHarness::DEFAULT_EPOCH_LENGTH, 10);
    let dropped_offsets = [0, 5, 9];
    let all_shard_ids = CloudArchiveHarness::all_shard_ids();
    let dropped_shard = all_shard_ids[0];
    let other_shard = all_shard_ids[1];
    let mut pattern = vec![true; 10];
    for offset in dropped_offsets {
        pattern[offset as usize] = false;
    }
    let mut h = CloudArchiveHarness::builder()
        .validators(4)
        .drop_chunks(dropped_shard, pattern)
        .disable_gc()
        .build();
    h.run_until_epoch(4);

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let state_root_at = |height| -> CryptoHash {
        *cloud_storage
            .get_shard_data(height, dropped_shard)
            .unwrap()
            .unwrap()
            .chunk_extra()
            .state_root()
    };
    for epoch in [1u64, 2] {
        let probe = epoch * h.epoch_length + h.epoch_length / 2;
        let epoch_id =
            *cloud_storage.get_block_data(probe).unwrap().unwrap().block().header().epoch_id();
        let epoch_start = cloud_storage.get_epoch_data(epoch_id).unwrap().epoch_start_height();
        for offset in dropped_offsets {
            let height = epoch_start + offset;
            let dropped = cloud_storage.get_shard_data(height, dropped_shard).unwrap().unwrap();
            let other = cloud_storage.get_shard_data(height, other_shard).unwrap().unwrap();
            assert!(dropped.chunk().is_none(), "carried chunk at h={height} must have chunk=None");
            assert!(other.chunk().is_some(), "other shard at h={height} must have a new chunk");
            // State advances at every block (bandwidth scheduler), so the
            // carried chunk's state_root must differ from both neighbors.
            let prev = height - 1;
            let next = height + 1;
            assert_ne!(
                state_root_at(height),
                state_root_at(prev),
                "state_root at h={height} (carried) must differ from h={prev}",
            );
            assert_ne!(
                state_root_at(height),
                state_root_at(next),
                "state_root at h={height} (carried) must differ from h={next}",
            );
        }
    }
    h.assert_heads_ok_before_gc();

    let start = h.epoch_length / 2;
    let target = 3 * h.epoch_length;
    h.bootstrap_reader(start, target);
    h.assert_reader_writer_parity(start, target);
    h.kill_reader();

    h.shutdown();
}

/// Verifies that each archived `ShardData` carries the outcomes and
/// receipt-to-tx info for its `(block_hash, shard_id)` matching the chain
/// store entry-by-entry. Walks every still-on-chain height up to `cloud_head`.
/// Assumes no chunk drops in the iterated window (every shard has a new chunk).
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_outcomes_and_receipts() {
    let mut h = CloudArchiveHarness::builder().disable_gc().build();
    let user_account: AccountId = CloudArchiveHarness::USER_ACCOUNT.parse().unwrap();
    // Cross-shard transfers exercise outgoing receipts; one self-transfer
    // produces a local (non-outgoing) action receipt whose ReceiptToTx the
    // writer must still archive.
    for _ in 0..3 {
        let tx = h.env.validator().tx_send_money(
            &user_account,
            &h.archival_id,
            Balance::from_yoctonear(100),
        );
        h.env.validator().submit_tx(tx);
    }
    let self_tx =
        h.env.validator().tx_send_money(&user_account, &user_account, Balance::from_yoctonear(1));
    h.env.validator().submit_tx(self_tx);
    h.run_until_epoch(3);

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let writer_store = h.writer_store();
    let chain_store = writer_store.chain_store();
    let writer_chunk_store = writer_store.chunk_store();

    // Iterate a fixed mid-chain window; assert cloud has caught up past it.
    let start = h.epoch_length / 2;
    let end = 2 * h.epoch_length;
    assert!(h.cloud_head() >= end, "cloud_head {} below end {end}", h.cloud_head());

    let mut total_outcomes = 0usize;
    let mut total_receipt_to_tx = 0usize;
    for height in start..=end {
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        for shard_id in &CloudArchiveHarness::all_shard_ids() {
            let shard_data = cloud_storage.get_shard_data(height, *shard_id).unwrap().unwrap();

            // Cloud outcome_ids must equal chain's OutcomeIds; each outcome must match.
            let cloud_stored_outcomes: HashMap<CryptoHash, &ExecutionOutcomeWithProof> = shard_data
                .transaction_result_for_block()
                .unwrap()
                .iter()
                .map(|(id, outcome)| (*id, outcome))
                .collect();
            let expected_outcome_ids: HashSet<CryptoHash> = chain_store
                .get_outcomes_by_block_hash_and_shard_id(&block_hash, *shard_id)
                .into_iter()
                .collect();
            let cloud_stored_outcome_ids: HashSet<CryptoHash> =
                cloud_stored_outcomes.keys().copied().collect();
            assert_eq!(
                cloud_stored_outcome_ids, expected_outcome_ids,
                "outcome_ids mismatch at h={height} shard={shard_id}"
            );
            for (id, cloud_stored_outcome) in &cloud_stored_outcomes {
                let chain_outcome =
                    chain_store.get_outcome_by_id_and_block_hash(id, &block_hash).unwrap();
                // `ExecutionOutcomeWithProof` has no `PartialEq`; compare via borsh.
                assert_eq!(
                    to_vec(&chain_outcome).unwrap(),
                    to_vec(*cloud_stored_outcome).unwrap(),
                    "outcome proof mismatch at h={height} shard={shard_id} outcome_id={id}"
                );
            }
            total_outcomes += cloud_stored_outcomes.len();

            let cloud_stored_receipt_to_tx: HashMap<CryptoHash, &ReceiptToTxInfo> = shard_data
                .receipt_to_tx()
                .map(|r| r.iter().map(|(id, info)| (*id, info)).collect())
                .unwrap_or_default();
            for (id, cloud_stored_info) in &cloud_stored_receipt_to_tx {
                assert_eq!(
                    &chain_store.get_receipt_to_tx(id).unwrap(),
                    *cloud_stored_info,
                    "receipt_to_tx info mismatch at h={height} shard={shard_id} receipt_id={id}"
                );
            }
            // Iterate chain's outgoing receipts and verify cloud carries each
            // one's ReceiptToTx when chain has it. Catches a writer that
            // skips entries chain has.
            let chain_outgoing = chain_store
                .get_outgoing_receipts(&block_hash, *shard_id)
                .map(|r| r.to_vec())
                .unwrap_or_default();
            for receipt in &chain_outgoing {
                let receipt_id = receipt.receipt_id();
                if chain_store.get_receipt_to_tx(receipt_id).is_some() {
                    assert!(
                        cloud_stored_receipt_to_tx.contains_key(receipt_id),
                        "outgoing receipt {receipt_id} at h={height} shard={shard_id}: chain has ReceiptToTx but cloud missing",
                    );
                }
            }
            // Cloud's FromTransaction count must equal the chunk's tx count.
            let cloud_stored_from_tx_count = cloud_stored_receipt_to_tx
                .values()
                .filter(|info| {
                    let ReceiptToTxInfo::V1(v1) = **info;
                    matches!(v1.origin, ReceiptOrigin::FromTransaction(_))
                })
                .count();
            let ChunkApplyStats::V1(stats) =
                writer_chunk_store.get_chunk_apply_stats(&block_hash, shard_id).unwrap()
            else {
                unreachable!("freshly written chunks must be ChunkApplyStats::V1");
            };
            assert_eq!(
                cloud_stored_from_tx_count, stats.transactions_num as usize,
                "FromTransaction count mismatch at h={height} shard={shard_id}: cloud has {cloud_stored_from_tx_count}, chunk apply processed {} transactions",
                stats.transactions_num,
            );
            total_receipt_to_tx += cloud_stored_receipt_to_tx.len();
        }
    }
    assert!(total_outcomes > 0, "no outcomes were compared");
    assert!(total_receipt_to_tx > 0, "no receipt_to_tx entries were compared");

    h.bootstrap_reader(start, end);
    h.assert_reader_writer_parity(start, end);
    h.kill_reader();

    h.shutdown();
}

/// Verifies that after reader bootstrap, the local store has entries in
/// the per-block cold columns the reader reconstructs from cloud data:
/// `BlockPerHeight`, `ChunkHashesByHeight`, and `NextBlockHashes`.
#[test]
// TODO(cloud_archival): un-ignore once the reader reconstructs per-block cold columns.
#[ignore]
fn test_cloud_archival_reader_reconstructs_per_block_columns() {
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(3 + MIN_GC_NUM_EPOCHS_TO_KEEP);
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);

    let store = h.reader_store();

    for height in start..=target {
        let block_hash: CryptoHash = store
            .get_ser(DBCol::BlockHeight, &index_to_bytes(height))
            .expect("BlockHeight missing");
        assert!(
            store.exists(DBCol::BlockPerHeight, &index_to_bytes(height)),
            "BlockPerHeight missing at h={height}"
        );
        assert!(
            store.exists(DBCol::ChunkHashesByHeight, &index_to_bytes(height)),
            "ChunkHashesByHeight missing at h={height}"
        );
        if height < target {
            assert!(
                store.exists(DBCol::NextBlockHashes, block_hash.as_ref()),
                "NextBlockHashes missing at h={height}"
            );
        }
    }

    h.kill_reader();
    h.shutdown();
}

/// Verifies that after reader bootstrap, the local store has entries in
/// the always-populated per-shard cold columns: `Chunks`, `ChunkExtra`,
/// `ChunkApplyStats`, `IncomingReceipts`, `OutgoingReceipts`, and
/// `OutcomeIds`.
#[test]
// TODO(cloud_archival): un-ignore once the reader reconstructs per-shard cold columns.
#[ignore]
fn test_cloud_archival_reader_reconstructs_per_shard_columns() {
    let mut h = CloudArchiveHarness::builder().build();
    h.run_until_epoch(3 + MIN_GC_NUM_EPOCHS_TO_KEEP);
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);

    let store = h.reader_store();

    for height in start..=target {
        let block_hash: CryptoHash = store
            .get_ser(DBCol::BlockHeight, &index_to_bytes(height))
            .expect("BlockHeight missing");
        let block: Block = store.get_ser(DBCol::Block, block_hash.as_ref()).unwrap();
        for shard_uid in &CloudArchiveHarness::all_shard_uids() {
            let shard_id = shard_uid.shard_id();
            let block_shard_key = get_block_shard_id(&block_hash, shard_id);
            let block_shard_uid_key = get_block_shard_uid(&block_hash, shard_uid);
            assert!(
                store.exists(DBCol::ChunkExtra, &block_shard_uid_key),
                "ChunkExtra missing at h={height} shard={shard_id}"
            );
            assert!(
                store.exists(DBCol::ChunkApplyStats, &block_shard_key),
                "ChunkApplyStats missing at h={height} shard={shard_id}"
            );
            assert!(
                store.exists(DBCol::IncomingReceipts, &block_shard_key),
                "IncomingReceipts missing at h={height} shard={shard_id}"
            );
            assert!(
                store.exists(DBCol::OutgoingReceipts, &block_shard_key),
                "OutgoingReceipts missing at h={height} shard={shard_id}"
            );
            assert!(
                store.exists(DBCol::OutcomeIds, &block_shard_key),
                "OutcomeIds missing at h={height} shard={shard_id}"
            );
            let chunk_header = block
                .chunks()
                .iter_raw()
                .find(|c| c.shard_id() == shard_id)
                .cloned()
                .unwrap_or_else(|| panic!("chunk header missing at h={height} shard={shard_id}"));
            assert!(
                store.exists(DBCol::Chunks, chunk_header.chunk_hash().as_ref()),
                "Chunks missing at h={height} shard={shard_id}"
            );
        }
    }

    h.kill_reader();
    h.shutdown();
}

/// Verifies that after reader bootstrap, the local store has entries in
/// the per-shard data columns the reader reconstructs from chunk-apply
/// activity: `Transactions`, `Receipts`, `TransactionResultForBlock`,
/// `ReceiptToTx`, and `StateChanges`. The test submits a cross-shard
/// transfer before the bootstrap range to populate them.
#[test]
// TODO(cloud_archival): un-ignore once the reader reconstructs per-shard cold columns.
#[ignore]
fn test_cloud_archival_reader_reconstructs_per_shard_data_columns() {
    let user_account: AccountId = CloudArchiveHarness::USER_ACCOUNT.parse().unwrap();
    let mut h = CloudArchiveHarness::builder().build();
    let tx = h.env.validator().tx_send_money(
        &user_account,
        &h.archival_id,
        Balance::from_yoctonear(100),
    );
    h.env.validator().submit_tx(tx);
    h.run_until_epoch(3 + MIN_GC_NUM_EPOCHS_TO_KEEP);
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);

    let store = h.reader_store();

    let mut have_transactions = false;
    let mut have_receipts = false;
    let mut have_transaction_result_for_block = false;
    let mut have_receipt_to_tx = false;
    let mut have_state_changes = false;

    for height in start..=target {
        let block_hash: CryptoHash = store
            .get_ser(DBCol::BlockHeight, &index_to_bytes(height))
            .expect("BlockHeight missing");
        let block: Block = store.get_ser(DBCol::Block, block_hash.as_ref()).unwrap();
        for shard_uid in &CloudArchiveHarness::all_shard_uids() {
            let shard_id = shard_uid.shard_id();
            let block_shard_key = get_block_shard_id(&block_hash, shard_id);
            let chunk_header =
                block.chunks().iter_raw().find(|c| c.shard_id() == shard_id).cloned().unwrap();
            let chunk: ShardChunk =
                store.get_ser(DBCol::Chunks, chunk_header.chunk_hash().as_ref()).unwrap();
            for tx in chunk.to_transactions() {
                if store.exists(DBCol::Transactions, tx.get_hash().as_ref()) {
                    have_transactions = true;
                }
            }
            for receipt in chunk.prev_outgoing_receipts() {
                if store.exists(DBCol::Receipts, receipt.receipt_id().as_ref()) {
                    have_receipts = true;
                }
            }
            let outcome_ids: Vec<CryptoHash> =
                store.get_ser(DBCol::OutcomeIds, &block_shard_key).unwrap_or_default();
            for outcome_id in &outcome_ids {
                if store.exists(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(outcome_id, &block_hash),
                ) {
                    have_transaction_result_for_block = true;
                }
            }
            let outgoing: Vec<Receipt> =
                store.get_ser(DBCol::OutgoingReceipts, &block_shard_key).unwrap_or_default();
            for receipt in &outgoing {
                if store.exists(DBCol::ReceiptToTx, receipt.receipt_id().as_ref()) {
                    have_receipt_to_tx = true;
                }
            }
        }
        let state_changes_prefix: Vec<u8> = KeyForStateChanges::for_block(&block_hash).into();
        if store.iter_prefix(DBCol::StateChanges, &state_changes_prefix).next().is_some() {
            have_state_changes = true;
        }
    }

    assert!(have_transactions, "no transactions reconstructed");
    assert!(have_receipts, "no receipts reconstructed");
    assert!(have_transaction_result_for_block, "no transaction_result_for_block reconstructed");
    assert!(have_receipt_to_tx, "no receipt_to_tx reconstructed");
    assert!(have_state_changes, "no state_changes reconstructed");

    h.kill_reader();
    h.shutdown();
}

/// Verifies that with one shard's chunk dropped at a specific height in the
/// bootstrapped range, the trie state for that shard is queryable at the
/// missing-chunk height and its immediate neighbors via the block's chunk
/// header `prev_state_root`.
#[test]
// TODO(cloud_archival): un-ignore once the reader reconstructs per-shard cold columns
// and applies per-block state deltas with insertion-only trie updates.
#[ignore]
fn test_cloud_archival_reader_intermediate_state_through_missing_chunk() {
    let dropped_shard = CloudArchiveHarness::all_shard_ids()[0];
    let dropped_shard_uid = CloudArchiveHarness::all_shard_uids()[0];
    let epoch_length = CloudArchiveHarness::DEFAULT_EPOCH_LENGTH;
    // Drop the dropped_shard chunk at one mid-epoch offset; the bootstrap
    // range below is chosen wide enough to cover `dropped_height` and its
    // `+/- 1` neighbors.
    let dropped_height: BlockHeight = epoch_length / 2 + 1;
    let mut pattern = vec![true; epoch_length as usize];
    pattern[dropped_height as usize] = false;

    let mut h =
        CloudArchiveHarness::builder().validators(4).drop_chunks(dropped_shard, pattern).build();
    h.run_until_epoch(3 + MIN_GC_NUM_EPOCHS_TO_KEEP);
    let start = h.epoch_length / 2;
    let target = h.epoch_length + h.epoch_length / 2;
    h.bootstrap_reader(start, target);

    let store = h.reader_store();
    let tries = build_shard_tries(&store);

    // For each height read the block, take dropped_shard's chunk header
    // `prev_state_root` as the state at that height for that shard, and check
    // it resolves in the trie. At `dropped_height` the chunk header must be
    // a reused one.
    for h_check in [dropped_height - 1, dropped_height, dropped_height + 1] {
        let hash: CryptoHash = store
            .get_ser(DBCol::BlockHeight, &index_to_bytes(h_check))
            .expect("BlockHeight missing");
        let block: Block = store.get_ser(DBCol::Block, hash.as_ref()).unwrap();
        let chunk_header =
            block.chunks().iter_raw().find(|c| c.shard_id() == dropped_shard).cloned().unwrap();
        if h_check == dropped_height {
            assert!(
                !chunk_header.is_new_chunk(h_check),
                "chunk for shard {dropped_shard} should be missing at h={h_check}"
            );
        }
        let state_root = chunk_header.prev_state_root();
        assert!(
            has_state_root(&tries, dropped_shard_uid, state_root),
            "state unreachable for shard {dropped_shard} at h={h_check}"
        );
    }

    h.kill_reader();
    h.shutdown();
}

/// The writer archives across a resharding boundary.
/// A shard the resharding removes ends its batch at the resharding block and the
/// new child shard starts the next, while the shards and blocks that survive the
/// resharding span it in one batch, so a `BatchId` never names two batches.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_resharding_batch_boundary() {
    let mut h = CloudArchiveHarness::builder().enable_resharding().build();

    // resharding data
    let r = h.run_until_one_epoch_after_resharding();

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let shard_batch =
        |height, shard| cloud_storage.get_shard_batch_for_height(height, shard).unwrap();
    let spans_boundary = |start: BlockHeight, end: BlockHeight| {
        start <= r.resharding_block_height && end > r.resharding_block_height
    };

    assert_eq!(
        shard_batch(r.resharding_block_height, r.parent_shard).end_height(),
        r.resharding_block_height,
        "removed parent shard batch must end at the resharding block"
    );
    assert_eq!(
        shard_batch(r.resharding_block_height + 1, r.child_shard).start_height(),
        r.resharding_block_height + 1,
        "new child shard batch must start after the resharding block"
    );
    let carried_batch = shard_batch(r.resharding_block_height, r.carried_shard);
    assert!(
        spans_boundary(carried_batch.start_height(), carried_batch.end_height()),
        "carried-over shard batch must span the resharding boundary"
    );
    let block_batch = cloud_storage.get_block_batch_for_height(r.resharding_block_height).unwrap();
    assert!(
        spans_boundary(block_batch.start_height(), block_batch.end_height()),
        "block batch must span the resharding boundary"
    );

    let parent_local_head = h
        .writer_store()
        .get_ser::<BlockHeight>(DBCol::BlockMisc, &cloud_shard_head_key(r.parent_shard))
        .expect("removed parent shard head recorded");
    assert_eq!(
        parent_local_head, r.resharding_block_height,
        "removed parent's local head must match its cloud head at the resharding block"
    );

    h.shutdown();
}

/// A resharding whose block is the last height of a batch leaves the whole batch
/// in the old epoch, so the writer archives every shard for the whole batch under
/// the old layout and the new child shards begin in the next batch.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_resharding_on_batch_boundary() {
    // batch_size 1 puts every block in its own batch, so the resharding block is
    // always the end of its batch.
    let mut h = CloudArchiveHarness::builder().enable_resharding().batch_size(1).build();

    // resharding data
    let r = h.run_until_one_epoch_after_resharding();

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let shard_batch =
        |height, shard| cloud_storage.get_shard_batch_for_height(height, shard).unwrap();

    assert_eq!(
        shard_batch(r.resharding_block_height, r.parent_shard).end_height(),
        r.resharding_block_height,
        "removed parent shard batch must end at the resharding block"
    );
    assert_eq!(
        shard_batch(r.resharding_block_height + 1, r.child_shard).start_height(),
        r.resharding_block_height + 1,
        "new child shard batch must start in the next batch, after the boundary"
    );

    h.shutdown();
}

/// The writer attaches inverse state changes to the new-layout shards inside the
/// resharding gap window. At the default batch size the gap sits in the batch
/// that straddles the resharding boundary.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_resharding_inverse_deltas() {
    let mut h = CloudArchiveHarness::builder().enable_resharding().build();

    let r = h.run_until_one_epoch_after_resharding();

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let gap_batch = cloud_storage.get_block_batch_for_height(r.new_epoch_first_height).unwrap();
    assert!(
        gap_batch.start_height() <= r.resharding_block_height,
        "the gap block must share the batch that straddles the resharding boundary"
    );

    h.assert_writer_inverse_deltas(&r);

    h.shutdown();
}

/// The writer still attaches inverse state changes at `batch_size` 1, where the
/// resharding boundary and the first gap block fall in separate batches.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_writer_resharding_inverse_deltas_batch_size_1() {
    let mut h = CloudArchiveHarness::builder().enable_resharding().batch_size(1).build();

    let r = h.run_until_one_epoch_after_resharding();

    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let gap_batch = cloud_storage.get_block_batch_for_height(r.new_epoch_first_height).unwrap();
    assert!(
        gap_batch.start_height() > r.resharding_block_height,
        "the resharding boundary and the first gap block must fall in separate batches"
    );

    h.assert_writer_inverse_deltas(&r);

    h.shutdown();
}

/// A resharding epoch's state snapshot is uploaded even when the snapshot cadence
/// would otherwise skip that epoch.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_resharding_snapshot_forced_off_cadence() {
    // A cadence past every epoch this test reaches skips all snapshots except the
    // forced resharding one.
    let mut h =
        CloudArchiveHarness::builder().enable_resharding().snapshot_every_n_epochs(1000).build();

    let r = h.run_until_one_epoch_after_resharding();
    h.assert_resharding_epoch_snapshot_forced(&r);

    h.shutdown();
}

/// The resharding writer matches carried-over shards by ShardUId and reads them
/// under the new layout across the boundary. That is correct only because a
/// version-stable resharding leaves a carried shard's account-to-shard mapping
/// unchanged; listing every version forces that assumption to be re-verified when
/// a new one lands.
#[test]
fn test_cloud_archival_writer_resharding_known_shard_layout_versions() {
    match CloudArchiveHarness::default_shard_layout() {
        ShardLayout::V0(_) | ShardLayout::V1(_) | ShardLayout::V2(_) | ShardLayout::V3(_) => {}
    }
}

/// Across a resharding boundary, the cloud `BlockData` captures every
/// `DBCol::ChunkProducers` row for a block, and `save_block_data` reconstructs
/// them byte-for-byte into a fresh store. The boundary block is the last block
/// of the pre-split epoch, so its rows are keyed by the NEXT (post-split)
/// layout's shard_ids - the case that proves BlockData placement captures rows
/// the own-epoch ShardData would miss.
#[test]
#[cfg(feature = "nightly")]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_cloud_archival_reader_reconstructs_chunk_producers() {
    let mut h = CloudArchiveHarness::builder().enable_resharding().disable_gc().build();
    let r = h.run_until_one_epoch_after_resharding();

    let boundary_height = r.resharding_block_height;
    let writer = h.writer_store();
    let block_hash = writer.chain_store().get_block_hash_by_height(boundary_height).unwrap();

    // Writer's own ChunkProducers rows for the boundary block, keyed
    // block_hash||shard_id.
    let writer_rows: BTreeMap<Vec<u8>, Vec<u8>> = writer
        .iter_prefix(DBCol::ChunkProducers, block_hash.as_ref())
        .map(|(k, v)| (k.into_vec(), v.into_vec()))
        .collect();
    assert!(
        !writer_rows.is_empty(),
        "EarlyKickout must be active so the boundary block has seeded ChunkProducers rows"
    );

    // The captured shard_ids must be the post-split layout's, proving BlockData
    // placement captures next-epoch-layout rows the own-epoch ShardData would miss.
    let captured_shards: HashSet<ShardId> =
        writer_rows.keys().map(|k| get_block_shard_id_rev(k).unwrap().1).collect();
    let new_shards: HashSet<ShardId> = h.new_shard_layout().shard_ids().collect();
    let base_shards: HashSet<ShardId> = CloudArchiveHarness::all_shard_ids().into_iter().collect();
    assert_eq!(captured_shards, new_shards, "boundary rows must use the post-split layout");
    assert_ne!(captured_shards, base_shards, "post-split layout must differ from the base layout");

    // TODO(cloud_archival): once an unignored test bootstraps a reader across a resharding
    // boundary and runs assert_reader_writer_parity over the boundary block, that covers
    // ChunkProducers and the two asserts below (blob vs writer, reconstructed vs writer)
    // become redundant; drop them then.
    // The cloud blob round-trips those rows: `build_block_data` populated
    // `chunk_producers`, borsh serialized it, and `get_block_data` deserialized it.
    let cloud_storage = get_cloud_storage(&h.env, &h.archival_id);
    let block_data = cloud_storage.get_block_data(boundary_height).unwrap().unwrap();
    let blob_rows: BTreeMap<Vec<u8>, Vec<u8>> = block_data
        .chunk_producers()
        .iter()
        .map(|(shard_id, stake)| {
            (get_block_shard_id(&block_hash, *shard_id), to_vec(stake).unwrap())
        })
        .collect();
    assert_eq!(blob_rows, writer_rows, "cloud blob rows must match the writer's rows");

    // `save_block_data` writes them back byte-for-byte into a fresh store. It
    // extends the merkle chain from prev_hash, so seed a placeholder tree to
    // keep the non-genesis path from panicking; only ChunkProducers is asserted.
    let fresh = create_test_store();
    {
        let mut update = fresh.store_update();
        update.set_ser(
            DBCol::BlockMerkleTree,
            block_data.block().header().prev_hash().as_ref(),
            &PartialMerkleTree::default(),
        );
        update.commit();
    }
    save_block_data(&fresh, &block_data);
    let fresh_rows: BTreeMap<Vec<u8>, Vec<u8>> = fresh
        .iter_prefix(DBCol::ChunkProducers, block_hash.as_ref())
        .map(|(k, v)| (k.into_vec(), v.into_vec()))
        .collect();
    assert_eq!(fresh_rows, writer_rows, "reconstructed rows must match the writer byte-for-byte");

    h.shutdown();
}

/// Bootstraps a reader across a resharding boundary and asserts the reader trie
/// holds every base (old-layout) shard's state at the resharding block, and
/// every new-layout shard's state both inside the resharding gap (reached by
/// inverse walk) and at the snapshot (reached by forward replay).
#[test]
// TODO(cloud_archival): un-ignore when resharding support is implemented.
#[ignore]
fn test_cloud_archival_resharding_gap_inverse_walk() {
    let mut h = CloudArchiveHarness::builder().enable_resharding().build();

    let r = h.run_until_one_epoch_after_resharding();
    let checkpoints: [(&str, BlockHeight, &[ShardUId]); 3] = [
        ("base (old layout)", r.resharding_block_height, r.base_shard_uids.as_slice()),
        ("new layout (gap)", r.new_epoch_first_height, r.new_shard_uids.as_slice()),
        ("new layout (snapshot)", r.sync_block_height, r.new_shard_uids.as_slice()),
    ];

    h.bootstrap_reader(r.resharding_block_height, r.sync_block_height);

    let reader_tries = build_shard_tries(&h.reader_store());
    // Expected state roots are the writer's own chunk extras; each must resolve
    // to a reachable root in the reader trie.
    for (label, height, shard_uids) in checkpoints {
        let block_hash = h.writer_store().chain_store().get_block_hash_by_height(height).unwrap();
        for uid in shard_uids {
            let state_root = *h
                .writer_store()
                .chunk_store()
                .get_chunk_extra(&block_hash, uid)
                .unwrap()
                .state_root();
            assert!(
                has_state_root(&reader_tries, *uid, state_root),
                "{label} shard {uid} state at h={height} unreachable in reader trie"
            );
        }
    }

    h.kill_reader();
    h.shutdown();
}
