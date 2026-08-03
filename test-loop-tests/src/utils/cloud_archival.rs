use crate::setup::env::TestLoopEnv;
use borsh::BorshDeserialize;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::types::Tip;
use near_chain_configs::{ClientConfig, CloudArchivalWriterConfig, TrackedShardsConfig};
use near_client::archive::cloud_archival_reader::{
    bootstrap_range, find_present_block_at_or_below, find_snapshot_at_or_before,
};
use near_client::archive::cloud_archival_writer::CloudArchivalWriterHandle;
use near_client::sync::external::{
    StateFileType, StateSyncConnection, external_storage_location, list_state_parts,
};
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::state_part::{PartId, StatePart};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, EpochId, ShardId,
};
use near_primitives::utils::{get_block_shard_id, index_to_bytes};
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::{CloudStorage, is_cloud_archive_reader_bootstrapped};
use near_store::db::{CLOUD_MIN_HEAD_KEY, CLOUD_PREV_EPOCH_END_KEY};
use near_store::flat::FlatStorageManager;
use near_store::trie::AccessOptions;
use near_store::{
    COLD_HEAD_KEY, DBCol, ShardTries, ShardUId, StateSnapshotConfig, Store, Trie, TrieConfig,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use strum::IntoEnumIterator;

#[derive(Clone)]
pub(crate) struct WriterConfig {
    pub id: AccountId,
    pub archive_block_data: bool,
    pub tracked_shards: Vec<ShardUId>,
    pub snapshot_every_n_epochs: u64,
}

pub fn run_node_until(env: &mut TestLoopEnv, account_id: &AccountId, target_height: BlockHeight) {
    env.runner_for_account(account_id).run_until_head_height(target_height);
}

/// Resharding data observed by a test.
pub struct ReshardingInfo {
    /// The last block of the epoch before the split.
    pub resharding_block_height: BlockHeight,
    /// The resharding epoch's first block, a produced block inside the gap.
    pub new_epoch_first_height: BlockHeight,
    /// The resharding epoch's sync_hash block height: the reader's snapshot
    /// anchor, above the gap the inverse walk must cover.
    pub sync_block_height: BlockHeight,
    /// Shard UIds of the pre-split layout.
    pub base_shard_uids: Vec<ShardUId>,
    /// Shard UIds of the post-split layout.
    pub new_shard_uids: Vec<ShardUId>,
    /// The shard the resharding removes.
    pub parent_shard: ShardId,
    /// One of the new child shards.
    pub child_shard: ShardId,
    /// A shard the resharding leaves unchanged.
    pub carried_shard: ShardId,
}

/// Runs the chain one epoch past the resharding to `new_layout`.
pub fn run_until_one_epoch_after_resharding(
    env: &mut TestLoopEnv,
    archival_id: &AccountId,
    base_layout: &ShardLayout,
    new_layout: &ShardLayout,
    boundary: &AccountId,
    epoch_length: BlockHeightDelta,
) -> ReshardingInfo {
    let timeout = Duration::seconds((5 * epoch_length) as i64);
    env.runner_for_account(archival_id).run_until(
        |node| {
            let epoch_id = node.head().epoch_id;
            node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap() == *new_layout
        },
        timeout,
    );
    let node = env.node_for_account(archival_id);
    let resharding_epoch_id = node.head().epoch_id;
    let epoch_manager = &node.client().epoch_manager;
    let head = node.head().last_block_hash;
    // Follow prev_hash to the old epoch's last block; it can sit below
    // epoch_start - 1 when slots before the epoch start are skipped.
    let epoch_first = *epoch_manager.get_block_info(&head).unwrap().epoch_first_block();
    let resharding_block = *epoch_manager.get_block_info(&epoch_first).unwrap().prev_hash();
    let resharding_block_height = epoch_manager.get_block_info(&resharding_block).unwrap().height();
    let new_epoch_first_height = epoch_manager.get_block_info(&epoch_first).unwrap().height();

    // Advance one epoch from the resharding epoch's first produced block so the
    // resharding epoch fully elapses and its sync_hash gets recorded.
    run_node_until(env, archival_id, new_epoch_first_height + epoch_length);

    let node = env.node_for_account(archival_id);
    let chain_store = node.client().chain.chain_store().store().chain_store();
    let sync_hash = chain_store
        .get_current_epoch_sync_hash(&resharding_epoch_id)
        .expect("resharding epoch sync_hash recorded one epoch past the split");
    let sync_block_height = chain_store.get_block_header(&sync_hash).unwrap().height();

    let base_shard_uids: Vec<ShardUId> = base_layout.shard_uids().collect();
    let new_shard_uids: Vec<ShardUId> = new_layout.shard_uids().collect();
    let parent_shard = base_layout.account_id_to_shard_id(boundary);
    let child_shard = new_layout.account_id_to_shard_id(boundary);
    let carried_shard = base_layout
        .shard_ids()
        .find(|shard_id| *shard_id != parent_shard)
        .expect("layout has a shard other than the resharding parent");
    ReshardingInfo {
        resharding_block_height,
        new_epoch_first_height,
        sync_block_height,
        base_shard_uids,
        new_shard_uids,
        parent_shard,
        child_shard,
        carried_shard,
    }
}

fn execute_future<F: Future>(fut: F) -> F::Output {
    // If this causes issues, use the testloop future spawner and wait for 0 blocks so the
    // event loop can run it.
    futures::executor::block_on(fut)
}

/// Sanity checks: heads alignment, GC tail bounds, external per-shard heads,
/// and (optional) lower bound for expected GC tail.
pub fn gc_and_heads_sanity_checks(
    env: &TestLoopEnv,
    writer_id: &AccountId,
    split_store_enabled: bool,
    num_gced_blocks: Option<BlockHeightDelta>,
) {
    let cloud_head = get_cloud_head(env, writer_id);
    let node = env.node_for_account(writer_id);
    let client = node.client();
    let chain_store = client.chain.chain_store();
    let epoch_store = chain_store.epoch_store();

    // Check if the first block of the epoch containing `cloud_head` is not gc-ed.
    let cloud_head_hash = chain_store.get_block_hash_by_height(cloud_head).unwrap();
    let cloud_head_block_info = epoch_store.get_block_info(&cloud_head_hash).unwrap();
    epoch_store.get_block_info(cloud_head_block_info.epoch_first_block()).unwrap();

    let gc_tail = chain_store.tail();
    if split_store_enabled {
        let cold_head = chain_store.store().get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY);
        let cold_head_height = cold_head.unwrap().height;
        assert!(cold_head_height > gc_tail);
    }
    assert!(cloud_head > gc_tail);
    if let Some(min_gc_tail) = num_gced_blocks {
        assert!(gc_tail >= min_gc_tail);
    } else {
        assert_eq!(gc_tail, 1);
    }

    // Check that all external per-shard heads are above gc_tail.
    let cloud_storage = get_cloud_storage(env, writer_id);
    let head = chain_store.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    for shard_id in shard_layout.shard_ids() {
        let ext_shard_head =
            execute_future(cloud_storage.retrieve_cloud_shard_head_if_exists(shard_id));
        match ext_shard_head {
            Ok(Some(shard_head)) => {
                assert!(
                    shard_head >= gc_tail,
                    "external shard head {} for shard {} is below gc_tail {}",
                    shard_head,
                    shard_id,
                    gc_tail,
                );
            }
            Ok(None) => {}
            Err(err) => {
                panic!("failed to retrieve cloud shard head for shard {}: {:?}", shard_id, err);
            }
        }
    }
}

/// Stops a node and restarts it with a new identifier `<old>-restart`.
pub(crate) fn stop_and_restart_node(env: &mut TestLoopEnv, node_identifier: &str) {
    let node_state = env.kill_node(node_identifier);
    let new_identifier = format!("{}-restart", node_identifier);
    env.restart_node(&new_identifier, node_state);
}

/// Returns the cloud archival writer handle for `archival_id`.
pub(crate) fn get_writer_handle<'a>(
    env: &'a TestLoopEnv,
    writer_id: &AccountId,
) -> &'a CloudArchivalWriterHandle {
    let node_data = env.get_node_data_by_account_id(writer_id);
    let writer_handle = &node_data.cloud_archival_writer_handle;
    env.test_loop.data.get(writer_handle).as_ref().unwrap()
}

fn get_hot_store(env: &TestLoopEnv, account_id: &AccountId) -> Store {
    let node_data = env.get_node_data_by_account_id(account_id);
    let client = &env.test_loop.data.get(&node_data.client_sender.actor_handle()).client;
    client.chain.chain_store().store()
}

pub(crate) fn get_cloud_storage(env: &TestLoopEnv, archival_id: &AccountId) -> Arc<CloudStorage> {
    let node_data = env.get_node_data_by_account_id(archival_id);
    let cloud_storage = env.test_loop.data.get(&node_data.cloud_storage_sender);
    cloud_storage.clone().unwrap()
}

/// Writer's stored min head: highest height up to which all components are
/// known archived (by us or another writer).
pub(crate) fn get_cloud_head(env: &TestLoopEnv, writer_id: &AccountId) -> BlockHeight {
    let hot_store = get_hot_store(env, writer_id);
    hot_store
        .get_ser::<BlockHeight>(DBCol::BlockMisc, CLOUD_MIN_HEAD_KEY)
        .expect("CLOUD_MIN_HEAD should exist")
}

/// Configures a client as a cloud archival writer with specific tracked shards.
pub(crate) fn apply_writer_settings(
    config: &mut ClientConfig,
    archive_block_data: bool,
    tracked_shards: &[ShardUId],
    snapshot_every_n_epochs: u64,
) {
    config.cloud_archival_writer = Some(CloudArchivalWriterConfig {
        archive_block_data,
        snapshot_every_n_epochs,
        ..Default::default()
    });
    config.tracked_shards_config = if tracked_shards.is_empty() {
        TrackedShardsConfig::NoShards
    } else {
        TrackedShardsConfig::Shards(tracked_shards.to_vec())
    };
}

/// Kills the writer, sets one shard's external head to a specific height, restarts.
pub(crate) fn simulate_lagging_shard(
    env: &mut TestLoopEnv,
    writer_id: &AccountId,
    shard_id: ShardId,
    target_height: BlockHeight,
) {
    let cloud_storage = get_cloud_storage(env, writer_id);
    let node_data = env.get_node_data_by_account_id(writer_id);
    let identifier = node_data.identifier.clone();
    let node_state = env.kill_node(&identifier);
    execute_future(cloud_storage.update_cloud_shard_head(shard_id, target_height)).unwrap();
    let new_identifier = format!("{}-restart", identifier);
    env.restart_node(&new_identifier, node_state);
}

/// Adds a new writer node mid-test.
pub(crate) fn add_writer_node(env: &mut TestLoopEnv, config: &WriterConfig) {
    let archive_block_data = config.archive_block_data;
    let tracked_shards = config.tracked_shards.clone();
    let snapshot_every_n_epochs = config.snapshot_every_n_epochs;
    let node_state = env
        .node_state_builder()
        .account_id(&config.id)
        .cloud_storage(true)
        .config_modifier(move |cfg| {
            apply_writer_settings(
                cfg,
                archive_block_data,
                &tracked_shards,
                snapshot_every_n_epochs,
            );
        })
        .build();
    env.add_node(config.id.as_ref(), node_state);
}

/// Verifies that exactly the listed shards have shard data at the given height.
/// Also checks that block data exists if any shards are expected.
pub(crate) fn check_data_at_height_for_shards(
    env: &TestLoopEnv,
    archival_id: &AccountId,
    height: BlockHeight,
    expected_shards: &[ShardId],
    all_shard_ids: &[ShardId],
) {
    let cloud_storage = get_cloud_storage(env, archival_id);
    if !expected_shards.is_empty() {
        assert!(
            matches!(cloud_storage.get_block_data(height), Ok(Some(_))),
            "block data should exist at height {height}"
        );
    }
    for shard_id in all_shard_ids {
        if expected_shards.contains(shard_id) {
            assert!(
                matches!(cloud_storage.get_shard_data(height, *shard_id), Ok(Some(_))),
                "shard data for shard {shard_id} should exist at height {height}"
            );
        } else {
            assert!(
                cloud_storage.get_shard_data(height, *shard_id).is_err(),
                "shard data for shard {shard_id} should NOT exist at height {height}"
            );
        }
    }
}

/// Queries the given node for an account and asserts the balance matches.
pub fn check_account_balance(
    env: &TestLoopEnv,
    node_id: &AccountId,
    account_id: &AccountId,
    expected_balance: Balance,
) {
    let node = env.node_for_account(node_id);
    let account_view = node.view_account_query(account_id).unwrap();
    assert_eq!(
        account_view.amount, expected_balance,
        "account {account_id} balance mismatch on node {node_id}"
    );
}

/// Checks that each epoch (except the final one) has a state header uploaded for each
/// shard and has epoch data uploaded. Panics if headers are missing for some shards
/// within an epoch or if epoch data is missing. With cadence > 1, only epoch heights
/// that are multiples of the cadence are expected to carry snapshots.
pub fn snapshots_sanity_check(
    env: &TestLoopEnv,
    archival_id: &AccountId,
    final_epoch_height: EpochHeight,
    snapshot_every_n_epochs: u64,
) {
    let store = get_hot_store(env, archival_id);
    let cloud_storage = get_cloud_storage(env, archival_id);
    let node = env.node_for_account(archival_id);
    let client = node.client();
    let mut epoch_heights_with_snapshot = HashSet::<EpochHeight>::new();
    let mut epoch_heights_with_epoch_data = HashSet::<EpochHeight>::new();
    for (epoch_id, epoch_info) in store.iter(DBCol::EpochInfo) {
        if epoch_id.as_ref() == AGGREGATOR_KEY {
            continue;
        }
        let epoch_id = EpochId::try_from_slice(epoch_id.as_ref()).unwrap();
        let epoch_info = EpochInfo::try_from_slice(epoch_info.as_ref()).unwrap();
        let epoch_height = epoch_info.epoch_height();
        let shards =
            client.epoch_manager.get_shard_layout(&epoch_id).unwrap().shard_ids().collect_vec();
        let mut num_shards_with_snapshot = 0;
        for shard_id in &shards {
            let fut = cloud_storage.retrieve_state_header(epoch_height, epoch_id, *shard_id);
            let state_header = execute_future(fut);
            if state_header.is_ok() {
                num_shards_with_snapshot += 1;
            }
        }
        if num_shards_with_snapshot == shards.len() {
            epoch_heights_with_snapshot.insert(epoch_height);
        } else if num_shards_with_snapshot > 0 {
            panic!(
                "Missing snapshots for some shards at epoch height {} (uploaded {} of {})",
                epoch_height,
                num_shards_with_snapshot,
                shards.len(),
            )
        }
        if cloud_storage.get_epoch_data(epoch_id).is_ok() {
            epoch_heights_with_epoch_data.insert(epoch_height);
        }
    }
    // Snapshots for the most recent epoch have not been uploaded yet.
    // With a cadence > 1, only epoch heights that are multiples of the cadence carry a snapshot.
    let expected_snapshots: HashSet<EpochHeight> =
        (1..final_epoch_height).filter(|h| h % snapshot_every_n_epochs == 0).collect();
    assert_eq!(epoch_heights_with_snapshot, expected_snapshots);

    // Epoch data is uploaded by the cloud archival writer at the last block of each
    // epoch, so it covers all epochs fully passed by the cloud head.
    let last_archived_epoch_last_block: CryptoHash =
        store.get_ser(DBCol::BlockMisc, CLOUD_PREV_EPOCH_END_KEY).unwrap();
    let last_archived_epoch_id =
        client.epoch_manager.get_epoch_id(&last_archived_epoch_last_block).unwrap();
    let last_archived_epoch_info = EpochInfo::try_from_slice(
        &store.get(DBCol::EpochInfo, last_archived_epoch_id.as_ref()).unwrap(),
    )
    .unwrap();
    let expected_epoch_data = HashSet::from_iter(1..=last_archived_epoch_info.epoch_height());
    assert_eq!(epoch_heights_with_epoch_data, expected_epoch_data);
}

/// Asserts inverse state changes cover the new child shards' gap blocks only,
/// with keys mirroring each block's forward changes and pre-values matching the
/// previous block's state. Carried-over and removed parent shards carry none.
pub fn assert_writer_inverse_deltas(
    env: &TestLoopEnv,
    writer_id: &AccountId,
    info: &ReshardingInfo,
) {
    let cloud_storage = get_cloud_storage(env, writer_id);
    let store = get_hot_store(env, writer_id);
    let tries = build_shard_tries(&store);

    // Inverse changes cover the gap window up to sync_prev_prev, the resharding
    // epoch's snapshot anchor; blocks above it carry none.
    let sync_hash = store.chain_store().get_block_hash_by_height(info.sync_block_height).unwrap();
    let sync_prev = *store.chain_store().get_block_header(&sync_hash).unwrap().prev_hash();
    let sync_prev_prev = *store.chain_store().get_block_header(&sync_prev).unwrap().prev_hash();
    let inverse_ceiling = store.chain_store().get_block_header(&sync_prev_prev).unwrap().height();

    // A new-layout shard that also exists in the old layout is carried over;
    // one that is new is a child of the split.
    let base_shards: HashSet<ShardUId> = info.base_shard_uids.iter().copied().collect();

    let mut checked = 0;
    for height in info.new_epoch_first_height..=info.sync_block_height {
        let Ok(block_hash) = store.chain_store().get_block_hash_by_height(height) else {
            continue;
        };
        let prev_block_hash =
            *store.chain_store().get_block_header(&block_hash).unwrap().prev_hash();
        for &shard_uid in &info.new_shard_uids {
            let shard_data = cloud_storage
                .get_shard_data(height, shard_uid.shard_id())
                .unwrap()
                .expect("gap-window shard data archived");
            let is_child = !base_shards.contains(&shard_uid);
            if !is_child || height > inverse_ceiling {
                assert!(
                    shard_data.inverse_state_changes().is_none(),
                    "only child shards below the anchor carry inverse changes \
                     (shard {shard_uid}, height {height})"
                );
                continue;
            }
            let inverse = shard_data
                .inverse_state_changes()
                .expect("child gap block carries inverse state changes");
            let forward_keys: BTreeSet<&TrieKey> =
                shard_data.state_changes().iter().map(|change| &change.trie_key).collect();
            let inverse_keys: BTreeSet<&TrieKey> = inverse.keys().collect();
            assert_eq!(
                inverse_keys, forward_keys,
                "inverse keys mirror forward keys at height {height}"
            );

            let prev_state_root = *store
                .chunk_store()
                .get_chunk_extra(&prev_block_hash, &shard_uid)
                .unwrap()
                .state_root();
            let trie = tries.get_trie_for_shard(shard_uid, prev_state_root);
            for (key, recorded_prev_value) in inverse {
                let state_prev_value = trie.get(&key.to_vec(), AccessOptions::DEFAULT).unwrap();
                assert_eq!(
                    *recorded_prev_value, state_prev_value,
                    "recorded pre-image matches state at height {height}"
                );
            }
            checked += 1;
        }
    }

    assert!(checked > 0, "no child gap block was verified; gap window is empty");

    // The removed parent shard sits below the anchor but is gone in the new
    // layout, so it carries no inverse changes at its last block.
    let parent_data = cloud_storage
        .get_shard_data(info.resharding_block_height, info.parent_shard)
        .unwrap()
        .expect("parent shard data archived at the resharding block");
    assert!(
        parent_data.inverse_state_changes().is_none(),
        "removed parent shard carries no inverse changes at the resharding block"
    );
}

/// Asserts the resharding epoch took a state snapshot for every new-layout shard
/// even though its epoch height is off the snapshot cadence.
pub fn assert_resharding_epoch_snapshot_forced(
    env: &TestLoopEnv,
    archival_id: &AccountId,
    info: &ReshardingInfo,
    snapshot_every_n_epochs: u64,
) {
    let cloud_storage = get_cloud_storage(env, archival_id);
    let store = get_hot_store(env, archival_id);
    let node = env.node_for_account(archival_id);
    let epoch_manager = &node.client().epoch_manager;

    let gap_block_hash =
        store.chain_store().get_block_hash_by_height(info.new_epoch_first_height).unwrap();
    let resharding_epoch_id = epoch_manager.get_epoch_id(&gap_block_hash).unwrap();
    let resharding_epoch_height =
        epoch_manager.get_epoch_info(&resharding_epoch_id).unwrap().epoch_height();
    assert_ne!(
        resharding_epoch_height % snapshot_every_n_epochs,
        0,
        "test needs the resharding epoch off the snapshot cadence"
    );
    for &shard_uid in &info.new_shard_uids {
        let state_header = execute_future(cloud_storage.retrieve_state_header(
            resharding_epoch_height,
            resharding_epoch_id,
            shard_uid.shard_id(),
        ));
        assert!(state_header.is_ok(), "resharding epoch snapshot forced for {shard_uid}");
    }
}

/// Bootstraps a reader node by downloading blocks from cloud and applying
/// state sync to reconstruct the state at `target_block_height`.
pub fn bootstrap_reader(
    env: &mut TestLoopEnv,
    reader_id: &AccountId,
    start_height: BlockHeight,
    target_block_height: BlockHeight,
) {
    let node_state = env.node_state_builder().account_id(reader_id).cloud_storage(true).build();
    env.add_node(reader_id.as_ref(), node_state);

    let cloud_storage = get_cloud_storage(env, reader_id);

    // Download all blocks in the range into the reader's store.
    {
        let store = env.node_for_account(reader_id).client().chain.chain_store.store();
        bootstrap_range(&store, &cloud_storage, start_height, target_block_height)
            .expect("bootstrap_range should succeed");
    }

    // Resolve the target's epoch for the shard layout. A skipped-slot target
    // carries no data, so snap it down to the nearest present block at or below
    // it (no state changes between them).
    let (target_block_height, target_block_data) =
        find_present_block_at_or_below(&cloud_storage, target_block_height).unwrap();
    let target_epoch_id = *target_block_data.block().header().epoch_id();
    let target_epoch_data = cloud_storage.get_epoch_data(target_epoch_id).unwrap();

    let chain = &env.node_for_account(reader_id).client().chain;
    let store = chain.chain_store.store();
    let tries = build_shard_tries(&store);

    // TODO(cloud_archival): support resharding; the shard layout can change
    // between the snapshot epoch and the target, which this loop assumes constant.
    for shard_id in target_epoch_data.shard_layout().shard_ids() {
        let shard_uid =
            ShardUId::from_shard_id_and_layout(shard_id, target_epoch_data.shard_layout());

        // Reconstruct from the nearest snapshot at or below the bootstrap start,
        // loaded from cloud state parts, then apply deltas forward to the target.
        let (snapshot_epoch_height, snapshot_epoch_id) =
            find_snapshot_at_or_before(&cloud_storage, start_height, shard_id).unwrap();
        let snapshot_epoch_data = cloud_storage.get_epoch_data(snapshot_epoch_id).unwrap();
        let sync_block =
            cloud_storage.get_block_data(snapshot_epoch_data.sync_block_height()).unwrap().unwrap();
        let sync_prev_block_height = sync_block.block().header().prev_height().unwrap();
        let state_sync_state_root = cloud_storage
            .get_state_header(snapshot_epoch_height, snapshot_epoch_id, shard_id)
            .unwrap()
            .chunk_prev_state_root();

        assert!(!has_state_root(&tries, shard_uid, state_sync_state_root));
        execute_future(download_and_apply_state_snapshot(
            &tries,
            &cloud_storage,
            &snapshot_epoch_id,
            snapshot_epoch_height,
            shard_uid,
            state_sync_state_root,
        ));
        assert!(has_state_root(&tries, shard_uid, state_sync_state_root));

        let target_state_root = *cloud_storage
            .get_shard_data(target_block_height, shard_id)
            .unwrap()
            .unwrap()
            .chunk_extra()
            .state_root();
        assert!(!has_state_root(&tries, shard_uid, target_state_root));
        apply_state_changes(
            &cloud_storage,
            &store,
            &tries,
            state_sync_state_root,
            sync_prev_block_height,
            target_block_height,
            shard_uid,
        );
        assert!(has_state_root(&tries, shard_uid, target_state_root));

        // Validate the restored state by reading from the trie.
        let trie = tries.get_trie_for_shard(shard_uid, target_state_root);
        let item_count = trie.disk_iter().unwrap().count();
        assert!(item_count > 0, "trie for shard {shard_id} should not be empty after bootstrap");
    }
}

/// Builds a `ShardTries` over `store` with state snapshots disabled.
pub(crate) fn build_shard_tries(store: &Store) -> ShardTries {
    ShardTries::new(
        store.trie_store(),
        TrieConfig::default(),
        FlatStorageManager::new(store.flat_store()),
        StateSnapshotConfig::Disabled,
    )
}

/// Whether `state_root` resolves to a reachable root node in `shard_uid`'s trie.
pub(crate) fn has_state_root(
    tries: &ShardTries,
    shard_uid: ShardUId,
    state_root: CryptoHash,
) -> bool {
    let trie = tries.get_trie_for_shard(shard_uid, state_root);
    trie.retrieve_root_node().is_ok()
}

/// Loads a shard's state snapshot by downloading its state parts from cloud and
/// applying them straight into the trie.
async fn download_and_apply_state_snapshot(
    tries: &ShardTries,
    cloud_storage: &CloudStorage,
    epoch_id: &EpochId,
    epoch_height: EpochHeight,
    shard_uid: ShardUId,
    state_root: CryptoHash,
) {
    let shard_id = shard_uid.shard_id();
    let connection = StateSyncConnection::from_cloud_storage(cloud_storage);
    let chain_id = cloud_storage.chain_id();
    let num_parts =
        list_state_parts(&connection, chain_id, epoch_id, epoch_height, shard_id).await.unwrap();
    for part_id in 0..num_parts {
        let file_type = StateFileType::StatePart { part_id, num_parts };
        let location =
            external_storage_location(chain_id, epoch_id, epoch_height, shard_id, &file_type);
        let bytes = connection.get_file(shard_id, &location, &file_type).await.unwrap();
        let partial_state = StatePart::from_bytes(bytes).unwrap().to_partial_state().unwrap();
        let apply_result =
            Trie::apply_state_part(&state_root, PartId::new(part_id, num_parts), partial_state);
        let mut store_update = tries.store_update();
        tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
        store_update.commit();
    }
}

/// Applies per-block state deltas from cloud storage to advance the trie from
/// `start_block_height` to `target_block_height`. Both endpoints must be
/// present; `start_block_height` must additionally have a new chunk for
/// `shard_uid` (initial `state_root` is verified against the chunk's
/// `prev_state_root`).
fn apply_state_changes(
    cloud_storage: &CloudStorage,
    store: &Store,
    tries: &ShardTries,
    mut state_root: CryptoHash,
    start_block_height: BlockHeight,
    target_block_height: BlockHeight,
    shard_uid: ShardUId,
) {
    let shard_id = shard_uid.shard_id();
    let start_block_shard_data =
        cloud_storage.get_shard_data(start_block_height, shard_id).unwrap().unwrap();
    assert_eq!(
        state_root,
        start_block_shard_data.chunk().unwrap().prev_state_root(),
        "initial state_root must match prev_state_root of the start block"
    );
    for block_height in start_block_height..=target_block_height {
        let Some(shard_data) = cloud_storage.get_shard_data(block_height, shard_id).unwrap() else {
            continue;
        };
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        let trie_changes = trie
            .update(
                shard_data.state_changes().iter().map(|raw_state_changes_with_trie_key| {
                    let raw_key = raw_state_changes_with_trie_key.trie_key.to_vec();
                    // Take the final value — each key may have multiple changes within a block.
                    let data = raw_state_changes_with_trie_key.changes.last().unwrap().data.clone();
                    (raw_key, data)
                }),
                AccessOptions::NO_SIDE_EFFECTS,
            )
            .unwrap();
        let mut store_update = store.trie_store().store_update();
        state_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();
        assert!(has_state_root(&tries, shard_uid, state_root));
    }
    let target_block_shard_data =
        cloud_storage.get_shard_data(target_block_height, shard_id).unwrap().unwrap();
    let expected_final_state_root = target_block_shard_data.chunk_extra().state_root();
    assert_eq!(state_root, *expected_final_state_root);
}

/// How the reader is checked against a set of writer rows.
enum Parity {
    /// Reader must equal the given writer rows exactly, holding no extra rows.
    Equality,
    /// Reader must contain every given writer row, but may hold extra rows
    /// (e.g. blocks backfilled below `start` to complete the merkle tree chain).
    Containment,
}

/// Asserts the reader reproduces the writer's rows over `[start, end]` for every
/// column the cloud-bootstrapped reader reconstructs today. Caller must
/// `.disable_gc()` so the writer retains the bootstrap range.
pub(crate) fn assert_reader_writer_parity(
    reader: &Store,
    writer: &Store,
    start: BlockHeight,
    end: BlockHeight,
) {
    // TODO(cloud_archival): compare the skipped columns too.
    let cols: Vec<DBCol> = DBCol::iter()
        .filter(|&c| {
            is_cloud_archive_reader_bootstrapped(c)
                && !matches!(
                    c,
                    // Not reconstructed yet.
                    DBCol::NextBlockHashes
                        | DBCol::BlockPerHeight
                        | DBCol::ChunkHashesByHeight
                        | DBCol::ChunkExtra
                        | DBCol::IncomingReceipts
                        | DBCol::OutcomeIds
                        | DBCol::TransactionResultForBlock
                        | DBCol::StateChanges
                        | DBCol::Transactions
                        | DBCol::Receipts
                        | DBCol::ReceiptToTx
                        | DBCol::State
                        // Reconstructed, but keyed off-height (genesis BlockInfo under
                        // CryptoHash::default(), EpochInfo under AGGREGATOR_KEY), so the
                        // height walk can't reproduce their key sets.
                        | DBCol::BlockInfo
                        | DBCol::EpochInfo
                        | DBCol::EpochStart
                )
        })
        .collect();

    let writer_kvs = writer_kvs(writer, &cols, start, end);

    for &col in &cols {
        let parity = match col {
            // The reader backfills these columns below `start` to complete the merkle
            // tree chain, so it holds extra rows: check containment, not equality.
            DBCol::BlockHeight | DBCol::Block | DBCol::BlockHeader | DBCol::BlockMerkleTree => {
                Parity::Containment
            }
            #[cfg(feature = "nightly")]
            DBCol::ChunkProducers => Parity::Containment,
            _ => Parity::Equality,
        };
        assert_keyed_parity(reader, col, &writer_kvs[&col], parity);
    }
}

/// Compares the writer's rows in `col` against the full reader.
fn assert_keyed_parity(
    reader: &Store,
    col: DBCol,
    writer_kvs: &BTreeMap<Vec<u8>, Vec<u8>>,
    parity: Parity,
) {
    let reader_all_kvs: BTreeMap<Vec<u8>, Vec<u8>> =
        reader.iter(col).map(|(k, v)| (k.into_vec(), v.into_vec())).collect();
    match parity {
        Parity::Equality => {
            assert_eq!(&reader_all_kvs, writer_kvs, "{col} parity mismatch")
        }
        Parity::Containment => {
            for (key, writer_value) in writer_kvs {
                assert_eq!(
                    reader_all_kvs.get(key),
                    Some(writer_value),
                    "{col}: writer key {key:?} missing or different at reader"
                );
            }
        }
    }
}

/// Collects the writer's per-(block, shard) column rows for one chunk into `kvs`.
fn collect_chunk_kvs(
    writer: &Store,
    kvs: &mut HashMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>>,
    block_hash: &CryptoHash,
    chunk_header: &ShardChunkHeader,
    height: BlockHeight,
) {
    let block_shard_id = get_block_shard_id(block_hash, chunk_header.shard_id());
    if chunk_header.is_new_chunk(height) {
        let chunk_hash = chunk_header.chunk_hash().as_ref().to_vec();
        let value = writer.get(DBCol::Chunks, &chunk_hash).unwrap();
        kvs.get_mut(&DBCol::Chunks).unwrap().insert(chunk_hash, value.to_vec());
        if let Some(value) = writer.get(DBCol::OutgoingReceipts, &block_shard_id) {
            kvs.get_mut(&DBCol::OutgoingReceipts)
                .unwrap()
                .insert(block_shard_id.clone(), value.to_vec());
        }
    }
    if let Some(value) = writer.get(DBCol::ChunkApplyStats, &block_shard_id) {
        kvs.get_mut(&DBCol::ChunkApplyStats).unwrap().insert(block_shard_id, value.to_vec());
    }
}

/// Returns the writer rows the reader is expected to hold over `[start, end]`,
/// plus the genesis block, one map per column in `cols`.
fn writer_kvs(
    writer: &Store,
    cols: &[DBCol],
    start: BlockHeight,
    end: BlockHeight,
) -> HashMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>> {
    let writer_store = writer.chain_store();
    let chain_head = writer_store.head().unwrap().height;
    let genesis_height = writer_store.get_genesis_height();

    let mut in_scope: HashMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>> =
        cols.iter().map(|&c| (c, BTreeMap::new())).collect();
    let mut out_of_scope: HashMap<DBCol, BTreeMap<Vec<u8>, Vec<u8>>> =
        cols.iter().map(|&c| (c, BTreeMap::new())).collect();

    // Walk the chain, reading each column's row at height `h` into the in-scope
    // or the out-of-scope map. Genesis is in scope wherever it sits.
    for h in 0..=chain_head {
        let Ok(block_hash) = writer_store.get_block_hash_by_height(h) else {
            continue;
        };
        let is_in_scope = (start..=end).contains(&h) || h == genesis_height;
        let kvs = if is_in_scope { &mut in_scope } else { &mut out_of_scope };
        let height_key = index_to_bytes(h).to_vec();
        if let Some(value) = writer.get(DBCol::BlockHeight, &height_key) {
            kvs.get_mut(&DBCol::BlockHeight).unwrap().insert(height_key, value.to_vec());
        }
        for col in [DBCol::Block, DBCol::BlockHeader, DBCol::BlockMerkleTree] {
            let key = block_hash.as_ref().to_vec();
            if let Some(value) = writer.get(col, &key) {
                kvs.get_mut(&col).unwrap().insert(key, value.to_vec());
            }
        }
        // ChunkProducers rows are keyed by block hash across all shards of the
        // next epoch, so one prefix scan captures every row for this block.
        #[cfg(feature = "nightly")]
        for (key, value) in writer.iter_prefix(DBCol::ChunkProducers, block_hash.as_ref()) {
            kvs.get_mut(&DBCol::ChunkProducers).unwrap().insert(key.into_vec(), value.into_vec());
        }
        let block = writer_store.get_block(&block_hash).expect("block exists, checked above");
        for chunk_header in block.chunks().iter_raw() {
            collect_chunk_kvs(writer, kvs, &block_hash, chunk_header, h);
        }
    }

    // TODO(cloud_archival): add a negative test (follow-up PR) that tampers a
    // reader row and confirms these checks catch it.
    // The walk must reproduce each writer column exactly, keys and values.
    for &col in cols {
        let writer_all: BTreeMap<Vec<u8>, Vec<u8>> =
            writer.iter(col).map(|(k, v)| (k.into_vec(), v.into_vec())).collect();
        let mut seen = in_scope[&col].clone();
        seen.extend(out_of_scope[&col].clone());
        assert_eq!(seen, writer_all, "{col}: walk did not reproduce writer's column");
        assert!(
            in_scope[&col].keys().all(|k| !out_of_scope[&col].contains_key(k)),
            "{col}: key in both in-scope and out-of-scope",
        );
    }

    in_scope
}
