use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use borsh::BorshDeserialize;

use itertools::Itertools;
use near_chain::types::Tip;
use near_chain::{Chain, ChainStoreAccess, ChainStoreUpdate};
use near_client::archive::cloud_archival_writer::CloudArchivalWriterHandle;
use near_client::sync::external::{
    StateSyncConnection, download_and_apply_state_parts_sequentially, list_state_parts,
};
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, EpochId, ShardId,
};
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::{BlockData, CloudStorage};
use near_store::db::CLOUD_HEAD_KEY;
use near_store::flat::FlatStorageManager;
use near_store::trie::AccessOptions;
use near_store::{
    COLD_HEAD_KEY, DBCol, ShardTries, ShardUId, StateSnapshotConfig, Store, TrieConfig,
};
use near_vm_runner::logic::ProtocolVersion;

use crate::setup::env::TestLoopEnv;

pub fn run_node_until(env: &mut TestLoopEnv, account_id: &AccountId, target_height: BlockHeight) {
    env.runner_for_account(account_id).run_until_head_height(target_height);
}

fn execute_future<F: Future>(fut: F) -> F::Output {
    // If this causes issues, use the testloop future spawner and wait for 0 blocks so the
    // event loop can run it.
    futures::executor::block_on(fut)
}

/// Sanity checks: heads alignment, GC tail bounds, and (optional) lower bound for expected GC tail.
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
}

/// Pauses the archival writer, runs sanity checks, then resumes it.
pub fn pause_and_resume_writer_with_sanity_checks(
    mut env: &mut TestLoopEnv,
    resume_height: BlockHeight,
    epoch_length: BlockHeightDelta,
    writer_id: &AccountId,
    split_store_enabled: bool,
) {
    // Run the node so that the cloud head advances a bit, but remains within the first epoch.
    run_node_until(&mut env, &writer_id, epoch_length);
    let cloud_head = get_cloud_head(&env, &writer_id);
    assert!(2 < cloud_head && cloud_head + 1 < epoch_length);

    // Stop the writer and let the node reach `resume_height` while the writer is paused.
    get_writer_handle(&env, &writer_id).0.stop();
    let node_data = env.get_node_data_by_account_id(&writer_id);
    let node_identifier = node_data.identifier.clone();
    env.runner_for_account(&writer_id).run_until_head_height(resume_height);

    // Run sanity checks.
    gc_and_heads_sanity_checks(env, writer_id, split_store_enabled, None);

    // Resume the writer and restart the node.
    get_writer_handle(&env, &writer_id).0.resume();
    stop_and_restart_node(&mut env, node_identifier.as_str());
}

/// Stops a node and restarts it with a new identifier `<old>-restart`.
fn stop_and_restart_node(env: &mut TestLoopEnv, node_identifier: &str) {
    let node_state = env.kill_node(node_identifier);
    let new_identifier = format!("{}-restart", node_identifier);
    env.restart_node(&new_identifier, node_state);
}

/// Returns the cloud archival writer handle for `archival_id`.
fn get_writer_handle<'a>(
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

fn get_cloud_storage(env: &TestLoopEnv, archival_id: &AccountId) -> Arc<CloudStorage> {
    let node_data = env.get_node_data_by_account_id(archival_id);
    let cloud_storage = env.test_loop.data.get(&node_data.cloud_storage_sender);
    cloud_storage.clone().unwrap()
}

fn get_cloud_head(env: &TestLoopEnv, writer_id: &AccountId) -> BlockHeight {
    let hot_store = get_hot_store(env, writer_id);
    hot_store.get_ser::<Tip>(DBCol::BlockMisc, CLOUD_HEAD_KEY).unwrap().height
}

/// Runs tests to verify that data for the block height exists in the archive.
pub fn check_data_at_height(env: &TestLoopEnv, archival_id: &AccountId, height: BlockHeight) {
    let cloud_storage = get_cloud_storage(env, archival_id);
    let block_data = cloud_storage.get_block_data(height).unwrap();
    for chunk_header in block_data.block().chunks().iter() {
        let shard_id = chunk_header.shard_id();
        let _shard_data = cloud_storage.get_shard_data(height, shard_id).unwrap();
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
/// within an epoch or if epoch data is missing.
pub fn snapshots_sanity_check(
    env: &TestLoopEnv,
    archival_id: &AccountId,
    final_epoch_height: EpochHeight,
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
    let expected_snapshots = HashSet::from_iter(1..final_epoch_height);
    assert_eq!(epoch_heights_with_snapshot, expected_snapshots);

    // Epoch data is uploaded by the cloud archival writer at the last block of each
    // epoch, so it covers all epochs fully passed by the cloud head.
    let cloud_head_tip: Tip =
        store.get_ser(DBCol::BlockMisc, CLOUD_HEAD_KEY).expect("cloud head should exist");
    let cloud_head_epoch_info = EpochInfo::try_from_slice(
        &store.get(DBCol::EpochInfo, cloud_head_tip.epoch_id.as_ref()).unwrap(),
    )
    .unwrap();
    let expected_epoch_data = HashSet::from_iter(1..cloud_head_epoch_info.epoch_height());
    assert_eq!(epoch_heights_with_epoch_data, expected_epoch_data);
}

/// Saves block header, block body, and block info from cloud archival data into the local store.
fn save_block_data(mut chain_store_update: ChainStoreUpdate, block_data: &BlockData) {
    let block = Arc::new(block_data.block().clone());
    let mut epoch_store_update = chain_store_update.store().epoch_store().store_update();
    chain_store_update.save_block_header(block.header().clone()).unwrap();
    chain_store_update.save_block(block);
    chain_store_update.commit().unwrap();

    epoch_store_update.set_block_info(block_data.block_info());
    epoch_store_update.commit();
}

/// Bootstraps a reader node to match the state at `target_block_height` by:
/// 1. Loading epoch data and required blocks from cloud storage.
/// 2. Applying state sync parts to reconstruct the state at the epoch boundary.
/// 3. Applying per-block state deltas to advance from the sync point to the target height.
pub fn bootstrap_reader_at_height(
    env: &mut TestLoopEnv,
    reader_id: &AccountId,
    target_block_height: BlockHeight,
) {
    let node_state = env.node_state_builder().account_id(reader_id).cloud_storage(true).build();
    env.add_node(reader_id.as_ref(), node_state);

    let cloud_storage = get_cloud_storage(env, reader_id);
    let target_block_data = cloud_storage.get_block_data(target_block_height).unwrap();
    let epoch_id = target_block_data.block().header().epoch_id();
    let epoch_data = cloud_storage.get_epoch_data(*epoch_id).unwrap();
    let epoch_height = epoch_data.epoch_info().epoch_height();
    let protocol_version = epoch_data.epoch_info().protocol_version();

    // Load blocks needed for state sync (shard-independent).
    let epoch_start_block = cloud_storage.get_block_data(epoch_data.epoch_start_height()).unwrap();
    let sync_block = cloud_storage.get_block_data(epoch_data.sync_block_height()).unwrap();
    let sync_hash = sync_block.block().hash();
    let sync_prev_block_height = sync_block.block().header().prev_height().unwrap();
    let sync_prev_block = cloud_storage.get_block_data(sync_prev_block_height).unwrap();
    let sync_prev_prev_block_height = sync_prev_block.block().header().prev_height().unwrap();
    let sync_prev_prev_block = cloud_storage.get_block_data(sync_prev_prev_block_height).unwrap();

    // Save blocks and merkle tree to the reader's store.
    {
        let mut node = env.node_for_account_mut(&reader_id);
        let chain = &mut node.client_actor().client.chain;
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_merkle_tree(
            *epoch_start_block.block().header().prev_hash(),
            epoch_data.epoch_start_prev_block_merkle_tree().clone(),
        );
        store_update.commit().unwrap();
        for block_data in [&epoch_start_block, &sync_prev_prev_block, &sync_prev_block, &sync_block]
        {
            save_block_data(chain.mut_chain_store().store_update(), block_data);
        }
    }

    let chain = &env.node_for_account(reader_id).client().chain;
    let store = chain.chain_store.store();
    let tries = ShardTries::new(
        store.trie_store(),
        TrieConfig::default(),
        FlatStorageManager::new(store.flat_store()),
        StateSnapshotConfig::Disabled,
    );
    let state_sync_connection = StateSyncConnection::from_cloud_storage(&cloud_storage);

    for shard_id in epoch_data.shard_layout().shard_ids() {
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, epoch_data.shard_layout());
        let target_block_shard_data =
            cloud_storage.get_shard_data(target_block_height, shard_id).unwrap();
        let target_state_root = *target_block_shard_data.chunk_extra().state_root();
        let state_header =
            cloud_storage.get_state_header(epoch_height, *epoch_id, shard_id).unwrap();
        let state_sync_state_root = state_header.chunk_prev_state_root();

        chain.state_sync_adapter.set_state_header(shard_id, *sync_hash, state_header).unwrap();

        assert!(!has_state_root(&tries, shard_uid, state_sync_state_root));
        execute_future(load_state_snapshot(
            chain,
            &state_sync_connection,
            cloud_storage.chain_id(),
            epoch_id,
            epoch_height,
            *sync_hash,
            &protocol_version,
            shard_id,
            state_sync_state_root,
        ));
        assert!(has_state_root(&tries, shard_uid, state_sync_state_root));

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

fn has_state_root(tries: &ShardTries, shard_uid: ShardUId, state_root: CryptoHash) -> bool {
    let trie = tries.get_trie_for_shard(shard_uid, state_root);
    trie.retrieve_root_node().is_ok()
}

async fn load_state_snapshot(
    chain: &Chain,
    external: &StateSyncConnection,
    chain_id: &str,
    epoch_id: &EpochId,
    epoch_height: EpochHeight,
    sync_hash: CryptoHash,
    protocol_version: &ProtocolVersion,
    shard_id: ShardId,
    state_root: CryptoHash,
) {
    let num_parts =
        list_state_parts(external, chain_id, epoch_id, epoch_height, shard_id).await.unwrap();
    download_and_apply_state_parts_sequentially(
        chain,
        external,
        chain_id,
        epoch_id,
        epoch_height,
        sync_hash,
        *protocol_version,
        shard_id,
        state_root,
        0..num_parts,
        num_parts,
    )
    .await
    .unwrap();
}

/// Applies per-block state deltas from cloud storage to advance the trie from
/// `start_block_height` to `target_block_height`.
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
        cloud_storage.get_shard_data(start_block_height, shard_id).unwrap();
    assert_eq!(
        state_root,
        start_block_shard_data.chunk().prev_state_root(),
        "initial state_root must match prev_state_root of the start block"
    );
    for block_height in start_block_height..=target_block_height {
        let shard_data = cloud_storage.get_shard_data(block_height, shard_id).unwrap();
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
        cloud_storage.get_shard_data(target_block_height, shard_id).unwrap();
    let expected_final_state_root = target_block_shard_data.chunk_extra().state_root();
    assert_eq!(state_root, *expected_final_state_root);
}
