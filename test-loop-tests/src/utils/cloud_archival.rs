use std::collections::HashSet;
use std::sync::Arc;

use borsh::BorshDeserialize;

use itertools::Itertools;
use near_chain::types::Tip;
use near_client::Client;
use near_client::archive::cloud_archival_writer::CloudArchivalWriterHandle;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, EpochHeight, EpochId};
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::db::CLOUD_HEAD_KEY;
use near_store::{COLD_HEAD_KEY, DBCol, Store};

use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;

pub fn run_node_until(env: &mut TestLoopEnv, account_id: &AccountId, target_height: BlockHeight) {
    let node = TestLoopNode::for_account(&env.node_datas, account_id);
    node.run_until_head_height(&mut env.test_loop, target_height);
}

fn execute_future<F: Future>(fut: F) -> F::Output {
    // If this causes issues, use the testloop future spawner and wait for 0 blocks so the
    // event loop can run it.
    futures::executor::block_on(fut)
}

/// Sanity checks: heads alignment, GC tail bounds, and optional minimum GC progress.
pub fn gc_and_heads_sanity_checks(
    env: &TestLoopEnv,
    writer_id: &AccountId,
    split_store_enabled: bool,
    num_gced_blocks: Option<BlockHeightDelta>,
) {
    let cloud_head = get_cloud_head(&env, &writer_id);
    let client = get_client(env, writer_id);
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
    let node_identifier = {
        let archival_node = TestLoopNode::for_account(&env.node_datas, &writer_id);
        archival_node.run_until_head_height(&mut env.test_loop, resume_height);
        archival_node.data().identifier.clone()
    };

    // Run sanity checks.
    gc_and_heads_sanity_checks(&env, &writer_id, split_store_enabled, None);

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

fn get_client<'a>(env: &'a TestLoopEnv, account_id: &'a AccountId) -> &'a Client {
    let archival_node = TestLoopNode::for_account(&env.node_datas, &account_id);
    archival_node.client(env.test_loop_data())
}

/// Returns the cloud archival writer handle for `archival_id`.
fn get_writer_handle<'a>(
    env: &'a TestLoopEnv,
    writer_id: &AccountId,
) -> &'a CloudArchivalWriterHandle {
    let archival_node = TestLoopNode::for_account(&env.node_datas, writer_id);
    let writer_handle = &archival_node.data().cloud_archival_writer_handle;
    env.test_loop.data.get(writer_handle).as_ref().unwrap()
}

fn get_hot_store(env: &TestLoopEnv, account_id: &AccountId) -> Store {
    let node = TestLoopNode::for_account(&env.node_datas, account_id);
    node.client(env.test_loop_data()).chain.chain_store().store()
}

fn get_cloud_storage(env: &TestLoopEnv, archival_id: &AccountId) -> Arc<CloudStorage> {
    let archival_node = TestLoopNode::for_account(&env.node_datas, archival_id);
    let cloud_storage_handle = &archival_node.data().cloud_storage_sender;
    let cloud_storage = env.test_loop.data.get(&cloud_storage_handle);
    cloud_storage.clone().unwrap()
}

fn get_cloud_head(env: &TestLoopEnv, writer_id: &AccountId) -> BlockHeight {
    let hot_store = get_hot_store(env, writer_id);
    hot_store.get_ser::<Tip>(DBCol::BlockMisc, CLOUD_HEAD_KEY).unwrap().height
}

/// Runs tests verifying that data for the block height exists in the archive,
pub fn check_data_at_height(env: &TestLoopEnv, archival_id: &AccountId, height: BlockHeight) {
    let cloud_storage = get_cloud_storage(env, archival_id);
    let block_data = cloud_storage.get_block_data(height).unwrap();
    for chunk_header in block_data.block().chunks().iter() {
        let shard_id = chunk_header.shard_id();
        let _shard_data = cloud_storage.get_shard_data(height, shard_id).unwrap();
    }
}

/// Checks that each epoch (except the final one) has a state header uploaded for each
/// shards. Panics if headers are missing for some shards within an epoch.
pub fn snapshots_sanity_check(
    env: &TestLoopEnv,
    archival_id: &AccountId,
    final_epoch_height: EpochHeight,
) {
    let store = get_hot_store(env, archival_id);
    let cloud_storage = get_cloud_storage(env, archival_id);
    let client = get_client(env, archival_id);
    let mut epoch_heights_with_snapshot = HashSet::<EpochHeight>::new();
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
    }
    // Snapshots for the most recent epoch have not been uploaded yet.
    assert_eq!(epoch_heights_with_snapshot, HashSet::from_iter(1..final_epoch_height));
}
