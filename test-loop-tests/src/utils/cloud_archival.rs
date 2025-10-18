use near_async::messaging::Handler;
use near_chain::types::Tip;
use near_client::GetBlock;
use near_client::archive::cloud_archival_writer::CloudArchivalWriterHandle;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, BlockId, BlockReference};
use near_store::adapter::StoreAdapter;
use near_store::db::CLOUD_HEAD_KEY;
use near_store::{COLD_HEAD_KEY, DBCol};

use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;

pub fn run_node_until(env: &mut TestLoopEnv, account_id: &AccountId, target_height: BlockHeight) {
    let node = TestLoopNode::for_account(&env.node_datas, account_id);
    node.run_until_head_height(&mut env.test_loop, target_height);
}

/// Sanity checks: heads alignment, GC tail bounds, and optional minimum GC progress.
pub fn gc_and_heads_sanity_checks(
    env: &TestLoopEnv,
    writer_id: &AccountId,
    split_store_enabled: bool,
    num_gced_blocks: Option<BlockHeightDelta>,
) {
    let cloud_head = get_cloud_head(&env, &writer_id);
    let archival_node = TestLoopNode::for_account(&env.node_datas, &writer_id);
    let client = archival_node.client(env.test_loop_data());
    let chain_store = client.chain.chain_store();
    let epoch_store = chain_store.epoch_store();

    // Check if the first block of the epoch containing `cloud_head` is not gc-ed.
    let cloud_head_hash = chain_store.get_block_hash_by_height(cloud_head).unwrap();
    let cloud_head_block_info = epoch_store.get_block_info(&cloud_head_hash).unwrap();
    epoch_store.get_block_info(cloud_head_block_info.epoch_first_block()).unwrap();

    let gc_tail = chain_store.tail().unwrap();
    if split_store_enabled {
        let cold_head = chain_store.store().get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY);
        let cold_head_height = cold_head.unwrap().unwrap().height;
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

/// Returns the cloud archival writer handle for `archival_id`.
fn get_writer_handle<'a>(
    env: &'a TestLoopEnv,
    writer_id: &AccountId,
) -> &'a CloudArchivalWriterHandle {
    let archival_node = TestLoopNode::for_account(&env.node_datas, writer_id);
    let writer_handle = &archival_node.data().cloud_archival_writer_handle;
    env.test_loop.data.get(writer_handle).as_ref().unwrap()
}

fn get_cloud_head(env: &TestLoopEnv, writer_id: &AccountId) -> BlockHeight {
    let archival_node = TestLoopNode::for_account(&env.node_datas, writer_id);
    let hot_store = archival_node.client(env.test_loop_data()).chain.chain_store().store();
    hot_store.get_ser::<Tip>(DBCol::BlockMisc, CLOUD_HEAD_KEY).unwrap().unwrap().height
}

pub fn test_view_client(env: &mut TestLoopEnv, archival_id: &AccountId, height: BlockHeight) {
    let archival_node = TestLoopNode::for_account(&env.node_datas, archival_id);
    let view_client_handle = archival_node.data().view_client_sender.actor_handle();
    let view_client = env.test_loop.data.get_mut(&view_client_handle);
    let block_reference = BlockReference::BlockId(BlockId::Height(height));
    let _block = view_client.handle(GetBlock(block_reference)).unwrap();
}
