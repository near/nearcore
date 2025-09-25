use near_chain::types::Tip;
use near_client::archive::cloud_archival_actor::CloudArchivalActor;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};
use near_store::adapter::StoreAdapter;
use near_store::db::CLOUD_HEAD_KEY;
use near_store::{DBCol, COLD_HEAD_KEY};

use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;

pub(crate) fn stop_and_restart_node(env: &mut TestLoopEnv, node_identifier: &str) {
    let node_state = env.kill_node(node_identifier);
    let new_identifier = format!("{}-restart", node_identifier);
    env.restart_node(&new_identifier, node_state);
}

pub(crate) fn get_cloud_writer<'a>(env: &'a TestLoopEnv, archival_id: &AccountId) -> &'a CloudArchivalActor {
    let archival_node = TestLoopNode::for_account(&env.node_datas, archival_id);
    let writer_testloop_handle =
        archival_node.data().cloud_archival_sender.as_ref().unwrap().actor_handle();
    env.test_loop.data.get(&writer_testloop_handle)
}

pub(crate) fn gc_and_heads_sanity_checks(
    env: &mut TestLoopEnv,
    archival_id: &AccountId,
    split_store_enabled: bool,
    num_gced_blocks: Option<BlockHeightDelta>,
) {
    let cloud_head = get_cloud_writer(&env, &archival_id).get_cloud_head();
    let archival_node = TestLoopNode::for_account(&env.node_datas, &archival_id);
    let chain_store = archival_node.client(env.test_loop_data()).chain.chain_store();
    let gc_tail = chain_store.tail().unwrap();

    if split_store_enabled {
        let cold_head = chain_store.store().get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY);
        let cold_head_height = cold_head.unwrap().unwrap().height;
        assert!(cold_head_height > gc_tail);
    }
    let stored_cloud_head = chain_store
        .store()
        .get_ser::<BlockHeight>(DBCol::BlockMisc, CLOUD_HEAD_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(cloud_head, stored_cloud_head);
    assert!(cloud_head > gc_tail);
    if let Some(min_gc_tail) = num_gced_blocks {
        assert!(gc_tail >= min_gc_tail);
    } else {
        assert_eq!(gc_tail, 1);
    }
}
