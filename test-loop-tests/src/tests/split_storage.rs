use near_async::messaging::Handler;
use near_async::time::Duration;
use near_client::GetSplitStorageInfo;
use near_o11y::testonly::init_test_logger;

use crate::setup::builder::{ArchivalKind, TestLoopBuilder};

/// Tests that an archival node with cold storage (split storage) has its cold
/// head advancing close to the final head. This is a migration of the pytest
/// `split_storage.py::step1_base_case_test`.
#[test]
fn test_split_storage_cold_head_advances() {
    init_test_logger();

    let epoch_length = 5;
    let gc_num_epochs_to_keep = 3;
    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .enable_archival_node(ArchivalKind::Cold)
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .build()
        .warmup();

    // Run long enough for cold storage migration to have meaningful work.
    let target_height = epoch_length * (gc_num_epochs_to_keep + 2);
    env.archival_runner().run_until_head_height(target_height);

    // Query split storage info from the archival node's view client.
    let info = env.archival_node_mut().view_client_actor().handle(GetSplitStorageInfo {});
    let info = info.unwrap();

    // The hot DB should be of kind "Hot" when cold storage is configured.
    assert_eq!(info.hot_db_kind.as_deref(), Some("Hot"), "hot_db_kind should be Hot");

    // Head and final head should have advanced to at least our target.
    let head_height = info.head_height.expect("head_height should be set");
    let final_head_height = info.final_head_height.expect("final_head_height should be set");
    assert!(head_height >= target_height, "head should be at least {target_height}");
    assert!(final_head_height >= target_height - 10, "final head should be close to target");

    // Cold head should exist and be reasonably close to final head.
    let cold_head_height = info.cold_head_height.expect("cold_head_height should be set");
    assert!(
        cold_head_height >= final_head_height - 10,
        "cold head ({cold_head_height}) should be close to final head ({final_head_height})"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that a killed-and-restarted archival node with cold storage can
/// catch up with the rest of the network. Inspired by the pytest
/// `split_storage.py::step2_archival_node_sync_test`.
///
/// 1. Kill the archival node after a few blocks.
/// 2. Let validators advance the chain.
/// 3. Restart the archival node and verify it catches up.
#[test]
fn test_split_storage_archival_node_restart() {
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .epoch_length(epoch_length)
        .enable_archival_node(ArchivalKind::Cold)
        .gc_num_epochs_to_keep(20)
        .build()
        .warmup();

    // Let the archival node progress a bit.
    let kill_height = 2 * epoch_length;
    env.archival_runner().run_until_head_height(kill_height);

    // Kill the archival node.
    let archival_data = &env.node_datas.last().unwrap();
    let archival_identifier = archival_data.identifier.clone();
    let archival_account = archival_data.account_id.clone();
    let killed_state = env.kill_node(&archival_identifier);

    // Let validators advance the chain while archival is down.
    let restart_height = kill_height + 2 * epoch_length;
    env.node_runner(0).run_until_head_height(restart_height);

    // Restart the archival node.
    let new_identifier = format!("{}-restart", archival_identifier);
    env.restart_node(&new_identifier, killed_state);

    // Give the restarted node time to catch up.
    env.node_runner(0).run_for_number_of_blocks(15);

    let archival_height = env.node_for_account(&archival_account).head().height;
    assert!(
        archival_height > restart_height,
        "restarted archival node should advance past restart height \
         ({archival_height} vs {restart_height})"
    );

    // Verify cold storage is still functional after restart.
    let info = env
        .node_for_account_mut(&archival_account)
        .view_client_actor()
        .handle(GetSplitStorageInfo {});
    let info = info.unwrap();
    assert_eq!(info.hot_db_kind.as_deref(), Some("Hot"));
    assert!(info.cold_head_height.is_some(), "cold head should be set after restart");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
