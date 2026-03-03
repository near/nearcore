use assert_matches::assert_matches;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client::{GetBlock, GetSplitStorageInfo};
use near_client_primitives::types::GetBlockError;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{BlockId, BlockReference};

use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::utils::account::create_account_id;

/// Tests that an archival node with cold storage (split storage) has its cold
/// head advancing close to the final head.
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

/// Tests that a killed-and-restarted archival node with cold storage can catch
/// up from another archival node when the chain has advanced past the GC period.
///
/// 1. Set up a validator, the primary archival node, and a second archival node.
/// 2. Kill the second archival node after a few blocks.
/// 3. Advance the chain well past the GC period so the validator GCs old blocks.
/// 4. Restart the second archival and verify it catches up from the primary one.
#[test]
fn test_split_storage_archival_node_sync() {
    init_test_logger();

    let epoch_length = 5;
    let gc_num_epochs_to_keep = 3;
    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .enable_archival_node(ArchivalKind::Cold)
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .build()
        .warmup();

    // Add a second archival node right after warmup (small gap, will catch up
    // during normal block production).
    let archival2_identifier = "archival2";
    let archival2 = create_account_id(archival2_identifier);
    let archival2_state = env
        .node_state_builder()
        .account_id(&archival2)
        .cold_storage(true)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.gc.gc_num_epochs_to_keep = gc_num_epochs_to_keep;
        })
        .build();
    env.add_node(archival2_identifier, archival2_state);

    let kill_height = env.validator().head().height + 1;
    env.runner_for_account(&archival2).run_until_head_height(kill_height);

    let killed_state = env.kill_node(archival2_identifier);

    // Advance the chain well past the GC period while the second archival is down.
    let target_height = kill_height + epoch_length * (gc_num_epochs_to_keep + 1);
    env.validator_runner().run_until_head_height(target_height);

    // Confirm the validator has GC'd an early block.
    let test_height = kill_height + 1;
    let early_block_req = GetBlock(BlockReference::BlockId(BlockId::Height(test_height)));
    let validator_result = env.validator_mut().view_client_actor().handle(early_block_req.clone());
    assert_matches!(validator_result, Err(GetBlockError::UnknownBlock { .. }));

    // Restart the second archival — it must catch up the missing blocks
    // from the primary archival node (the validator no longer has them).
    let archival2_restart_identifier = format!("{}-restart", archival2_identifier);
    env.restart_node(&archival2_restart_identifier, killed_state);

    // Give the restarted node time to catch up.
    env.validator_runner().run_for_number_of_blocks(15);

    let archival2_height = env.node_for_account(&archival2).head().height;
    assert!(
        archival2_height > target_height,
        "restarted archival node should advance past target height \
         ({archival2_height} vs {target_height})"
    );

    // Confirm the restarted archival has the early block the validator GC'd.
    let archival2_result =
        env.node_for_account_mut(&archival2).view_client_actor().handle(early_block_req);
    assert!(
        archival2_result.is_ok(),
        "restarted archival should have block at height {test_height}"
    );

    // Verify cold storage is still functional after restart.
    let info =
        env.node_for_account_mut(&archival2).view_client_actor().handle(GetSplitStorageInfo {});
    let info = info.unwrap();
    assert_eq!(info.hot_db_kind.as_deref(), Some("Hot"));
    assert!(info.cold_head_height.is_some(), "cold head should be set after restart");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
