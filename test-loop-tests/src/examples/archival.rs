use near_async::messaging::Handler;
use near_async::time::Duration;
use near_client::GetBlock;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{BlockId, BlockReference};

use crate::setup::builder::{ArchivalKind, TestLoopBuilder};

/// Demonstrates setting up an archival node with cold storage (split storage).
///
/// With cold storage, the archival node migrates old block data from the hot DB
/// to a separate cold DB. The hot store is then garbage-collected to stay small,
/// while the view client uses split storage to transparently serve data from
/// both stores. This means old blocks remain accessible even after hot-store GC.
///
/// This example sets up 1 validator and 1 non-validator archival node with cold
/// storage, runs the chain long enough for GC to trigger, and verifies that:
/// - The validator cannot access early blocks after GC.
/// - The archival node can still serve early blocks via its view client.
/// - The cold head is advancing (data is being migrated to cold storage).
#[test]
fn test_archival_node_with_cold_storage() {
    init_test_logger();

    let epoch_length = 5;
    let gc_num_epochs_to_keep = 3;
    // Height 0 is the genesis block which is handled specially, so we pick
    // height 1 as an early block that will be GC-ed by validators but retained
    // by the archival node.
    let early_height = 1;
    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        .enable_archival_node(ArchivalKind::Cold)
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .build()
        .warmup();

    // Run long enough for GC to kick in.
    let target_height = epoch_length * (gc_num_epochs_to_keep + 2);
    env.archival_runner().run_until_head_height(target_height);

    let early_block_request = GetBlock(BlockReference::BlockId(BlockId::Height(early_height)));
    // Validator cannot access an early block after GC.
    let validator_result =
        env.validator_mut().view_client_actor().handle(early_block_request.clone());
    assert!(validator_result.is_err(), "validator should have GC'd the early block");
    // Archival node can still serve the early block via split storage.
    let archival_result = env.archival_node_mut().view_client_actor().handle(early_block_request);
    assert!(archival_result.is_ok(), "archival node should retain all blocks via cold storage");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
