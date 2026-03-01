use std::collections::HashSet;

use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain::types::Tip;
use near_client::GetBlock;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, BlockId, BlockReference};
use near_store::{COLD_HEAD_KEY, DBCol};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
/// An early block height that will be GC-ed by validators but retained by the
/// archival node. Height 0 is the genesis block which is handled specially, so
/// we pick height 1 instead.
const EARLY_HEIGHT: u64 = 1;

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

    let archival_id: AccountId = "archival".parse().unwrap();

    // Build genesis with a single validator.
    let validators_spec = create_validators_spec(1, 0);
    let mut clients = validators_spec_clients(&validators_spec);
    // Add the archival node as a non-validator client.
    clients.push(archival_id.clone());

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .validators_spec(validators_spec)
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .cold_storage_archival_clients(HashSet::from([archival_id.clone()]))
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    // Run long enough for GC to kick in.
    let target_height = EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2);
    env.runner_for_account(&archival_id).run_until_head_height(target_height);

    // Validator cannot access an early block after GC.
    let early_block_request = GetBlock(BlockReference::BlockId(BlockId::Height(EARLY_HEIGHT)));
    let validator_result = env.node_mut(0).view_client_actor().handle(early_block_request.clone());
    assert!(validator_result.is_err(), "validator should have GC'd the early block");

    // Archival node can still serve the early block via split storage.
    let archival_idx = env.account_data_idx(&archival_id);
    let archival_result =
        env.node_mut(archival_idx).view_client_actor().handle(early_block_request);
    assert!(archival_result.is_ok(), "archival node should retain all blocks via cold storage");

    // Cold head is advancing — data is being migrated to cold storage.
    let archival = env.node_for_account(&archival_id);
    let cold_head: Tip =
        archival.store().get_ser(DBCol::BlockMisc, COLD_HEAD_KEY).expect("cold head should exist");
    assert!(cold_head.height > 0, "cold head should have advanced past genesis");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
