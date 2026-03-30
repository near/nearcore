//! Validator kickout tests after sync.
//!
//! These tests verify the interaction between validator kickout thresholds and
//! node restart/sync. Both tests kill a single validator while others continue
//! producing, with `kickouts_standard_80_percent()` enabled. They cover the two
//! outcomes of the block production threshold mechanism:
//!
//! - `test_validator_kickout_after_restart`: validator is down too long → kicked
//! - `test_validator_no_kickout_after_quick_restart`: validator restarts quickly → stays in set
//!
//! Background: If a validator performs badly in epoch E (below the 80% block
//! production threshold), it gets kicked out in epoch E+2. The +2 offset exists
//! because the validator set for epoch E+1 is already decided before epoch E ends.
//!
//! Both tests verify the node goes through near-horizon sync (BlockSync) after
//! restart, since the gap is within `epoch_sync_horizon`.

use super::util::{assert_near_horizon_sync_sequence, run_until_synced, track_sync_status};
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, EpochId, ValidatorInfoIdentifier};

/// Check if `account_id` is in the validator set for the given epoch.
fn is_validator_in_epoch(env: &TestLoopEnv, epoch_id: &EpochId, account_id: &AccountId) -> bool {
    env.node(1)
        .client()
        .epoch_manager
        .get_epoch_all_validators(epoch_id)
        .unwrap()
        .iter()
        .any(|v| v.account_id() == account_id)
}

/// Number of blocks produced by `account_id` in the given epoch, queried from
/// the epoch manager's validator stats (no block iteration needed).
/// Returns 0 if the validator was not in the set for this epoch (e.g., kicked).
fn blocks_produced_in_epoch(env: &TestLoopEnv, epoch_id: &EpochId, account_id: &AccountId) -> u64 {
    env.node(1)
        .client()
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::EpochId(*epoch_id))
        .unwrap()
        .current_validators
        .iter()
        .find(|v| &v.account_id == account_id)
        .map(|v| v.num_produced_blocks)
        .unwrap_or(0)
}

/// Kill a single validator and verify the kickout lifecycle across epochs.
/// The restarted node syncs via near-horizon (BlockSync).
///
/// Scenario (`kickouts_standard_80_percent()`, `block_producer_kickout_threshold=80`):
///
/// ```text
///          E                    E+1                      E+2
///        ──────────────────┬──────────────────────  ┬──────────────────────
/// node:  produced blocks,  │ restarts mid-epoch,    │ kicked out
///        killed mid-epoch  │ syncs via BlockSync,   │ (bad production
///                          │ still in validator set │ in epoch E)
/// ```
///
/// Steps:
/// 1. 4 validators with `kickouts_standard_80_percent()`, epoch_length=10.
/// 2. Kill one validator mid-epoch E.
/// 3. Remaining 3 validators advance to epoch E+1.
/// 4. Restart the killed validator in E+1. It syncs via near-horizon (BlockSync).
/// 5. Advance to epoch E+2.
/// 6. Verify: validator was in set for E and E+1, produced blocks in E before
///    kill, and is NOT in set for E+2 (kicked for <80% production in E).
///
/// Unlike `slow_test_validator_restart_under_cross_shard_load` which kills all validators
/// with kickout thresholds at 0, this test exercises the kickout mechanism with a single
/// validator killed while others continue producing.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_validator_kickout_after_restart() {
    init_test_logger();

    let epoch_length = 10;
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .kickouts_standard_80_percent()
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    // account_id is the validator identity (used for validator set checks).
    // node identifier is the testloop label (used for kill/restart/denylist).
    let account_id = clients[0].clone();

    // Run all validators for a few blocks in epoch E, then kill one.
    env.node_runner(0).run_until_head_height(3);
    let epoch_e = env.node(0).head().epoch_id;

    let node_identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&node_identifier);

    // Remaining 3 validators advance to epoch E+1.
    env.node_runner(1).run_until_new_epoch();
    let epoch_e1 = env.node(1).head().epoch_id;

    // Restart the killed validator in E+1 and verify near-horizon sync.
    env.restart_node("node0_restart", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 1);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Advance to epoch E+2, then one more to verify chain keeps progressing.
    env.node_runner(1).run_until_new_epoch();
    let epoch_e2 = env.node(1).head().epoch_id;
    env.node_runner(1).run_until_new_epoch();

    // Validator set membership: in E and E+1 (decided before E ended), kicked in E+2.
    assert!(is_validator_in_epoch(&env, &epoch_e, &account_id));
    assert!(is_validator_in_epoch(&env, &epoch_e1, &account_id));
    assert!(!is_validator_in_epoch(&env, &epoch_e2, &account_id));

    // Block production: produced in E (before kill) and E+1 (restarted, still in
    // validator set), but zero in E+2 (kicked).
    assert!(blocks_produced_in_epoch(&env, &epoch_e, &account_id) > 0);
    assert!(blocks_produced_in_epoch(&env, &epoch_e1, &account_id) > 0);
    assert_eq!(blocks_produced_in_epoch(&env, &epoch_e2, &account_id), 0);
}

/// Kill a single validator briefly and restart it within the same epoch, verifying
/// it produces enough blocks to stay above the kickout threshold and is NOT kicked.
/// The restarted node syncs via near-horizon (BlockSync).
///
/// This is the complement of `test_validator_kickout_after_restart` — that test
/// verifies the kickout path, this test verifies the no-kickout path. Together
/// they cover both outcomes of the block production threshold mechanism.
///
/// Scenario (`kickouts_standard_80_percent()`, `block_producer_kickout_threshold=80`,
///           epoch_length=80 — long enough for the node to restart and still produce
///           >80% of its assigned blocks):
///
/// ```text
///          E                                    E+1              E+2
///        ───────────────────────────────── ┬────────────────┬──────────────────────
/// node:  run ~10%, killed, restart mid-E,  │ still in       │ still in
///        syncs via BlockSync, produces     │ validator set  │ validator set
///        remaining ~90% of blocks          │                │ (>80% production in E)
/// ```
///
/// Steps:
/// 1. 4 validators with `kickouts_standard_80_percent()`, epoch_length=80.
/// 2. All validators run for ~10% of epoch E (~8 blocks), then kill one.
/// 3. Remaining 3 advance 2 blocks, then restart the killed validator.
/// 4. Validator syncs via near-horizon (BlockSync), then all advance to E+2.
/// 5. Verify: validator produced blocks in E, stays in the validator set for
///    E, E+1, and E+2 (not kicked).
///
/// Uses epoch_length=80 (unlike the kickout test which uses 10) to ensure the
/// validator has enough runway after restart to cross the 80% threshold.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_validator_no_kickout_after_quick_restart() {
    init_test_logger();

    let epoch_length: u64 = 80;
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .kickouts_standard_80_percent()
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    // account_id is the validator identity (used for validator set checks).
    // node identifier is the testloop label (used for kill/restart/denylist).
    let account_id = clients[0].clone();

    // Run all validators for ~10% of epoch E (~8 blocks), then kill one.
    env.node_runner(0).run_until_head_height(8);
    let epoch_e = env.node(0).head().epoch_id;

    let node_identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&node_identifier);

    // Remaining 3 advance 2 blocks (still early in epoch E), then restart.
    env.node_runner(1).run_for_number_of_blocks(2);

    env.restart_node("node0_restart", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 1);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // All validators continue through E+1 and into E+2.
    env.node_runner(1).run_until_new_epoch();
    let epoch_e1 = env.node(1).head().epoch_id;
    env.node_runner(1).run_until_new_epoch();
    let epoch_e2 = env.node(1).head().epoch_id;
    env.node_runner(1).run_until_new_epoch();

    // Validator set membership: in E, E+1, and E+2 (not kicked — produced >80%).
    assert!(is_validator_in_epoch(&env, &epoch_e, &account_id));
    assert!(is_validator_in_epoch(&env, &epoch_e1, &account_id));
    assert!(is_validator_in_epoch(&env, &epoch_e2, &account_id));

    // Block production: produced in E (before kill + after restart), E+1, and E+2.
    assert!(blocks_produced_in_epoch(&env, &epoch_e, &account_id) > 0);
    assert!(blocks_produced_in_epoch(&env, &epoch_e1, &account_id) > 0);
    assert!(blocks_produced_in_epoch(&env, &epoch_e2, &account_id) > 0);

    // Restarted node is within 1 block of the network tip.
    let restarted = env.node(restarted_idx).head().height;
    let reference = env.node(1).head().height;
    assert!(restarted >= reference - 1, "restarted node fell behind: {restarted} vs {reference}");
}
