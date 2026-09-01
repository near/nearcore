//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::spice::boundary::is_spice_activation_parent;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::Balance;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;

const EPOCH_LENGTH: u64 = 5;

/// A chain starting pre-spice that votes straight up to spice, so it crosses the
/// activation boundary a couple of epochs in. The GC window is wide so the blocks a
/// killed node needs to catch up on are still there.
fn setup_upgrading_chain(num_validators: usize) -> TestLoopEnv {
    TestLoopBuilder::new()
        .validators(num_validators, 0)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .track_all_shards()
        .gc_num_epochs_to_keep(20)
        .add_user_account(&create_account_id("user"), Balance::from_near(100))
        .build()
}

/// Kill a node when its head is the activation parent and restart it once the other
/// nodes have advanced into spice. Catching up re-runs the activation seeding, which
/// must be idempotent, and the executor's `start_actor` recovery must re-bootstrap
/// the boundary so the node follows the chain across it without panicking.
#[test]
#[ignore = "TODO(spice-boundary): restarting between the activation parent and the first \
            certification needs the executor's start_actor boundary recovery; un-ignore \
            when it lands"]
fn test_restart_mid_boundary() {
    init_test_logger();

    // Four validators, so the chain keeps making progress while one is down.
    let mut env = setup_upgrading_chain(4);

    let restart_identifier = env.node_datas[0].identifier.clone();

    // Run until the head is the last pre-spice block, so the kill lands between the
    // activation parent and the first spice epoch's certification.
    env.node_runner(0).run_until(
        |node| {
            let head_block_hash = node.head().last_block_hash;
            is_spice_activation_parent(node.client().epoch_manager.as_ref(), &head_block_hash)
                .unwrap_or(false)
        },
        Duration::seconds(60),
    );
    let killed_node_state = env.kill_node(&restart_identifier);

    // The remaining nodes cross the boundary and run within the first spice epoch.
    env.node_runner(1).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(60));
    let catch_up_height = env.node(1).head().height;

    let new_identifier = format!("{restart_identifier}-restart");
    env.restart_node(&new_identifier, killed_node_state);
    // `restart_node` appends the new node rather than replacing the killed one, which
    // still holds an entry for the same account, so address the restarted node by index.
    let restarted_index = env.node_datas.len() - 1;
    env.node_runner(restarted_index).run_until_head_height(catch_up_height);

    let restarted = env.node(restarted_index);
    assert!(restarted.head_block().is_spice_block(), "the restarted node must cross the boundary");
    // Catching up re-ran the activation seeding: both execution heads must be present.
    restarted.client().chain.chain_store.spice_execution_head().unwrap();
    restarted.client().chain.chain_store.spice_final_execution_head().unwrap();
}
