//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::is_spice_activation_parent;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::{Balance, Gas};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;
use std::collections::HashSet;

const EPOCH_LENGTH: u64 = 5;

/// A chain starting pre-spice that votes straight up to spice, so it crosses the
/// activation boundary a couple of epochs in. The GC window is wide so the blocks a
/// killed node needs to catch up on are still there.
fn setup_upgrading_chain(num_validators: usize) -> TestLoopEnv {
    TestLoopBuilder::new()
        .validators(num_validators, 0)
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .track_all_shards()
        // Zero inflation, so the supply identity at the boundary is exact with no
        // minting term. A real gas price (the test genesis default is zero) so
        // transactions burn tokens and the identity subtracts a real burn.
        .max_inflation_rate(Rational32::new(0, 1))
        .gas_prices(Balance::from_yoctonear(100_000_000), Balance::from_yoctonear(10_000_000_000))
        .gas_price_adjustment_rate(Rational32::new(1, 10))
        .gas_limit(Gas::from_gigagas(400))
        .gc_num_epochs_to_keep(20)
        .add_user_account(&create_account_id("user"), Balance::from_near(100))
        .build()
}

/// The minimal upgrade test: a pre-spice chain votes itself into spice, the
/// activation parent certifies under spice from the boundary bootstrap, execution
/// follows across the boundary, and the chain keeps running spice epochs. Every node
/// tracks all shards: without the apply-time witness a validator that does not track
/// a shard cannot endorse the activation parent's chunk for it.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice() {
    init_test_logger();
    let mut env = setup_upgrading_chain(4);
    let user = create_account_id("user");
    let receiver = create_account_id("validator1");

    // Trickle a transfer every block, so the blocks around the boundary burn gas:
    // the supply identity below must subtract a real burn to mean anything.
    let mut crossed = false;
    for _ in 0..10 * EPOCH_LENGTH {
        let tx = env.node(0).tx_send_money(&user, &receiver, Balance::from_yoctonear(1));
        env.node(0).submit_tx(tx);
        env.node_runner(0).run_for_number_of_blocks(1);
        if env.node(0).head_block().is_spice_block() {
            crossed = true;
            break;
        }
    }
    assert!(crossed, "chain never crossed the activation boundary");

    // Locate the first spice block on the chain and its pre-spice parent.
    let (activation_parent, parent_supply, parent_burnt, num_shards) = {
        let node = env.node(0);
        let mut first_spice = node.head_block();
        loop {
            let prev = node.client().chain.get_block(first_spice.header().prev_hash()).unwrap();
            if !prev.is_spice_block() {
                break;
            }
            first_spice = prev;
        }
        let parent = node.client().chain.get_block(first_spice.header().prev_hash()).unwrap();

        // The parent's burn, from the chunk extras its pre-spice apply wrote.
        let shard_layout =
            node.client().epoch_manager.get_shard_layout(parent.header().epoch_id()).unwrap();
        let mut parent_burnt = Balance::ZERO;
        for shard_uid in shard_layout.shard_uids() {
            let chunk_extra =
                node.client().chain.chain_store.get_chunk_extra(parent.hash(), &shard_uid).unwrap();
            parent_burnt = parent_burnt.checked_add(chunk_extra.balance_burnt()).unwrap();
        }
        assert!(
            parent_burnt > Balance::ZERO,
            "the activation parent must burn gas for the supply identity to be meaningful",
        );
        // The witness of the activation parent replays its apply with the gas price
        // of the parent's parent (the pre-spice convention); the spice convention
        // would take the parent's own.
        let grandparent = node.client().chain.get_block(parent.header().prev_hash()).unwrap();
        assert_ne!(
            grandparent.header().next_gas_price(),
            parent.header().next_gas_price(),
            "gas price must move at the boundary for the era convention to matter",
        );
        (
            parent.clone(),
            parent.header().total_supply(),
            parent_burnt,
            shard_layout.num_shards() as usize,
        )
    };
    let parent_height = activation_parent.header().height();

    // Two further spice epochs. The first spice epoch cannot end before the
    // activation parent certifies, so getting here proves certification liveness.
    env.node_runner(0).run_until_head_height(parent_height + 3 * EPOCH_LENGTH);

    let node = env.node(0);
    assert!(node.head_block().is_spice_block());
    // Execution followed across the boundary.
    let final_execution_head =
        node.client().chain.chain_store.spice_final_execution_head().unwrap();
    assert!(
        final_execution_head.height > parent_height,
        "execution must advance past the boundary",
    );

    // The certifying block: the first block whose core statements complete the
    // activation parent's execution results. Nothing else can certify before the
    // parent does, and inflation is zero, so the supply must stay untouched up to
    // the certifying block and drop by exactly the parent's burn there.
    let mut chain_blocks = Vec::new();
    let mut block = node.head_block();
    while block.header().height() > parent_height {
        let prev_hash = *block.header().prev_hash();
        chain_blocks.push(block);
        block = node.client().chain.get_block(&prev_hash).unwrap();
    }
    chain_blocks.reverse();

    let mut certified_shards = HashSet::new();
    let mut certifying_block = None;
    for block in &chain_blocks {
        for (chunk_id, _) in block.spice_core_statements().iter_execution_results() {
            if &chunk_id.block_hash == activation_parent.hash() {
                certified_shards.insert(chunk_id.shard_id);
            }
        }
        if certified_shards.len() == num_shards {
            certifying_block = Some(block);
            break;
        }
    }
    let certifying_block = certifying_block.expect("the activation parent must certify");

    let before_certifying =
        node.client().chain.get_block(certifying_block.header().prev_hash()).unwrap();
    assert_eq!(
        before_certifying.header().total_supply(),
        parent_supply,
        "supply must be untouched until the activation parent certifies",
    );
    assert_eq!(
        certifying_block.header().total_supply(),
        parent_supply.checked_sub(parent_burnt).unwrap(),
        "the certifying block must subtract exactly the activation parent's burn",
    );
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
