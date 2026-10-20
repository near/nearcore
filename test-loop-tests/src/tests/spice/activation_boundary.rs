//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::spice::boundary_synthesis::execution_result_and_receipt_proofs_from_pre_spice_apply;
use near_chain::{Block, ChainStoreAccess};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::{Balance, BlockHeight, BlockHeightDelta, Gas, ShardId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const EPOCH_LENGTH: BlockHeightDelta = 5;

/// A chain starting pre-spice that votes straight up to spice, so it crosses the
/// activation boundary a couple of epochs in. Returned before warmup, so a test can add
/// its own drop condition first.
fn build_pre_spice_chain_voting_for_spice() -> TestLoopEnv {
    TestLoopBuilder::new()
        .validators(2, 2)
        .enable_rpc()
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        // Zero inflation, so the supply identity across the boundary is exact with no
        // minting term. A real gas price, so transactions burn tokens and the identity
        // subtracts a real burn.
        .max_inflation_rate(Rational32::new(0, 1))
        .gas_prices(Balance::from_yoctonear(100_000_000), Balance::from_yoctonear(10_000_000_000))
        .gas_price_adjustment_rate(Rational32::new(1, 10))
        // The gas price starts at the minimum and only rises when a chunk uses more than
        // half its gas limit, which one transfer per block never does with the default
        // limit. A limit a transfer fills past half makes the price move at the
        // boundary, which the plain upgrade test relies on.
        .gas_limit(Gas::from_gigagas(400))
        .add_user_account(&create_account_id("user"), Balance::from_near(100))
        // On the other shard than "user" (the layout splits at "test1"), so
        // transfers between the two cross shards.
        .add_user_account(&create_account_id("receiver"), Balance::from_near(100))
        .delay_warmup()
        .build()
}

/// Basic upgrade test: a pre-spice chain votes itself into spice.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice().warmup();
    let first_spice = send_transfers_until_first_spice_block(&mut env);
    let last_pre_spice = prev_block(&env, &first_spice);
    let before_last_pre_spice = prev_block(&env, &last_pre_spice);
    // The boundary replay must price gas with the prev block's `next_gas_price`, as the
    // pre-spice apply did. If it equals the block's own, a replay using the block's own
    // would still certify.
    assert_ne!(
        before_last_pre_spice.header().next_gas_price(),
        last_pre_spice.header().next_gas_price(),
        "gas price must move at the boundary to catch a replay priced with the block's own",
    );

    run_two_more_spice_epochs(&mut env);

    assert_boundary_crossed_without_missing_chunks(&env, &last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, &last_pre_spice);
    assert_boundary_results_validated(&env, &last_pre_spice);
}

/// The upgrade with every chunk missing at the last pre-spice block.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_missing_chunks_at_boundary() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice()
        .drop(DropCondition::ProtocolUpgradeChunkRange(
            ProtocolFeature::Spice.protocol_version(),
            HashMap::from([(0, -1..0), (1, -1..0)]),
        ))
        .warmup();
    let first_spice = send_transfers_until_first_spice_block(&mut env);
    let last_pre_spice = prev_block(&env, &first_spice);
    assert_eq!(last_pre_spice.header().chunk_mask(), &[false, false]);

    run_two_more_spice_epochs(&mut env);

    assert_boundary_crossed_without_missing_chunks(&env, &last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, &last_pre_spice);
    assert_boundary_results_validated(&env, &last_pre_spice);
}

/// The upgrade with staggered gaps straddling the anchor: one shard's chunk missing at
/// the last pre-spice block, the other's missing there and at the block before it.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_staggered_missing_chunks_at_boundary() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice()
        .drop(DropCondition::ProtocolUpgradeChunkRange(
            ProtocolFeature::Spice.protocol_version(),
            HashMap::from([(0, -1..0), (1, -2..0)]),
        ))
        .warmup();
    let first_spice = send_transfers_until_first_spice_block(&mut env);
    let last_pre_spice = prev_block(&env, &first_spice);
    let before_last_pre_spice = prev_block(&env, &last_pre_spice);
    assert_eq!(last_pre_spice.header().chunk_mask(), &[false, false]);
    assert_eq!(before_last_pre_spice.header().chunk_mask(), &[true, false]);

    run_two_more_spice_epochs(&mut env);

    assert_boundary_crossed_without_missing_chunks(&env, &last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, &last_pre_spice);
    assert_boundary_results_validated(&env, &last_pre_spice);
}

/// The upgrade with endorsements delayed, so the boundary results stay pending for a few
/// blocks and the supply identity is checked across blocks that certify nothing yet.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_execution_delay() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice().warmup();
    env.delay_endorsements_propagation(3);
    let first_spice = send_transfers_until_first_spice_block(&mut env);
    let last_pre_spice = prev_block(&env, &first_spice);

    run_two_more_spice_epochs(&mut env);

    assert_boundary_crossed_without_missing_chunks(&env, &last_pre_spice);
    let certifying_height = assert_supply_subtracts_certified_burn_once(&env, &last_pre_spice);
    assert!(
        certifying_height > last_pre_spice.header().height() + 1,
        "the delay must leave the boundary results pending for at least one block",
    );
    assert_boundary_results_validated(&env, &last_pre_spice);
}

/// Trickles a transfer every block, so the blocks around the boundary burn gas, until the
/// head is a spice block. Returns the first spice block.
fn send_transfers_until_first_spice_block(env: &mut TestLoopEnv) -> Arc<Block> {
    let user = create_account_id("user");
    let receiver = create_account_id("receiver");
    for _ in 0..10 * EPOCH_LENGTH {
        let tx = env.rpc_node().tx_send_money(&user, &receiver, Balance::from_yoctonear(1));
        env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(1);
        let head_block = env.rpc_node().head_block();
        if head_block.is_spice_block() {
            assert!(
                !prev_block(env, &head_block).is_spice_block(),
                "the head must be the first spice block",
            );
            return head_block;
        }
    }
    panic!("chain never crossed the activation boundary");
}

fn prev_block(env: &TestLoopEnv, block: &Block) -> Arc<Block> {
    env.rpc_node().block(*block.header().prev_hash())
}

/// An epoch cannot end before its first block certifies, so the first transition out of
/// the first spice epoch proves the boundary certified. The second leaves blocks past the
/// certifying one for the supply walk.
fn run_two_more_spice_epochs(env: &mut TestLoopEnv) {
    for _ in 0..2 {
        env.rpc_runner().run_until_new_epoch();
    }
}

/// The blocks above `height` on the rpc node's chain, oldest first, up to the head.
fn blocks_after(node: &TestLoopNode<'_>, height: BlockHeight) -> Vec<Arc<Block>> {
    let mut blocks = Vec::new();
    let mut block = node.head_block();
    while block.header().height() > height {
        let prev_hash = *block.header().prev_hash();
        blocks.push(block);
        block = node.block(prev_hash);
    }
    blocks.reverse();
    blocks
}

/// From the last pre-spice block to the head no height is skipped and no chunk is
/// missing: the boundary costs nothing in liveness. Execution advanced past it.
fn assert_boundary_crossed_without_missing_chunks(env: &TestLoopEnv, last_pre_spice: &Block) {
    let node = env.rpc_node();
    let last_pre_spice_height = last_pre_spice.header().height();
    assert!(node.head_block().is_spice_block());
    let final_execution_head =
        node.client().chain.chain_store.spice_final_execution_head().unwrap();
    assert!(
        final_execution_head.height > last_pre_spice_height,
        "execution must advance past the boundary",
    );

    let blocks = blocks_after(&node, last_pre_spice_height);
    assert_eq!(
        blocks.len() as BlockHeightDelta,
        node.head().height - last_pre_spice_height,
        "height skipped crossing the boundary",
    );
    for block in &blocks {
        assert!(
            block.header().chunk_mask().iter().all(|mask| *mask),
            "chunk missing at height {} crossing the boundary",
            block.header().height(),
        );
    }
}

/// Every block after the last pre-spice block subtracts from the supply exactly the burn
/// of the blocks its core statements newly certify. So the last pre-spice block's burn,
/// the one its pre-spice apply wrote, enters the supply once, at the block certifying
/// it, and never again. Returns that block's height.
///
/// The expected burn comes from the core statements alone, independent of the production
/// code: a block certifies in the block whose core statements carry its last missing
/// shard's result. Inflation is zero, so there is no minting term.
fn assert_supply_subtracts_certified_burn_once(
    env: &TestLoopEnv,
    last_pre_spice: &Block,
) -> BlockHeight {
    let node = env.rpc_node();
    let client = node.client();
    let shard_layout =
        client.epoch_manager.get_shard_layout(last_pre_spice.header().epoch_id()).unwrap();
    let num_shards = shard_layout.num_shards() as usize;

    // The burn in the last pre-spice block's chunk extras. A missing chunk's extra
    // repeats the burn of the last chunk applied on its shard.
    let mut last_pre_spice_burnt = Balance::ZERO;
    for shard_uid in shard_layout.shard_uids() {
        let chunk_extra =
            client.chain.chain_store.get_chunk_extra(last_pre_spice.hash(), &shard_uid).unwrap();
        last_pre_spice_burnt =
            last_pre_spice_burnt.checked_add(chunk_extra.balance_burnt()).unwrap();
    }
    assert!(
        last_pre_spice_burnt > Balance::ZERO,
        "the last pre-spice block must burn gas for the supply identity to be meaningful",
    );

    let mut shards_by_block: HashMap<CryptoHash, HashSet<ShardId>> = HashMap::new();
    let mut burnt_by_block: HashMap<CryptoHash, Balance> = HashMap::new();
    let mut certifying_height = None;
    let mut prev_supply = last_pre_spice.header().total_supply();
    for block in blocks_after(&node, last_pre_spice.header().height()) {
        let mut expected_burn = Balance::ZERO;
        for (chunk_id, execution_result) in block.spice_core_statements().iter_execution_results() {
            let shards = shards_by_block.entry(chunk_id.block_hash).or_default();
            assert!(
                shards.insert(chunk_id.shard_id),
                "result of {chunk_id:?} appears in two blocks' core statements",
            );
            let burnt = burnt_by_block.entry(chunk_id.block_hash).or_default();
            *burnt = burnt.checked_add(execution_result.chunk_extra.balance_burnt()).unwrap();
            if shards.len() == num_shards {
                expected_burn = expected_burn.checked_add(*burnt).unwrap();
                if chunk_id.block_hash == *last_pre_spice.hash() {
                    certifying_height = Some(block.header().height());
                }
            }
        }
        assert_eq!(
            block.header().total_supply(),
            prev_supply.checked_sub(expected_burn).unwrap(),
            "block {} must subtract exactly the burn of the blocks it newly certifies",
            block.header().height(),
        );
        prev_supply = block.header().total_supply();
    }
    let certifying_height = certifying_height.expect("the last pre-spice block must certify");
    assert_eq!(
        burnt_by_block[last_pre_spice.hash()],
        last_pre_spice_burnt,
        "the certified burn of the last pre-spice block differs from its pre-spice apply",
    );
    certifying_height
}

/// The certified execution results of the last pre-spice block must be exactly what
/// its pre-spice apply produced, for every shard, and each must be endorsed by at
/// least one validator that never ran that apply. Such a validator can only have
/// endorsed by validating the boundary witness, replaying the apply from the witness
/// and arriving at the same result.
fn assert_boundary_results_validated(env: &TestLoopEnv, last_pre_spice: &Block) {
    let rpc_node = env.rpc_node();
    let client = rpc_node.client();
    let epoch_manager = client.epoch_manager.as_ref();
    let core_reader = &client.chain.spice_core_reader;
    let shard_layout = epoch_manager.get_shard_layout(last_pre_spice.header().epoch_id()).unwrap();

    let certified = core_reader.get_execution_results_by_shard_id(last_pre_spice.header()).unwrap();
    assert_eq!(
        certified.len(),
        shard_layout.num_shards() as usize,
        "every chunk of the last pre-spice block must be certified",
    );

    for shard_id in shard_layout.shard_ids() {
        let certified = certified[&shard_id].as_ref();
        // The rpc node tracks every shard, so it applied the last pre-spice block in
        // full the pre-spice way; this is the result the boundary attests.
        let (applied, _) = execution_result_and_receipt_proofs_from_pre_spice_apply(
            &client.chain.chain_store,
            epoch_manager,
            last_pre_spice,
            shard_id,
        )
        .unwrap();
        assert_eq!(
            certified, &applied,
            "certified result of shard {shard_id} differs from the pre-spice apply",
        );

        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let mut stateless_endorsers = Vec::new();
        for (index, data) in env.node_datas.iter().enumerate() {
            let applied_locally = env
                .node(index)
                .client()
                .chain
                .chain_store
                .get_chunk_extra(last_pre_spice.hash(), &shard_uid)
                .is_ok();
            if applied_locally {
                continue;
            }
            let Some(endorsement) =
                core_reader.get_endorsement(last_pre_spice.hash(), shard_id, &data.account_id)
            else {
                continue;
            };
            assert_eq!(
                endorsement.execution_result_hash,
                certified.compute_hash(),
                "{} endorsed a different result for shard {shard_id} than certified",
                data.account_id,
            );
            stateless_endorsers.push(data.account_id.clone());
        }
        assert!(
            !stateless_endorsers.is_empty(),
            "no validator without the pre-spice apply of shard {shard_id} endorsed its boundary \
             chunk; the boundary witness validation went unexercised",
        );
    }
}

/// Kill a node when its head is the last pre-spice block and restart it once the other
/// nodes have advanced into spice. Catching up re-runs the activation seeding, which
/// must be idempotent. On startup the executor must re-bootstrap the boundary and the
/// data distributor must resume waiting on the boundary witnesses, so the node follows
/// the chain across it without panicking.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_mid_boundary() {
    run_restart_across_boundary(|node| {
        is_last_pre_spice_block(node.client().epoch_manager.as_ref(), &node.head().last_block_hash)
            .unwrap()
    });
}

/// Kill a node once its head is a spice block but it has not executed past the boundary,
/// so its spice execution heads are still the pre-spice blocks activation seeded them to.
/// On startup the executor must reach the first spice block from there.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_after_first_spice_block() {
    run_restart_across_boundary(|node| {
        let chain_store = &node.client().chain.chain_store;
        node.head_block().is_spice_block()
            && !chain_store
                .get_block_header(&chain_store.spice_execution_head().unwrap().last_block_hash)
                .unwrap()
                .is_spice()
    });
}

/// Kills node 0 as soon as `kill_at` holds for it, lets the other nodes advance into
/// spice, then restarts it and checks it executes across the boundary.
fn run_restart_across_boundary(kill_at: impl Fn(&TestLoopNode<'_>) -> bool) {
    init_test_logger();

    // A pre-spice chain voting straight up to spice, with enough validators that it
    // keeps making progress while one is down.
    let mut env = TestLoopBuilder::new()
        .validators(4, 4)
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .build();

    let restart_identifier = env.node_datas[0].identifier.clone();

    env.node_runner(0).run_until(&kill_at, Duration::seconds(60));
    let last_pre_spice_height = {
        let node = env.node(0);
        let mut header =
            node.client().chain.get_block_header(&node.head().last_block_hash).unwrap();
        while header.is_spice() {
            header = node.client().chain.get_block_header(header.prev_hash()).unwrap();
        }
        header.height()
    };
    let killed_node_state = env.kill_node(&restart_identifier);

    // The remaining nodes cross the boundary and run a few spice blocks past it, so the
    // restarted node has spice blocks to catch up on.
    env.node_runner(1).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(60));
    env.node_runner(1).run_for_number_of_blocks(3);
    let catch_up_height = env.node(1).head().height;

    let new_identifier = format!("{restart_identifier}-restart");
    let restarted_index = env.restart_node(&new_identifier, killed_node_state);
    env.node_runner(restarted_index).run_until_head_height(catch_up_height);

    let restarted = env.node(restarted_index);
    assert!(restarted.head_block().is_spice_block(), "the restarted node must cross the boundary");

    // The re-bootstrapped executor must execute across the boundary, not just follow headers.
    env.node_runner(restarted_index).run_until(
        |node| {
            node.client()
                .chain
                .chain_store
                .spice_execution_head()
                .is_ok_and(|head| head.height > last_pre_spice_height)
        },
        Duration::seconds(60),
    );
}
