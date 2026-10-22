//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::spice::boundary_synthesis::{
    execution_result_and_receipt_proofs_from_pre_spice_apply, get_undelivered_receipt_proofs,
    get_undelivered_receipts,
};
use near_chain::{Block, Chain, ChainStoreAccess, ReceiptFilter, get_incoming_receipts_for_shard};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ChunkHash;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::{Balance, BlockHeight, BlockHeightDelta, Gas, ShardId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
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
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let last_pre_spice = &trickle.last_pre_spice;
    let before_last_pre_spice = prev_block(&env, last_pre_spice);
    // The boundary replay must price gas with the prev block's `next_gas_price`, as the
    // pre-spice apply did. If it equals the block's own, a replay using the block's own
    // would still certify.
    assert_ne!(
        before_last_pre_spice.header().next_gas_price(),
        last_pre_spice.header().next_gas_price(),
        "gas price must move at the boundary to catch a replay priced with the block's own",
    );

    assert_boundary_crossed_without_missing_chunks(&env, last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, last_pre_spice);
    assert_boundary_results_validated(&env, last_pre_spice);
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
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let last_pre_spice = &trickle.last_pre_spice;
    assert_eq!(last_pre_spice.header().chunk_mask(), &[false, false]);

    assert_boundary_crossed_without_missing_chunks(&env, last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, last_pre_spice);
    assert_boundary_results_validated(&env, last_pre_spice);
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
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let last_pre_spice = &trickle.last_pre_spice;
    let before_last_pre_spice = prev_block(&env, last_pre_spice);
    assert_eq!(last_pre_spice.header().chunk_mask(), &[false, false]);
    assert_eq!(before_last_pre_spice.header().chunk_mask(), &[true, false]);

    assert_boundary_crossed_without_missing_chunks(&env, last_pre_spice);
    assert_supply_subtracts_certified_burn_once(&env, last_pre_spice);
    assert_boundary_results_validated(&env, last_pre_spice);
}

/// The upgrade with endorsements delayed, so the boundary results stay pending for a few
/// blocks and the supply identity is checked across blocks that certify nothing yet.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_execution_delay() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice().warmup();
    env.delay_endorsements_propagation(3);
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let last_pre_spice = &trickle.last_pre_spice;

    assert_boundary_crossed_without_missing_chunks(&env, last_pre_spice);
    let certifying_height = assert_supply_subtracts_certified_burn_once(&env, last_pre_spice);
    assert!(
        certifying_height > last_pre_spice.header().height() + 1,
        "the delay must leave the boundary results pending for at least one block",
    );
    assert_boundary_results_validated(&env, last_pre_spice);
}

/// The deposit each trickled transfer carries.
const TRICKLE_AMOUNT: Balance = Balance::from_yoctonear(1);

/// What [`cross_boundary_with_transfer_trickle`] observed.
struct BoundaryTrickle {
    last_pre_spice: Arc<Block>,
    head_height: BlockHeight,
    initial_receiver_balance: Balance,
    /// Inclusion height per submitted transfer; each was included exactly once.
    inclusion_heights: HashMap<CryptoHash, BlockHeight>,
}

impl BoundaryTrickle {
    /// A transfer included at the last pre-spice block, so its receipt is in flight
    /// across the boundary.
    fn transfer_in_flight_at_boundary(&self) -> CryptoHash {
        let boundary_height = self.last_pre_spice.header().height();
        *self
            .inclusion_heights
            .iter()
            .find(|(_, height)| **height == boundary_height)
            .map(|(tx_hash, _)| tx_hash)
            .expect("some transfer must be included at the last pre-spice block")
    }
}

/// Submits a cross-shard "user" -> "receiver" transfer of [`TRICKLE_AMOUNT`] every block,
/// so the blocks around the boundary burn gas, until the head is a spice block. Then runs
/// two more spice epochs and lets certification catch up to the head: an epoch cannot end
/// before its first block certifies, so the first transition out of the first spice epoch
/// proves the boundary certified, and the second leaves blocks past the certifying one
/// for the supply walk.
fn cross_boundary_with_transfer_trickle(env: &mut TestLoopEnv) -> BoundaryTrickle {
    let sender = create_account_id("user");
    let receiver = create_account_id("receiver");
    {
        let node = env.rpc_node();
        let shard_layout =
            node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap();
        assert_ne!(
            shard_layout.account_id_to_shard_id(&sender),
            shard_layout.account_id_to_shard_id(&receiver),
            "the transfers must cross shards",
        );
    }
    let initial_receiver_balance = env.rpc_node().view_account_query(&receiver).unwrap().amount;

    let mut submitted = Vec::new();
    let mut first_spice = None;
    for _ in 0..10 * EPOCH_LENGTH {
        let tx = env.rpc_node().tx_send_money(&sender, &receiver, TRICKLE_AMOUNT);
        submitted.push(env.rpc_node().submit_tx(tx));
        env.rpc_runner().run_for_number_of_blocks(1);
        let head_block = env.rpc_node().head_block();
        if head_block.is_spice_block() {
            first_spice = Some(head_block);
            break;
        }
    }
    let first_spice = first_spice.expect("chain never crossed the activation boundary");
    let last_pre_spice = prev_block(env, &first_spice);
    assert!(!last_pre_spice.is_spice_block(), "the head must be the first spice block");

    for _ in 0..2 {
        env.rpc_runner().run_until_new_epoch();
    }
    let head_height = env.rpc_node().head().height;
    env.rpc_runner().run_until_certified(head_height);

    // Scan every block's new chunks for the submitted transactions.
    let node = env.rpc_node();
    let submitted: HashSet<CryptoHash> = submitted.into_iter().collect();
    let mut multi_inclusions: HashMap<CryptoHash, Vec<BlockHeight>> = HashMap::new();
    let genesis_height = node.client().chain.chain_store.get_genesis_height();
    for height in genesis_height + 1..=head_height {
        let Ok(block_hash) = node.client().chain.chain_store.get_block_hash_by_height(height)
        else {
            continue;
        };
        let block = node.client().chain.get_block(&block_hash).unwrap();
        for chunk_header in block.chunks().iter() {
            if !chunk_header.is_new_chunk() {
                continue;
            }
            let chunk = node.client().chain.get_chunk(&chunk_header.chunk_hash()).unwrap();
            for tx in chunk.to_transactions() {
                if submitted.contains(&tx.get_hash()) {
                    multi_inclusions.entry(tx.get_hash()).or_default().push(height);
                }
            }
        }
    }
    let mut inclusion_heights = HashMap::new();
    for (tx_hash, heights) in multi_inclusions {
        assert_eq!(heights.len(), 1, "transaction {tx_hash} included more than once: {heights:?}");
        inclusion_heights.insert(tx_hash, heights[0]);
    }
    assert_eq!(inclusion_heights.len(), submitted.len(), "every transfer must be included");
    BoundaryTrickle { last_pre_spice, head_height, initial_receiver_balance, inclusion_heights }
}

fn prev_block(env: &TestLoopEnv, block: &Block) -> Arc<Block> {
    env.rpc_node().block(*block.header().prev_hash())
}

/// The blocks above `height` on `node`'s chain, oldest first, up to the head.
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
/// shard's result. Inflation is zero, so there is no minting term. Assumes the shard
/// layout does not change from the last pre-spice block to the head.
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

/// A cross-shard transfer in flight at the boundary: a transaction included in a
/// pre-spice chunk whose receipt executes in the first spice epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_receipt_in_flight() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice().warmup();
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    // Panics unless a transfer was included at the last pre-spice block: the receipt
    // whose exactly-once execution this test is about.
    trickle.transfer_in_flight_at_boundary();

    // Exactly-once execution: the receiver gained exactly one deposit per transfer.
    let receiver = create_account_id("receiver");
    let final_balance = env.rpc_node().view_account_query(&receiver).unwrap().amount;
    let expected = TRICKLE_AMOUNT.checked_mul(trickle.inclusion_heights.len() as u128).unwrap();
    assert_eq!(
        final_balance.checked_sub(trickle.initial_receiver_balance).unwrap(),
        expected,
        "every in-flight deposit must land exactly once",
    );
}

/// View queries addressed at blocks on both sides of the boundary, on a node that
/// crossed it.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_view_queries() {
    init_test_logger();
    let mut env = build_pre_spice_chain_voting_for_spice().warmup();
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let receiver = create_account_id("receiver");
    let boundary_height = trickle.last_pre_spice.header().height();

    // The receiver's balance as of block at height h: the deposit of a transfer
    // included at height i rides a cross-shard receipt applied at i + 1, so it is
    // visible exactly from state at h >= i + 1.
    let node = env.rpc_node();
    for queried_height in [
        boundary_height - 2,
        boundary_height - 1,
        boundary_height,
        boundary_height + 1,
        boundary_height + 2,
        trickle.head_height - 1,
    ] {
        let block_hash =
            node.client().chain.chain_store.get_block_hash_by_height(queried_height).unwrap();
        let balance = node.view_account_balance_at(block_hash, &receiver).unwrap_or_else(|err| {
            panic!("view query at height {queried_height} must work: {err:?}")
        });
        let executed_deposits =
            trickle.inclusion_heights.values().filter(|height| **height < queried_height).count();
        assert_eq!(
            balance.checked_sub(trickle.initial_receiver_balance).unwrap(),
            TRICKLE_AMOUNT.checked_mul(executed_deposits as u128).unwrap(),
            "balance as of height {queried_height} must reflect exactly the receipts \
             executed by then",
        );
    }

    // An outcome pair spanning the boundary: the transaction's outcome sits in a
    // pre-spice block, its receipt's outcome in a spice block.
    let boundary_tx = trickle.transfer_in_flight_at_boundary();
    let tx_outcome = node.execution_outcome_with_proof(boundary_tx);
    let receipt_id = node.tx_receipt_id(boundary_tx);
    let receipt_outcome = node.execution_outcome_with_proof(receipt_id);
    let outcome_height = |outcome: &ExecutionOutcomeWithIdAndProof| {
        node.client().chain.get_block_header(&outcome.block_hash).unwrap().height()
    };
    assert_eq!(outcome_height(&tx_outcome), boundary_height);
    assert!(
        outcome_height(&receipt_outcome) > boundary_height,
        "the boundary transaction's receipt must have executed under spice",
    );
}

/// Runs until the rpc node knows the height of the first spice block, and returns it.
fn run_until_estimated_first_spice_height(env: &mut TestLoopEnv) -> BlockHeight {
    let estimated_first_spice_height = |node: &TestLoopNode<'_>| {
        node.client()
            .epoch_manager
            .get_estimated_protocol_upgrade_block_height(node.head().last_block_hash)
            .unwrap()
    };
    env.rpc_runner()
        .run_until(|node| estimated_first_spice_height(node).is_some(), Duration::seconds(100));
    estimated_first_spice_height(&env.rpc_node()).unwrap()
}

/// Drops `shard_index`'s chunk at every height `pattern` marks 'x'. The pattern covers
/// the last `pattern.len()` pre-spice heights, oldest first.
fn drop_chunks_marked_missing(env: TestLoopEnv, shard_index: usize, pattern: &str) -> TestLoopEnv {
    let first_height_offset = -(pattern.len() as i64);
    pattern.chars().enumerate().filter(|(_, presence)| *presence == 'x').fold(
        env,
        |env, (position, _)| {
            let offset = first_height_offset + position as i64;
            env.drop(DropCondition::ProtocolUpgradeChunkRange(
                ProtocolFeature::Spice.protocol_version(),
                HashMap::from([(shard_index, offset..offset + 1)]),
            ))
        },
    )
}

/// The chunk presence of `shard_index` at `heights`: 'x' missing, '.' present.
fn observed_chunk_pattern(
    chain: &Chain,
    shard_index: usize,
    heights: Range<BlockHeight>,
) -> String {
    heights
        .map(|height| {
            let block = chain.get_block_by_height(height).unwrap();
            if block.chunks()[shard_index].is_new_chunk(height) { '.' } else { 'x' }
        })
        .collect()
}

/// Receipts sent pre-spice to a shard that misses chunks before the boundary are pending
/// for it at the last pre-spice block, carried by the sending shard's chunks after its
/// last one, and applied exactly once by its first spice chunk; across missing-chunk
/// patterns on both shards, including gaps on the sending shard.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_applies_each_receipt_once_with_mixed_missing_chunks() {
    init_test_logger();
    // Chunks of each shard at heights H-4, H-3, H-2, H-1, where H is the first spice
    // height: 'x' missing, '.' present. Then the number of receipts each shard's
    // boundary result carries for a shard that missed them, counting gas refunds and
    // receipts forwarded from the outgoing buffer.
    //   (low shard, upper shard, undelivered from low, undelivered from upper)
    let chunk_patterns = [
        ("....", "...x", 2, 0),
        ("...x", "....", 0, 2),
        ("....", ".x.x", 3, 0),
        ("x.x.", ".x.x", 0, 0),
        ("..x.", "x..x", 3, 0),
        (".xx.", "x.xx", 0, 0),
        ("xx..", "..xx", 1, 0),
        ("...x", ".xxx", 2, 0),
    ];
    // Long enough that transfers can start 7 heights before the first spice block.
    let epoch_length: BlockHeightDelta = 10;
    let amount = Balance::from_near(1);
    let heights_with_transfers: u64 = 7;
    // The shard layout splits at "test1": "low..." sorts before it, "upper..." after it.
    let low_sender = create_account_id("low_sender");
    let upper_sender = create_account_id("upper_sender");
    let low_target_from_low = create_account_id("low_target_from_low");
    let low_target_from_upper = create_account_id("low_target_from_upper");
    let upper_target_from_upper = create_account_id("upper_target_from_upper");
    let upper_target_from_low = create_account_id("upper_target_from_low");
    let transfers = [
        (&low_sender, &low_target_from_low),
        (&low_sender, &upper_target_from_low),
        (&upper_sender, &upper_target_from_upper),
        (&upper_sender, &low_target_from_upper),
    ];

    for (
        low_shard_pattern,
        upper_shard_pattern,
        expected_undelivered_from_low_shard,
        expected_undelivered_from_upper_shard,
    ) in chunk_patterns
    {
        let env = TestLoopBuilder::new()
            .validators(2, 2)
            .enable_rpc()
            .num_shards(2)
            .epoch_length(epoch_length)
            .protocol_version(pre_spice_protocol_version())
            .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
                ProtocolFeature::Spice.protocol_version(),
            ))
            .add_user_account(&low_sender, Balance::from_near(100))
            .add_user_account(&upper_sender, Balance::from_near(100))
            .add_user_account(&low_target_from_low, Balance::from_near(100))
            .add_user_account(&low_target_from_upper, Balance::from_near(100))
            .add_user_account(&upper_target_from_upper, Balance::from_near(100))
            .add_user_account(&upper_target_from_low, Balance::from_near(100))
            .delay_warmup()
            .build();
        let shard_layout = {
            let node = env.rpc_node();
            node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap()
        };
        let low_shard_id = shard_layout.account_id_to_shard_id(&low_sender);
        let upper_shard_id = shard_layout.account_id_to_shard_id(&upper_sender);
        assert_ne!(low_shard_id, upper_shard_id);
        for (account, expected_shard_id) in [
            (&low_target_from_low, low_shard_id),
            (&low_target_from_upper, low_shard_id),
            (&upper_target_from_upper, upper_shard_id),
            (&upper_target_from_low, upper_shard_id),
        ] {
            assert_eq!(shard_layout.account_id_to_shard_id(account), expected_shard_id);
        }
        let low_shard_index = shard_layout.get_shard_index(low_shard_id).unwrap();
        let upper_shard_index = shard_layout.get_shard_index(upper_shard_id).unwrap();
        let env = drop_chunks_marked_missing(env, low_shard_index, low_shard_pattern);
        let mut env =
            drop_chunks_marked_missing(env, upper_shard_index, upper_shard_pattern).warmup();

        let first_spice_height = run_until_estimated_first_spice_height(&mut env);
        let initial_target_balances: Vec<Balance> = transfers
            .iter()
            .map(|(_, target)| env.rpc_node().view_account_query(target).unwrap().amount)
            .collect();
        let mut tx_hashes = Vec::new();
        for height in first_spice_height - heights_with_transfers..first_spice_height {
            env.rpc_runner().run_until_head_height(height);
            for (sender, target) in &transfers {
                let tx = env.rpc_node().tx_send_money(sender, target, amount);
                tx_hashes.push(env.rpc_node().submit_tx(tx));
            }
        }
        for tx_hash in &tx_hashes {
            env.rpc_runner().run_until_outcome_available(*tx_hash, Duration::seconds(30));
            let receipt_id = env.rpc_node().tx_receipt_id(*tx_hash);
            env.rpc_runner().run_until_outcome_available(receipt_id, Duration::seconds(30));
        }
        env.rpc_runner().run_until_certified(first_spice_height);

        let node = env.rpc_node();
        let chain = &node.client().chain;
        let epoch_manager = chain.epoch_manager.as_ref();
        let first_spice = chain.get_block_by_height(first_spice_height).unwrap();
        let last_pre_spice = chain.get_block(first_spice.header().prev_hash()).unwrap();
        assert!(is_last_pre_spice_block(epoch_manager, last_pre_spice.hash()).unwrap());
        let pattern_heights = first_spice_height - 4..first_spice_height;
        assert_eq!(
            observed_chunk_pattern(chain, low_shard_index, pattern_heights.clone()),
            low_shard_pattern,
        );
        assert_eq!(
            observed_chunk_pattern(chain, upper_shard_index, pattern_heights),
            upper_shard_pattern,
        );
        let patterns = format!("patterns {low_shard_pattern} / {upper_shard_pattern}");
        let shard_layout =
            epoch_manager.get_shard_layout(last_pre_spice.header().epoch_id()).unwrap();
        let last_included_height =
            |shard_index: usize| last_pre_spice.chunks()[shard_index].height_included();
        let low_shard = (low_shard_id, low_shard_index);
        let upper_shard = (upper_shard_id, upper_shard_index);
        let mut undelivered_receipt_ids_by_receiving_shard: HashMap<ShardId, Vec<CryptoHash>> =
            HashMap::new();
        for (
            (source_shard_id, source_shard_index),
            (target_shard_id, target_shard_index),
            expected_undelivered_receipts,
        ) in [
            (low_shard, upper_shard, expected_undelivered_from_low_shard),
            (upper_shard, low_shard, expected_undelivered_from_upper_shard),
        ] {
            // The source's chunks after the target's last one carry what the target
            // missed, each with one proof to the target.
            let target_last_included_height = last_included_height(target_shard_index);
            let expected_carriers: HashSet<ChunkHash> = (target_last_included_height + 1
                ..first_spice_height)
                .filter_map(|height| {
                    let block = chain.get_block_by_height(height).unwrap();
                    let chunks = block.chunks();
                    let chunk_header = &chunks[source_shard_index];
                    chunk_header.is_new_chunk(height).then(|| chunk_header.chunk_hash().clone())
                })
                .collect();
            let undelivered_receipt_proofs = get_undelivered_receipt_proofs(
                &chain.chain_store,
                epoch_manager,
                &last_pre_spice,
                source_shard_id,
            )
            .unwrap();
            assert_eq!(
                undelivered_receipt_proofs.keys().cloned().collect::<HashSet<_>>(),
                expected_carriers,
                "{patterns}: the carriers of shard {source_shard_id} must be its chunks after \
                 shard {target_shard_id}'s last one",
            );
            for proofs in undelivered_receipt_proofs.values() {
                assert_eq!(
                    proofs.len(),
                    1,
                    "{patterns}: one proof per target that missed the chunk"
                );
                assert_eq!(proofs[0].1.to_shard_id, target_shard_id);
            }

            let undelivered_receipts = get_undelivered_receipts(
                &chain.chain_store,
                epoch_manager,
                &last_pre_spice,
                source_shard_id,
            )
            .unwrap();
            assert_eq!(
                undelivered_receipts.len(),
                expected_undelivered_receipts,
                "{patterns}: undelivered receipts from shard {source_shard_id}",
            );
            for receipt in &undelivered_receipts {
                let receipt_outcome = chain.get_execution_outcome(receipt.receipt_id()).unwrap();
                assert_eq!(
                    receipt_outcome.block_hash,
                    *first_spice.hash(),
                    "{patterns}: an undelivered receipt must execute in the first spice block",
                );
                undelivered_receipt_ids_by_receiving_shard
                    .entry(shard_layout.account_id_to_shard_id(receipt.receiver_id()))
                    .or_default()
                    .push(*receipt.receipt_id());
            }
        }
        for (receiving_shard_id, undelivered_receipt_ids) in
            undelivered_receipt_ids_by_receiving_shard
        {
            // The receipts were pending for the receiving shard at the boundary: sent
            // after its last pre-spice chunk, so no pre-spice chunk of it applied them.
            let receiving_shard_index = shard_layout.get_shard_index(receiving_shard_id).unwrap();
            let receiving_last_included_height = last_included_height(receiving_shard_index);
            assert!(receiving_last_included_height < last_pre_spice.header().height());
            let pending_receipt_ids: HashSet<CryptoHash> = get_incoming_receipts_for_shard(
                &chain.chain_store,
                epoch_manager,
                receiving_shard_id,
                &shard_layout,
                *last_pre_spice.hash(),
                receiving_last_included_height,
                ReceiptFilter::TargetShard,
            )
            .unwrap()
            .iter()
            .flat_map(|response| response.1.iter())
            .flat_map(|proof| proof.0.iter())
            .map(|receipt| *receipt.receipt_id())
            .collect();
            for receipt_id in &undelivered_receipt_ids {
                assert!(
                    pending_receipt_ids.contains(receipt_id),
                    "{patterns}: receipt {receipt_id} carried to shard {receiving_shard_id} must \
                     be pending for it at the boundary",
                );
            }

            // A validator that did not apply the receiving shard's first spice chunk
            // endorsed its certified result, so it validated the delivery from the witness.
            let certified = chain
                .spice_core_reader
                .get_execution_results_by_shard_id(first_spice.header())
                .unwrap()
                .remove(&receiving_shard_id)
                .unwrap();
            let shard_uid = ShardUId::from_shard_id_and_layout(receiving_shard_id, &shard_layout);
            let stateless_endorsers = env
                .node_datas
                .iter()
                .enumerate()
                .filter(|(index, _)| {
                    env.node(*index)
                        .client()
                        .chain
                        .chain_store
                        .get_chunk_extra(first_spice.hash(), &shard_uid)
                        .is_err()
                })
                .filter(|(_, data)| {
                    chain
                        .spice_core_reader
                        .get_endorsement(first_spice.hash(), receiving_shard_id, &data.account_id)
                        .is_some_and(|endorsement| {
                            endorsement.execution_result_hash == certified.compute_hash()
                        })
                })
                .count();
            assert!(
                stateless_endorsers > 0,
                "{patterns}: no validator endorsed shard {receiving_shard_id}'s first spice chunk from its witness",
            );
        }

        let expected_target_balance_increase =
            amount.checked_mul(heights_with_transfers as u128).unwrap();
        for ((_, target), initial_balance) in transfers.iter().zip(&initial_target_balances) {
            let final_balance = node.view_account_query(target).unwrap().amount;
            assert_eq!(
                final_balance.checked_sub(*initial_balance).unwrap(),
                expected_target_balance_increase,
                "{patterns}: {target} must receive each transfer exactly once",
            );
        }
        assert_boundary_results_validated(&env, &last_pre_spice);
    }
}
