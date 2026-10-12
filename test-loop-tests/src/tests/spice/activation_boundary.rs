//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::spice::boundary_synthesis::execution_result_and_receipt_proofs_from_pre_spice_apply;
use near_chain::{Block, ChainStoreAccess};
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::{Balance, Gas, ShardId};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;
use std::collections::{BTreeMap, HashMap, HashSet};

const EPOCH_LENGTH: u64 = 5;

/// A chain starting pre-spice that votes straight up to spice, so it crosses the
/// activation boundary a couple of epochs in. The GC window is wide so the blocks a
/// killed node needs to catch up on are still there.
fn setup_upgrading_chain(num_producers: usize, num_chunk_validators: usize) -> TestLoopEnv {
    TestLoopBuilder::new()
        .validators(num_producers, num_chunk_validators)
        .enable_rpc()
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .protocol_version(pre_spice_protocol_version())
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        // Zero inflation, so the supply identity at the boundary is exact with no
        // minting term. A real gas price so transactions burn tokens and the identity subtracts a real burn.
        .max_inflation_rate(Rational32::new(0, 1))
        .gas_prices(Balance::from_yoctonear(100_000_000), Balance::from_yoctonear(10_000_000_000))
        .gas_price_adjustment_rate(Rational32::new(1, 10))
        .gas_limit(Gas::from_gigagas(400))
        .gc_num_epochs_to_keep(20)
        .add_user_account(&create_account_id("user"), Balance::from_near(100))
        // On the other shard than "user" (the layout splits at "test1"), so
        // transfers between the two cross shards.
        .add_user_account(&create_account_id("receiver"), Balance::from_near(100))
        .build()
}

/// Basic upgrade test: a pre-spice chain votes itself into spice
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice() {
    run_protocol_upgrade_to_spice(BoundaryChunkDrops::None);
}

/// The upgrade with every chunk missing at the last pre-spice block
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_missing_chunks_at_boundary() {
    run_protocol_upgrade_to_spice(BoundaryChunkDrops::AllAtLastPreSpice);
}

/// The upgrade with staggered gaps straddling the anchor
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_staggered_missing_chunks_at_boundary() {
    run_protocol_upgrade_to_spice(BoundaryChunkDrops::Staggered);
}

#[derive(Clone, Copy, PartialEq)]
enum BoundaryChunkDrops {
    None,
    /// Every shard's chunk missing at the last pre-spice block.
    AllAtLastPreSpice,
    /// One shard's chunk missing at the last pre-spice block; the other's missing at
    /// the last pre-spice block and the block before it.
    Staggered,
}

fn run_protocol_upgrade_to_spice(drops: BoundaryChunkDrops) {
    init_test_logger();
    let num_producers = 2;
    let mut env = setup_upgrading_chain(num_producers, 2);
    let user = create_account_id("user");
    let receiver = create_account_id("receiver");

    // Trickle a transfer every block, so the blocks around the boundary burn gas:
    // the supply identity below must subtract a real burn to mean anything.
    let mut crossed = false;
    let mut boundary_height = None;
    // Per producer node: pause chunk production at the first head height, resume at
    // the second. Chunks for a height are produced one block ahead, so pausing at
    // head H skips chunks from height H + 2 on.
    let mut drop_schedule: BTreeMap<usize, (u64, u64)> = BTreeMap::new();
    let mut long_gap_shard_id = None;
    let mut stops_sent = HashSet::new();
    let mut resumes_sent = HashSet::new();
    for _ in 0..10 * EPOCH_LENGTH {
        let tx = env.rpc_node().tx_send_money(&user, &receiver, Balance::from_yoctonear(1));
        env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(1);
        let head_block = env.rpc_node().head_block();
        if head_block.is_spice_block() {
            crossed = true;
            break;
        }
        if drops != BoundaryChunkDrops::None && boundary_height.is_none() {
            let epoch_manager = env.rpc_node().client().epoch_manager.clone();
            let next_protocol_version =
                epoch_manager.get_next_epoch_protocol_version(head_block.hash()).unwrap();
            if ProtocolFeature::Spice.enabled(next_protocol_version) {
                // The head is in the last pre-spice epoch, whose final block is
                // the last pre-spice block.
                let epoch_start_height =
                    epoch_manager.get_epoch_start_height(head_block.hash()).unwrap();
                let last_pre_spice_height = epoch_start_height + EPOCH_LENGTH - 1;
                boundary_height = Some(last_pre_spice_height);
                match drops {
                    BoundaryChunkDrops::None => unreachable!(),
                    BoundaryChunkDrops::AllAtLastPreSpice => {
                        for i in 0..num_producers {
                            drop_schedule
                                .insert(i, (last_pre_spice_height - 2, last_pre_spice_height - 1));
                        }
                    }
                    BoundaryChunkDrops::Staggered => {
                        // One producer per shard; the long-gap shard's producer
                        // pauses one height earlier, so its shard also misses the
                        // block before the last pre-spice block.
                        let epoch_id = epoch_manager.get_epoch_id(head_block.hash()).unwrap();
                        let node_for_shard = |shard_id| {
                            let account = epoch_manager
                                .get_chunk_producer_info(&ChunkProductionKey {
                                    shard_id,
                                    epoch_id,
                                    height_created: last_pre_spice_height,
                                })
                                .unwrap()
                                .take_account_id();
                            env.node_datas
                                .iter()
                                .position(|data| data.account_id == account)
                                .unwrap()
                        };
                        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
                        let shard_ids: Vec<_> = shard_layout.shard_ids().collect();
                        let (short_gap, long_gap) = (shard_ids[0], shard_ids[1]);
                        long_gap_shard_id = Some(long_gap);
                        let short_node = node_for_shard(short_gap);
                        let long_node = node_for_shard(long_gap);
                        assert_ne!(
                            short_node, long_node,
                            "staggering needs one producer per shard",
                        );
                        drop_schedule.insert(
                            short_node,
                            (last_pre_spice_height - 2, last_pre_spice_height - 1),
                        );
                        drop_schedule.insert(
                            long_node,
                            (last_pre_spice_height - 3, last_pre_spice_height - 1),
                        );
                    }
                }
            }
        }
        let head_height = head_block.header().height();
        for (&node_index, &(stop_height, resume_height)) in &drop_schedule {
            if head_height == stop_height && stops_sent.insert(node_index) {
                env.node_runner(node_index).send_adversarial_message(
                    NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
                );
            }
            if head_height == resume_height && resumes_sent.insert(node_index) {
                env.node_runner(node_index).send_adversarial_message(
                    NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::Valid),
                );
            }
        }
    }
    assert!(crossed, "chain never crossed the activation boundary");
    if drops != BoundaryChunkDrops::None {
        assert_eq!(
            stops_sent.len(),
            drop_schedule.len(),
            "head skipped a chunk-drop stop height; drop schedule {drop_schedule:?}",
        );
        assert_eq!(
            resumes_sent.len(),
            drop_schedule.len(),
            "head skipped a chunk-drop resume height; drop schedule {drop_schedule:?}",
        );
    }

    // Locate the first spice block on the chain and the last pre-spice block.
    let (last_pre_spice, last_pre_spice_supply, last_pre_spice_burnt, num_shards) = {
        let node = env.rpc_node();
        let mut first_spice = node.head_block();
        loop {
            let prev = node.client().chain.get_block(first_spice.header().prev_hash()).unwrap();
            if !prev.is_spice_block() {
                break;
            }
            first_spice = prev;
        }
        let last_pre_spice =
            node.client().chain.get_block(first_spice.header().prev_hash()).unwrap();

        // The last pre-spice block's burn, from the chunk extras its pre-spice apply wrote.
        let shard_layout = node
            .client()
            .epoch_manager
            .get_shard_layout(last_pre_spice.header().epoch_id())
            .unwrap();
        let mut last_pre_spice_burnt = Balance::ZERO;
        for shard_uid in shard_layout.shard_uids() {
            let chunk_extra = node
                .client()
                .chain
                .chain_store
                .get_chunk_extra(last_pre_spice.hash(), &shard_uid)
                .unwrap();
            last_pre_spice_burnt =
                last_pre_spice_burnt.checked_add(chunk_extra.balance_burnt()).unwrap();
        }
        assert!(
            last_pre_spice_burnt > Balance::ZERO,
            "the last pre-spice block must burn gas for the supply identity to be meaningful",
        );
        match drops {
            BoundaryChunkDrops::None => {
                let prev =
                    node.client().chain.get_block(last_pre_spice.header().prev_hash()).unwrap();
                // A pre-spice apply prices gas with the prev block's `next_gas_price`, a
                // spice apply with the block's own. If the two are equal, a boundary
                // replay using the wrong one would still certify.
                assert_ne!(
                    prev.header().next_gas_price(),
                    last_pre_spice.header().next_gas_price(),
                    "gas price must move at the boundary to tell the pre-spice and spice pricing apart",
                );
            }
            BoundaryChunkDrops::AllAtLastPreSpice | BoundaryChunkDrops::Staggered => {
                assert_eq!(Some(last_pre_spice.header().height()), boundary_height);
                assert!(
                    last_pre_spice.header().chunk_mask().iter().all(|mask| !*mask),
                    "every chunk must be missing at the last pre-spice block",
                );
            }
        }
        if drops == BoundaryChunkDrops::Staggered {
            let prev = node.client().chain.get_block(last_pre_spice.header().prev_hash()).unwrap();
            let long_gap_index = shard_layout.get_shard_index(long_gap_shard_id.unwrap()).unwrap();
            let mask = prev.header().chunk_mask();
            assert!(
                !mask[long_gap_index],
                "long-gap shard must be missing before the last pre-spice block"
            );
            assert!(
                mask.iter().enumerate().all(|(i, present)| *present || i == long_gap_index),
                "only the long-gap shard may be missing before the last pre-spice block",
            );
        }
        (
            last_pre_spice.clone(),
            last_pre_spice.header().total_supply(),
            last_pre_spice_burnt,
            shard_layout.num_shards() as usize,
        )
    };
    let last_pre_spice_height = last_pre_spice.header().height();

    // Two further spice epochs. The first spice epoch cannot end before the
    // last pre-spice block certifies, so getting here proves certification liveness.
    env.rpc_runner().run_until_head_height(last_pre_spice_height + 3 * EPOCH_LENGTH);

    let node = env.rpc_node();
    assert!(node.head_block().is_spice_block());
    let final_execution_head =
        node.client().chain.chain_store.spice_final_execution_head().unwrap();
    assert!(
        final_execution_head.height > last_pre_spice_height,
        "execution must advance past the boundary",
    );

    // The certifying block: the first block whose core statements complete the
    // last pre-spice block's execution results.
    let mut chain_blocks = Vec::new();
    let mut block = node.head_block();
    while block.header().height() > last_pre_spice_height {
        let prev_hash = *block.header().prev_hash();
        chain_blocks.push(block);
        block = node.client().chain.get_block(&prev_hash).unwrap();
    }
    chain_blocks.reverse();

    // Strict crossing: from the last pre-spice block to the head no height is skipped
    // and no chunk goes missing — the boundary costs nothing in liveness.
    let mut expected_height = last_pre_spice_height;
    for block in &chain_blocks {
        expected_height += 1;
        assert_eq!(
            block.header().height(),
            expected_height,
            "height skipped crossing the boundary",
        );
        assert!(
            block.header().chunk_mask().iter().all(|mask| *mask),
            "chunk missing at height {} crossing the boundary",
            block.header().height(),
        );
    }

    // Walk the core statements forward, tracking per block when its execution
    // results complete
    let mut shards_by_block: HashMap<CryptoHash, HashSet<ShardId>> = HashMap::new();
    let mut burnt_by_block: HashMap<CryptoHash, Balance> = HashMap::new();
    let mut certifying_block = None;
    let mut expected_drop = Balance::ZERO;
    'outer: for block in &chain_blocks {
        let completed_before: HashSet<CryptoHash> = shards_by_block
            .iter()
            .filter(|(_, shards)| shards.len() == num_shards)
            .map(|(block_hash, _)| *block_hash)
            .collect();
        assert!(
            completed_before.is_empty(),
            "blocks {completed_before:?} completed before the last pre-spice block certified",
        );
        for (chunk_id, execution_result) in block.spice_core_statements().iter_execution_results() {
            let burnt = burnt_by_block.entry(chunk_id.block_hash).or_default();
            *burnt = burnt.checked_add(execution_result.chunk_extra.balance_burnt()).unwrap();
            let shards = shards_by_block.entry(chunk_id.block_hash).or_default();
            shards.insert(chunk_id.shard_id);
            if shards.len() == num_shards && chunk_id.block_hash == *last_pre_spice.hash() {
                // No block completed in an earlier block (asserted above), so the
                // completed blocks' burns all enter the supply in this block.
                certifying_block = Some(block);
            }
        }
        if certifying_block.is_some() {
            for (block_hash, shards) in &shards_by_block {
                if shards.len() == num_shards {
                    expected_drop = expected_drop.checked_add(burnt_by_block[block_hash]).unwrap();
                }
            }
            break 'outer;
        }
    }
    let certifying_block = certifying_block.expect("the last pre-spice block must certify");

    // The last pre-spice block's certified burn is exactly what its pre-spice apply burned —
    // entering the supply exactly once, at the certifying block.
    assert_eq!(burnt_by_block[last_pre_spice.hash()], last_pre_spice_burnt);
    let before_certifying =
        node.client().chain.get_block(certifying_block.header().prev_hash()).unwrap();
    assert_eq!(
        before_certifying.header().total_supply(),
        last_pre_spice_supply,
        "supply must be untouched until the last pre-spice block certifies",
    );
    assert_eq!(
        certifying_block.header().total_supply(),
        last_pre_spice_supply.checked_sub(expected_drop).unwrap(),
        "the certifying block must subtract exactly the newly certified blocks' burns",
    );

    assert_boundary_results_validated(&env, &last_pre_spice);
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

    // Enough validators that the chain keeps making progress while one is down.
    let mut env = setup_upgrading_chain(4, 4);

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
    env.restart_node(&new_identifier, killed_node_state);
    // `restart_node` appends the new node rather than replacing the killed one, which
    // still holds an entry for the same account, so address the restarted node by index.
    let restarted_index = env.node_datas.len() - 1;
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
