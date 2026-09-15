//! Tests for crossing the pre-spice -> spice activation boundary.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::is_spice_activation_parent;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::{create_test_signer, pre_spice_protocol_version};
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::ShardId;
use near_primitives::types::{AccountId, AccountInfo, Balance, Gas};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::num_rational::Rational32;
use std::collections::{HashMap, HashSet};

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

/// The upgrade with every chunk missing at the activation parent
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_missing_chunks_at_boundary() {
    run_protocol_upgrade_to_spice(BoundaryChunkDrops::AllAtParent);
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
    /// Every shard's chunk missing at the activation parent.
    AllAtParent,
    /// One shard's chunk missing at the activation parent; the other's missing at
    /// the parent and the block before it.
    Staggered,
}

fn run_protocol_upgrade_to_spice(drops: BoundaryChunkDrops) {
    init_test_logger();
    let num_producers = 2;
    let mut env = setup_upgrading_chain(num_producers, 2);
    let user = create_account_id("user");
    let receiver = create_account_id("validator1");

    // Trickle a transfer every block, so the blocks around the boundary burn gas:
    // the supply identity below must subtract a real burn to mean anything.
    let mut crossed = false;
    let mut boundary_height = None;
    // Per producer node: pause chunk production at the first head height, resume at
    // the second. Chunks for a height are produced one block ahead, so pausing at
    // head H skips chunks from height H + 2 on.
    let mut drop_schedule: HashMap<usize, (u64, u64)> = HashMap::new();
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
                // the activation parent.
                let epoch_start_height =
                    epoch_manager.get_epoch_start_height(head_block.hash()).unwrap();
                let parent_height = epoch_start_height + EPOCH_LENGTH - 1;
                boundary_height = Some(parent_height);
                match drops {
                    BoundaryChunkDrops::None => unreachable!(),
                    BoundaryChunkDrops::AllAtParent => {
                        for i in 0..num_producers {
                            drop_schedule.insert(i, (parent_height - 2, parent_height - 1));
                        }
                    }
                    BoundaryChunkDrops::Staggered => {
                        // One producer per shard; the long-gap shard's producer
                        // pauses one height earlier, so its shard also misses the
                        // block before the activation parent.
                        let epoch_id = epoch_manager.get_epoch_id(head_block.hash()).unwrap();
                        let node_for_shard = |shard_id| {
                            let account = epoch_manager
                                .get_chunk_producer_info(&ChunkProductionKey {
                                    shard_id,
                                    epoch_id,
                                    height_created: parent_height,
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
                        drop_schedule.insert(short_node, (parent_height - 2, parent_height - 1));
                        drop_schedule.insert(long_node, (parent_height - 3, parent_height - 1));
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
        assert_eq!(stops_sent.len(), drop_schedule.len());
        assert_eq!(resumes_sent.len(), drop_schedule.len());
    }

    // Locate the first spice block on the chain and its pre-spice parent.
    let (activation_parent, parent_supply, parent_burnt, num_shards) = {
        let node = env.rpc_node();
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
        match drops {
            BoundaryChunkDrops::None => {
                let grandparent =
                    node.client().chain.get_block(parent.header().prev_hash()).unwrap();
                assert_ne!(
                    grandparent.header().next_gas_price(),
                    parent.header().next_gas_price(),
                    "gas price must move at the boundary for the era convention to matter",
                );
            }
            BoundaryChunkDrops::AllAtParent | BoundaryChunkDrops::Staggered => {
                assert_eq!(Some(parent.header().height()), boundary_height);
                assert!(
                    parent.header().chunk_mask().iter().all(|mask| !*mask),
                    "every chunk must be missing at the activation parent",
                );
            }
        }
        if drops == BoundaryChunkDrops::Staggered {
            let grandparent = node.client().chain.get_block(parent.header().prev_hash()).unwrap();
            let long_gap_index = shard_layout.get_shard_index(long_gap_shard_id.unwrap()).unwrap();
            let mask = grandparent.header().chunk_mask();
            assert!(!mask[long_gap_index], "long-gap shard must be missing before the parent");
            assert!(
                mask.iter().enumerate().all(|(i, present)| *present || i == long_gap_index),
                "only the long-gap shard may be missing before the parent",
            );
        }
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
    env.rpc_runner().run_until_head_height(parent_height + 3 * EPOCH_LENGTH);

    let node = env.rpc_node();
    assert!(node.head_block().is_spice_block());
    let final_execution_head =
        node.client().chain.chain_store.spice_final_execution_head().unwrap();
    assert!(
        final_execution_head.height > parent_height,
        "execution must advance past the boundary",
    );

    // The certifying block: the first block whose core statements complete the
    // activation parent's execution results.
    let mut chain_blocks = Vec::new();
    let mut block = node.head_block();
    while block.header().height() > parent_height {
        let prev_hash = *block.header().prev_hash();
        chain_blocks.push(block);
        block = node.client().chain.get_block(&prev_hash).unwrap();
    }
    chain_blocks.reverse();

    // Strict crossing: from the activation parent to the head no height is skipped
    // and no chunk goes missing — the boundary costs nothing in liveness.
    let mut expected_height = parent_height;
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
        for (chunk_id, execution_result) in block.spice_core_statements().iter_execution_results() {
            let burnt = burnt_by_block.entry(chunk_id.block_hash).or_default();
            *burnt = burnt.checked_add(execution_result.chunk_extra.balance_burnt()).unwrap();
            let shards = shards_by_block.entry(chunk_id.block_hash).or_default();
            shards.insert(chunk_id.shard_id);
            if shards.len() == num_shards && chunk_id.block_hash == *activation_parent.hash() {
                // Everything completing does so in this block: sum the completed
                // blocks' burns after finishing this block's statements.
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
    let certifying_block = certifying_block.expect("the activation parent must certify");

    // The parent's certified burn is exactly what its pre-spice apply burned —
    // entering the supply exactly once, at the certifying block.
    assert_eq!(burnt_by_block[activation_parent.hash()], parent_burnt);
    let before_certifying =
        node.client().chain.get_block(certifying_block.header().prev_hash()).unwrap();
    assert_eq!(
        before_certifying.header().total_supply(),
        parent_supply,
        "supply must be untouched until the activation parent certifies",
    );
    assert_eq!(
        certifying_block.header().total_supply(),
        parent_supply.checked_sub(expected_drop).unwrap(),
        "the certifying block must subtract exactly the newly certified blocks' burns",
    );
}

/// Kill a node when its head is the activation parent and restart it once the other
/// nodes have advanced into spice. Catching up re-runs the activation seeding, which
/// must be idempotent, and the executor's `start_actor` recovery must re-bootstrap
/// the boundary so the node follows the chain across it without panicking.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_restart_mid_boundary() {
    init_test_logger();

    // Enough validators that the chain keeps making progress while one is down.
    let mut env = setup_upgrading_chain(4, 4);

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

/// The upgrade with chunk-producer shard assignments shuffled every epoch, so shard
/// tracking rotates exactly at the boundary: a producer that applied the activation
/// parent's chunk of a shard need not track that shard under spice, and vice versa.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_with_shard_rotation() {
    init_test_logger();

    let epoch_length = 10;
    let num_producers = 4;
    let accounts: Vec<AccountId> =
        (0..num_producers).map(|i| format!("validator{i}").parse().unwrap()).collect();
    let validators: Vec<AccountInfo> = accounts
        .iter()
        .map(|account_id| AccountInfo {
            public_key: create_test_signer(account_id.as_str()).public_key(),
            account_id: account_id.clone(),
            amount: Balance::from_near(100),
        })
        .collect();
    let validators_spec =
        ValidatorsSpec::raw(validators, num_producers, num_producers, num_producers);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(ShardLayout::multi_shard(2, 0))
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .protocol_version(pre_spice_protocol_version())
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
            ProtocolFeature::Spice.protocol_version(),
        ))
        .build();

    // Cross the boundary, then locate the activation parent under the head.
    env.node_runner(0).run_until(|node| node.head_block().is_spice_block(), Duration::seconds(120));
    let activation_parent = {
        let node = env.node(0);
        let chain_store = node.client().chain.chain_store();
        let mut header = node.head_block().header().clone();
        while header.is_spice() {
            header = BlockHeader::clone(&chain_store.get_block_header(header.prev_hash()).unwrap());
        }
        header
    };
    let boundary_height = activation_parent.height();

    // The shuffle must actually rotate tracking at the boundary, or this test shows
    // nothing: some shard's chunk-producer set has to change across it.
    {
        let node = env.node(0);
        let epoch_manager = node.client().epoch_manager.clone();
        assert!(
            is_spice_activation_parent(epoch_manager.as_ref(), activation_parent.hash()).unwrap()
        );
        let pre_spice_epoch_id = activation_parent.epoch_id();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(activation_parent.hash()).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(pre_spice_epoch_id).unwrap();
        let rotated = shard_layout.shard_ids().any(|shard_id| {
            epoch_manager.get_epoch_chunk_producers_for_shard(pre_spice_epoch_id, shard_id).unwrap()
                != epoch_manager
                    .get_epoch_chunk_producers_for_shard(&spice_epoch_id, shard_id)
                    .unwrap()
        });
        assert!(rotated, "the shuffle left every shard's chunk-producer set unchanged");
    }

    // Certification must cross the boundary: the rotated-in producers bootstrap and
    // distribute the activation parent's receipts and witnesses.
    env.node_runner(0).run_until_certified(boundary_height + 2);
}

/// The deposit each trickled transfer carries.
const TRICKLE_AMOUNT: Balance = Balance::from_yoctonear(1);

/// What [`cross_boundary_with_transfer_trickle`] observed.
struct BoundaryTrickle {
    boundary_height: u64,
    head_height: u64,
    initial_receiver_balance: Balance,
    /// Inclusion height per submitted transfer; each was included exactly once,
    /// at least one of them at the activation parent.
    inclusion_heights: HashMap<CryptoHash, u64>,
}

/// Runs the upgrade submitting a cross-shard "user" -> "receiver" transfer of
/// [`TRICKLE_AMOUNT`] every block until the chain crosses the boundary, then lets
/// certification catch up past every transfer's receipt.
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
    let mut crossed = false;
    for _ in 0..10 * EPOCH_LENGTH {
        let tx = env.rpc_node().tx_send_money(&sender, &receiver, TRICKLE_AMOUNT);
        submitted.push(tx.get_hash());
        env.rpc_node().submit_tx(tx);
        env.rpc_runner().run_for_number_of_blocks(1);
        if env.rpc_node().head_block().is_spice_block() {
            crossed = true;
            break;
        }
    }
    assert!(crossed, "the chain must cross the boundary");

    // Locate the activation parent, then run on and let execution catch up past
    // every receipt of the submitted transfers.
    let boundary_height = {
        let node = env.rpc_node();
        let chain_store = node.client().chain.chain_store();
        let mut header = node.head_block().header().clone();
        while header.is_spice() {
            header = BlockHeader::clone(&chain_store.get_block_header(header.prev_hash()).unwrap());
        }
        header.height()
    };
    env.rpc_runner().run_until_head_height(boundary_height + 2 * EPOCH_LENGTH);
    let head_height = env.rpc_node().head().height;
    env.rpc_runner().run_until_certified(head_height);

    // Scan every block's new chunks for the submitted transactions.
    let node = env.rpc_node();
    let submitted: HashSet<CryptoHash> = submitted.into_iter().collect();
    let mut multi_inclusions: HashMap<CryptoHash, Vec<u64>> = HashMap::new();
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
    assert!(
        inclusion_heights.values().any(|height| *height == boundary_height),
        "some transfer must be included at the activation parent, so its receipt is \
         in flight across the boundary",
    );
    BoundaryTrickle { boundary_height, head_height, initial_receiver_balance, inclusion_heights }
}

/// A cross-shard transfer in flight at the boundary: a transaction included in a
/// pre-spice chunk whose receipt executes in the first spice epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_protocol_upgrade_to_spice_receipt_in_flight() {
    init_test_logger();
    let mut env = setup_upgrading_chain(2, 2);
    let trickle = cross_boundary_with_transfer_trickle(&mut env);

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
    let mut env = setup_upgrading_chain(2, 2);
    let trickle = cross_boundary_with_transfer_trickle(&mut env);
    let receiver = create_account_id("receiver");
    let boundary_height = trickle.boundary_height;

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
    let (boundary_tx, _) = trickle
        .inclusion_heights
        .iter()
        .find(|(_, height)| **height == boundary_height)
        .expect("the trickle asserted an inclusion at the activation parent");
    let tx_outcome = node.execution_outcome_with_proof(*boundary_tx);
    let receipt_id = node.tx_receipt_id(*boundary_tx);
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
