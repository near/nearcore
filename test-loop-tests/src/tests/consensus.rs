use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use near_async::messaging::CanSend as _;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::client_actor::ClientActorInner;
use near_client::{BlockApproval, BlockResponse};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::types::{AccountId, BlockHeight, EpochId, NumSeats};
use parking_lot::RwLock;
use rand::{Rng as _, thread_rng};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;

/// Rotates three independent sets of block producers producing blocks with a very short epoch length.
/// Occasionally when an endorsement comes, make all the endorsers send a skip message far-ish into
/// the future, and delay the distribution of the block produced this way.
/// Periodically verify finality is not violated.
/// This test is designed to reproduce finality bugs on the epoch boundaries.
#[test]
fn ultra_slow_test_consensus_with_epoch_switches() {
    init_test_logger();

    let seed: u64 = thread_rng().r#gen();
    println!("RNG seed: {seed}. If test fails use it to find the issue.");
    let rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(seed);
    let rng = Arc::new(RwLock::new(rng));

    let validators: Vec<Vec<AccountId>> = [
        ["test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7", "test1.8"],
        ["test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6", "test2.7", "test2.8"],
        ["test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7", "test3.8"],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();
    let seats: NumSeats = validators[0].len().try_into().unwrap();

    let stake = ONE_NEAR;
    let mut runner = RotatingValidatorsRunner::new(stake, validators.clone());
    // With skips epochs may be longer and rotation validators may be unstable.
    runner.skip_assert_validators_rotation();
    runner.set_max_epoch_duration(Duration::seconds(100));

    let accounts = runner.all_validators_accounts();
    let epoch_length: u64 = 7;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::multi_shard(8, 3))
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        // With short epoch length sync hash may not be available for catchup so we track all
        // shards.
        .track_all_shards()
        .build()
        .warmup();

    let min_delay = 3;
    let handler = Arc::new(RwLock::new(NetworkHandlingData::new(&env, validators)));

    for node_datas in &env.node_datas {
        let from_whom = node_datas.account_id.clone();
        let peer_id = node_datas.peer_id.clone();

        let handler = handler.clone();
        let rng = rng.clone();

        let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
            let mut handler = handler.write();
            let mut rng = rng.write();

            match request {
                NetworkRequests::Block { ref block } => {
                    if !handler.all_blocks.contains_key(&block.header().height()) {
                        println!(
                            "BLOCK @{} HASH: {:?} EPOCH: {:?}, APPROVALS: {:?}",
                            block.header().height(),
                            block.hash(),
                            block.header().epoch_id(),
                            block
                                .header()
                                .approvals()
                                .iter()
                                .map(|x| if x.is_some() { 1 } else { 0 })
                                .collect::<Vec<_>>()
                        );
                    }
                    handler.all_blocks.insert(block.header().height(), block.clone());
                    handler.block_to_prev_block.insert(*block.hash(), *block.header().prev_hash());
                    handler.block_to_height.insert(*block.hash(), block.header().height());

                    if handler.largest_block_height / 20 < block.header().height() / 20 {
                        // Periodically verify the finality
                        println!("VERIFYING FINALITY CONDITIONS");
                        for block in handler.all_blocks.values() {
                            if let Some(prev_hash) = handler.block_to_prev_block.get(&block.hash())
                            {
                                if let Some(prev_height) = handler.block_to_height.get(prev_hash) {
                                    let cur_height = block.header().height();
                                    for f in &handler.final_block_heights {
                                        if f < &cur_height && f > prev_height {
                                            assert!(
                                                false,
                                                "{} < {} < {}",
                                                prev_height, f, cur_height
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if block.header().height() >= handler.largest_block_height + min_delay {
                        handler.largest_block_height = block.header().height();
                        handler.delayed_blocks_count += 1;
                        if handler.delayed_blocks.len() < 2 {
                            handler.delayed_blocks.push(block.clone());
                            return None;
                        }
                    }
                    handler.largest_block_height =
                        std::cmp::max(block.header().height(), handler.largest_block_height);

                    let mut new_delayed_blocks = vec![];
                    for delayed_block in &handler.delayed_blocks {
                        if delayed_block.hash() == block.hash() {
                            return Some(request);
                        }
                        if delayed_block.header().height() <= block.header().height() + 2 {
                            for (_, sender) in &handler.client_senders {
                                sender.send(BlockResponse {
                                    block: delayed_block.clone(),
                                    peer_id: peer_id.clone(),
                                    was_requested: true,
                                });
                            }
                        } else {
                            new_delayed_blocks.push(delayed_block.clone())
                        }
                    }
                    handler.delayed_blocks = new_delayed_blocks;

                    let mut heights = vec![];
                    let mut cur_hash = *block.hash();
                    while let Some(&height) = handler.block_to_height.get(&cur_hash) {
                        heights.push(height);
                        cur_hash = *handler.block_to_prev_block.get(&cur_hash).unwrap();
                        if heights.len() > 10 {
                            break;
                        }
                    }
                    // Use Doomslug finality, since without duplicate blocks at the same height
                    // it also provides safety under 1/3 faults
                    let is_final = heights.len() > 1 && heights[1] + 1 == heights[0];
                    println!(
                        "IS_FINAL: {} DELAYED: ({:?}) BLOCK: {} HISTORY: {:?}",
                        is_final,
                        handler
                            .delayed_blocks
                            .iter()
                            .map(|x| x.header().height())
                            .collect::<Vec<_>>(),
                        block.hash(),
                        heights,
                    );

                    if is_final {
                        handler.final_block_heights.insert(heights[1]);
                    }
                }
                NetworkRequests::Approval { ref approval_message } => {
                    // Identify who we are
                    let mut my_ord = 100;
                    for i in 0..handler.validators.len() {
                        for j in 0..handler.validators[i].len() {
                            if handler.validators[i][j] == from_whom {
                                my_ord = i * 8 + j;
                            }
                        }
                    }
                    assert_ne!(my_ord, 100);

                    // For each height we define `skips_per_height`, and each block producer sends
                    // skips that far into the future from that source height.
                    let source_height = match approval_message.approval.inner {
                        ApprovalInner::Endorsement(_) => {
                            if *handler.largest_target_height.entry(from_whom.clone()).or_default()
                                >= approval_message.approval.target_height
                                && my_ord % 8 >= 2
                            {
                                // We already manually sent a skip conflicting with this endorsement
                                // my_ord % 8 < 2 are two malicious actors in every epoch and they
                                // continue sending endorsements
                                return None;
                            }

                            approval_message.approval.target_height - 1
                        }
                        ApprovalInner::Skip(source_height) => source_height,
                    };

                    while source_height as usize >= handler.skips_per_height.len() {
                        handler.skips_per_height.push(if rng.gen_bool(0.8) {
                            0
                        } else {
                            // Blocks more than epoch_length away are likely to be in a different epoch
                            // which makes it harder to figure out correct block producer.
                            rng.gen_range(min_delay..epoch_length)
                        });
                    }

                    if handler.skips_per_height[source_height as usize] > 0
                        && approval_message.approval.target_height - source_height == 1
                    {
                        let delta = handler.skips_per_height[source_height as usize];
                        let mut approval = Approval {
                            target_height: source_height + delta as u64,
                            inner: ApprovalInner::Skip(source_height),
                            ..approval_message.approval.clone()
                        };
                        handler
                            .largest_target_height
                            .entry(from_whom.clone())
                            .and_modify(|height| {
                                *height = std::cmp::max(*height, approval.target_height as u64);
                            })
                            .or_insert(approval.target_height);

                        let signer = create_user_test_signer(&approval.account_id);
                        let message =
                            Approval::get_data_for_sig(&approval.inner, approval.target_height);
                        approval.signature = signer.sign(&message);

                        let target_height = approval.target_height;
                        let recipient = handler
                            .epoch_manager
                            .get_block_producer(&handler.current_epoch, target_height)
                            .unwrap();
                        let sender = handler.client_senders.get(&recipient).unwrap();
                        sender.send(BlockApproval(approval, peer_id.clone()));

                        // Do not send the endorsement for couple block producers in each epoch
                        // This is needed because otherwise the block with enough endorsements
                        // sometimes comes faster than the sufficient number of skips is created,
                        // (because the block producer themselves doesn't send the endorsement
                        // over the network, they have one more approval ready to produce their
                        // block than the block producer that will be at the later height). If
                        // such a block is indeed produced faster than all the skips are created,
                        // the participants who haven't sent their endorsements to be converted
                        // to skips change their head.
                        if my_ord % 8 < 2 {
                            return None;
                        }
                    }
                }
                _ => {}
            };
            Some(request)
        }));
    }

    const HEIGHT_GOAL: u64 = 140;
    let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
    runner.run_until(
        &mut env,
        |test_loop_data| {
            let mut handler = handler.write();

            let client = &test_loop_data.get(client_actor_handle).client;
            let head = client.chain.head().unwrap();
            let head_height = head.height;
            handler.current_epoch = head.epoch_id;
            head_height > HEIGHT_GOAL
        },
        Duration::seconds(2 * (HEIGHT_GOAL as i64)),
    );

    let delayed_blocks_count = handler.read().delayed_blocks_count;
    assert!(delayed_blocks_count > 0, "no blocks were delayed");
    println!("Delayed {} blocks", delayed_blocks_count);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

struct NetworkHandlingData {
    block_to_prev_block: HashMap<CryptoHash, CryptoHash>,
    block_to_height: HashMap<CryptoHash, u64>,

    all_blocks: BTreeMap<BlockHeight, Block>,
    final_block_heights: HashSet<BlockHeight>,

    largest_target_height: HashMap<AccountId, BlockHeight>,
    skips_per_height: Vec<u64>,
    largest_block_height: BlockHeight,
    delayed_blocks: Vec<Block>,
    delayed_blocks_count: u64,

    current_epoch: EpochId,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    client_senders: HashMap<AccountId, TestLoopSender<ClientActorInner>>,
    validators: Vec<Vec<AccountId>>,
}

impl NetworkHandlingData {
    fn new(env: &TestLoopEnv, validators: Vec<Vec<AccountId>>) -> Self {
        let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
        let client = &env.test_loop.data.get(client_actor_handle).client;

        let head = client.chain.head().unwrap();
        let client_senders: HashMap<AccountId, _> = env
            .node_datas
            .iter()
            .map(|datas| (datas.account_id.clone(), datas.client_sender.clone()))
            .collect();

        Self {
            block_to_prev_block: HashMap::new(),
            block_to_height: HashMap::new(),

            all_blocks: BTreeMap::new(),
            final_block_heights: HashSet::new(),

            largest_target_height: HashMap::new(),
            skips_per_height: Vec::new(),
            largest_block_height: head.height,
            delayed_blocks: Vec::new(),
            delayed_blocks_count: 0,

            current_epoch: head.epoch_id,
            epoch_manager: client.epoch_manager.clone(),
            client_senders,
            validators,
        }
    }
}
