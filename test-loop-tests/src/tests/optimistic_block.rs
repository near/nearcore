use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::optimistic_block::OptimisticBlock;
#[cfg(feature = "test_features")]
use near_primitives::optimistic_block::OptimisticBlockAdvType;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeight};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;

fn get_builder(num_shards: usize) -> TestLoopBuilder {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let epoch_length = 100;
    let accounts =
        (0..3).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &accounts.iter().map(|account_id| account_id.as_str()).collect_vec(),
        &[],
    );

    let shard_layout = ShardLayout::multi_shard(num_shards as u64, 1);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients)
}

#[test]
fn test_optimistic_block() {
    let num_shards = 3;
    let mut env: TestLoopEnv = get_builder(num_shards).build().warmup();
    env.test_loop.run_for(Duration::seconds(10));

    {
        let chain =
            &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client.chain;
        // Under normal block processing, there can be only one optimistic
        // block waiting to be processed.
        assert!(chain.optimistic_block_chunks.num_blocks() <= 1);
        // Under normal block processing, number of waiting chunks can't exceed
        // delta between the highest block height and the final block height
        // (normally 3), multiplied by the number of shards.
        assert!(chain.optimistic_block_chunks.num_chunks() <= 3 * num_shards);
        // There should be at least one optimistic block result in the cache.
        assert!(chain.apply_chunk_results_cache.len() > 0);
        // Optimistic block result should be used at every height.
        // We do not process the first 2 blocks of the network.
        let expected_hits = chain.head().map_or(0, |t| t.height - 2);
        assert!(chain.apply_chunk_results_cache.hits() >= (expected_hits as usize));
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[cfg(feature = "test_features")]
/// Create an invalid optimistic block based on the adversarial type.
fn make_invalid_ob(env: &TestLoopEnv, adv_type: OptimisticBlockAdvType) -> OptimisticBlock {
    let client = &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client;

    let epoch_manager = &client.epoch_manager;
    let head = client.chain.head().unwrap();
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + 1;
    let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();

    // Get producer client
    let client_data =
        &env.node_datas.iter().find(|data| data.account_id == block_producer).unwrap();
    let client = &env.test_loop.data.get(&client_data.client_sender.actor_handle()).client;
    let chain = &client.chain;

    let prev_header = chain.get_block_header(parent_hash).unwrap();

    let validator_signer = client.validator_signer.get().unwrap();

    OptimisticBlock::adv_produce(
        &prev_header,
        height,
        &*validator_signer,
        client.clock.clone(),
        None,
        adv_type,
    )
}

#[test]
#[cfg(feature = "test_features")]
/// Check if validation fails on malformed optimistic blocks.
fn test_invalid_optimistic_block() {
    let mut env = get_builder(3).build().warmup();
    env.test_loop.run_for(Duration::seconds(10));
    let chain =
        &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client.chain;
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(&env, OptimisticBlockAdvType::InvalidVrfValue))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(&env, OptimisticBlockAdvType::InvalidVrfProof))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(
                &env,
                OptimisticBlockAdvType::InvalidRandomValue
            ))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(
                &env,
                OptimisticBlockAdvType::InvalidTimestamp(0)
            ))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(
                &env,
                OptimisticBlockAdvType::InvalidPrevBlockHash
            ))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(
                &env,
                OptimisticBlockAdvType::InvalidSignature
            ))
            .is_err()
    );
    assert!(
        &chain
            .check_optimistic_block(&make_invalid_ob(&env, OptimisticBlockAdvType::Normal))
            .is_ok()
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Returns the block height within the epoch where the next producer is not the same.
/// This is due to the limitation of the Block message dropper.
fn get_height_to_skip_and_next_producer(env: &TestLoopEnv) -> (BlockHeight, ValidatorStake) {
    let client = &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client;
    let chain = &client.chain;
    let head = chain.head().unwrap();
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let epoch_end_height = epoch_manager
        .get_estimated_next_epoch_start(&&epoch_manager.get_block_info(parent_hash).unwrap())
        .unwrap();

    let mut prev_block_producer =
        epoch_manager.get_block_producer_info(&epoch_id, head.height + 1).unwrap();
    for height in head.height + 2..(epoch_end_height - 1) {
        let block_producer = epoch_manager.get_block_producer_info(&epoch_id, height).unwrap();
        if block_producer != prev_block_producer {
            return (height - 1, block_producer);
        }
        prev_block_producer = block_producer;
    }
    panic!(
        "No block producer change found. Either the epoch is too short or the test setup is incorrect."
    );
}

/// Register a handler to track optimistic blocks received by the producers.
fn track_optimistic_blocks(
    env: &mut TestLoopEnv,
    produced_optimistic_blocks: Arc<Mutex<HashMap<BlockHeight, OptimisticBlock>>>,
) {
    for data in &env.node_datas {
        let peer_actor = env.test_loop.data.get_mut(&data.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new({
            let ob_store = produced_optimistic_blocks.clone();
            move |request: NetworkRequests| {
                if let NetworkRequests::OptimisticBlock { optimistic_block } = &request {
                    ob_store
                        .lock()
                        .unwrap()
                        .insert(optimistic_block.height(), optimistic_block.clone());
                };
                Some(request)
            }
        }));
    }
}

#[test]
/// Test that the optimistic block production does not break after a missing block.
fn test_optimistic_block_after_missing_block() {
    let num_shards = 4;
    let mut env: TestLoopEnv = get_builder(num_shards).build().warmup();

    // Hash of optimistic blocks by height
    let producer_optimistic_blocks: Arc<Mutex<HashMap<BlockHeight, OptimisticBlock>>> =
        Arc::new(Mutex::new(HashMap::new()));
    track_optimistic_blocks(env.borrow_mut(), producer_optimistic_blocks.clone());

    env.test_loop.run_for(Duration::seconds(10));

    let (height_to_skip, producer_after_skip) = get_height_to_skip_and_next_producer(&env);
    tracing::info!(target: "test", ?height_to_skip, ?producer_after_skip, "Skipping block at height");
    env = env.drop(DropCondition::BlocksByHeight([height_to_skip].into_iter().collect()));

    let client_handle = env.node_datas[0].client_sender.actor_handle();

    let (hit_count_before_skip, height_before_skip) = {
        let chain = &env.test_loop.data.get(&client_handle).client.chain;
        (chain.apply_chunk_results_cache.hits(), chain.head().unwrap().height)
    };

    // Wait for a few blocks after the missed block for optimistic blocks to be used again.
    let wait_blocks_after_skip = 5;
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height
                > (height_to_skip + wait_blocks_after_skip)
        },
        Duration::seconds(20),
    );

    let hit_count_after_skip = {
        let chain = &env.test_loop.data.get(&client_handle).client.chain;
        chain.apply_chunk_results_cache.hits()
    };

    // Remove the hits before the skip happened.
    let blocks_before_skip = (height_to_skip - height_before_skip) as usize;
    let hits_after_skip = hit_count_after_skip - hit_count_before_skip - blocks_before_skip;
    assert!(hits_after_skip > 0, "Optimistic block not used after a missing block");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));

    {
        let optimistic_block_heights =
            producer_optimistic_blocks.lock().unwrap().keys().cloned().collect::<HashSet<_>>();
        tracing::info!(target: "test_loop", ?optimistic_block_heights, "Optimistic block received");
        ((height_to_skip + 2)..(height_to_skip + wait_blocks_after_skip))
            .for_each(|height| assert!(optimistic_block_heights.contains(&height)));
    }
}
