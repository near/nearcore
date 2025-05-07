#[cfg(feature = "test_features")]
use std::sync::Arc;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
#[cfg(feature = "test_features")]
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
#[cfg(feature = "test_features")]
use near_primitives::optimistic_block::{OptimisticBlock, OptimisticBlockAdvType};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, BlockHeight};
#[cfg(feature = "test_features")]
use near_primitives::validator_signer::ValidatorSigner;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;

fn get_builder(num_shards: usize) -> TestLoopBuilder {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let epoch_length = 100;
    // Keep it above 3 to prevent missing blocks from stalling the network.
    let accounts =
        (0..4).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
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
        client.clock.now_utc().unix_timestamp_nanos() as u64,
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
/// Returns the height, the producer at the height and the next producer.
/// This is due to the limitation of the Block message dropper.
fn get_height_to_skip_and_producers(
    env: &TestLoopEnv,
) -> (BlockHeight, ValidatorStake, ValidatorStake) {
    let client = &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client;
    let chain = &client.chain;
    let head = chain.head().unwrap();
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let epoch_end_height = epoch_manager
        .get_estimated_next_epoch_start(&&epoch_manager.get_block_info(parent_hash).unwrap())
        .unwrap();

    let start_height = head.height + 3;
    let mut prev_block_producer =
        epoch_manager.get_block_producer_info(&epoch_id, start_height - 1).unwrap();
    for height in start_height..(epoch_end_height - 1) {
        let block_producer = epoch_manager.get_block_producer_info(&epoch_id, height).unwrap();
        if block_producer != prev_block_producer {
            return (height - 1, prev_block_producer, block_producer);
        }
        prev_block_producer = block_producer;
    }
    panic!(
        "No block producer change found. Either the epoch is too short or the test setup is incorrect."
    );
}

#[test]
/// Test that the optimistic block production does not break after a missing block.
fn test_optimistic_block_after_missing_block() {
    let num_shards = 3;
    let mut env: TestLoopEnv = get_builder(num_shards).build().warmup();

    env.test_loop.run_for(Duration::seconds(10));

    let (height_to_skip, producer, next_producer) = get_height_to_skip_and_producers(&env);
    tracing::info!(target: "test", ?height_to_skip, ?producer, "Skipping block at height");
    env = env.drop(DropCondition::BlocksByHeight([height_to_skip].into_iter().collect()));

    let client_handle = &env
        .get_node_data_by_account_id(next_producer.account_id())
        .unwrap()
        .client_sender
        .actor_handle();

    let (hit_count_before_skip, height_before_skip) =
        get_hit_count_and_height(&env, &next_producer);

    // Wait for a few blocks after the missed block for optimistic blocks to be used again.
    let wait_blocks_after_skip = 5;
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height
                > (height_to_skip + wait_blocks_after_skip)
        },
        Duration::seconds(10),
    );

    let (hit_count_after_skip, height_after_skip) = get_hit_count_and_height(&env, &next_producer);

    let hit_delta = hit_count_after_skip - hit_count_before_skip;
    let height_delta = (height_after_skip - height_before_skip) as usize;
    assert!(
        height_delta == hit_delta + 2,
        "Block was skipped on height h={}. OptimisticBlock was not supposed to be used on height h and h+1. It was used {} out of {} times.",
        height_to_skip,
        hit_delta,
        height_delta,
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[cfg(feature = "test_features")]
fn alter_optimistic_block_at_height(
    env: &mut TestLoopEnv,
    height: BlockHeight,
    signer: Arc<ValidatorSigner>,
) {
    use near_primitives::optimistic_block;

    for data in &env.node_datas {
        let peer_actor = env.test_loop.data.get_mut(&data.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new({
            let validator_signer = signer.clone();
            move |request: NetworkRequests| {
                if let NetworkRequests::OptimisticBlock { optimistic_block } = &request {
                    if optimistic_block.height() == height {
                        let altered_ob = optimistic_block::OptimisticBlock::alter(
                            optimistic_block,
                            &validator_signer,
                            OptimisticBlockAdvType::InvalidTimestamp(
                                optimistic_block.block_timestamp() - 15000000,
                            ),
                        );
                        return Some(NetworkRequests::OptimisticBlock {
                            optimistic_block: altered_ob,
                        });
                    }
                };
                Some(request)
            }
        }));
    }
}

fn get_hit_count_and_height(env: &TestLoopEnv, producer: &ValidatorStake) -> (usize, BlockHeight) {
    let client_handler = &env
        .get_node_data_by_account_id(producer.account_id())
        .unwrap()
        .client_sender
        .actor_handle();
    let chain = &env.test_loop.data.get(&client_handler).client.chain;
    (chain.apply_chunk_results_cache.hits(), chain.head().unwrap().height)
}

#[test]
#[cfg(feature = "test_features")]
/// Test that the optimistic block outcome is dropped on other nodes when
/// the optimistic block content is different than the block.
/// In this test we change the block timestamp of the optimistic block that
/// is shared with the other nodes.
fn test_optimistic_block_with_invalidated_outcome() {
    let num_shards = 3;
    let mut env: TestLoopEnv = get_builder(num_shards).build().warmup();

    env.test_loop.run_for(Duration::seconds(10));

    let (height_to_skip, producer, next_producer) = get_height_to_skip_and_producers(&env);

    tracing::info!(target: "test", ?height_to_skip, ?producer, "Alter optimistic block at height");

    let producer_client_handle = &env
        .get_node_data_by_account_id(producer.account_id())
        .unwrap()
        .client_sender
        .actor_handle();
    let affected_client_handle = &env
        .get_node_data_by_account_id(next_producer.account_id())
        .unwrap()
        .client_sender
        .actor_handle();

    let client = &env.test_loop.data.get(&producer_client_handle).client;
    let signer = client.validator_signer.get().unwrap();
    alter_optimistic_block_at_height(&mut env, height_to_skip, signer);

    // Wait for a few blocks after the invalid OB to confirm the miss.
    let (producer_node_ob_hit_count_before, producer_node_height_before) =
        get_hit_count_and_height(&env, &producer);
    let (affected_node_ob_hit_count_before, affected_node_height_before) =
        get_hit_count_and_height(&env, &next_producer);

    let wait_blocks_after_skip = 5;
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&affected_client_handle).client.chain.head().unwrap().height
                > (height_to_skip + wait_blocks_after_skip)
        },
        Duration::seconds(20),
    );

    let (producer_node_ob_hit_count_after, producer_node_height_after) =
        get_hit_count_and_height(&env, &producer);
    let (affected_node_ob_hit_count_after, affected_node_height_after) =
        get_hit_count_and_height(&env, &next_producer);

    let affected_node_hit_delta =
        affected_node_ob_hit_count_after - affected_node_ob_hit_count_before;
    let affected_node_height_delta =
        (affected_node_height_after - affected_node_height_before) as usize;
    assert!(
        affected_node_height_delta == affected_node_hit_delta + 1,
        "We must have one miss corresponding to the invalid OptimisticBlock outcome"
    );

    let producer_node_hit_delta =
        producer_node_ob_hit_count_after - producer_node_ob_hit_count_before;
    let producer_node_height_delta =
        (producer_node_height_after - producer_node_height_before) as usize;
    assert!(
        producer_node_hit_delta >= producer_node_height_delta,
        "Producer of the invalid OptimisticBlock must have all hits because it itself uses correct OptimisticBlock"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
