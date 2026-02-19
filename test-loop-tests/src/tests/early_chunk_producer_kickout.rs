use near_async::time::Duration;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolFeature;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

/// End-to-end test for early chunk producer kickouts using the test-loop framework.
///
/// Exercises the full blacklist pipeline:
///   validator stops producing chunks
///   → misses accumulate in finalized EpochInfoAggregator
///   → compute_chunk_producer_blacklist triggers
///   → save_chunk_producers_for_header writes replacement to DBCol::ChunkProducers
///   → get_chunk_producer_info returns the replacement
///
/// Also verifies that the blacklist resets at the epoch boundary.
///
/// Requires "test_features" for AdvProduceChunksMode::StopProduce.
#[test]
fn slow_test_early_chunk_producer_kickout() {
    init_test_logger();

    let epoch_length = 100;
    let protocol_version = ProtocolFeature::EarlyChunkProducerKickout.protocol_version();
    let shard_layout = ShardLayout::multi_shard(2, 1);
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);
    let stopped_account: near_primitives::types::AccountId = "validator0".parse().unwrap();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(protocol_version)
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Phase 1: Stop chunk production on validator0 and run until the blacklist kicks in.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::StopProduce,
    ));

    // Run until we observe the blacklist replacing validator0.
    // With 4 equal-stake validators, each is assigned ~25% of chunks.
    // After ~80 blocks, validator0 has ~20 missed chunks per shard (all missed, 0% ratio),
    // exceeding the threshold (>=20 misses, <95% ratio).
    env.node_runner(0).run_until(
        |node| {
            let tip = node.head();
            if tip.height < 80 {
                return false;
            }
            let epoch_manager = &node.client().epoch_manager;
            let block = node.client().chain.get_block(&tip.last_block_hash).unwrap();
            let prev_hash = block.header().prev_hash();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();

            // Compare original assignment (sample_chunk_producer, no blacklist) with
            // actual assignment (DBCol::ChunkProducers, blacklist-aware).
            for shard_id in shard_layout.shard_ids() {
                let original = epoch_manager
                    .get_chunk_producer_for_height(&epoch_id, tip.height, shard_id)
                    .unwrap();
                let actual = epoch_manager.get_chunk_producer_info(prev_hash, shard_id).unwrap();
                if original.account_id() == &stopped_account
                    && actual.account_id() != &stopped_account
                {
                    // Found a height/shard where validator0 was originally assigned
                    // but the blacklist replaced it with someone else.
                    return true;
                }
            }
            false
        },
        Duration::seconds(epoch_length as i64),
    );

    // Phase 2: Re-enable chunk production and verify blacklist resets after epoch boundary.
    env.node_runner(0).send_adversarial_message(NetworkAdversarialMessage::AdvProduceChunks(
        AdvProduceChunksMode::Valid,
    ));

    // Run past the epoch boundary into epoch 2.
    // The blacklist should reset because the EpochInfoAggregator starts fresh for the new epoch.
    env.node_runner(0).run_until(
        |node| {
            let tip = node.head();
            let epoch_manager = &node.client().epoch_manager;
            let epoch_height =
                epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
            // Wait until we're a few blocks into the second epoch so the DB column
            // is populated with new-epoch assignments.
            if epoch_height < 2 {
                return false;
            }
            let block = node.client().chain.get_block(&tip.last_block_hash).unwrap();
            let prev_hash = block.header().prev_hash();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();

            // In the new epoch, validator0 should be assigned normally again
            // (blacklist is empty because stats reset).
            for shard_id in shard_layout.shard_ids() {
                let original = epoch_manager
                    .get_chunk_producer_for_height(&epoch_id, tip.height, shard_id)
                    .unwrap();
                let actual = epoch_manager.get_chunk_producer_info(prev_hash, shard_id).unwrap();
                if original.account_id() == &stopped_account {
                    assert_eq!(
                        actual.account_id(),
                        &stopped_account,
                        "Blacklist should have reset in new epoch: validator0 was originally \
                         assigned at height {} shard {} but got {} instead",
                        tip.height,
                        shard_id,
                        actual.account_id(),
                    );
                    // Found validator0 assigned normally in the new epoch.
                    return true;
                }
            }
            false
        },
        Duration::seconds(epoch_length as i64),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Count the total number of chunks produced (chunk_mask = true) across all shards
/// in the given height range.
fn count_chunks_produced(node: &TestLoopNode<'_>, start: BlockHeight, end: BlockHeight) -> usize {
    (start..=end)
        .filter_map(|h| node.client().chain.get_block_by_height(h).ok())
        .map(|b| b.header().chunk_mask().iter().filter(|&&m| m).count())
        .sum()
}

fn send_stop(env: &mut crate::setup::env::TestLoopEnv, node_idx: usize) {
    env.node_runner(node_idx).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::StopProduce),
    );
}

fn send_valid(env: &mut crate::setup::env::TestLoopEnv, node_idx: usize) {
    env.node_runner(node_idx).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::Valid),
    );
}

/// Tests the safety valve: when ALL chunk producers are blacklisted, the system
/// falls through to the original `sample_chunk_producer` assignment and the chain
/// keeps going.
///
/// Then cycles validators on/off one at a time, verifying that chunks are produced
/// whenever the assigned validator is enabled, and the chain survives every transition.
#[test]
fn slow_test_early_chunk_producer_kickout_safety_valve() {
    init_test_logger();

    let epoch_length = 200;
    let num_validators = 4;
    let protocol_version = ProtocolFeature::EarlyChunkProducerKickout.protocol_version();
    let shard_layout = ShardLayout::multi_shard(2, 1);
    let validators_spec = create_validators_spec(num_validators, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(protocol_version)
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // ── Phase 1: Stop all validators → all blacklisted → safety valve ──
    for i in 0..num_validators {
        send_stop(&mut env, i);
    }

    // Run until all validators are blacklisted.
    // With 4 equal-stake validators, each is expected ~25% of chunks.
    // At height 90, each has ~22 expected, 0 produced → all blacklisted.
    env.node_runner(0).run_until_head_height(90);

    // Verify safety valve: actual assignment == original assignment for all shards.
    // When all producers are blacklisted, sample_chunk_producer_excluding returns None
    // and save_chunk_producers_for_header falls through to sample_chunk_producer.
    {
        let node = env.node(0);
        let tip = node.head();
        let epoch_manager = &node.client().epoch_manager;
        let block = node.client().chain.get_block(&tip.last_block_hash).unwrap();
        let prev_hash = block.header().prev_hash();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();

        for shard_id in shard_layout.shard_ids() {
            let original = epoch_manager
                .get_chunk_producer_for_height(&epoch_id, tip.height, shard_id)
                .unwrap();
            let actual = epoch_manager.get_chunk_producer_info(prev_hash, shard_id).unwrap();
            assert_eq!(
                original.account_id(),
                actual.account_id(),
                "Safety valve: actual should match original when all producers blacklisted \
                 (height={}, shard={})",
                tip.height,
                shard_id,
            );
        }

        // Verify no chunks produced while all validators are stopped.
        assert_eq!(
            count_chunks_produced(&node, 85, 90),
            0,
            "No chunks should be produced when all validators are stopped"
        );
    }

    // ── Phase 2: Cycle validators on/off ──
    // Enable one validator at a time for 10 blocks, then switch to the next.
    // Each enabled validator produces chunks at heights where it is the assigned
    // producer (~25% of chunk slots), proving the safety valve routes correctly.

    // Cycle A: enable validator0
    send_valid(&mut env, 0);
    env.node_runner(0).run_until_head_height(100);
    {
        let chunks_a = count_chunks_produced(&env.node(0), 91, 100);
        assert!(chunks_a > 0, "Cycle A: validator0 should produce some chunks, got {chunks_a}");
    }

    // Cycle B: stop validator0, enable validator1
    send_stop(&mut env, 0);
    send_valid(&mut env, 1);
    env.node_runner(0).run_until_head_height(110);
    {
        let chunks_b = count_chunks_produced(&env.node(0), 101, 110);
        assert!(chunks_b > 0, "Cycle B: validator1 should produce some chunks, got {chunks_b}");
    }

    // Cycle C: stop validator1, enable validator2
    send_stop(&mut env, 1);
    send_valid(&mut env, 2);
    env.node_runner(0).run_until_head_height(120);
    {
        let chunks_c = count_chunks_produced(&env.node(0), 111, 120);
        assert!(chunks_c > 0, "Cycle C: validator2 should produce some chunks, got {chunks_c}");
    }

    // ── Phase 3: All stopped again ──
    send_stop(&mut env, 2);
    // Run a few extra blocks so the stop message propagates before we check.
    env.node_runner(0).run_until_head_height(128);
    {
        let node = env.node(0);
        // Check a range that starts after the stop has taken effect.
        assert_eq!(
            count_chunks_produced(&node, 124, 128),
            0,
            "No chunks should be produced when all validators are stopped again"
        );
        // Chain still advanced — block height reached 128.
        assert!(node.head().height >= 128, "Chain should still advance with all chunks missing");
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
