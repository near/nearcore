use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use itertools::Itertools;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{ExecutionOutcome, ExecutionOutcomeWithId};
use near_primitives::types::Balance;
use near_replay::{ChunkReplayResult, MemtrieShardReplayController};

/// Tests the SequentialChunksReplayController with multiple shards,
/// replaying backwards from the head.
///
/// Setup: 1 validator + 1 RPC node, 2 shards. Deploy contracts and call
/// functions, then initialize a controller per shard from the RPC node's
/// store and advance backwards to the block containing the calls,
/// verifying chunk extras and execution outcomes match.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_replay_chunks_controller() {
    init_test_logger();

    let users = [create_account_id("user_shard0"), create_account_id("user_shard1")];
    let boundary = create_account_id("user_shard01");
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary], 1);
    let shard_uids = shard_layout.shard_uids().collect_vec();

    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .enable_rpc()
        .shard_layout(shard_layout)
        .gc_num_epochs_to_keep(3)
        .add_user_accounts(&users, Balance::from_near(100))
        .build();

    // Deploy the test contract to both accounts.
    for user in &users {
        let deploy_tx = env.rpc_node().tx_deploy_test_contract(&user);
        env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));
    }

    // Submit "log_something" calls to both contracts.
    let mut call_txs = vec![];
    for user in &users {
        let call_tx = env.rpc_node().tx_call(
            &user,
            &user,
            "log_something",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        call_txs.push(env.rpc_node().submit_tx(call_tx));
    }

    env.rpc_runner().run_until_outcome_available(call_txs[0], Duration::seconds(5));

    let mut receipt_outcomes = vec![];
    for tx_hash in call_txs {
        let receipt_id = env.rpc_node().tx_receipt_id(tx_hash);
        let receipt_outcome = env.rpc_node().execution_outcome_with_proof(receipt_id);
        assert_eq!(receipt_outcome.outcome_with_id.outcome.logs, vec!["hello"]);
        receipt_outcomes.push(receipt_outcome);
    }
    assert_eq!(receipt_outcomes[0].block_hash, receipt_outcomes[1].block_hash);

    let call_height = env.rpc_node().block(receipt_outcomes[0].block_hash).header().height();

    // Run a few more blocks so the head is past the call block.
    env.rpc_runner().run_for_number_of_blocks(3);

    let target_block_hash = receipt_outcomes[0].block_hash;

    for (shard_uid, receipt_outcome) in shard_uids.into_iter().zip(receipt_outcomes) {
        let rpc_client = env.rpc_node().client();
        let runtime = rpc_client.runtime_adapter.clone();
        // Make sure memtrie is not loaded for that shard to make sure
        // the controller properly loads it
        runtime.get_tries().unload_memtrie(&shard_uid);
        let mut controller = MemtrieShardReplayController::load_memtrie(
            rpc_client.chain.chain_store.clone(),
            runtime,
            rpc_client.epoch_manager.clone(),
            shard_uid.shard_id(),
        )
        .expect("failed to create replay controller");

        // Replay backwards from the head, verifying each chunk, until we
        // reach the block containing the calls.
        let replay_result = loop {
            let prepared = controller.prepare_next_replay().expect("prepare_next_replay failed");
            let result = prepared.replay().expect("replay failed");
            result.verify().expect("chunk extra mismatch");
            assert!(
                result.block_height >= call_height,
                "advanced past call block without finding it (height {} < {})",
                result.block_height,
                call_height,
            );
            if result.block_hash == target_block_hash {
                break result;
            }
        };

        assert_replayed_outcome(&replay_result, &receipt_outcome.outcome_with_id);
    }
}

/// Finds the execution outcome for the given receipt across all replayed
/// chunks and asserts it matches the expected outcome (ignoring
/// `compute_usage`, which is `borsh(skip)` and not persisted).
fn assert_replayed_outcome(chunk_result: &ChunkReplayResult, expected: &ExecutionOutcomeWithId) {
    let receipt_id = expected.id;
    let replayed = chunk_result
        .apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .unwrap_or_else(|| panic!("receipt outcome {} not found in replay result", receipt_id));
    let comparable = ExecutionOutcome { compute_usage: None, ..replayed.outcome.clone() };
    assert_eq!(comparable, expected.outcome);
}
