use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::runtime::NightshadeRuntime;
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::ExecutionOutcome;
use near_primitives::types::Balance;
use near_replay::{CombinedDatabase, ReplayStorageMode, SequentialChunksReplayController};
use near_store::{Store, TrieConfig};
use near_vm_runner::ContractRuntimeCache;
use near_vm_runner::FilesystemContractRuntimeCache;

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_memtries_replay_chunks_controller() {
    test_replay_chunks_controller(ReplayStorageMode::Memtries);
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_flat_state_replay_chunks_controller() {
    test_replay_chunks_controller(ReplayStorageMode::FlatState);
}

/// Tests the SequentialChunksReplayController with multiple shards.
///
/// Setup: 1 validator + 1 RPC node, 2 shards with boundary at "user_shard01" so
/// that "user_shard0" and "user_shard1" land in different shards. We snapshot the RPC
/// node's DB, deploy contracts to both accounts, submit function calls to
/// both in the same block, let the RPC catch up, then replay and verify
/// chunk extras and execution outcomes match in both shards.
fn test_replay_chunks_controller(storage_mode: ReplayStorageMode) {
    init_test_logger();

    let user_shard0 = create_account_id("user_shard0");
    let user_shard1 = create_account_id("user_shard1");
    let boundary = create_account_id("user_shard01");
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary], 1);

    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .enable_rpc()
        .shard_layout(shard_layout)
        .gc_num_epochs_to_keep(3)
        .add_user_accounts([&user_shard0, &user_shard1], Balance::from_near(100))
        .build();

    // Take a deep copy of the RPC node's database for later use as the
    // memtrie source. A plain clone() would share the underlying database.
    let snapshot_db = env.rpc_node().store().database().copy_if_test(None).unwrap();
    let snapshot_store = Store::new(snapshot_db);

    // Kill the RPC node so it falls behind while we produce blocks.
    let rpc_identifier = env.rpc_node().node_data.identifier.clone();
    let killed_state = env.kill_node(&rpc_identifier);

    // Deploy the test contract to both accounts.
    let deploy_tx0 = env.validator().tx_deploy_test_contract(&user_shard0);
    env.validator_runner().run_tx(deploy_tx0, Duration::seconds(5));
    let deploy_tx1 = env.validator().tx_deploy_test_contract(&user_shard1);
    env.validator_runner().run_tx(deploy_tx1, Duration::seconds(5));

    // Submit "log_something" calls to both contracts so they are processed
    // in the same block (one per shard).
    let call_tx0 = env.validator().tx_call(
        &user_shard0,
        &user_shard0,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_tx1 = env.validator().tx_call(
        &user_shard1,
        &user_shard1,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_tx_hash0 = env.validator().submit_tx(call_tx0);
    let call_tx_hash1 = env.validator().submit_tx(call_tx1);

    env.validator_runner().run_until_outcome_available(call_tx_hash1, Duration::seconds(5));
    let receipt_id0 = env.validator().tx_receipt_id(call_tx_hash0);
    let receipt_outcome0 = env.validator().execution_outcome_with_proof(receipt_id0);
    let receipt_id1 = env.validator().tx_receipt_id(call_tx_hash1);
    let receipt_outcome1 = env.validator().execution_outcome_with_proof(receipt_id1);
    assert_eq!(receipt_outcome0.block_hash, receipt_outcome1.block_hash);
    assert_eq!(receipt_outcome0.outcome_with_id.outcome.logs, vec!["hello"]);
    assert_eq!(receipt_outcome1.outcome_with_id.outcome.logs, vec!["hello"]);

    let call_height = env.validator().block(receipt_outcome0.block_hash).header().height();

    // Re-enable the RPC node and let it catch up. Run enough blocks so
    // the RPC node's final head advances past call_height, ensuring flat
    // storage moves well beyond the blocks we want to replay.
    let new_rpc_identifier = format!("{}-restart", rpc_identifier);
    env.restart_node(&new_rpc_identifier, killed_state);
    env.rpc_runner().run_until(
        |node| node.client().chain.final_head().unwrap().height >= call_height + 3,
        Duration::seconds(20),
    );

    let mut controller = create_replay_controller(&env, &snapshot_store, storage_mode);

    // Verify the controller starts before the call block.
    let rpc_client = env.rpc_node().client();
    let start_block = rpc_client.chain.get_block(controller.current_block_hash()).unwrap();
    assert!(
        start_block.header().height() <= call_height,
        "controller starts at height {} which is past call_height {}",
        start_block.header().height(),
        call_height,
    );

    // Advance the controller to the block containing the function calls.
    while rpc_client.chain.get_block(controller.current_block_hash()).unwrap().header().height()
        < call_height
    {
        assert!(controller.advance().expect("advance failed"), "ran out of blocks");
    }

    // Replay the block and verify both shards' chunk extras match.
    let result = controller.replay_current_block().expect("replay failed");
    assert_eq!(result.block_header.height(), call_height);
    assert_eq!(result.chunk_results.len(), 2);
    for chunk_result in &result.chunk_results {
        assert_eq!(chunk_result.expected.state_root(), chunk_result.actual.state_root());
    }

    // Verify execution outcomes for both function call receipts.
    assert_replayed_outcome(&result, receipt_id0, &receipt_outcome0.outcome_with_id.outcome);
    assert_replayed_outcome(&result, receipt_id1, &receipt_outcome1.outcome_with_id.outcome);
}

/// Creates a `SequentialChunksReplayController` using a `CombinedDatabase`
/// that reads flat-state columns from the snapshot and everything else
/// from the RPC node's current store.
fn create_replay_controller(
    env: &TestLoopEnv,
    snapshot_store: &Store,
    storage_mode: ReplayStorageMode,
) -> SequentialChunksReplayController {
    let rpc_client = env.rpc_node().client();
    let chain_store = rpc_client.chain.chain_store.clone();
    let epoch_manager = rpc_client.epoch_manager.clone();
    let genesis_config = &env.shared_state.genesis.config;

    let combined_store = CombinedDatabase::new(snapshot_store.clone(), env.rpc_node().store());

    let home_dir =
        NodeExecutionData::homedir(&env.shared_state.tempdir, &env.rpc_node().node_data.identifier);
    let runtime_epoch_manager =
        EpochManager::new_arc_handle(combined_store.clone(), genesis_config, Some(&home_dir));
    let contract_cache = FilesystemContractRuntimeCache::test().expect("filesystem contract cache");
    let runtime = NightshadeRuntime::test_with_trie_config(
        &home_dir,
        combined_store,
        ContractRuntimeCache::handle(&contract_cache),
        genesis_config,
        runtime_epoch_manager,
        env.shared_state.runtime_config_store.clone(),
        TrieConfig::default(),
        3,
        false,
        false,
    );

    SequentialChunksReplayController::new(chain_store, runtime, epoch_manager, storage_mode)
        .expect("failed to create replay controller")
}

/// Finds the execution outcome for the given receipt in the replay result
/// and asserts it matches the expected outcome (ignoring `compute_usage`
/// which is `borsh(skip)` and not persisted in the database).
fn assert_replayed_outcome(
    result: &near_replay::BlockReplayResult,
    receipt_id: CryptoHash,
    expected: &ExecutionOutcome,
) {
    let replayed = result
        .chunk_results
        .iter()
        .flat_map(|c| &c.apply_result.outcomes)
        .find(|o| o.id == receipt_id)
        .unwrap_or_else(|| panic!("receipt outcome {} not found in replay result", receipt_id));
    let comparable = ExecutionOutcome { compute_usage: None, ..replayed.outcome.clone() };
    assert_eq!(comparable, *expected);
}
