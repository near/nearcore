use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::runtime::NightshadeRuntime;
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::transaction::ExecutionOutcome;
use near_primitives::types::Balance;
use near_replay::MemtriesChunksReplayController;
use near_store::{Store, TrieConfig};
use near_vm_runner::ContractRuntimeCache;
use near_vm_runner::FilesystemContractRuntimeCache;

/// Tests the MemtriesChunksReplayController using test-loop.
///
/// Setup: 1 validator + 1 RPC node. We take a deep copy of the RPC node's
/// database, then deploy and call a contract. After the RPC node catches up
/// and runs further (so flat storage advances well past the call), we
/// initialize the replay controller using the earlier snapshot for memtries
/// and the RPC node's current store for chain data, then verify that
/// replaying the block containing the function call produces matching
/// ChunkExtras.
#[test]
fn test_replay_chunks_controller() {
    init_test_logger();

    let user = create_account_id("user");
    let mut env = TestLoopBuilder::new()
        .validators(1, 0)
        .enable_rpc()
        .gc_num_epochs_to_keep(3)
        .add_user_account(&user, Balance::from_near(100))
        .build();

    // Take a deep copy of the RPC node's database for later use as the
    // memtrie source. A plain clone() would share the underlying database.
    let snapshot_db = env.rpc_node().store().database().copy_if_test(None).unwrap();
    let snapshot_store = Store::new(snapshot_db);

    // Kill the RPC node so it falls behind while we produce blocks.
    let rpc_identifier = env.rpc_node().node_data.identifier.clone();
    let killed_state = env.kill_node(&rpc_identifier);

    // Deploy the test contract.
    let deploy_tx = env.validator().tx_deploy_test_contract(&user);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call "log_something" on the contract.
    let call_tx = env.validator().tx_call(
        &user,
        &user,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_tx_hash = call_tx.get_hash();
    env.validator_runner().run_tx(call_tx, Duration::seconds(5));

    // Find the block height at which the function call receipt was executed.
    let receipt_id = env.validator().tx_receipt_id(call_tx_hash);
    let receipt_outcome = env.validator().execution_outcome_with_proof(receipt_id);
    assert_eq!(receipt_outcome.outcome_with_id.outcome.logs, vec!["hello"]);
    let call_height = env.validator().block(receipt_outcome.block_hash).header().height();

    // Re-enable the RPC node and let it catch up. Run enough blocks so
    // the RPC node's final head advances past call_height, ensuring flat
    // storage moves well beyond the blocks we want to replay.
    let new_rpc_identifier = format!("{}-restart", rpc_identifier);
    env.restart_node(&new_rpc_identifier, killed_state);
    env.rpc_runner().run_until(
        |node| node.client().chain.final_head().unwrap().height >= call_height + 3,
        Duration::seconds(20),
    );

    let mut controller = create_replay_controller(&env, &snapshot_store);

    // Verify the controller starts before the call block — this ensures
    // the snapshot's flat head is earlier than the blocks we want to replay.
    let rpc_client = env.rpc_node().client();
    let start_block = rpc_client.chain.get_block(controller.current_block_hash()).unwrap();
    assert!(
        start_block.header().height() <= call_height,
        "controller starts at height {} which is past call_height {}",
        start_block.header().height(),
        call_height,
    );

    // Advance the controller to the block containing the function call.
    while rpc_client.chain.get_block(controller.current_block_hash()).unwrap().header().height()
        < call_height
    {
        assert!(controller.advance().expect("advance failed"), "ran out of blocks");
    }

    // Replay the block and verify the single shard's chunk extra matches.
    let result = controller.replay_current_block().expect("replay failed");
    assert_eq!(result.height, call_height);
    assert_eq!(result.chunk_results.len(), 1);
    let chunk_result = &result.chunk_results[0];
    assert_eq!(chunk_result.expected.state_root(), chunk_result.actual.state_root());

    // Verify the execution outcome for the function call receipt matches.
    let replayed = chunk_result
        .apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .expect("receipt outcome not found in replay result");
    assert_execution_outcome(&replayed.outcome, &receipt_outcome.outcome_with_id.outcome);
}

/// Creates a `MemtriesChunksReplayController` from the given snapshot store
/// (for memtries) and the RPC node's current chain store and epoch manager
/// (for replay data).
fn create_replay_controller(
    env: &TestLoopEnv,
    memtrie_source_store: &Store,
) -> MemtriesChunksReplayController {
    let rpc_client = env.rpc_node().client();
    let chain_store = rpc_client.chain.chain_store.clone();
    let epoch_manager = rpc_client.epoch_manager.clone();
    let genesis_config = &env.shared_state.genesis.config;

    // Create a NightshadeRuntime backed by the RPC node's store so that
    // trie data reads during replay have access to the full state.
    let home_dir =
        NodeExecutionData::homedir(&env.shared_state.tempdir, &env.rpc_node().node_data.identifier);
    let rpc_store = env.rpc_node().store();
    let runtime_epoch_manager =
        EpochManager::new_arc_handle(rpc_store.clone(), genesis_config, Some(&home_dir));
    let contract_cache = FilesystemContractRuntimeCache::test().expect("filesystem contract cache");
    let runtime = NightshadeRuntime::test_with_trie_config(
        &home_dir,
        rpc_store,
        ContractRuntimeCache::handle(&contract_cache),
        genesis_config,
        runtime_epoch_manager,
        env.shared_state.runtime_config_store.clone(),
        TrieConfig::default(),
        3,
        false,
        false,
    );

    MemtriesChunksReplayController::load_memtries(
        chain_store,
        runtime,
        epoch_manager,
        memtrie_source_store,
    )
    .expect("failed to create replay controller")
}

/// Asserts that two execution outcomes are equal, ignoring `compute_usage`
/// which is `borsh(skip)` and not persisted in the database.
fn assert_execution_outcome(actual: &ExecutionOutcome, expected: &ExecutionOutcome) {
    let comparable = ExecutionOutcome { compute_usage: None, ..actual.clone() };
    assert_eq!(comparable, *expected);
}
