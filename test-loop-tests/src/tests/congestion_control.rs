use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::transactions::{
    TransactionRunner, call_contract, check_txs, deploy_contract, execute_tx, make_accounts,
    prepare_transfer_tx,
};
use crate::utils::{ONE_NEAR, TGAS};
use assert_matches::assert_matches;
use core::panic;
use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::client_actor::ClientActorInner;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfig;
use near_primitives::errors::InvalidTxError;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::types::BlockHeightDelta;

const NUM_ACCOUNTS: usize = 100;
const NUM_PRODUCERS: usize = 2;
const NUM_VALIDATORS: usize = 2;
const NUM_RPC: usize = 1;
const NUM_CLIENTS: usize = NUM_PRODUCERS + NUM_VALIDATORS + NUM_RPC;

/// A very simple test that exercises congestion control in the typical setup
/// with producers, validators, rpc nodes, single shard tracking and state sync.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn slow_test_congestion_control_simple() {
    init_test_logger();

    // Test setup

    let contract_id: AccountId = "000".parse().unwrap();
    let mut accounts = make_accounts(NUM_ACCOUNTS);
    accounts.push(contract_id.clone());

    let (env, rpc_id) = setup(&accounts, 10, &["account3", "account5", "account7"]);
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = env;

    // Test

    // Deploy the contract.
    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &contract_id);

    // Call the contract from all accounts.
    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &contract_id, &accounts);

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| height_condition(test_loop_data, &client_handle, 10050),
        Duration::seconds(100),
    );

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn slow_test_one_shard_congested() {
    init_test_logger();

    let mut accounts = make_accounts(NUM_PRODUCERS + NUM_VALIDATORS);
    let shard1_acc1: AccountId = "0000".parse().unwrap();
    let shard1_acc2: AccountId = "1111".parse().unwrap();
    let shard2_acc1: AccountId = "xxxx".parse().unwrap();
    accounts.push(shard1_acc1.clone());
    accounts.push(shard1_acc2.clone());
    accounts.push(shard2_acc1.clone());

    let runtime_config = RuntimeConfig::test();
    let max_missed_chunks = runtime_config.congestion_control_config.max_congestion_missed_chunks;
    let epoch_length = max_missed_chunks * 100;
    let (env, rpc_id) = setup(&accounts, epoch_length, &["near"]);
    assert_eq!(rpc_id, shard1_acc1);

    // Assert correct shard layout
    let shard_layout = &env.shared_state.genesis.config.shard_layout;
    let shard1 = shard_layout.account_id_to_shard_id(&shard1_acc1);
    let shard2 = shard_layout.account_id_to_shard_id(&shard2_acc1);
    assert_eq!(shard1, shard_layout.account_id_to_shard_id(&shard1_acc2));
    assert_ne!(shard1, shard2);

    // Configure test env to drop chunks for shard 2
    let dropped_chunks = [
        (shard2, (0..epoch_length).map(|_| false).collect_vec()), // All chunks in the epoch
    ]
    .into_iter()
    .collect();
    let mut env = env.drop(DropCondition::ChunksProducedByHeight(dropped_chunks));

    // Run for `max_missed_chunks * 2` blocks to make sure shard 2 is congested
    env.run_blocks(max_missed_chunks * 2);

    // Check if shard 2 is congested
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client_actor = env.test_loop.data.get(&client_handle);
    let head = client_actor.client.chain.get_head_block().unwrap();
    let missed_chunks = head.block_congestion_info().get(&shard2).unwrap().missed_chunks_count;
    assert!(missed_chunks >= max_missed_chunks);

    // Send transfer from shard 1 to shard 1 – should succeed
    let block_time = client_actor.client.config.max_block_production_delay;
    let tx = prepare_transfer_tx(&mut env, &shard1_acc1, &shard1_acc2, ONE_NEAR);
    let tx_outcome = execute_tx(
        &mut env.test_loop,
        &rpc_id,
        TransactionRunner::new(tx, false),
        &env.node_datas,
        block_time * 3,
    )
    .unwrap();
    assert_matches!(tx_outcome.status, FinalExecutionStatus::SuccessValue(_));

    // Send transfer from shard 1 to shard 2 – should fail, because shard 2 is congested
    let tx = prepare_transfer_tx(&mut env, &shard1_acc1, &shard2_acc1, ONE_NEAR);
    let tx_error = execute_tx(
        &mut env.test_loop,
        &rpc_id,
        TransactionRunner::new(tx, false),
        &env.node_datas,
        block_time * 3,
    )
    .unwrap_err();
    assert_matches!(tx_error, InvalidTxError::ShardStuck { shard_id, .. } if shard_id == Into::<u32>::into(shard2));

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn setup(
    accounts: &Vec<AccountId>,
    epoch_length: BlockHeightDelta,
    boundary_accounts: &[&str],
) -> (TestLoopEnv, AccountId) {
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(NUM_PRODUCERS);
    let (validators, tmp) = tmp.split_at(NUM_VALIDATORS);
    let (rpcs, tmp) = tmp.split_at(NUM_RPC);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let [rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

    let boundary_accounts = boundary_accounts.iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let validators_spec = ValidatorsSpec::desired_roles(&producers, &validators);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();
    (env, rpc_id.clone())
}

/// Deploy the contract and wait until the transaction is executed.
fn do_deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<NodeExecutionData>,
    rpc_id: &AccountId,
    contract_id: &AccountId,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
    let code = near_test_contracts::rs_contract().to_vec();
    let tx = deploy_contract(test_loop, node_datas, rpc_id, contract_id, code, 1);
    test_loop.run_for(Duration::seconds(5));
    check_txs(&test_loop.data, node_datas, rpc_id, &[tx]);
}

/// Call the contract from all accounts and wait until the transactions are executed.
fn do_call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<NodeExecutionData>,
    rpc_id: &AccountId,
    contract_id: &AccountId,
    accounts: &Vec<AccountId>,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "Calling contract.");
    let method_name = "burn_gas_raw".to_owned();
    let burn_gas: u64 = 250 * TGAS;
    let args = burn_gas.to_le_bytes().to_vec();
    let mut txs = vec![];
    for sender_id in accounts {
        let tx = call_contract(
            test_loop,
            node_datas,
            rpc_id,
            &sender_id,
            &contract_id,
            method_name.clone(),
            args.clone(),
            2,
        );
        txs.push(tx);
    }
    test_loop.run_for(Duration::seconds(20));
    check_txs(&test_loop.data, node_datas, &rpc_id, &txs);
}

/// The condition that can be used for the test loop to wait until the chain
/// height is greater than the target height.
fn height_condition(
    test_loop_data: &TestLoopData,
    client_handle: &TestLoopDataHandle<ClientActorInner>,
    target_height: BlockHeight,
) -> bool {
    test_loop_data.get(&client_handle).client.chain.head().unwrap().height > target_height
}
