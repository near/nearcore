use core::panic;

use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::client_actor::ClientActorInner;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::FinalExecutionStatus;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{call_contract, deploy_contract, get_node_data};
use crate::test_loop::utils::ONE_NEAR;

const NUM_PRODUCERS: usize = 2;
const NUM_VALIDATORS: usize = 2;
const NUM_RPC: usize = 1;
const NUM_CLIENTS: usize = NUM_PRODUCERS + NUM_VALIDATORS + NUM_RPC;

/// A very simple test that exercises congestion control in the typical setup
/// with producers, validators, rpc nodes, single shard tracking and state sync.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_congestion_control_simple() {
    init_test_logger();

    // Test setup

    let contract_id: AccountId = "000".parse().unwrap();
    let mut accounts = (0..100).map(make_account).collect_vec();
    accounts.push(contract_id.clone());

    let (env, rpc_id) = setup(&accounts);
    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = env;

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
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn setup(accounts: &Vec<AccountId>) -> (TestLoopEnv, AccountId) {
    let initial_balance = 10000 * ONE_NEAR;
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

    let builder = TestLoopBuilder::new();
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&producers, &validators)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, epoch_config_store) = genesis_builder.build();

    let env =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();
    (env, rpc_id.clone())
}

/// Deploy the contract and wait until the transaction is executed.
fn do_deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_id: &AccountId,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
    let tx = deploy_contract(test_loop, node_datas, rpc_id, contract_id);
    test_loop.run_for(Duration::seconds(5));
    check_txs(&*test_loop, node_datas, rpc_id, &[tx]);
}

/// Call the contract from all accounts and wait until the transactions are executed.
fn do_call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_id: &AccountId,
    accounts: &Vec<AccountId>,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "Calling contract.");
    let mut txs = vec![];
    for sender_id in accounts {
        let tx = call_contract(test_loop, node_datas, &sender_id, &contract_id);
        txs.push(tx);
    }
    test_loop.run_for(Duration::seconds(20));
    check_txs(&*test_loop, node_datas, &rpc_id, &txs);
}

/// Check the status of the transactions and assert that they are successful.
///
/// Please note that it's important to use an rpc node that tracks all shards.
/// Otherwise, the transactions may not be found.
fn check_txs(
    test_loop: &TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc: &AccountId,
    txs: &[CryptoHash],
) {
    let rpc = rpc_client(test_loop, node_datas, rpc);

    for &tx in txs {
        let tx_outcome = rpc.chain.get_partial_transaction_result(&tx);
        let status = tx_outcome.as_ref().map(|o| o.status.clone());
        let status = status.unwrap();
        tracing::info!(target: "test", ?tx, ?status, "transaction status");
        assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
    }
}

/// The condition that can be used for the test loop to wait until the chain
/// height is greater than the target height.
fn height_condition(
    test_loop_data: &mut TestLoopData,
    client_handle: &TestLoopDataHandle<ClientActorInner>,
    target_height: BlockHeight,
) -> bool {
    test_loop_data.get(&client_handle).client.chain.head().unwrap().height > target_height
}

/// Get the client for the provided rpd node account id.
fn rpc_client<'a>(
    test_loop: &'a TestLoopV2,
    node_datas: &'a Vec<TestData>,
    rpc_id: &AccountId,
) -> &'a Client {
    let node_data = get_node_data(node_datas, rpc_id);
    let client_actor_handle = node_data.client_sender.actor_handle();
    let client_actor = test_loop.data.get(&client_actor_handle);
    &client_actor.client
}

/// Make the account id for the provided index.
fn make_account(i: i32) -> AccountId {
    format!("account{}", i).parse().unwrap()
}
