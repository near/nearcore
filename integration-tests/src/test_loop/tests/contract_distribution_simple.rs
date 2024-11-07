use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{
    call_contract, check_txs, deploy_contract, make_account, make_accounts,
};
use crate::test_loop::utils::ONE_NEAR;

const NUM_ACCOUNTS: usize = 2;
const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 1;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

/// Executes a test that deploys to a contract to an account and calls it.
fn test_contract_distribution_single_account(clear_cache: bool) {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = setup(&accounts);

    let rpc_id = make_account(0);
    let account = rpc_id.clone();

    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account, 1);

    if clear_cache {
        #[cfg(feature = "test_features")]
        clear_compiled_contract_caches(&mut test_loop, &node_datas);
    }

    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account, 2);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests a simple scenario where we deploy and call a contract.
#[test]
fn test_contract_distribution_deploy_and_call_single_account() {
    test_contract_distribution_single_account(false);
}

/// Tests a simple scenario where we deploy a contract, and then
/// we clear the compiled contract cache and call the deployed contract call.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_contract_distribution_single_account_call_after_clear() {
    test_contract_distribution_single_account(true);
}

/// Executes a test that deploys to a contract to two different accounts and calls them.
fn test_contract_distribution_different_accounts(clear_cache: bool) {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = setup(&accounts);

    let rpc_id = make_account(0);
    let account = rpc_id.clone();

    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account, 1);
    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account, 2);

    if clear_cache {
        #[cfg(feature = "test_features")]
        clear_compiled_contract_caches(&mut test_loop, &node_datas);
    }

    let account = make_account(1);

    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account, 3);
    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account, 4);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests a simple scenario where we deploy and call a contract on two different accounts.
#[test]
fn test_contract_distribution_deploy_and_call_different_accounts() {
    test_contract_distribution_different_accounts(false);
}

/// Tests a simple scenario where we deploy and call a contract on an account, and then
/// we clear the compiled contract cache and deploy and call contract on another account.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_contract_distribution_different_accounts_call_after_clear() {
    test_contract_distribution_different_accounts(true);
}

fn setup(accounts: &Vec<AccountId>) -> TestLoopEnv {
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();

    let clients = accounts.iter().take(NUM_VALIDATORS).cloned().collect_vec();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(GENESIS_HEIGHT)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_single()
        .transaction_validity_period(1000)
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&block_and_chunk_producers, &chunk_validators_only)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, epoch_config_store) = genesis_builder.build();

    let env =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();
    env
}

fn do_deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    account: &AccountId,
    nonce: u64,
) {
    tracing::info!(target: "test", "Deploying contract.");
    let code = near_test_contracts::sized_contract(100).to_vec();
    let tx = deploy_contract(test_loop, &node_datas, rpc_id, account, code, nonce);
    test_loop.run_for(Duration::seconds(2));
    check_txs(test_loop, node_datas, rpc_id, &[tx]);
}

fn do_call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    account: &AccountId,
    nonce: u64,
) {
    tracing::info!(target: "test", "Calling contract.");
    let tx = call_contract(
        test_loop,
        node_datas,
        rpc_id,
        account,
        account,
        "main".to_owned(),
        vec![],
        nonce,
    );
    test_loop.run_for(Duration::seconds(2));
    check_txs(test_loop, node_datas, rpc_id, &[tx]);
}

/// Clears the compiled contract caches for all the clients.
#[cfg(feature = "test_features")]
pub fn clear_compiled_contract_caches(test_loop: &mut TestLoopV2, node_datas: &Vec<TestData>) {
    for i in 0..node_datas.len() {
        let client_handle = node_datas[i].client_sender.actor_handle();
        let contract_cache_handle =
            test_loop.data.get(&client_handle).client.runtime_adapter.compiled_contract_cache();
        contract_cache_handle.test_only_clear().unwrap();
    }
}
