use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, BlockHeight};
use rand::Rng;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{
    call_contract, check_txs, deploy_contract, make_accounts,
};
use crate::test_loop::utils::ONE_NEAR;

const NUM_ACCOUNTS: usize = 40;
const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 8;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 16;
const NUM_RPC: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

/// Tests a scenario that different contracts are deployed to a number of accounts and
/// these contracts are called from a set of accounts.
#[test]
fn test_contract_distribution_testloop() {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let (env, rpc_id) = setup(&accounts);
    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = env;

    let mut nonce = 2;

    // Deploy contracts for accounts at index 0, 10, 20, and so on.
    let contract_ids =
        accounts.iter().enumerate().filter(|(i, _)| i % 10 == 0).map(|(_, a)| a).collect_vec();
    deploy_contracts(&mut test_loop, &node_datas, &rpc_id, contract_ids.clone(), &mut nonce);

    let mut rng = rand::thread_rng();
    let num_contracts = contract_ids.len();

    // Each account calls one of the contracts randomly.
    let sender_to_contract_ids = accounts
        .iter()
        .map(|a| (a, vec![contract_ids[rng.gen_range(0..num_contracts)]]))
        .collect_vec();
    call_contracts(&mut test_loop, &node_datas, &rpc_id, sender_to_contract_ids, &mut nonce);

    // Clear the contract cache and make the same calls.
    clear_compiled_contract_caches(&mut test_loop, &node_datas);

    // Each account calls one of the contracts randomly.
    let sender_to_contract_ids = accounts
        .iter()
        .map(|a| (a, vec![contract_ids[rng.gen_range(0..num_contracts)]]))
        .collect_vec();
    call_contracts(&mut test_loop, &node_datas, &rpc_id, sender_to_contract_ids, &mut nonce);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn setup(accounts: &Vec<AccountId>) -> (TestLoopEnv, AccountId) {
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();

    let clients = accounts.iter().take(NUM_VALIDATORS + NUM_RPC).cloned().collect_vec();
    let rpc_id = accounts[NUM_VALIDATORS].clone();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(GENESIS_HEIGHT)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account9", "account19", "account29"])
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
    (env, rpc_id)
}

/// Deploy the contract for selected accounts based on turn and wait until the transaction is executed.
/// Returns the list of accounts for which a contract was deployed.
fn deploy_contracts(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_ids: Vec<&AccountId>,
    nonce: &mut u64,
) -> Vec<AccountId> {
    let start_height = get_current_height(test_loop, node_datas);
    let mut deployed_accounts = vec![];
    let mut txs = vec![];
    for (i, contract_id) in contract_ids.into_iter().enumerate() {
        tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
        let code = near_test_contracts::sized_contract((i + 1) * 100).to_vec();
        let tx = deploy_contract(test_loop, node_datas, rpc_id, contract_id, code, *nonce);
        txs.push(tx);
        deployed_accounts.push(contract_id.clone());
        *nonce += 1;
    }
    run_until_height(test_loop, node_datas, start_height + 5);
    check_txs(&*test_loop, node_datas, rpc_id, &txs);
    deployed_accounts
}

/// Deploy the contract for selected accounts based on turn and wait until the transaction is executed.
/// Returns the list of accounts for which a contract was deployed.
fn call_contracts(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    sender_to_contract_ids: Vec<(&AccountId, Vec<&AccountId>)>,
    nonce: &mut u64,
) {
    let start_height = get_current_height(test_loop, node_datas);
    let method_name = "main".to_owned();
    let mut txs = vec![];
    for (sender_id, contract_ids) in sender_to_contract_ids.iter() {
        for contract_id in contract_ids.iter() {
            tracing::info!(target: "test", ?rpc_id, ?sender_id, ?contract_id, "Calling contract.");
            let tx = call_contract(
                test_loop,
                node_datas,
                rpc_id,
                sender_id,
                contract_id,
                method_name.clone(),
                vec![],
                *nonce,
            );
            txs.push(tx);
            *nonce += 1;
        }
    }
    run_until_height(test_loop, node_datas, start_height + 5);
    check_txs(&*test_loop, node_datas, &rpc_id, &txs);
}

fn clear_compiled_contract_caches(test_loop: &mut TestLoopV2, node_datas: &Vec<TestData>) {
    for i in 0..node_datas.len() {
        let client_handle = node_datas[i].client_sender.actor_handle();
        let contract_cache_handle =
            test_loop.data.get(&client_handle).client.runtime_adapter.compiled_contract_cache();
        contract_cache_handle.test_only_clear().unwrap();
    }
}

fn get_current_height(test_loop: &mut TestLoopV2, node_datas: &Vec<TestData>) -> BlockHeight {
    let client_handle = node_datas[0].client_sender.actor_handle();
    let height = test_loop.data.get(&client_handle).client.chain.head().unwrap().height;
    height
}

fn run_until_height(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    target_height: BlockHeight,
) {
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > target_height
        },
        Duration::seconds(60),
    );
}
