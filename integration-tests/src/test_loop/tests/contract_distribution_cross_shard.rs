use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{
    call_contract, check_txs, deploy_contract, make_accounts,
};
use crate::test_loop::utils::ONE_NEAR;

const NUM_ACCOUNTS: usize = 9;
const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_RPC: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

/// Tests a scenario that different contracts are deployed to a number of accounts and
/// these contracts are called from a set of accounts.
/// Test setup: 2 shards with 9 accounts, for 8 validators and 1 RPC node.
/// Deploys contract to one account from each shard.
/// Make 2 accounts from each shard make calls to these contracts.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_contract_distribution_cross_shard() {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let (env, rpc_id) = setup(&accounts);
    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = env;

    let mut nonce = 2;

    // Deploy a contract for each shard (account0 from first one, and account4 from second one).
    // Then take two accounts from each shard (one with a contract deployed and one without) and
    // make them call both the contracts, so we cover same-shard and cross-shard contract calls.
    let contract_ids = [&accounts[0], &accounts[4]];
    let sender_ids = [&accounts[0], &accounts[1], &accounts[4], &accounts[5]];

    // First deploy and call the contracts as described above.
    // Next, clear the compiled contract cache and repeat the same contract calls.
    deploy_contracts(&mut test_loop, &node_datas, &rpc_id, &contract_ids, &mut nonce);

    call_contracts(&mut test_loop, &node_datas, &rpc_id, &contract_ids, &sender_ids, &mut nonce);

    #[cfg(feature = "test_features")]
    clear_compiled_contract_caches(&mut test_loop, &node_datas);

    call_contracts(&mut test_loop, &node_datas, &rpc_id, &contract_ids, &sender_ids, &mut nonce);

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
        .shard_layout_simple_v1(&["account4"])
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

/// Deploys a contract for the given accounts (`contract_ids`) and waits until the transactions are executed.
/// Each account in `contract_ids` gets a fake contract with a different size (thus code-hashes are different)
fn deploy_contracts(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_ids: &[&AccountId],
    nonce: &mut u64,
) {
    let mut txs = vec![];
    for (i, contract_id) in contract_ids.into_iter().enumerate() {
        tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
        let code = near_test_contracts::sized_contract((i + 1) * 100).to_vec();
        let tx = deploy_contract(test_loop, node_datas, rpc_id, contract_id, code, *nonce);
        txs.push(tx);
        *nonce += 1;
    }
    test_loop.run_for(Duration::seconds(2));
    check_txs(&*test_loop, node_datas, rpc_id, &txs);
}

/// Makes calls to the contracts from sender_ids to the contract_ids (at which contracts are deployed).
fn call_contracts(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_ids: &[&AccountId],
    sender_ids: &[&AccountId],
    nonce: &mut u64,
) {
    let method_name = "main".to_owned();
    let mut txs = vec![];
    for sender_id in sender_ids.into_iter() {
        for contract_id in contract_ids.into_iter() {
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
    test_loop.run_for(Duration::seconds(2));
    check_txs(&*test_loop, node_datas, &rpc_id, &txs);
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
