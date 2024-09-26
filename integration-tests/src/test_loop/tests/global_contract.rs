use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Nonce};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{check_txs, deploy_permanent_contract};
use crate::test_loop::utils::ONE_NEAR;

const NUM_PRODUCERS: usize = 2;
const NUM_VALIDATORS: usize = 2;
const NUM_RPC: usize = 1;
const NUM_CLIENTS: usize = NUM_PRODUCERS + NUM_VALIDATORS + NUM_RPC;

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
        .shard_layout_simple_v1(&["account0", "account1", "account2"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&producers, &validators)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let env = builder.genesis(genesis).clients(clients).build();
    (env, rpc_id.clone())
}

#[test]
fn test_permanent_contracts_root() {
    // init_integration_logger();
    let accounts =
        (0..10).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let (env, rpc_id) = setup(&accounts);
    let contract_id = "account0".parse().unwrap();
    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = env;

    let code = near_test_contracts::rs_contract();

    let mut nonce = 1;
    do_deploy_permanent_contract(
        &mut test_loop,
        &node_datas,
        &rpc_id,
        &contract_id,
        code.to_vec(),
        nonce,
    );
    nonce += 1;
    let client_handle = node_datas[0].client_sender.actor_handle();
    let block = test_loop.data.get(&client_handle).client.chain.get_head_block().unwrap();
    let root1 = block.header().global_contract_root();
    assert_ne!(root1, CryptoHash::default());
    // deploy another contract and check that the root is updated
    let new_code = near_test_contracts::ft_contract();
    do_deploy_permanent_contract(
        &mut test_loop,
        &node_datas,
        &rpc_id,
        &contract_id,
        new_code.to_vec(),
        nonce,
    );
    nonce += 1;

    let block = test_loop.data.get(&client_handle).client.chain.get_head_block().unwrap();
    let root2 = block.header().global_contract_root();
    assert_ne!(root1, root2);
    // deploy the same contract again. This time the root should not change
    do_deploy_permanent_contract(
        &mut test_loop,
        &node_datas,
        &rpc_id,
        &contract_id,
        new_code.to_vec(),
        nonce,
    );
    let block = test_loop.data.get(&client_handle).client.chain.get_head_block().unwrap();
    let root3 = block.header().global_contract_root();
    assert_eq!(root2, root3);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Deploy a permanent contract and wait until the transaction is executed.
fn do_deploy_permanent_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    contract_id: &AccountId,
    contract_code: Vec<u8>,
    nonce: Nonce,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
    let tx =
        deploy_permanent_contract(test_loop, node_datas, rpc_id, contract_id, contract_code, nonce);
    test_loop.run_for(Duration::seconds(3));
    check_txs(&*test_loop, node_datas, rpc_id, &[tx]);
}
