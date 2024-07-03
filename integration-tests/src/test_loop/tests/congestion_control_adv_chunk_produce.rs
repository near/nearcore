use core::panic;

use itertools::Itertools;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::client_actor::ClientActorInner;
use near_client::test_utils::test_loop::ClientQueries;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{call_contract, deploy_contracts};
use crate::test_loop::utils::ONE_NEAR;

const NUM_PRODUCERS: usize = 2;
const NUM_VALIDATORS: usize = 2;
const NUM_RPC: usize = 1;
const NUM_CLIENTS: usize = NUM_PRODUCERS + NUM_VALIDATORS + NUM_RPC;

/// This test checks that a chunk with too many transactions is rejected by the
/// chunk validators.
#[test]
fn test_congestion_control_adv_chunk_produce() {
    init_test_logger();

    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(NUM_PRODUCERS);
    let (validators, tmp) = tmp.split_at(NUM_VALIDATORS);
    let (rpcs, tmp) = tmp.split_at(NUM_RPC);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let [rpc] = rpcs else { panic!("Expected exactly one rpc node") };

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
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis).clients(clients).build();

    let first_epoch_tracked_shards = get_tracked_shards(&test_loop, &node_datas);
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    // Deploy the contracts.
    let txs = deploy_contracts(&mut test_loop, &node_datas);
    test_loop.run_for(Duration::seconds(5));

    tracing::info!(target: "test", "deployed contracts");
    log_txs(&test_loop, &node_datas, &rpc, &txs);

    // Call the contracts.
    let mut txs = vec![];
    for account in accounts.iter().take(NUM_PRODUCERS + NUM_VALIDATORS) {
        let tx = call_contract(&mut test_loop, &node_datas, account);
        txs.push(tx);
    }
    test_loop.run_for(Duration::seconds(20));

    tracing::info!(target: "test", "called contracts");
    log_txs(&test_loop, &node_datas, &rpc, &txs);

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| height_condition(test_loop_data, &client_handle, 10050),
        Duration::seconds(100),
    );

    let later_epoch_tracked_shards = get_tracked_shards(&test_loop, &node_datas);
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn height_condition(
    test_loop_data: &mut TestLoopData,
    client_handle: &TestLoopDataHandle<ClientActorInner>,
    target_height: BlockHeight,
) -> bool {
    test_loop_data.get(&client_handle).client.chain.head().unwrap().height > target_height
}

fn get_tracked_shards(test_loop: &TestLoopV2, node_datas: &Vec<TestData>) -> Vec<Vec<u64>> {
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    clients.tracked_shards_for_each_client()
}

fn rpc_client<'a>(
    test_loop: &'a TestLoopV2,
    node_datas: &'a Vec<TestData>,
    rpc: &AccountId,
) -> &'a Client {
    for node_data in node_datas {
        if &node_data.account_id == rpc {
            let handle = node_data.client_sender.actor_handle();
            let client_actor = test_loop.data.get(&handle);
            return &client_actor.client;
        }
    }
    panic!("RPC client not found");
}

fn log_txs(
    test_loop: &TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc: &AccountId,
    txs: &Vec<CryptoHash>,
) {
    let rpc = rpc_client(test_loop, node_datas, rpc);

    for &tx in txs {
        let tx_outcome = rpc.chain.get_partial_transaction_result(&tx);
        let status = tx_outcome.as_ref().map(|o| o.status.clone());
        tracing::info!(target: "test", ?tx, ?status, "transaction status");
    }
}
