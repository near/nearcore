use itertools::Itertools;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::client_actor::ClientActorInner;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::{
    call_contract, check_txs, do_create_account, do_delete_account, do_deploy_contract,
    get_next_nonce,
};

/// Write block height to contract storage.
fn do_call_contract(env: &mut TestLoopEnv, rpc_id: &AccountId, contract_id: &AccountId) {
    tracing::info!(target: "test", "Calling contract.");
    let nonce = get_next_nonce(&env.test_loop.data, &env.node_datas, contract_id);
    let tx = call_contract(
        &mut env.test_loop,
        &env.node_datas,
        rpc_id,
        contract_id,
        contract_id,
        "write_block_height".to_string(),
        vec![],
        nonce,
    );
    env.test_loop.run_for(Duration::seconds(5));
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &[tx]);
}

/// Tracks latest block heights and checks that all chunks are produced.
fn check_chunks(
    actor: &ClientActorInner,
    runner: &mut dyn DelayedActionRunner<ClientActorInner>,
    latest_block_height: std::cell::Cell<u64>,
) {
    let client = &actor.client;
    let tip = client.chain.head().unwrap().height;
    if tip > latest_block_height.get() {
        latest_block_height.set(tip);
        let block = client.chain.get_block_by_height(tip).unwrap();
        let num_shards = block.header().chunk_mask().len();
        println!("Chain tip: {} Chunks: {:?}", tip, block.header().chunk_mask());
        assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
    }

    runner.run_later("check_chunks", Duration::milliseconds(500), move |this, runner| {
        check_chunks(this, runner, latest_block_height);
    });
}

/// Tests account existence flow, from creation to deletion.
#[test]
fn test_create_delete_account() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let epoch_length = 10;
    let accounts =
        (0..5).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.clone();

    // Split the clients into producers, validators, and rpc nodes.
    let tmp = accounts.iter().map(|t| t.as_str()).collect_vec();
    let (producers, tmp) = tmp.split_at(2);
    let (validators, tmp) = tmp.split_at(2);
    let (rpcs, tmp) = tmp.split_at(1);
    let rpc_id = rpcs[0].parse().unwrap();
    assert!(tmp.is_empty());

    // Build test environment.
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(&producers, &validators))
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    // Launch a task to check that all chunks are produced.
    // Needed to make sure that chunks are valid. Currently, if chunk
    // validation fails, it only prevents production of new chunks, but the
    // last chunk containing problematic tx or receipt is still executed on the
    // rpc nodes, and result will be considered final based on block
    // endorsements.
    let mut client_sender = env.node_datas[0].client_sender.clone();
    client_sender.run_later("check_chunks", Duration::seconds(0), move |actor, runner| {
        check_chunks(actor, runner, std::cell::Cell::new(0));
    });

    let new_account: AccountId = format!("alice.{}", accounts[0]).parse().unwrap();
    let contract_code = near_test_contracts::rs_contract().to_vec();

    // Create account.
    do_create_account(&mut env, &rpc_id, &accounts[0], &new_account, 100 * ONE_NEAR);
    // Deploy contract.
    do_deploy_contract(&mut env, &rpc_id, &new_account, contract_code);
    // Write a key-value pair to the contract storage.
    do_call_contract(&mut env, &rpc_id, &new_account);
    // Delete account. Should remove everything - account, contract code and
    // storage.
    do_delete_account(&mut env, &rpc_id, &new_account, &accounts[1]);

    env.test_loop.run_for(Duration::seconds(20));
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
