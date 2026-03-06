use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::run_for_number_of_blocks;
use crate::utils::transactions::{
    TransactionRunner, check_txs, execute_tx, make_accounts, prepare_transfer_tx,
};
use assert_matches::assert_matches;
use core::panic;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfig;
use near_primitives::errors::InvalidTxError;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, Gas};
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
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_congestion_control_simple() {
    init_test_logger();

    // Test setup
    let epoch_length = 10;
    let contract_id: AccountId = create_account_id("000");
    let mut accounts = make_accounts(NUM_ACCOUNTS);
    accounts.push(contract_id.clone());
    let (mut env, rpc_id) = setup(&accounts, epoch_length, &["account3", "account5", "account7"]);

    // Deploy the contract.
    let tx = env.node_for_account(&rpc_id).tx_deploy_test_contract(&contract_id);
    env.runner_for_account(&rpc_id).run_tx(tx, Duration::seconds(5));

    // Call the contract from all accounts.
    do_call_contract(&mut env, &rpc_id, &contract_id, &accounts);

    // Make sure the chain progresses for several epochs.
    env.node_runner(0).run_for_number_of_blocks(2 * epoch_length as usize);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
    run_for_number_of_blocks(&mut env, &rpc_id, max_missed_chunks as usize * 2);

    // Check if shard 2 is congested
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client_actor = env.test_loop.data.get(&client_handle);
    let head = client_actor.client.chain.get_head_block().unwrap();
    let missed_chunks = head.block_congestion_info().get(&shard2).unwrap().missed_chunks_count;
    assert!(missed_chunks >= max_missed_chunks);

    // Send transfer from shard 1 to shard 1 – should succeed
    let block_time = client_actor.client.config.max_block_production_delay;
    let tx = prepare_transfer_tx(&mut env, &shard1_acc1, &shard1_acc2, Balance::from_near(1));
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
    let tx = prepare_transfer_tx(&mut env, &shard1_acc1, &shard2_acc1, Balance::from_near(1));
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
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
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

/// Call the contract from all accounts and wait until the transactions are executed.
fn do_call_contract(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    contract_id: &AccountId,
    accounts: &Vec<AccountId>,
) {
    tracing::info!(target: "test", ?rpc_id, ?contract_id, "calling contract");
    let burn_gas = Gas::from_teragas(250);
    let args = burn_gas.as_gas().to_le_bytes().to_vec();
    let mut txs = vec![];
    let node = env.node_for_account(rpc_id);
    for sender_id in accounts {
        let tx = node.tx_call(
            sender_id,
            contract_id,
            "burn_gas_raw",
            args.clone(),
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        let tx_hash = tx.get_hash();
        node.submit_tx(tx);
        txs.push(tx_hash);
    }
    env.test_loop.run_for(Duration::seconds(20));
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &txs);
}
