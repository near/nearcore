use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{check_txs, make_accounts, prepare_transfer_tx, submit_tx};
use crate::utils::transactions::{deploy_contract, get_next_nonce};
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};

// setup high epoch length to avoid kickout out the stopped validator
const EPOCH_LENGTH: u64 = 1000;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_VALIDATORS: usize = 4;

/// Set up a test environment with 2 shards and 4 chunk producers.
fn setup(accounts: &[AccountId]) -> TestLoopEnv {
    let builder = TestLoopBuilder::new();

    let block_and_chunk_producers =
        accounts[1..NUM_VALIDATORS + 1].iter().map(|id| id.as_str()).collect_vec();
    let clients = accounts[0..NUM_VALIDATORS + 1].iter().cloned().collect_vec();

    let boundary_accounts = vec![accounts[3].clone()];
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let validators_spec = ValidatorsSpec::desired_roles(&block_and_chunk_producers, &[]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(accounts, Balance::from_near(1_000_000))
        .genesis_height(GENESIS_HEIGHT)
        .build();

    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();

    builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup()
}

/// Tests if contract could be deployed on a shard if a validator is down
#[test]
fn test_deploy_contract_validator_down() {
    init_test_logger();

    let accounts = make_accounts(NUM_VALIDATORS + 2);
    let mut env = setup(&accounts);

    let rpc_id = &accounts[0]; // "account0" is on shard 2
    let contract_id = &accounts[NUM_VALIDATORS + 1]; // "account5" is on shard 1

    let validator_to_stop = get_validator_to_stop(&env, rpc_id, contract_id);
    let validator_to_stop = env.get_node_data_by_account_id(&validator_to_stop).unwrap().clone();

    // // Stop validator "account3" (chunk producer for shard 1)
    // let validator_to_stop = env.node_datas[3].clone();
    env.kill_node(&validator_to_stop.identifier);
    tracing::info!(
        target: "test",
        "Stopped validator {} (peer id {})",
        validator_to_stop.account_id,
        validator_to_stop.peer_id
    );

    // Verify that blocks are still being produced after one validator is down.
    let node = TestLoopNode::for_account(&env.node_datas, rpc_id);
    node.run_for_number_of_blocks(&mut env.test_loop, 10);

    // Try to make a transfer to an account.
    let transfer_tx = prepare_transfer_tx(&env, rpc_id, contract_id, Balance::from_near(1));
    let transfer_tx_hash = transfer_tx.get_hash();
    submit_tx(&env.node_datas, rpc_id, transfer_tx);
    node.run_for_number_of_blocks(&mut env.test_loop, 10);
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &[transfer_tx_hash]);

    // Try to deploy a contract. This can fail if the stopped validator happens
    // to be the chunk producer for heights head + {1, 2, 3, 6}. Otherwise the
    // contract deployment should succeed.
    tracing::info!(target: "test", "deploying contract");
    let code = near_test_contracts::rs_contract().to_vec();
    let nonce = get_next_nonce(&env.test_loop.data, &env.node_datas, contract_id);
    let tx = deploy_contract(&mut env.test_loop, &env.node_datas, rpc_id, contract_id, code, nonce);
    node.run_for_number_of_blocks(&mut env.test_loop, 10);
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &[tx]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

// Find a validator that is a chunk producer for the shard where `contract_id`
// is located.
fn get_validator_to_stop(
    env: &TestLoopEnv,
    rpc_id: &AccountId,
    contract_id: &AccountId,
) -> AccountId {
    let node = TestLoopNode::for_account(&env.node_datas, rpc_id);
    let env: &TestLoopEnv = env;
    let node: &TestLoopNode<'_> = &node;
    let client = node.client(env.test_loop_data());
    let epoch_id = client.chain.head().unwrap().epoch_id;
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();

    let contract_shard_id = shard_layout.account_id_to_shard_id(contract_id);
    tracing::info!(target: "test", ?contract_shard_id, "Contract shard id");

    let chunk_producers = client
        .epoch_manager
        .get_epoch_chunk_producers_for_shard(&epoch_id, contract_shard_id)
        .unwrap();
    tracing::info!(target: "test", ?chunk_producers, "Chunk Producers");

    chunk_producers[0].clone()
}
