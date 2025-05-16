use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_vm_runner::ContractCode;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::contract_distribution::{
    assert_all_chunk_endorsements_received, clear_compiled_contract_caches,
    run_until_caches_contain_contract,
};
use crate::utils::transactions::{call_contract, check_txs, deploy_contract, make_accounts};
use crate::utils::{ONE_NEAR, get_node_head_height};

const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 4;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 4;
const NUM_RPC: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;
const NUM_ACCOUNTS: usize = NUM_VALIDATORS + NUM_RPC;

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

    let (mut env, rpc_id) = setup(&accounts);

    let mut nonce = 1;
    let rpc_index = 8;
    assert_eq!(accounts[rpc_index], rpc_id);

    // Deploy a contract for each shard (account0 from first one, and account4 from second one).
    // Then take two accounts from each shard (one with a contract deployed and one without) and
    // make them call both the contracts, so we cover same-shard and cross-shard contract calls.
    let contract_ids = [&accounts[0], &accounts[4]];
    let sender_ids = [&accounts[0], &accounts[1], &accounts[4], &accounts[5]];

    let start_height = get_node_head_height(&env, &accounts[0]);

    // First deploy and call the contracts as described above.
    // Next, clear the compiled contract cache and repeat the same contract calls.
    let contracts = deploy_contracts(&mut env, &rpc_id, &contract_ids, &mut nonce);

    for contract in contracts {
        run_until_caches_contain_contract(&mut env, contract.hash());
    }

    call_contracts(&mut env, &rpc_id, &contract_ids, &sender_ids, &mut nonce);

    clear_compiled_contract_caches(&mut env);

    call_contracts(&mut env, &rpc_id, &contract_ids, &sender_ids, &mut nonce);

    let end_height = get_node_head_height(&env, &accounts[0]);
    assert_all_chunk_endorsements_received(&mut env, start_height, end_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn setup(accounts: &Vec<AccountId>) -> (TestLoopEnv, AccountId) {
    let builder = TestLoopBuilder::new();

    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();

    let clients = accounts.iter().take(NUM_VALIDATORS + NUM_RPC).cloned().collect_vec();
    let rpc_id = accounts[NUM_VALIDATORS].clone();

    let shard_layout = ShardLayout::simple_v1(&["account4"]);
    let validators_spec =
        ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(GENESIS_HEIGHT)
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .minimum_validators_per_shard(2)
        .build_store_for_genesis_protocol_version();

    let env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();
    (env, rpc_id)
}

/// Deploys a contract for the given accounts (`contract_ids`) and waits until the transactions are executed.
/// Each account in `contract_ids` gets a fake contract with a different size (thus code-hashes are different)
/// Returns the list of contracts deployed.
fn deploy_contracts(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    contract_ids: &[&AccountId],
    nonce: &mut u64,
) -> Vec<ContractCode> {
    let mut contracts = vec![];
    let mut txs = vec![];
    for (i, contract_id) in contract_ids.into_iter().enumerate() {
        tracing::info!(target: "test", ?rpc_id, ?contract_id, "Deploying contract.");
        let contract =
            ContractCode::new(near_test_contracts::sized_contract((i + 1) * 100).to_vec(), None);
        let tx = deploy_contract(
            &mut env.test_loop,
            &env.node_datas,
            rpc_id,
            contract_id,
            contract.code().to_vec(),
            *nonce,
        );
        txs.push(tx);
        *nonce += 1;
        contracts.push(contract);
    }
    env.test_loop.run_for(Duration::seconds(2));
    check_txs(&env.test_loop.data, &env.node_datas, rpc_id, &txs);
    contracts
}

/// Makes calls to the contracts from sender_ids to the contract_ids (at which contracts are deployed).
fn call_contracts(
    env: &mut TestLoopEnv,
    rpc_id: &AccountId,
    contract_ids: &[&AccountId],
    sender_ids: &[&AccountId],
    nonce: &mut u64,
) {
    let method_name = "main".to_owned();
    let mut txs = vec![];
    for sender_id in sender_ids {
        for contract_id in contract_ids {
            tracing::info!(target: "test", ?rpc_id, ?sender_id, ?contract_id, "Calling contract.");
            let tx = call_contract(
                &mut env.test_loop,
                &env.node_datas,
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
    env.test_loop.run_for(Duration::seconds(2));
    check_txs(&env.test_loop.data, &env.node_datas, &rpc_id, &txs);
}
