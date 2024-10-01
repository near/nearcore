//! This file contains standard setups for test loop tests.
//! Using TestLoopBuilder gives a lot of flexibility, but sometimes you just need some basic blockchain.

use itertools::Itertools;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

/// 2 producers, 2 validators, 1 rpc node, 4 shards, 20 accounts (account{i}) with 10k NEAR each.
pub fn standard_setup_1() -> TestLoopEnv {
    let num_clients = 5;
    let num_producers = 2;
    let num_validators = 2;
    let num_rpc = 1;
    let accounts =
        (0..20).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();

    let initial_balance = 10000 * ONE_NEAR;
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    // split the clients into producers, validators, and rpc nodes
    let tmp = clients.clone();
    let (producers, tmp) = tmp.split_at(num_producers);
    let (validators, tmp) = tmp.split_at(num_validators);
    let (rpcs, tmp) = tmp.split_at(num_rpc);
    assert!(tmp.is_empty());

    let producers = producers.iter().map(|account| account.as_str()).collect_vec();
    let validators = validators.iter().map(|account| account.as_str()).collect_vec();
    let [_rpc_id] = rpcs else { panic!("Expected exactly one rpc node") };

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

    builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build()
}
