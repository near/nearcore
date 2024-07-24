use std::collections::HashSet;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;
use crate::test_loop::utils::ONE_NEAR;

const NUM_VALIDATORS: usize = 2;
const NUM_ACCOUNTS: usize = 20;
const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 2;

#[test]
fn test_rpc_to_archival_node() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    // Validators take all the roles: block+chunk producer and chunk validator.
    let validators = (0..NUM_VALIDATORS).map(|idx| accounts[idx].as_str()).collect_vec();

    // Contains the accounts of the validators and the non-validator archival node.
    let all_clients: Vec<AccountId> =
        accounts.iter().take(NUM_VALIDATORS + 1).cloned().collect_vec();
    // Contains the account of the non-validator archival node.
    let archival_clients: HashSet<AccountId> =
        vec![all_clients[NUM_VALIDATORS].clone()].into_iter().collect();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(0)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&validators, &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .clients(all_clients)
        .archival(archival_clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build();

    let client_handle = node_datas[0].client_sender.actor_handle();

    let non_validator_accounts = accounts.iter().skip(NUM_VALIDATORS).cloned().collect_vec();
    execute_money_transfers(&mut test_loop, &node_datas, &non_validator_accounts);

    // Run the chain until it transitions to a different epoch then prev_epoch_id.
    let target_height: u64 = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    test_loop.run_until(
        |test_loop_data| {
            let chain = &test_loop_data.get(&client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(target_height as i64),
    );

    /// Sanity test: Ask for block at a low height, expect archive node to provide it but validator node not.
    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
