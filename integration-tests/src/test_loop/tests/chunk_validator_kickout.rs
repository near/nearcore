use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use std::string::ToString;

const VALIDATOR_TO_KICKOUT: &str = "account6";

/// Checks that chunk validator with low endorsement stats is kicked out.
#[test]
fn test_chunk_validator_kickout() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let epoch_length = 10;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let clients_str = clients.iter().map(|a| a.as_str()).collect_vec();
    let (block_and_chunk_producers, chunk_validators_only) = clients_str.split_at(6);
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout_simple_v1(&["account2", "account4", "account6"])
        .epoch_length(epoch_length)
        // Select 6 block&chunk producers and 2 chunk validators.
        .validators_desired_roles(block_and_chunk_producers, chunk_validators_only)
        // Set up config to kick out only chunk validators for low performance.
        .kickouts_for_chunk_validators_only()
        // Target giving one mandate to each chunk validator, which results in
        // every chunk validator validating only one shard in most cases.
        .target_validator_mandates_per_shard(1);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas } = builder
        .genesis(genesis)
        .clients(clients)
        // Drop only chunks validated by `VALIDATOR_TO_KICKOUT`.
        // By how our endorsement stats are computed, this will count as this
        // validator validating zero chunks.
        .drop_chunks_validated_by(VALIDATOR_TO_KICKOUT)
        .build();

    // Run chain until our targeted chunk validator is kicked out.
    let client_handle = node_datas[0].client_sender.actor_handle();
    let initial_validators = get_epoch_all_validators(&test_loop.data.get(&client_handle).client);
    assert_eq!(initial_validators.len(), 8);
    assert!(initial_validators.contains(&VALIDATOR_TO_KICKOUT.to_string()));
    test_loop.run_until(
        |test_loop_data| {
            let validators = get_epoch_all_validators(&test_loop_data.get(&client_handle).client);
            if validators.len() == 7 {
                assert!(!validators.contains(&VALIDATOR_TO_KICKOUT.to_string()));
                return true;
            }
            return false;
        },
        // Timeout at producing 5 epochs, approximately.
        Duration::seconds((5 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Get all validator account names for the latest epoch.
fn get_epoch_all_validators(client: &Client) -> Vec<String> {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let all_validators = client.epoch_manager.get_epoch_all_validators(&epoch_id).unwrap();
    all_validators.into_iter().map(|vs| vs.account_id().to_string()).collect()
}
