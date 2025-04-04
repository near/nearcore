use itertools::Itertools as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, NumSeats};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;

#[test]
fn test_validator_rotation() {
    init_test_logger();

    let stake = ONE_NEAR;

    let validators: Vec<Vec<AccountId>> = [
        vec!["test1.1", "test1.2", "test1.3"],
        vec!["test2.1", "test2.2", "test2.3", "test2.4", "test2.5"],
    ]
    .iter()
    .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
    .collect();

    let mut runner = RotatingValidatorsRunner::new(stake, validators.clone());

    let accounts = runner.all_validators_accounts();
    let epoch_length: u64 = 10;
    let seats: NumSeats = validators[0].len().try_into().unwrap();
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::single_shard())
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    let client_actor_handle = env.node_datas[0].client_sender.actor_handle();

    let assert_current_validators = |env: &TestLoopEnv, validators: &[AccountId]| {
        let client = &env.test_loop.data.get(&client_actor_handle).client;
        let epoch_id = client.chain.head().unwrap().epoch_id;
        let current_validators: Vec<_> = client
            .epoch_manager
            .get_epoch_all_validators(&epoch_id)
            .unwrap()
            .into_iter()
            .map(|v| {
                let (account, _, _) = v.destructure();
                account
            })
            .sorted()
            .collect();
        let validators: Vec<AccountId> =
            validators.iter().map(ToOwned::to_owned).sorted().collect();
        assert_eq!(current_validators, validators);
    };

    assert_current_validators(&env, &validators[0]);

    // At the end of epoch T we define epoch_info (which defines validator set) for epoch T+2 using
    // current staking. Because of this validator reassignment would have 1 epoch lag compared to
    // staking distribution.
    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[0]);

    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[1]);

    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[0]);

    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[1]);

    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[0]);

    runner.run_for_an_epoch(&mut env);
    assert_current_validators(&env, &validators[1]);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
