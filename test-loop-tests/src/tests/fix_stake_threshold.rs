use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::validators::get_epoch_all_validators;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::types::AccountInfo;
use near_primitives::version::PROTOCOL_VERSION;

#[test]
fn slow_test_fix_validator_stake_threshold() {
    init_test_logger();

    let test_loop_builder = TestLoopBuilder::new();
    let epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let epoch_length = 10;
    let initial_balance = 1_000_000 * ONE_NEAR;
    let accounts =
        (0..6).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let validators = vec![
        AccountInfo {
            account_id: accounts[0].clone(),
            public_key: create_test_signer(accounts[0].as_str()).public_key(),
            amount: 300_000 * 62_500 * ONE_NEAR,
        },
        AccountInfo {
            account_id: accounts[1].clone(),
            public_key: create_test_signer(accounts[1].as_str()).public_key(),
            amount: 300_000 * 62_500 * ONE_NEAR,
        },
        AccountInfo {
            account_id: accounts[2].clone(),
            public_key: create_test_signer(accounts[2].as_str()).public_key(),
            amount: 100_000 * ONE_NEAR,
        },
    ];
    let validators_spec = ValidatorsSpec::raw(validators, 3, 3, 3);

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&test_loop_builder.clock())
        .shard_layout(epoch_config_store.get_config(PROTOCOL_VERSION).shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .max_inflation_rate(Rational32::new(0, 1))
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();

    let TestLoopEnv { test_loop, node_datas, shared_state } = test_loop_builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let client = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client;

    let head = client.chain.head().unwrap();
    let epoch_id =
        client.epoch_manager.get_epoch_id_from_prev_block(&head.prev_block_hash).unwrap();
    let epoch_info = client.epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let validators = get_epoch_all_validators(client);
    let total_stake = validators
        .iter()
        .map(|v| {
            let account_id = &v.parse().unwrap();
            epoch_info.get_validator_stake(account_id).unwrap()
        })
        .sum::<u128>();

    assert_eq!(validators.len(), 2, "proposal with stake at threshold should not be approved");
    assert_eq!(total_stake / ONE_NEAR, 37_500_000_000);

    // after threshold fix
    // threshold = min_stake_ratio * total_stake / (1 - min_stake_ratio)
    //           = (1 / 62_500) * total_stake / (62_499 / 62_500)
    //           = total_stake / 62_499
    assert_eq!(epoch_info.seat_price() / ONE_NEAR, total_stake / 62_499 / ONE_NEAR);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
