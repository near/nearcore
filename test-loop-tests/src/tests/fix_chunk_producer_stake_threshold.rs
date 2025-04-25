use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::validators::get_epoch_all_validators;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::types::AccountInfo;

#[test]
fn slow_test_fix_cp_stake_threshold() {
    init_test_logger();

    let epoch_length = 10;
    let accounts =
        (0..6).map(|i| format!("test{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect::<Vec<_>>();
    let num_shards = 6;
    let shard_layout = ShardLayout::multi_shard(num_shards, 1);
    let validators = vec![
        AccountInfo {
            account_id: accounts[0].clone(),
            public_key: create_test_signer(accounts[0].as_str()).public_key(),
            amount: 30 * 62500 * ONE_NEAR,
        },
        AccountInfo {
            account_id: accounts[1].clone(),
            public_key: create_test_signer(accounts[1].as_str()).public_key(),
            amount: 30 * 62500 * ONE_NEAR,
        },
        AccountInfo {
            account_id: accounts[2].clone(),
            public_key: create_test_signer(accounts[2].as_str()).public_key(),
            // cp min stake ratio was `(1 / 62500) / num_shards` before the fix
            // stake is right at threshold, and proposal should not be approved
            amount: ((30 + 30) * 62500 / 62500 / num_shards) as u128 * ONE_NEAR,
        },
        AccountInfo {
            account_id: accounts[3].clone(),
            public_key: create_test_signer(accounts[3].as_str()).public_key(),
            // stake is above threshold, so proposal should be approved
            amount: ((30 + 30) * 62500 / 62500 / num_shards + 1) as u128 * ONE_NEAR,
        },
    ];
    let validators_spec = ValidatorsSpec::raw(validators, 5, 5, 5);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let TestLoopEnv { test_loop, node_datas, shared_state } = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let client = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client;
    let validators = get_epoch_all_validators(client);
    assert_eq!(validators, vec![String::from("test0"), String::from("test1")]);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
