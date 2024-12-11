use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::validators::get_epoch_all_validators;
use crate::test_loop::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::types::AccountInfo;
use near_primitives_core::version::ProtocolFeature;

#[test]
fn slow_test_fix_validator_stake_threshold() {
    init_test_logger();

    let protocol_version = ProtocolFeature::FixStakingThreshold.protocol_version() - 1;
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
        .protocol_version(protocol_version)
        .genesis_time_from_clock(&test_loop_builder.clock())
        .shard_layout(epoch_config_store.get_config(protocol_version).shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .max_inflation_rate(Rational32::new(0, 1))
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();

    let TestLoopEnv { mut test_loop, datas: node_data, tempdir } = test_loop_builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store.clone())
        .clients(clients)
        .build();

    let sender = node_data[0].client_sender.clone();
    let handle = sender.actor_handle();
    let client = &test_loop.data.get(&handle).client;

    let epoch_id = client
        .epoch_manager
        .get_epoch_id_from_prev_block(&client.chain.head().unwrap().last_block_hash)
        .unwrap();
    let epoch_info = client.epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let protocol_version = client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
    let validators = get_epoch_all_validators(client);
    let total_stake = validators
        .iter()
        .map(|v| {
            let account_id = &v.parse().unwrap();
            epoch_info.get_validator_stake(account_id).unwrap()
        })
        .sum::<u128>();
    let num_shards = epoch_config_store.get_config(protocol_version).shard_layout.num_shards();

    assert!(protocol_version < ProtocolFeature::FixStakingThreshold.protocol_version());
    assert_eq!(validators.len(), 2, "proposal with stake at threshold should not be approved");
    assert_eq!(total_stake / ONE_NEAR, 37_500_000_000);
    // prior to threshold fix
    // threshold = min_stake_ratio * total_stake
    //           = (1 / 62_500) * total_stake
    // TODO Chunk producer stake threshold is dependent on the number of shards, which should no
    // longer be the case.  Get rid of num_shards once protocol is updated to correct the ratio.
    assert_eq!(epoch_info.seat_price() * num_shards as u128 / ONE_NEAR, 600_000);

    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| {
            let client = &test_loop_data.get(&handle).client;
            let head = client.chain.head().unwrap();
            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&head.prev_block_hash)
                .unwrap();
            // ensure loop is exited because condition is met instead of timeout
            assert!(epoch_height < 3);

            let epoch_id = client
                .epoch_manager
                .get_epoch_id_from_prev_block(&client.chain.head().unwrap().last_block_hash)
                .unwrap();
            // chain will advance to the latest protocol version
            let protocol_version =
                client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
            if protocol_version >= ProtocolFeature::FixStakingThreshold.protocol_version() {
                let epoch_info = client.epoch_manager.get_epoch_info(&epoch_id).unwrap();
                let num_shards =
                    epoch_config_store.get_config(protocol_version).shard_layout.num_shards();
                // after threshold fix
                // threshold = min_stake_ratio * total_stake / (1 - min_stake_ratio)
                //           = (1 / 62_500) * total_stake / (62_499 / 62_500)
                assert_eq!(epoch_info.seat_price() * num_shards as u128 / ONE_NEAR, 600_001);
                true
            } else {
                false
            }
        },
        Duration::seconds(4 * epoch_length as i64),
    );

    TestLoopEnv { test_loop, datas: node_data, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
