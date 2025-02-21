use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;
use crate::test_loop::utils::validators::get_epoch_all_validators;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::GenesisAndEpochConfigParams;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::test_genesis::build_genesis_and_epoch_config_store;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::AccountId;
use near_primitives::types::AccountInfo;
use near_primitives_core::version::ProtocolFeature;

#[test]
fn slow_test_fix_cp_stake_threshold() {
    init_test_logger();

    let protocol_version = ProtocolFeature::FixChunkProducerStakingThreshold.protocol_version() - 1;
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
    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            protocol_version,
            epoch_length,
            accounts: &accounts,
            shard_layout,
            validators_spec,
        },
        |genesis_builder| genesis_builder,
        |epoch_config_builder| epoch_config_builder,
    );

    let TestLoopEnv { mut test_loop, datas: node_data, tempdir } = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store.clone())
        .clients(clients)
        .build();

    let sender = node_data[0].client_sender.clone();
    let handle = sender.actor_handle();
    let client = &test_loop.data.get(&handle).client;

    // premise checks
    let epoch_id = client
        .epoch_manager
        .get_epoch_id_from_prev_block(&client.chain.head().unwrap().last_block_hash)
        .unwrap();
    let protocol_version = client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
    let validators = get_epoch_all_validators(client);
    assert!(
        protocol_version < ProtocolFeature::FixChunkProducerStakingThreshold.protocol_version()
    );
    assert_eq!(
        epoch_config_store.get_config(protocol_version).shard_layout.num_shards(),
        num_shards
    );
    assert_eq!(
        validators,
        vec![String::from("test0"), String::from("test1"), String::from("test3")]
    );

    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| {
            let client = &test_loop_data.get(&handle).client;
            let head = client.chain.head().unwrap();
            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&head.prev_block_hash)
                .unwrap();
            // ensure loop is exited because of condition is met
            assert!(epoch_height < 3);

            let epoch_id = client
                .epoch_manager
                .get_epoch_id_from_prev_block(&client.chain.head().unwrap().last_block_hash)
                .unwrap();
            let protocol_version =
                client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
            // exits when protocol version catches up with the fix
            protocol_version >= ProtocolFeature::FixChunkProducerStakingThreshold.protocol_version()
        },
        Duration::seconds(4 * epoch_length as i64),
    );

    test_loop.run_until(
        |test_loop_data: &mut TestLoopData| {
            let client = &test_loop_data.get(&handle).client;
            let head = client.chain.head().unwrap();
            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&head.prev_block_hash)
                .unwrap();
            // ensure loop is exited because of condition is met
            assert!(epoch_height < 5);

            // threshold is raised to approximately 6x of the previous in this case as threshold is
            // no longer divided by num_shards (6), so test3's proposal won't be approved
            let validators = get_epoch_all_validators(client);
            if validators.len() == 2 {
                assert_eq!(validators, vec![String::from("test0"), String::from("test1")]);
                true
            } else {
                false
            }
        },
        Duration::seconds(3 * epoch_length as i64),
    );

    TestLoopEnv { test_loop, datas: node_data, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
