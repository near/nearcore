use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, EpochId};
use near_primitives_core::types::EpochHeight;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[test]
fn test_chunk_validator_kickout() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let clients_str = clients.iter().map(|a| a.as_str()).collect_vec();
    let (block_and_chunk_producers, chunk_validators_only) = clients_str.split_at(6);
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account2", "account4", "account6"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(block_and_chunk_producers, chunk_validators_only)
        .target_validator_mandates_per_shard(1);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas } =
        builder.genesis(genesis).clients(clients).drop_chunks_validated_by("account6").build();

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    let mut epoch_infos = Arc::new(Mutex::new(BTreeMap::<EpochHeight, EpochId>::new()));
    test_loop.run_until(
        |test_loop_data| {
            let client = &test_loop_data.get(&client_handle).client;
            let tip = client.chain.head().unwrap();
            let epoch_id = tip.epoch_id;
            let epoch_height = if epoch_id == EpochId::default() {
                0 // weird corner case.
            } else {
                client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap()
            };
            let mut epoch_infos_lock = epoch_infos.lock().unwrap();
            if let Some(epoch_id) = epoch_infos_lock.get(&epoch_height) {
                assert_eq!(epoch_id, &tip.epoch_id);
            } else {
                epoch_infos_lock.insert(epoch_height, tip.epoch_id);
            }
            epoch_height >= 3
        },
        Duration::seconds(40),
    );

    let mut epoch_infos_lock = epoch_infos.lock().unwrap();
    let client = &test_loop.data.get(&client_handle).client;
    for (epoch_height, epoch_id) in epoch_infos_lock.iter() {
        // client.epoch_manager.
        println!("Epoch {}: {:?}", epoch_height, epoch_id);
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    TestLoopEnv { test_loop, datas: node_datas }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
