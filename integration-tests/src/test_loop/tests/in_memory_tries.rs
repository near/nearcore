use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::test_utils::test_loop::ClientQueries;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use near_store::ShardUId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::{execute_money_transfers, ONE_NEAR};

/// Runs chain with sequence of chunks with empty state changes, long enough to
/// cover 5 epochs which is default GC period.
/// After that, it checks that memtrie for the shard can be loaded.
/// This is a repro for #11583 where flat storage head was not moved at all at
/// this scenario, so chain data related to that block was garbage collected,
/// and loading memtrie failed because of missing `ChunkExtra` with desired
/// state root.
#[test]
fn test_load_memtrie_after_empty_chunks() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let num_accounts = 3;
    let num_clients = 2;
    let epoch_length = 5;
    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (num_accounts - num_clients..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let client_accounts = accounts.iter().take(num_clients).cloned().collect_vec();
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        // Set 2 shards, first of which doesn't have any validators.
        .shard_layout_simple_v1(&["account1"])
        .transaction_validity_period(1000)
        .epoch_length(epoch_length)
        .validators_desired_roles(&client_accounts.iter().map(|t| t.as_str()).collect_vec(), &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas } =
        builder.genesis(genesis).clients(client_accounts).build();

    // Bootstrap the test by starting the components.
    for idx in 0..num_clients {
        let state_sync_dumper_handle = node_datas[idx].state_sync_dumper_handle.clone();
        test_loop.send_adhoc_event("start_state_sync_dumper".to_owned(), move |test_loop_data| {
            test_loop_data.get_mut(&state_sync_dumper_handle).start().unwrap();
        });
    }

    // Give it some condition to stop running at. Here we run the test until the first client
    // reaches height 10003, with a timeout of 5sec (failing if it doesn't reach 10003 in time).
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            let client_actor = test_loop_data.get(&client_handle);
            client_actor.client.chain.head().unwrap().height == 10003
        },
        Duration::seconds(5),
    );
    for idx in 0..num_clients {
        let client_handle = node_datas[idx].client_sender.actor_handle();
        let event = move |test_loop_data: &mut TestLoopData| {
            let client_actor = test_loop_data.get(&client_handle);
            let block = client_actor.client.chain.get_block_by_height(10002).unwrap();
            assert_eq!(block.header().chunk_mask(), &(0..num_clients).map(|_| true).collect_vec());
        };
        test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
    }
    test_loop.run_instant();

    execute_money_transfers(&mut test_loop, &node_datas, &accounts);

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height
                > 10000 + epoch_length * 10
        },
        Duration::seconds(10),
    );

    // Find client currently tracking shard 0.
    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let idx = {
        let current_tracked_shards = clients.tracked_shards_for_each_client();
        tracing::info!("Current tracked shards: {:?}", current_tracked_shards);
        current_tracked_shards
            .iter()
            .enumerate()
            .find_map(|(idx, shards)| if shards.contains(&0) { Some(idx) } else { None })
            .expect("Not found any client tracking shard 0")
    };

    // Unload memtrie and load it back, check that it doesn't panic.
    let tip = clients[idx].chain.head().unwrap();
    let shard_layout = clients[idx].epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    clients[idx]
        .runtime_adapter
        .get_tries()
        .unload_mem_trie(&ShardUId::from_shard_id_and_layout(0, &shard_layout));
    clients[idx]
        .runtime_adapter
        .get_tries()
        .load_mem_trie(&ShardUId::from_shard_id_and_layout(0, &shard_layout), None, true)
        .expect("Couldn't load memtrie");

    for idx in 0..num_clients {
        test_loop.data.get_mut(&node_datas[idx].state_sync_dumper_handle).stop();
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
