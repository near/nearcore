use std::collections::HashMap;

use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::test_utils::test_loop::ClientQueries;
use near_client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;
const NUM_CLIENTS: usize = 4;

#[test]
fn test_client_with_multi_test_loop() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

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
        .validators_desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[])
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas } =
        builder.genesis(genesis).clients(clients).build();

    // Bootstrap the test by starting the components.
    for idx in 0..NUM_CLIENTS {
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
    for idx in 0..NUM_CLIENTS {
        let client_handle = node_datas[idx].client_sender.actor_handle();
        let event = move |test_loop_data: &mut TestLoopData| {
            let client_actor = test_loop_data.get(&client_handle);
            let block = client_actor.client.chain.get_block_by_height(10002).unwrap();
            assert_eq!(block.header().chunk_mask(), &(0..NUM_CLIENTS).map(|_| true).collect_vec());
        };
        test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
    }
    test_loop.run_instant();

    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let first_epoch_tracked_shards = clients.tracked_shards_for_each_client();
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    let mut balances = accounts
        .iter()
        .cloned()
        .map(|account| (account, initial_balance))
        .collect::<HashMap<_, _>>();

    let anchor_hash = *clients[0].chain.get_block_by_height(10002).unwrap().hash();
    for i in 0..accounts.len() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let tx = SignedTransaction::send_money(
            1,
            accounts[i].clone(),
            accounts[(i + 1) % accounts.len()].clone(),
            &create_user_test_signer(&accounts[i]).into(),
            amount,
            anchor_hash,
        );
        *balances.get_mut(&accounts[i]).unwrap() -= amount;
        *balances.get_mut(&accounts[(i + 1) % accounts.len()]).unwrap() += amount;
        let future = node_datas[i % NUM_CLIENTS]
            .client_sender
            .clone()
            .with_delay(Duration::milliseconds(300 * i as i64))
            .send_async(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
        drop(future);
    }

    // Give plenty of time for these transactions to complete.
    test_loop.run_for(Duration::seconds(40));

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(10),
    );

    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    for account in &accounts {
        assert_eq!(
            clients.query_balance(account),
            *balances.get(account).unwrap(),
            "Account balance mismatch for account {}",
            account
        );
    }

    let later_epoch_tracked_shards = clients.tracked_shards_for_each_client();
    tracing::info!("Later epoch tracked shards: {:?}", later_epoch_tracked_shards);
    assert_ne!(first_epoch_tracked_shards, later_epoch_tracked_shards);

    for idx in 0..NUM_CLIENTS {
        test_loop.data.get_mut(&node_datas[idx].state_sync_dumper_handle).stop();
    }

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test_loop.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
