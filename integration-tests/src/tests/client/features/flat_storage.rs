use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::tests::client::process_blocks::deploy_test_contract_with_protocol_version;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::Genesis;
use near_client::test_utils::test_loop::ClientQueries;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_parameters::ExtCosts;
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::{create_user_test_signer, encode};
use near_primitives::transaction::{
    Action, ExecutionMetadata, FunctionCallAction, SignedTransaction, Transaction, TransactionV0,
};
use near_primitives::types::AccountId;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::Gas;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use std::collections::HashMap;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

/// Check that after flat storage upgrade:
/// - value read from contract is the same;
/// - touching trie node cost for read decreases to zero.
#[test]
fn test_flat_storage_upgrade() {
    // The immediate protocol upgrade needs to be set for this test to pass in
    // the release branch where the protocol upgrade date is set.
    std::env::set_var("NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE", "now");

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 12;
    let new_protocol_version = ProtocolFeature::FlatStorageReads.protocol_version();
    let old_protocol_version = new_protocol_version - 1;
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = old_protocol_version;
    let runtime_config = near_parameters::RuntimeConfigStore::new(None);
    let mut env = TestEnv::builder(&genesis.config)
        .nightshade_runtimes_with_runtime_config_store(&genesis, vec![runtime_config])
        .build();

    // We assume that it is enough to process 4 blocks to get a single txn included and processed.
    // At the same time, once we process `>= 2 * epoch_length` blocks, protocol can get
    // auto-upgraded to latest version. We use this value to process 3 transactions for older
    // protocol version. So we choose this value to be `epoch_length / 3` and we process only
    // `epoch_length` blocks in total.
    // TODO (#8703): resolve this properly
    let blocks_to_process_txn = epoch_length / 3;

    // Deploy contract to state.
    deploy_test_contract_with_protocol_version(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::backwards_compatible_rs_contract(),
        blocks_to_process_txn,
        1,
        old_protocol_version,
    );

    let signer: Signer =
        InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0").into();
    let gas = 20_000_000_000_000;
    let tx = TransactionV0 {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![],
        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Write key-value pair to state.
    {
        let write_value_action = vec![Action::FunctionCall(Box::new(FunctionCallAction {
            args: encode(&[1u64, 10u64]),
            method_name: "write_key_value".to_string(),
            gas,
            deposit: 0,
        }))];
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction = Transaction::V0(TransactionV0 {
            nonce: 10,
            block_hash: tip.last_block_hash,
            actions: write_value_action,
            ..tx.clone()
        })
        .sign(&signer);
        let tx_hash = signed_transaction.get_hash();
        assert_eq!(
            env.clients[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
        for i in 0..blocks_to_process_txn {
            env.produce_block(0, tip.height + i + 1);
        }

        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap().assert_success();
    }

    let touching_trie_node_costs: Vec<_> = (0..2)
        .map(|i| {
            let read_value_action = vec![Action::FunctionCall(Box::new(FunctionCallAction {
                args: encode(&[1u64]),
                method_name: "read_value".to_string(),
                gas,
                deposit: 0,
            }))];
            let tip = env.clients[0].chain.head().unwrap();
            let signed_transaction = Transaction::V0(TransactionV0 {
                nonce: 20 + i,
                block_hash: tip.last_block_hash,
                actions: read_value_action,
                ..tx.clone()
            })
            .sign(&signer);
            let tx_hash = signed_transaction.get_hash();
            assert_eq!(
                env.clients[0].process_tx(signed_transaction, false, false),
                ProcessTxResponse::ValidTx
            );
            for i in 0..blocks_to_process_txn {
                env.produce_block(0, tip.height + i + 1);
            }
            if i == 0 {
                env.upgrade_protocol_to_latest_version();
            }

            let final_transaction_result =
                env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
            final_transaction_result.assert_success();
            let receipt_id = final_transaction_result.receipts_outcome[0].id;
            let metadata = env.clients[0]
                .chain
                .get_execution_outcome(&receipt_id)
                .unwrap()
                .outcome_with_id
                .outcome
                .metadata;
            if let ExecutionMetadata::V3(profile_data) = metadata {
                profile_data.get_ext_cost(ExtCosts::touching_trie_node)
            } else {
                panic!("Too old version of metadata: {metadata:?}");
            }
        })
        .collect();

    // Guaranteed touching trie node cost in all protocol versions until
    // `ProtocolFeature::FlatStorageReads`, included.
    let touching_trie_node_base_cost: Gas = 16_101_955_926;

    // For the first read, cost should be 3 TTNs because trie path is:
    // (Branch) -> (Extension) -> (Leaf) -> (Value)
    // but due to a bug in storage_read we don't charge for Value.
    assert_eq!(touching_trie_node_costs[0], touching_trie_node_base_cost * 3);

    // For the second read, we don't go to Flat storage and don't charge TTN.
    assert_eq!(touching_trie_node_costs[1], 0);
}

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
    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (num_accounts - num_clients..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();
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
        .epoch_length(5)
        .validators_desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas } =
        builder.genesis(genesis).clients(clients).build();

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

    let clients = node_datas
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
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
        let future = node_datas[i % num_clients]
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

    // Find client currently tracking shard 0.
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
