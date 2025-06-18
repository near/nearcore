use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::{CanSend as _, Handler as _};
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{ProcessTxRequest, Query};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockReference};
use near_primitives::views::{QueryRequest, QueryResponseKind};
use parking_lot::Mutex;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::transactions::get_anchor_hash;
use crate::utils::{ONE_NEAR, get_node_data};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts: Vec<AccountId> =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect_vec();

    let num_block_producers = 4;
    let num_validators = 0;

    let block_and_chunk_producers =
        accounts.iter().take(num_block_producers).cloned().collect_vec();
    let validators_only: Vec<AccountId> =
        (0..num_validators).map(|i| format!("validator{i}").parse().unwrap()).collect_vec();

    let clients =
        block_and_chunk_producers.iter().cloned().chain(validators_only.clone()).collect_vec();

    let epoch_length = 10;
    // With each block producer tracking more than one shard we are more likely to find bugs of
    // chunks being processed inconsistently.
    // More than one shard in total allows testing cross-shard communications.
    let shard_layout =
        ShardLayout::multi_shard_custom(vec![accounts[accounts.len() / 2].clone()], 1);
    let validators_spec = ValidatorsSpec::desired_roles(
        &block_and_chunk_producers.iter().map(|a| a.as_str()).collect_vec(),
        &validators_only.iter().map(|a| a.as_str()).collect_vec(),
    );

    const INITIAL_BALANCE: u128 = 1_000_000 * ONE_NEAR;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, INITIAL_BALANCE)
        .genesis_height(10000)
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let client_handles =
        node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

    // TODO(spice): Should be able to use execute_money_transfers here eventually. It shouldn't reach into
    // runtime directly though, or do it correctly with separated execution and chunks.

    let client = &test_loop.data.get(&client_handles[0]).client;
    let epoch_manager = client.epoch_manager.clone();

    let get_balance = |test_loop_data: &mut TestLoopData, account, epoch_id| {
        let shard_id = shard_layout.account_id_to_shard_id(account);
        let cp =
            &epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id).unwrap()[0];

        let view_client_handle = get_node_data(&node_datas, cp).view_client_sender.actor_handle();

        let view_client = test_loop_data.get_mut(&view_client_handle);

        let query_response = view_client.handle(Query::new(
            BlockReference::latest(),
            QueryRequest::ViewAccount { account_id: account.clone() },
        ));

        let query_response = query_response.unwrap();

        let QueryResponseKind::ViewAccount(view_account_result) = query_response.kind else {
            panic!();
        };
        view_account_result.amount
    };

    let epoch_id = client.chain.head().unwrap().epoch_id;
    for account in &accounts {
        let got_balance = get_balance(&mut test_loop.data, account, epoch_id);
        assert_eq!(got_balance, INITIAL_BALANCE);
    }

    let (sent_txs, balance_changes) = schedule_send_money_txs(&node_datas, &accounts, &test_loop);

    let mut observed_txs = HashSet::new();
    test_loop.run_until(
        |test_loop_data| {
            let clients = client_handles
                .iter()
                .map(|handle| &test_loop_data.get(&handle).client)
                .collect_vec();

            let head = clients[0].chain.final_head().unwrap();
            let block = clients[0].chain.get_block(&head.last_block_hash).unwrap();

            let height = head.height;
            let chunk_mask = block.header().chunk_mask();
            assert_eq!(chunk_mask, vec![true; chunk_mask.len()]);

            for chunk in block.chunks().iter_raw() {
                for client in &clients {
                    let Ok(chunk) = client.chain.get_chunk(&chunk.chunk_hash()) else {
                        continue;
                    };
                    for tx in chunk.to_transactions() {
                        observed_txs.insert(tx.get_hash());
                    }
                    assert_eq!(chunk.prev_outgoing_receipts(), vec![]);
                }
            }

            height > 10030
        },
        Duration::seconds(35),
    );

    assert_eq!(*sent_txs.lock(), observed_txs);

    let client = &test_loop.data.get(&client_handles[0]).client;
    let epoch_id = client.chain.head().unwrap().epoch_id;

    assert!(!balance_changes.is_empty());
    for (account, balance_change) in &balance_changes {
        let got_balance = get_balance(&mut test_loop.data, account, epoch_id);
        let want_balance = (INITIAL_BALANCE as i128 + balance_change) as u128;
        assert_eq!(got_balance, want_balance);
        assert_ne!(*balance_change, 0);
    }

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn schedule_send_money_txs(
    node_datas: &Vec<crate::setup::state::NodeExecutionData>,
    accounts: &[AccountId],
    test_loop: &near_async::test_loop::TestLoopV2,
) -> (Arc<Mutex<HashSet<near_primitives::hash::CryptoHash>>>, HashMap<AccountId, i128>) {
    let sent_txs = Arc::new(Mutex::new(HashSet::new()));
    let mut balance_changes = HashMap::new();
    let node_data = Arc::new(node_datas.to_vec());
    for (i, sender) in accounts.iter().cloned().enumerate() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let receiver = accounts[(i + 1) % accounts.len()].clone();
        *balance_changes.entry(sender.clone()).or_default() -= amount as i128;
        *balance_changes.entry(receiver.clone()).or_default() += amount as i128;

        let node_data = node_data.clone();
        let sent_txs = sent_txs.clone();
        test_loop.send_adhoc_event_with_delay(
            format!("transaction {}", i),
            Duration::milliseconds(100 * i as i64),
            move |data| {
                let clients = node_data
                    .iter()
                    .map(|test_data| &data.get(&test_data.client_sender.actor_handle()).client)
                    .collect_vec();

                let anchor_hash = get_anchor_hash(&clients);

                let tx = SignedTransaction::send_money(
                    1,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender),
                    amount,
                    anchor_hash,
                );
                sent_txs.lock().insert(tx.get_hash());
                let process_tx_request =
                    ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
                node_data[i % clients.len()].rpc_handler_sender.send(process_tx_request);
            },
        );
    }
    (sent_txs, balance_changes)
}
