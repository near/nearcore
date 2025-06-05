use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use parking_lot::Mutex;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::get_anchor_hash;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let accounts: Vec<AccountId> =
        (0..10).map(|i| format!("account{}", i).parse().unwrap()).collect_vec();
    let block_and_chunk_producers = accounts.iter().take(4).cloned().collect_vec();

    let validators_only: Vec<AccountId> =
        (0..5).map(|i| format!("validator{i}").parse().unwrap()).collect_vec();

    let clients =
        block_and_chunk_producers.iter().cloned().chain(validators_only.clone()).collect_vec();

    let epoch_length = 10;
    let shard_layout = ShardLayout::multi_shard_custom(
        ["account3", "account5", "account7"].map(|a| a.parse().unwrap()).to_vec(),
        1,
    );
    let validators_spec = ValidatorsSpec::desired_roles(
        &block_and_chunk_producers.iter().map(|a| a.as_str()).collect_vec(),
        &validators_only.iter().map(|a| a.as_str()).collect_vec(),
    );
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
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

    let sent_txs = schedule_send_money_txs(&node_datas, accounts, &test_loop);

    let mut observed_txs = HashSet::new();
    let client_handles =
        node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();
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

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn schedule_send_money_txs(
    node_datas: &Vec<crate::setup::state::NodeExecutionData>,
    accounts: Vec<AccountId>,
    test_loop: &near_async::test_loop::TestLoopV2,
) -> Arc<Mutex<HashSet<near_primitives::hash::CryptoHash>>> {
    let sent_txs = Arc::new(Mutex::new(HashSet::new()));
    let node_data = Arc::new(node_datas.to_vec());
    for (i, sender) in accounts.iter().cloned().enumerate() {
        let amount = ONE_NEAR * (i as u128 + 1);
        let receiver = accounts[(i + 1) % accounts.len()].clone();
        let node_data = node_data.clone();
        let sent_txs = sent_txs.clone();
        test_loop.send_adhoc_event_with_delay(
            format!("transaction {}", i),
            Duration::milliseconds(300 * i as i64),
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
    sent_txs
}
