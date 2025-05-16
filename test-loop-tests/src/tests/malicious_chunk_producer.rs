#![cfg(feature = "test_features")] // required for adversarial behaviors
//! Test behaviors of the network when the chunk producer is malicious or misbehaving.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::get_anchor_hash;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_client::client_actor::{AdvProduceChunksMode, NetworkAdversarialMessage};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::{QueryRequest, QueryResponseKind};

#[test]
fn test_producer_with_expired_transactions() {
    let accounts =
        (0..3).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let chunk_producer = accounts[0].as_str();
    let validators: Vec<_> = accounts[1..].iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &validators);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(ShardLayout::simple_v1(&[]))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(10)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts.clone())
        .build()
        .warmup();
    let TestLoopEnv { test_loop, node_datas, .. } = &mut test_loop_env;

    // First we're gonna ask the chunk producer to keep producing empty chunks and send some
    // transactions as well. This will keep transactions in the transaction pool for more blocks
    // than the transactions are valid for.
    let chunk_producer = &node_datas[0];
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("set chunk production without transactions".into(), move |_| {
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTx,
        ));
    });
    for account in &accounts[1..] {
        let chunk_producer = chunk_producer.clone();
        let sender = account.clone();
        let receiver = accounts[0].clone();
        test_loop.send_adhoc_event("transaction".into(), move |data| {
            let signer = create_user_test_signer(&sender);
            let clients = vec![&data.get(&chunk_producer.client_sender.actor_handle()).client];
            let response = clients.runtime_query(
                &receiver,
                QueryRequest::ViewAccessKey {
                    account_id: sender.clone(),
                    public_key: signer.public_key(),
                },
            );
            let QueryResponseKind::AccessKey(access_key) = response.kind else {
                panic!("Expected AccessKey response");
            };
            let anchor_hash = get_anchor_hash(&clients);
            let tx = SignedTransaction::send_money(
                access_key.nonce + 1,
                sender,
                receiver,
                &signer,
                ONE_NEAR,
                anchor_hash,
            );
            let process_tx_request =
                ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
            chunk_producer.rpc_handler_sender.send(process_tx_request);
        });
    }

    // Now that we've sent the transactions, wait for 25 chunk productions without transactions
    // being included.
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data
                .get(&chunk_producer.client_sender.actor_handle())
                .client
                .chain
                .head()
                .unwrap()
                .height
                > 10025
        },
        Duration::seconds(30),
    );

    // Produce chunks without validity checks! The chunks should contain the transactions, but the
    // validators should simply discard the transactions.
    // For a good measure insert some invalid transactions that may be invalid in other ways than
    // them having been expired.
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("produce chunks without validity checks".into(), move |_| {
        data_clone[0]
            .client_sender
            .send(NetworkAdversarialMessage::AdvInsertInvalidTransactions(true));
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTxValidityCheck,
        ));
    });
    test_loop.run_until(
        |test_loop_data| {
            for node in &node_datas[..] {
                let c = &test_loop_data.get(&node.client_sender.actor_handle()).client;
                let h = c.chain.head().unwrap().height;
                if h <= 10050 {
                    return false;
                }
            }
            true
        },
        Duration::seconds(30),
    );

    // I'd have loved to check this holds true for chunk validators but they do not track shards
    // and thus cannot provide the info about balances.
    let clients = vec![&test_loop.data.get(&chunk_producer.client_sender.actor_handle()).client];
    for account in &accounts {
        let actual = clients.query_balance(account);
        assert_eq!(actual, 1000000 * ONE_NEAR, "no transfers should have happened");
    }

    let Some(applied_tx_metric) = near_o11y::metrics::prometheus::gather()
        .into_iter()
        .find(|m| m.get_name() == "near_transaction_applied_total")
    else {
        panic!("no applied transactions metric found");
    };
    let [metric] = applied_tx_metric.get_metric() else { panic!("unexpected metric shape") };
    let applied_txs = metric.get_counter().get_value();
    assert_eq!(applied_txs, 70.0, "should have applied the submitted transactions");

    test_loop_env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
