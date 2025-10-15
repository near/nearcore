use assert_matches::assert_matches;
use near_async::futures::FutureSpawnerExt;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_client::client_actor::AdvProduceChunksMode;
use near_client::{NetworkAdversarialMessage, ProcessTxRequest};
use near_indexer::{AwaitForNodeSyncedEnum, IndexerConfig, StreamerMessage, SyncModeEnum, start};
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Finality, NumBlocks};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::ExecutionStatusView;
use near_store::StoreConfig;
use tokio::sync::mpsc;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::{
    create_account_id, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;

#[test]
fn test_indexer_basic() {
    init_test_logger();

    let mut env = setup();
    let start_block_height = 1;
    let last_block_height = 5;
    let mut indexer_receiver = start_indexer(&env, SyncModeEnum::BlockHeight(start_block_height));
    for expected_height in start_block_height..=last_block_height {
        let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
        assert_eq!(msg.block.header.height, expected_height);
    }

    shutdown(env);
}

#[test]
fn test_indexer_local_receipt() {
    init_test_logger();

    let mut env = setup();
    let tx = create_local_tx(&env);
    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    let submit_tx_height = rpc_node.head(env.test_loop_data()).height;
    let outcome = rpc_node.execute_tx(&mut env.test_loop, tx, Duration::seconds(5)).unwrap();
    let tx_outcome_status = outcome.transaction_outcome.outcome.status;
    let ExecutionStatusView::SuccessReceiptId(receipt_id) = tx_outcome_status else {
        panic!("failed to convert transaction to receipt {tx_outcome_status:?}");
    };
    assert_eq!(outcome.receipts_outcome.len(), 1);
    let receipt_outcome = &outcome.receipts_outcome[0];

    let tx_included_height = submit_tx_height + 3;
    let mut indexer_receiver = start_indexer(&env, SyncModeEnum::BlockHeight(tx_included_height));
    let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
    let indexer_shard = &msg.shards[0];
    let chunk_view = indexer_shard.chunk.as_ref().unwrap();
    assert_eq!(chunk_view.transactions.len(), 1);
    assert_eq!(indexer_shard.receipt_execution_outcomes.len(), 1);
    let receipt_execution_outcome = &indexer_shard.receipt_execution_outcomes[0];
    assert_eq!(receipt_execution_outcome.receipt.receipt_id, receipt_id);
    assert_eq!(&receipt_execution_outcome.execution_outcome, receipt_outcome);

    shutdown(env);
}

#[test]
fn test_indexer_failed_local_tx() {
    init_test_logger();
    if !ProtocolFeature::InvalidTxGenerateOutcomes.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut env = setup();
    let validator_node = TestLoopNode::validator(&env.node_datas, 0);
    validator_node.send_adversarial_message(
        &env.test_loop,
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::ProduceWithoutTx),
    );
    let tx = create_local_tx(&env);
    let process_tx_request =
        ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
    validator_node.data().rpc_handler_sender.send(process_tx_request);
    // Wait for the transaction to expire. This will happen because the chunk producer
    // does not include any transactions in the chunks (enabled by the adversarial message above).
    validator_node.run_for_number_of_blocks(&mut env.test_loop, (TX_VALIDITY_PERIOD + 1) as usize);
    // Make chunk producer include the transaction without checking the validity period.
    // This effectively results in invalid transaction included in the chunk.
    validator_node.send_adversarial_message(
        &env.test_loop,
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTxValidityCheck,
        ),
    );

    let tx_included_height = validator_node.head(env.test_loop_data()).height + 2;
    let mut indexer_receiver = start_indexer(&env, SyncModeEnum::BlockHeight(tx_included_height));
    let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
    let indexer_shard = &msg.shards[0];
    let chunk_view = indexer_shard.chunk.as_ref().unwrap();
    assert_eq!(chunk_view.transactions.len(), 1);
    let outcome = &chunk_view.transactions[0].outcome.execution_outcome.outcome;
    assert_matches!(outcome.status, ExecutionStatusView::Failure(_));
    assert!(outcome.receipt_ids.is_empty());
    assert!(indexer_shard.receipt_execution_outcomes.is_empty());

    shutdown(env);
}

fn user_account() -> AccountId {
    create_account_id("user")
}

const TX_VALIDITY_PERIOD: NumBlocks = 5;

fn setup() -> TestLoopEnv {
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_account_simple(user_account(), Balance::from_near(1))
        .transaction_validity_period(TX_VALIDITY_PERIOD)
        .build();
    let env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();
    env
}

fn start_indexer(env: &TestLoopEnv, sync_mode: SyncModeEnum) -> mpsc::Receiver<StreamerMessage> {
    let node_data = &env.node_datas[0];
    let node = TestLoopNode::for_account(&env.node_datas, &node_data.account_id);
    let indexer_config = IndexerConfig {
        home_dir: NodeExecutionData::homedir(&env.shared_state.tempdir, &node_data.identifier),
        sync_mode,
        await_for_node_synced: AwaitForNodeSyncedEnum::StreamWhileSyncing,
        finality: Finality::None,
        validate_genesis: false,
    };

    let shard_tracker = node.client(env.test_loop_data()).shard_tracker.clone();
    let store_config =
        StoreConfig { path: Some(indexer_config.home_dir.clone()), ..Default::default() };
    let (sender, receiver) = tokio::sync::mpsc::channel(100);
    let future = start(
        node_data.view_client_sender.clone().into(),
        node_data.client_sender.clone().into(),
        shard_tracker,
        indexer_config,
        store_config,
        sender,
        env.test_loop.clock(),
    );
    env.test_loop.future_spawner("Indexer").spawn("main indexer loop", future);
    receiver
}

fn receive_indexer_message(
    env: &mut TestLoopEnv,
    indexer_receiver: &mut mpsc::Receiver<StreamerMessage>,
) -> StreamerMessage {
    let mut ret: Option<StreamerMessage> = None;
    env.test_loop.run_until(
        |_| {
            if let Ok(msg) = indexer_receiver.try_recv() {
                ret = Some(msg);
                true
            } else {
                false
            }
        },
        Duration::seconds(20),
    );
    ret.unwrap()
}

fn shutdown(env: TestLoopEnv) {
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn create_local_tx(env: &TestLoopEnv) -> SignedTransaction {
    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    let block_hash = rpc_node.head(env.test_loop_data()).last_block_hash;
    let nonce = 1;
    let tx = SignedTransaction::call(
        nonce,
        user_account(),
        user_account(),
        &create_user_test_signer(&user_account()),
        Balance::ZERO,
        "does_not_matter".to_owned(),
        vec![],
        Gas::from_teragas(100),
        block_hash,
    );
    tx
}
