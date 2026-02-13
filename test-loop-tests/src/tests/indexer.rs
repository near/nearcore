use std::iter::repeat_with;
use std::sync::atomic::AtomicU64;

use assert_matches::assert_matches;
use itertools::Itertools;
use near_async::futures::FutureSpawnerExt;
use near_async::time::Duration;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_crypto::Signer;
use near_indexer::{
    AwaitForNodeSyncedEnum, IndexerConfig, IndexerExecutionOutcomeWithReceipt, StreamerMessage,
    SyncModeEnum, start,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::types::{AccountId, Balance, Finality, Nonce, NumBlocks};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{ActionView, ExecutionStatusView, ReceiptEnumView, ReceiptView};
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
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
    let indexer_chunk = indexer_shard.chunk.as_ref().unwrap();
    assert_eq!(indexer_chunk.transactions.len(), 1);
    assert_eq!(indexer_chunk.local_receipts.len(), 1);
    assert!(indexer_chunk.receipts.is_empty());
    assert_eq!(indexer_shard.receipt_execution_outcomes.len(), 1);
    let receipt_execution_outcome = &indexer_shard.receipt_execution_outcomes[0];
    assert_eq!(receipt_execution_outcome.receipt.receipt_id, receipt_id);
    assert_eq!(&receipt_execution_outcome.execution_outcome, receipt_outcome);

    shutdown(env);
}

/// Test that instant receipts (PromiseYield) are correctly indexed.
///
/// The PromiseYield instant receipt is stored/postponed in DBCol::Receipts when
/// it is first processed (persisted as a PromiseYield receipt awaiting data via
/// `promise_yield_create`). When the yield
/// is later resumed, the callback executes and produces an execution outcome.
/// The indexer pairs the outcome with the receipt fetched from DBCol::Receipts.
///
/// Without storing instant receipts, the indexer would fail to find the receipt
/// for the callback's execution outcome (it's neither an incoming receipt nor
/// a local receipt generated from a transaction).
///
/// This test needs two transactions (unlike the GC test which only needs one):
/// the GC test only checks that the receipt is *stored*, while this test needs
/// the callback to *execute* so the indexer has an outcome to index.
#[test]
#[cfg_attr(not(feature = "nightly"), ignore)]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_indexer_instant_receipt() {
    init_test_logger();

    let mut env = setup();
    deploy_test_contract(&mut env);
    let rpc_node = TestLoopNode::rpc(&env.node_datas);

    // Step 1: Call yield_create — produces a PromiseYield instant receipt that
    // gets stored/postponed as a PromiseYield receipt (awaiting data) and persisted in DBCol::Receipts.
    let yield_create_tx = SignedTransaction::call(
        next_nonce(),
        user_account(),
        user_account(),
        &user_signer(),
        Balance::ZERO,
        "call_yield_create_return_promise".to_owned(),
        vec![42u8; 16],
        Gas::from_teragas(300),
        tx_block_hash(&env),
    );
    rpc_node.submit_tx(yield_create_tx.clone());
    let tx_outcome = rpc_node.run_until_outcome_available(
        &mut env.test_loop,
        yield_create_tx.get_hash(),
        Duration::seconds(5),
    );
    let ExecutionStatus::SuccessReceiptId(local_receipt_id) =
        tx_outcome.outcome_with_id.outcome.status
    else {
        panic!("failed to convert transaction to receipt");
    };
    // Wait for local receipt to execute (this also processes the instant receipt).
    let local_outcome = rpc_node.run_until_outcome_available(
        &mut env.test_loop,
        local_receipt_id,
        Duration::seconds(5),
    );
    let [yield_receipt_id] = local_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single child receipt (the PromiseYield instant receipt)")
    };

    // Step 2: Call yield_resume — provides data for the PromiseYield, causing
    // the callback to execute in a subsequent block.
    let resume_tx = SignedTransaction::call(
        next_nonce(),
        user_account(),
        user_account(),
        &user_signer(),
        Balance::ZERO,
        "call_yield_resume_read_data_id_from_storage".to_owned(),
        vec![42u8; 16],
        Gas::from_teragas(300),
        tx_block_hash(&env),
    );
    rpc_node.run_tx(&mut env.test_loop, resume_tx, Duration::seconds(5));

    // Wait for the PromiseYield callback execution outcome.
    let yield_outcome = rpc_node.run_until_outcome_available(
        &mut env.test_loop,
        yield_receipt_id,
        Duration::seconds(5),
    );

    // Step 3: Start the indexer at the block where the callback executed.
    let callback_block_hash = yield_outcome.block_hash;
    let callback_height = rpc_node
        .client(env.test_loop_data())
        .chain
        .get_block(&callback_block_hash)
        .unwrap()
        .header()
        .height();
    let mut indexer_receiver = start_indexer(&env, SyncModeEnum::BlockHeight(callback_height));
    let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
    assert_eq!(msg.block.header.height, callback_height);
    let indexer_shard = &msg.shards[0];

    // Verify the PromiseYield callback is in receipt_execution_outcomes
    // with the correct receipt data (is_promise_yield: true).
    let indexed_outcome = indexer_shard
        .receipt_execution_outcomes
        .iter()
        .find(|o| o.execution_outcome.id == yield_receipt_id)
        .expect("PromiseYield callback should be present in receipt_execution_outcomes");

    let ReceiptEnumView::Action { is_promise_yield, .. } = &indexed_outcome.receipt.receipt else {
        panic!("expected Action receipt variant for PromiseYield callback");
    };
    assert!(is_promise_yield, "receipt should have is_promise_yield=true");

    shutdown(env);
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_indexer_delayed_local_receipt() {
    init_test_logger();

    let mut env = setup();
    deploy_test_contract(&mut env);

    // Each transaction generates local receipt consuming more than a half
    // the chunk space, so chunk can only fit 2 such receipts.
    let gas_to_burn = GAS_LIMIT.checked_div(2).unwrap().checked_add(Gas::from_gas(1)).unwrap();
    // Use 5 transactions so execution of the last receipt is delayed by 2 blocks.
    // This way we ensure that the receipt can be found beyond previous block.
    let txs = repeat_with(|| create_burn_gas_tx(&env, gas_to_burn)).take(5).collect_vec();
    let validator_node = TestLoopNode::from(&env.node_datas[0]);
    for tx in &txs {
        validator_node.submit_tx(tx.clone());
    }
    let last_tx = txs.last().unwrap();
    let last_tx_outcome = validator_node.run_until_outcome_available(
        &mut env.test_loop,
        last_tx.get_hash(),
        Duration::seconds(2),
    );
    let last_tx_included_height = validator_node.head(env.test_loop_data()).height;
    let ExecutionStatus::SuccessReceiptId(last_tx_receipt_id) =
        last_tx_outcome.outcome_with_id.outcome.status
    else {
        panic!("failed to convert tx to receipt");
    };
    let last_tx_receipt_outcome = validator_node.run_until_outcome_available(
        &mut env.test_loop,
        last_tx_receipt_id,
        Duration::seconds(2),
    );
    let last_tx_receipt_executed_height = validator_node.head(env.test_loop_data()).height;
    assert_eq!(last_tx_receipt_executed_height, last_tx_included_height + 2);

    let mut indexer_receiver =
        start_indexer(&env, SyncModeEnum::BlockHeight(last_tx_receipt_executed_height));
    let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
    let shard_outcomes = &msg.shards[0].receipt_execution_outcomes;
    assert_eq!(shard_outcomes.len(), 1);
    let delayed_receipt_outcome = &shard_outcomes[0];
    assert_eq!(delayed_receipt_outcome.execution_outcome, last_tx_receipt_outcome.into());
    assert_eq!(delayed_receipt_outcome.receipt.receipt_id, last_tx_receipt_id);

    shutdown(env);
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_indexer_failed_local_tx() {
    init_test_logger();
    if !ProtocolFeature::InvalidTxGenerateOutcomes.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut env = setup();
    let validator_node = TestLoopNode::from(&env.node_datas[0]);
    validator_node.send_adversarial_message(
        &env.test_loop,
        NetworkAdversarialMessage::AdvProduceChunks(AdvProduceChunksMode::ProduceWithoutTx),
    );
    validator_node.submit_tx(create_local_tx(&env));
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
    let indexer_chunk = indexer_shard.chunk.as_ref().unwrap();
    assert_eq!(indexer_chunk.transactions.len(), 1);
    assert!(indexer_chunk.local_receipts.is_empty());
    assert!(indexer_chunk.receipts.is_empty());
    let outcome = &indexer_chunk.transactions[0].outcome.execution_outcome.outcome;
    assert_matches!(outcome.status, ExecutionStatusView::Failure(_));
    assert!(outcome.receipt_ids.is_empty());
    assert!(indexer_shard.receipt_execution_outcomes.is_empty());

    shutdown(env);
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_indexer_deploy_contract_local_tx() {
    init_test_logger();
    let mut env = setup();
    deploy_test_contract(&mut env);
    let validator_node = TestLoopNode::from(&env.node_datas[0]);
    let deploy_contract_height = validator_node.head(env.test_loop_data()).height;

    let mut indexer_receiver =
        start_indexer(&env, SyncModeEnum::BlockHeight(deploy_contract_height));
    let msg = receive_indexer_message(&mut env, &mut indexer_receiver);
    let indexer_shard = &msg.shards[0];
    assert_eq!(indexer_shard.chunk.as_ref().unwrap().transactions.len(), 1);
    let [
        IndexerExecutionOutcomeWithReceipt {
            receipt: ReceiptView { receipt: ReceiptEnumView::Action { actions, .. }, .. },
            ..
        },
    ] = indexer_shard.receipt_execution_outcomes.as_slice()
    else {
        panic!("expected single action receipt")
    };
    let [ActionView::DeployContract { code }] = actions.as_slice() else {
        panic!("expected single deploy contract action")
    };
    assert_eq!(code, CryptoHash::hash_bytes(near_test_contracts::rs_contract()).as_bytes());

    shutdown(env);
}

fn user_account() -> AccountId {
    create_account_id("user")
}

const TX_VALIDITY_PERIOD: NumBlocks = 5;
const GAS_LIMIT: Gas = Gas::from_teragas(300);

fn setup() -> TestLoopEnv {
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .gas_limit(GAS_LIMIT)
        .add_user_account_simple(user_account(), Balance::from_near(1000))
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

fn next_nonce() -> Nonce {
    static NEXT_VALUE: AtomicU64 = AtomicU64::new(1);
    NEXT_VALUE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn tx_block_hash(env: &TestLoopEnv) -> CryptoHash {
    TestLoopNode::rpc(&env.node_datas).head(env.test_loop_data()).last_block_hash
}

fn user_signer() -> Signer {
    create_user_test_signer(&user_account())
}

fn create_local_tx(env: &TestLoopEnv) -> SignedTransaction {
    SignedTransaction::call(
        next_nonce(),
        user_account(),
        user_account(),
        &user_signer(),
        Balance::ZERO,
        "does_not_matter".to_owned(),
        vec![],
        Gas::from_teragas(100),
        tx_block_hash(env),
    )
}

fn create_burn_gas_tx(env: &TestLoopEnv, gas_to_burn: Gas) -> SignedTransaction {
    SignedTransaction::call(
        next_nonce(),
        user_account(),
        user_account(),
        &user_signer(),
        Balance::ZERO,
        "burn_gas_raw".to_owned(),
        gas_to_burn.as_gas().to_le_bytes().to_vec(),
        GAS_LIMIT,
        tx_block_hash(env),
    )
}

fn deploy_test_contract(env: &mut TestLoopEnv) {
    let tx = SignedTransaction::deploy_contract(
        next_nonce(),
        &user_account(),
        near_test_contracts::rs_contract().to_vec(),
        &user_signer(),
        tx_block_hash(env),
    );
    TestLoopNode::rpc(&env.node_datas).run_tx(&mut env.test_loop, tx, Duration::seconds(3));
}
