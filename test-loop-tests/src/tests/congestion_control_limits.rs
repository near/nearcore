//! Transaction-inclusion limits and RPC rejection under congestion control: how
//! many transactions get included per chunk as a shard becomes congested, and that
//! the RPC layer rejects transactions to a fully-congested shard. Runs under spice
//! and non-spice; congestion is read from the executed `ChunkExtra` at the
//! execution head.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{TransactionRunner, execute_tx};
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::config::CongestionControlConfig;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::account::id::AccountId;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::errors::{
    ActionErrorKind, FunctionCallError, InvalidTxError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, Gas, ShardId};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;
use std::sync::Arc;

const ACCOUNT_PARENT_ID: &str = "near";
const CONTRACT_ID: &str = "contract.near";

// Make 1 wasm op cost ~4 GGas, to let "loop_forever" finish more quickly.
fn set_wasm_cost(config: &mut RuntimeConfig) {
    let wasm_config = Arc::make_mut(&mut config.wasm_config);
    wasm_config.regular_op_cost = u32::MAX;
}

// Pin the congestion control parameters so the test doesn't need fixing every
// time the live parameters change.
fn set_default_congestion_control(config_store: &RuntimeConfigStore, config: &mut RuntimeConfig) {
    // TODO(limited_replayability): Start using congestion control config from latest protocol version.
    #[allow(deprecated)]
    let cc_protocol_version = ProtocolFeature::_DeprecatedCongestionControl.protocol_version();
    let cc_config = config_store.get_config(cc_protocol_version);
    config.congestion_control_config = cc_config.congestion_control_config;
}

/// Single node tracking all 4 shards, with inflated per-op wasm cost (so
/// gas-burning calls finish fast) and pinned congestion-control parameters.
fn setup_congestion_env() -> TestLoopEnv {
    let parent: AccountId = ACCOUNT_PARENT_ID.parse().unwrap();
    let test0: AccountId = "test0".parse().unwrap();

    let validators_spec = ValidatorsSpec::desired_roles(&[ACCOUNT_PARENT_ID], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .transaction_validity_period(20)
        .shard_layout(ShardLayout::multi_shard(4, 3))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&[parent.clone(), test0], Balance::from_near(1_000_000_000))
        .build();
    let epoch_config_store =
        TestEpochConfigBuilder::from_genesis(&genesis).build_store_for_genesis_protocol_version();

    let config_store = RuntimeConfigStore::new(None);
    let mut config = RuntimeConfig::test_protocol_version(PROTOCOL_VERSION);
    set_wasm_cost(&mut config);
    set_default_congestion_control(&config_store, &mut config);
    let runtime_config_store = RuntimeConfigStore::with_one_config(config);

    TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .runtime_config_store(runtime_config_store)
        .clients(vec![parent])
        .track_all_shards()
        .build()
}

// ---- low-level helpers operating on the single node (index 0) ----

/// Submit a transaction to the node's pool synchronously and return the
/// immediate processing response (does not advance the chain).
fn process_tx(env: &TestLoopEnv, tx: SignedTransaction) -> ProcessTxResponse {
    let handle = env.node_datas[0].rpc_handler_sender.actor_handle();
    env.test_loop.data.get(&handle).process_tx(tx, false, false)
}

/// Submit `tx` and run the chain until it reaches a final state, returning the
/// outcome (panics on rejection before execution).
fn execute_setup_tx(env: &mut TestLoopEnv, tx: SignedTransaction) -> FinalExecutionStatus {
    let node_account: AccountId = ACCOUNT_PARENT_ID.parse().unwrap();
    execute_tx(
        &mut env.test_loop,
        &node_account,
        TransactionRunner::new(tx, false),
        &env.node_datas,
        Duration::seconds(40),
    )
    .unwrap()
    .status
}

fn head_chunk(node: &TestLoopNode, shard_id: ShardId) -> ShardChunk {
    let block = node.head_block();
    let shard_layout =
        node.client().epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    node.block_chunks(&block)[shard_index].clone()
}

fn head_chunk_num_tx(node: &TestLoopNode, shard_id: ShardId) -> usize {
    head_chunk(node, shard_id).to_transactions().len()
}

/// Congestion info of the shard's last executed chunk, from its `ChunkExtra` at
/// `last_executed()` (the execution head, which trails consensus under spice).
fn head_congestion_info(node: &TestLoopNode, shard_id: ShardId) -> CongestionInfo {
    let executed_hash = node.last_executed().last_block_hash;
    let block = node.block(executed_hash);
    let shard_layout =
        node.client().epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    node.client()
        .chain
        .chain_store()
        .get_chunk_extra(&executed_hash, &shard_uid)
        .unwrap()
        .congestion_info()
}

fn head_congestion_control_config(node: &TestLoopNode) -> CongestionControlConfig {
    let block = node.head_block();
    let epoch_id = block.header().epoch_id();
    let protocol_version =
        node.client().epoch_manager.get_epoch_protocol_version(epoch_id).unwrap();
    node.client().runtime_adapter.get_runtime_config(protocol_version).congestion_control_config
}

// ---- transaction builders ----

/// A function call with 100 Tgas attached that burns it all (`loop_forever`).
fn new_fn_call_100tgas(
    nonce_source: &mut u64,
    signer: &Signer,
    block_hash: CryptoHash,
) -> SignedTransaction {
    let nonce = *nonce_source;
    *nonce_source += 1;
    SignedTransaction::call(
        nonce,
        signer.get_account_id(),
        CONTRACT_ID.parse().unwrap(),
        signer,
        Balance::ZERO,
        "loop_forever".to_owned(),
        vec![],
        Gas::from_teragas(100),
        block_hash,
    )
}

/// A cheap (1 Tgas) function call that is valid but allowed to fail on execution.
fn new_cheap_fn_call(
    nonce_source: &mut u64,
    signer: &Signer,
    receiver: AccountId,
    block_hash: CryptoHash,
) -> SignedTransaction {
    let nonce = *nonce_source;
    *nonce_source += 1;
    SignedTransaction::call(
        nonce,
        signer.get_account_id(),
        receiver,
        signer,
        Balance::ZERO,
        "foo_does_not_exists".to_owned(),
        vec![],
        Gas::from_teragas(1),
        block_hash,
    )
}

/// Submit N 100-Tgas function calls to the pool. Returns the number accepted
/// (the rest were rejected for shard congestion).
fn submit_n_100tgas_fns(env: &TestLoopEnv, n: u32, nonce: &mut u64, signer: &Signer) -> u32 {
    let block_hash = env.validator().head().last_block_hash;
    let mut included = 0;
    for _ in 0..n {
        let tx = new_fn_call_100tgas(nonce, signer, block_hash);
        match process_tx(env, tx) {
            ProcessTxResponse::ValidTx => included += 1,
            ProcessTxResponse::InvalidTx(InvalidTxError::ShardCongested { .. }) => {}
            other => panic!("unexpected result from submitting tx: {other:?}"),
        }
    }
    included
}

/// Submit N cheap function calls to the pool (all expected to be accepted).
fn submit_n_cheap_fns(
    env: &TestLoopEnv,
    n: u32,
    nonce: &mut u64,
    signer: &Signer,
    receiver: &AccountId,
) {
    let block_hash = env.validator().head().last_block_hash;
    for _ in 0..n {
        let tx = new_cheap_fn_call(nonce, signer, receiver.clone(), block_hash);
        assert_eq!(process_tx(env, tx), ProcessTxResponse::ValidTx);
    }
}

// ---- account / contract setup ----

fn setup_account(env: &mut TestLoopEnv, account_id: &AccountId, account_parent_id: &AccountId) {
    let signer = InMemorySigner::test_signer(account_parent_id);
    let block_hash = env.validator().head().last_block_hash;
    let nonce = env.validator().get_next_nonce(account_parent_id);
    let public_key = PublicKey::from_seed(KeyType::ED25519, account_id.as_str());
    let tx = SignedTransaction::create_account(
        nonce,
        account_parent_id.clone(),
        account_id.clone(),
        Balance::from_near(100),
        public_key,
        &signer,
        block_hash,
    );
    assert_matches!(execute_setup_tx(env, tx), FinalExecutionStatus::SuccessValue(_));
}

/// Deploy the congestion-control test contract (provides `loop_forever`),
/// advance the chain to complete it, and verify a call burns all its gas.
fn setup_contract(env: &mut TestLoopEnv) {
    let parent: AccountId = ACCOUNT_PARENT_ID.parse().unwrap();
    let signer = InMemorySigner::test_signer(&parent);
    let contract = near_test_contracts::congestion_control_test_contract();

    let block_hash = env.validator().head().last_block_hash;
    let nonce = env.validator().get_next_nonce(&parent);
    let create_contract_tx = SignedTransaction::create_contract(
        nonce,
        parent.clone(),
        CONTRACT_ID.parse().unwrap(),
        contract.to_vec(),
        Balance::from_near(100),
        PublicKey::from_seed(KeyType::ED25519, CONTRACT_ID),
        &signer,
        block_hash,
    );
    assert_matches!(
        execute_setup_tx(env, create_contract_tx),
        FinalExecutionStatus::SuccessValue(_)
    );

    // The function call should end in a gas-exceeded error.
    let block_hash = env.validator().head().last_block_hash;
    let mut nonce = env.validator().get_next_nonce(&parent);
    let fn_tx = new_fn_call_100tgas(&mut nonce, &signer, block_hash);
    let FinalExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) =
        execute_setup_tx(env, fn_tx)
    else {
        panic!("test setup error: should result in action error")
    };
    assert_eq!(
        action_error.kind,
        ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
            "Exceeded the prepaid gas.".to_owned()
        )),
        "test setup error: should result in gas exceeded error"
    );
}

enum UpperLimitCongestion {
    BelowRejectThreshold,
    AboveRejectThreshold,
}

/// Calls [`measure_tx_limit`] with accounts on three different shards.
fn measure_remote_tx_limit(
    upper_limit_congestion: UpperLimitCongestion,
) -> (usize, usize, usize, usize) {
    let contract_id: AccountId = CONTRACT_ID.parse().unwrap();
    let remote_id: AccountId = "test1.near".parse().unwrap();
    let dummy_id: AccountId = "test2.near".parse().unwrap();
    let env = setup_congestion_env();

    let (contract_shard_id, remote_shard_id, dummy_shard_id) = {
        let node = env.validator();
        let tip = node.head();
        let shard_layout = node.client().epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
        (
            shard_layout.account_id_to_shard_id(&contract_id),
            shard_layout.account_id_to_shard_id(&remote_id),
            shard_layout.account_id_to_shard_id(&dummy_id),
        )
    };

    // For a clean test setup, ensure we use 3 different shards.
    assert_ne!(remote_shard_id, contract_shard_id);
    assert_ne!(dummy_shard_id, contract_shard_id);
    assert_ne!(dummy_shard_id, remote_shard_id);

    measure_tx_limit(env, remote_id, contract_id, dummy_id, upper_limit_congestion)
}

/// Create the target incoming congestion level and measure the difference of
/// included transactions with and without the congestion, on the local shard
/// and on remote shards sending to the congested shard.
fn measure_tx_limit(
    mut env: TestLoopEnv,
    remote_id: AccountId,
    contract_id: AccountId,
    dummy_receiver: AccountId,
    upper_limit_congestion: UpperLimitCongestion,
) -> (usize, usize, usize, usize) {
    setup_contract(&mut env);
    if remote_id != contract_id {
        setup_account(&mut env, &remote_id, &ACCOUNT_PARENT_ID.parse().unwrap());
    }

    let remote_signer: Signer = InMemorySigner::test_signer(&remote_id);
    let local_signer: Signer = InMemorySigner::test_signer(&contract_id);
    let (remote_shard_id, contract_shard_id, config) = {
        let node = env.validator();
        let tip = node.head();
        let shard_layout = node.client().epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
        (
            shard_layout.account_id_to_shard_id(&remote_id),
            shard_layout.account_id_to_shard_id(&contract_id),
            head_congestion_control_config(&node),
        )
    };

    // Put in enough transactions to create up to `reject_tx_congestion_threshold`
    // incoming congestion.
    let upper_limit_congestion = match upper_limit_congestion {
        UpperLimitCongestion::BelowRejectThreshold => config.reject_tx_congestion_threshold,
        UpperLimitCongestion::AboveRejectThreshold => config.reject_tx_congestion_threshold * 2.0,
    };

    let num_full_congestion = config.max_congestion_incoming_gas.as_teragas() / 100;
    let n = num_full_congestion as f64 * upper_limit_congestion;
    // Account keys created at height H start their nonce at H * 1_000_000.
    let mut nonce = env.validator().head().height * 1_000_000 + 1;
    submit_n_100tgas_fns(&env, n as u32, &mut nonce, &remote_signer);
    submit_n_cheap_fns(&env, 1000, &mut nonce, &local_signer, &dummy_receiver);
    env.validator_runner().run_for_number_of_blocks(1);

    // Produce blocks until all transactions are included.
    let timeout = 1000;
    let mut all_included = false;
    let mut remote_tx_included_without_congestion = 0;
    let mut local_tx_included_without_congestion = 0;
    for i in 2..timeout {
        env.validator_runner().run_for_number_of_blocks(1);

        let (remote_num_tx, local_num_tx) = {
            let node = env.validator();
            (head_chunk_num_tx(&node, remote_shard_id), head_chunk_num_tx(&node, contract_shard_id))
        };

        if i == 2 {
            remote_tx_included_without_congestion = remote_num_tx;
            local_tx_included_without_congestion = local_num_tx;
        }
        if remote_num_tx == 0 && local_num_tx == 0 {
            all_included = true;
            break;
        }
    }
    assert!(all_included, "loop timed out before all transactions were included");

    // Now we expect the contract's shard to have non-trivial incoming congestion.
    let (incoming_congestion, congestion_level, congestion_info) = {
        let node = env.validator();
        let congestion_info = head_congestion_info(&node, contract_shard_id);
        (
            congestion_info.incoming_congestion(&config),
            congestion_info.localized_congestion_level(&config),
            congestion_info,
        )
    };
    assert!(
        incoming_congestion > upper_limit_congestion / 2.0,
        "{incoming_congestion} > {upper_limit_congestion} / 2 failed, {congestion_info:?}"
    );
    assert!(
        congestion_level < upper_limit_congestion,
        "{congestion_level} < {upper_limit_congestion} failed, {congestion_info:?}"
    );

    // Send more transactions to see how many will be accepted now with congestion.
    submit_n_100tgas_fns(&env, n as u32, &mut nonce, &remote_signer);
    submit_n_cheap_fns(&env, 1000, &mut nonce, &local_signer, &dummy_receiver);
    env.validator_runner().run_for_number_of_blocks(1);
    env.validator_runner().run_for_number_of_blocks(1);
    let node = env.validator();
    let remote_tx_included_with_congestion = head_chunk_num_tx(&node, remote_shard_id);
    let local_tx_included_with_congestion = head_chunk_num_tx(&node, contract_shard_id);

    (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    )
}

/// Test that less gas is attributed to transactions when the local shard has
/// delayed receipts (local traffic only, contract signs its own calls).
#[test]
fn test_transaction_limit_for_local_congestion() {
    init_test_logger();

    let upper_limit_congestion = UpperLimitCongestion::BelowRejectThreshold;
    let contract_id: AccountId = CONTRACT_ID.parse().unwrap();
    let sender_id = contract_id.clone();
    let dummy_receiver: AccountId = "a_dummy_receiver".parse().unwrap();
    let env = setup_congestion_env();

    let (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    ) = measure_tx_limit(env, sender_id, contract_id, dummy_receiver, upper_limit_congestion);

    assert_ne!(local_tx_included_without_congestion, 0);
    assert_ne!(remote_tx_included_with_congestion, 0);
    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    assert!(
        remote_tx_included_with_congestion < remote_tx_included_without_congestion,
        "{remote_tx_included_with_congestion} < {remote_tx_included_without_congestion} failed"
    );
}

/// Same as above but with remote traffic, staying below the reject threshold.
#[test]
fn test_transaction_limit_for_remote_congestion() {
    init_test_logger();
    let upper_limit_congestion = UpperLimitCongestion::BelowRejectThreshold;

    let (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    ) = measure_remote_tx_limit(upper_limit_congestion);

    assert_ne!(remote_tx_included_without_congestion, 0);
    assert_ne!(remote_tx_included_with_congestion, 0);
    assert_ne!(local_tx_included_without_congestion, 0);
    assert_ne!(local_tx_included_with_congestion, 0);

    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    // remote transactions should be unaffected below the reject threshold
    assert!(
        remote_tx_included_with_congestion == remote_tx_included_without_congestion,
        "{remote_tx_included_with_congestion} == {remote_tx_included_without_congestion} failed"
    );
}

/// Test that clients stop including transactions to fully congested receivers.
#[test]
fn slow_test_transaction_filtering() {
    init_test_logger();

    let upper_limit_congestion = UpperLimitCongestion::AboveRejectThreshold;

    let (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    ) = measure_remote_tx_limit(upper_limit_congestion);

    assert_ne!(remote_tx_included_without_congestion, 0);
    assert_ne!(local_tx_included_without_congestion, 0);
    assert_ne!(local_tx_included_with_congestion, 0);

    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    // remote transactions, with full congestion, should be rejected (0 included)
    assert_eq!(remote_tx_included_with_congestion, 0);
}

/// Test that RPC clients stop accepting transactions when the receiver is
/// fully congested.
#[test]
fn test_rpc_client_rejection() {
    init_test_logger();

    let sender_id: AccountId = "test0".parse().unwrap();
    let mut env = setup_congestion_env();

    setup_contract(&mut env);

    let signer: Signer = InMemorySigner::test_signer(&sender_id);

    // Check we can send transactions at the start.
    let mut nonce = env.validator().get_next_nonce(&sender_id);
    let block_hash = env.validator().head().last_block_hash;
    let fn_tx = new_fn_call_100tgas(&mut nonce, &signer, block_hash);
    assert_eq!(process_tx(&env, fn_tx), ProcessTxResponse::ValidTx);

    // Congest the network with a burst of 100 PGas.
    submit_n_100tgas_fns(&env, 1_000, &mut nonce, &signer);

    // Allow transactions to enter the chain and receipts to arrive at the
    // receiver shard for it to become congested.
    for _ in 0..9 {
        env.validator_runner().run_for_number_of_blocks(1);
    }

    // Check that congestion control rejects new transactions.
    let block_hash = env.validator().head().last_block_hash;
    let fn_tx = new_fn_call_100tgas(&mut nonce, &signer, block_hash);
    assert_matches!(
        process_tx(&env, fn_tx),
        ProcessTxResponse::InvalidTx(InvalidTxError::ShardCongested { .. })
    );
}
