use assert_matches::assert_matches;
use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::account::id::AccountId;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::errors::{
    ActionErrorKind, FunctionCallError, InvalidTxError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::ShardId;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;
use near_vm_runner::logic::ProtocolVersion;
use std::sync::Arc;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use crate::env::test_env_builder::TestEnvBuilder;

const ACCOUNT_PARENT_ID: &str = "near";
const CONTRACT_ID: &str = "contract.near";

fn get_runtime_config(
    config_store: &RuntimeConfigStore,
    protocol_version: ProtocolVersion,
) -> Arc<near_parameters::RuntimeConfig> {
    let mut config = config_store.get_config(protocol_version).clone();
    let mut_config = Arc::make_mut(&mut config);

    set_wasm_cost(mut_config);

    config
}

// Make 1 wasm op cost ~4 GGas, to let "loop_forever" finish more quickly.
fn set_wasm_cost(config: &mut RuntimeConfig) {
    let wasm_config = Arc::make_mut(&mut config.wasm_config);
    wasm_config.regular_op_cost = u32::MAX;
}

// Set the default congestion control parameters for the given runtime config.
// This is important to prevent needing to fix the congestion control tests
// every time the parameters are updated.
fn set_default_congestion_control(config_store: &RuntimeConfigStore, config: &mut RuntimeConfig) {
    // TODO(limited_replayability): Start using congestion control config from latest protocol version.
    #[allow(deprecated)]
    let cc_protocol_version = ProtocolFeature::_DeprecatedCongestionControl.protocol_version();
    let cc_config = get_runtime_config(&config_store, cc_protocol_version);
    config.congestion_control_config = cc_config.congestion_control_config;
}

/// Set up the test runtime with the given protocol version and runtime configs.
/// The test version of runtime has custom gas cost.
fn setup_test_runtime(_sender_id: AccountId, protocol_version: ProtocolVersion) -> TestEnv {
    let accounts = TestEnvBuilder::make_accounts(1);
    let mut genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    genesis.config.epoch_length = 10;
    genesis.config.protocol_version = protocol_version;

    // Chain must be sharded to test cross-shard congestion control.
    genesis.config.shard_layout = ShardLayout::multi_shard(4, 3);

    let config_store = RuntimeConfigStore::new(None);
    let mut config = RuntimeConfig::test_protocol_version(protocol_version);
    set_wasm_cost(&mut config);
    set_default_congestion_control(&config_store, &mut config);

    let runtime_configs = vec![RuntimeConfigStore::with_one_config(config)];
    TestEnv::builder(&genesis.config)
        .nightshade_runtimes_with_runtime_config_store(&genesis, runtime_configs)
        .build()
}

fn setup_account(
    env: &mut TestEnv,
    nonce: &mut u64,
    account_id: &AccountId,
    account_parent_id: &AccountId,
) {
    let block = env.clients[0].chain.get_head_block().unwrap();
    let block_hash = block.hash();

    let signer_id = account_parent_id.clone();
    let signer = InMemorySigner::test_signer(&signer_id);

    let public_key = PublicKey::from_seed(KeyType::ED25519, account_id.as_str());
    let amount = 10 * 10u128.pow(24);

    *nonce += 1;
    let create_account_tx = SignedTransaction::create_account(
        *nonce,
        signer_id,
        account_id.clone(),
        amount,
        public_key,
        &signer,
        *block_hash,
    );

    env.execute_tx(create_account_tx).unwrap().assert_success();
}

/// Set up the RS-Contract, which includes useful functions, such as
/// `loop_forever`.
///
/// This function also advances the chain to complete the deployment and checks
/// it can be called successfully.
fn setup_contract(env: &mut TestEnv, nonce: &mut u64) {
    let block = env.clients[0].chain.get_head_block().unwrap();
    let contract = near_test_contracts::congestion_control_test_contract();

    let signer_id: AccountId = ACCOUNT_PARENT_ID.parse().unwrap();
    let signer = InMemorySigner::test_signer(&signer_id);

    *nonce += 1;
    let create_contract_tx = SignedTransaction::create_contract(
        *nonce,
        signer_id,
        CONTRACT_ID.parse().unwrap(),
        contract.to_vec(),
        10 * 10u128.pow(24),
        PublicKey::from_seed(KeyType::ED25519, CONTRACT_ID),
        &signer,
        *block.hash(),
    );
    // this adds the tx to the pool and then produces blocks until the tx result is available
    env.execute_tx(create_contract_tx).unwrap().assert_success();

    // Test the function call works as expected, ending in a gas exceeded error.
    *nonce += 1;
    let block = env.clients[0].chain.get_head_block().unwrap();
    let fn_tx = new_fn_call_100tgas(nonce, &signer, *block.hash());
    let FinalExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) =
        env.execute_tx(fn_tx).unwrap().status
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

fn head_congestion_control_config(
    env: &TestEnv,
) -> near_parameters::config::CongestionControlConfig {
    let block = env.clients[0].chain.get_head_block().unwrap();
    let runtime_config = env.get_runtime_config(0, *block.header().epoch_id());
    runtime_config.congestion_control_config
}

fn head_congestion_info(env: &TestEnv, shard_id: ShardId) -> CongestionInfo {
    let chunk = head_chunk_header(env, shard_id);
    chunk.congestion_info()
}

fn head_chunk_header(env: &TestEnv, shard_id: ShardId) -> ShardChunkHeader {
    let block = env.clients[0].chain.get_head_block().unwrap();
    let chunks = block.chunks();

    let epoch_id = block.header().epoch_id();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(epoch_id).unwrap();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    chunks.get(shard_index).expect("chunk header must be available").clone()
}

fn head_chunk(env: &TestEnv, shard_id: ShardId) -> ShardChunk {
    let chunk_header = head_chunk_header(&env, shard_id);
    env.clients[0].chain.get_chunk(&chunk_header.chunk_hash()).expect("chunk must be available")
}

/// Create a function call that has 100 Tgas attached and will burn it all.
fn new_fn_call_100tgas(
    nonce_source: &mut u64,
    signer: &Signer,
    block_hash: CryptoHash,
) -> SignedTransaction {
    let hundred_tgas = 100 * 10u64.pow(12);
    let deposit = 0;
    let nonce = *nonce_source;
    *nonce_source += 1;
    SignedTransaction::call(
        nonce,
        signer.get_account_id(),
        CONTRACT_ID.parse().unwrap(),
        &signer,
        deposit,
        // easy way to burn all attached gas
        "loop_forever".to_owned(),
        vec![],
        hundred_tgas,
        block_hash,
    )
}

/// Create a dummy function call that is valid but allowed to fail when
/// executed. It has only 1 Tgas attached.
fn new_cheap_fn_call(
    nonce_source: &mut u64,
    signer: &Signer,
    receiver: AccountId,
    block_hash: CryptoHash,
) -> SignedTransaction {
    let one_tgas = 1 * 10u64.pow(12);
    let deposit = 0;
    let nonce = *nonce_source;
    *nonce_source += 1;
    SignedTransaction::call(
        nonce,
        signer.get_account_id(),
        receiver,
        &signer,
        deposit,
        "foo_does_not_exists".to_owned(),
        vec![],
        one_tgas,
        block_hash,
    )
}

/// Submit N transaction containing a function call action with 100 Tgas
/// attached that will all be burned when called.
fn submit_n_100tgas_fns(env: &TestEnv, n: u32, nonce: &mut u64, signer: &Signer) -> u32 {
    let mut included = 0;
    let block = env.clients[0].chain.get_head_block().unwrap();
    for _ in 0..n {
        let fn_tx = new_fn_call_100tgas(nonce, signer, *block.hash());
        // this only adds the tx to the pool, no chain progress is made
        let response = env.rpc_handlers[0].process_tx(fn_tx, false, false);
        match response {
            ProcessTxResponse::ValidTx => {
                included += 1;
            }
            ProcessTxResponse::InvalidTx(InvalidTxError::ShardCongested { .. }) => (),
            other => panic!("unexpected result from submitting tx: {other:?}"),
        }
    }
    included
}

/// Submit N transaction containing a cheap function call action.
fn submit_n_cheap_fns(
    env: &TestEnv,
    n: u32,
    nonce: &mut u64,
    signer: &Signer,
    receiver: &AccountId,
) {
    let block = env.clients[0].chain.get_head_block().unwrap();
    for _ in 0..n {
        let fn_tx = new_cheap_fn_call(nonce, signer, receiver.clone(), *block.hash());
        // this only adds the tx to the pool, no chain progress is made
        let response = env.rpc_handlers[0].process_tx(fn_tx, false, false);
        assert_eq!(response, ProcessTxResponse::ValidTx);
    }
}

/// Test that less gas is attributed to transactions when the local shard has
/// delayed receipts.
///
/// This test operates on one shard with transactions signed by the contract
/// itself, producing only local receipts. This should be enough to trigger the
/// linear interpolation between `max_tx_gas` and `min_tx_gas` and we want to
/// test that indeed local traffic is enough.
///
/// See [`test_transaction_limit_for_remote_congestion`] for a similar test but
/// with remote traffic.
#[test]
fn test_transaction_limit_for_local_congestion() {
    init_test_logger();

    // Fix the initial configuration of congestion control for the tests.
    // We don't want to go into the TX rejection limit in this test.
    let upper_limit_congestion = UpperLimitCongestion::BelowRejectThreshold;

    // For this test, the contract and the sender are on the same shard, even
    // the same account.
    let contract_id: AccountId = CONTRACT_ID.parse().unwrap();
    let sender_id = contract_id.clone();
    let dummy_receiver: AccountId = "a_dummy_receiver".parse().unwrap();
    let env = setup_test_runtime("test0".parse().unwrap(), PROTOCOL_VERSION);

    let (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    ) = measure_tx_limit(env, sender_id, contract_id, dummy_receiver, upper_limit_congestion);

    assert_ne!(local_tx_included_without_congestion, 0);
    assert_ne!(remote_tx_included_with_congestion, 0);
    // local transactions should be limited
    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    // remote transactions in this case also start on the congested shard, so
    // they should be limited, too
    assert!(
        remote_tx_included_with_congestion < remote_tx_included_without_congestion,
        "{remote_tx_included_with_congestion} < {remote_tx_included_without_congestion} failed"
    );
}

/// Test that clients adjust included transactions based on the congestion level
/// of the local delayed receipts queue only, in this test with remote traffic.
///
/// We expect to see less transactions accepted on the congested shard, to give
/// more capacity towards processing delayed receipts. But in this test we stay
/// below `reject_tx_congestion_threshold`, meaning that the shard sending the
/// remote traffic to the congested shard should not stop, nor should it tighten
/// the limit on how many are accepted.
///
/// [`test_transaction_limit_for_local_congestion`] is a similar test but uses
/// only local receipts. [`test_transaction_filtering`] is even closer to this
/// test but goes beyond `reject_tx_congestion_threshold` to test the tx
/// rejection.
#[test]
fn test_transaction_limit_for_remote_congestion() {
    init_test_logger();
    // We don't want to go into the TX rejection limit in this test.
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

    // local transactions should be limited
    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    // remote transactions should be unaffected
    assert!(
        remote_tx_included_with_congestion == remote_tx_included_without_congestion,
        "{remote_tx_included_with_congestion} == {remote_tx_included_without_congestion} failed"
    );
}

/// Test that clients stop including transactions to fully congested receivers.
#[test]
fn slow_test_transaction_filtering() {
    init_test_logger();

    // This test should go beyond into the TX rejection limit in this test.
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

    // local transactions should be limited
    assert!(
        local_tx_included_with_congestion < local_tx_included_without_congestion,
        "{local_tx_included_with_congestion} < {local_tx_included_without_congestion} failed"
    );
    // remote transactions, with congestion, should be 0
    assert_eq!(remote_tx_included_with_congestion, 0);
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
    let env = setup_test_runtime(remote_id.clone(), PROTOCOL_VERSION);

    let tip = env.clients[0].chain.head().unwrap();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let contract_shard_id = shard_layout.account_id_to_shard_id(&contract_id);
    let remote_shard_id = shard_layout.account_id_to_shard_id(&remote_id);
    let dummy_shard_id = shard_layout.account_id_to_shard_id(&dummy_id);

    // For a clean test setup, ensure we use 3 different shards.
    assert_ne!(remote_shard_id, contract_shard_id);
    assert_ne!(dummy_shard_id, contract_shard_id);
    assert_ne!(dummy_shard_id, remote_shard_id);

    measure_tx_limit(env, remote_id, contract_id, dummy_id, upper_limit_congestion)
}

/// Create the target incoming congestion level and measure the difference of
/// included transactions with and without the congestion, on the local shard
/// and on remote shards sending to the congested shard.
///
/// This helper function operates on three accounts. One account has the
/// contract that will be congested. Another account is used to send remote
/// traffic to that shard. And lastly, we send dummy transactions from the
/// congested account to a third account, to observe the limit applied to it.
///
/// The caller can choose to place the accounts on different shards or on the
/// same shard.
fn measure_tx_limit(
    mut env: TestEnv,
    remote_id: AccountId,
    contract_id: AccountId,
    dummy_receiver: AccountId,
    upper_limit_congestion: UpperLimitCongestion,
) -> (usize, usize, usize, usize) {
    let mut nonce = 1;
    setup_contract(&mut env, &mut nonce);
    if remote_id != contract_id {
        setup_account(&mut env, &mut nonce, &remote_id, &ACCOUNT_PARENT_ID.parse().unwrap());
    }

    let remote_signer = InMemorySigner::test_signer(&remote_id);
    let local_signer = InMemorySigner::test_signer(&contract_id);
    let tip = env.clients[0].chain.head().unwrap();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let remote_shard_id = shard_layout.account_id_to_shard_id(&remote_id);
    let contract_shard_id = shard_layout.account_id_to_shard_id(&contract_id);

    // put in enough transactions to create up to
    // `reject_tx_congestion_threshold` incoming congestion
    let config = head_congestion_control_config(&env);
    let upper_limit_congestion = match upper_limit_congestion {
        UpperLimitCongestion::BelowRejectThreshold => config.reject_tx_congestion_threshold,
        UpperLimitCongestion::AboveRejectThreshold => config.reject_tx_congestion_threshold * 2.0,
    };

    let num_full_congestion = config.max_congestion_incoming_gas / (100 * 10u64.pow(12));
    let n = num_full_congestion as f64 * upper_limit_congestion;
    // Key of new account starts at block_height * 1_000_000
    let tip = env.clients[0].chain.head().unwrap();
    let mut nonce = tip.height * 1_000_000 + 1;
    submit_n_100tgas_fns(&mut env, n as u32, &mut nonce, &remote_signer);
    let tip = env.clients[0].chain.head().unwrap();
    // submit enough cheap transaction to at least fill the tx limit once
    submit_n_cheap_fns(&mut env, 1000, &mut nonce, &local_signer, &dummy_receiver);
    env.produce_block(0, tip.height + 1);

    // Produce blocks until all transactions are included.
    let timeout = 1000;
    let mut all_included = false;
    let mut remote_tx_included_without_congestion = 0;
    let mut local_tx_included_without_congestion = 0;
    for i in 2..timeout {
        let height = tip.height + i;
        env.produce_block(0, height);

        let remote_chunk = head_chunk(&env, remote_shard_id);
        let contract_chunk = head_chunk(&env, contract_shard_id);
        let remote_num_tx = remote_chunk.to_transactions().len();
        let local_num_tx = contract_chunk.to_transactions().len();

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

    // Now we expect the contract's shard to have non-trivial incoming
    // congestion.
    let congestion_info = head_congestion_info(&env, contract_shard_id);
    let incoming_congestion = congestion_info.incoming_congestion(&config);
    let congestion_level = congestion_info.localized_congestion_level(&config);
    // congestion should be non-trivial and below the upper limit
    assert!(
        incoming_congestion > upper_limit_congestion / 2.0,
        "{incoming_congestion} > {upper_limit_congestion} / 2 failed, {congestion_info:?}"
    );
    assert!(
        congestion_level < upper_limit_congestion,
        "{congestion_level} < {upper_limit_congestion} failed, {congestion_info:?}"
    );

    // Send some more transactions to see how many will be accepted now with congestion.
    submit_n_100tgas_fns(&mut env, n as u32, &mut nonce, &remote_signer);
    submit_n_cheap_fns(&mut env, 1000, &mut nonce, &local_signer, &dummy_receiver);
    let tip = env.clients[0].chain.head().unwrap();
    env.produce_block(0, tip.height + 1);
    env.produce_block(0, tip.height + 2);
    let remote_chunk = head_chunk(&env, remote_shard_id);
    let local_chunk = head_chunk(&env, contract_shard_id);
    let remote_tx_included_with_congestion = remote_chunk.to_transactions().len();
    let local_tx_included_with_congestion = local_chunk.to_transactions().len();
    (
        remote_tx_included_without_congestion,
        local_tx_included_without_congestion,
        remote_tx_included_with_congestion,
        local_tx_included_with_congestion,
    )
}

/// Test that RPC clients stop accepting transactions when the receiver is
/// congested.
#[test]
fn test_rpc_client_rejection() {
    let sender_id: AccountId = "test0".parse().unwrap();
    let mut env = setup_test_runtime(sender_id.clone(), PROTOCOL_VERSION);

    // prepare a contract to call
    let mut nonce = 10;
    setup_contract(&mut env, &mut nonce);

    let signer = InMemorySigner::test_signer(&sender_id);

    // Check we can send transactions at the start.
    let fn_tx = new_fn_call_100tgas(
        &mut nonce,
        &signer,
        *env.clients[0].chain.head_header().unwrap().hash(),
    );
    let response = env.rpc_handlers[0].process_tx(fn_tx, false, false);
    assert_eq!(response, ProcessTxResponse::ValidTx);

    // Congest the network with a burst of 100 PGas.
    submit_n_100tgas_fns(&mut env, 1_000, &mut nonce, &signer);

    // Allow transactions to enter the chain and enough receipts to arrive at
    // the receiver shard for it to become congested.
    let tip = env.clients[0].chain.head().unwrap();
    for i in 1..10 {
        env.produce_block(0, tip.height + i);
    }

    // Check that congestion control rejects new transactions.
    let fn_tx = new_fn_call_100tgas(
        &mut nonce,
        &signer,
        *env.clients[0].chain.head_header().unwrap().hash(),
    );
    let response = env.rpc_handlers[0].process_tx(fn_tx, false, false);

    assert_matches!(response, ProcessTxResponse::InvalidTx(InvalidTxError::ShardCongested { .. }));
}
