use std::sync::Arc;

use near_async::time::Duration;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_parameters::config::TEST_CONFIG_YIELD_TIMEOUT_LENGTH;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptEnum, VersionedReceiptEnum};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::{TrieKey, col, trie_key_parsers};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;
use near_store::adapter::StoreAdapter;
use near_store::{ShardUId, Trie, TrieDBStorage};
use std::str::FromStr;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;

// The height of the block in which the promise yield is created.
const YIELD_CREATE_HEIGHT: u64 = 4;

// The height of the next block after environment setup is complete.
const NEXT_BLOCK_HEIGHT_AFTER_SETUP: u64 = 5;

// The height of the block in which we expect the yield timeout to trigger,
// producing a YieldResume receipt.
const YIELD_TIMEOUT_HEIGHT: u64 = YIELD_CREATE_HEIGHT + TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

/// Helper function which checks the outgoing receipts from the latest block.
/// Returns yield data ids for all PromiseYield and PromiseResume receipts.
fn find_yield_data_ids_from_latest_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let node = env.validator();
    let client = node.client();
    let genesis_block = client.chain.get_block_by_height(0).unwrap();
    let epoch_id = *genesis_block.header().epoch_id();
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = shard_layout.account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap());
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block_height = client.chain.head().unwrap().height;

    let mut result = vec![];

    for receipt in client
        .chain
        .get_outgoing_receipts_for_shard(last_block_hash, shard_id, last_block_height)
        .unwrap()
    {
        if let VersionedReceiptEnum::PromiseYield(action_receipt) = receipt.versioned_receipt() {
            result.push(action_receipt.input_data_ids()[0]);
        }
        if let ReceiptEnum::PromiseResume(data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }

    result
}

/// Read all the `PromiseYield` receipts stored in the latest state and collect their data_ids.
pub(crate) fn get_yield_data_ids_in_latest_state(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let node = env.validator();
    let client = node.client();
    let head = client.chain.head().unwrap();
    let block_hash = head.last_block_hash;
    let epoch_id = head.epoch_id;
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(&"test0".parse::<AccountId>().unwrap());

    let latest_state_root = get_latest_state_state_root(client, block_hash, shard_uid);
    get_yield_data_ids_in_state(client, latest_state_root, shard_uid)
}

/// Get the latest available state root.
fn get_latest_state_state_root(
    client: &Client,
    latest_block_hash: CryptoHash,
    shard_uid: ShardUId,
) -> CryptoHash {
    let block = client.chain.get_block(&latest_block_hash).unwrap();
    let chunks = block.chunks();
    let chunk_header =
        chunks.iter().find(|header| header.shard_id() == shard_uid.shard_id()).unwrap();

    let state_root =
        if let Ok(chunk_extra) = client.chain.get_chunk_extra(&latest_block_hash, &shard_uid) {
            // Use post-state of the latest chunk if available
            *chunk_extra.state_root()
        } else {
            // Otherwise use the pre-state of latest chunk
            chunk_header.prev_state_root()
        };

    state_root
}

/// Iterate over all PromiseYieldReceipt entries in the given state and collect their data_ids.
fn get_yield_data_ids_in_state(
    client: &Client,
    state_root: CryptoHash,
    shard_uid: ShardUId,
) -> Vec<CryptoHash> {
    let store = client.chain.chain_store().store();
    let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
    let trie = Trie::new(trie_storage, state_root, None);
    let locked_trie = trie.lock_for_iter();
    let mut iter = locked_trie.iter().unwrap();
    iter.seek_prefix(&[col::PROMISE_YIELD_RECEIPT]).unwrap();

    let mut result = vec![];
    for item in iter {
        let (key, _val) = item.unwrap();
        if !key.starts_with(&[col::PROMISE_YIELD_RECEIPT]) {
            break;
        }

        let account = trie_key_parsers::parse_account_id_from_raw_key(&key).unwrap().unwrap();
        let data_id = CryptoHash(key[(key.len() - 32)..].try_into().unwrap());
        let parsed_key = TrieKey::PromiseYieldReceipt { receiver_id: account, data_id };
        assert_eq!(&key, &parsed_key.to_vec());

        result.push(data_id);
    }
    result
}

fn get_promise_yield_statuses_in_state(
    client: &Client,
    state_root: CryptoHash,
    shard_uid: ShardUId,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let store = client.chain.chain_store().store();
    let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
    let trie = Trie::new(trie_storage, state_root, None);
    let locked_trie = trie.lock_for_iter();
    let mut iter = locked_trie.iter().unwrap();
    iter.seek_prefix(&[col::PROMISE_YIELD_STATUS]).unwrap();

    let mut result = vec![];
    for item in iter {
        let (key, val) = item.unwrap();
        if !key.starts_with(&[col::PROMISE_YIELD_STATUS]) {
            break;
        }

        result.push((key, val))
    }
    result
}

/// Assert that there are no leftover PromiseYieldStatus values stored in the Trie.
pub(crate) fn assert_no_promise_yield_status_in_state(env: &TestLoopEnv) {
    let node = env.validator();
    let client = node.client();
    let head = client.chain.head().unwrap();
    let epoch_id = head.epoch_id;
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.account_id_to_shard_uid(&"test0".parse::<AccountId>().unwrap());

    let state_root = get_latest_state_state_root(client, head.last_block_hash, shard_uid);
    let promise_yield_statuses = get_promise_yield_statuses_in_state(client, state_root, shard_uid);
    assert_eq!(promise_yield_statuses, Vec::new());
}

/// Create environment with an unresolved promise yield callback.
/// Returns the test environment, the yield tx hash, and the data id for resuming the yield.
fn prepare_env_with_yield(
    anticipated_yield_payload: Vec<u8>,
    test_env_gas_limit: Option<u64>,
) -> (TestLoopEnv, CryptoHash, CryptoHash) {
    init_test_logger();

    let test_account: AccountId = "test0".parse().unwrap();
    let test_account_signer = create_user_test_signer(&test_account).into();

    let runtime_config = RuntimeConfig::test();
    assert_eq!(
        runtime_config.wasm_config.limit_config.yield_timeout_length_in_blocks,
        TEST_CONFIG_YIELD_TIMEOUT_LENGTH
    );
    let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);

    let mut builder = TestLoopBuilder::new()
        .genesis_height(0)
        .add_user_account(&test_account, Balance::from_near(1_000_000))
        .runtime_config_store(runtime_config_store)
        .skip_warmup();
    if let Some(gas_limit) = test_env_gas_limit {
        builder = builder.gas_limit(Gas::from_gas(gas_limit));
    }
    let mut env = builder.build();

    assert_eq!(env.validator().head().height, 0);

    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();

    // Submit transaction deploying contract to test0
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::rs_contract().into(),
        &test_account_signer,
        *genesis_block.hash(),
    );
    env.validator().submit_tx(deploy_contract_tx.clone());

    // Allow two blocks for the contract to be deployed
    env.validator_runner().run_until_head_height(2);
    assert!(matches!(
        env.validator()
            .client()
            .chain
            .get_final_transaction_result(&deploy_contract_tx.get_hash())
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    // Submit transaction making a function call which will invoke yield create
    let yield_transaction = SignedTransaction::from_actions(
        10,
        test_account.clone(),
        test_account,
        &test_account_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: anticipated_yield_payload,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    let yield_tx_hash = yield_transaction.get_hash();
    env.validator().submit_tx(yield_transaction);
    env.validator_runner().run_until_head_height(4);
    assert!(matches!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started,
    ));

    let yield_data_ids = if ProtocolFeature::InstantPromiseYield.enabled(PROTOCOL_VERSION) {
        // After InstantPromiseYield, the PromiseYield receipt is immediately processed and saved in the state.
        get_yield_data_ids_in_latest_state(&env)
    } else {
        // Before InstantPromiseYield, the PromiseYield receipt was sent as an outgoing receipt.
        find_yield_data_ids_from_latest_block(&env)
    };
    assert_eq!(yield_data_ids.len(), 1);

    let last_block_height = env.validator().head().height;
    assert_eq!(NEXT_BLOCK_HEIGHT_AFTER_SETUP, last_block_height + 1);

    (env, yield_tx_hash, yield_data_ids[0])
}

fn invoke_yield_resume(
    env: &TestLoopEnv,
    data_id: CryptoHash,
    yield_payload: Vec<u8>,
) -> CryptoHash {
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();

    let resume_transaction = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume".to_string(),
            args: yield_payload.into_iter().chain(data_id.as_bytes().iter().cloned()).collect(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    let tx_hash = resume_transaction.get_hash();
    env.validator().submit_tx(resume_transaction);
    tx_hash
}

/// Add a bunch of function call transactions, congesting the chain.
///
/// Note that these transactions start to be processed in the *second* block produced after they are
/// inserted to client 0's mempool.
fn create_congestion(env: &TestLoopEnv) {
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();

    for i in 0..25 {
        let signed_transaction = SignedTransaction::from_actions(
            i + 100,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "epoch_height".to_string(),
                args: vec![],
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            *genesis_block.hash(),
        );
        env.validator().submit_tx(signed_transaction);
    }
}

/// Simple test of timeout execution.
/// Advances sufficiently many blocks, then verifies that the callback was executed.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_simple_yield_timeout() {
    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(vec![], None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // Advance through the blocks during which the yield will await resumption
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        env.validator_runner().run_until_head_height(block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.validator()
                .client()
                .chain
                .get_partial_transaction_result(&yield_tx_hash)
                .unwrap()
                .status,
            FinalExecutionStatus::Started
        );
    }

    // In this block the timeout is processed, producing a YieldResume receipt.
    env.validator_runner().run_until_head_height(YIELD_TIMEOUT_HEIGHT);
    // Checks that the anticipated YieldResume receipt was produced.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id]);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started
    );

    // In this block the resume receipt is applied and the callback will execute.
    env.validator_runner().run_until_head_height(YIELD_TIMEOUT_HEIGHT + 1);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Yield timeouts have the least (worst) priority for inclusion to a chunk.
/// In this test, we introduce congestion and verify that the timeout execution is
/// delayed as expected, but ultimately succeeds without error.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_timeout_under_congestion() {
    let (mut env, yield_tx_hash, _) = prepare_env_with_yield(vec![], Some(10_000_000_000_000));
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // By introducing congestion, we can delay the yield timeout
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..(YIELD_TIMEOUT_HEIGHT + 3) {
        // Submit txns to congest the block at height YIELD_TIMEOUT_HEIGHT and delay the timeout
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            create_congestion(&env);
        }

        env.validator_runner().run_until_head_height(block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.validator()
                .client()
                .chain
                .get_partial_transaction_result(&yield_tx_hash)
                .unwrap()
                .status,
            FinalExecutionStatus::Started
        );
    }

    // Advance until the congestion clears and the yield callback is executed.
    env.validator_runner().run_until(
        |node| {
            let tx_status =
                node.client().chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status;

            if tx_status == FinalExecutionStatus::Started {
                return false;
            }

            assert_eq!(tx_status, FinalExecutionStatus::SuccessValue(vec![0u8]));
            return true;
        },
        Duration::seconds(15),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// In this case we invoke yield_resume at the last block possible.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_resume_just_before_timeout() {
    let yield_payload = vec![6u8; 16];
    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(yield_payload.clone(), None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        // Submit txn so that yield_resume is invoked in the block at height YIELD_TIMEOUT_HEIGHT
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            invoke_yield_resume(&env, data_id, yield_payload.clone());
        }

        env.validator_runner().run_until_head_height(block_height);

        // The transaction will not have a result until the yield execution is resumed
        assert_eq!(
            env.validator()
                .client()
                .chain
                .get_partial_transaction_result(&yield_tx_hash)
                .unwrap()
                .status,
            FinalExecutionStatus::Started
        );
    }

    // In this block the `yield_resume` host function is invoked, producing a YieldResume receipt.
    env.validator_runner().run_until_head_height(YIELD_TIMEOUT_HEIGHT);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started
    );
    // Here we expect two receipts to be produced; one from yield_resume and one from timeout.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id, data_id]);

    // In this block the resume receipt is applied and the callback is executed with the resume payload.
    env.validator_runner().run_until_head_height(YIELD_TIMEOUT_HEIGHT + 1);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// In this test we introduce congestion to delay the yield timeout so that we can invoke
/// yield resume after the timeout height has passed.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_resume_after_timeout_height() {
    let yield_payload = vec![6u8; 16];
    let (mut env, yield_tx_hash, data_id) =
        prepare_env_with_yield(yield_payload.clone(), Some(10_000_000_000_000));
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // By introducing congestion, we can delay the yield timeout
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..(YIELD_TIMEOUT_HEIGHT + 3) {
        // Submit txns to congest the block at height YIELD_TIMEOUT_HEIGHT and delay the timeout
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            create_congestion(&env);
        }

        env.validator_runner().run_until_head_height(block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.validator()
                .client()
                .chain
                .get_partial_transaction_result(&yield_tx_hash)
                .unwrap()
                .status,
            FinalExecutionStatus::Started
        );
    }

    invoke_yield_resume(&env, data_id, yield_payload);

    // Advance until the congestion clears and the yield callback is executed.
    env.validator_runner().run_until(
        |node| {
            let tx_status =
                node.client().chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status;

            if tx_status == FinalExecutionStatus::Started {
                return false;
            }

            assert_eq!(tx_status, FinalExecutionStatus::SuccessValue(vec![16u8]),);
            true
        },
        Duration::seconds(15),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// In this test there is no block produced at height YIELD_TIMEOUT_HEIGHT.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg(feature = "test_features")]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_skip_timeout_height() {
    use assert_matches::assert_matches;
    use near_chain::Error;

    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(vec![], None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // Advance through the blocks during which the yield will await resumption
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        env.validator_runner().run_until_head_height(block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.validator()
                .client()
                .chain
                .get_partial_transaction_result(&yield_tx_hash)
                .unwrap()
                .status,
            FinalExecutionStatus::Started
        );
    }

    // Skip the timeout height and produce a block at height YIELD_TIMEOUT_HEIGHT + 1.
    // We still expect the timeout to be processed and produce a YieldResume receipt.
    assert_eq!(env.validator().head().height, YIELD_TIMEOUT_HEIGHT - 1);
    // Produce block at YIELD_TIMEOUT_HEIGHT+1 using the one at YIELD_TIMEOUT_HEIGHT-1 as the previous block.
    env.validator_mut().client_actor().adv_produce_blocks_on(
        1,
        true,
        near_client::client_actor::AdvProduceBlockHeightSelection::SelectedHeightOnLatestKnown {
            produced_block_height: YIELD_TIMEOUT_HEIGHT + 1,
        },
    );
    env.validator_runner()
        .run_until_head_height_with_timeout(YIELD_TIMEOUT_HEIGHT + 1, Duration::seconds(3));
    assert_eq!(env.validator().head().height, YIELD_TIMEOUT_HEIGHT + 1);
    // The block at YIELD_TIMEOUT_HEIGHT should be missing.
    assert_matches!(
        env.validator().client().chain.get_block_by_height(YIELD_TIMEOUT_HEIGHT),
        Err(Error::DBNotFoundErr(_))
    );

    // Checks that the anticipated YieldResume receipt was produced.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id]);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started
    );

    // In this block the resume receipt is applied and the callback will execute.
    env.validator_runner().run_until_head_height(YIELD_TIMEOUT_HEIGHT + 2);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
