use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_parameters::config::TEST_CONFIG_YIELD_TIMEOUT_LENGTH;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptEnum::{PromiseResume, PromiseYield};
use near_primitives::shard_layout::account_id_to_shard_id;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::AccountId;
use near_primitives::views::FinalExecutionStatus;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

// The height of the block in which the promise yield is created.
const YIELD_CREATE_HEIGHT: u64 = 4;

// The height of the next block after environment setup is complete.
const NEXT_BLOCK_HEIGHT_AFTER_SETUP: u64 = 5;

// The height of the block in which we expect the yield timeout to trigger,
// producing a YieldResume receipt.
const YIELD_TIMEOUT_HEIGHT: u64 = YIELD_CREATE_HEIGHT + TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

/// Helper function which checks the outgoing receipts from the latest block.
/// Returns yield data ids for all PromiseYield and PromiseResume receipts.
fn find_yield_data_ids_from_latest_block(env: &TestEnv) -> Vec<CryptoHash> {
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let epoch_id = genesis_block.header().epoch_id().clone();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap(), &shard_layout);
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let last_block_height = env.clients[0].chain.head().unwrap().height;

    let mut result = vec![];

    for receipt in env.clients[0]
        .chain
        .get_outgoing_receipts_for_shard(last_block_hash, shard_id, last_block_height)
        .unwrap()
    {
        if let PromiseYield(ref action_receipt) = receipt.receipt() {
            result.push(action_receipt.input_data_ids[0]);
        }
        if let PromiseResume(ref data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }

    result
}

/// Create environment with an unresolved promise yield callback.
/// Returns the test environment, the yield tx hash, and the data id for resuming the yield.
fn prepare_env_with_yield(
    anticipated_yield_payload: Vec<u8>,
    test_env_gas_limit: Option<u64>,
) -> (TestEnv, CryptoHash, CryptoHash) {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    if let Some(gas_limit) = test_env_gas_limit {
        genesis.config.gas_limit = gas_limit;
    }
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");

    // Submit transaction deploying contract to test0
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
        0,
    );
    let tx_hash = tx.get_hash();
    assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    // Allow two blocks for the contract to be deployed
    for i in 1..3 {
        env.produce_block(0, i);
    }
    assert!(matches!(
        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    // Submit transaction making a function call which will invoke yield create
    let yield_transaction = SignedTransaction::from_actions(
        10,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: anticipated_yield_payload,
            gas: 300_000_000_000_000,
            deposit: 0,
        }))],
        *genesis_block.hash(),
        0,
    );
    let yield_tx_hash = yield_transaction.get_hash();
    assert_eq!(
        env.clients[0].process_tx(yield_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    // Allow two blocks for the function call to be executed
    for i in 3..5 {
        env.produce_block(0, i);
    }

    let yield_data_ids = find_yield_data_ids_from_latest_block(&env);
    assert_eq!(yield_data_ids.len(), 1);

    let last_block_height = env.clients[0].chain.head().unwrap().height;
    assert_eq!(NEXT_BLOCK_HEIGHT_AFTER_SETUP, last_block_height + 1);

    (env, yield_tx_hash, yield_data_ids[0])
}

/// Add a transaction which invokes yield resume using given data id.
fn invoke_yield_resume(
    env: &mut TestEnv,
    data_id: CryptoHash,
    yield_payload: Vec<u8>,
) -> CryptoHash {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let resume_transaction = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume".to_string(),
            args: yield_payload.into_iter().chain(data_id.as_bytes().iter().cloned()).collect(),
            gas: 300_000_000_000_000,
            deposit: 0,
        }))],
        *genesis_block.hash(),
        0,
    );
    let tx_hash = resume_transaction.get_hash();
    assert_eq!(
        env.clients[0].process_tx(resume_transaction, false, false),
        ProcessTxResponse::ValidTx
    );
    tx_hash
}

/// Add a bunch of function call transactions, congesting the chain.
///
/// Note that these transactions start to be processed in the *second* block produced after they are
/// inserted to client 0's mempool.
fn create_congestion(env: &mut TestEnv) {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let mut tx_hashes = vec![];

    for i in 0..25 {
        let signed_transaction = SignedTransaction::from_actions(
            i + 100,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "epoch_height".to_string(),
                args: vec![],
                gas: 100,
                deposit: 0,
            }))],
            *genesis_block.hash(),
            0,
        );
        tx_hashes.push(signed_transaction.get_hash());
        assert_eq!(
            env.clients[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::ValidTx
        );
    }
}

/// Simple test of timeout execution.
/// Advances sufficiently many blocks, then verifies that the callback was executed.
#[test]
fn simple_yield_timeout() {
    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(vec![], None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // Advance through the blocks during which the yield will await resumption
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        env.produce_block(0, block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started
        );
    }

    // In this block the timeout is processed, producing a YieldResume receipt.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT);
    // Checks that the anticipated YieldResume receipt was produced.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id]);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::Started
    );

    // In this block the resume receipt is applied and the callback will execute.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT + 1);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
    );
}

/// Yield timeouts have the least (worst) priority for inclusion to a chunk.
/// In this test, we introduce congestion and verify that the timeout execution is
/// delayed as expected, but ultimately succeeds without error.
#[test]
fn yield_timeout_under_congestion() {
    let (mut env, yield_tx_hash, _) = prepare_env_with_yield(vec![], Some(10_000_000_000_000));
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // By introducing congestion, we can delay the yield timeout
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..(YIELD_TIMEOUT_HEIGHT + 3) {
        // Submit txns to congest the block at height YIELD_TIMEOUT_HEIGHT and delay the timeout
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            create_congestion(&mut env);
        }

        env.produce_block(0, block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started
        );
    }

    // Advance until the congestion clears and the yield callback is executed.
    let mut next_block_height = YIELD_TIMEOUT_HEIGHT + 3;
    loop {
        env.produce_block(0, next_block_height);
        next_block_height += 1;

        let tx_status =
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status;

        if tx_status == FinalExecutionStatus::Started {
            continue;
        }

        assert_eq!(tx_status, FinalExecutionStatus::SuccessValue(vec![0u8]));
        break;
    }
}

/// In this case we invoke yield_resume at the last block possible.
#[test]
fn yield_resume_just_before_timeout() {
    let yield_payload = vec![6u8; 16];
    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(yield_payload.clone(), None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        // Submit txn so that yield_resume is invoked in the block at height YIELD_TIMEOUT_HEIGHT
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            invoke_yield_resume(&mut env, data_id, yield_payload.clone());
        }

        env.produce_block(0, block_height);

        // The transaction will not have a result until the yield execution is resumed
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started
        );
    }

    // In this block the `yield_resume` host function is invoked, producing a YieldResume receipt.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::Started
    );
    // Here we expect two receipts to be produced; one from yield_resume and one from timeout.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id, data_id]);

    // In this block the resume receipt is applied and the callback is executed with the resume payload.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT + 1);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );
}

/// In this test we introduce congestion to delay the yield timeout so that we can invoke
/// yield resume after the timeout height has passed.
#[test]
fn yield_resume_after_timeout_height() {
    let yield_payload = vec![6u8; 16];
    let (mut env, yield_tx_hash, data_id) =
        prepare_env_with_yield(yield_payload.clone(), Some(10_000_000_000_000));
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // By introducing congestion, we can delay the yield timeout
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..(YIELD_TIMEOUT_HEIGHT + 3) {
        // Submit txns to congest the block at height YIELD_TIMEOUT_HEIGHT and delay the timeout
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            create_congestion(&mut env);
        }

        env.produce_block(0, block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started
        );
    }

    invoke_yield_resume(&mut env, data_id, yield_payload);

    // Advance until the congestion clears and the yield callback is executed.
    let mut next_block_height = YIELD_TIMEOUT_HEIGHT + 3;
    loop {
        env.produce_block(0, next_block_height);
        next_block_height += 1;

        let tx_status =
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status;

        if tx_status == FinalExecutionStatus::Started {
            continue;
        }

        assert_eq!(tx_status, FinalExecutionStatus::SuccessValue(vec![16u8]),);
        break;
    }
}

/// In this test there is no block produced at height YIELD_TIMEOUT_HEIGHT.
#[test]
fn skip_timeout_height() {
    let (mut env, yield_tx_hash, data_id) = prepare_env_with_yield(vec![], None);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // Advance through the blocks during which the yield will await resumption
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        env.produce_block(0, block_height);

        // The transaction will not have a result until the timeout is reached
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started
        );
    }

    // Skip the timeout height and produce a block at height YIELD_TIMEOUT_HEIGHT + 1.
    // We still expect the timeout to be processed and produce a YieldResume receipt.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT + 1);
    // Checks that the anticipated YieldResume receipt was produced.
    assert_eq!(find_yield_data_ids_from_latest_block(&env), vec![data_id]);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::Started
    );

    // In this block the resume receipt is applied and the callback will execute.
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT + 2);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
    );
}
