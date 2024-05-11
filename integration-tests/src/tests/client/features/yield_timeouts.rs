use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_parameters::config::TEST_CONFIG_YIELD_TIMEOUT_LENGTH;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptEnum::PromiseYield;
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

// The height of the block in which we expect the yield timeout to trigger.
const YIELD_TIMEOUT_HEIGHT: u64 = YIELD_CREATE_HEIGHT + TEST_CONFIG_YIELD_TIMEOUT_LENGTH + 1;

// Lowered gas limit used to facilitate congestion.
const TEST_ENV_GAS_LIMIT: u64 = 10_000_000_000_000;

// The number of transactions each containing one FunctionCallAction needed to fill a block.
const NUM_FUNCTION_CALL_TXNS_PER_BLOCK: u64 = 3;

/// Create environment with an unresolved promise yield callback.
/// Returns the test environment, the yield tx hash, and the data id for resuming the yield.
/// The gas limit is lowered to allow testing under congestion.
fn prepare_env_with_yield(anticipated_yield_payload: Vec<u8>) -> (TestEnv, CryptoHash, CryptoHash) {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.gas_limit = TEST_ENV_GAS_LIMIT;
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
            code: near_test_contracts::nightly_rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
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

    // Find and return the input data id for the promise yield
    let epoch_id = genesis_block.header().epoch_id().clone();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap(), &shard_layout);
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    for receipt in env.clients[0]
        .chain
        .get_outgoing_receipts_for_shard(last_block_hash, shard_id, YIELD_CREATE_HEIGHT)
        .unwrap()
    {
        if let PromiseYield(data_receipt) = receipt.receipt {
            return (env, yield_tx_hash, data_receipt.input_data_ids[0]);
        }
    }

    panic!("Expected to produce a PromiseYield receipt");
}

/// Add a bunch of function call transactions, congesting the chain.
///
/// Note that these transactions start to be processed in the *second* block produced after they are
/// inserted to client 0's mempool.
fn create_congestion(env: &mut TestEnv, num_congested_blocks: u64) {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let mut tx_hashes = vec![];

    for i in 0..NUM_FUNCTION_CALL_TXNS_PER_BLOCK * num_congested_blocks {
        let signed_transaction = SignedTransaction::from_actions(
            i + 100,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_promise".to_string(),
                args: vec![],
                gas: 100,
                deposit: 0,
            }))],
            *genesis_block.hash(),
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
    let (mut env, yield_tx_hash, _) = prepare_env_with_yield(vec![]);
    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    // Advance through the blocks during which the yield will await resumption
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..YIELD_TIMEOUT_HEIGHT {
        env.produce_block(0, block_height);

        // The transaction will not have a final result until the timeout is reached
        assert!(matches!(
            env.clients[0].chain.get_final_transaction_result(&yield_tx_hash),
            Err(_)
        ));
    }

    // Advance one more block, triggering the timeout
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT);
    assert_eq!(
        env.clients[0].chain.get_final_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![23u8]),
    );
}

/// Yield timeouts have the least (worst) priority for inclusion to a chunk.
/// In this test, we introduce congestion and verify that the timeout execution is
/// delayed as expected, but ultimately succeeds without error.
#[test]
fn yield_timeout_under_congestion() {
    let (mut env, yield_tx_hash, _) = prepare_env_with_yield(vec![]);

    assert!(NEXT_BLOCK_HEIGHT_AFTER_SETUP < YIELD_TIMEOUT_HEIGHT);

    const NUM_CONGESTED_BLOCKS: u64 = 5;

    // By introducing congestion, we can delay the yield timeout
    for block_height in NEXT_BLOCK_HEIGHT_AFTER_SETUP..(YIELD_TIMEOUT_HEIGHT + NUM_CONGESTED_BLOCKS)
    {
        // Submit txns in time to congest the block at height YIELD_TIMEOUT_HEIGHT
        // and prevent the timeout from being delivered
        if block_height == YIELD_TIMEOUT_HEIGHT - 1 {
            create_congestion(&mut env, NUM_CONGESTED_BLOCKS);
        }

        env.produce_block(0, block_height);

        // The yield transaction will not have a final result until the timeout is delivered
        assert!(matches!(
            env.clients[0].chain.get_final_transaction_result(&yield_tx_hash),
            Err(_)
        ));
    }

    // Advance one more block. Congestion has cleared and the callback will be executed
    env.produce_block(0, YIELD_TIMEOUT_HEIGHT + NUM_CONGESTED_BLOCKS);
    assert_eq!(
        env.clients[0].chain.get_final_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![23u8]),
    );
}
