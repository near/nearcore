use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::views::FinalExecutionStatus;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

/// Create environment with an unresolved promise yield callback.
/// The gas limit is lowered to allow testing under congestion.
fn prepare_env_with_yield() -> (TestEnv, CryptoHash) {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.gas_limit = 10_000_000_000_000;
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
            args: vec![],
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

    (env, yield_tx_hash)
}

/// Advances sufficiently many blocks, then verifies that the callback was executed by timeout.
#[test]
fn simple_yield_timeout() {
    let (mut env, yield_tx_hash) = prepare_env_with_yield();

    let yield_timeout_length =
        RuntimeConfig::test().wasm_config.limit_config.yield_timeout_length_in_blocks;

    // Advance yield_timeout_length blocks, during which the yield will await resumption
    for i in 5..(5 + yield_timeout_length) {
        env.produce_block(0, i);

        // The transaction will not have a final result until the timeout is reached
        assert!(matches!(
            env.clients[0].chain.get_final_transaction_result(&yield_tx_hash),
            Err(_)
        ));
    }

    // Advance one more block, triggering the timeout
    env.produce_block(0, 5 + yield_timeout_length);
    assert_eq!(
        env.clients[0].chain.get_final_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![23u8]),
    );
}

// Add a bunch of function call transactions that generate promises, congesting the chain.
fn create_congestion(env: &mut TestEnv) {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let gas_1 = 9_000_000_000_000;
    let gas_2 = gas_1 / 3;
    let mut tx_hashes = vec![];

    for i in 0..10 {
        let data = serde_json::json!([
            {"create": {
            "account_id": "test0",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": gas_2,
            }, "id": 0 }
        ]);

        let signed_transaction = SignedTransaction::from_actions(
            i + 100,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_promise".to_string(),
                args: serde_json::to_vec(&data).unwrap(),
                gas: gas_1,
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

/// Yield timeouts have the least (worst) priority for inclusion to a chunk.
/// In this test, we introduce congestion and verify that the timeout execution is
/// delayed as expected, but ultimately succeeds without error.
#[test]
fn yield_timeout_under_congestion() {
    let (mut env, yield_tx_hash) = prepare_env_with_yield();

    let yield_timeout_length =
        RuntimeConfig::test().wasm_config.limit_config.yield_timeout_length_in_blocks;

    let mut next_block_height = 5;

    // Advance yield_timeout_length - 1 blocks, during which the yield will await resumption.
    for _ in 0..yield_timeout_length - 1 {
        env.produce_block(0, next_block_height);
        next_block_height += 1;

        // The yield transaction will not have a final result until the timeout is reached
        assert!(matches!(
            env.clients[0].chain.get_final_transaction_result(&yield_tx_hash),
            Err(_)
        ));
    }

    create_congestion(&mut env);

    // Advance more blocks. The congestion should prevent the timeout from executing.
    for _ in 0..5 {
        env.produce_block(0, next_block_height);
        next_block_height += 1;

        // The yield transaction will not have a final result until the timeout is reached
        assert!(matches!(
            env.clients[0].chain.get_final_transaction_result(&yield_tx_hash),
            Err(_)
        ));
    }

    // Advance one more block, triggering the timeout. Check for the expected outcome.
    env.produce_block(0, next_block_height);
    assert_eq!(
        env.clients[0].chain.get_final_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![23u8]),
    );
}
