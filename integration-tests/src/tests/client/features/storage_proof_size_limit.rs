use assert_matches::assert_matches;
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_parameters::RuntimeConfigStore;
use near_primitives::action::{Action, DeployContractAction, FunctionCallAction};
use near_primitives::checked_feature;
use near_primitives::errors::FunctionCallError;
use near_primitives::errors::{ActionErrorKind, TxExecutionError};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::FinalExecutionStatus;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

/// Test:
/// * per-receipt hard storage proof size limit
/// * per-chunk soft storage proof size limit
#[test]
fn test_storage_proof_size_limit() {
    near_o11y::testonly::init_test_logger();

    let epoch_length = 100;
    let contract_account: AccountId = "test0".parse().unwrap();
    let user_account: AccountId = "test1".parse().unwrap();
    let runtime_config_store = RuntimeConfigStore::new(None);
    let mut env = {
        let mut genesis = Genesis::test(vec![contract_account.clone(), user_account.clone()], 1);
        genesis.config.epoch_length = epoch_length;
        genesis.config.protocol_version = PROTOCOL_VERSION;
        TestEnv::builder(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(&genesis, vec![runtime_config_store])
            .build()
    };

    // setup: deploy the contract
    {
        let code = near_test_contracts::rs_contract().to_vec();
        let actions = vec![Action::DeployContract(DeployContractAction { code })];

        let signer = InMemorySigner::from_seed(
            contract_account.clone(),
            KeyType::ED25519,
            contract_account.as_ref(),
        );
        let tx = env.tx_from_actions(actions, &signer, signer.account_id.clone());
        env.execute_tx(tx).unwrap().assert_success();
    }

    // Fetching the correct nonce from `env` is a bit fiddly, we would have to
    // query the access key of the user. It's easier to keep a shared counter
    // that starts at 1 and increases monotonically.
    let mut nonce = 1;
    let signer =
        InMemorySigner::from_seed(user_account.clone(), KeyType::ED25519, user_account.as_ref());

    // Write 1MB values under keys 0, 1, 2, 3, ..., 23.
    // 24MB of data in total
    for idx in 0_u8..24 {
        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "write_one_megabyte".to_string(),
            args: vec![idx],
            gas: 300_000_000_000_000,
            deposit: 0,
        }));

        let tx = SignedTransaction::from_actions(
            nonce,
            signer.account_id.clone(),
            contract_account.clone(),
            &signer,
            vec![action],
            env.clients[0].chain.head().unwrap().last_block_hash,
        );
        nonce += 1;
        let res = env.execute_tx(tx).unwrap();
        assert_matches!(res.status, FinalExecutionStatus::SuccessValue(_));
    }

    let after_writes_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    // read_n_megabytes reads keys between from..to, (to - from) MB of data in total.
    let mut make_read_transaction = |from: u8, to: u8| {
        let action = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "read_n_megabytes".to_string(),
            args: vec![from, to],
            gas: 300_000_000_000_000,
            deposit: 0,
        }));
        let tx = SignedTransaction::from_actions(
            nonce,
            signer.account_id.clone(),
            contract_account.clone(),
            &signer,
            vec![action],
            after_writes_block_hash,
        );
        nonce += 1;
        tx
    };

    // Test the hard per-receipt limit
    // First perform a 3MB read (keys 0..3), which should succeed.
    let read3_tx = make_read_transaction(0, 3);
    let res = env.execute_tx(read3_tx).unwrap();
    assert_matches!(res.status, FinalExecutionStatus::SuccessValue(_));

    // Now perform a 20MB read (keys 0..20), which should fail due to the hard per-receipt storage proof size limit.
    let read20_tx = make_read_transaction(0, 20);
    let res = env.execute_tx(read20_tx).unwrap();
    if checked_feature!("stable", PerReceiptHardStorageProofLimit, PROTOCOL_VERSION) {
        assert_matches!(res.status, FinalExecutionStatus::Failure(_));
        let error_string = match res.status {
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) => {
                match action_error.kind {
                    ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                        error_string,
                    )) => error_string,
                    other => panic!("Bad ActionErrorKind: {:?}", other),
                }
            }
            other => panic!("Bad FinalExecutionStatus: {:?}", other),
        };
        assert!(error_string
            .contains("Size of the recorded trie storage proof has exceeded the allowed limit"));
    } else {
        assert_matches!(res.status, FinalExecutionStatus::SuccessValue(_));
    }

    // Now test the per-chunk soft limit.
    // Spawn 3 transactions, each reading 2MB of data. The first two should end up in the same chunk.
    // After the first two receipts the 3MB soft limit will be hit and the third receipt will be postponed to
    // the next chunk.
    // We must read different values in every receipt to make sure that the receipts are always recording fresh data.
    let read2_txs =
        [make_read_transaction(0, 2), make_read_transaction(2, 4), make_read_transaction(4, 6)];
    for read2_tx in &read2_txs {
        let response = env.clients[0].process_tx(read2_tx.clone(), false, false);
        assert_eq!(response, ProcessTxResponse::ValidTx);
    }

    let mut next_chunk = || {
        let tip = env.clients[0].chain.head().unwrap();
        let block = env.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        let chunk_header = block.chunks().get(0).unwrap().clone();
        env.clients[0].chain.get_chunk(&chunk_header.chunk_hash()).unwrap()
    };

    // Empty chunk
    next_chunk();

    // Chunk A - 3 submitted transactions
    let chunk = next_chunk();
    assert_eq!(chunk.transactions().len(), 3);
    assert_eq!(chunk.prev_outgoing_receipts().len(), 0);

    // Chunk B - 3 FuntionCall receipts (converted from transactions)
    let chunk = next_chunk();
    assert_eq!(chunk.transactions().len(), 0);
    assert_eq!(count_function_call_receipts(chunk.prev_outgoing_receipts()), 3);
    assert_eq!(count_transfer_receipts(chunk.prev_outgoing_receipts()), 0);

    // Chunk C - 2 transfer receipts from two executed FunctionCals, third FunctionCall moved to delayed receipt queue
    let chunk = next_chunk();
    assert_eq!(chunk.transactions().len(), 0);
    assert_eq!(count_function_call_receipts(chunk.prev_outgoing_receipts()), 0);
    if checked_feature!("stable", StateWitnessSizeLimit, PROTOCOL_VERSION) {
        assert_eq!(count_transfer_receipts(chunk.prev_outgoing_receipts()), 2);
    } else {
        // Without soft limit the receipts are processed immediately.
        assert_eq!(count_transfer_receipts(chunk.prev_outgoing_receipts()), 3);
    }

    // Chunk D - 1 transfer receipt from the third FunctionCall
    let chunk = next_chunk();
    assert_eq!(chunk.transactions().len(), 0);
    assert_eq!(count_function_call_receipts(chunk.prev_outgoing_receipts()), 0);
    if checked_feature!("stable", StateWitnessSizeLimit, PROTOCOL_VERSION) {
        assert_eq!(count_transfer_receipts(chunk.prev_outgoing_receipts()), 1);
    } else {
        assert_eq!(count_transfer_receipts(chunk.prev_outgoing_receipts()), 0);
    }
}

fn count_function_call_receipts(receipts: &[Receipt]) -> usize {
    receipts.iter().filter(|r| matches!(receipt_action(r), Action::FunctionCall(_))).count()
}

fn count_transfer_receipts(receipts: &[Receipt]) -> usize {
    receipts.iter().filter(|r| matches!(receipt_action(r), Action::Transfer(_))).count()
}

fn receipt_action(receipt: &Receipt) -> &Action {
    match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => &action_receipt.actions[0],
        _ => panic!("Expected Action receipt"),
    }
}
