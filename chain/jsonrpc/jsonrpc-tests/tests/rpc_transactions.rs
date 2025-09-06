use std::ops::ControlFlow;

use near_crypto::InMemorySigner;
use near_jsonrpc::client::new_client;
use near_jsonrpc_primitives::types::transactions::{RpcTransactionStatusRequest, TransactionInfo};
use near_network::test_utils::wait_or_timeout;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockReference;
use near_primitives::views::{FinalExecutionStatus, TxExecutionStatus};

use near_jsonrpc_tests::{
    NodeType, create_test_setup_with_accounts_and_validity, create_test_setup_with_node_type,
};

/// Test sending transaction via json rpc without waiting.
#[actix::test]
async fn test_send_tx_async() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    // Send async transaction
    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        &signer,
        100,
        block_hash,
    );
    let bytes = borsh::to_vec(&tx).unwrap();
    let tx_hash = tx.get_hash();

    // Broadcast transaction asynchronously
    let result = client.broadcast_tx_async(to_base64(&bytes)).await.unwrap();
    assert_eq!(tx_hash.to_string(), result);

    // Wait for transaction to be executed using wait_or_timeout
    wait_or_timeout(100, 10000, || async {
        match client
            .tx(RpcTransactionStatusRequest {
                transaction_info: TransactionInfo::TransactionId {
                    tx_hash,
                    sender_account_id: "test1".parse().unwrap(),
                },
                wait_until: TxExecutionStatus::Executed,
            })
            .await
        {
            Ok(result) => {
                if let FinalExecutionStatus::SuccessValue(_) =
                    result.final_execution_outcome.unwrap().into_outcome().status
                {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
            Err(_) => ControlFlow::Continue(()),
        }
    })
    .await
    .expect("Transaction should be executed within timeout");
}

/// Test sending transaction and waiting for it to be committed to a block.
#[actix::test]
async fn test_send_tx_commit() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        &signer,
        100,
        block_hash,
    );
    let bytes = borsh::to_vec(&tx).unwrap();
    let result = client.broadcast_tx_commit(to_base64(&bytes)).await.unwrap();
    assert_eq!(
        result.final_execution_outcome.unwrap().into_outcome().status,
        FinalExecutionStatus::SuccessValue(Vec::new())
    );
    assert!(
        [TxExecutionStatus::Executed, TxExecutionStatus::Final]
            .contains(&result.final_execution_status),
        "All the receipts should be already executed"
    );
}

/// Test that expired transaction should be rejected
#[actix::test]
async fn test_expired_tx() {
    // Create setup with very short transaction validity period (1 block)
    let accounts = vec!["test1".parse().unwrap(), "test2".parse().unwrap()];
    let setup = create_test_setup_with_accounts_and_validity(
        accounts,
        "test1".parse().unwrap(),
        "test1".parse().unwrap(),
        1, // Very short validity period
    );
    let client = new_client(&setup.server_addr);

    // Get initial block info
    let initial_block = client.block(BlockReference::latest()).await.unwrap();
    let old_block_hash = initial_block.header.hash;

    // Wait for at least 2 more blocks to be produced so transaction becomes expired
    wait_or_timeout(100, 10000, || async {
        let current_block = client.block(BlockReference::latest()).await.unwrap();
        if current_block.header.height >= initial_block.header.height + 2 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .await
    .expect("Should produce at least 2 more blocks");

    // Try to send transaction with the old block hash (should be expired)
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        &signer,
        100,
        old_block_hash,
    );
    let bytes = borsh::to_vec(&tx).unwrap();

    // This should fail with "Expired" error
    match client.broadcast_tx_commit(to_base64(&bytes)).await {
        Err(err) => {
            assert_eq!(
                *err.data.unwrap(),
                serde_json::json!({"TxExecutionError": {
                    "InvalidTxError": "Expired"
                }})
            );
        }
        Ok(_) => panic!("Expected transaction to be rejected as expired"),
    }
}

/// Test sending transaction based on a different fork should be rejected
#[actix::test]
async fn test_replay_protection() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test2".parse().unwrap(),
        &signer,
        100,
        hash(&[1]),
    );
    let bytes = borsh::to_vec(&tx).unwrap();
    if let Ok(_) = client.broadcast_tx_commit(to_base64(&bytes)).await {
        panic!("transaction should not succeed");
    }
}

#[actix::test]
async fn test_tx_status_missing_tx() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let request = RpcTransactionStatusRequest {
        transaction_info: TransactionInfo::TransactionId {
            tx_hash: CryptoHash::new(),
            sender_account_id: "test1".parse().unwrap(),
        },
        wait_until: TxExecutionStatus::None,
    };
    match client.tx(request).await {
        Err(e) => {
            let s = serde_json::to_string(&e.data.unwrap()).unwrap();
            assert_eq!(s, "\"Transaction 11111111111111111111111111111111 doesn't exist\"");
        }
        Ok(_) => panic!("transaction should not succeed"),
    }
}

#[actix::test]
async fn test_check_invalid_tx() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    // invalid base hash
    let request = RpcTransactionStatusRequest {
        transaction_info: TransactionInfo::from_signed_tx(SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            &signer,
            100,
            hash(&[1]),
        )),
        wait_until: TxExecutionStatus::None,
    };
    match client.tx(request).await {
        Err(e) => {
            let s = serde_json::to_string(&e.data.unwrap()).unwrap();
            assert_eq!(s, "{\"TxExecutionError\":{\"InvalidTxError\":\"Expired\"}}");
        }
        Ok(_) => panic!("transaction should not succeed"),
    }
}
