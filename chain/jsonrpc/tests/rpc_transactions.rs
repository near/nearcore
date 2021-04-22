use std::sync::{Arc, Mutex};

use actix::{Actor, System};
use borsh::BorshSerialize;
use futures::{future, FutureExt, TryFutureExt};

use near_actix_test_utils::run_actix_until_stop;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc::client::new_client;
use near_logger_utils::{init_integration_logger, init_test_logger};
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::{to_base, to_base64};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockReference;
use near_primitives::views::FinalExecutionStatus;

#[macro_use]
pub mod test_utils;

/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    init_test_logger();

    run_actix_until_stop(async {
        let (_, addr) = test_utils::start_all(test_utils::NodeType::Validator);

        let client = new_client(&format!("http://{}", addr.clone()));

        let tx_hash2 = Arc::new(Mutex::new(None));
        let tx_hash2_1 = tx_hash2.clone();
        let tx_hash2_2 = tx_hash2.clone();
        let signer_account_id = "test1".to_string();

        actix::spawn(client.block(BlockReference::latest()).then(move |res| {
            let block_hash = res.unwrap().header.hash;
            let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
            let tx = SignedTransaction::send_money(
                1,
                signer_account_id,
                "test2".to_string(),
                &signer,
                100,
                block_hash,
            );
            let bytes = tx.try_to_vec().unwrap();
            let tx_hash = tx.get_hash().to_string();
            *tx_hash2_1.lock().unwrap() = Some(tx.get_hash());
            client
                .broadcast_tx_async(to_base64(&bytes))
                .map_ok(move |result| assert_eq!(tx_hash, result))
                .map(drop)
        }));
        let client1 = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                let signer_account_id = "test1".to_string();
                if let Some(tx_hash) = *tx_hash2_2.lock().unwrap() {
                    actix::spawn(
                        client1
                            .tx(tx_hash.to_string(), signer_account_id)
                            .map_err(|err| println!("Error: {:?}", err))
                            .map_ok(|result| {
                                if let FinalExecutionStatus::SuccessValue(_) = result.status {
                                    System::current().stop();
                                }
                            })
                            .map(drop),
                    );
                }
            }),
            100,
            2000,
        )
        .start();
    });
}

/// Test sending transaction and waiting for it to be committed to a block.
#[test]
fn test_send_tx_commit() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test2".to_string(),
            &signer,
            100,
            block_hash,
        );
        let bytes = tx.try_to_vec().unwrap();
        let result = client.broadcast_tx_commit(to_base64(&bytes)).await.unwrap();
        assert_eq!(result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    });
}

/// Test that expired transaction should be rejected
#[test]
fn test_expired_tx() {
    init_integration_logger();
    run_actix_until_stop(async {
        let (_, addr) = test_utils::start_all_with_validity_period_and_no_epoch_sync(
            test_utils::NodeType::Validator,
            1,
            false,
        );

        let block_hash = Arc::new(Mutex::new(None));
        let block_height = Arc::new(Mutex::new(None));

        WaitOrTimeout::new(
            Box::new(move |_| {
                let block_hash = block_hash.clone();
                let block_height = block_height.clone();
                let client = new_client(&format!("http://{}", addr));
                actix::spawn(client.block(BlockReference::latest()).then(move |res| {
                    let header = res.unwrap().header;
                    let hash = block_hash.lock().unwrap().clone();
                    let height = block_height.lock().unwrap().clone();
                    if let Some(block_hash) = hash {
                        if let Some(height) = height {
                            if header.height - height >= 2 {
                                let signer =
                                    InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
                                let tx = SignedTransaction::send_money(
                                    1,
                                    "test1".to_string(),
                                    "test2".to_string(),
                                    &signer,
                                    100,
                                    block_hash,
                                );
                                let bytes = tx.try_to_vec().unwrap();
                                actix::spawn(
                                    client
                                        .broadcast_tx_commit(to_base64(&bytes))
                                        .map_err(|err| {
                                            assert_eq!(
                                                err.data.unwrap(),
                                                serde_json::json!({"TxExecutionError": {
                                                    "InvalidTxError": "Expired"
                                                }})
                                            );
                                            System::current().stop();
                                        })
                                        .map(|_| ()),
                                );
                            }
                        }
                    } else {
                        *block_hash.lock().unwrap() = Some(header.hash);
                        *block_height.lock().unwrap() = Some(header.height);
                    };
                    future::ready(())
                }));
            }),
            100,
            1000,
        )
        .start();
    });
}

/// Test sending transaction based on a different fork should be rejected
#[test]
fn test_replay_protection() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test2".to_string(),
            &signer,
            100,
            hash(&[1]),
        );
        let bytes = tx.try_to_vec().unwrap();
        if let Ok(_) = client.broadcast_tx_commit(to_base64(&bytes)).await {
            panic!("transaction should not succeed");
        }
    });
}

#[test]
fn test_tx_status_invalid_account_id() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        match client.tx(to_base(&CryptoHash::default()), "".to_string()).await {
            Err(e) => {
                let s = serde_json::to_string(&e.data.unwrap()).unwrap();
                assert!(s.starts_with("\"Invalid account id"));
            }
            Ok(_) => panic!("transaction should not succeed"),
        };
    });
}

#[test]
fn test_tx_status_missing_tx() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        match client.tx(to_base(&CryptoHash::default()), "test1".to_string()).await {
            Err(e) => {
                let s = serde_json::to_string(&e.data.unwrap()).unwrap();
                assert_eq!(s, "\"Transaction 11111111111111111111111111111111 doesn't exist\"");
            }
            Ok(_) => panic!("transaction should not succeed"),
        }
    });
}

#[test]
fn test_check_invalid_tx() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        // invalid base hash
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test2".to_string(),
            &signer,
            100,
            hash(&[1]),
        );
        let bytes = tx.try_to_vec().unwrap();
        match client.EXPERIMENTAL_check_tx(to_base64(&bytes)).await {
            Err(e) => {
                let s = serde_json::to_string(&e.data.unwrap()).unwrap();
                println!("{}", s);
                assert_eq!(s, "{\"TxExecutionError\":{\"InvalidTxError\":\"Expired\"}}");
            }
            Ok(_) => panic!("transaction should not succeed"),
        }
    });
}
