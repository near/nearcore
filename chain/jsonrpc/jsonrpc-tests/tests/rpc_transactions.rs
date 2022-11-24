use std::sync::{Arc, Mutex};

use actix::{Actor, System};
use borsh::BorshSerialize;
use futures::{future, FutureExt, TryFutureExt};

use near_actix_test_utils::run_actix;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc::client::new_client;
use near_network::test_utils::WaitOrTimeoutActor;
use near_o11y::testonly::{init_integration_logger, init_test_logger};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockReference, Finality};
use near_primitives::views::FinalExecutionStatus;

use near_jsonrpc_tests::{self as test_utils, test_with_client};

/// Test sending transaction via json rpc without waiting.
#[test]
fn test_send_tx_async() {
    init_test_logger();

    run_actix(async {
        let (_, addr) = test_utils::start_all(test_utils::NodeType::Validator);

        let client = new_client(&format!("http://{}", addr));

        let tx_hash2 = Arc::new(Mutex::new(None));
        let tx_hash2_1 = tx_hash2.clone();
        let tx_hash2_2 = tx_hash2;
        let signer_account_id = "test1".to_string();

        actix::spawn(client.block(BlockReference::latest()).then(move |res| {
            let block_hash = res.unwrap().header.hash;
            let signer =
                InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
            let tx = SignedTransaction::send_money(
                1,
                signer_account_id.parse().unwrap(),
                "test2".parse().unwrap(),
                &signer,
                100,
                block_hash,
            );
            let bytes = tx.try_to_vec().unwrap();
            let tx_hash = tx.get_hash().to_string();
            *tx_hash2_1.lock().unwrap() = Some(tx.get_hash());
            client
                .broadcast_tx_async(&to_base64(&bytes))
                .map_ok(move |result| assert_eq!(tx_hash, result))
                .map(drop)
        }));
        let client1 = new_client(&format!("http://{}", addr));
        WaitOrTimeoutActor::new(
            Box::new(move |_| {
                let signer_account_id = "test1".parse().unwrap();
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

#[test]
fn test_send_tx_async_wait() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            &signer,
            100,
            block_hash,
        );
        let tx_encoded = to_base64(tx.try_to_vec().unwrap());

        let tx_hash = client.broadcast_tx_async(&tx_encoded).await.unwrap();
        assert_eq!(tx_hash, tx.get_hash().to_string());

        macro_rules! execution_wait {
            ($finality:expr, $wait_type:expr) => {{
                let tx_encoded1 = tx_encoded.clone();
                let wait_client = new_client(&client.server_addr);
                actix::spawn(async move {
                    let result = wait_client
                        .EXPERIMENTAL_tx_wait(&tx_encoded1, $finality, $wait_type)
                        .await
                        .unwrap();
                    assert_eq!(result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
                })
            }};
        }

        macro_rules! inclusion_wait {
            ($finality:expr) => {{
                let tx_encoded1 = tx_encoded.clone();
                let wait_client = new_client(&client.server_addr);
                actix::spawn(async move {
                    let result = wait_client
                        .EXPERIMENTAL_tx_inclusion_wait(&tx_encoded1, $finality)
                        .await
                        .unwrap();
                    result.block_hash
                })
            }};
        }

        // TODO ! This is broken because DB doesn't seem to be updated in these cases
        // TODO ..need to explore if it's because the data isn't stored on the shard or if
        // TODO ..testing infra is just mocked (which it seems like)
        // let f_f_handle = execution_wait!(Finality::Final, "full");
        // let f_e_handle = execution_wait!(Finality::Final, "execution_result");
        // let ds_f_handle = execution_wait!(Finality::DoomSlug, "full");
        let n_f_handle = execution_wait!(Finality::None, "full");

        // TODO ! re-enable tests when fixed
        // let i_f_hash = inclusion_wait!(Finality::Final).await.unwrap();
        // let i_ds_hash = inclusion_wait!(Finality::DoomSlug).await.unwrap();
        // let i_n_hash = inclusion_wait!(Finality::None).await.unwrap();

        // Transaction should be able to be queried now that inclusion has awaited on.
        // get block hash to verify against
        // let result = client.tx(tx.get_hash().to_string(), tx.transaction.signer_id).await.unwrap();
        // assert_eq!(result.transaction_outcome.block_hash, i_f_hash);
        // assert_eq!(result.transaction_outcome.block_hash, i_ds_hash);
        // assert_eq!(result.transaction_outcome.block_hash, i_n_hash);

        // Await on all executions to be awaited and status checked
        // f_f_handle.await.unwrap();
        // f_e_handle.await.unwrap();
        // ds_f_handle.await.unwrap();
        n_f_handle.await.unwrap();
    });
}

/// Test sending transaction and waiting for it to be committed to a block.
#[test]
fn test_send_tx_commit() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            &signer,
            100,
            block_hash,
        );
        let bytes = tx.try_to_vec().unwrap();
        let result = client.broadcast_tx_commit(to_base64(&bytes)).await.unwrap();
        assert_eq!(result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    });
}

/// Test that expired transaction should be rejected
#[test]
fn test_expired_tx() {
    init_integration_logger();
    run_actix(async {
        let (_, addr) = test_utils::start_all_with_validity_period_and_no_epoch_sync(
            test_utils::NodeType::Validator,
            1,
            false,
        );

        let block_hash = Arc::new(Mutex::new(None));
        let block_height = Arc::new(Mutex::new(None));

        WaitOrTimeoutActor::new(
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
                                let signer = InMemorySigner::from_seed(
                                    "test1".parse().unwrap(),
                                    KeyType::ED25519,
                                    "test1",
                                );
                                let tx = SignedTransaction::send_money(
                                    1,
                                    "test1".parse().unwrap(),
                                    "test2".parse().unwrap(),
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
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
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
fn test_tx_status_missing_tx() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        match client.tx(CryptoHash::new().to_string(), "test1".parse().unwrap()).await {
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
        let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
        // invalid base hash
        let tx = SignedTransaction::send_money(
            1,
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
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
