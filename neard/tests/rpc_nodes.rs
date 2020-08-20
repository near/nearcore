use std::time::Duration;

use actix::clock::delay_for;
use actix::{Actor, System};
use borsh::BorshSerialize;
use futures::future::join_all;
use futures::{future, FutureExt, TryFutureExt};

use near_client::{GetBlock, GetExecutionOutcome, TxStatus};
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc::client::new_client;
use near_logger_utils::init_integration_logger;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{compute_root_from_path_and_item, verify_path};
use near_primitives::serialize::{from_base64, to_base64};
use near_primitives::transaction::{PartialExecutionStatus, SignedTransaction};
use near_primitives::types::{BlockId, BlockReference, TransactionOrReceiptId};
use near_primitives::views::{
    ExecutionOutcomeView, ExecutionStatusView, FinalExecutionStatus, QueryResponseKind,
};
use neard::config::TESTING_INIT_BALANCE;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use testlib::{genesis_block, start_nodes, test_helpers::heavy_test};

macro_rules! panic_on_rpc_error {
    ($e:expr) => {
        if !serde_json::to_string(&$e.data.clone().unwrap_or_default())
            .unwrap()
            .contains("IsSyncing")
        {
            panic!("{:?}", $e)
        }
    };
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client and checks that a node can return tx status.
#[test]
fn test_tx_propagation() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10, 0);
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.1", KeyType::ED25519, "near.1");
        let transaction = SignedTransaction::send_money(
            1,
            "near.1".to_string(),
            "near.2".to_string(),
            &signer,
            10000,
            genesis_hash,
        );
        let tx_hash = transaction.get_hash();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let transaction_copy1 = transaction.clone();
                let tx_hash_clone = tx_hash.clone();
                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        let bytes = transaction_copy.try_to_vec().unwrap();
                        actix::spawn(
                            client
                                .broadcast_tx_async(to_base64(&bytes))
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| {
                                    assert_eq!(String::from(&tx_hash_clone), result)
                                })
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
                actix::spawn(
                    view_client
                        .send(TxStatus { tx_hash, signer_account_id: "near.1".to_string() })
                        .then(move |res| {
                            match &res {
                                Ok(Ok(Some(feo)))
                                    if feo.status
                                        == FinalExecutionStatus::SuccessValue("".to_string())
                                        && feo.transaction == transaction_copy1.into() =>
                                {
                                    System::current().stop();
                                }
                                _ => return future::ready(()),
                            };
                            future::ready(())
                        }),
                );
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client through `broadcast_tx_commit` and checks that the transaction succeeds.
#[test]
fn test_tx_propagation_through_rpc() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 1000, 0);
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.1", KeyType::ED25519, "near.1");
        let transaction = SignedTransaction::send_money(
            1,
            "near.1".to_string(),
            "near.2".to_string(),
            &signer,
            10000,
            genesis_hash,
        );
        let has_sent_tx = Arc::new(AtomicBool::new(false));

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let has_sent_tx1 = has_sent_tx.clone();
                // We are sending this tx unstop, just to get over the warm up period.
                // Probably make sense to stop after 1 time though.
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 10 && !has_sent_tx1.load(SeqCst) {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        let bytes = transaction_copy.try_to_vec().unwrap();
                        actix::spawn(
                            client
                                .broadcast_tx_commit(to_base64(&bytes))
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| {
                                    if result.status
                                        == FinalExecutionStatus::SuccessValue("".to_string())
                                    {
                                        System::current().stop();
                                    } else {
                                        panic!("wrong transaction status");
                                    }
                                })
                                .map(drop),
                        );
                        has_sent_tx1.store(true, SeqCst);
                    }
                    future::ready(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client and checks that the light client can get transaction status
#[test]
fn test_tx_status_with_light_client() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10, 0);
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.1", KeyType::ED25519, "near.1");
        let transaction = SignedTransaction::send_money(
            1,
            "near.1".to_string(),
            "near.2".to_string(),
            &signer,
            10000,
            genesis_hash,
        );
        let tx_hash = transaction.get_hash();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let rpc_addrs_copy1 = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let signer_account_id = transaction_copy.transaction.signer_id.clone();
                let tx_hash_clone = tx_hash.clone();
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        let bytes = transaction_copy.try_to_vec().unwrap();
                        actix::spawn(
                            client
                                .broadcast_tx_async(to_base64(&bytes))
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| {
                                    assert_eq!(String::from(&tx_hash_clone), result)
                                })
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
                let client = new_client(&format!("http://{}", rpc_addrs_copy1[2].clone()));
                actix::spawn(
                    client
                        .tx(tx_hash_clone.to_string(), signer_account_id)
                        .map_err(|_| ())
                        .map_ok(move |result| {
                            if result.status == FinalExecutionStatus::SuccessValue("".to_string()) {
                                System::current().stop();
                            }
                        })
                        .map(drop),
                );
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client and checks that the light client can get transaction status
#[test]
fn test_tx_status_with_light_client1() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10, 0);
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.3", KeyType::ED25519, "near.3");
        let transaction = SignedTransaction::send_money(
            1,
            "near.3".to_string(),
            "near.0".to_string(),
            &signer,
            10000,
            genesis_hash,
        );
        let tx_hash = transaction.get_hash();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let rpc_addrs_copy1 = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let signer_account_id = transaction_copy.transaction.signer_id.clone();
                let tx_hash_clone = tx_hash.clone();
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        let bytes = transaction_copy.try_to_vec().unwrap();
                        actix::spawn(
                            client
                                .broadcast_tx_async(to_base64(&bytes))
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| {
                                    assert_eq!(String::from(&tx_hash_clone), result)
                                })
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
                let client = new_client(&format!("http://{}", rpc_addrs_copy1[2].clone()));
                actix::spawn(
                    client
                        .tx(tx_hash_clone.to_string(), signer_account_id)
                        .map_err(|_| ())
                        .map_ok(move |result| {
                            if result.status == FinalExecutionStatus::SuccessValue("".to_string()) {
                                System::current().stop();
                            }
                        })
                        .map(drop),
                );
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

#[test]
fn test_rpc_routing() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (_, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10, 0);
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        actix::spawn(
                            client
                                .query_by_path("account/near.2".to_string(), "".to_string())
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| match result.kind {
                                    QueryResponseKind::ViewAccount(account_view) => {
                                        assert_eq!(account_view.amount, TESTING_INIT_BALANCE);
                                        System::current().stop();
                                    }
                                    _ => panic!("wrong query response"),
                                })
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

/// When we call rpc to view an account that does not exist, an error should be routed back.
#[test]
fn test_rpc_routing_error() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (_, rpc_addrs, clients) = start_nodes(4, &dirs, 2, 2, 10, 0);
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    if res.unwrap().unwrap().header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                        actix::spawn(
                            client
                                .query_by_path("account/nonexistent".to_string(), "".to_string())
                                .map_err(|err| {
                                    println!("error: {}", err.to_string());
                                    System::current().stop();
                                })
                                .map_ok(|_| panic!("wrong query response"))
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

#[test]
fn test_get_validator_info_rpc() {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 1;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (_, rpc_addrs, clients) = start_nodes(1, &dirs, 1, 0, 10, 0);
        let view_client = clients[0].1.clone();

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
                    let res = res.unwrap().unwrap();
                    if res.header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[0]));
                        let block_hash = res.header.hash;
                        actix::spawn(
                            client
                                .validators(Some(BlockId::Hash(block_hash)))
                                .map_err(|err| panic_on_rpc_error!(err))
                                .map_ok(move |result| {
                                    assert_eq!(result.current_validators.len(), 1);
                                    assert!(result
                                        .current_validators
                                        .iter()
                                        .any(|r| r.account_id == "near.0".to_string()));
                                    System::current().stop();
                                })
                                .map(drop),
                        );
                    }
                    future::ready(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
    });
}

fn outcome_view_to_hashes(outcome: &ExecutionOutcomeView) -> Vec<CryptoHash> {
    let status = match &outcome.status {
        ExecutionStatusView::Unknown => PartialExecutionStatus::Unknown,
        ExecutionStatusView::SuccessValue(s) => {
            PartialExecutionStatus::SuccessValue(from_base64(s).unwrap())
        }
        ExecutionStatusView::Failure(_) => PartialExecutionStatus::Failure,
        ExecutionStatusView::SuccessReceiptId(id) => PartialExecutionStatus::SuccessReceiptId(*id),
    };
    let mut result = vec![hash(
        &(
            outcome.receipt_ids.clone(),
            outcome.gas_burnt,
            outcome.tokens_burnt,
            outcome.executor_id.clone(),
            status,
        )
            .try_to_vec()
            .expect("Failed to serialize"),
    )];
    for log in outcome.logs.iter() {
        result.push(hash(log.as_bytes()));
    }
    result
}

fn test_get_execution_outcome(is_tx_successful: bool) {
    init_integration_logger();
    heavy_test(|| {
        let system = System::new("NEAR");
        let num_nodes = 2;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("tx_propagation{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let (genesis_config, rpc_addrs, clients) = start_nodes(1, &dirs, 1, 1, 1000, 0);
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(genesis_config).hash();
        let signer = InMemorySigner::from_seed("near.0", KeyType::ED25519, "near.0");
        let transaction = if is_tx_successful {
            SignedTransaction::send_money(
                1,
                "near.0".to_string(),
                "near.1".to_string(),
                &signer,
                10000,
                genesis_hash,
            )
        } else {
            SignedTransaction::create_account(
                1,
                "near.0".to_string(),
                "near.1".to_string(),
                10,
                signer.public_key.clone(),
                &signer,
                genesis_hash,
            )
        };

        WaitOrTimeout::new(
            Box::new(move |_ctx| {
                let client = new_client(&format!("http://{}", rpc_addrs[0]));
                let bytes = transaction.try_to_vec().unwrap();
                let view_client1 = view_client.clone();
                actix::spawn(client.broadcast_tx_commit(to_base64(&bytes)).then(move |res| {
                    let final_transaction_outcome = match res {
                        Ok(outcome) => outcome,
                        Err(_) => return future::ready(()),
                    };
                    actix::spawn(delay_for(Duration::from_secs(1)).then(move |_| {
                        let mut futures = vec![];
                        for id in vec![TransactionOrReceiptId::Transaction {
                            transaction_hash: final_transaction_outcome.transaction_outcome.id,
                            sender_id: "near.0".to_string(),
                        }]
                        .into_iter()
                        .chain(
                            final_transaction_outcome.receipts_outcome.into_iter().map(|r| {
                                TransactionOrReceiptId::Receipt {
                                    receipt_id: r.id,
                                    receiver_id: "near.1".to_string(),
                                }
                            }),
                        ) {
                            let view_client2 = view_client1.clone();
                            let fut =
                                view_client1.send(GetExecutionOutcome { id }).then(move |res| {
                                    let execution_outcome_response = res.unwrap().unwrap();
                                    view_client2
                                        .send(GetBlock(BlockReference::BlockId(BlockId::Hash(
                                            execution_outcome_response.outcome_proof.block_hash,
                                        ))))
                                        .then(move |res| {
                                            let res = res.unwrap().unwrap();
                                            let mut outcome_with_id_to_hash =
                                                vec![execution_outcome_response.outcome_proof.id];
                                            outcome_with_id_to_hash.extend(outcome_view_to_hashes(
                                                &execution_outcome_response.outcome_proof.outcome,
                                            ));
                                            let chunk_outcome_root =
                                                compute_root_from_path_and_item(
                                                    &execution_outcome_response.outcome_proof.proof,
                                                    &outcome_with_id_to_hash,
                                                );
                                            assert!(verify_path(
                                                res.header.outcome_root,
                                                &execution_outcome_response.outcome_root_proof,
                                                &chunk_outcome_root
                                            ));
                                            future::ready(())
                                        })
                                });
                            futures.push(fut);
                        }
                        actix::spawn(join_all(futures).then(|_| {
                            System::current().stop();
                            future::ready(())
                        }));
                        future::ready(())
                    }));

                    future::ready(())
                }));
            }),
            100,
            20000,
        )
        .start();

        system.run().unwrap();
        clients.into_iter().for_each(|c| c.2.into_iter().for_each(|mut a| a.join().unwrap()));
    });
}

#[test]
fn test_get_execution_outcome_tx_success() {
    test_get_execution_outcome(true);
}

#[test]
fn test_get_execution_outcome_tx_failure() {
    test_get_execution_outcome(false);
}
