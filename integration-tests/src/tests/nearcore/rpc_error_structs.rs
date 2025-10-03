use std::ops::ControlFlow;
use std::str::FromStr;

use crate::tests::nearcore::node_cluster::NodeCluster;
use crate::utils::genesis_helpers::genesis_block;
use near_async::messaging::CanSendAsync;
use near_client::GetBlock;
use near_crypto::InMemorySigner;
use near_jsonrpc::client::new_client;
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_integration_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockId};

// Queries json-rpc block that doesn't exists
// Checks if the struct is expected and contains the proper data
#[tokio::test]
async fn slow_test_block_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = view_client.clone();
                async move {
                    // We are sending this tx unstop, just to get over the warm up period.
                    // Probably make sense to stop after 1 time though.
                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 1 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            match client
                                .block_by_id(BlockId::Height(block.header.height + 100))
                                .await
                            {
                                Err(err) => {
                                    let error_json = serde_json::to_value(err).unwrap();
                                    assert_eq!(
                                        error_json["name"],
                                        serde_json::json!("HANDLER_ERROR")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["name"],
                                        serde_json::json!("UNKNOWN_BLOCK")
                                    );
                                    return ControlFlow::Break(());
                                }
                                Ok(_) => panic!("The block mustn't be found"),
                            }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

// Queries json-rpc chunk that doesn't exists
// (random-ish chunk hash, we hope it won't happen in test case)
// Checks if the struct is expected and contains the proper data
#[tokio::test]
async fn slow_test_chunk_unknown_chunk_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = view_client.clone();
                async move {
                    // We are sending this tx unstop, just to get over the warm up period.
                    // Probably make sense to stop after 1 time though.
                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 1 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            match client
                                .chunk(near_jsonrpc::client::ChunkId::Hash(
                                    CryptoHash::from_str(
                                        "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu",
                                    )
                                    .unwrap(),
                                ))
                                .await
                            {
                                Err(err) => {
                                    let error_json = serde_json::to_value(err).unwrap();
                                    assert_eq!(
                                        error_json["name"],
                                        serde_json::json!("HANDLER_ERROR")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["name"],
                                        serde_json::json!("UNKNOWN_CHUNK")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["info"]["chunk_hash"],
                                        serde_json::json!(
                                            "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu"
                                        )
                                    );
                                    return ControlFlow::Break(());
                                }
                                Ok(_) => panic!("The chunk mustn't be found"),
                            }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

// Queries json-rpc EXPERIMENTAL_protocol_config that doesn't exists
// Checks if the struct is expected and contains the proper data
#[tokio::test]
async fn slow_test_protocol_config_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = view_client.clone();
                async move {
                    // We are sending this tx unstop, just to get over the warm up period.
                    // Probably make sense to stop after 1 time though.
                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 1 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            match client
                            .EXPERIMENTAL_protocol_config(
                                near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest {
                                    block_reference:
                                        near_primitives::types::BlockReference::BlockId(
                                            BlockId::Height(block.header.height + 100),
                                        ),
                                },
                            )
                            .await
                        {
                            Err(err) => {
                                let error_json = serde_json::to_value(err).unwrap();

                                assert_eq!(error_json["name"], serde_json::json!("HANDLER_ERROR"));
                                assert_eq!(
                                    error_json["cause"]["name"],
                                    serde_json::json!("UNKNOWN_BLOCK")
                                );
                                return ControlFlow::Break(());
                            }
                            Ok(_) => panic!("The block mustn't be found"),
                        }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

// Queries json-rpc gas_price that doesn't exists
// Checks if the struct is expected and contains the proper data
#[tokio::test]
async fn slow_test_gas_price_unknown_block_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = view_client.clone();
                async move {
                    // We are sending this tx unstop, just to get over the warm up period.
                    // Probably make sense to stop after 1 time though.
                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 1 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            match client
                                .gas_price(Some(BlockId::Height(block.header.height + 100)))
                                .await
                            {
                                Err(err) => {
                                    let error_json = serde_json::to_value(err).unwrap();

                                    assert_eq!(
                                        error_json["name"],
                                        serde_json::json!("HANDLER_ERROR")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["name"],
                                        serde_json::json!("UNKNOWN_BLOCK")
                                    );
                                    return ControlFlow::Break(());
                                }
                                Ok(_) => panic!("The block mustn't be found"),
                            }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

// Queries json-rpc EXPERIMENTAL_receipt that doesn't exists
// Checks if the struct is expected and contains the proper data
#[tokio::test]
async fn slow_test_receipt_id_unknown_receipt_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = view_client.clone();
                async move {
                    // We are sending this tx unstop, just to get over the warm up period.
                    // Probably make sense to stop after 1 time though.
                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 1 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            match client
                            .EXPERIMENTAL_receipt(
                                near_jsonrpc_primitives::types::receipts::RpcReceiptRequest {
                                    receipt_reference:
                                        near_jsonrpc_primitives::types::receipts::ReceiptReference {
                                            receipt_id: CryptoHash::from_str(
                                                "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu",
                                            )
                                            .unwrap(),
                                        },
                                },
                            )
                            .await
                        {
                            Err(err) => {
                                let error_json = serde_json::to_value(err).unwrap();

                                assert_eq!(error_json["name"], serde_json::json!("HANDLER_ERROR"));
                                assert_eq!(
                                    error_json["cause"]["name"],
                                    serde_json::json!("UNKNOWN_RECEIPT")
                                );
                                assert_eq!(
                                    error_json["cause"]["info"]["receipt_id"],
                                    serde_json::json!(
                                        "3tMcx4KU2KvkwJPMWPXqK2MUU1FDVbigPFNiAeuVa7Tu"
                                    )
                                );
                                return ControlFlow::Break(());
                            }
                            Ok(_) => panic!("The block mustn't be found"),
                        }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

/// Starts 2 validators and 2 light clients (not tracking anything).
/// Sends tx to first light client through `broadcast_tx_commit` and checks that the transaction has failed.
/// Checks if the struct is expected and contains the proper data
#[tokio::test]
#[ignore = "Invalid test setup. broadcast_tx_commit times out because we haven't implemented forwarding logic. Fix and reenable."]
async fn test_tx_invalid_tx_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(4)
        .set_num_validator_seats(2)
        .set_num_lightclients(2)
        .set_epoch_length(1000)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|genesis, rpc_addrs, clients| async move {
            let view_client = clients[0].1.clone();

            let genesis_hash = *genesis_block(&genesis).hash();
            let signer = InMemorySigner::test_signer(&"near.5".parse().unwrap());
            let transaction = SignedTransaction::send_money(
                1,
                "near.5".parse().unwrap(),
                "near.2".parse().unwrap(),
                &signer,
                Balance::from_yoctonear(10000),
                genesis_hash,
            );

            wait_or_timeout(100, 40000, move || {
                let rpc_addrs_copy = rpc_addrs.clone();
                let transaction_copy = transaction.clone();
                let view_client = view_client.clone();
                async move {
                    let tx_hash = transaction_copy.get_hash();

                    if let Ok(Ok(block)) = view_client.send_async(GetBlock::latest()).await {
                        if block.header.height > 10 {
                            let client = new_client(&format!("http://{}", rpc_addrs_copy[2]));
                            let bytes = borsh::to_vec(&transaction_copy).unwrap();
                            match client.broadcast_tx_commit(to_base64(&bytes)).await {
                                Err(err) => {
                                    let error_json = serde_json::to_value(err).unwrap();

                                    assert_eq!(
                                        error_json["name"],
                                        serde_json::json!("HANDLER_ERROR")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["name"],
                                        serde_json::json!("REQUEST_ROUTED")
                                    );
                                    assert_eq!(
                                        error_json["cause"]["info"]["transaction_hash"],
                                        serde_json::json!(tx_hash)
                                    );
                                    return ControlFlow::Break(());
                                }
                                Ok(_) => panic!("The transaction mustn't succeed"),
                            }
                        }
                    }
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        })
        .await;
}

#[tokio::test]
async fn test_query_rpc_account_view_unknown_block_must_return_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster
        .run_and_then_shutdown(|_, rpc_addrs, _| async move {
            let client = new_client(&format!("http://{}", rpc_addrs[0]));
            let query_response = client
                .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                    block_reference: near_primitives::types::BlockReference::BlockId(
                        BlockId::Height(1),
                    ),
                    request: near_primitives::views::QueryRequest::ViewAccount {
                        account_id: "near.0".parse().unwrap(),
                    },
                })
                .await;

            let error = match query_response {
                Ok(result) => panic!("expected error but received Ok: {:?}", result.kind),
                Err(err) => serde_json::to_value(err).unwrap(),
            };

            assert_eq!(error["cause"]["name"], serde_json::json!("UNKNOWN_BLOCK"),);
        })
        .await;
}
