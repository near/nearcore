use std::convert::TryFrom;

use actix::{Actor, System};
use futures::{future, FutureExt};

use near_chain_configs::PROTOCOL_VERSION;
use near_crypto::{KeyType, PublicKey, Signature};
use near_jsonrpc::client::new_client;
use near_jsonrpc_client::ChunkId;
use near_network::test_utils::WaitOrTimeout;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::rpc::{RpcGenesisRecordsRequest, RpcPagination, RpcQueryRequest};
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::{BlockId, BlockIdOrFinality, Finality, ShardId};
use near_primitives::views::{QueryRequest, QueryResponseKind};

#[macro_use]
pub mod test_utils;

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
        assert_eq!(block.author, "test1");
        assert_eq!(block.header.height, 0);
        assert_eq!(block.header.epoch_id.0.as_ref(), &[0; 32]);
        assert_eq!(block.header.hash.0.as_ref().len(), 32);
        assert_eq!(block.header.prev_hash.0.as_ref(), &[0; 32]);
        assert_eq!(
            block.header.prev_state_root,
            CryptoHash::try_from("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap()
        );
        assert!(block.header.timestamp > 0);
        assert_eq!(block.header.validator_proposals.len(), 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
        let same_block = client.block_by_id(BlockId::Hash(block.header.hash)).await.unwrap();
        assert_eq!(block.header.height, 0);
        assert_eq!(same_block.header.height, 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_query() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block_response1 =
            client.block(BlockIdOrFinality::BlockId(BlockId::Height(0))).await.unwrap();
        let block_response2 = client
            .block(BlockIdOrFinality::BlockId(BlockId::Hash(block_response1.header.hash)))
            .await
            .unwrap();
        let block_response3 = client.block(BlockIdOrFinality::latest()).await.unwrap();
        for block in [block_response1, block_response2, block_response3].iter() {
            assert_eq!(block.author, "test1");
            assert_eq!(block.header.height, 0);
            assert_eq!(block.header.epoch_id.0.as_ref(), &[0; 32]);
            assert_eq!(block.header.hash.0.as_ref().len(), 32);
            assert_eq!(block.header.prev_hash.0.as_ref(), &[0; 32]);
            assert_eq!(
                block.header.prev_state_root,
                CryptoHash::try_from("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap()
            );
            assert!(block.header.timestamp > 0);
            assert_eq!(block.header.validator_proposals.len(), 0);
        }
        // no doomslug final or nfg final block
        assert!(client.block(BlockIdOrFinality::Finality(Finality::DoomSlug)).await.is_err());
        assert!(client.block(BlockIdOrFinality::Finality(Finality::Final)).await.is_err());
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk = client
            .chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::from(0u64)))
            .await
            .unwrap();
        assert_eq!(chunk.author, "test2");
        assert_eq!(chunk.header.balance_burnt, 0);
        assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.encoded_length, 8);
        assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
        assert_eq!(chunk.header.gas_limit, 1000000);
        assert_eq!(chunk.header.gas_used, 0);
        assert_eq!(chunk.header.height_created, 0);
        assert_eq!(chunk.header.height_included, 0);
        assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
        assert_eq!(chunk.header.rent_paid, 0);
        assert_eq!(chunk.header.shard_id, 0);
        assert!(if let Signature::ED25519(_) = chunk.header.signature { true } else { false });
        assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
        assert_eq!(chunk.header.validator_proposals, vec![]);
        assert_eq!(chunk.header.validator_reward, 0);
        let same_chunk = client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).await.unwrap();
        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_invalid_shard_id() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk = client.chunk(ChunkId::BlockShardId(BlockId::Height(0), 100)).await;
        match chunk {
            Ok(_) => panic!("should result in an error"),
            Err(e) => {
                let s = serde_json::to_string(&e.data.unwrap()).unwrap();
                assert!(s.starts_with("\"Shard id 100 does not exist"));
            }
        }
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;
        let query_response =
            client.query_by_path("account/test".to_string(), "".to_string()).await.unwrap();
        assert_eq!(query_response.block_height, 0);
        assert_eq!(query_response.block_hash, block_hash);
        let account_info = if let QueryResponseKind::ViewAccount(account) = query_response.kind {
            account
        } else {
            panic!("queried account, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(account_info.amount, 0);
        assert_eq!(account_info.code_hash.as_ref(), &[0; 32]);
        assert_eq!(account_info.locked, 0);
        assert_eq!(account_info.storage_paid_at, 0);
        assert_eq!(account_info.storage_usage, 0);
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;
        let query_response_1 = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::latest(),
                request: QueryRequest::ViewAccount { account_id: "test".to_string() },
            })
            .await
            .unwrap();
        let query_response_2 = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::BlockId(BlockId::Height(0)),
                request: QueryRequest::ViewAccount { account_id: "test".to_string() },
            })
            .await
            .unwrap();
        let query_response_3 = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::BlockId(BlockId::Hash(block_hash)),
                request: QueryRequest::ViewAccount { account_id: "test".to_string() },
            })
            .await
            .unwrap();
        for query_response in [query_response_1, query_response_2, query_response_3].iter() {
            assert_eq!(query_response.block_height, 0);
            assert_eq!(query_response.block_hash, block_hash);
            let account_info = if let QueryResponseKind::ViewAccount(ref account) =
                query_response.kind
            {
                account
            } else {
                panic!("queried account, but received something else: {:?}", query_response.kind);
            };
            assert_eq!(account_info.amount, 0);
            assert_eq!(account_info.code_hash.as_ref(), &[0; 32]);
            assert_eq!(account_info.locked, 0);
            assert_eq!(account_info.storage_paid_at, 0);
            assert_eq!(account_info.storage_usage, 0);
        }

        let non_finalized_query_response_1 = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::Finality(Finality::DoomSlug),
                request: QueryRequest::ViewAccount { account_id: "test".to_string() },
            })
            .await;
        let non_finalized_query_response_2 = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::Finality(Finality::Final),
                request: QueryRequest::ViewAccount { account_id: "test".to_string() },
            })
            .await;
        for query_response in
            [non_finalized_query_response_1, non_finalized_query_response_2].iter()
        {
            let rpc_error = if let Err(err) = query_response {
                err
            } else {
                panic!("the error is expected since the latest block is not finalized yet");
            };
            assert_eq!(rpc_error.code, -32000);
            assert!(rpc_error.data.as_ref().unwrap().as_str().unwrap().contains("DB Not Found"));
        }
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response =
            client.query_by_path("access_key/test".to_string(), "".to_string()).await.unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, PublicKey::empty(KeyType::ED25519));
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::latest(),
                request: QueryRequest::ViewAccessKeyList { account_id: "test".to_string() },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, PublicKey::empty(KeyType::ED25519));
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query_by_path(
                "access_key/test/ed25519:23vYngy8iL7q94jby3gszBnZ9JptpMf5Hgf7KVVa2yQ2".to_string(),
                "".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::latest(),
                request: QueryRequest::ViewAccessKey {
                    account_id: "test".to_string(),
                    public_key: PublicKey::try_from(
                        "ed25519:23vYngy8iL7q94jby3gszBnZ9JptpMf5Hgf7KVVa2yQ2",
                    )
                    .unwrap(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query state.
#[test]
fn test_query_state() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::latest(),
                request: QueryRequest::ViewState {
                    account_id: "test".to_string(),
                    prefix: vec![].into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let state = if let QueryResponseKind::ViewState(state) = query_response.kind {
            state
        } else {
            panic!("queried state, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(state.values.len(), 0);
    });
}

/// Connect to json rpc and call function
#[test]
fn test_query_call_function() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(RpcQueryRequest {
                block_id_or_finality: BlockIdOrFinality::latest(),
                request: QueryRequest::CallFunction {
                    account_id: "test".to_string(),
                    method_name: "method".to_string(),
                    args: vec![].into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let call_result = if let QueryResponseKind::CallResult(call_result) = query_response.kind {
            call_result
        } else {
            panic!(
                "expected a call function result, but received something else: {:?}",
                query_response.kind
            );
        };
        assert_eq!(call_result.result.len(), 0);
        assert_eq!(call_result.logs.len(), 0);
    });
}

/// Retrieve client status via JSON RPC.
#[test]
fn test_status() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        assert_eq!(status.chain_id, "unittest");
        assert_eq!(status.sync_info.latest_block_height, 0);
        assert_eq!(status.sync_info.syncing, false);
    });
}

/// Retrieve client status failed.
#[test]
fn test_status_fail() {
    init_test_logger();

    System::run(|| {
        let (_, addr) = test_utils::start_all(test_utils::NodeType::NonValidator);

        let client = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.health().then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::ready(())
                }));
            }),
            100,
            10000,
        )
        .start();
    })
    .unwrap();
}

/// Check health fails when node is absent.
#[test]
fn test_health_fail() {
    init_test_logger();

    System::run(|| {
        let client = new_client(&"http://127.0.0.1:12322/health");
        actix::spawn(client.health().then(|res| {
            assert!(res.is_err());
            System::current().stop();
            future::ready(())
        }));
    })
    .unwrap();
}

/// Health fails when node doesn't produce block for period of time.
#[test]
fn test_health_fail_no_blocks() {
    init_test_logger();

    System::run(|| {
        let (_, addr) = test_utils::start_all(test_utils::NodeType::NonValidator);

        let client = new_client(&format!("http://{}", addr));
        WaitOrTimeout::new(
            Box::new(move |_| {
                actix::spawn(client.health().then(|res| {
                    if res.is_err() {
                        System::current().stop();
                    }
                    future::ready(())
                }));
            }),
            300,
            10000,
        )
        .start();
    })
    .unwrap();
}

/// Retrieve client health.
#[test]
fn test_health_ok() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let health = client.health().await;
        assert!(health.is_ok());
    });
}

/// Retrieve genesis config via JSON RPC.
/// WARNING: Be mindful about changing genesis structure as it is part of the public protocol!
#[test]
fn test_genesis_config() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let genesis_config = client.EXPERIMENTAL_genesis_config().await.unwrap();
        assert_eq!(genesis_config["config_version"].as_u64().unwrap(), 1);
        assert_eq!(genesis_config["protocol_version"].as_u64().unwrap(), PROTOCOL_VERSION as u64);
        assert!(!genesis_config["chain_id"].as_str().unwrap().is_empty());
        assert!(!genesis_config.as_object().unwrap().contains_key("records"));
    });
}

/// Retrieve genesis records via JSON RPC.
#[test]
fn test_genesis_records() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let genesis_records = client
            .EXPERIMENTAL_genesis_records(RpcGenesisRecordsRequest {
                pagination: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(genesis_records.records.len(), 100);
        let first100_records = genesis_records.records.to_vec();

        let second_genesis_record = client
            .EXPERIMENTAL_genesis_records(RpcGenesisRecordsRequest {
                pagination: RpcPagination { offset: 1, limit: 1 },
            })
            .await
            .unwrap();
        assert_eq!(second_genesis_record.records.len(), 1);

        assert_eq!(
            serde_json::to_value(&first100_records[1]).unwrap(),
            serde_json::to_value(&second_genesis_record.records[0]).unwrap()
        );
    });
}

/// Check invalid arguments to genesis records via JSON RPC.
#[test]
fn test_invalid_genesis_records_arguments() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let genesis_records_response = client
            .EXPERIMENTAL_genesis_records(RpcGenesisRecordsRequest {
                pagination: RpcPagination { offset: 1, limit: 101 },
            })
            .await;
        let validation_error = genesis_records_response.err().unwrap();
        assert_eq!(validation_error.code, -32_602);
        assert_eq!(validation_error.message, "Invalid params");
        assert_eq!(
            validation_error.data.unwrap(),
            serde_json::json!({
                "pagination": {
                    "limit": [
                        {
                            "code": "range",
                            "message": null,
                            "params": {
                                "max": 100.0,
                                "value": 101,
                                "min": 1.0,
                            }
                        }
                    ]
                }
            })
        );
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price = client.gas_price(Some(BlockId::Height(0))).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block(BlockIdOrFinality::BlockId(BlockId::Height(0))).await.unwrap();
        let gas_price = client.gas_price(Some(BlockId::Hash(block.header.hash))).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price = client.gas_price(None).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}
