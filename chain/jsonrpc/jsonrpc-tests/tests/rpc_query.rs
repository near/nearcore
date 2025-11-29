use std::ops::ControlFlow;

use near_chain_configs::test_utils::TESTING_INIT_BALANCE;
use near_primitives::action::GlobalContractDeployMode;
use near_primitives::transaction::SignedTransaction;
use reqwest::StatusCode;

use near_crypto::{InMemorySigner, Signature};
use near_jsonrpc::client::{ChunkId, JsonRpcClient, new_client};
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::errors::RpcErrorKind;
use near_jsonrpc_primitives::types::call_function::RpcCallFunctionRequest;
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest;
use near_jsonrpc_primitives::types::view_access_key::RpcViewAccessKeyRequest;
use near_jsonrpc_primitives::types::view_access_key_list::RpcViewAccessKeyListRequest;
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_jsonrpc_primitives::types::view_code::RpcViewCodeRequest;
use near_jsonrpc_primitives::types::view_gas_key::RpcViewGasKeyRequest;
use near_jsonrpc_primitives::types::view_gas_key_list::RpcViewGasKeyListRequest;
use near_jsonrpc_primitives::types::view_state::RpcViewStateRequest;
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockId, BlockReference, EpochId, Gas, ShardId, SyncCheckpoint,
};
use near_primitives::views::{FinalExecutionStatus, QueryRequest};
use serde_json::Value;

use near_jsonrpc_tests::{NodeType, create_test_setup_with_node_type};

/// Retrieve blocks via json rpc
#[tokio::test]
async fn test_block_by_id_height() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
    assert_eq!(block.author, "test1");
    assert_eq!(block.header.height, 0);
    assert_eq!(block.header.epoch_id.0.as_ref(), &[0; 32]);
    assert_eq!(block.header.hash.0.as_ref().len(), 32);
    assert_eq!(block.header.prev_hash.0.as_ref(), &[0; 32]);
    assert_eq!(
        block.header.prev_state_root.to_string(),
        "CfKJ4CZqCCtLAESUk1RnWSrXvwenMVooWYrvoMsDrCAH"
    );
    assert!(block.header.timestamp > 0);
    assert_eq!(block.header.validator_proposals.len(), 0);
}

/// Retrieve blocks via json rpc
#[tokio::test]
async fn test_block_by_id_hash() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
    let same_block = client.block_by_id(BlockId::Hash(block.header.hash)).await.unwrap();
    assert_eq!(block.header.height, 0);
    assert_eq!(same_block.header.height, 0);
}

/// Retrieve blocks via json rpc
#[tokio::test]
async fn test_block_query() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let block_response1 = client.block(BlockReference::BlockId(BlockId::Height(0))).await.unwrap();
    let block_response2 = client
        .block(BlockReference::BlockId(BlockId::Hash(block_response1.header.hash)))
        .await
        .unwrap();
    let block_response3 = client.block(BlockReference::latest()).await.unwrap();
    let block_response4 =
        client.block(BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis)).await.unwrap();
    let block_response5 = client
        .block(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable))
        .await
        .unwrap();
    // Verify blocks 1, 2, 4, 5 are at height 0 (genesis blocks)
    for (i, block) in
        [&block_response1, &block_response2, &block_response4, &block_response5].iter().enumerate()
    {
        assert_eq!(block.author, "test1");
        assert_eq!(block.header.height, 0, "Block {} should be at height 0", i);
        assert_eq!(block.header.epoch_id.as_ref(), &[0; 32]);
        assert_eq!(block.header.hash.as_ref().len(), 32);
        assert_eq!(block.header.prev_hash.as_ref(), &[0; 32]);
        assert_eq!(
            block.header.prev_state_root.to_string(),
            "CfKJ4CZqCCtLAESUk1RnWSrXvwenMVooWYrvoMsDrCAH"
        );
        assert!(block.header.timestamp > 0);
        assert_eq!(block.header.validator_proposals.len(), 0);
    }

    // Verify latest block (block_response3) - might be at higher height due to block production
    assert_eq!(block_response3.author, "test1");
    assert!(block_response3.header.height < 100); // Should be reasonable for test
    assert_eq!(block_response3.header.epoch_id.as_ref(), &[0; 32]);
    assert_eq!(block_response3.header.hash.as_ref().len(), 32);
    assert!(block_response3.header.timestamp > 0);
    assert_eq!(block_response3.header.validator_proposals.len(), 0);
}

/// Retrieve chunk via json rpc
#[tokio::test]
async fn test_chunk_by_hash() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let chunk =
        client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::new(0))).await.unwrap();
    assert_eq!(chunk.author, "test1");
    assert!(chunk.header.balance_burnt.is_zero());
    assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
    assert_eq!(chunk.header.encoded_length, 8);
    assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
    assert_eq!(chunk.header.gas_limit, Gas::from_teragas(1000));
    assert_eq!(chunk.header.gas_used, Gas::ZERO);
    assert_eq!(chunk.header.height_created, 0);
    assert_eq!(chunk.header.height_included, 0);
    assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
    assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
    assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
    assert!(chunk.header.rent_paid.is_zero());
    assert_eq!(chunk.header.shard_id, ShardId::new(0));
    assert!(if let Signature::ED25519(_) = chunk.header.signature { true } else { false });
    assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
    assert_eq!(chunk.header.validator_proposals, vec![]);
    assert!(chunk.header.validator_reward.is_zero());
    let same_chunk = client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).await.unwrap();
    assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
}

/// Retrieve chunk via json rpc
#[tokio::test]
async fn test_chunk_invalid_shard_id() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let chunk = client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::new(100))).await;
    match chunk {
        Ok(_) => panic!("should result in an error"),
        Err(e) => {
            let s = serde_json::to_string(&e.data.unwrap()).unwrap();
            assert!(s.starts_with("\"Shard id 100 does not exist"));
        }
    }
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[tokio::test]
async fn test_query_by_path_account() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);
    let query_response =
        client.query_by_path("account/test".to_string(), "".to_string()).await.unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    // Block hash may vary due to continued block production
    assert_ne!(query_response.block_hash, CryptoHash::default());
    let account_info = if let QueryResponseKind::ViewAccount(account) = query_response.kind {
        account
    } else {
        panic!("queried account, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(account_info.amount, TESTING_INIT_BALANCE);
    assert_eq!(account_info.code_hash, CryptoHash::default());
    assert!(account_info.locked.is_zero());
    assert_eq!(account_info.storage_paid_at, 0);
    assert_eq!(account_info.global_contract_hash, None);
    assert_eq!(account_info.global_contract_account_id, None);
}

/// Connect to json rpc and query account info.
#[tokio::test]
async fn test_query_account() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let status = client.status().await.unwrap();
    let block_hash = status.sync_info.latest_block_hash;
    let query_response_1 = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccount { account_id: "test1".parse().unwrap() },
        })
        .await
        .unwrap();
    let query_response_2 = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::BlockId(BlockId::Height(0)),
            request: QueryRequest::ViewAccount { account_id: "test1".parse().unwrap() },
        })
        .await
        .unwrap();
    let query_response_3 = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
            request: QueryRequest::ViewAccount { account_id: "test1".parse().unwrap() },
        })
        .await
        .unwrap();
    for query_response in &[query_response_1, query_response_2, query_response_3] {
        // Block height may vary due to block production in tests
        assert!(query_response.block_height < 100);
        // Block hash may vary due to continued block production
        assert_ne!(query_response.block_hash, CryptoHash::default());
        let account_info = if let QueryResponseKind::ViewAccount(ref account) = query_response.kind
        {
            account
        } else {
            panic!("queried account, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(
            account_info.amount.checked_add(account_info.locked).unwrap(),
            TESTING_INIT_BALANCE
        );
        assert_eq!(account_info.code_hash, CryptoHash::default());
        assert_ne!(account_info.locked, Balance::ZERO);
        assert_eq!(account_info.storage_paid_at, 0);
        assert_eq!(account_info.global_contract_hash, None);
        assert_eq!(account_info.global_contract_account_id, None);
    }
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[tokio::test]
async fn test_query_by_path_access_keys() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);
    let query_response =
        client.query_by_path("access_key/test1".to_string(), "".to_string()).await.unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind {
        access_keys
    } else {
        panic!("queried access keys, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(access_keys.keys.len(), 1);
    assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
    assert_eq!(access_keys.keys[0].public_key, signer.public_key());
}

// here
/// Connect to json rpc and query account info.
#[tokio::test]
async fn test_query_access_keys() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let query_response = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccessKeyList { account_id: "test1".parse().unwrap() },
        })
        .await
        .unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind {
        access_keys
    } else {
        panic!("queried access keys, but received something else: {:?}", query_response.kind);
    };
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    assert_eq!(access_keys.keys.len(), 1);
    assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
    assert_eq!(access_keys.keys[0].public_key, signer.public_key());
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[tokio::test]
async fn test_query_by_path_access_key() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);
    let query_response = client
        .query_by_path(format!("access_key/test1/{}", signer.public_key()), "".to_string())
        .await
        .unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
        access_keys
    } else {
        panic!("queried access keys, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(access_key.nonce, 0);
    assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
}

/// Connect to json rpc and query account info.
#[tokio::test]
async fn test_query_access_key() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);
    let query_response = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccessKey {
                account_id: account.clone(),
                public_key: signer.public_key(),
            },
        })
        .await
        .unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
        access_keys
    } else {
        panic!("queried access keys, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(access_key.nonce, 0);
    assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
}

/// Connect to json rpc and query state.
#[tokio::test]
async fn test_query_state() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let query_response = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewState {
                account_id: "test1".parse().unwrap(),
                prefix: vec![].into(),
                include_proof: false,
            },
        })
        .await
        .unwrap();
    // Block height may vary due to block production in tests
    assert!(query_response.block_height < 100);
    let state = if let QueryResponseKind::ViewState(state) = query_response.kind {
        state
    } else {
        panic!("queried state, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(state.values.len(), 0);
}

/// Connect to json rpc and call function
#[tokio::test]
// TODO(spice): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn test_query_call_function() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    deploy_contract(&client, &account, code.clone()).await;

    let query_response = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::CallFunction {
                account_id: "test1".parse().unwrap(),
                method_name: "run_test".to_string(),
                args: vec![].into(),
            },
        })
        .await
        .unwrap();
    let call_result = if let QueryResponseKind::CallResult(call_result) = query_response.kind {
        call_result
    } else {
        panic!(
            "expected a call function result, but received something else: {:?}",
            query_response.kind
        );
    };
    assert_eq!(call_result.result, 10i32.to_le_bytes());
    assert_eq!(call_result.logs.len(), 0);
}

/// query contract code
#[tokio::test]
// TODO(spice): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn test_query_contract_code() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    deploy_contract(&client, &account, code.clone()).await;

    let query_response = client
        .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewCode { account_id: account.clone() },
        })
        .await
        .unwrap();
    let response_code = if let QueryResponseKind::ViewCode(code) = query_response.kind {
        code
    } else {
        panic!("queried code, but received something else: {:?}", query_response.kind);
    };
    assert_eq!(response_code.code, code);
    assert_eq!(response_code.hash, CryptoHash::hash_bytes(&code));
}

async fn deploy_contract(client: &JsonRpcClient, account: &AccountId, code: Vec<u8>) {
    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&account);
    let tx = SignedTransaction::deploy_contract(1, &account, code, &signer, block_hash);
    let bytes = borsh::to_vec(&tx).unwrap();
    let result =
        client.broadcast_tx_commit(near_primitives::serialize::to_base64(&bytes)).await.unwrap();
    assert_eq!(
        result.final_execution_outcome.unwrap().into_outcome().status,
        FinalExecutionStatus::SuccessValue(Vec::new())
    );
}

/// Retrieve client status via JSON RPC.
#[tokio::test]
async fn test_status() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let status = client.status().await.unwrap();
    assert_eq!(status.chain_id, "unittest");
    // Height may vary depending on block production, so we just check that we get a valid response
    assert!(status.sync_info.latest_block_height < 100); // Should be reasonable for test
    assert_eq!(status.sync_info.syncing, false);
    assert_eq!(status.sync_info.epoch_id, Some(EpochId::default()));
    assert_eq!(status.sync_info.epoch_start_height, Some(0));
}

/// Retrieve client status failed.
#[tokio::test]
async fn test_status_fail() {
    init_test_logger();
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    // Stop the actor system to simulate failure
    setup.actor_system.as_ref().unwrap().stop();

    wait_or_timeout(100, 10000, || async {
        let res = client.health().await;
        if res.is_err() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .await
    .unwrap();
}

/// Check health fails when node is absent.
#[tokio::test]
async fn test_health_fail() {
    init_test_logger();

    // Test with a non-existent server address
    let client = new_client("http://127.0.0.1:12322/health");
    let res = client.health().await;
    assert!(res.is_err());
}

/// Health fails when node doesn't produce block for period of time.
#[tokio::test]
async fn test_health_fail_no_blocks() {
    init_test_logger();
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    wait_or_timeout(300, 10000, || async {
        let res = client.health().await;
        if res.is_err() {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .await
    .unwrap();
}

/// Retrieve client health.
#[tokio::test]
async fn test_health_ok() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let health = client.health().await;
    assert_eq!(health, Ok(()));
}

#[tokio::test]
async fn test_validators_ordered() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let validators = client
        .EXPERIMENTAL_validators_ordered(RpcValidatorsOrderedRequest { block_id: None })
        .await
        .unwrap();
    assert_eq!(
        validators.into_iter().map(|v| v.take_account_id()).collect::<Vec<_>>(),
        vec!["test1"]
    );
}

/// Retrieve genesis config via JSON RPC.
/// WARNING: Be mindful about changing genesis structure as it is part of the public protocol!
#[tokio::test]
async fn test_genesis_config() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let genesis_config = client.genesis_config().await.unwrap();
    if !cfg!(feature = "nightly") {
        assert_eq!(
            genesis_config["protocol_version"].as_u64().unwrap(),
            near_primitives::version::PROTOCOL_VERSION as u64
        );
    }
    assert!(!genesis_config["chain_id"].as_str().unwrap().is_empty());
    assert!(!genesis_config.as_object().unwrap().contains_key("records"));
}

/// Retrieve gas price
#[tokio::test]
async fn test_gas_price_by_height() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let gas_price = client.gas_price(Some(BlockId::Height(0))).await.unwrap();
    assert!(gas_price.gas_price > Balance::ZERO);
}

/// Retrieve gas price
#[tokio::test]
async fn test_gas_price_by_hash() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let block = client.block(BlockReference::BlockId(BlockId::Height(0))).await.unwrap();
    let gas_price = client.gas_price(Some(BlockId::Hash(block.header.hash))).await.unwrap();
    assert!(gas_price.gas_price > Balance::ZERO);
}

/// Retrieve gas price
#[tokio::test]
async fn test_gas_price() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let gas_price = client.gas_price(None).await.unwrap();
    assert!(gas_price.gas_price > Balance::ZERO);
}

#[tokio::test]
async fn test_invalid_methods() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let method_names = vec![
        serde_json::json!("\u{0}\u{0}\u{0}k\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}SRP"),
        serde_json::json!(null),
        serde_json::json!(true),
        serde_json::json!(false),
        serde_json::json!(0),
        serde_json::json!(""),
    ];

    for method_name in method_names {
        let json = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": &method_name,
            "params": serde_json::json!([]),
        });
        let response = client
            .client
            .post(&client.server_addr)
            .header("Content-Type", "application/json")
            .json(&json)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response =
            serde_json::from_value::<serde_json::Value>(response.json().await.unwrap()).unwrap();

        assert!(
            response["error"] != serde_json::json!(null),
            "Invalid method {:?} must return error",
            method_name
        );
    }
}

#[tokio::test]
async fn test_parse_error_status_code() {
    // cspell:ignore badtx frolik
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "tx",
        "params": serde_json::json!({
            "tx": "badtx",
            "sender_account_id": "frolik.near"
        }),
    });

    let response = &mut client
        .client
        .post(&client.server_addr)
        .header("Content-Type", "application/json")
        .json(&json)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn slow_test_bad_handler_error_status_code() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "tx",
        "params": serde_json::json!({
            "tx_hash": CryptoHash::new().to_string(),
            "sender_account_id": "frolik.near"
        }),
    });

    let response = &mut client
        .client
        .post(&client.server_addr)
        .header("Content-Type", "application/json")
        .json(&json)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}

#[tokio::test]
async fn test_good_handler_error_status_code() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "EXPERIMENTAL_receipt",
        "params": serde_json::json!({"receipt_id": CryptoHash::new().to_string()})
    });

    let response = &mut client
        .client
        .post(&client.server_addr)
        .header("Content-Type", "application/json")
        .json(&json)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_chunk_with_object_in_params() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let chunk =
        client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::new(0))).await.unwrap();
    assert_eq!(chunk.author, "test1");
    assert!(chunk.header.balance_burnt.is_zero());
    assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
    assert_eq!(chunk.header.encoded_length, 8);
    assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
    assert_eq!(chunk.header.gas_limit, Gas::from_teragas(1000));
    assert_eq!(chunk.header.gas_used, Gas::ZERO);
    assert_eq!(chunk.header.height_created, 0);
    assert_eq!(chunk.header.height_included, 0);
    assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
    assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
    assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
    assert!(chunk.header.rent_paid.is_zero());
    assert_eq!(chunk.header.shard_id, ShardId::new(0));
    assert!(if let Signature::ED25519(_) = chunk.header.signature { true } else { false });
    assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
    assert_eq!(chunk.header.validator_proposals, vec![]);
    assert!(chunk.header.validator_reward.is_zero());
    let same_chunk = client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).await.unwrap();
    assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
}

#[tokio::test]
// TODO(spice): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn test_query_global_contract_code_by_hash() {
    test_query_global_contract_code(GlobalContractDeployMode::CodeHash).await;
}

#[tokio::test]
// TODO(spice): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn test_query_global_contract_code_by_account_id() {
    test_query_global_contract_code(GlobalContractDeployMode::AccountId).await;
}

async fn test_query_global_contract_code(deploy_mode: GlobalContractDeployMode) {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    let code_hash = CryptoHash::hash_bytes(&code);
    deploy_global_contract(&client, &account, code.clone(), deploy_mode.clone()).await;

    // Global contract distribution takes time, so we might not be able to query the contract
    // immediately after broadcast_tx_commit.
    wait_or_timeout(100, 10000, || async {
        let request = match deploy_mode {
            GlobalContractDeployMode::CodeHash => {
                QueryRequest::ViewGlobalContractCode { code_hash }
            }
            GlobalContractDeployMode::AccountId => {
                QueryRequest::ViewGlobalContractCodeByAccountId { account_id: account.clone() }
            }
        };
        let query_res = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request,
            })
            .await;

        let Ok(query_response) = query_res else {
            return ControlFlow::Continue(());
        };

        let response_code = if let QueryResponseKind::ViewCode(code) = query_response.kind {
            code
        } else {
            panic!("queried code, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(response_code.code, code);
        assert_eq!(response_code.hash, code_hash);
        ControlFlow::Break(())
    })
    .await
    .unwrap();
}

async fn deploy_global_contract(
    client: &JsonRpcClient,
    account: &AccountId,
    code: Vec<u8>,
    deploy_mode: GlobalContractDeployMode,
) {
    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&account);
    let tx = SignedTransaction::deploy_global_contract(
        1,
        account.clone(),
        code,
        &signer,
        block_hash,
        deploy_mode,
    );
    let bytes = borsh::to_vec(&tx).unwrap();
    let result =
        client.broadcast_tx_commit(near_primitives::serialize::to_base64(&bytes)).await.unwrap();
    assert_eq!(
        result.final_execution_outcome.unwrap().into_outcome().status,
        FinalExecutionStatus::SuccessValue(Vec::new())
    );
}

// ==================== EXPERIMENTAL query methods tests ====================

fn error_name_and_info<'a>(err: &'a RpcError) -> (&'a str, &'a Value) {
    match err.error_struct.as_ref() {
        Some(RpcErrorKind::HandlerError(value)) | Some(RpcErrorKind::InternalError(value)) => {
            let name =
                value.get("name").and_then(|n| n.as_str()).unwrap();
            let info = value.get("info").unwrap();
            (name, info)
        }
        other => panic!("unexpected RPC error kind: {other:?}"),
    }
}

fn assert_missing_account_error<T: std::fmt::Debug>(
    result: Result<T, RpcError>,
    account_id: &AccountId,
    method_name: &str,
) {
    let err = result.expect_err("expected RPC error");
    let (name, info) = error_name_and_info(&err);
    assert_eq!(
        name, "UNKNOWN_ACCOUNT",
        "{method_name} error must indicate missing account, got: {name:?}"
    );
    let requested_account =
        info.get("requested_account_id").and_then(|v| v.as_str()).unwrap_or_default();
    assert_eq!(
        requested_account,
        account_id.as_str(),
        "{method_name} error must mention missing account {account_id}, got info: {info}"
    );
}

fn assert_unknown_block_error<T: std::fmt::Debug>(result: Result<T, RpcError>, method_name: &str) {
    let err = result.expect_err("expected RPC error");
    let (name, _info) = error_name_and_info(&err);
    assert!(
        name == "UNKNOWN_BLOCK" || name == "GARBAGE_COLLECTED_BLOCK",
        "{method_name} error must indicate unknown block, got: {name:?}"
    );
}

/// Test EXPERIMENTAL_view_account method
#[tokio::test]
async fn test_experimental_view_account() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let response = client
        .EXPERIMENTAL_view_account(RpcViewAccountRequest {
            block_reference: BlockReference::latest(),
            account_id: "test1".parse().unwrap(),
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(
        response.account.amount.checked_add(response.account.locked).unwrap(),
        TESTING_INIT_BALANCE
    );
    assert_eq!(response.account.code_hash, CryptoHash::default());
    assert_eq!(response.account.storage_paid_at, 0);
}

/// Test EXPERIMENTAL_view_account with different block references
#[tokio::test]
async fn test_experimental_view_account_block_references() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let status = client.status().await.unwrap();
    let block_hash = status.sync_info.latest_block_hash;

    // Test with latest block
    let response1 = client
        .EXPERIMENTAL_view_account(RpcViewAccountRequest {
            block_reference: BlockReference::latest(),
            account_id: "test1".parse().unwrap(),
        })
        .await
        .unwrap();

    // Test with block height
    let response2 = client
        .EXPERIMENTAL_view_account(RpcViewAccountRequest {
            block_reference: BlockReference::BlockId(BlockId::Height(0)),
            account_id: "test1".parse().unwrap(),
        })
        .await
        .unwrap();

    // Test with block hash
    let response3 = client
        .EXPERIMENTAL_view_account(RpcViewAccountRequest {
            block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
            account_id: "test1".parse().unwrap(),
        })
        .await
        .unwrap();

    for response in &[response1, response2, response3] {
        assert!(response.block_height < 100);
        assert_ne!(response.block_hash, CryptoHash::default());
    }
}

/// Test EXPERIMENTAL_view_account error on missing account
#[tokio::test]
async fn test_experimental_view_account_missing_account() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let missing_account: AccountId = "missing.test".parse().unwrap();
    let result = client
        .EXPERIMENTAL_view_account(RpcViewAccountRequest {
            block_reference: BlockReference::latest(),
            account_id: missing_account.clone(),
        })
        .await;

    assert_missing_account_error(result, &missing_account, "EXPERIMENTAL_view_account");
}

/// Test EXPERIMENTAL_view_code method
#[tokio::test]
async fn test_experimental_view_code() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    deploy_contract(&client, &account, code.clone()).await;

    let response = client
        .EXPERIMENTAL_view_code(RpcViewCodeRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(response.code.code, code);
    assert_eq!(response.code.hash, CryptoHash::hash_bytes(&code));
}

/// Test EXPERIMENTAL_view_code error when code is missing
#[tokio::test]
async fn test_experimental_view_code_missing_code_error() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let result = client
        .EXPERIMENTAL_view_code(RpcViewCodeRequest {
            block_reference: BlockReference::latest(),
            account_id: account.clone(),
        })
        .await;

    let err = result.expect_err("expected missing contract code error");
    let (name, info) = error_name_and_info(&err);
    assert_eq!(name, "NO_CONTRACT_CODE");
    let contract_account =
        info.get("contract_account_id").and_then(|v| v.as_str()).unwrap_or_default();
    assert_eq!(contract_account, account.as_str());
}

/// Test EXPERIMENTAL_view_state method
#[tokio::test]
async fn test_experimental_view_state() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let response = client
        .EXPERIMENTAL_view_state(RpcViewStateRequest {
            block_reference: BlockReference::latest(),
            account_id: "test1".parse().unwrap(),
            prefix: vec![].into(),
            include_proof: false,
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(response.state.values.len(), 0);
}

/// Test EXPERIMENTAL_view_state with include_proof
#[tokio::test]
async fn test_experimental_view_state_with_proof() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let response = client
        .EXPERIMENTAL_view_state(RpcViewStateRequest {
            block_reference: BlockReference::latest(),
            account_id: "test1".parse().unwrap(),
            prefix: vec![].into(),
            include_proof: true,
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
}

/// Test EXPERIMENTAL_view_access_key method
#[tokio::test]
async fn test_experimental_view_access_key() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);

    let response = client
        .EXPERIMENTAL_view_access_key(RpcViewAccessKeyRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
            public_key: signer.public_key(),
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(response.access_key.nonce, 0);
    assert_eq!(response.access_key.permission, AccessKeyPermission::FullAccess.into());
}

/// Test EXPERIMENTAL_view_access_key error on unknown key
#[tokio::test]
async fn test_experimental_view_access_key_unknown_key() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let missing_key_signer = InMemorySigner::test_signer(&"missing.test".parse().unwrap());

    let result = client
        .EXPERIMENTAL_view_access_key(RpcViewAccessKeyRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
            public_key: missing_key_signer.public_key(),
        })
        .await;

    let err = result.expect_err("expected missing access key error");
    let (name, info) = error_name_and_info(&err);
    assert_eq!(name, "UNKNOWN_ACCESS_KEY");
    let public_key = info.get("public_key").and_then(|v| v.as_str()).unwrap_or_default();
    assert_eq!(
        public_key,
        missing_key_signer.public_key().to_string(),
        "Error must mention missing access key"
    );
}

/// Test EXPERIMENTAL_view_access_key_list method
#[tokio::test]
async fn test_experimental_view_access_key_list() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);

    let response = client
        .EXPERIMENTAL_view_access_key_list(RpcViewAccessKeyListRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(response.access_key_list.keys.len(), 1);
    assert_eq!(response.access_key_list.keys[0].access_key, AccessKey::full_access().into());
    assert_eq!(response.access_key_list.keys[0].public_key, signer.public_key());
}

/// Test EXPERIMENTAL_view_access_key_list error on unknown block
#[tokio::test]
async fn test_experimental_view_access_key_list_unknown_block() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let result = client
        .EXPERIMENTAL_view_access_key_list(RpcViewAccessKeyListRequest {
            block_reference: BlockReference::BlockId(BlockId::Hash(CryptoHash::new())),
            account_id: "test1".parse().unwrap(),
        })
        .await;

    assert_unknown_block_error(result, "EXPERIMENTAL_view_access_key_list");
}

/// Test EXPERIMENTAL_call_function method
#[tokio::test]
async fn test_experimental_call_function() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    deploy_contract(&client, &account, code).await;

    let response = client
        .EXPERIMENTAL_call_function(RpcCallFunctionRequest {
            block_reference: BlockReference::latest(),
            account_id: "test1".parse().unwrap(),
            method_name: "run_test".to_string(),
            args: vec![].into(),
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    assert_eq!(response.result.result, 10i32.to_le_bytes());
    assert_eq!(response.result.logs.len(), 0);
}

/// Test EXPERIMENTAL_call_function error on missing method (MethodNotFound)
#[tokio::test]
async fn test_experimental_call_function_nonexisting_method() {
    let setup = create_test_setup_with_node_type(NodeType::Validator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let code = near_test_contracts::rs_contract().to_vec();
    deploy_contract(&client, &account, code).await;

    let result = client
        .EXPERIMENTAL_call_function(RpcCallFunctionRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
            method_name: "nonexisting".to_string(),
            args: vec![].into(),
        })
        .await;

    let err = result.expect_err("expected method not found error");
    let (name, info) = error_name_and_info(&err);
    assert_eq!(name, "CONTRACT_EXECUTION_ERROR");
    println!("Error info: {:?}", info);
    let method_error = info
        .get("vm_error")
        .and_then(|error| error.get("MethodResolveError"))
        .expect("expected MethodResolveError for missing method");
    assert_eq!(
        method_error.as_str().unwrap_or_default(),
        "MethodNotFound",
        "Expected MethodNotFound error when calling missing method"
    );
}

/// Test EXPERIMENTAL_view_gas_key method - expects error since test account has no gas keys
#[tokio::test]
async fn test_experimental_view_gas_key_not_found() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();
    let signer = InMemorySigner::test_signer(&account);

    let result = client
        .EXPERIMENTAL_view_gas_key(RpcViewGasKeyRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
            public_key: signer.public_key(),
        })
        .await;

    // Gas keys don't exist for test accounts, so this should return an error
    let err = result.expect_err("expected missing gas key error");
    let (name, info) = error_name_and_info(&err);
    assert_eq!(name, "UNKNOWN_GAS_KEY");
    let public_key = info.get("public_key").and_then(|v| v.as_str()).unwrap_or_default();
    assert_eq!(public_key, signer.public_key().to_string(), "Error must mention missing gas key");
}

/// Test EXPERIMENTAL_view_gas_key_list method - expects empty list since test account has no gas keys
#[tokio::test]
async fn test_experimental_view_gas_key_list() {
    let setup = create_test_setup_with_node_type(NodeType::NonValidator);
    let client = new_client(&setup.server_addr);

    let account: AccountId = "test1".parse().unwrap();

    let response = client
        .EXPERIMENTAL_view_gas_key_list(RpcViewGasKeyListRequest {
            block_reference: BlockReference::latest(),
            account_id: account,
        })
        .await
        .unwrap();

    assert!(response.block_height < 100);
    assert_ne!(response.block_hash, CryptoHash::default());
    // Test accounts don't have gas keys by default, so list should be empty
    assert_eq!(response.gas_key_list.keys.len(), 0);
}
