use crate::utils::sharded_rpc::{TwoShardHarness, assert_rpc_error};
use near_async::time::Duration;
use near_jsonrpc::client::ChunkId;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::call_function::RpcCallFunctionRequest;
use near_jsonrpc_primitives::types::chunks::ChunkReference;
use near_jsonrpc_primitives::types::congestion::RpcCongestionLevelRequest;
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_jsonrpc_primitives::types::receipts::{
    ReceiptReference, RpcReceiptRequest, RpcReceiptResponse,
};
use near_jsonrpc_primitives::types::view_access_key::RpcViewAccessKeyRequest;
use near_jsonrpc_primitives::types::view_access_key_list::RpcViewAccessKeyListRequest;
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_jsonrpc_primitives::types::view_code::RpcViewCodeRequest;
use near_jsonrpc_primitives::types::view_gas_key_nonces::RpcViewGasKeyNoncesRequest;
use near_jsonrpc_primitives::types::view_state::RpcViewStateRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{
    AccountId, Balance, BlockId, BlockReference, Finality, FunctionArgs, Gas, StoreKey,
};
use near_primitives::views::QueryRequest;
use near_store::ShardUId;

/// EXPERIMENTAL_view_account queries should be forwarded to the right shard.
#[test]
fn test_rpc_view_account_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_view_account = |node_id: &AccountId,
                                account: &AccountId,
                                expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: account.clone(),
                })
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.account.amount, expected_balance, "node: {node_id}, account: {account}");
        Ok(())
    };

    // Cross-shard forwarding.
    run_view_account(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_view_account(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Standard `query` ViewAccount should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_account_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_query_view_account = |node_id: &AccountId,
                                      account: &AccountId,
                                      expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::None),
                request: QueryRequest::ViewAccount { account_id: account.clone() },
            },
            Duration::seconds(5),
        )?;
        match result.kind {
            QueryResponseKind::ViewAccount(view) => {
                assert_eq!(view.amount, expected_balance, "node: {node_id}, account: {account}");
            }
            other => panic!("expected ViewAccount, got: {other:?}"),
        }
        Ok(())
    };

    // Cross-shard forwarding.
    run_query_view_account(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query_view_account(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_query_view_account(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query_view_account(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Standard `query` ViewAccessKey should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_access_key_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_query_view_access_key = |node_id: &AccountId,
                                         account: &AccountId|
     -> Result<(), RpcError> {
        let public_key =
            near_primitives::test_utils::create_user_test_signer(account.as_ref()).public_key();
        let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::None),
                request: QueryRequest::ViewAccessKey { account_id: account.clone(), public_key },
            },
            Duration::seconds(5),
        )?;
        match result.kind {
            QueryResponseKind::AccessKey(_) => {}
            other => panic!("expected AccessKey, got: {other:?}"),
        }
        Ok(())
    };

    // Cross-shard forwarding.
    run_query_view_access_key(&h.zoe_node, &h.alice).unwrap();
    run_query_view_access_key(&h.alice_node, &h.zoe).unwrap();
    // Local.
    run_query_view_access_key(&h.alice_node, &h.alice).unwrap();
    run_query_view_access_key(&h.zoe_node, &h.zoe).unwrap();
}

/// Cross-shard query for a nonexistent access key should return the backward-compatible
/// error.
#[test]
fn test_rpc_query_unknown_access_key_error_format() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    // Use a public key that doesn't belong to alice.
    let bogus_key: near_crypto::PublicKey =
        "ed25519:6E8sCci9badyRkXb3JoRpBj5p8C6Tw41ELDZoiihKEtp".parse().unwrap();

    let response = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                let request = Message::request(
                    "query".to_string(),
                    serde_json::to_value(RpcQueryRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        request: QueryRequest::ViewAccessKey {
                            account_id: alice.clone(),
                            public_key: bogus_key.clone(),
                        },
                    })
                    .unwrap(),
                );
                client.transport.send_jsonrpc_request(request, false)
            },
            Duration::seconds(5),
        )
        .unwrap();

    // process_query_response wraps UnknownAccessKey as a successful JSON-RPC result.
    match response {
        Message::Response(resp) => {
            let value = resp.result.expect("expected Ok result with backward-compat error JSON");
            assert!(value.get("error").is_some());
            assert!(value.get("logs").is_some());
            assert!(value.get("block_height").is_some());
            assert!(value.get("block_hash").is_some());
            let error_msg = value["error"].as_str().unwrap();
            assert!(
                error_msg.contains("does not exist while viewing"),
                "unexpected error message: {error_msg}"
            );
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// Standard `query` ViewCode should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_code_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let mut run_query_view_code =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewCode { account_id: account.clone() },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::ViewCode(view) => {
                    assert!(!view.code.is_empty(), "node: {node_id}, account: {account}");
                }
                other => panic!("expected ViewCode, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard.
    run_query_view_code(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_query_view_code(&h.alice_node, &h.alice).unwrap();
}

/// Standard `query` ViewState should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_state_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    // Write a key-value pair to alice's contract state.
    let alice = h.alice.clone();
    let alice_node = h.alice_node.clone();
    let mut args = b"key42".to_vec();
    args.extend_from_slice(&42u64.to_le_bytes());
    let tx = h.env.node_for_account(&alice_node).tx_call(
        &alice,
        &alice,
        "write_key_value",
        args,
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    h.env.runner_for_account(&alice_node).run_tx(tx, Duration::seconds(5));

    let mut run_query_view_state =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewState {
                        account_id: account.clone(),
                        prefix: StoreKey::from(vec![]),
                        include_proof: false,
                    },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::ViewState(view) => {
                    assert!(!view.values.is_empty(), "node: {node_id}, account: {account}");
                }
                other => panic!("expected ViewState, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard.
    run_query_view_state(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_query_view_state(&h.alice_node, &h.alice).unwrap();
}

/// Standard `query` ViewAccessKeyList should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_access_key_list_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_query_view_access_key_list =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewAccessKeyList { account_id: account.clone() },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::AccessKeyList(view) => {
                    assert!(!view.keys.is_empty(), "node: {node_id}, account: {account}");
                }
                other => panic!("expected AccessKeyList, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard forwarding.
    run_query_view_access_key_list(&h.zoe_node, &h.alice).unwrap();
    run_query_view_access_key_list(&h.alice_node, &h.zoe).unwrap();
    // Local.
    run_query_view_access_key_list(&h.alice_node, &h.alice).unwrap();
    run_query_view_access_key_list(&h.zoe_node, &h.zoe).unwrap();
}

/// Standard `query` CallFunction should be forwarded to the right shard.
#[test]
fn test_rpc_query_call_function_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let mut run_query_call_function =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::CallFunction {
                        account_id: account.clone(),
                        method_name: "log_something".to_string(),
                        args: FunctionArgs::from(vec![]),
                    },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::CallResult(_) => {}
                other => panic!("expected CallResult, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard.
    run_query_call_function(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_query_call_function(&h.alice_node, &h.alice).unwrap();
}

/// Standard `query` ViewGasKeyNonces should be forwarded to the right shard.
/// Genesis accounts don't have gas keys, so this returns UNKNOWN_GAS_KEY.
/// The important thing is we get that error (not UNAVAILABLE_SHARD), proving forwarding works.
#[test]
fn test_rpc_query_view_gas_key_nonces_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_query_view_gas_key_nonces = |node_id: &AccountId, account: &AccountId| {
        let public_key =
            near_primitives::test_utils::create_user_test_signer(account.as_ref()).public_key();
        let err = h
            .env
            .runner_for_account(node_id)
            .run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewGasKeyNonces {
                        account_id: account.clone(),
                        public_key,
                    },
                },
                Duration::seconds(5),
            )
            .unwrap_err();
        assert!(
            err.message.contains("Server error"),
            "node: {node_id}, account: {account}, error: {err:?}"
        );
        match err.error_struct.unwrap() {
            near_jsonrpc_primitives::errors::RpcErrorKind::HandlerError(value) => {
                assert_eq!(value["name"], "UNKNOWN_GAS_KEY");
            }
            other => panic!("expected HandlerError, got: {other:?}"),
        }
    };

    // Cross-shard forwarding.
    run_query_view_gas_key_nonces(&h.zoe_node, &h.alice);
    run_query_view_gas_key_nonces(&h.alice_node, &h.zoe);
    // Local.
    run_query_view_gas_key_nonces(&h.alice_node, &h.alice);
    run_query_view_gas_key_nonces(&h.zoe_node, &h.zoe);
}

/// Standard `query` ViewGlobalContractCodeByAccountId should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_global_contract_code_by_account_id_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    let alice_node = h.alice_node.clone();

    // Deploy a global contract with AccountId mode.
    let code = near_test_contracts::trivial_contract().to_vec();
    let tx = h.env.node_for_account(&alice_node).tx_deploy_global_contract(
        &alice,
        code,
        near_primitives::action::GlobalContractDeployMode::AccountId,
    );
    h.env.runner_for_account(&alice_node).run_tx(tx, Duration::seconds(5));

    // Have alice use the global contract.
    let tx = h.env.node_for_account(&alice_node).tx_use_global_contract(
        &alice,
        near_primitives::action::GlobalContractIdentifier::AccountId(alice.clone()),
    );
    h.env.runner_for_account(&alice_node).run_tx(tx, Duration::seconds(5));

    let mut run_query_view_global_contract =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewGlobalContractCodeByAccountId {
                        account_id: account.clone(),
                    },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::ViewCode(view) => {
                    assert!(!view.code.is_empty(), "node: {node_id}, account: {account}");
                }
                other => panic!("expected ViewCode, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard.
    run_query_view_global_contract(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_query_view_global_contract(&h.alice_node, &h.alice).unwrap();
}

/// Cross-shard CallFunction that triggers a VM error should return the backward-compatible
/// error format from `process_query_response`.
#[test]
fn test_rpc_query_call_function_error_format() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let alice = h.alice.clone();
    // Cross-shard: call a nonexistent method on alice's contract from zoe's node.
    let response = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                let request = Message::request(
                    "query".to_string(),
                    serde_json::to_value(RpcQueryRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        request: QueryRequest::CallFunction {
                            account_id: alice.clone(),
                            method_name: "nonexistent_method".to_string(),
                            args: FunctionArgs::from(vec![]),
                        },
                    })
                    .unwrap(),
                );
                client.transport.send_jsonrpc_request(request, false)
            },
            Duration::seconds(5),
        )
        .unwrap();

    // process_query_response wraps ContractExecutionError as a successful JSON-RPC result.
    match response {
        Message::Response(resp) => {
            let value = resp.result.expect("expected Ok result with backward-compat error JSON");
            assert!(value.get("error").is_some());
            let error_msg = value["error"].as_str().unwrap();
            assert!(error_msg.contains("MethodNotFound"), "unexpected error message: {error_msg}");
            assert!(value.get("logs").is_some());
            assert!(value.get("block_height").is_some());
            assert!(value.get("block_hash").is_some());
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// EXPERIMENTAL_receipt queries should fan out across shards and return the receipt
/// regardless of which RPC node receives the query.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_receipt_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    // Submit a cross-shard transfer to generate a receipt.
    let alice = h.alice.clone();
    let zoe = h.zoe.clone();
    let validator = h.validator.clone();
    let tx = h.env.node_for_account(&validator).tx_send_money(&alice, &zoe, Balance::from_near(1));
    let tx_hash = tx.get_hash();
    h.env.node_for_account(&validator).submit_tx(tx);

    // Wait for execution to complete.
    let target_height = h.env.node_for_account(&validator).head().height + 10;
    h.env.runner_for_account(&validator).run_until_executed_height(target_height);

    // Get receipt ID from the transaction outcome.
    let outcome =
        h.env.node_for_account(&validator).client().chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_id = outcome.outcome_with_id.outcome.receipt_ids[0];

    let run_receipt_query = |h: &mut TwoShardHarness,
                             node_id: &AccountId,
                             receipt_id: CryptoHash|
     -> Result<RpcReceiptResponse, RpcError> {
        h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt(RpcReceiptRequest {
                    receipt_reference: ReceiptReference { receipt_id },
                })
            },
            Duration::seconds(5),
        )
    };

    // Both RPC nodes should be able to return the receipt, even though only one shard has it.
    let alice_node = h.alice_node.clone();
    let zoe_node = h.zoe_node.clone();

    for node_id in [&alice_node, &zoe_node] {
        let receipt = run_receipt_query(&mut h, node_id, receipt_id)
            .unwrap_or_else(|e| panic!("receipt query from {node_id} failed: {e:?}"));
        let view = &receipt.receipt_view;
        assert_eq!(view.receipt_id, receipt_id);
        assert_eq!(view.predecessor_id, alice);
        assert_eq!(view.receiver_id, zoe);
    }

    // Query a nonexistent receipt — both nodes should return UnknownReceipt.
    let bogus_receipt_id = CryptoHash::hash_bytes(b"bogus");

    let err = run_receipt_query(&mut h, &alice_node, bogus_receipt_id).unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_RECEIPT");

    let err = run_receipt_query(&mut h, &zoe_node, bogus_receipt_id).unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_RECEIPT");
}

/// EXPERIMENTAL_view_code queries should be forwarded to the right shard.
#[test]
fn test_rpc_experimental_view_code_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let mut run_view_code = |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_code(RpcViewCodeRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: account.clone(),
                })
            },
            Duration::seconds(5),
        )?;
        assert!(!result.code.code.is_empty(), "node: {node_id}, account: {account}");
        Ok(())
    };

    // Cross-shard.
    run_view_code(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_view_code(&h.alice_node, &h.alice).unwrap();
}

/// EXPERIMENTAL_view_state queries should be forwarded to the right shard.
#[test]
fn test_rpc_experimental_view_state_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    // Write a key-value pair to alice's contract state.
    let alice = h.alice.clone();
    let alice_node = h.alice_node.clone();
    let mut args = b"key42".to_vec();
    args.extend_from_slice(&42u64.to_le_bytes());
    let tx = h.env.node_for_account(&alice_node).tx_call(
        &alice,
        &alice,
        "write_key_value",
        args,
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    h.env.runner_for_account(&alice_node).run_tx(tx, Duration::seconds(5));

    let mut run_view_state = |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_state(RpcViewStateRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: account.clone(),
                    prefix: StoreKey::from(vec![]),
                    include_proof: false,
                })
            },
            Duration::seconds(5),
        )?;
        assert!(!result.state.values.is_empty(), "node: {node_id}, account: {account}");
        Ok(())
    };

    // Cross-shard.
    run_view_state(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_view_state(&h.alice_node, &h.alice).unwrap();
}

/// EXPERIMENTAL_view_access_key queries should be forwarded to the right shard.
#[test]
fn test_rpc_experimental_view_access_key_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_view_access_key =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let public_key =
                near_primitives::test_utils::create_user_test_signer(account.as_ref()).public_key();
            let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_view_access_key(RpcViewAccessKeyRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        account_id: account.clone(),
                        public_key,
                    })
                },
                Duration::seconds(5),
            )?;
            // Just verify we got a response without error.
            let _ = result.access_key;
            Ok(())
        };

    // Cross-shard forwarding.
    run_view_access_key(&h.zoe_node, &h.alice).unwrap();
    run_view_access_key(&h.alice_node, &h.zoe).unwrap();
    // Local.
    run_view_access_key(&h.alice_node, &h.alice).unwrap();
    run_view_access_key(&h.zoe_node, &h.zoe).unwrap();
}

/// EXPERIMENTAL_view_access_key_list queries should be forwarded to the right shard.
#[test]
fn test_rpc_experimental_view_access_key_list_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_view_access_key_list =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_view_access_key_list(RpcViewAccessKeyListRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        account_id: account.clone(),
                    })
                },
                Duration::seconds(5),
            )?;
            assert!(!result.access_key_list.keys.is_empty(), "node: {node_id}, account: {account}");
            Ok(())
        };

    // Cross-shard forwarding.
    run_view_access_key_list(&h.zoe_node, &h.alice).unwrap();
    run_view_access_key_list(&h.alice_node, &h.zoe).unwrap();
    // Local.
    run_view_access_key_list(&h.alice_node, &h.alice).unwrap();
    run_view_access_key_list(&h.zoe_node, &h.zoe).unwrap();
}

/// EXPERIMENTAL_view_gas_key_nonces queries should be forwarded to the right shard.
/// Genesis accounts don't have gas keys, so this returns an error.
/// The important thing is we get UNKNOWN_GAS_KEY (not UNAVAILABLE_SHARD), proving forwarding works.
#[test]
fn test_rpc_experimental_view_gas_key_nonces_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_view_gas_key_nonces = |node_id: &AccountId, account: &AccountId| {
        let public_key =
            near_primitives::test_utils::create_user_test_signer(account.as_ref()).public_key();
        let err = h
            .env
            .runner_for_account(node_id)
            .run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_view_gas_key_nonces(RpcViewGasKeyNoncesRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        account_id: account.clone(),
                        public_key,
                    })
                },
                Duration::seconds(5),
            )
            .unwrap_err();
        assert!(
            err.message.contains("Server error"),
            "node: {node_id}, account: {account}, error: {err:?}"
        );
        assert_rpc_error(&err, "UNKNOWN_GAS_KEY");
    };

    // Cross-shard forwarding.
    run_view_gas_key_nonces(&h.zoe_node, &h.alice);
    run_view_gas_key_nonces(&h.alice_node, &h.zoe);
    // Local.
    run_view_gas_key_nonces(&h.alice_node, &h.alice);
    run_view_gas_key_nonces(&h.zoe_node, &h.zoe);
}

/// EXPERIMENTAL_call_function queries should be forwarded to the right shard.
#[test]
fn test_rpc_experimental_call_function_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let mut run_call_function =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_call_function(RpcCallFunctionRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        account_id: account.clone(),
                        method_name: "log_something".to_string(),
                        args: FunctionArgs::from(vec![]),
                    })
                },
                Duration::seconds(5),
            )?;
            let _ = result.result;
            Ok(())
        };

    // Cross-shard.
    run_call_function(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_call_function(&h.alice_node, &h.alice).unwrap();
}

/// EXPERIMENTAL_congestion_level with BlockShardId should be forwarded to the right shard.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_experimental_congestion_level_block_shard_id_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let shard_layout = ShardLayout::multi_shard(2, 1);
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let head_height = h.env.node_for_account(&h.validator).head().height;

    let mut run_congestion_level =
        |node_id: &AccountId, shard_uid: ShardUId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_congestion_level(RpcCongestionLevelRequest {
                        chunk_reference: ChunkReference::BlockShardId {
                            block_id: near_primitives::types::BlockId::Height(head_height),
                            shard_id: shard_uid.shard_id(),
                        },
                    })
                },
                Duration::seconds(5),
            )?;
            assert!(result.congestion_level >= 0.0);
            Ok(())
        };

    // Cross-shard: query shard 1 from shard 0's node and vice versa.
    run_congestion_level(&h.alice_node, shard_uids[1]).unwrap();
    run_congestion_level(&h.zoe_node, shard_uids[0]).unwrap();
    // Local.
    run_congestion_level(&h.alice_node, shard_uids[0]).unwrap();
    run_congestion_level(&h.zoe_node, shard_uids[1]).unwrap();
}

/// EXPERIMENTAL_congestion_level with ChunkHash should be forwarded across shards.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_experimental_congestion_level_chunk_hash_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    // Get a chunk hash from the head block.
    let validator = h.validator.clone();
    let head = h.env.node_for_account(&validator).head();
    let head_block =
        h.env.node_for_account(&validator).client().chain.get_block(&head.last_block_hash).unwrap();
    let chunk_hash = head_block.chunks()[0].chunk_hash().0;

    let mut run_congestion_level =
        |node_id: &AccountId, chunk_id: CryptoHash| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_congestion_level(RpcCongestionLevelRequest {
                        chunk_reference: ChunkReference::ChunkHash { chunk_id },
                    })
                },
                Duration::seconds(5),
            )?;
            assert!(result.congestion_level >= 0.0);
            Ok(())
        };

    // Both nodes should be able to serve a ChunkHash query (falls back to all nodes).
    let alice_node = h.alice_node.clone();
    let zoe_node = h.zoe_node.clone();
    run_congestion_level(&alice_node, chunk_hash).unwrap();
    run_congestion_level(&zoe_node, chunk_hash).unwrap();
}

/// chunk queries by BlockId::Height + ShardId should be forwarded across shards.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_chunk_block_height_shard_id_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let shard_layout = ShardLayout::multi_shard(2, 1);
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let head_height = h.env.node_for_account(&h.validator).head().height;

    let mut run_chunk = |node_id: &AccountId, shard_uid: ShardUId| -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.chunk(ChunkId::BlockShardId(
                    BlockId::Height(head_height),
                    shard_uid.shard_id(),
                ))
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.header.shard_id, shard_uid.shard_id());
        Ok(())
    };

    // Cross-shard: query shard 1 from shard 0's node and vice versa.
    run_chunk(&h.alice_node, shard_uids[1]).unwrap();
    run_chunk(&h.zoe_node, shard_uids[0]).unwrap();
    // Local.
    run_chunk(&h.alice_node, shard_uids[0]).unwrap();
    run_chunk(&h.zoe_node, shard_uids[1]).unwrap();
}

/// chunk queries by BlockId::Hash + ShardId should be forwarded across shards.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_chunk_block_hash_shard_id_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let shard_layout = ShardLayout::multi_shard(2, 1);
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let validator = h.validator.clone();
    let head = h.env.node_for_account(&validator).head();
    let block_hash = head.last_block_hash;

    let mut run_chunk = |node_id: &AccountId, shard_uid: ShardUId| -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.chunk(ChunkId::BlockShardId(BlockId::Hash(block_hash), shard_uid.shard_id()))
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.header.shard_id, shard_uid.shard_id());
        Ok(())
    };

    // Cross-shard: query shard 1 from shard 0's node and vice versa.
    run_chunk(&h.alice_node, shard_uids[1]).unwrap();
    run_chunk(&h.zoe_node, shard_uids[0]).unwrap();
    // Local.
    run_chunk(&h.alice_node, shard_uids[0]).unwrap();
    run_chunk(&h.zoe_node, shard_uids[1]).unwrap();
}

/// chunk queries by ChunkHash should be forwarded across shards.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_chunk_hash_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    // Get chunk hashes from both shards in the head block.
    let validator = h.validator.clone();
    let head = h.env.node_for_account(&validator).head();
    let head_block =
        h.env.node_for_account(&validator).client().chain.get_block(&head.last_block_hash).unwrap();
    let chunk_hash_shard0 = head_block.chunks()[0].chunk_hash().0;
    let chunk_hash_shard1 = head_block.chunks()[1].chunk_hash().0;

    let mut run_chunk = |node_id: &AccountId, chunk_id: CryptoHash| -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| client.chunk(ChunkId::Hash(chunk_id)),
            Duration::seconds(5),
        )?;
        assert_eq!(result.header.chunk_hash, chunk_id);
        Ok(())
    };

    // Both nodes should be able to serve ChunkHash queries for both shards
    // (resolved via partial chunk store).
    let alice_node = h.alice_node.clone();
    let zoe_node = h.zoe_node.clone();
    run_chunk(&alice_node, chunk_hash_shard0).unwrap();
    run_chunk(&alice_node, chunk_hash_shard1).unwrap();
    run_chunk(&zoe_node, chunk_hash_shard0).unwrap();
    run_chunk(&zoe_node, chunk_hash_shard1).unwrap();
}

/// Cross-shard EXPERIMENTAL_view_code for an account with no contract should return a proper error.
#[test]
fn test_rpc_experimental_view_code_error_format() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    // zoe has no contract deployed.
    let err = h
        .env
        .runner_for_account(&h.alice_node)
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_code(RpcViewCodeRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: h.zoe.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "NO_CONTRACT_CODE");
}

/// Cross-shard EXPERIMENTAL_call_function on a nonexistent method should return a proper error.
#[test]
fn test_rpc_experimental_call_function_error_format() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let err = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_call_function(RpcCallFunctionRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: h.alice.clone(),
                    method_name: "nonexistent_method".to_string(),
                    args: FunctionArgs::from(vec![]),
                })
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "CONTRACT_EXECUTION_ERROR");
}

/// Cross-shard EXPERIMENTAL_view_access_key for a nonexistent key should return a proper error.
#[test]
fn test_rpc_experimental_view_access_key_error_format() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let bogus_key: near_crypto::PublicKey =
        "ed25519:6E8sCci9badyRkXb3JoRpBj5p8C6Tw41ELDZoiihKEtp".parse().unwrap();

    let err = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_access_key(RpcViewAccessKeyRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    account_id: h.alice.clone(),
                    public_key: bogus_key.clone(),
                })
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_ACCESS_KEY");
}

/// Queries with Finality::Final should be forwarded cross-shard just like
/// Finality::None, and the response should reference the final block (not head).
#[test]
fn test_rpc_view_account_finality_final() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    let (final_height, _head_height) = h.ensure_finality_lag();

    let mut run_view_account = |node_id: &AccountId,
                                account: &AccountId,
                                expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::Final),
                    account_id: account.clone(),
                })
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.account.amount, expected_balance, "node: {node_id}, account: {account}");
        assert!(
            result.block_height <= final_height,
            "expected block_height <= final_height ({final_height}), got {}",
            result.block_height,
        );
        Ok(())
    };

    // Cross-shard forwarding with Finality::Final.
    run_view_account(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_view_account(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Queries with Finality::DoomSlug should route correctly and reference a
/// near-final block.
#[test]
fn test_rpc_view_account_finality_doomslug() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    let (final_height, head_height) = h.ensure_finality_lag();

    let mut run_view_account = |node_id: &AccountId,
                                account: &AccountId,
                                expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_view_account(RpcViewAccountRequest {
                    block_reference: BlockReference::Finality(Finality::DoomSlug),
                    account_id: account.clone(),
                })
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.account.amount, expected_balance, "node: {node_id}, account: {account}");
        // DoomSlug finality should reference a block at or between final and head.
        assert!(
            result.block_height >= final_height && result.block_height <= head_height,
            "expected final_height ({final_height}) <= block_height <= head_height ({head_height}), got {}",
            result.block_height,
        );
        Ok(())
    };

    // Cross-shard forwarding with Finality::DoomSlug.
    run_view_account(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_view_account(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_view_account(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Contract calls with Finality::Final should be forwarded cross-shard.
/// Note: this test verifies routing, not that the result comes from the final
/// block's state specifically.
#[test]
fn test_rpc_call_function_finality_final() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    // Advance a few blocks so the contract deploy is included in the final block.
    let validator = h.validator.clone();
    h.env.runner_for_account(&validator).run_for_number_of_blocks(3);

    let mut run_call_function =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::Final),
                    request: QueryRequest::CallFunction {
                        account_id: account.clone(),
                        method_name: "log_something".to_string(),
                        args: FunctionArgs::from(vec![]),
                    },
                },
                Duration::seconds(5),
            )?;
            match result.kind {
                QueryResponseKind::CallResult(_) => {}
                other => panic!("expected CallResult, got: {other:?}"),
            }
            Ok(())
        };

    // Cross-shard with Finality::Final.
    run_call_function(&h.zoe_node, &h.alice).unwrap();
    // Local.
    run_call_function(&h.alice_node, &h.alice).unwrap();
}

/// Queries with an explicit BlockId::Height should succeed cross-shard,
/// exercising the BlockHint::Height code path in nodes_for_query.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_query_view_account_by_block_height() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let validator = h.validator.clone();
    let head_height = h.env.node_for_account(&validator).head().height;

    let mut run_query = |node_id: &AccountId,
                         account: &AccountId,
                         expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(head_height)),
                request: QueryRequest::ViewAccount { account_id: account.clone() },
            },
            Duration::seconds(5),
        )?;
        assert_eq!(
            result.block_height, head_height,
            "result should come from the requested height"
        );
        match result.kind {
            QueryResponseKind::ViewAccount(view) => {
                assert_eq!(view.amount, expected_balance, "node: {node_id}, account: {account}");
            }
            other => panic!("expected ViewAccount, got: {other:?}"),
        }
        Ok(())
    };

    // Cross-shard forwarding with explicit block height.
    run_query(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_query(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Queries with an explicit BlockId::Hash should succeed cross-shard,
/// exercising the BlockHint::Hash code path in nodes_for_query.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_query_view_account_by_block_hash() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let validator = h.validator.clone();
    let head_hash = h.env.node_for_account(&validator).head().last_block_hash;

    let mut run_query = |node_id: &AccountId,
                         account: &AccountId,
                         expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(head_hash)),
                request: QueryRequest::ViewAccount { account_id: account.clone() },
            },
            Duration::seconds(5),
        )?;
        assert_eq!(result.block_hash, head_hash, "result should come from the requested hash");
        match result.kind {
            QueryResponseKind::ViewAccount(view) => {
                assert_eq!(view.amount, expected_balance, "node: {node_id}, account: {account}");
            }
            other => panic!("expected ViewAccount, got: {other:?}"),
        }
        Ok(())
    };

    // Cross-shard forwarding with explicit block hash.
    run_query(&h.zoe_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query(&h.alice_node, &h.zoe, Balance::from_near(200)).unwrap();
    // Local.
    run_query(&h.alice_node, &h.alice, Balance::from_near(100)).unwrap();
    run_query(&h.zoe_node, &h.zoe, Balance::from_near(200)).unwrap();
}

/// Queries referencing nonexistent blocks should fall back to trying all nodes
/// and return UnknownBlock, not UnavailableShard or a panic.
#[test]
fn test_rpc_query_unknown_block_fallback() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    let alice_node = h.alice_node.clone();

    // Unknown block hash — exercises the fallback.
    let err = h
        .env
        .runner_for_account(&alice_node)
        .run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(CryptoHash::hash_bytes(
                    b"bogus",
                ))),
                request: QueryRequest::ViewAccount { account_id: alice.clone() },
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_BLOCK");

    // Unknown block height — exercises the fallback.
    let err = h
        .env
        .runner_for_account(&alice_node)
        .run_jsonrpc_query(
            RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(999_999_999)),
                request: QueryRequest::ViewAccount { account_id: alice },
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_BLOCK");
}
