use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_primitives::errors::{RpcError, RpcErrorKind};
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_jsonrpc_primitives::types::receipts::{
    ReceiptReference, RpcReceiptRequest, RpcReceiptResponse,
};
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{
    AccountId, Balance, BlockReference, Finality, FunctionArgs, Gas, StoreKey,
};
use near_primitives::views::QueryRequest;
use near_store::ShardUId;

/// Test harness for sharded RPC tests.
///
/// Sets up a 2-shard environment with 2 RPC nodes (each tracking one shard)
/// and 1 validator. Two user accounts (alice, zoe) are placed on different shards.
struct TwoShardHarness {
    env: TestLoopEnv,
    alice: AccountId,
    zoe: AccountId,
    /// RPC node that tracks alice's shard.
    alice_node: AccountId,
    /// RPC node that tracks zoe's shard.
    zoe_node: AccountId,
    /// Validator node (tracks all shards by default).
    validator: AccountId,
}

impl TwoShardHarness {
    fn new() -> Self {
        let shard_layout = ShardLayout::multi_shard(2, 1);
        let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();

        // "alice" < "test1" (boundary) and "zoe" >= "test1", so they land on different shards.
        let alice = create_account_id("alice");
        let zoe = create_account_id("zoe");
        let alice_shard = shard_layout.account_id_to_shard_id(&alice);
        let zoe_shard = shard_layout.account_id_to_shard_id(&zoe);
        assert_ne!(alice_shard, zoe_shard);

        let validator0: AccountId = create_account_id("validator");
        let validators_spec = ValidatorsSpec::desired_roles(&[validator0.as_str()], &[]);
        let genesis = TestLoopBuilder::new_genesis_builder()
            .shard_layout(shard_layout)
            .validators_spec(validators_spec)
            .add_user_account_simple(alice.clone(), Balance::from_near(100))
            .add_user_account_simple(zoe.clone(), Balance::from_near(200))
            .build();

        let rpc0: AccountId = create_account_id("rpc0");
        let rpc0_shard = shard_uids[0];
        let rpc1: AccountId = create_account_id("rpc1");
        let rpc1_shard = shard_uids[1];
        let clients = vec![rpc0.clone(), rpc1.clone(), validator0.clone()];
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .config_modifier(move |config, client_index| match client_index {
                0 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard]),
                1 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard]),
                _ => {}
            })
            .add_rpc_pool([rpc0.clone(), rpc1.clone()])
            .build();

        let (alice_node, zoe_node) =
            if alice_shard == rpc0_shard.shard_id() { (rpc0, rpc1) } else { (rpc1, rpc0) };

        Self { env, alice, zoe, alice_node, zoe_node, validator: validator0 }
    }

    /// Deploy the standard test contract to alice.
    fn deploy_contract_to_alice(&mut self) {
        let tx = self.env.node_for_account(&self.alice_node).tx_deploy_test_contract(&self.alice);
        self.env.runner_for_account(&self.alice_node).run_tx(tx, Duration::seconds(5));
    }
}

/// EXPERIMENTAL_view_account queries should be forwarded to the right shard.
#[test]
fn test_rpc_view_account_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let mut run_view_account = |node_id: &AccountId,
                                account: &AccountId,
                                expected_balance: Balance|
     -> Result<(), RpcError> {
        let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
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
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewAccount { account_id: account.clone() },
                })
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

    let mut run_query_view_access_key =
        |node_id: &AccountId, account: &AccountId| -> Result<(), RpcError> {
            let public_key =
                near_primitives::test_utils::create_user_test_signer(account.as_ref()).public_key();
            let result = h.env.runner_for_account(node_id).run_jsonrpc_query(
                |client| {
                    client.query(RpcQueryRequest {
                        block_reference: BlockReference::Finality(Finality::None),
                        request: QueryRequest::ViewAccessKey {
                            account_id: account.clone(),
                            public_key,
                        },
                    })
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
        .run_jsonrpc_query(
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

    let alice = h.alice.clone();
    // Cross-shard: query alice's code from zoe's node.
    let result = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewCode { account_id: alice.clone() },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::ViewCode(view) => {
            assert!(!view.code.is_empty(), "expected non-empty contract code");
        }
        other => panic!("expected ViewCode, got: {other:?}"),
    }
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

    // Cross-shard: query alice's state from zoe's node.
    let result = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewState {
                        account_id: alice.clone(),
                        prefix: StoreKey::from(vec![]),
                        include_proof: false,
                    },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::ViewState(view) => {
            assert!(!view.values.is_empty(), "expected non-empty state");
        }
        other => panic!("expected ViewState, got: {other:?}"),
    }
}

/// Standard `query` ViewAccessKeyList should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_access_key_list_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    // Cross-shard: query alice's access key list from zoe's node.
    let result = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewAccessKeyList { account_id: alice.clone() },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::AccessKeyList(view) => {
            assert!(!view.keys.is_empty(), "expected non-empty access key list");
        }
        other => panic!("expected AccessKeyList, got: {other:?}"),
    }
}

/// Standard `query` CallFunction should be forwarded to the right shard.
#[test]
fn test_rpc_query_call_function_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();
    h.deploy_contract_to_alice();

    let alice = h.alice.clone();
    // Cross-shard: call a view function on alice's contract from zoe's node.
    let result = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::CallFunction {
                        account_id: alice.clone(),
                        method_name: "log_something".to_string(),
                        args: FunctionArgs::from(vec![]),
                    },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::CallResult(_) => {}
        other => panic!("expected CallResult, got: {other:?}"),
    }
}

/// Standard `query` ViewGasKeyNonces should be forwarded to the right shard.
#[test]
fn test_rpc_query_view_gas_key_nonces_forwarding() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    let public_key =
        near_primitives::test_utils::create_user_test_signer(alice.as_ref()).public_key();
    // Cross-shard: query alice's gas key nonces from zoe's node.
    // Genesis accounts don't have gas keys, so this returns UNKNOWN_GAS_KEY.
    // The important thing is we get that error (not UNAVAILABLE_SHARD), proving forwarding works.
    let result = h.env.runner_for_account(&h.zoe_node).run_jsonrpc_query(
        |client| {
            client.query(RpcQueryRequest {
                block_reference: BlockReference::Finality(Finality::None),
                request: QueryRequest::ViewGasKeyNonces {
                    account_id: alice.clone(),
                    public_key: public_key.clone(),
                },
            })
        },
        Duration::seconds(5),
    );
    let err = result.unwrap_err();
    assert!(err.message.contains("Server error"), "unexpected error: {err:?}");
    let error_struct = err.error_struct.unwrap();
    match error_struct {
        near_jsonrpc_primitives::errors::RpcErrorKind::HandlerError(value) => {
            assert_eq!(value["name"], "UNKNOWN_GAS_KEY");
        }
        other => panic!("expected HandlerError, got: {other:?}"),
    }
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

    // Cross-shard: query global contract code by alice's account_id from zoe's node.
    let result = h
        .env
        .runner_for_account(&h.zoe_node)
        .run_jsonrpc_query(
            |client| {
                client.query(RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewGlobalContractCodeByAccountId {
                        account_id: alice.clone(),
                    },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    match result.kind {
        QueryResponseKind::ViewCode(view) => {
            assert!(!view.code.is_empty(), "expected non-empty global contract code");
        }
        other => panic!("expected ViewCode, got: {other:?}"),
    }
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
        .run_jsonrpc_query(
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
            assert!(
                error_msg.contains("MethodNotFound"),
                "unexpected error message: {error_msg}"
            );
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
        h.env.runner_for_account(node_id).run_jsonrpc_query(
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

/// Assert that an RpcError is a HandlerError with the expected error name.
fn assert_rpc_error(err: &RpcError, expected_name: &str) {
    match err.error_struct.as_ref().expect("expected error_struct") {
        RpcErrorKind::HandlerError(val) => {
            assert_eq!(val["name"].as_str(), Some(expected_name), "unexpected error: {val}");
        }
        other => panic!("expected HandlerError({expected_name}), got: {other:?}"),
    }
}
