use crate::utils::sharded_rpc::{TwoShardHarness, assert_rpc_error};
use near_async::time::Duration;
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::query::RpcQueryRequest;
use near_jsonrpc_primitives::types::receipts::{ReceiptReference, RpcReceiptRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, Balance, BlockReference, Finality};
use near_primitives::views::QueryRequest;

/// A cross-shard query for a nonexistent account must return UNKNOWN_ACCOUNT,
/// not UNAVAILABLE_SHARD. The latter would mean the coordinator gave up instead
/// of forwarding to a node that tracks the account's shard.
#[test]
fn test_rpc_coordinator_sequential_retry() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let nonexistent: AccountId = "nonexistent.near".parse().unwrap();

    for node_id in [&h.alice_node, &h.zoe_node] {
        let err = h
            .env
            .runner_for_account(node_id)
            .run_jsonrpc_query(
                RpcQueryRequest {
                    block_reference: BlockReference::Finality(Finality::None),
                    request: QueryRequest::ViewAccount { account_id: nonexistent.clone() },
                },
                Duration::seconds(5),
            )
            .unwrap_err();
        assert_rpc_error(&err, "UNKNOWN_ACCOUNT");
    }
}

/// Requests marked with the coordinator header (RequestSource::Coordinator)
/// must be processed locally to prevent forwarding loops. When an RPC node
/// receives a coordinator request for a shard it doesn't track, it should
/// return UnavailableShard rather than forwarding again.
///
/// Contrast: the same query WITHOUT the coordinator header succeeds because
/// the pool coordinator forwards it to the node that tracks the shard.
#[test]
fn test_rpc_coordinator_header_bypass() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    let zoe_node = h.zoe_node.clone();

    // First, verify the baseline: a normal (non-coordinator) request for alice's
    // account from zoe's node succeeds because it gets forwarded to alice's node.
    let normal_result = h.env.runner_for_account(&zoe_node).run_jsonrpc_query(
        RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::None),
            request: QueryRequest::ViewAccount { account_id: alice.clone() },
        },
        Duration::seconds(5),
    );
    assert!(normal_result.is_ok(), "baseline cross-shard query should succeed via forwarding");

    // Now send the SAME query with the coordinator header. Zoe's node doesn't
    // track alice's shard, so without forwarding it must fail with UnavailableShard.
    let request = Message::request(
        "query".to_string(),
        serde_json::to_value(RpcQueryRequest {
            block_reference: BlockReference::Finality(Finality::None),
            request: QueryRequest::ViewAccount { account_id: alice },
        })
        .unwrap(),
    );

    let response = h
        .env
        .runner_for_account(&zoe_node)
        .run_with_jsonrpc_client(
            |client| client.transport.send_jsonrpc_request(request, true),
            Duration::seconds(5),
        )
        .unwrap();

    // The coordinator-flagged request was processed locally on zoe's node.
    // Since zoe's node doesn't track alice's shard, it must return UnavailableShard.
    match response {
        Message::Response(resp) => {
            let err =
                resp.result.expect_err("coordinator request should fail locally (no forwarding)");
            assert_rpc_error(&err, "UNAVAILABLE_SHARD");
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// ParallelTakeFirst fans out to all nodes. The receipt lives on one shard, so
/// the node that doesn't have it returns UnknownReceipt. The strategy must
/// return the successful result from the other node, not propagate the failure.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_parallel_take_first_partial_failure() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let alice = h.alice.clone();
    let zoe = h.zoe.clone();
    let validator = h.validator.clone();
    let tx = h.env.node_for_account(&validator).tx_send_money(&alice, &zoe, Balance::from_near(1));
    let tx_hash = tx.get_hash();
    h.env.node_for_account(&validator).submit_tx(tx);

    let target_height = h.env.node_for_account(&validator).head().height + 10;
    h.env.runner_for_account(&validator).run_until_executed_height(target_height);

    let outcome =
        h.env.node_for_account(&validator).client().chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_id = *outcome
        .outcome_with_id
        .outcome
        .receipt_ids
        .first()
        .expect("transfer should produce at least one receipt");

    let alice_node = h.alice_node.clone();
    let zoe_node = h.zoe_node.clone();

    for node_id in [&alice_node, &zoe_node] {
        let result = h
            .env
            .runner_for_account(node_id)
            .run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_receipt(RpcReceiptRequest {
                        receipt_reference: ReceiptReference { receipt_id },
                    })
                },
                Duration::seconds(5),
            )
            .unwrap_or_else(|e| panic!("receipt query from {node_id} failed: {e:?}"));
        assert_eq!(result.receipt_view.receipt_id, receipt_id);
    }
}
