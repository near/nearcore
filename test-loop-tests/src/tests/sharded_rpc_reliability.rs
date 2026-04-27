use crate::setup::rpc::RpcFault;
use crate::utils::sharded_rpc::{ThreeNodeHarness, TwoShardHarness, assert_rpc_error};
use near_async::time::Duration;
use near_jsonrpc::client::ChunkId;
use near_jsonrpc_primitives::errors::RpcErrorKind;
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockByTypeResponse,
    RpcStateChangesInBlockRequest, RpcStateChangesInBlockResponse,
};
use near_jsonrpc_primitives::types::chunks::{ChunkReference, RpcChunkRequest};
use near_jsonrpc_primitives::types::query::RpcQueryRequest;
use near_jsonrpc_primitives::types::receipts::{ReceiptReference, RpcReceiptRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockId, BlockReference, Finality};
use near_primitives::views::{QueryRequest, StateChangesRequestView};
use near_store::ShardUId;

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

/// Same as test_rpc_coordinator_header_bypass but for the chunk method.
/// A coordinator chunk request sent to a node that doesn't track the shard
/// must fail locally (UNKNOWN_CHUNK) instead of being forwarded again.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_chunk_coordinator_header_bypass() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let shard_layout = ShardLayout::multi_shard(2, 1);
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let head_height = h.env.node_for_account(&h.validator).head().height;
    let zoe_node = h.zoe_node.clone();

    // Baseline: a normal chunk request for shard 0 from zoe's node succeeds
    // because the pool coordinator forwards it to alice's node.
    let normal_result = h.env.runner_for_account(&zoe_node).run_with_jsonrpc_client(
        |client| {
            client.chunk(ChunkId::BlockShardId(
                BlockId::Height(head_height),
                shard_uids[0].shard_id(),
            ))
        },
        Duration::seconds(5),
    );
    assert!(
        normal_result.is_ok(),
        "baseline cross-shard chunk query should succeed via forwarding"
    );

    // Now send the same query with the coordinator header. Zoe's node doesn't
    // track shard 0, so without forwarding it must fail.
    let request = Message::request(
        "chunk".to_string(),
        serde_json::to_value(RpcChunkRequest {
            chunk_reference: ChunkReference::BlockShardId {
                block_id: BlockId::Height(head_height),
                shard_id: shard_uids[0].shard_id(),
            },
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

    match response {
        Message::Response(resp) => {
            let err = resp.result.expect_err("coordinator chunk request should fail locally");
            assert_rpc_error(&err, "UNKNOWN_CHUNK");
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// A chunk query by ChunkHash for a chunk that doesn't exist anywhere falls
/// through the partial-chunk-store resolution and triggers the
/// ParallelTakeFirst fallback. All nodes return UnknownChunk; the strategy
/// must surface UNKNOWN_CHUNK rather than an unrelated error or a timeout.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_rpc_chunk_bogus_hash_parallel_fallback() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let bogus_chunk_hash = CryptoHash::hash_bytes(b"bogus chunk hash");

    for node_id in [&h.alice_node.clone(), &h.zoe_node.clone()] {
        let err = h
            .env
            .runner_for_account(node_id)
            .run_with_jsonrpc_client(
                |client| client.chunk(ChunkId::Hash(bogus_chunk_hash)),
                Duration::seconds(5),
            )
            .unwrap_err();
        assert_rpc_error(&err, "UNKNOWN_CHUNK");
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

/// Coordinator-flagged `block_effects` requests must be processed locally
/// (no scatter-gather), so a node that doesn't track a shard returns only
/// its local changes — not changes from other shards. This prevents
/// forwarding loops in the scatter-gather protocol.
#[test]
fn test_rpc_block_effects_coordinator_bypass() {
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
    let block_hash = outcome.block_hash;

    let zoe_node = h.zoe_node.clone();

    // Baseline: a normal request from zoe_node gets changes for both shards
    // via scatter-gather.
    let result = h
        .env
        .runner_for_account(&zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                })
            },
            Duration::seconds(5),
        )
        .unwrap();
    let normal_accounts: Vec<_> = result.changes.iter().map(|c| c.account_id().clone()).collect();
    assert!(
        normal_accounts.iter().any(|a| a == &alice),
        "baseline: scatter-gather should include alice's changes, got: {normal_accounts:?}",
    );

    // Coordinator-flagged request from zoe_node: processed locally, no scatter-gather.
    // Zoe's node only tracks zoe's shard, so alice's changes should be missing.
    let request = Message::request(
        "block_effects".to_string(),
        serde_json::to_value(RpcStateChangesInBlockRequest {
            block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
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

    match response {
        Message::Response(resp) => {
            let value = resp.result.expect("coordinator request should succeed with local data");
            let parsed: RpcStateChangesInBlockByTypeResponse =
                serde_json::from_value(value).expect("failed to parse response");
            let coord_accounts: Vec<_> =
                parsed.changes.iter().map(|c| c.account_id().clone()).collect();
            assert!(
                !coord_accounts.contains(&alice),
                "coordinator bypass: should NOT include alice's changes (wrong shard), got: {coord_accounts:?}",
            );
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// Coordinator-flagged `changes` requests must be processed locally. When the
/// request targets an account whose shard this node doesn't track, the node
/// must reject with SHARD_NOT_APPLIED rather than silently returning empty
/// data — otherwise partial results look identical to genuinely empty ones.
#[test]
fn test_rpc_changes_coordinator_bypass() {
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
    let block_hash = outcome.block_hash;

    let zoe_node = h.zoe_node.clone();

    // Baseline: normal request from zoe_node for alice's changes succeeds via scatter-gather.
    let params = serde_json::to_value(RpcStateChangesInBlockByTypeRequest {
        block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
        state_changes_request: StateChangesRequestView::AccountChanges {
            account_ids: vec![alice.clone()],
        },
    })
    .unwrap();
    let response = h
        .env
        .runner_for_account(&zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                client.transport.send_jsonrpc_request(
                    Message::request("changes".to_string(), params.clone()),
                    false,
                )
            },
            Duration::seconds(5),
        )
        .unwrap();
    match &response {
        Message::Response(resp) => {
            let value = resp.result.as_ref().expect("baseline should succeed");
            let parsed: RpcStateChangesInBlockResponse =
                serde_json::from_value(value.clone()).expect("failed to parse");
            let accounts: Vec<_> =
                parsed.changes.iter().map(|c| c.value.account_id().clone()).collect();
            assert!(
                accounts.contains(&alice),
                "baseline: scatter-gather should include alice's changes, got: {accounts:?}",
            );
        }
        other => panic!("expected Response, got: {other:?}"),
    }

    // Coordinator-flagged: zoe_node processes locally and must reject with
    // SHARD_NOT_APPLIED because alice's shard isn't tracked here.
    let coord_response = h
        .env
        .runner_for_account(&zoe_node)
        .run_with_jsonrpc_client(
            |client| {
                client.transport.send_jsonrpc_request(
                    Message::request("changes".to_string(), params.clone()),
                    true,
                )
            },
            Duration::seconds(5),
        )
        .unwrap();
    match coord_response {
        Message::Response(resp) => {
            let err = resp.result.expect_err(
                "coordinator request for an untracked shard must fail with SHARD_NOT_APPLIED",
            );
            assert_rpc_error(&err, "SHARD_NOT_APPLIED");
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}

/// A `block_effects` request with a bogus block hash must return UNKNOWN_BLOCK,
/// not a timeout or internal error. The scatter-gather coordinator resolves the
/// block before fanning out, so a bad hash fails immediately.
#[test]
fn test_rpc_block_effects_bogus_block_hash() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let bogus_hash = CryptoHash::hash_bytes(b"bogus block hash");
    let alice_node = h.alice_node.clone();

    let err = h
        .env
        .runner_for_account(&alice_node)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(bogus_hash)),
                })
            },
            Duration::seconds(5),
        )
        .unwrap_err();
    assert_rpc_error(&err, "UNKNOWN_BLOCK");
}

/// Drive a scatter-gather `block_effects` from rpc1 with rpc0 faulted out, and
/// assert the response matches the validator's (full, baseline) response.
///
/// When the first-picked remote for a shard is unresponsive, `run_scatter_gather`
/// must exclude it on the per-peer timeout and retry via a backup node. rpc0's
/// transport is wired to hang, so the coordinator's 10s per-peer timeout fires
/// and the retry picks rpc2. Both failure modes drive the same retry path; this
/// test covers the timeout branch.
///
/// Same retry path, but rpc0 fails immediately with an internal error
/// (simulating a stale node) rather than timing out. The retry loop must still
/// fall back to rpc2.
#[test]
fn test_rpc_block_effects_scatter_gather_retry() {
    init_test_logger();
    let mut h = ThreeNodeHarness::new();

    let alice = h.alice.clone();
    let zoe = h.zoe.clone();
    let validator = h.validator.clone();
    let rpc1 = h.rpc1.clone();

    let tx = h.env.node_for_account(&validator).tx_send_money(&alice, &zoe, Balance::from_near(1));
    let tx_hash = tx.get_hash();
    h.env.node_for_account(&validator).submit_tx(tx);

    let target_height = h.env.node_for_account(&validator).head().height + 10;
    h.env.runner_for_account(&validator).run_until_executed_height(target_height);

    let outcome =
        h.env.node_for_account(&validator).client().chain.get_execution_outcome(&tx_hash).unwrap();
    let block_hash = outcome.block_hash;

    // Baseline: validator tracks all shards so one call returns the full picture.
    let baseline = h
        .env
        .runner_for_account(&validator)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                })
            },
            Duration::seconds(5),
        )
        .expect("baseline block_effects query on validator");
    let mut want: Vec<_> = baseline.changes.iter().map(|c| c.account_id().clone()).collect();
    want.sort();

    // Case 1: rpc0 hangs, triggers per-peer timeout and retry via rpc2.
    let fault = RpcFault::Hang;
    let budget = Duration::seconds(30);
    *h.rpc0_fault.write() = Some(fault);

    let result = h
        .env
        .runner_for_account(&rpc1)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                })
            },
            budget,
        )
        .expect("scatter-gather should succeed via rpc2 after rpc0 is excluded");

    assert_eq!(result.block_hash, baseline.block_hash);
    let mut got: Vec<_> = result.changes.iter().map(|c| c.account_id().clone()).collect();
    got.sort();
    assert_eq!(got, want, "scatter-gather response must match validator baseline");
    assert!(got.contains(&alice), "retry via rpc2 should surface alice's changes; got: {got:?}",);

    // Case 2: rpc0 fails immediately. The retry loop must still exclude rpc0 and succeed via rpc2 within the budget.
    let fault = RpcFault::Fail("stale rpc0".to_string());
    let budget = Duration::seconds(10);
    *h.rpc0_fault.write() = Some(fault);

    let result = h
        .env
        .runner_for_account(&rpc1)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                })
            },
            budget,
        )
        .expect("scatter-gather should succeed via rpc2 after rpc0 is excluded");

    assert_eq!(result.block_hash, baseline.block_hash);
    let mut got: Vec<_> = result.changes.iter().map(|c| c.account_id().clone()).collect();
    got.sort();
    assert_eq!(got, want, "scatter-gather response must match validator baseline");
    assert!(got.contains(&alice), "retry via rpc2 should surface alice's changes; got: {got:?}",);
}

/// When every candidate for a shard fails, `run_scatter_gather` must exhaust
/// its retries and surface an error rather than returning a partial response
/// or hanging. Both rpc0 and rpc2 track alice's shard; faulting both leaves
/// the coordinator with no candidate after exclusion. The per-round
/// `one_node_per_group_of_shard` call then returns "No available host for
/// given shard" once rpc0 and rpc2 are excluded. The log trace (rpc0 failure
/// → retry → rpc2 failure → give-up) confirms both candidates were tried.
#[test]
fn test_rpc_block_effects_scatter_gather_all_nodes_fail() {
    init_test_logger();
    let mut h = ThreeNodeHarness::new();

    let alice = h.alice.clone();
    let zoe = h.zoe.clone();
    let validator = h.validator.clone();
    let rpc1 = h.rpc1.clone();

    let tx = h.env.node_for_account(&validator).tx_send_money(&alice, &zoe, Balance::from_near(1));
    let tx_hash = tx.get_hash();
    h.env.node_for_account(&validator).submit_tx(tx);

    let target_height = h.env.node_for_account(&validator).head().height + 10;
    h.env.runner_for_account(&validator).run_until_executed_height(target_height);

    let outcome =
        h.env.node_for_account(&validator).client().chain.get_execution_outcome(&tx_hash).unwrap();
    let block_hash = outcome.block_hash;

    *h.rpc0_fault.write() = Some(RpcFault::Fail("rpc0 down".to_string()));
    *h.rpc2_fault.write() = Some(RpcFault::Fail("rpc2 down".to_string()));

    let err = h
        .env
        .runner_for_account(&rpc1)
        .run_with_jsonrpc_client(
            |client| {
                client.block_effects(RpcStateChangesInBlockRequest {
                    block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                })
            },
            Duration::seconds(10),
        )
        .expect_err("scatter-gather must fail when no node can serve alice's shard");

    // Must be an InternalError — a RequestValidationError would abort the
    // whole request before retrying, so seeing one here would mean the retry
    // path never ran.
    match err.error_struct.as_ref().expect("expected error_struct") {
        RpcErrorKind::InternalError(val) => {
            let msg = val["info"]["error_message"].as_str().unwrap_or("");
            assert!(
                msg.contains("No available host"),
                "error should reflect the exhausted pool; got: {val}",
            );
        }
        other => panic!("expected InternalError, got: {other:?}"),
    }
}

/// A `changes` request with a bogus block hash must return UNKNOWN_BLOCK.
#[test]
fn test_rpc_changes_bogus_block_hash() {
    init_test_logger();
    let mut h = TwoShardHarness::new();

    let bogus_hash = CryptoHash::hash_bytes(b"bogus block hash");
    let alice_node = h.alice_node.clone();
    let alice = h.alice.clone();

    let params = serde_json::to_value(RpcStateChangesInBlockByTypeRequest {
        block_reference: BlockReference::BlockId(BlockId::Hash(bogus_hash)),
        state_changes_request: StateChangesRequestView::AccountChanges { account_ids: vec![alice] },
    })
    .unwrap();
    let response = h
        .env
        .runner_for_account(&alice_node)
        .run_with_jsonrpc_client(
            |client| {
                client.transport.send_jsonrpc_request(
                    Message::request("changes".to_string(), params.clone()),
                    false,
                )
            },
            Duration::seconds(5),
        )
        .unwrap();
    match response {
        Message::Response(resp) => {
            let err = resp.result.expect_err("bogus block hash should fail");
            assert_rpc_error(&err, "UNKNOWN_BLOCK");
        }
        other => panic!("expected Response, got: {other:?}"),
    }
}
