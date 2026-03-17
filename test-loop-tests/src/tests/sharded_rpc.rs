use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::Message;
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest};
use near_jsonrpc_primitives::types::view_account::RpcViewAccountRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockReference, Finality};
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
        let clients = vec![rpc0.clone(), rpc1.clone(), validator0];
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .config_modifier(move |config, client_index| match client_index {
                0 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc0_shard]),
                1 => config.tracked_shards_config = TrackedShardsConfig::Shards(vec![rpc1_shard]),
                _ => {}
            })
            .build();

        let (alice_node, zoe_node) =
            if alice_shard == rpc0_shard.shard_id() { (rpc0, rpc1) } else { (rpc1, rpc0) };

        Self { env, alice, zoe, alice_node, zoe_node }
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
