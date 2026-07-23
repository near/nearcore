use crate::env::setup::setup_no_network;
use near_async::ActorSystem;
use near_async::messaging::CanSendAsync;
use near_async::time::{Clock, Duration};
use near_client::{
    GetBlock, GetBlockWithMerkleTree, GetExecutionOutcomesForBlock, Query, TxStatus,
    TxStatusOutcome,
};
use near_client_primitives::types::Status;
use near_crypto::InMemorySigner;
use near_network::client::{BlockResponse, ProcessTxRequest, ProcessTxResponse};
use near_network::types::PeerInfo;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockReference, EpochId, ShardId};
use near_primitives::views::{QueryRequest, QueryResponseKind};
use std::sync::Arc;

/// Query account from view client
#[tokio::test]
async fn query_client() {
    init_test_logger();
    let actor_system = ActorSystem::new();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
        vec!["test".parse().unwrap()],
        "other".parse().unwrap(),
        true,
        true,
    );
    let res = actor_handles
        .view_client_actor
        .send_async(Query::new(
            BlockReference::latest(),
            QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
        ))
        .await;
    match res.unwrap().unwrap().kind {
        QueryResponseKind::ViewAccount(_) => (),
        _ => panic!("Invalid response"),
    }
    actor_system.stop();
}

/// When we receive health check and the latest block's timestamp is in the future, the client
/// should not crash.
#[tokio::test]
async fn query_status_not_crash() {
    init_test_logger();
    let actor_system = ActorSystem::new();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
        vec!["test".parse().unwrap()],
        "other".parse().unwrap(),
        true,
        false,
    );
    let signer = Arc::new(create_test_signer("test"));
    let res = actor_handles.view_client_actor.send_async(GetBlockWithMerkleTree::latest()).await;
    let (block, block_merkle_tree) = res.unwrap().unwrap();
    let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
    let future_timestamp_nanos =
        (Clock::real().now_utc() + Duration::seconds(60)).unix_timestamp_nanos() as u64;
    let next_block = TestBlockBuilder::from_prev_block_view(Clock::real(), &block, signer)
        .epoch_id(EpochId(block.header.next_epoch_id))
        .next_epoch_id(EpochId(block.header.hash))
        .next_bp_hash(block.header.next_bp_hash)
        .block_merkle_tree(&mut block_merkle_tree)
        .max_gas_price(Balance::from_yoctonear(100))
        .timestamp_nanos(future_timestamp_nanos)
        .build();

    let _ = actor_handles
        .client_actor
        .send_async(
            BlockResponse {
                block: next_block,
                peer_id: PeerInfo::random().id,
                was_requested: false,
            }
            .span_wrap(),
        )
        .await;

    let _ = actor_handles
        .client_actor
        .send_async(Status { is_health_check: true, detailed: false }.span_wrap())
        .await;

    actor_system.stop();
}

#[tokio::test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
async fn test_execution_outcome_for_chunk() {
    init_test_logger();
    let actor_system = ActorSystem::new();
    let actor_handles = setup_no_network(
        Clock::real(),
        actor_system.clone(),
        vec!["test".parse().unwrap()],
        "test".parse().unwrap(),
        true,
        false,
    );
    let signer = InMemorySigner::test_signer(&"test".parse().unwrap());

    let block_hash = actor_handles
        .view_client_actor
        .send_async(GetBlock::latest())
        .await
        .unwrap()
        .unwrap()
        .header
        .hash;

    let transaction = SignedTransaction::send_money(
        1,
        "test".parse().unwrap(),
        "near".parse().unwrap(),
        &signer,
        Balance::from_yoctonear(10),
        block_hash,
    );
    let tx_hash = transaction.get_hash();
    let res = actor_handles
        .rpc_handler_actor
        .send_async(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
        .await
        .unwrap();
    assert!(matches!(res, ProcessTxResponse::ValidTx));

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let tx_status_outcome = actor_handles
        .view_client_actor
        .send_async(TxStatus {
            tx_hash,
            signer_account_id: "test".parse().unwrap(),
            fetch_receipt: false,
        })
        .await
        .unwrap()
        .unwrap();
    let TxStatusOutcome::Observed(tx_status) = tx_status_outcome else {
        panic!("expected the transaction to be observed");
    };
    let block_hash = (*tx_status).into_outcome().unwrap().transaction_outcome.block_hash;

    let mut execution_outcomes_in_block = actor_handles
        .view_client_actor
        .send_async(GetExecutionOutcomesForBlock { block_hash })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution_outcomes_in_block.len(), 1);
    let outcomes = execution_outcomes_in_block.remove(&ShardId::new(0)).unwrap();
    assert_eq!(outcomes[0].id, tx_hash);
    actor_system.stop();
}
