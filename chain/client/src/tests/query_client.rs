use crate::test_utils::{setup_no_network, setup_only_view};
use crate::{
    GetBlock, GetBlockWithMerkleTree, GetExecutionOutcomesForBlock, Query, Status, TxStatus,
};
use actix::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::messaging::IntoMultiSender;
use near_async::time::{Clock, Duration};
use near_chain::test_utils::ValidatorSchedule;
use near_crypto::{InMemorySigner, KeyType};
use near_network::client::{
    BlockResponse, ProcessTxRequest, ProcessTxResponse, StateRequestHeader,
};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::PeerInfo;
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockId, BlockReference, EpochId};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use num_rational::Ratio;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            true,
        );
        let actor = actor_handles.view_client_actor.send(
            Query::new(
                BlockReference::latest(),
                QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            )
            .with_span_context(),
        );
        let actor = actor.then(|res| {
            match res.unwrap().unwrap().kind {
                QueryResponseKind::ViewAccount(_) => (),
                _ => panic!("Invalid response"),
            }
            System::current().stop();
            future::ready(())
        });
        actix::spawn(actor);
    });
}

/// When we receive health check and the latest block's timestamp is in the future, the client
/// should not crash.
#[test]
fn query_status_not_crash() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            false,
        );
        let signer = create_test_signer("test");
        let actor = actor_handles
            .view_client_actor
            .send(GetBlockWithMerkleTree::latest().with_span_context());
        let actor = actor.then(move |res| {
            let (block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            let header: BlockHeader = block.header.clone().into();
            block_merkle_tree.insert(*header.hash());
            let mut next_block = Block::produce(
                PROTOCOL_VERSION,
                PROTOCOL_VERSION,
                &header,
                block.header.height + 1,
                header.block_ordinal() + 1,
                block.chunks.into_iter().map(|c| c.into()).collect(),
                vec![],
                EpochId(block.header.next_epoch_id),
                EpochId(block.header.hash),
                None,
                vec![],
                Ratio::from_integer(0),
                0,
                100,
                None,
                vec![],
                vec![],
                &signer,
                block.header.next_bp_hash,
                block_merkle_tree.root(),
                Clock::real(),
            );
            next_block.mut_header().get_mut().inner_lite.timestamp =
                (next_block.header().timestamp() + Duration::seconds(60)).unix_timestamp_nanos()
                    as u64;
            next_block.mut_header().resign(&signer);

            actix::spawn(
                actor_handles
                    .client_actor
                    .send(
                        BlockResponse {
                            block: next_block,
                            peer_id: PeerInfo::random().id,
                            was_requested: false,
                        }
                        .with_span_context(),
                    )
                    .then(move |_| {
                        actix::spawn(
                            actor_handles
                                .client_actor
                                .send(
                                    Status { is_health_check: true, detailed: false }
                                        .with_span_context(),
                                )
                                .then(move |_| {
                                    System::current().stop();
                                    future::ready(())
                                }),
                        );
                        future::ready(())
                    }),
            );
            future::ready(())
        });
        actix::spawn(actor);
        near_network::test_utils::wait_or_panic(5000);
    });
}

#[test]
fn test_execution_outcome_for_chunk() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
        );
        let signer = InMemorySigner::from_seed("test".parse().unwrap(), KeyType::ED25519, "test");

        actix::spawn(async move {
            let block_hash = actor_handles
                .view_client_actor
                .send(GetBlock::latest().with_span_context())
                .await
                .unwrap()
                .unwrap()
                .header
                .hash;

            let transaction = SignedTransaction::send_money(
                1,
                "test".parse().unwrap(),
                "near".parse().unwrap(),
                &signer.into(),
                10,
                block_hash,
            );
            let tx_hash = transaction.get_hash();
            let res = actor_handles
                .client_actor
                .send(
                    ProcessTxRequest { transaction, is_forwarded: false, check_only: false }
                        .with_span_context(),
                )
                .await
                .unwrap();
            assert!(matches!(res, ProcessTxResponse::ValidTx));

            actix::clock::sleep(std::time::Duration::from_millis(500)).await;
            let block_hash = actor_handles
                .view_client_actor
                .send(
                    TxStatus {
                        tx_hash,
                        signer_account_id: "test".parse().unwrap(),
                        fetch_receipt: false,
                    }
                    .with_span_context(),
                )
                .await
                .unwrap()
                .unwrap()
                .into_outcome()
                .unwrap()
                .transaction_outcome
                .block_hash;

            let mut execution_outcomes_in_block = actor_handles
                .view_client_actor
                .send(GetExecutionOutcomesForBlock { block_hash }.with_span_context())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(execution_outcomes_in_block.len(), 1);
            let outcomes = execution_outcomes_in_block.remove(&0).unwrap();
            assert_eq!(outcomes[0].id, tx_hash);
            System::current().stop();
        });
        near_network::test_utils::wait_or_panic(5000);
    });
}

#[test]
fn test_state_request() {
    run_actix(async {
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        let view_client = setup_only_view(
            Clock::real(),
            vs,
            10000000,
            "test".parse().unwrap(),
            true,
            200,
            400,
            false,
            true,
            false,
            true,
            MockPeerManagerAdapter::default().into_multi_sender(),
            100,
        );
        actix::spawn(async move {
            actix::clock::sleep(std::time::Duration::from_millis(500)).await;
            let block_hash = view_client
                .send(GetBlock(BlockReference::BlockId(BlockId::Height(0))).with_span_context())
                .await
                .unwrap()
                .unwrap()
                .header
                .hash;
            for _ in 0..30 {
                let res = view_client
                    .send(
                        StateRequestHeader { shard_id: 0, sync_hash: block_hash }
                            .with_span_context(),
                    )
                    .await
                    .unwrap();
                assert!(res.is_some());
            }

            // immediately query again, should be rejected
            let res = view_client
                .send(StateRequestHeader { shard_id: 0, sync_hash: block_hash }.with_span_context())
                .await
                .unwrap();
            assert!(res.is_none());
            actix::clock::sleep(std::time::Duration::from_secs(40)).await;
            let res = view_client
                .send(StateRequestHeader { shard_id: 0, sync_hash: block_hash }.with_span_context())
                .await
                .unwrap();
            assert!(res.is_some());
            System::current().stop();
        });
        near_network::test_utils::wait_or_panic(50000);
    });
}
