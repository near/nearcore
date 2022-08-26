use actix::System;
use futures::{future, FutureExt};
use near_chain::test_utils::ValidatorSchedule;
use near_primitives::merkle::PartialMerkleTree;
use std::sync::Arc;
use std::time::Duration;

use crate::test_utils::{setup_mock_all_validators, setup_no_network, setup_only_view};
use crate::{
    GetBlock, GetBlockWithMerkleTree, GetExecutionOutcomesForBlock, Query, QueryError, Status,
    TxStatus,
};
use near_actix_test_utils::run_actix;
use near_chain_configs::DEFAULT_GC_NUM_EPOCHS_TO_KEEP;
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_test_logger;
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{
    NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_network::types::{
    NetworkViewClientMessages, NetworkViewClientResponses, PeerInfo,
};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::time::Utc;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockId, BlockReference, EpochId};
use near_primitives::utils::to_timestamp;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{FinalExecutionOutcomeViewEnum, QueryRequest, QueryResponseKind};
use num_rational::Ratio;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    run_actix(async {
        let (_, view_client) =
            setup_no_network(vec!["test".parse().unwrap()], "other".parse().unwrap(), true, true);
        actix::spawn(
            view_client
                .send(Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
                ))
                .then(|res| {
                    match res.unwrap().unwrap().kind {
                        QueryResponseKind::ViewAccount(_) => (),
                        _ => panic!("Invalid response"),
                    }
                    System::current().stop();
                    future::ready(())
                }),
        );
    });
}

/// When we receive health check and the latest block's timestamp is in the future, the client
/// should not crash.
#[test]
fn query_status_not_crash() {
    init_test_logger();
    run_actix(async {
        let (client, view_client) =
            setup_no_network(vec!["test".parse().unwrap()], "other".parse().unwrap(), true, false);
        let signer =
            InMemoryValidatorSigner::from_seed("test".parse().unwrap(), KeyType::ED25519, "test");
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
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
                None,
            );
            next_block.mut_header().get_mut().inner_lite.timestamp =
                to_timestamp(next_block.header().timestamp() + chrono::Duration::seconds(60));
            next_block.mut_header().resign(&signer);

            actix::spawn(
                client
                    .send(NetworkClientMessages::Block(next_block, PeerInfo::random().id, false))
                    .then(move |_| {
                        actix::spawn(
                            client.send(Status { is_health_check: true, detailed: false }).then(
                                move |_| {
                                    System::current().stop();
                                    future::ready(())
                                },
                            ),
                        );
                        future::ready(())
                    }),
            );
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    });
}

#[test]
fn test_execution_outcome_for_chunk() {
    init_test_logger();
    run_actix(async {
        let (client, view_client) =
            setup_no_network(vec!["test".parse().unwrap()], "test".parse().unwrap(), true, false);
        let signer = InMemorySigner::from_seed("test".parse().unwrap(), KeyType::ED25519, "test");

        actix::spawn(async move {
            let block_hash =
                view_client.send(GetBlock::latest()).await.unwrap().unwrap().header.hash;

            let transaction = SignedTransaction::send_money(
                1,
                "test".parse().unwrap(),
                "near".parse().unwrap(),
                &signer,
                10,
                block_hash,
            );
            let tx_hash = transaction.get_hash();
            let res = client
                .send(NetworkClientMessages::Transaction {
                    transaction,
                    is_forwarded: false,
                    check_only: false,
                })
                .await
                .unwrap();
            assert!(matches!(res, NetworkClientResponses::ValidTx));

            actix::clock::sleep(Duration::from_millis(500)).await;
            let execution_outcome = view_client
                .send(TxStatus {
                    tx_hash,
                    signer_account_id: "test".parse().unwrap(),
                    fetch_receipt: false,
                })
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let feo = match execution_outcome {
                FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(outcome) => outcome,
                FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(outcome) => {
                    outcome.into()
                }
            };

            let mut execution_outcomes_in_block = view_client
                .send(GetExecutionOutcomesForBlock {
                    block_hash: feo.transaction_outcome.block_hash,
                })
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
            vs,
            10000000,
            "test".parse().unwrap(),
            true,
            200,
            400,
            false,
            true,
            false,
            Arc::new(MockPeerManagerAdapter::default()),
            100,
            Utc::now(),
        );
        actix::spawn(async move {
            actix::clock::sleep(Duration::from_millis(500)).await;
            let block_hash = view_client
                .send(GetBlock(BlockReference::BlockId(BlockId::Height(0))))
                .await
                .unwrap()
                .unwrap()
                .header
                .hash;
            for _ in 0..30 {
                let res = view_client
                    .send(NetworkViewClientMessages::StateRequestHeader {
                        shard_id: 0,
                        sync_hash: block_hash,
                    })
                    .await
                    .unwrap();
                assert!(matches!(res, NetworkViewClientResponses::StateResponse(_)));
            }

            // immediately query again, should be rejected
            let res = view_client
                .send(NetworkViewClientMessages::StateRequestHeader {
                    shard_id: 0,
                    sync_hash: block_hash,
                })
                .await
                .unwrap();
            assert!(matches!(res, NetworkViewClientResponses::NoResponse));
            actix::clock::sleep(Duration::from_secs(40)).await;
            let res = view_client
                .send(NetworkViewClientMessages::StateRequestHeader {
                    shard_id: 0,
                    sync_hash: block_hash,
                })
                .await
                .unwrap();
            assert!(matches!(res, NetworkViewClientResponses::StateResponse(_)));
            System::current().stop();
        });
        near_network::test_utils::wait_or_panic(50000);
    });
}

#[test]
/// When querying data which was garbage collected on a node it returns
/// `QueryError::GarbageCollectedBlock`.
fn test_garbage_collection() {
    init_test_logger();
    run_actix(async {
        let block_prod_time = 100;
        let epoch_length = 5;
        let target_height = epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1);
        let vs = ValidatorSchedule::new().num_shards(2).block_producers_per_epoch(vec![vec![
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
        ]]);

        setup_mock_all_validators(
            vs,
            vec![PeerInfo::random(), PeerInfo::random()],
            true,
            block_prod_time,
            false,
            false,
            epoch_length,
            true,
            vec![false, true], // first validator non-archival, second archival
            vec![true, true],
            true,
            Box::new(
                move |conns,
                      _,
                      msg: &PeerManagerMessageRequest|
                      -> (PeerManagerMessageResponse, bool) {
                    if let NetworkRequests::Block { block } = msg.as_network_requests_ref() {
                        if block.header().height() > target_height {
                            let view_client_non_archival = &conns[0].1;
                            let view_client_archival = &conns[1].1;
                            let mut tests = vec![];

                            // Recent data is present on all nodes (archival or not).
                            let prev_height = block.header().prev_height().unwrap();
                            for (_, view_client) in conns.iter() {
                                tests.push(actix::spawn(
                                    view_client
                                        .send(Query::new(
                                            BlockReference::BlockId(BlockId::Height(prev_height)),
                                            QueryRequest::ViewAccount {
                                                account_id: "test1".parse().unwrap(),
                                            },
                                        ))
                                        .then(move |res| {
                                            let res = res.unwrap().unwrap();
                                            match res.kind {
                                                QueryResponseKind::ViewAccount(_) => (),
                                                _ => panic!("Invalid response"),
                                            }
                                            futures::future::ready(())
                                        }),
                                ));
                            }

                            // On non-archival node old data is garbage collected.
                            tests.push(actix::spawn(
                                view_client_non_archival
                                    .send(Query::new(
                                        BlockReference::BlockId(BlockId::Height(1)),
                                        QueryRequest::ViewAccount {
                                            account_id: "test1".parse().unwrap(),
                                        },
                                    ))
                                    .then(move |res| {
                                        let res = res.unwrap();
                                        match res {
                                            Err(err) => assert!(matches!(
                                                err,
                                                QueryError::GarbageCollectedBlock { .. }
                                            )),
                                            Ok(_) => panic!("Unexpected Ok variant"),
                                        }
                                        futures::future::ready(())
                                    }),
                            ));

                            // On archival node old data is _not_ garbage collected.
                            tests.push(actix::spawn(
                                view_client_archival
                                    .send(Query::new(
                                        BlockReference::BlockId(BlockId::Height(1)),
                                        QueryRequest::ViewAccount {
                                            account_id: "test1".parse().unwrap(),
                                        },
                                    ))
                                    .then(move |res| {
                                        let res = res.unwrap().unwrap();
                                        match res.kind {
                                            QueryResponseKind::ViewAccount(_) => (),
                                            _ => panic!("Invalid response"),
                                        }
                                        futures::future::ready(())
                                    }),
                            ));

                            actix::spawn(futures::future::join_all(tests).then(|_| {
                                System::current().stop();
                                futures::future::ready(())
                            }));
                        }
                    }
                    (NetworkResponses::NoResponse.into(), true)
                },
            ),
        );

        near_network::test_utils::wait_or_panic(block_prod_time * target_height * 2 + 2000);
    })
}
