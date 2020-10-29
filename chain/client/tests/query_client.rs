use actix::System;
use futures::{future, FutureExt};

use near_client::test_utils::setup_no_network;
use near_client::{
    GetBlock, GetBlockWithMerkleTree, GetExecutionOutcomesForBlock, Query, Status, TxStatus,
};
use near_crypto::{InMemorySigner, KeyType};
use near_logger_utils::init_test_logger;
use near_network::{NetworkClientMessages, NetworkClientResponses, PeerInfo};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockReference, EpochId};
use near_primitives::utils::to_timestamp;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{FinalExecutionOutcomeViewEnum, QueryRequest, QueryResponseKind};
use num_rational::Rational;
use std::time::Duration;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    System::run(|| {
        let (_, view_client) = setup_no_network(vec!["test"], "other", true, true);
        actix::spawn(
            view_client
                .send(Query::new(
                    BlockReference::latest(),
                    QueryRequest::ViewAccount { account_id: "test".to_owned() },
                ))
                .then(|res| {
                    match res.unwrap().unwrap().unwrap().kind {
                        QueryResponseKind::ViewAccount(_) => (),
                        _ => panic!("Invalid response"),
                    }
                    System::current().stop();
                    future::ready(())
                }),
        );
    })
    .unwrap();
}

/// When we receive health check and the latest block's timestamp is in the future, the client
/// should not crash.
#[test]
fn query_status_not_crash() {
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_no_network(vec!["test"], "other", true, false);
        let signer = InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test");
        actix::spawn(view_client.send(GetBlockWithMerkleTree::latest()).then(move |res| {
            let (block, mut block_merkle_tree) = res.unwrap().unwrap();
            let header: BlockHeader = block.header.clone().into();
            block_merkle_tree.insert(*header.hash());
            let mut next_block = Block::produce(
                PROTOCOL_VERSION,
                &header,
                block.header.height + 1,
                block.chunks.into_iter().map(|c| c.into()).collect(),
                EpochId(block.header.next_epoch_id),
                EpochId(block.header.hash),
                vec![],
                Rational::from_integer(0),
                0,
                100,
                None,
                vec![],
                vec![],
                &signer,
                block.header.next_bp_hash,
                block_merkle_tree.root(),
            );
            next_block.mut_header().get_mut().inner_lite.timestamp =
                to_timestamp(next_block.header().timestamp() + chrono::Duration::seconds(60));
            next_block.mut_header().resign(&signer);

            actix::spawn(
                client
                    .send(NetworkClientMessages::Block(next_block, PeerInfo::random().id, false))
                    .then(move |_| {
                        actix::spawn(client.send(Status { is_health_check: true }).then(
                            move |_| {
                                System::current().stop();
                                future::ready(())
                            },
                        ));
                        future::ready(())
                    }),
            );
            future::ready(())
        }));
        near_network::test_utils::wait_or_panic(5000);
    })
    .unwrap();
}

#[test]
fn test_execution_outcome_for_chunk() {
    init_test_logger();
    System::run(|| {
        let (client, view_client) = setup_no_network(vec!["test"], "test", true, false);
        let signer = InMemorySigner::from_seed("test", KeyType::ED25519, "test");

        actix::spawn(async move {
            let block_hash =
                view_client.send(GetBlock::latest()).await.unwrap().unwrap().header.hash;

            let transaction = SignedTransaction::send_money(
                1,
                "test".to_string(),
                "near".to_string(),
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

            actix::clock::delay_for(Duration::from_millis(500)).await;

            let execution_outcome = view_client
                .send(TxStatus {
                    tx_hash,
                    signer_account_id: "test".to_string(),
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
    })
    .unwrap();
}
