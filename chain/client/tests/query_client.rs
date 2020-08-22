use actix::System;
use futures::{future, FutureExt};

use near_client::test_utils::setup_no_network;
use near_client::{GetBlockWithMerkleTree, Query, Status};
use near_crypto::KeyType;
use near_logger_utils::init_test_logger;
use near_network::{NetworkClientMessages, PeerInfo};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::types::{BlockReference, EpochId};
use near_primitives::utils::to_timestamp;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use num_rational::Rational;

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
