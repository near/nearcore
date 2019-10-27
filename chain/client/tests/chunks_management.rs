use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use actix::{Addr, System};
use futures::{future, Future};

use near_chain::ChainGenesis;
use near_client::test_utils::{setup_mock_all_validators, TestEnv};
use near_client::{ClientActor, GetBlock, ViewClientActor};
use near_network::types::{ChunkOnePartRequestMsg, PeerId};
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::heavy_test;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::SignedTransaction;

#[test]
fn chunks_produced_and_distributed_all_in_all_shards() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(1);
    });
}

#[test]
fn chunks_produced_and_distributed_2_vals_per_shard() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(2);
    });
}

#[test]
fn chunks_produced_and_distributed_one_val_per_shard() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(4);
    });
}

/// Runs block producing client and stops after network mock received seven blocks
/// Confirms that the blocks form a chain (which implies the chunks are distributed).
/// Confirms that the number of messages transmitting the chunks matches the expected number.
fn chunks_produced_and_distributed_common(validator_groups: u64) {
    let validators_per_shard = 4 / validator_groups;
    init_integration_logger();
    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));
        let heights = Arc::new(RwLock::new(HashMap::new()));
        let heights1 = heights.clone();

        let check_height =
            move |hash: CryptoHash, height| match heights1.write().unwrap().entry(hash.clone()) {
                Entry::Occupied(entry) => {
                    assert_eq!(*entry.get(), height);
                }
                Entry::Vacant(entry) => {
                    entry.insert(height);
                }
            };

        let validators = vec![vec!["test1", "test2", "test3", "test4"]];
        let key_pairs =
            vec![PeerInfo::random(), PeerInfo::random(), PeerInfo::random(), PeerInfo::random()];

        let mut one_part_msgs = 0;
        let mut part_msgs = 0;
        let mut part_request_msgs = 0;

        let (_, conn) = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validator_groups,
            true,
            1500,
            false,
            5,
            Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                match msg {
                    NetworkRequests::Block { block } => {
                        check_height(block.hash(), block.header.inner.height);
                        check_height(block.header.inner.prev_hash, block.header.inner.height - 1);

                        println!(
                            "BLOCK HEIGHT {}; HEADER HEIGHTS: {} / {} / {} / {}",
                            block.header.inner.height,
                            block.chunks[0].inner.height_created,
                            block.chunks[1].inner.height_created,
                            block.chunks[2].inner.height_created,
                            block.chunks[3].inner.height_created
                        );

                        if block.header.inner.height > 1 {
                            for shard_id in 0..4 {
                                assert_eq!(
                                    block.header.inner.height,
                                    block.chunks[shard_id].inner.height_created
                                );
                            }
                        }

                        if block.header.inner.height >= 6 {
                            println!("PREV BLOCK HASH: {}", block.header.inner.prev_hash);
                            println!(
                                "STATS: one_parts: {} part_requests: {} parts: {}",
                                one_part_msgs, part_request_msgs, part_msgs
                            );

                            // Because there is no response, we are resend requests for parts multiple times.
                            assert!(
                                part_request_msgs >= one_part_msgs * (validators_per_shard - 1)
                            );
                            assert!(part_msgs >= one_part_msgs * (validators_per_shard - 1));

                            System::current().stop();
                        }
                    }
                    NetworkRequests::ChunkOnePartMessage { account_id: _, header_and_part: _ }
                    | NetworkRequests::ChunkOnePartResponse { peer_id: _, header_and_part: _ } => {
                        one_part_msgs += 1;
                    }
                    NetworkRequests::ChunkPartRequest { account_id: _, part_request: _ } => {
                        part_request_msgs += 1;
                    }
                    NetworkRequests::ChunkPart { peer_id: _, part: _ } => {
                        part_msgs += 1;
                    }
                    _ => {}
                };
                (NetworkResponses::NoResponse, true)
            })),
        );
        *connectors.write().unwrap() = conn;

        let view_client = connectors.write().unwrap()[0].1.clone();
        actix::spawn(view_client.send(GetBlock::Best).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            let connectors_ = connectors.write().unwrap();
            connectors_[0]
                .0
                .do_send(NetworkClientMessages::Transaction(SignedTransaction::empty(block_hash)));
            connectors_[1]
                .0
                .do_send(NetworkClientMessages::Transaction(SignedTransaction::empty(block_hash)));
            connectors_[2]
                .0
                .do_send(NetworkClientMessages::Transaction(SignedTransaction::empty(block_hash)));
            future::ok(())
        }));
    })
    .unwrap();
}

#[test]
fn test_request_chunk_restart() {
    init_integration_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    for i in 1..4 {
        env.produce_block(0, i);
        env.network_adapters[0].pop();
    }
    let block1 = env.clients[0].chain.get_block_by_height(3).unwrap().clone();
    let request = ChunkOnePartRequestMsg {
        shard_id: 0,
        chunk_hash: block1.chunks[0].chunk_hash(),
        height: block1.header.inner.height,
        part_id: 0,
        tracking_shards: HashSet::default(),
    };
    let client = &mut env.clients[0];
    client
        .shards_mgr
        .process_chunk_one_part_request(request.clone(), PeerId::random(), client.chain.mut_store())
        .unwrap();
    assert!(env.network_adapters[0].pop().is_some());

    env.restart(0);
    let client = &mut env.clients[0];
    client
        .shards_mgr
        .process_chunk_one_part_request(request, PeerId::random(), client.chain.mut_store())
        .unwrap();
    // TODO(1434): should be some() with the same chunk.
    assert!(env.network_adapters[0].pop().is_none());
}
