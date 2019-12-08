use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use actix::{Addr, System};
use futures::{future, Future};
use log::info;

use near_chain::ChainGenesis;
use near_client::test_utils::{setup_mock_all_validators, TestEnv};
use near_client::{ClientActor, GetBlock, ViewClientActor};
use near_network::types::PartialEncodedChunkRequestMsg;
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::test_utils::{heavy_test, init_test_logger};
use near_primitives::transaction::SignedTransaction;

#[test]
fn chunks_produced_and_distributed_all_in_all_shards() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(1, false, 1500);
    });
}

#[test]
fn chunks_produced_and_distributed_2_vals_per_shard() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(2, false, 1500);
    });
}

#[test]
fn chunks_produced_and_distributed_one_val_per_shard() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(4, false, 1500);
    });
}

#[test]
fn chunks_recovered_from_others() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(2, true, 1500);
    });
}

#[test]
#[should_panic]
fn chunks_recovered_from_full_timeout_too_short() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(4, true, 1500);
    });
}

#[test]
fn chunks_recovered_from_full() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(4, true, 4000);
    });
}

/// Runs block producing client and stops after network mock received seven blocks
/// Confirms that the blocks form a chain (which implies the chunks are distributed).
/// Confirms that the number of messages transmitting the chunks matches the expected number.
fn chunks_produced_and_distributed_common(
    validator_groups: u64,
    drop_from_1_to_4: bool,
    block_timeout: u64,
) {
    init_test_logger();
    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));
        let heights = Arc::new(RwLock::new(HashMap::new()));
        let heights1 = heights.clone();

        let height_to_hash = Arc::new(RwLock::new(HashMap::new()));

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

        let mut partial_chunk_msgs = 0;
        let mut partial_chunk_request_msgs = 0;

        let (_, conn) = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            validator_groups,
            true,
            block_timeout,
            false,
            false,
            5,
            Arc::new(RwLock::new(move |from_whom: String, msg: &NetworkRequests| {
                match msg {
                    NetworkRequests::Block { block } => {
                        check_height(block.hash(), block.header.inner_lite.height);
                        check_height(block.header.prev_hash, block.header.inner_lite.height - 1);

                        let mut height_to_hash = height_to_hash.write().unwrap();
                        height_to_hash.insert(block.header.inner_lite.height, block.hash());

                        println!(
                            "BLOCK {} HEIGHT {}; HEADER HEIGHTS: {} / {} / {} / {}; QUORUMS: {} / {}",
                            block.hash(),
                            block.header.inner_lite.height,
                            block.chunks[0].inner.height_created,
                            block.chunks[1].inner.height_created,
                            block.chunks[2].inner.height_created,
                            block.chunks[3].inner.height_created,
                            block.header.inner_rest.last_quorum_pre_vote,
                            block.header.inner_rest.last_quorum_pre_commit,
                        );

                        // Make sure blocks are finalized. 6 is the epoch boundary.
                        let h = block.header.inner_lite.height;
                        if h > 1 && h != 6 {
                            assert_eq!(block.header.inner_rest.last_quorum_pre_vote, *height_to_hash.get(&(h - 1)).unwrap());
                        }
                        if h > 2 && (h != 6 && h != 7) {
                            assert_eq!(block.header.inner_rest.last_quorum_pre_commit, *height_to_hash.get(&(h - 2)).unwrap());
                        }

                        if block.header.inner_lite.height > 1 {
                            for shard_id in 0..4 {
                                // If messages from 1 to 4 are dropped, 4 at their heights will
                                //    receive the block significantly later than the chunks, and
                                //    thus would discard the chunks
                                if !drop_from_1_to_4 || block.header.inner_lite.height % 4 != 3 {
                                    assert_eq!(
                                        block.header.inner_lite.height,
                                        block.chunks[shard_id].inner.height_created
                                    );
                                }
                            }
                        }

                        if block.header.inner_lite.height >= 8 {
                            println!("PREV BLOCK HASH: {}", block.header.prev_hash);
                            println!(
                                "STATS: responses: {} requests: {}",
                                partial_chunk_msgs, partial_chunk_request_msgs
                            );

                            System::current().stop();
                        }
                    }
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id: to_whom,
                        partial_encoded_chunk: _,
                    } => {
                        partial_chunk_msgs += 1;
                        if drop_from_1_to_4 && from_whom == "test1" && to_whom == "test4" {
                            println!("Dropping Partial Encoded Chunk Message from test1 to test4");
                            return (NetworkResponses::NoResponse, false);
                        }
                    }
                    NetworkRequests::PartialEncodedChunkResponse {
                        route_back: _,
                        partial_encoded_chunk: _,
                    } => {
                        partial_chunk_msgs += 1;
                    }
                    NetworkRequests::PartialEncodedChunkRequest {
                        account_id: to_whom,
                        request: _,
                    } => {
                        if drop_from_1_to_4 && from_whom == "test4" && to_whom == "test1" {
                            info!("Dropping Partial Encoded Chunk Request from test4 to test1");
                            return (NetworkResponses::NoResponse, false);
                        }
                        if drop_from_1_to_4 && from_whom == "test4" && to_whom == "test2" {
                            info!("Observed Partial Encoded Chunk Request from test4 to test2");
                        }
                        partial_chunk_request_msgs += 1;
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
    let request = PartialEncodedChunkRequestMsg {
        chunk_hash: block1.chunks[0].chunk_hash(),
        part_ords: vec![0],
        tracking_shards: HashSet::default(),
    };
    let client = &mut env.clients[0];
    client.shards_mgr.process_partial_encoded_chunk_request(
        request.clone(),
        CryptoHash::default(),
        client.chain.mut_store(),
    );
    assert!(env.network_adapters[0].pop().is_some());

    env.restart(0);
    let client = &mut env.clients[0];
    client.shards_mgr.process_partial_encoded_chunk_request(
        request,
        CryptoHash::default(),
        client.chain.mut_store(),
    );
    let response = env.network_adapters[0].pop().unwrap();
    if let NetworkRequests::PartialEncodedChunkResponse { partial_encoded_chunk, .. } = response {
        assert_eq!(partial_encoded_chunk.chunk_hash, block1.chunks[0].chunk_hash());
    } else {
        println!("{:?}", response);
        assert!(false);
    }
}

#[test]
fn test_orphan_chain() {
    init_integration_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    for i in 1..12 {
        env.produce_block(0, i);
        let last_block = env.clients[0].chain.get_block_by_height(i - 1).unwrap().clone();
        env.clients[1].chain.save_orphan(&last_block);
    }
    let block0 = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let orphans_missing_chunks = Arc::new(RwLock::new(vec![]));
    env.clients[1].chain.check_orphans_with_missing_chunks(&None, |missing_chunks| {
        orphans_missing_chunks.write().unwrap().push(missing_chunks);
    });
    let mut is_missing = false;
    for (parent_hash, missing_chunks) in orphans_missing_chunks.write().unwrap().drain(..) {
        println!("{:?} {:?}", parent_hash, missing_chunks);
        is_missing = true;
        assert_eq!(parent_hash, block0.header.hash);
        for chunk in missing_chunks.iter() {
            assert!(chunk.inner.height_created >= 1);
            assert!(chunk.inner.height_created <= 5);
        }
    }
    assert!(is_missing);
    // Checking that process_blocks_with_missing_chunks works
    let v = env.clients[1].process_blocks_with_missing_chunks(CryptoHash::default());
    println!("accepted blocks = {:?}", v.len());
}
