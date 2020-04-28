use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use actix::{Addr, System};
use futures::{future, FutureExt};
use log::info;

use near_chain::ChainGenesis;
use near_client::test_utils::{setup_mock_all_validators, TestEnv};
use near_client::{ClientActor, GetBlock, ViewClientActor};
use near_crypto::KeyType;
use near_network::types::PartialEncodedChunkRequestMsg;
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::BlockHeader;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::{PartialEncodedChunk, ShardChunkHeader};
use near_primitives::test_utils::init_integration_logger;
use near_primitives::test_utils::{heavy_test, init_test_logger};
use near_primitives::transaction::SignedTransaction;
use near_primitives::validator_signer::InMemoryValidatorSigner;

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

/// The timeout for requesting chunk from others is 1s. 3000 block timeout means that a participant
/// that is otherwise ready to produce a block will wait for 3000/2 milliseconds for all the chunks.
/// We block all the communication from test1 to test4, and expect that in 1.5 seconds test4 will
/// give up on getting the part from test1 and will get it from test2 (who will have it because
/// `validator_groups=2`)
#[test]
fn chunks_recovered_from_others() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(2, true, 1500);
    });
}

/// Same test as above, but the number of validator groups is four, therefore test2 doesn't have the
/// part test4 needs. The only way test4 can recover the part is by reconstructing the whole chunk,
/// but they won't do it for the first 3 seconds, and 3s block_timeout means that the block producers
/// only wait for 3000/2 milliseconds until they produce a block with some chunks missing
#[test]
#[should_panic]
fn chunks_recovered_from_full_timeout_too_short() {
    heavy_test(|| {
        chunks_produced_and_distributed_common(4, true, 1500);
    });
}

/// Same test as above, but the timeout is sufficiently large for test4 now to reconstruct the full
/// chunk
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
        let height_to_epoch = Arc::new(RwLock::new(HashMap::new()));

        let check_height =
            move |hash: CryptoHash, height| match heights1.write().unwrap().entry(hash.clone()) {
                Entry::Occupied(entry) => {
                    assert_eq!(*entry.get(), height);
                }
                Entry::Vacant(entry) => {
                    entry.insert(height);
                }
            };

        let validators = vec![vec!["test1", "test2", "test3", "test4"],vec!["test5", "test6", "test7", "test8"]];
        let key_pairs = (0..8).map(|_| PeerInfo::random()).collect::<Vec<_>>();

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
            true,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            Arc::new(RwLock::new(Box::new(move |from_whom: String, msg: &NetworkRequests| {
                match msg {
                    NetworkRequests::Block { block } => {
                        check_height(block.hash(), block.header.inner_lite.height);
                        check_height(block.header.prev_hash, block.header.inner_lite.height - 1);

                        let h = block.header.inner_lite.height;

                        let mut height_to_hash = height_to_hash.write().unwrap();
                        height_to_hash.insert(h, block.hash());

                        let mut height_to_epoch = height_to_epoch.write().unwrap();
                        height_to_epoch.insert(h, block.header.inner_lite.epoch_id.clone());

                        println!(
                            "[{:?}]: BLOCK {} HEIGHT {}; HEADER HEIGHTS: {} / {} / {} / {};\nAPPROVALS: {:?}",
                            Instant::now(),
                            block.hash(),
                            block.header.inner_lite.height,
                            block.chunks[0].inner.height_created,
                            block.chunks[1].inner.height_created,
                            block.chunks[2].inner.height_created,
                            block.chunks[3].inner.height_created,
                            block.header.inner_rest.approvals,
                        );

                        if h > 1 {
                            // Make sure doomslug finality is computed correctly.
                            assert_eq!(block.header.inner_rest.last_ds_final_block, *height_to_hash.get(&(h - 1)).unwrap());

                            // Make sure epoch length actually corresponds to the desired epoch length
                            // The switches are expected at 0->1, 5->6 and 10->11
                            let prev_epoch_id = height_to_epoch.get(&(h - 1)).unwrap().clone();
                            assert_eq!(block.header.inner_lite.epoch_id == prev_epoch_id, h % 5 != 1);

                            // Make sure that the blocks leading to the epoch switch have twice as
                            // many approval slots
                            assert_eq!(block.header.inner_rest.approvals.len() == 8, h % 5 == 0 || h % 5 == 4);
                        }
                        if h > 2 {
                            // Make sure BFT finality is computed correctly
                            assert_eq!(block.header.inner_rest.last_final_block, *height_to_hash.get(&(h - 2)).unwrap());
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

                        if block.header.inner_lite.height >= 12 {
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
            }))),
        );
        *connectors.write().unwrap() = conn;

        let view_client = connectors.write().unwrap()[0].1.clone();
        actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
            let header: BlockHeader = res.unwrap().unwrap().header.into();
            let block_hash = header.hash;
            let connectors_ = connectors.write().unwrap();
            connectors_[0]
                .0
                .do_send(NetworkClientMessages::Transaction { transaction: SignedTransaction::empty(block_hash), is_forwarded:false, check_only: false });
            connectors_[1]
                .0
                .do_send(NetworkClientMessages::Transaction { transaction: SignedTransaction::empty(block_hash), is_forwarded:false, check_only: false });
            connectors_[2]
                .0
                .do_send(NetworkClientMessages::Transaction { transaction: SignedTransaction::empty(block_hash), is_forwarded:false, check_only: false });
            future::ready(())
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
fn store_partial_encoded_chunk_sanity() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
    let mut partial_encoded_chunk = PartialEncodedChunk {
        shard_id: 0,
        chunk_hash: Default::default(),
        header: Some(ShardChunkHeader::new(
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            &signer,
        )),
        parts: vec![],
        receipts: vec![],
    };
    let block_hash = env.clients[0].chain.genesis().hash();
    let block = env.clients[0].chain.get_block(&block_hash).unwrap().clone();
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 0);
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );

    // Check replacing
    partial_encoded_chunk.chunk_hash.0 = hash(vec![123].as_ref());
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );

    // Check adding
    let mut partial_encoded_chunk2 = partial_encoded_chunk.clone();
    let h = ShardChunkHeader::new(
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        1,
        1,
        173465755,
        0,
        0,
        0,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        &signer,
    );
    partial_encoded_chunk2.header = Some(h);
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk2.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 2);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&173465755],
        partial_encoded_chunk2
    );

    // Check horizon
    env.produce_block(0, 3);
    let mut partial_encoded_chunk3 = partial_encoded_chunk.clone();
    let mut h = ShardChunkHeader::new(
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        1,
        2,
        1,
        0,
        0,
        0,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        &signer,
    );
    partial_encoded_chunk3.header = Some(h.clone());
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(2).len(), 0);
    h.inner.height_created = 9;
    partial_encoded_chunk3.header = Some(h.clone());
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(9).len(), 0);
    h.inner.height_created = 5;
    partial_encoded_chunk3.header = Some(h.clone());
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(5).len(), 1);

    // No header
    let mut partial_encoded_chunk4 = partial_encoded_chunk.clone();
    partial_encoded_chunk4.header = None;
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header, partial_encoded_chunk4.clone());
}
