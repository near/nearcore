use near_primitives::time::Instant;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use actix::{Addr, System};
use futures::{future, FutureExt};
use tracing::info;

use crate::test_helpers::heavy_test;
use near_actix_test_utils::run_actix;
use near_chunks::{
    CHUNK_REQUEST_RETRY_MS, CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS,
    CHUNK_REQUEST_SWITCH_TO_OTHERS_MS,
};
use near_client::test_utils::setup_mock_all_validators;
use near_client::{ClientActor, GetBlock, ViewClientHandle};
use near_logger_utils::init_test_logger;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{NetworkClientMessages, NetworkRequests, NetworkResponses};
use near_network_primitives::types::{AccountIdOrPeerTrackingShard, PeerInfo};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;

/// Runs block producing client and stops after network mock received seven blocks
/// Confirms that the blocks form a chain (which implies the chunks are distributed).
/// Confirms that the number of messages transmitting the chunks matches the expected number.
fn chunks_produced_and_distributed_common(
    validator_groups: u64,
    drop_from_1_to_4: bool,
    block_timeout: u64,
) {
    init_test_logger();

    let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, ViewClientHandle)>>> =
        Arc::new(RwLock::new(vec![]));
    let heights = Arc::new(RwLock::new(HashMap::new()));
    let heights1 = heights;

    let height_to_hash = Arc::new(RwLock::new(HashMap::new()));
    let height_to_epoch = Arc::new(RwLock::new(HashMap::new()));

    let check_heights = move |prev_hash: &CryptoHash, hash: &CryptoHash, height| {
        let mut map = heights1.write().unwrap();
        // Note that height of the previous block is not guaranteed to be height
        // - 1.  All we know is that it’s less than height of the current block.
        if let Some(prev_height) = map.get(prev_hash) {
            assert!(*prev_height < height);
        }
        assert_eq!(*map.entry(*hash).or_insert(height), height);
    };

    let validators = vec![
        vec![
            "test1".parse().unwrap(),
            "test2".parse().unwrap(),
            "test3".parse().unwrap(),
            "test4".parse().unwrap(),
        ],
        vec![
            "test5".parse().unwrap(),
            "test6".parse().unwrap(),
            "test7".parse().unwrap(),
            "test8".parse().unwrap(),
        ],
    ];
    let key_pairs = (0..8).map(|_| PeerInfo::random()).collect::<Vec<_>>();

    let mut partial_chunk_msgs = 0;
    let mut partial_chunk_request_msgs = 0;

    let (_, conn, _) = setup_mock_all_validators(
        validators.clone(),
        key_pairs,
        validator_groups,
        true,
        block_timeout,
        false,
        false,
        5,
        true,
        vec![false; validators.iter().map(|x| x.len()).sum()],
        vec![true; validators.iter().map(|x| x.len()).sum()],
        false,
        Arc::new(RwLock::new(Box::new(
            move |from_whom: AccountId, msg: &PeerManagerMessageRequest| {
                let msg = msg.as_network_requests_ref();
                match msg {
                    NetworkRequests::Block { block } => {
                        let h = block.header().height();
                        check_heights(block.header().prev_hash(), block.hash(), h);

                        let mut height_to_hash = height_to_hash.write().unwrap();
                        height_to_hash.insert(h, *block.hash());

                        let mut height_to_epoch = height_to_epoch.write().unwrap();
                        height_to_epoch.insert(h, block.header().epoch_id().clone());

                        println!(
                            "[{:?}]: BLOCK {} HEIGHT {}; HEADER HEIGHTS: {} / {} / {} / {};\nAPPROVALS: {:?}",
                            Instant::now(),
                            block.hash(),
                            block.header().height(),
                            block.chunks()[0].height_created(),
                            block.chunks()[1].height_created(),
                            block.chunks()[2].height_created(),
                            block.chunks()[3].height_created(),
                            block.header().approvals(),
                        );

                        if h > 1 {
                            // Make sure doomslug finality is computed correctly.
                            assert_eq!(
                                block.header().last_ds_final_block(),
                                height_to_hash.get(&(h - 1)).unwrap()
                            );

                            // Make sure epoch length actually corresponds to the desired epoch length
                            // The switches are expected at 0->1, 5->6 and 10->11
                            let prev_epoch_id = height_to_epoch.get(&(h - 1)).unwrap().clone();
                            assert_eq!(block.header().epoch_id() == &prev_epoch_id, h % 5 != 1);

                            // Make sure that the blocks leading to the epoch switch have twice as
                            // many approval slots
                            assert_eq!(
                                block.header().approvals().len() == 8,
                                h % 5 == 0 || h % 5 == 4
                            );
                        }
                        if h > 2 {
                            // Make sure BFT finality is computed correctly
                            assert_eq!(
                                block.header().last_final_block(),
                                height_to_hash.get(&(h - 2)).unwrap()
                            );
                        }

                        if block.header().height() > 1 {
                            for shard_id in 0..4 {
                                // If messages from 1 to 4 are dropped, 4 at their heights will
                                //    receive the block significantly later than the chunks, and
                                //    thus would discard the chunks
                                if !drop_from_1_to_4 || block.header().height() % 4 != 3 {
                                    assert_eq!(
                                        block.header().height(),
                                        block.chunks()[shard_id].height_created()
                                    );
                                }
                            }
                        }

                        if block.header().height() >= 12 {
                            println!("PREV BLOCK HASH: {}", block.header().prev_hash());
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
                        if drop_from_1_to_4
                            && from_whom.as_ref() == "test1"
                            && to_whom.as_ref() == "test4"
                        {
                            println!("Dropping Partial Encoded Chunk Message from test1 to test4");
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                    }
                    NetworkRequests::PartialEncodedChunkForward { account_id: to_whom, .. } => {
                        if drop_from_1_to_4
                            && from_whom.as_ref() == "test1"
                            && to_whom.as_ref() == "test4"
                        {
                            println!(
                            "Dropping Partial Encoded Chunk Forward Message from test1 to test4"
                        );
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                    }
                    NetworkRequests::PartialEncodedChunkResponse { route_back: _, response: _ } => {
                        partial_chunk_msgs += 1;
                    }
                    NetworkRequests::PartialEncodedChunkRequest {
                        target: AccountIdOrPeerTrackingShard { account_id: Some(to_whom), .. },
                        ..
                    } => {
                        if drop_from_1_to_4
                            && from_whom.as_ref() == "test4"
                            && to_whom.as_ref() == "test1"
                        {
                            info!("Dropping Partial Encoded Chunk Request from test4 to test1");
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                        if drop_from_1_to_4
                            && from_whom.as_ref() == "test4"
                            && to_whom.as_ref() == "test2"
                        {
                            info!("Observed Partial Encoded Chunk Request from test4 to test2");
                        }
                        partial_chunk_request_msgs += 1;
                    }
                    _ => {}
                };
                (NetworkResponses::NoResponse.into(), true)
            },
        ))),
    );
    *connectors.write().unwrap() = conn;

    let view_client = connectors.write().unwrap()[0].1.clone();
    actix::spawn(view_client.send(GetBlock::latest()).then(move |res| {
        let block_hash = res.unwrap().unwrap().header.hash;
        let connectors_ = connectors.write().unwrap();
        connectors_[0].0.do_send(NetworkClientMessages::Transaction {
            transaction: SignedTransaction::empty(block_hash),
            is_forwarded: false,
            check_only: false,
        });
        connectors_[1].0.do_send(NetworkClientMessages::Transaction {
            transaction: SignedTransaction::empty(block_hash),
            is_forwarded: false,
            check_only: false,
        });
        connectors_[2].0.do_send(NetworkClientMessages::Transaction {
            transaction: SignedTransaction::empty(block_hash),
            is_forwarded: false,
            check_only: false,
        });
        future::ready(())
    }));
}

#[test]
fn chunks_produced_and_distributed_all_in_all_shards() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(1, false, 15 * CHUNK_REQUEST_RETRY_MS);
        });
    });
}

#[test]
fn chunks_produced_and_distributed_2_vals_per_shard() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(2, false, 15 * CHUNK_REQUEST_RETRY_MS);
        });
    });
}

#[test]
fn chunks_produced_and_distributed_one_val_per_shard() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(4, false, 15 * CHUNK_REQUEST_RETRY_MS);
        });
    });
}

/// The timeout for requesting chunk from others is 1s. 3000 block timeout means that a participant
/// that is otherwise ready to produce a block will wait for 3000/2 milliseconds for all the chunks.
/// We block all the communication from test1 to test4, and expect that in 1.5 seconds test4 will
/// give up on getting the part from test1 and will get it from test2 (who will have it because
/// `validator_groups=2`)
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_others() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(2, true, 4 * CHUNK_REQUEST_SWITCH_TO_OTHERS_MS);
        });
    });
}

/// Same test as above, but the number of validator groups is four, therefore test2 doesn't have the
/// part test4 needs. The only way test4 can recover the part is by reconstructing the whole chunk,
/// but they won't do it for the first 3 seconds, and 3s block_timeout means that the block producers
/// only wait for 3000/2 milliseconds until they produce a block with some chunks missing
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
#[should_panic]
fn chunks_recovered_from_full_timeout_too_short() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(4, true, 2 * CHUNK_REQUEST_SWITCH_TO_OTHERS_MS);
        });
    });
}

/// Same test as above, but the timeout is sufficiently large for test4 now to reconstruct the full
/// chunk
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_full() {
    heavy_test(|| {
        run_actix(async {
            chunks_produced_and_distributed_common(
                4,
                true,
                2 * CHUNK_REQUEST_SWITCH_TO_FULL_FETCH_MS,
            );
        })
    });
}
