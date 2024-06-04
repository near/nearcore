use crate::test_helpers::heavy_test;
use actix::System;
use futures::{future, FutureExt};
use near_actix_test_utils::run_actix;
use near_async::time::{self, Clock};
use near_chain::test_utils::ValidatorSchedule;
use near_chain_configs::{ChunkDistributionNetworkConfig, ChunkDistributionUris};
use near_chunks::shards_manager_actor::{
    CHUNK_REQUEST_RETRY, CHUNK_REQUEST_SWITCH_TO_FULL_FETCH, CHUNK_REQUEST_SWITCH_TO_OTHERS,
};
use near_client::test_utils::{setup_mock_all_validators, ActorHandlesForTesting};
use near_client::{GetBlock, ProcessTxRequest};
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{AccountIdOrPeerTrackingShard, PeerInfo};
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::testonly::init_test_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockId, BlockReference, EpochId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::info;

/// Configuration for `test4` validator in tests.
struct Test4Config {
    /// All partial chunk messages from these accounts to `test4` will be dropped.
    drop_messages_from: &'static [&'static str],
    /// If true, the test will assert that `test4` missed at least one chunk.
    assert_missed_chunk: bool,
}

struct Test {
    validator_groups: u64,
    chunk_only_producers: bool,
    test4_config: Test4Config,
    drop_all_chunk_forward_msgs: bool,
    block_timeout: time::Duration,
}

impl Test {
    fn run(self) {
        heavy_test(move || run_actix(async move { self.run_impl(None) }))
    }

    fn run_with_chunk_distribution_network(self, config: ChunkDistributionNetworkConfig) {
        heavy_test(move || run_actix(async move { self.run_impl(Some(config)) }))
    }

    /// Runs block producing client and stops after network mock received seven blocks
    /// Confirms that the blocks form a chain (which implies the chunks are distributed).
    /// Confirms that the number of messages transmitting the chunks matches the expected number.
    fn run_impl(self, chunk_distribution_config: Option<ChunkDistributionNetworkConfig>) {
        init_test_logger();

        let connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>> = Arc::new(RwLock::new(vec![]));
        let mut vs = ValidatorSchedule::new()
            .num_shards(4)
            .block_producers_per_epoch(vec![
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
            ])
            .validator_groups(self.validator_groups);
        if self.chunk_only_producers {
            vs = vs.chunk_only_producers_per_epoch_per_shard(vec![
                vec![
                    vec!["cop1".parse().unwrap()],
                    vec!["cop2".parse().unwrap()],
                    vec!["cop3".parse().unwrap()],
                    vec!["cop4".parse().unwrap()],
                ],
                vec![
                    vec!["cop5".parse().unwrap()],
                    vec!["cop6".parse().unwrap()],
                    vec!["cop7".parse().unwrap()],
                    vec!["cop8".parse().unwrap()],
                ],
            ]);
        }
        let archive = vec![false; vs.all_validators().count()];
        let epoch_sync_enabled = vec![true; vs.all_validators().count()];
        let key_pairs =
            (0..vs.all_validators().count()).map(|_| PeerInfo::random()).collect::<Vec<_>>();

        let (conn, _) = setup_mock_all_validators(
            Clock::real(),
            vs,
            key_pairs,
            true,
            self.block_timeout.whole_milliseconds() as u64, // TODO: use Duration for callees
            false,
            false,
            5,
            true,
            archive,
            epoch_sync_enabled,
            false,
            chunk_distribution_config,
            Box::new(move |_, from_whom: AccountId, msg: &PeerManagerMessageRequest| {
                let msg = msg.as_network_requests_ref();
                match msg {
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id: to_whom,
                        partial_encoded_chunk: _,
                    } => {
                        if self.test4_config.drop_messages_from.contains(&from_whom.as_str())
                            && to_whom == "test4"
                        {
                            println!(
                                "Dropping Partial Encoded Chunk Message from {from_whom} to test4"
                            );
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                    }
                    NetworkRequests::PartialEncodedChunkForward { account_id: to_whom, .. } => {
                        if self.drop_all_chunk_forward_msgs {
                            println!("Dropping Partial Encoded Chunk Forward Message");
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                        if self.test4_config.drop_messages_from.contains(&from_whom.as_str())
                            && to_whom == "test4"
                        {
                            println!(
                            "Dropping Partial Encoded Chunk Forward Message from {from_whom} to test4"
                        );
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                    }
                    NetworkRequests::PartialEncodedChunkRequest {
                        target: AccountIdOrPeerTrackingShard { account_id: Some(to_whom), .. },
                        ..
                    } => {
                        if self.test4_config.drop_messages_from.contains(&to_whom.as_str())
                            && from_whom == "test4"
                        {
                            info!("Dropping Partial Encoded Chunk Request from test4 to {to_whom}");
                            return (NetworkResponses::NoResponse.into(), false);
                        }
                        if !self.test4_config.drop_messages_from.is_empty()
                            && from_whom == "test4"
                            && to_whom == "test2"
                        {
                            info!("Observed Partial Encoded Chunk Request from test4 to test2");
                        }
                    }
                    _ => {}
                };
                (NetworkResponses::NoResponse.into(), true)
            }),
        );
        *connectors.write().unwrap() = conn;

        let view_client = connectors.write().unwrap()[0].view_client_actor.clone();
        let view_client_loop = view_client.clone();

        let actor = view_client.send(GetBlock::latest().with_span_context());
        let actor = actor.then(move |res| {
            let block_hash = res.unwrap().unwrap().header.hash;
            let connectors_ = connectors.write().unwrap();
            connectors_[0].client_actor.do_send(
                ProcessTxRequest {
                    transaction: SignedTransaction::empty(block_hash),
                    is_forwarded: false,
                    check_only: false,
                }
                .with_span_context(),
            );
            connectors_[1].client_actor.do_send(
                ProcessTxRequest {
                    transaction: SignedTransaction::empty(block_hash),
                    is_forwarded: false,
                    check_only: false,
                }
                .with_span_context(),
            );
            connectors_[2].client_actor.do_send(
                ProcessTxRequest {
                    transaction: SignedTransaction::empty(block_hash),
                    is_forwarded: false,
                    check_only: false,
                }
                .with_span_context(),
            );
            future::ready(())
        });
        actix::spawn(actor);

        // Main test loop. Observe blocks up to height 12 and check that chunks
        // are (not) included.
        let stop_height = 13;
        let mut current_height = 0;
        let mut found_test4_missed_chunk = false;
        let future = async move {
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

            while current_height < stop_height {
                let Ok(block) = view_client_loop
                    .send(
                        GetBlock(BlockReference::BlockId(BlockId::Height(current_height)))
                            .with_span_context(),
                    )
                    .await
                    .unwrap()
                else {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                };

                assert_eq!(block.header.height, current_height);
                current_height += 1;

                let h = block.header.height;
                check_heights(&block.header.prev_hash, &block.header.hash, h);

                let mut height_to_hash = height_to_hash.write().unwrap();
                height_to_hash.insert(h, block.header.hash);

                let mut height_to_epoch = height_to_epoch.write().unwrap();
                height_to_epoch.insert(h, EpochId(block.header.epoch_id));

                let block_producer = block.author;
                println!(
                    "[{:?}]: BLOCK {} PRODUCER {} HEIGHT {}; CHUNK HEADER HEIGHTS: {} / {} / {} / {};\nAPPROVALS: {:?}",
                    Instant::now(),
                    block.header.hash,
                    block_producer,
                    block.header.height,
                    block.chunks[0].height_created,
                    block.chunks[1].height_created,
                    block.chunks[2].height_created,
                    block.chunks[3].height_created,
                    block.header.approvals,
                );

                if h > 1 {
                    // Make sure doomslug finality is computed correctly.
                    assert_eq!(
                        block.header.last_ds_final_block,
                        *height_to_hash.get(&(h - 1)).unwrap()
                    );

                    // Make sure epoch length actually corresponds to the desired epoch length
                    // The switches are expected at 0->1, 5->6 and 10->11
                    let prev_epoch_id = height_to_epoch.get(&(h - 1)).unwrap().clone();
                    assert_eq!(EpochId(block.header.epoch_id) == prev_epoch_id, h % 5 != 1);

                    // Make sure that the blocks leading to the epoch switch have twice as
                    // many approval slots
                    assert_eq!(block.header.approvals.len() == 8, h % 5 == 0 || h % 5 == 4);
                }
                if h > 2 {
                    // Make sure BFT finality is computed correctly
                    assert_eq!(
                        block.header.last_final_block,
                        *height_to_hash.get(&(h - 2)).unwrap()
                    );
                }

                if h <= 1 {
                    // For the first two blocks we may not have any chunks yet.
                    continue;
                }

                if block_producer == "test4" {
                    if !self.test4_config.drop_messages_from.is_empty() {
                        // If messages to test4 are dropped, on block production it
                        // generally will receive block approvals significantly later
                        // than chunks, so some chunks may end up missing.
                        if self.test4_config.assert_missed_chunk {
                            assert!(
                                block.chunks.iter().any(|c| c.height_created != h),
                                "All chunks are present"
                            );
                            found_test4_missed_chunk = true;
                        }
                        continue;
                    }
                }

                // Main test assertion: all chunks are included in the block.
                for shard_id in 0..4 {
                    assert_eq!(
                        h, block.chunks[shard_id].height_created,
                        "New chunk at height {h} for shard {shard_id} wasn't included by {block_producer}"
                    );
                }
            }

            if self.test4_config.assert_missed_chunk && !found_test4_missed_chunk {
                panic!("test4 didn't produce any block");
            };
            System::current().stop();
        };

        actix::spawn(future);
    }
}

#[test]
#[ignore] // TODO: #8855
fn chunks_produced_and_distributed_all_in_all_shards() {
    Test {
        validator_groups: 1,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: 15 * CHUNK_REQUEST_RETRY,
    }
    .run()
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_2_vals_per_shard() {
    Test {
        validator_groups: 2,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_one_val_per_shard() {
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

// Nothing the chunk distribution config can do should break chunk distribution
// because we always fallback on the p2p mechanism. This test runs with a config
// where `enabled: false`.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_chunk_distribution_network_disabled() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris { set: String::new(), get: String::new() },
    };
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run_with_chunk_distribution_network(config)
}

// Nothing the chunk distribution config can do should break chunk distribution
// because we always fallback on the p2p mechanism. This test runs with a config
// where the URIs are not real endpoints.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_chunk_distribution_network_wrong_urls() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris {
            set: "http://www.fake-set-url.com".into(),
            get: "http://www.fake-get-url.com".into(),
        },
    };
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run_with_chunk_distribution_network(config)
}

// Nothing the chunk distribution config can do should break chunk distribution
// because we always fallback on the p2p mechanism. This test runs with a config
// where the `get` URI points at a random http server (therefore it does not
// return valid chunks).
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_chunk_distribution_network_incorrect_get_return() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris { set: String::new(), get: "https://www.google.com".into() },
    };
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run_with_chunk_distribution_network(config)
}

#[test]
#[ignore] // TODO: #8853
fn chunks_produced_and_distributed_all_in_all_shards_should_succeed_even_without_forwarding() {
    Test {
        validator_groups: 1,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_2_vals_per_shard_should_succeed_even_without_forwarding() {
    Test {
        validator_groups: 2,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_one_val_per_shard_should_succeed_even_without_forwarding() {
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

/// The timeout for requesting chunk from others is 1s. 3000 block timeout means that a participant
/// that is otherwise ready to produce a block will wait for 3000/2 milliseconds for all the chunks.
/// We block all the communication from test1 to test4, and expect that in 1.5 seconds test4 will
/// give up on getting the part from test1 and will get it from test2 (who will have it because
/// `validator_groups=2`)
///
/// Note that due to #7385 (which sends chunk forwarding messages irrespective of shard assignment),
/// we disable chunk forwarding messages for the following tests, so we can focus on chunk
/// requesting behavior.
/// TODO: this test is broken due to (#8395) - with fix in #8211

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_others() {
    Test {
        validator_groups: 2,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_OTHERS * 4,
    }
    .run()
}

/// Same test as above, but the number of validator groups is four, therefore test2 doesn't have the
/// part test4 needs. The only way test4 can recover the part is by reconstructing the whole chunk,
/// but they won't do it for the first 3 seconds, and 3s block_timeout means that the block producers
/// only wait for 3000/2 milliseconds until they produce a block with some chunks missing
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_full_timeout_too_short() {
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: true },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_OTHERS * 2,
    }
    .run()
}

/// Same test as above, but the timeout is sufficiently large for test4 now to reconstruct the full
/// chunk
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_full() {
    Test {
        validator_groups: 4,
        chunk_only_producers: false,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_FULL_FETCH * 2,
    }
    .run()
}

// And now let's add chunk-only producers into the mix

/// Happy case -- each shard is handled by one cop and one block producers.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_produced_and_distributed_one_val_shard_cop() {
    Test {
        validator_groups: 4,
        chunk_only_producers: true,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

/// `test4` can't talk to `test1`, so it'll fetch the chunk for first shard from `cop1`.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_others_cop() {
    Test {
        validator_groups: 1,
        chunk_only_producers: true,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_OTHERS * 4,
    }
    .run()
}

/// `test4` can't talk neither to `cop1` nor to `test1`, so it can't fetch chunk
/// from chunk producers and has to reconstruct it.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_full_timeout_too_short_cop() {
    Test {
        validator_groups: 4,
        chunk_only_producers: true,
        test4_config: Test4Config {
            drop_messages_from: &["test1", "cop1"],
            assert_missed_chunk: true,
        },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_OTHERS * 2,
    }
    .run()
}

/// Same as above, but with longer block production timeout which should allow for full reconstruction.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn chunks_recovered_from_full_cop() {
    Test {
        validator_groups: 4,
        chunk_only_producers: true,
        test4_config: Test4Config {
            drop_messages_from: &["test1", "cop1"],
            assert_missed_chunk: false,
        },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_FULL_FETCH * 2,
    }
    .run()
}
