use std::collections::HashMap;
use std::sync::Arc;

use near_async::messaging::Handler as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_chain_configs::{ChunkDistributionNetworkConfig, ChunkDistributionUris};
use near_chunks::shards_manager_actor::{
    CHUNK_REQUEST_RETRY, CHUNK_REQUEST_SWITCH_TO_FULL_FETCH, CHUNK_REQUEST_SWITCH_TO_OTHERS,
};
use near_client::GetBlock;
use near_network::types::{AccountIdOrPeerTrackingShard, NetworkRequests};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockId, BlockReference, EpochId, NumSeats};
use parking_lot::RwLock;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::ONE_NEAR;
use crate::utils::rotating_validators_runner::RotatingValidatorsRunner;

/// Configuration for `test4` validator in tests.
struct Test4Config {
    /// All partial chunk messages from these accounts to `test4` will be dropped.
    drop_messages_from: &'static [&'static str],
    /// If true, the test will assert that `test4` missed at least one chunk.
    assert_missed_chunk: bool,
}

struct Test {
    min_validators: u64,
    test4_config: Test4Config,
    drop_all_chunk_forward_msgs: bool,
    block_timeout: Duration,
}

impl Test {
    fn run(self) {
        self.run_impl(None)
    }

    fn run_with_chunk_distribution_network(self, config: ChunkDistributionNetworkConfig) {
        self.run_impl(Some(config))
    }

    /// Runs block producing client and stops after network mock received several blocks
    /// Confirms that the blocks form a chain (which implies the chunks are distributed).
    /// Confirms that the number of messages transmitting the chunks matches the expected number.
    fn run_impl(self, chunk_distribution_config: Option<ChunkDistributionNetworkConfig>) {
        init_test_logger();

        let validators: Vec<Vec<AccountId>> =
            [["test1", "test2", "test3", "test4"], ["test5", "test6", "test7", "test8"]]
                .iter()
                .map(|vec| vec.iter().map(|account| account.parse().unwrap()).collect())
                .collect();
        let seats: NumSeats = validators[0].len().try_into().unwrap();

        let stake = ONE_NEAR;
        let epoch_length: u64 = 10;
        let mut runner = RotatingValidatorsRunner::new(stake, validators);
        runner.set_max_epoch_duration(self.block_timeout * 3 * (epoch_length + 1) as i32);

        let accounts = runner.all_validators_accounts();
        let genesis = TestLoopBuilder::new_genesis_builder()
            .epoch_length(epoch_length)
            .validators_spec(runner.genesis_validators_spec(seats, seats, seats))
            .add_user_accounts_simple(&accounts, stake)
            .shard_layout(ShardLayout::multi_shard(4, 3))
            .genesis_height(0)
            .build();
        let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
            .minimum_validators_per_shard(self.min_validators)
            .build_store_for_genesis_protocol_version();
        let mut env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(accounts)
            .epoch_config_store(epoch_config_store)
            .config_modifier(move |config, _| {
                config.min_block_production_delay = self.block_timeout;
                config.max_block_production_delay = 3 * self.block_timeout;
                config.max_block_wait_delay = 3 * self.block_timeout;
                config.chunk_distribution_network = chunk_distribution_config.clone();
            })
            .build()
            .warmup();

        for node_datas in &env.node_datas {
            let from_whom = node_datas.account_id.clone();
            let peer_actor_handle = node_datas.peer_manager_sender.actor_handle();
            let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
            peer_actor.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
                match request {
                    NetworkRequests::PartialEncodedChunkMessage {
                        account_id: ref to_whom,
                        partial_encoded_chunk: _,
                    } => {
                        if self.test4_config.drop_messages_from.contains(&from_whom.as_str())
                            && to_whom == "test4"
                        {
                            println!(
                                "Dropping Partial Encoded Chunk Message from {from_whom} to test4"
                            );
                            return None;
                        }
                    }
                    NetworkRequests::PartialEncodedChunkForward { account_id: ref to_whom, .. } => {
                        if self.drop_all_chunk_forward_msgs {
                            println!("Dropping Partial Encoded Chunk Forward Message");
                            return None;
                        }
                        if self.test4_config.drop_messages_from.contains(&from_whom.as_str())
                            && to_whom == "test4"
                        {
                            println!(
                                "Dropping Partial Encoded Chunk Forward Message from {from_whom} to test4"
                            );
                            return None;
                        }
                    }
                    NetworkRequests::PartialEncodedChunkRequest {
                        target: AccountIdOrPeerTrackingShard { account_id: Some(ref to_whom), .. },
                        ..
                    } => {
                        if self.test4_config.drop_messages_from.contains(&to_whom.as_str())
                            && from_whom == "test4"
                        {
                            tracing::info!("Dropping Partial Encoded Chunk Request from test4 to {to_whom}");
                            return None;
                        }
                        if !self.test4_config.drop_messages_from.is_empty()
                             && from_whom == "test4"
                             && to_whom == "test2"
                        {
                            tracing::warn!("Observed Partial Encoded Chunk Request from {from_whom} to {to_whom}");
                        }
                    }
                    _ => {}
                };
                return Some(request);
            }));
        }

        // We run for an epoch to make sure that validators are switched each epoch during testing
        // below.
        runner.run_for_an_epoch(&mut env);

        let client_actor_handle = &env.node_datas[0].client_sender.actor_handle();
        let client = &env.test_loop.data.get(client_actor_handle).client;
        let view_client_actor_handle = &env.node_datas[0].view_client_sender.actor_handle();

        let start_height = client.chain.head().unwrap().height;
        let stop_height = start_height + epoch_length * 3;

        let heights = Arc::new(RwLock::new(HashMap::new()));
        let height_to_hash = Arc::new(RwLock::new(HashMap::new()));
        let height_to_epoch = Arc::new(RwLock::new(HashMap::new()));

        let check_heights = move |prev_hash: &CryptoHash, hash: &CryptoHash, height| {
            let mut map = heights.write();
            // Note that height of the previous block is not guaranteed to be height
            // - 1.  All we know is that it’s less than height of the current block.
            if let Some(prev_height) = map.get(prev_hash) {
                assert!(*prev_height < height);
            }
            assert_eq!(*map.entry(*hash).or_insert(height), height);
        };

        let mut current_height = 0;
        let mut found_test4_missed_chunk = false;
        runner.run_until(
            &mut env,
            |test_loop_data| {
                while current_height < stop_height {
                    let view_client = test_loop_data.get_mut(view_client_actor_handle);

                    let Ok(block) = view_client.handle(GetBlock(BlockReference::BlockId(BlockId::Height(current_height))))
                    else {
                        return false;
                    };

                    assert_eq!(block.header.height, current_height);
                    current_height += 1;

                    let h = block.header.height;
                    check_heights(&block.header.prev_hash, &block.header.hash, h);

                    let mut height_to_hash = height_to_hash.write();
                    height_to_hash.insert(h, block.header.hash);

                    let mut height_to_epoch = height_to_epoch.write();
                    height_to_epoch.insert(h, EpochId(block.header.epoch_id));

                    let block_producer = block.author;
                    println!(
                        "BLOCK {} PRODUCER {} HEIGHT {}; CHUNK HEADER HEIGHTS: {} / {} / {} / {};\nAPPROVALS: {:?}",
                        block.header.hash,
                        block_producer,
                        block.header.height,
                        block.chunks[0].height_created,
                        block.chunks[1].height_created,
                        block.chunks[2].height_created,
                        block.chunks[3].height_created,
                        block.header.approvals.len(),
                    );

                    if h > 1 {
                        // Make sure doomslug finality is computed correctly.
                        assert_eq!(
                            block.header.last_ds_final_block,
                            *height_to_hash.get(&(h - 1)).unwrap()
                        );

                        // Make sure epoch length actually corresponds to the desired epoch length
                        // The switches are expected at 0->1, epoch_length -> epoch_length + 1, ...
                        let prev_epoch_id = *height_to_epoch.get(&(h - 1)).unwrap();
                        assert_eq!(EpochId(block.header.epoch_id) == prev_epoch_id, h % epoch_length != 1);

                        // Make sure that the blocks leading to the epoch switch have twice as
                        // many approval slots once validators are being rotated.
                        if current_height > start_height {
                            assert_eq!(block.header.approvals.len() == 8, h % epoch_length == 0 || h % epoch_length == epoch_length - 1);
                        }
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
                        return false;
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
                            return false;
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
                true
            },
            self.block_timeout * 3 * (stop_height - start_height) as i32,
        );

        if self.test4_config.assert_missed_chunk && !found_test4_missed_chunk {
            panic!("test4 didn't produce any block");
        };

        env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}

#[test]
fn slow_test_chunks_produced_and_distributed_all_in_all_shards() {
    Test {
        min_validators: 4,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: 15 * CHUNK_REQUEST_RETRY,
    }
    .run()
}

#[test]
fn slow_test_chunks_produced_and_distributed_2_vals_per_shard() {
    Test {
        min_validators: 2,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
fn slow_test_chunks_produced_and_distributed_one_val_per_shard() {
    Test {
        min_validators: 1,
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
fn slow_test_chunks_produced_and_distributed_chunk_distribution_network_disabled() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris { set: String::new(), get: String::new() },
    };
    Test {
        min_validators: 1,
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
fn slow_test_chunks_produced_and_distributed_chunk_distribution_network_wrong_urls() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris {
            set: "http://www.fake-set-url.com".into(),
            get: "http://www.fake-get-url.com".into(),
        },
    };
    Test {
        min_validators: 1,
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
fn slow_test_chunks_produced_and_distributed_chunk_distribution_network_incorrect_get_return() {
    let config = ChunkDistributionNetworkConfig {
        enabled: false,
        uris: ChunkDistributionUris { set: String::new(), get: "https://www.google.com".into() },
    };
    Test {
        min_validators: 1,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: false,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run_with_chunk_distribution_network(config)
}

#[test]
fn slow_test_chunks_produced_and_distributed_all_in_all_shards_should_succeed_even_without_forwarding()
 {
    Test {
        min_validators: 4,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
fn slow_test_chunks_produced_and_distributed_2_vals_per_shard_should_succeed_even_without_forwarding()
 {
    Test {
        min_validators: 2,
        test4_config: Test4Config { drop_messages_from: &[], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_RETRY * 15,
    }
    .run()
}

#[test]
fn slow_test_chunks_produced_and_distributed_one_val_per_shard_should_succeed_even_without_forwarding()
 {
    Test {
        min_validators: 1,
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
#[test]
fn slow_test_chunks_recovered_from_others() {
    Test {
        min_validators: 2,
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
fn slow_test_chunks_recovered_from_full_timeout_too_short() {
    Test {
        min_validators: 1,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: true },
        drop_all_chunk_forward_msgs: true,
        // This has to be large enough to allow test4 to keep up when it's not chunk
        // producer, but small enough for it not to have enough time to reconstruct chunks from
        // test1 when it is chunk producer.
        block_timeout: CHUNK_REQUEST_SWITCH_TO_FULL_FETCH * 1.1,
    }
    .run()
}

/// Same test as above, but the timeout is sufficiently large for test4 now to reconstruct the full
/// chunk
#[test]
fn slow_test_chunks_recovered_from_full() {
    Test {
        min_validators: 1,
        test4_config: Test4Config { drop_messages_from: &["test1"], assert_missed_chunk: false },
        drop_all_chunk_forward_msgs: true,
        block_timeout: CHUNK_REQUEST_SWITCH_TO_FULL_FETCH * 2,
    }
    .run()
}
