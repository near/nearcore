use std::collections::HashSet;

use near_async::{
    messaging::CanSend,
    time::{FakeClock, Utc},
};
use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_chunks::{
    shards_manager_actor::CHUNK_REQUEST_SWITCH_TO_FULL_FETCH,
    test_utils::ShardsManagerResendChunkRequests,
};
use near_client::test_utils::TestEnv;
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_o11y::testonly::init_test_logger;
use near_primitives::utils::from_timestamp;
use near_primitives::{
    shard_layout::ShardLayout,
    types::{AccountId, EpochId, ShardId},
};
use near_primitives_core::{checked_feature, version::PROTOCOL_VERSION};
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use tracing::log::debug;

struct AdversarialBehaviorTestData {
    num_validators: usize,
    env: TestEnv,
    clock: FakeClock,
}

const EPOCH_LENGTH: u64 = 20;

impl AdversarialBehaviorTestData {
    fn new() -> AdversarialBehaviorTestData {
        let num_clients = 8;
        let num_validators = 8 as usize;
        let num_block_producers = 4;
        let epoch_length = EPOCH_LENGTH;

        let accounts: Vec<AccountId> =
            (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        let mut genesis = Genesis::test(accounts, num_validators as u64);
        {
            let config = &mut genesis.config;
            config.genesis_time = from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64);
            config.epoch_length = epoch_length;
            config.shard_layout = ShardLayout::v1_test();
            config.num_block_producer_seats_per_shard = vec![
                num_block_producers as u64,
                num_block_producers as u64,
                num_block_producers as u64,
                num_block_producers as u64,
            ];
            config.num_block_producer_seats = num_block_producers as u64;
            // Configure kickout threshold at 50%.
            config.block_producer_kickout_threshold = 50;
            config.chunk_producer_kickout_threshold = 50;
            config.chunk_validator_only_kickout_threshold = 50;
        }
        let env = TestEnv::builder(&genesis.config)
            .clock(clock.clock())
            .clients_count(num_clients)
            .validator_seats(num_validators as usize)
            .track_all_shards()
            .nightshade_runtimes(&genesis)
            .build();

        AdversarialBehaviorTestData { num_validators, env, clock }
    }

    fn process_one_peer_message(
        &mut self,
        client_id: usize,
        requests: NetworkRequests,
    ) -> Option<NetworkRequests> {
        match requests {
            NetworkRequests::PartialEncodedChunkRequest { .. } => {
                self.env.process_partial_encoded_chunk_request(
                    client_id,
                    PeerManagerMessageRequest::NetworkRequests(requests),
                );
                None
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                self.env.shards_manager(&account_id).send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                        partial_encoded_chunk.into(),
                    ),
                );
                None
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                self.env.shards_manager(&account_id).send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward),
                );
                None
            }
            _ => Some(requests),
        }
    }

    fn process_all_actor_messages(&mut self) {
        loop {
            // Force trigger any chunk request retries.
            // NOTE(hpmv): Additionally dial time forward to trigger a full fetch. Why? Probably
            // because during epoch transitions we don't exactly get this correct. But honestly,
            // I don't know what I'm doing and it doesn't matter for the test.
            self.clock.advance(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH);
            for i in 0..self.num_validators {
                self.env.shards_manager_adapters[i].send(ShardsManagerResendChunkRequests);
            }
            let mut any_message_processed = false;
            for i in 0..self.num_validators {
                let network_adapter = self.env.network_adapters[i].clone();
                any_message_processed |= network_adapter.handle_filtered(|request| match request {
                    PeerManagerMessageRequest::NetworkRequests(requests) => {
                        self.process_one_peer_message(i, requests).map(|ignored_request| {
                            PeerManagerMessageRequest::NetworkRequests(ignored_request)
                        })
                    }
                    _ => Some(request),
                });
            }
            for i in 0..self.env.clients.len() {
                any_message_processed |= self.env.process_shards_manager_responses(i);
            }
            if !any_message_processed {
                break;
            }
        }
    }
}

#[test]
fn test_non_adversarial_case() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    let epoch_manager = test.env.clients[0].epoch_manager.clone();
    for height in 1..=EPOCH_LENGTH * 4 + 5 {
        debug!(target: "test", "======= Height {} ======", height);
        test.process_all_actor_messages();
        let epoch_id = epoch_manager
            .get_epoch_id_from_prev_block(
                &test.env.clients[0].chain.head().unwrap().last_block_hash,
            )
            .unwrap();
        let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();

        let block = test.env.client(&block_producer).produce_block(height).unwrap().unwrap();
        assert_eq!(block.header().height(), height);

        if height > 1 {
            assert_eq!(block.header().prev_height().unwrap(), height - 1);
            let prev_block =
                test.env.clients[0].chain.get_block(&block.header().prev_hash()).unwrap();
            for i in 0..4 {
                // TODO: mysteriously we might miss a chunk around epoch boundaries.
                // Figure out why...
                assert!(
                    block.chunks()[i].height_created() == prev_block.header().height() + 1
                        || (height % EPOCH_LENGTH == 1
                            && block.chunks()[i].chunk_hash()
                                == prev_block.chunks()[i].chunk_hash())
                );
            }
        }

        for i in 0..test.num_validators {
            debug!(target: "test", "Processing block {} as validator #{}", height, i);
            let signer = test.env.clients[i].validator_signer.get();
            let _ = test.env.clients[i].start_process_block(
                block.clone().into(),
                if i == 0 { Provenance::PRODUCED } else { Provenance::NONE },
                None,
                &signer,
            );
            let mut accepted_blocks =
                test.env.clients[i].finish_block_in_processing(block.header().hash());
            // Process any chunk part requests that this client sent. Note that this would also
            // process other network messages (such as production of the next chunk) which is OK.
            test.process_all_actor_messages();
            accepted_blocks.extend(test.env.clients[i].finish_blocks_in_processing());

            assert_eq!(
                accepted_blocks.len(),
                1,
                "Processing of block {} failed at validator #{}",
                height,
                i
            );
            assert_eq!(&accepted_blocks[0], block.header().hash());
            assert_eq!(test.env.clients[i].chain.head().unwrap().height, height);
        }

        test.process_all_actor_messages();
        test.env.propagate_chunk_state_witnesses_and_endorsements(false);
    }

    // Sanity check that the final chain head is what we expect
    assert_eq!(test.env.clients[0].chain.head().unwrap().height, EPOCH_LENGTH * 4 + 5);
    let final_prev_block_hash = test.env.clients[0].chain.head().unwrap().prev_block_hash;
    let final_epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(&final_prev_block_hash).unwrap();
    let final_block_producers = epoch_manager
        .get_epoch_block_producers_ordered(&final_epoch_id, &final_prev_block_hash)
        .unwrap();
    // No producers should be kicked out.
    assert_eq!(final_block_producers.len(), 4);
    let final_chunk_producers = epoch_manager.get_epoch_chunk_producers(&final_epoch_id).unwrap();
    assert_eq!(final_chunk_producers.len(), 8);
}

// Not marking this with test_features, because it's good to ensure this compiles, and also
// if we mark this with features we'd also have to mark a bunch of imports as features.
#[allow(dead_code)]
fn test_banning_chunk_producer_when_seeing_invalid_chunk_base(
    mut test: AdversarialBehaviorTestData,
) {
    let uses_stateless_validation =
        checked_feature!("stable", StatelessValidation, PROTOCOL_VERSION);
    let epoch_manager = test.env.clients[0].epoch_manager.clone();
    let bad_chunk_producer =
        test.env.clients[7].validator_signer.get().unwrap().validator_id().clone();
    let mut epochs_seen_invalid_chunk: HashSet<EpochId> = HashSet::new();
    let mut last_block_skipped = false;
    for height in 1..=EPOCH_LENGTH * 4 + 5 {
        debug!(target: "test", "======= Height {} ======", height);
        test.process_all_actor_messages();
        let epoch_id = epoch_manager
            .get_epoch_id_from_prev_block(
                &test.env.clients[0].chain.head().unwrap().last_block_hash,
            )
            .unwrap();
        let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();

        let block = test.env.client(&block_producer).produce_block(height).unwrap().unwrap();
        assert_eq!(block.header().height(), height);

        let mut invalid_chunks_in_this_block: HashSet<ShardId> = HashSet::new();
        let mut this_block_should_be_skipped = false;
        if height > 1 {
            if last_block_skipped {
                assert_eq!(block.header().prev_height().unwrap(), height - 2);
            } else {
                assert_eq!(block.header().prev_height().unwrap(), height - 1);
            }
            for shard_id in shard_layout.shard_ids() {
                let chunk_producer = epoch_manager
                    .get_chunk_producer(
                        &epoch_id,
                        block.header().prev_height().unwrap() + 1,
                        shard_id,
                    )
                    .unwrap();
                if &chunk_producer == &bad_chunk_producer {
                    invalid_chunks_in_this_block.insert(shard_id);
                    if !epochs_seen_invalid_chunk.contains(&epoch_id) {
                        epochs_seen_invalid_chunk.insert(epoch_id);

                        // This is the first block with invalid chunks in the current epoch.
                        // In pre-stateless validation protocol the first block with invalid chunks
                        // was skipped.
                        // In the old protocol, chunks are first included in the block and then the block
                        // is validated. This means that this block, which includes an invalid chunk,
                        // will be invalid and it should be skipped. Once this happens, the malicious
                        // chunk producer is banned for the whole epoch and no blocks are skipped until
                        // we reach the next epoch.
                        // With stateless validation the block usually isn't skipped. Chunk validators
                        // won't send chunk endorsements for this chunk, which means that it won't be
                        // included in the block at all.
                        if !uses_stateless_validation {
                            this_block_should_be_skipped = true;
                        }
                    }
                }
            }
        }
        debug!(target: "test", "Epoch id of new block: {:?}", epoch_id);
        debug!(target: "test", "Block should be skipped: {}; previous block skipped: {}",
            this_block_should_be_skipped, last_block_skipped);

        if height > 1 {
            let prev_block =
                test.env.clients[0].chain.get_block(&block.header().prev_hash()).unwrap();
            for shard_id in shard_layout.shard_ids() {
                let shard_index = shard_layout.get_shard_index(shard_id);
                if invalid_chunks_in_this_block.contains(&shard_id) && !this_block_should_be_skipped
                {
                    assert_eq!(
                        block.chunks()[shard_index].chunk_hash(),
                        prev_block.chunks()[shard_index].chunk_hash()
                    );
                } else {
                    // TODO: mysteriously we might miss a chunk around epoch boundaries.
                    // Figure out why...
                    let is_epoch_boundary = height % EPOCH_LENGTH == 1;
                    let chunk_header = &block.chunks()[shard_index];
                    let prev_chunk_header = &prev_block.chunks()[shard_index];
                    let is_new_chunk =
                        chunk_header.height_created() == prev_block.header().height() + 1;
                    let is_old_chunk_same_as_prev =
                        chunk_header.chunk_hash() == prev_chunk_header.chunk_hash();
                    assert!(is_new_chunk || (is_epoch_boundary && is_old_chunk_same_as_prev));
                }
            }
        }

        // The block producer of course has the complete block so we can process that.
        for i in 0..test.num_validators {
            debug!(target: "test", "Processing block {} as validator #{}", height, i);
            let signer = test.env.clients[i].validator_signer.get();
            let _ = test.env.clients[i].start_process_block(
                block.clone().into(),
                if i == 0 { Provenance::PRODUCED } else { Provenance::NONE },
                None,
                &signer,
            );
            let mut accepted_blocks =
                test.env.clients[i].finish_block_in_processing(block.header().hash());
            // Process any chunk part requests that this client sent. Note that this would also
            // process other network messages (such as production of the next chunk) which is OK.
            test.process_all_actor_messages();
            accepted_blocks.extend(test.env.clients[i].finish_blocks_in_processing());

            if this_block_should_be_skipped {
                assert_eq!(
                    accepted_blocks.len(),
                    0,
                    "Processing of block {} should have failed due to invalid chunk",
                    height
                );
            } else {
                assert_eq!(
                    accepted_blocks.len(),
                    1,
                    "Processing of block {} failed at validator #{}",
                    height,
                    i
                );
                assert_eq!(&accepted_blocks[0], block.header().hash());
                assert_eq!(test.env.clients[i].chain.head().unwrap().height, height);
            }
        }
        test.process_all_actor_messages();
        test.env.propagate_chunk_state_witnesses_and_endorsements(true);
        last_block_skipped = this_block_should_be_skipped;
    }

    // Sanity check that the final chain head is what we expect
    assert_eq!(test.env.clients[0].chain.head().unwrap().height, EPOCH_LENGTH * 4 + 5);
    // Bad validator should've been kicked out in the third epoch, so it only had two chances
    // to produce bad chunks. Other validators should not be kicked out.
    assert_eq!(epochs_seen_invalid_chunk.len(), 2);
    let final_prev_block_hash = test.env.clients[0].chain.head().unwrap().prev_block_hash;
    let final_epoch_id =
        epoch_manager.get_epoch_id_from_prev_block(&final_prev_block_hash).unwrap();
    let final_block_producers = epoch_manager
        .get_epoch_block_producers_ordered(&final_epoch_id, &final_prev_block_hash)
        .unwrap();
    assert!(final_block_producers.len() >= 3); // 3 validators if the bad validator was a block producer
    let final_chunk_producers = epoch_manager.get_epoch_chunk_producers(&final_epoch_id).unwrap();
    assert_eq!(final_chunk_producers.len(), 7);
}

#[test]
#[cfg(feature = "test_features")]
fn test_banning_chunk_producer_when_seeing_invalid_chunk() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    test.env.clients[7].produce_invalid_chunks = true;
    for client in test.env.clients.iter_mut() {
        client.chunk_validator.set_should_panic_on_validation_error(false);
    }
    test_banning_chunk_producer_when_seeing_invalid_chunk_base(test);
}

#[test]
#[cfg(feature = "test_features")]
fn test_banning_chunk_producer_when_seeing_invalid_tx_in_chunk() {
    init_test_logger();
    let mut test = AdversarialBehaviorTestData::new();
    test.env.clients[7].produce_invalid_tx_in_chunks = true;
    for client in test.env.clients.iter_mut() {
        client.chunk_validator.set_should_panic_on_validation_error(false);
    }
    test_banning_chunk_producer_when_seeing_invalid_chunk_base(test);
}
