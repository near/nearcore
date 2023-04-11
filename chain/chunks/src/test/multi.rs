use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::IntoSender,
    test_loop::{
        adhoc::{handle_adhoc_events, AdhocEvent, AdhocEventSender},
        event_handler::capture_events,
    },
    time,
};
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_epoch_manager::EpochManagerAdapter;
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork, test_loop::SupportsRoutingLookup,
    types::PeerManagerMessageRequest,
};
use near_primitives::types::{AccountId, NumShards};
use near_store::test_utils::create_test_store;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
        route_shards_manager_network_messages, MockChainForShardsManager,
        MockChainForShardsManagerConfig,
    },
    test_utils::default_tip,
    ShardsManager,
};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    shards_manager: ShardsManager,
    chain: MockChainForShardsManager,
    client_events: Vec<ShardsManagerResponse>,
    account_id: AccountId,
}

#[derive(EnumTryInto, Debug, EnumFrom)]
enum TestEvent {
    Adhoc(AdhocEvent<TestData>),
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerToClient(ShardsManagerResponse),
    OutboundNetwork(PeerManagerMessageRequest),
}

type ShardsManagerTestLoop = near_async::test_loop::TestLoop<Vec<TestData>, (usize, TestEvent)>;
type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<(usize, TestEvent)>;

struct BasicSetupConfig {
    block_producers: Vec<AccountId>,
    chunk_only_producers: Vec<AccountId>,
    epoch_length: u64,
    num_shards: NumShards,
    track_all_shards: bool,
}

const NETWORK_DELAY: time::Duration = time::Duration::milliseconds(10);

fn basic_setup(config: BasicSetupConfig) -> ShardsManagerTestLoop {
    let builder = ShardsManagerTestLoopBuilder::new();
    let all_accounts = config.block_producers.iter().chain(config.chunk_only_producers.iter());
    let data = all_accounts
        .enumerate()
        .map(|(idx, account)| {
            let store = create_test_store();
            let chain = MockChainForShardsManager::new(
                store.clone(),
                MockChainForShardsManagerConfig {
                    account_id: account.clone(),
                    block_producers: config.block_producers.clone(),
                    chunk_only_producers: config.chunk_only_producers.clone(),
                    epoch_length: config.epoch_length,
                    num_shards: config.num_shards,
                    track_all_shards: config.track_all_shards,
                    shards_manager: builder.sender().for_index(idx).into_sender(),
                },
            );
            let shards_manager = ShardsManager::new(
                builder.clock(),
                Some(account.clone()),
                chain.epoch_manager.clone(),
                chain.shard_tracker.clone(),
                builder.sender().for_index(idx).into_sender(),
                builder.sender().for_index(idx).into_sender(),
                ReadOnlyChunksStore::new(store),
                default_tip(),
                default_tip(),
            );
            TestData { shards_manager, chain, client_events: vec![], account_id: account.clone() }
        })
        .collect::<Vec<_>>();
    let mut test = builder.build(data);
    for idx in 0..test.data.len() {
        test.register_handler(handle_adhoc_events().for_index(idx));
        test.register_handler(forward_client_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(forward_network_request_to_shards_manager().widen().for_index(idx));
        test.register_handler(capture_events::<ShardsManagerResponse>().widen().for_index(idx));
        test.register_handler(route_shards_manager_network_messages(NETWORK_DELAY));
        // Note that we don't have the periodically resending requests handler, because
        // our forwarding logic means that we don't need to resend requests, unless
        // there is unreliable network, which is tested separately.
    }
    test
}

/// Tests that when we have some block producers (validators) in the network,
/// and one block producer produces a chunk, the chunk is distributed to the
/// other block producers properly and all other block producers report the
/// completion of the chunk to the client.
#[test]
fn test_distribute_chunk_basic() {
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..10).map(|idx| format!("validator_{}", idx).parse().unwrap()).collect(),
        chunk_only_producers: Vec::new(),
        epoch_length: 4, // arbitrary
        num_shards: 3,   // arbitrary
        track_all_shards: false,
    });
    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);
    test.sender().for_index(chunk_producer_idx).send_adhoc_event("produce chunk", |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);
    });
    // Two network rounds is enough because each node should have
    // forwarded the parts to those block producers that need them.
    test.run_for(NETWORK_DELAY * 2);

    // All other nodes should have received the chunk header and their owned
    // parts, but only those that track the shard should have persisted the
    // entire chunk.
    for idx in 0..test.data.len() {
        if idx == chunk_producer_idx {
            continue;
        }
        let data = test.data.get(idx).unwrap();
        assert_eq!(data.client_events.len(), 2);
        match &data.client_events[0] {
            ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                chunk_producer: producer,
                ..
            } => {
                assert_eq!(producer, &chunk_producer);
            }
            _ => panic!("Unexpected event"),
        }
        match &data.client_events[1] {
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                if data.chain.cares_about_shard_this_or_next_epoch(1) {
                    assert_eq!(
                        partial_chunk.parts().len(),
                        data.chain.epoch_manager.num_total_parts()
                    );
                    assert!(shard_chunk.is_some());
                } else {
                    assert_eq!(partial_chunk.parts().len(), 1);
                    assert_eq!(partial_chunk.parts()[0].part_ord as usize, idx);
                    assert!(shard_chunk.is_none());
                }
            }
            _ => panic!("Unexpected event"),
        }
    }
}

/// Tests that when we have some block producers (validators) in the network,
/// and one block producer produces a chunk, the chunk is distributed to the
/// other block producers properly and all other block producers report the
/// completion of the chunk to the client. Unlike test_distribute_chunk_basic,
/// this test has all block producers track all shards.
#[test]
fn test_distribute_chunk_track_all_shards() {
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..10).map(|idx| format!("validator_{}", idx).parse().unwrap()).collect(),
        chunk_only_producers: Vec::new(),
        epoch_length: 4, // arbitrary
        num_shards: 3,   // arbitrary
        track_all_shards: true,
    });
    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);
    test.sender().for_index(chunk_producer_idx).send_adhoc_event("produce chunk", |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);
    });
    // Two network rounds is enough because each node should have
    // forwarded the parts to those block producers that need them.
    // TODO: after phase 2, we will need a longer delay because validators
    // that don't track the shard will not get forwards. We may also need
    // to add the periodic resending handler.
    test.run_for(NETWORK_DELAY * 2);

    // All other nodes should have received the complete chunk.
    for idx in 0..test.data.len() {
        if idx == chunk_producer_idx {
            continue;
        }
        let data = test.data.get(idx).unwrap();
        assert_eq!(data.client_events.len(), 2);
        match &data.client_events[0] {
            ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                chunk_producer: producer,
                ..
            } => {
                assert_eq!(producer, &chunk_producer);
            }
            _ => panic!("Unexpected event"),
        }
        match &data.client_events[1] {
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                assert_eq!(partial_chunk.parts().len(), data.chain.epoch_manager.num_total_parts());
                assert!(shard_chunk.is_some());
            }
            _ => panic!("Unexpected event"),
        }
    }
}

/// Tests that when the network has some block producers and also some chunk-
/// only producers, the chunk-only producers are also able to request and
/// complete chunks.
#[test]
fn test_distribute_chunk_with_chunk_only_producers() {
    const NUM_BLOCK_PRODUCERS: usize = 8; // arbitrary
    const NUM_CHUNK_ONLY_PRODUCERS: usize = 6; // arbitrary
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..NUM_BLOCK_PRODUCERS)
            .map(|idx| format!("bp_{}", idx).parse().unwrap())
            .collect(),
        chunk_only_producers: (0..NUM_CHUNK_ONLY_PRODUCERS)
            .map(|idx| format!("cp_{}", idx).parse().unwrap())
            .collect(),
        epoch_length: 4, // arbitrary
        num_shards: 3,   // arbitrary
        track_all_shards: false,
    });

    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);

    let sender = test.sender();
    // Typically the chunk-only producer would receive a block later because the block
    // producer would need to first produce a block. So we'll just have some arbitrary
    // delay here.
    let chunk_producer_receive_block_at: time::Duration = time::Duration::milliseconds(50);
    test.sender().for_index(chunk_producer_idx).send_adhoc_event("produce chunk", move |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);

        // The chunk producers may not be forwarded the chunk header, so we
        // trigger their processing of it manually, as if they had received it
        // via a produced block.
        for idx in NUM_BLOCK_PRODUCERS..NUM_BLOCK_PRODUCERS + NUM_CHUNK_ONLY_PRODUCERS {
            let chunk_header = chunk.header();
            sender.clone().for_index(idx).schedule_adhoc_event(
                "request chunk from block",
                move |data| data.chain.request_chunk_for_block(chunk_header),
                chunk_producer_receive_block_at,
            );
        }
    });
    // Run for a network roundtrip after the chunk producers receive the block.
    // This should be enough time for the chunk producers to request and
    // receive the missing chunk parts.
    test.run_for(chunk_producer_receive_block_at + NETWORK_DELAY * 2);

    for idx in 0..test.data.len() {
        if idx == chunk_producer_idx {
            continue;
        }
        let chunk_producer = chunk_producer.clone();
        // Run assertions in the test loop so if something fails we know which
        // instance failed.
        test.sender().for_index(idx).send_adhoc_event("assertions", move |data| {
            assert_eq!(data.client_events.len(), 2);
            match &data.client_events[0] {
                ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                    chunk_producer: producer,
                    ..
                } => {
                    assert_eq!(producer, &chunk_producer);
                }
                _ => panic!("Unexpected event"),
            }
            match &data.client_events[1] {
                ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                    if data.chain.cares_about_shard_this_or_next_epoch(1) {
                        // If we track this shard we should have all parts.
                        assert_eq!(
                            partial_chunk.parts().len(),
                            data.chain.epoch_manager.num_total_parts()
                        );
                        assert!(shard_chunk.is_some());
                    } else {
                        assert!(shard_chunk.is_none());
                        if idx < NUM_BLOCK_PRODUCERS {
                            // Block producers own one part each.
                            assert_eq!(partial_chunk.parts().len(), 1);
                            assert_eq!(partial_chunk.parts()[0].part_ord as usize, idx);
                        } else {
                            // Chunk producers don't own any parts.
                            assert_eq!(partial_chunk.parts().len(), 0);
                        }
                    }
                    for shard in 0..3 {
                        if data.chain.cares_about_shard_this_or_next_epoch(shard) {
                            if data.chain.cares_about_shard_this_or_next_epoch(1) {
                                // If we track shard 1, we should have the full chunk
                                // and thus have every receipt proof.
                                assert_eq!(partial_chunk.receipts().len(), 3);
                            } else {
                                // Otherwise, we should only have the receipt proof for the
                                // shard we care about.
                                assert_eq!(partial_chunk.receipts().len(), 1);
                                assert_eq!(partial_chunk.receipts()[0].1.to_shard_id, shard);
                            }
                        }
                    }
                }
                _ => panic!("Unexpected event"),
            }
        });
    }
    test.run_instant();
}
