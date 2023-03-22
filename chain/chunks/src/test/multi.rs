use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::IntoSender,
    test_loop::{
        adhoc::{handle_adhoc_events, AdhocEvent, AdhocRunner},
        event_handler::capture_events,
    },
};
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_epoch_manager::EpochManagerAdapter;
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork, test_loop::SupportsRoutingLookup,
    types::PeerManagerMessageRequest,
};
use near_primitives::types::AccountId;
use near_primitives::{time, types::NumShards};
use near_store::test_utils::create_test_store;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
        periodically_resend_shards_manager_requests, route_shards_manager_network_messages,
        MockChainForShardsManager, MockChainForShardsManagerConfig, ShardsManagerResendRequests,
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
    ShardsManagerResendRequests(ShardsManagerResendRequests),
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
        test.register_handler(route_shards_manager_network_messages(time::Duration::milliseconds(
            10,
        )));
        test.register_handler(
            periodically_resend_shards_manager_requests(time::Duration::milliseconds(400))
                .widen()
                .for_index(idx),
        )
    }
    test
}

#[test]
fn test_distribute_chunk_basic() {
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..10).map(|idx| format!("validator_{}", idx).parse().unwrap()).collect(),
        chunk_only_producers: Vec::new(),
        epoch_length: 4,
        num_shards: 3,
        track_all_shards: false,
    });
    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);
    test.sender().for_index(chunk_producer_idx).run("produce chunk", |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);
    });
    // Run for 100 virtual milliseconds. This isn't long enough to trigger
    // resends (of partial chunk requests), but that should be enough because
    // each node should have forwarded the parts to those block producers that
    // need them.
    test.run(time::Duration::milliseconds(100));

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

#[test]
fn test_distribute_chunk_track_all_shards() {
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..10).map(|idx| format!("validator_{}", idx).parse().unwrap()).collect(),
        chunk_only_producers: Vec::new(),
        epoch_length: 4,
        num_shards: 3,
        track_all_shards: true,
    });
    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);
    test.sender().for_index(chunk_producer_idx).run("produce chunk", |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);
    });
    // Run for 100 virtual milliseconds. This isn't long enough to trigger
    // resends (of partial chunk requests), but for phase 1 we forward all
    // parts to all block producers, so we should not need to rely on retries.
    // TODO: Remove this once we have phase 2 and no longer forward every
    // part to every block producer.
    test.run(time::Duration::seconds(1));

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

#[test]
fn test_distribute_chunk_with_chunk_only_producers() {
    const NUM_BLOCK_PRODUCERS: usize = 8;
    const NUM_CHUNK_ONLY_PRODUCERS: usize = 6;
    let mut test = basic_setup(BasicSetupConfig {
        block_producers: (0..NUM_BLOCK_PRODUCERS)
            .map(|idx| format!("bp_{}", idx).parse().unwrap())
            .collect(),
        chunk_only_producers: (0..NUM_CHUNK_ONLY_PRODUCERS)
            .map(|idx| format!("cp_{}", idx).parse().unwrap())
            .collect(),
        epoch_length: 4,
        num_shards: 3,
        track_all_shards: false,
    });

    let chunk_producer = test.data[0].chain.next_chunk_producer(1);
    let chunk_producer_idx = test.data.index_for_account(&chunk_producer);

    let sender = test.sender();
    test.sender().for_index(chunk_producer_idx).run("produce chunk", move |data| {
        let chunk = data.chain.produce_chunk(1);
        data.chain.distribute_chunk(&chunk);

        // The chunk producers may not be forwarded the chunk header, so we
        // trigger their processing of it manually, as if they had received it
        // via a produced block.
        for idx in NUM_BLOCK_PRODUCERS..NUM_BLOCK_PRODUCERS + NUM_CHUNK_ONLY_PRODUCERS {
            let chunk_header = chunk.header();
            sender.clone().for_index(idx).run_with_delay(
                "request chunk from block",
                move |data| data.chain.request_chunk_for_block(chunk_header),
                time::Duration::milliseconds(100),
            );
        }
    });
    // Run for only 200ms virtual time, because the chunk only producers should
    // not need a timeout to request the chunk parts; they should immediately
    // request them.
    test.run(time::Duration::milliseconds(200));

    for idx in 0..test.data.len() {
        if idx == chunk_producer_idx {
            continue;
        }
        let chunk_producer = chunk_producer.clone();
        // Run assertions in the test loop so if something fails we know which
        // instance failed.
        test.sender().for_index(idx).run("assertions", move |data| {
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
    test.run(time::Duration::milliseconds(1));
}
