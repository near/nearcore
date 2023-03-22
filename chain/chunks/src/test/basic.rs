use std::collections::HashSet;

use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::{CanSend, IntoSender, Sender},
    test_loop::{
        adhoc::{handle_adhoc_events, AdhocEvent, AdhocRunner},
        event_handler::capture_events,
    },
};
use near_chain::chunks_store::ReadOnlyChunksStore;
use near_epoch_manager::test_utils::hash_range;
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::{
    time,
    types::{AccountId, BlockHeight},
};
use near_store::test_utils::create_test_store;
use tracing::log::info;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
        periodically_resend_shards_manager_requests, MockChainForShardsManager,
        MockChainForShardsManagerConfig, ShardsManagerResendRequests,
    },
    test_utils::default_tip,
    ShardsManager,
};

#[derive(derive_more::AsMut)]
struct TestData {
    shards_manager: ShardsManager,
    chain: MockChainForShardsManager,
    /// Captured events sent to the client.
    client_events: Vec<ShardsManagerResponse>,
    /// Captured events sent to the network.
    network_events: Vec<PeerManagerMessageRequest>,
}

impl TestData {
    fn new(shards_manager: ShardsManager, chain: MockChainForShardsManager) -> Self {
        Self { shards_manager, chain, client_events: vec![], network_events: vec![] }
    }
}

#[derive(EnumTryInto, Debug, EnumFrom)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerToClient(ShardsManagerResponse),
    ShardsManagerToNetwork(PeerManagerMessageRequest),
    ShardsManagerResendRequests(ShardsManagerResendRequests),
    Adhoc(AdhocEvent<TestData>),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<TestEvent>;

#[test]
fn test_basic() {
    let builder = ShardsManagerTestLoopBuilder::new();
    let validators = (0..5)
        .map(|i| format!("validator_{}", i).parse::<AccountId>().unwrap())
        .collect::<Vec<_>>();

    let store = create_test_store();
    let chain = MockChainForShardsManager::new(
        store.clone(),
        MockChainForShardsManagerConfig {
            account_id: validators[0].clone(),
            block_producers: validators.clone(),
            chunk_only_producers: vec![],
            epoch_length: 2,
            num_shards: 3,
            track_all_shards: true,
            shards_manager: builder.sender().into_sender(),
        },
    );

    let shards_manager = ShardsManager::new(
        builder.clock(),
        Some(validators[0].clone()),
        chain.epoch_manager.clone(),
        chain.shard_tracker.clone(),
        Sender::noop(),
        builder.sender().into_sender(),
        ReadOnlyChunksStore::new(store),
        default_tip(),
        default_tip(),
    );
    let test_data = TestData::new(shards_manager, chain);
    let mut test = builder.build(test_data);
    test.register_handler(forward_client_request_to_shards_manager().widen());
    test.register_handler(forward_network_request_to_shards_manager().widen());
    test.register_handler(capture_events::<ShardsManagerResponse>().widen());

    // Have the ShardsManager receive a PartialEncodedChunk with all parts.
    let chunk = test.data.chain.produce_chunk_signed_by_chunk_producer(2);
    test.sender().send(TestEvent::NetworkToShardsManager(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
            chunk.make_partial_encoded_chunk(&chunk.part_ords(), &[]),
        ),
    ));
    test.run(time::Duration::seconds(1));

    assert_eq!(test.data.client_events.len(), 2);
    match &test.data.client_events[0] {
        ShardsManagerResponse::ChunkHeaderReadyForInclusion { .. } => {}
        _ => panic!(),
    }
    match &test.data.client_events[1] {
        ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
            assert_eq!(partial_chunk.parts().len(), chunk.part_ords().len());
            assert!(shard_chunk.is_some());
        }
        _ => panic!(),
    }
}

#[test]
fn test_chunk_forward() {
    let builder = ShardsManagerTestLoopBuilder::new();
    // Need at least 7 block producers for this test, so that the validator
    // who receives one part cannot decode the entire chunk and needs to
    // request more.
    let block_producers: Vec<AccountId> =
        (0..10).map(|idx| format!("bp_{}", idx).parse().unwrap()).collect();
    let chunk_only_producers: Vec<AccountId> =
        (0..10).map(|idx| format!("cp_{}", idx).parse().unwrap()).collect();
    let store = create_test_store();
    let chain = MockChainForShardsManager::new(
        store.clone(),
        MockChainForShardsManagerConfig {
            account_id: block_producers[0].clone(),
            block_producers: block_producers.clone(),
            chunk_only_producers: chunk_only_producers.clone(),
            num_shards: 1,
            epoch_length: 5,
            track_all_shards: true,
            shards_manager: builder.sender().into_sender(),
        },
    );
    let shards_manager = ShardsManager::new(
        builder.clock(),
        Some(block_producers[0].clone()),
        chain.epoch_manager.clone(),
        chain.shard_tracker.clone(),
        builder.sender().into_sender(),
        builder.sender().into_sender(),
        ReadOnlyChunksStore::new(store),
        default_tip(),
        default_tip(),
    );
    let mut test = builder.build(TestData::new(shards_manager, chain));
    test.register_handler(capture_events::<ShardsManagerResponse>().widen());
    test.register_handler(capture_events::<PeerManagerMessageRequest>().widen());
    test.register_handler(forward_client_request_to_shards_manager().widen());
    test.register_handler(forward_network_request_to_shards_manager().widen());
    test.register_handler(
        periodically_resend_shards_manager_requests(time::Duration::milliseconds(400)).widen(),
    );
    test.register_handler(handle_adhoc_events());

    test.sender().run("produce chunk", {
        let sender = test.sender();
        let chunk_only_producers = chunk_only_producers.clone();
        move |data| {
            let hashes = hash_range(100);
            for (i, hash) in hashes.iter().enumerate() {
                let partial_encoded_chunk = data.chain.produce_chunk_signed_by_chunk_producer(0);
                data.chain.record_block(*hash, i as BlockHeight + 1);
                let next_chunk_producer = data.chain.next_chunk_producer(0);
                if !chunk_only_producers.contains(&next_chunk_producer) {
                    info!(target: "test", "Trying again at height {} which has chunk producer {}, we want the next chunk producer to be a chunk only producer",
                          i + 1, next_chunk_producer);
                    continue;
                }
                sender.send(TestEvent::NetworkToShardsManager(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                        partial_encoded_chunk.make_partial_encoded_chunk(&[0], &[]),
                    ),
                ));
                break;
            }
        }
    });
    test.run(time::Duration::milliseconds(100));

    // The logic implemented right now is that all block producers will get the
    // forwarding (due to tracking all shards), and for chunk only producers,
    // they will only get the forwarding if they are the next chunk producer.
    let mut next_chunk_producer_forwarded = false;
    let mut block_producer_forwarded = HashSet::new();
    while let Some(r) = test.data.network_events.pop() {
        match r.as_network_requests_ref() {
            NetworkRequests::PartialEncodedChunkForward { account_id, .. } => {
                if account_id == &test.data.chain.next_chunk_producer(0) {
                    next_chunk_producer_forwarded = true;
                } else {
                    assert!(
                        !chunk_only_producers.contains(&account_id),
                        "shouldn't forward to {:?}",
                        account_id
                    );
                    block_producer_forwarded.insert(account_id.clone());
                }
            }
            NetworkRequests::PartialEncodedChunkRequest { .. } => {
                panic!("Shouldn't request chunk part yet; should wait for forwarding");
            }
            _ => {
                panic!("Unexpected network request: {:?}", r);
            }
        }
    }
    assert!(next_chunk_producer_forwarded);
    assert_eq!(block_producer_forwarded.len(), block_producers.len() - 1);

    // Now run for a bit longer to trigger resend. The validator should now
    // request the missing parts.
    test.run(time::Duration::milliseconds(400));
    let mut seen_part_request = false;
    while let Some(r) = test.data.network_events.pop() {
        match r.as_network_requests_ref() {
            NetworkRequests::PartialEncodedChunkRequest { .. } => {
                seen_part_request = true;
            }
            _ => {
                panic!("Unexpected network request: {:?}", r);
            }
        }
    }
    assert!(seen_part_request);
}
