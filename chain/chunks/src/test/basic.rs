use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::{CanSend, IntoSender, Sender},
    test_loop::event_handler::capture_events,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_primitives::time;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
    },
    test_utils::ChunkTestFixture,
    ShardsManager,
};

#[derive(derive_more::AsMut)]
struct TestData {
    shards_manager: ShardsManager,
    /// Captured events sent to the client.
    client_events: Vec<ShardsManagerResponse>,
}

#[derive(EnumTryInto, Debug, EnumFrom)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerToClient(ShardsManagerResponse),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<TestEvent>;

#[test]
fn test_basic() {
    let fixture = ChunkTestFixture::default(); // TODO: eventually remove
    let builder = ShardsManagerTestLoopBuilder::new();
    let sender = builder.sender();
    let shards_manager = ShardsManager::new(
        builder.clock(),
        Some(fixture.mock_chunk_part_owner.clone()),
        fixture.mock_runtime.clone(),
        Sender::noop(),
        builder.sender().into_sender(),
        fixture.chain_store.new_read_only_chunks_store(),
        fixture.mock_chain_head.clone(),
        fixture.mock_chain_head.clone(),
    );
    let test_data = TestData { shards_manager, client_events: vec![] };
    let mut test = builder.build(test_data);
    test.register_handler(forward_client_request_to_shards_manager().widen());
    test.register_handler(forward_network_request_to_shards_manager().widen());
    test.register_handler(capture_events::<ShardsManagerResponse>().widen());

    // Have the ShardsManager receive a PartialEncodedChunk with all parts.
    sender.send(TestEvent::NetworkToShardsManager(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
            fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
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
            assert_eq!(partial_chunk.parts().len(), fixture.all_part_ords.len());
            assert!(shard_chunk.is_some());
        }
        _ => panic!(),
    }
}
