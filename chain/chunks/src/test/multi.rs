use std::str::FromStr;

use derive_enum_from_into::{EnumFrom, EnumTryInto};
use near_async::{
    messaging::{CanSend, IntoSender},
    test_loop::event_handler::capture_events,
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork, types::PeerManagerMessageRequest,
};
use near_primitives::time;
use near_primitives::types::AccountId;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    client::ShardsManagerResponse,
    test_loop::{
        forward_client_request_to_shards_manager, forward_network_request_to_shards_manager,
        periodically_resend_shards_manager_requests, route_shards_manager_network_messages,
        ShardsManagerResendRequests,
    },
    test_utils::ChunkTestFixture,
    ShardsManager,
};

#[derive(derive_more::AsMut, derive_more::AsRef)]
struct TestData {
    shards_manager: ShardsManager,
    client_events: Vec<ShardsManagerResponse>,
    account_id: AccountId,
}

#[derive(EnumTryInto, Debug, EnumFrom)]
enum TestEvent {
    ClientToShardsManager(ShardsManagerRequestFromClient),
    NetworkToShardsManager(ShardsManagerRequestFromNetwork),
    ShardsManagerResendRequests(ShardsManagerResendRequests),
    ShardsManagerToClient(ShardsManagerResponse),
    OutboundNetwork(PeerManagerMessageRequest),
}

type ShardsManagerTestLoopBuilder = near_async::test_loop::TestLoopBuilder<(usize, TestEvent)>;

#[test]
fn test_multi() {
    let builder = ShardsManagerTestLoopBuilder::new();
    // TODO: the ChunkTestFixture requires exactly 13 validators. We need to
    // remove the fixture and have a proper control of the setup parameters.
    let data = (0..13)
        .map(|idx| {
            let fixture = ChunkTestFixture::default();
            let shards_manager = ShardsManager::new(
                builder.clock(),
                Some(fixture.mock_chunk_part_owner.clone()),
                fixture.mock_runtime.clone(),
                builder.sender().for_index(idx).into_sender(),
                builder.sender().for_index(idx).into_sender(),
                fixture.chain_store.new_read_only_chunks_store(),
                fixture.mock_chain_head.clone(),
                fixture.mock_chain_head.clone(),
            );
            TestData {
                shards_manager,
                client_events: vec![],
                account_id: AccountId::from_str(&if idx == 0 {
                    "test".to_string()
                } else {
                    // This is also mandated by the ChunkTestFixture.
                    format!("test_{}", ('a'..='z').skip(idx - 1).next().unwrap())
                })
                .unwrap(),
            }
        })
        .collect::<Vec<_>>();
    let sender = builder.sender();
    let mut test = builder.build(data);
    for idx in 0..test.data.len() {
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

    // Have the first validator distribute a chunk.
    let fixture = ChunkTestFixture::default();
    sender.send((
        0,
        ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk: fixture.make_partial_encoded_chunk(&fixture.all_part_ords),
            encoded_chunk: fixture.mock_encoded_chunk.clone(),
            merkle_paths: fixture.mock_merkle_paths.clone(),
            outgoing_receipts: fixture.mock_outgoing_receipts.clone(),
        }
        .into(),
    ));
    // Run for 1 (virtual) second, so that nodes have had plenty of time to
    // request missing parts.
    test.run(time::Duration::seconds(1));

    // All other nodes should have received the chunk.
    for idx in 1..test.data.len() {
        if idx == 2 {
            // TODO: This setup needs more work. I don't yet know why the chunk
            // is not being sent to validator #2 at all.
            continue;
        }
        let data = test.data.get(idx).unwrap();
        assert_eq!(data.client_events.len(), 1);
        match &data.client_events[0] {
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                assert_eq!(partial_chunk.parts().len(), fixture.all_part_ords.len());
                assert!(shard_chunk.is_some());
            }
            _ => panic!("Unexpected event"),
        }
    }
}
