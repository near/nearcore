use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::{NetworkRequests, NetworkResponses, PartialEncodedChunkRequestMsg};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

/// A peer-supplied PartialEncodedChunkRequest for the genesis
/// chunk used to panic on a receipts-root assert (genesis stores
/// CryptoHash::default(); the recomputed root is non-default).
#[test]
fn test_genesis_chunk_request_does_not_panic() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().build();

    let response_seen = Arc::new(Mutex::new(None));
    let response_seen_handler = response_seen.clone();
    let peer_handle = env.node_datas[0].peer_manager_sender.actor_handle();
    let peer_actor = env.test_loop.data.get_mut(&peer_handle);
    peer_actor.register_override_handler(Box::new(move |request| match request {
        NetworkRequests::PartialEncodedChunkResponse { response, .. } => {
            *response_seen_handler.lock() = Some(response);
            HandlerResult::Handled(NetworkResponses::NoResponse)
        }
        _ => HandlerResult::Unhandled(request),
    }));

    let genesis_chunk_hash = env
        .validator()
        .client()
        .chain
        .genesis_block()
        .chunks()
        .iter_raw()
        .next()
        .unwrap()
        .chunk_hash()
        .clone();

    let request = PartialEncodedChunkRequestMsg {
        chunk_hash: genesis_chunk_hash.clone(),
        part_ords: vec![0],
        tracking_shards: HashSet::new(),
    };
    env.node_datas[0].shards_manager_sender.send(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request: request,
            route_back: CryptoHash::default(),
        },
    );

    env.test_loop.run_for(Duration::seconds(1));

    let response =
        response_seen.lock().take().expect("expected response for genesis chunk request");
    assert_eq!(response.chunk_hash, genesis_chunk_hash);
    assert!(response.parts.is_empty(), "expected no parts for genesis chunk response");
    assert!(response.receipts.is_empty(), "expected no receipts for genesis chunk response");
}
