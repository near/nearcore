use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::Action;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_network::recv_permit::RecvMessagePermit;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::{NetworkRequests, PartialEncodedChunkRequestMsg};
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

    // The response is kept instead of sent, so the test can look at it.
    let response_seen = Arc::new(Mutex::new(None));
    let response_seen_handler = response_seen.clone();
    env.network_rule(
        0,
        |request| matches!(request, NetworkRequests::PartialEncodedChunkResponse { .. }),
        Action::Replace(Box::new(move |request| {
            let NetworkRequests::PartialEncodedChunkResponse { response, .. } = request else {
                unreachable!("the rule matches chunk responses only");
            };
            *response_seen_handler.lock() = Some(response);
            Vec::new()
        })),
    );

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
            recv_permit: RecvMessagePermit::none(),
        },
    );

    env.test_loop.run_for(Duration::seconds(1));

    let response =
        response_seen.lock().take().expect("expected response for genesis chunk request");
    assert_eq!(response.chunk_hash, genesis_chunk_hash);
    assert!(response.parts.is_empty(), "expected no parts for genesis chunk response");
    assert!(response.receipts.is_empty(), "expected no receipts for genesis chunk response");
}
