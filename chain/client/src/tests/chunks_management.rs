use std::collections::HashSet;

use crate::test_utils::TestEnv;
use near_async::messaging::CanSend;
use near_chain::ChainGenesis;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::NetworkRequests;
use near_network::types::PartialEncodedChunkRequestMsg;
use near_o11y::testonly::init_integration_logger;
use near_primitives::hash::CryptoHash;

#[test]
fn test_request_chunk_restart() {
    init_integration_logger();
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    for i in 1..4 {
        env.produce_block(0, i);
        env.network_adapters[0].pop();
    }
    let block1 = env.clients[0].chain.get_block_by_height(3).unwrap();
    let request = PartialEncodedChunkRequestMsg {
        chunk_hash: block1.chunks()[0].chunk_hash(),
        part_ords: vec![0],
        tracking_shards: HashSet::default(),
    };
    env.shards_manager_adapters[0].send(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request: request.clone(),
            route_back: CryptoHash::default(),
        },
    );
    assert!(env.network_adapters[0].pop().is_some());

    env.restart(0);
    env.shards_manager_adapters[0].send(
        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
            partial_encoded_chunk_request: request,
            route_back: CryptoHash::default(),
        },
    );
    let response = env.network_adapters[0].pop().unwrap().as_network_requests();

    if let NetworkRequests::PartialEncodedChunkResponse { response: response_body, .. } = response {
        assert_eq!(response_body.chunk_hash, block1.chunks()[0].chunk_hash());
    } else {
        println!("{:?}", response);
        assert!(false);
    }
}
