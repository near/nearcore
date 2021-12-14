use std::collections::HashSet;

use crate::test_utils::TestEnv;
use near_chain::ChainGenesis;
use near_crypto::KeyType;
use near_logger_utils::{init_integration_logger, init_test_logger};
use near_network::types::NetworkRequests;
use near_network_primitives::types::PartialEncodedChunkRequestMsg;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunkV2, ShardChunkHeader, ShardChunkHeaderInner, ShardChunkHeaderV2,
};
use near_primitives::types::BlockHeight;
use near_primitives::validator_signer::InMemoryValidatorSigner;

#[test]
fn test_request_chunk_restart() {
    init_integration_logger();
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    for i in 1..4 {
        env.produce_block(0, i);
        env.network_adapters[0].pop();
    }
    let block1 = env.clients[0].chain.get_block_by_height(3).unwrap().clone();
    let request = PartialEncodedChunkRequestMsg {
        chunk_hash: block1.chunks()[0].chunk_hash(),
        part_ords: vec![0],
        tracking_shards: HashSet::default(),
    };
    let client = &mut env.clients[0];
    client.shards_mgr.process_partial_encoded_chunk_request(
        request.clone(),
        CryptoHash::default(),
        client.chain.mut_store(),
    );
    assert!(env.network_adapters[0].pop().is_some());

    env.restart(0);
    let client = &mut env.clients[0];
    client.shards_mgr.process_partial_encoded_chunk_request(
        request,
        CryptoHash::default(),
        client.chain.mut_store(),
    );
    let response = env.network_adapters[0].pop().unwrap().as_network_requests();

    if let NetworkRequests::PartialEncodedChunkResponse { response: response_body, .. } = response {
        assert_eq!(response_body.chunk_hash, block1.chunks()[0].chunk_hash());
    } else {
        println!("{:?}", response);
        assert!(false);
    }
}

fn update_chunk_hash(chunk: PartialEncodedChunkV2, new_hash: ChunkHash) -> PartialEncodedChunkV2 {
    let new_header = match chunk.header {
        ShardChunkHeader::V1(mut header) => {
            header.hash = new_hash;
            ShardChunkHeader::V1(header)
        }
        ShardChunkHeader::V2(mut header) => {
            header.hash = new_hash;
            ShardChunkHeader::V2(header)
        }
        ShardChunkHeader::V3(mut header) => {
            header.hash = new_hash;
            ShardChunkHeader::V3(header)
        }
    };
    PartialEncodedChunkV2 { header: new_header, parts: chunk.parts, receipts: chunk.receipts }
}

fn update_chunk_height_created(
    header: ShardChunkHeader,
    new_height: BlockHeight,
) -> ShardChunkHeader {
    match header {
        ShardChunkHeader::V1(mut header) => {
            header.inner.height_created = new_height;
            ShardChunkHeader::V1(header)
        }
        ShardChunkHeader::V2(mut header) => {
            header.inner.height_created = new_height;
            ShardChunkHeader::V2(header)
        }
        ShardChunkHeader::V3(mut header) => {
            match &mut header.inner {
                ShardChunkHeaderInner::V1(inner) => inner.height_created = new_height,
                ShardChunkHeaderInner::V2(inner) => inner.height_created = new_height,
            }
            ShardChunkHeader::V3(header)
        }
    }
}
