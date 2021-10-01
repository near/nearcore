use std::collections::HashSet;

use near_chain::ChainGenesis;
use near_client::test_utils::TestEnv;
use near_crypto::KeyType;
use near_logger_utils::{init_integration_logger, init_test_logger};
use near_network::types::PartialEncodedChunkRequestMsg;
use near_network::NetworkRequests;
use near_primitives::hash::{hash, CryptoHash};
#[cfg(feature = "protocol_feature_block_header_v3")]
use near_primitives::sharding::ShardChunkHeaderInner;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunkV2, ShardChunkHeader, ShardChunkHeaderV2,
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
    let response = env.network_adapters[0].pop().unwrap();
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
        #[cfg(feature = "protocol_feature_block_header_v3")]
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
        #[cfg(feature = "protocol_feature_block_header_v3")]
        ShardChunkHeader::V3(mut header) => {
            match &mut header.inner {
                ShardChunkHeaderInner::V1(inner) => inner.height_created = new_height,
                ShardChunkHeaderInner::V2(inner) => inner.height_created = new_height,
            }
            ShardChunkHeader::V3(header)
        }
    }
}

#[test]
fn store_partial_encoded_chunk_sanity() {
    init_test_logger();
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let mut partial_encoded_chunk = PartialEncodedChunkV2 {
        header: ShardChunkHeader::V2(ShardChunkHeaderV2::new(
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            1,
            1,
            0,
            0,
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            &signer,
        )),
        parts: vec![],
        receipts: vec![],
    };
    let block_hash = *env.clients[0].chain.genesis().hash();
    let block = env.clients[0].chain.get_block(&block_hash).unwrap().clone();
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 0);
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );

    // Check replacing
    partial_encoded_chunk = update_chunk_hash(partial_encoded_chunk, ChunkHash(hash(&[123])));
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );

    // Check adding
    let mut partial_encoded_chunk2 = partial_encoded_chunk.clone();
    let h = ShardChunkHeader::V2(ShardChunkHeaderV2::new(
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        1,
        1,
        173465755,
        0,
        0,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        &signer,
    ));
    partial_encoded_chunk2.header = h;
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 1);
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk2.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1).len(), 2);
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&0],
        partial_encoded_chunk
    );
    assert_eq!(
        env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(1)[&173465755],
        partial_encoded_chunk2
    );

    // Check horizon
    env.produce_block(0, 3);
    let mut partial_encoded_chunk3 = partial_encoded_chunk.clone();
    let mut h = ShardChunkHeader::V2(ShardChunkHeaderV2::new(
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        1,
        2,
        1,
        0,
        0,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        &signer,
    ));
    partial_encoded_chunk3.header = h.clone();
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(2).len(), 0);
    h = update_chunk_height_created(h, 9);
    partial_encoded_chunk3.header = h.clone();
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(9).len(), 0);
    h = update_chunk_height_created(h, 5);
    partial_encoded_chunk3.header = h.clone();
    env.clients[0]
        .shards_mgr
        .store_partial_encoded_chunk(&block.header(), partial_encoded_chunk3.clone());
    assert_eq!(env.clients[0].shards_mgr.get_stored_partial_encoded_chunks(5).len(), 1);
}
