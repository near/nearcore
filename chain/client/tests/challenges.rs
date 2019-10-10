use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use near_chain::{Block, ChainGenesis, Provenance};
use near_client::test_utils::{setup_client, MockNetworkAdapter};
use near_crypto::InMemoryBlsSigner;
use near_network::types::{ChunkOnePartRequestMsg, PeerId};
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::MerkleHash;
use near_store::test_utils::create_test_store;

#[test]
fn test_request_chunk_restart() {
    init_test_logger();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::test();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let mut client = setup_client(
        store.clone(),
        vec![vec!["test1"]],
        1,
        1,
        "test1",
        network_adapter.clone(),
        chain_genesis.clone(),
    );
    let mut block = None;
    for i in 1..3 {
        block = client.produce_block(i, Duration::from_millis(100)).unwrap();
        let (accepted_blocks, _) =
            client.process_block(block.clone().unwrap(), Provenance::PRODUCED);
        client.on_block_accepted(
            accepted_blocks[0].hash.clone(),
            accepted_blocks[0].status.clone(),
            accepted_blocks[0].provenance.clone(),
        );
        network_adapter.pop();
    }
    let block1 = block.clone().unwrap();
    let request = ChunkOnePartRequestMsg {
        shard_id: 0,
        chunk_hash: block1.chunks[0].chunk_hash(),
        height: block1.header.inner.height,
        part_id: 0,
        tracking_shards: HashSet::default(),
    };
    client.shards_mgr.process_chunk_one_part_request(request.clone(), PeerId::random()).unwrap();
    assert!(network_adapter.pop().is_some());

    let mut client2 = setup_client(
        store,
        vec![vec!["test1"]],
        1,
        1,
        "test1",
        network_adapter.clone(),
        chain_genesis,
    );
    client2.shards_mgr.process_chunk_one_part_request(request, PeerId::random()).unwrap();
    // TODO: should be some() with the same chunk.
    assert!(network_adapter.pop().is_none());
}

/// Validator signed on block X on fork A, and then signs on block X + 1 on fork B which doesn't have X.
#[test]
fn test_sign_on_competing_fork() {}

fn create_block_with_invalid_chunk(
    prev_block_header: &BlockHeader,
    account_id: &str,
) -> (Block, EncodedShardChunk) {
    let signer = Arc::new(InMemoryBlsSigner::from_seed(account_id, account_id));
    let (invalid_encoded_chunk, _merkle_paths) = EncodedShardChunk::new(
        prev_block_header.hash,
        CryptoHash::from_base("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf").unwrap(),
        9, /* TODO MOO */
        1,
        0,
        20,
        12,
        0,
        0,
        0,
        MerkleHash::default(),
        vec![],
        &vec![],
        &vec![],
        MerkleHash::default(),
        signer.clone(),
    )
    .unwrap();
    let block_with_invalid_chunk = Block::produce(
        &prev_block_header,
        1,
        vec![invalid_encoded_chunk.header.clone()],
        prev_block_header.inner.epoch_id.clone(),
        HashMap::default(),
        0,
        None,
        signer,
    );
    (block_with_invalid_chunk, invalid_encoded_chunk)
}

/// Receive invalid state transition in chunk as next chunk producer.
#[test]
fn test_receive_invalid_chunk_as_chunk_producer() {
    init_test_logger();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::test();
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let mut client = setup_client(
        store,
        vec![vec!["test1", "test2"]],
        1,
        1,
        "test2",
        network_adapter,
        chain_genesis,
    );
    let prev_block_header = client.chain.get_header_by_height(0).unwrap();
    let (block_with_invalid_chunk, _) = create_block_with_invalid_chunk(prev_block_header, "test2");
    let (_, result) = client.process_block(block_with_invalid_chunk.clone(), Provenance::NONE);
    // We have declined block with invalid chunk, but everyone who doesn't track this shard have accepted.
    // At this point we should create a challenge and add it.
    assert!(result.is_err());
    assert_eq!(client.chain.head().unwrap().height, 0);
}

/// Receive invalid state transition in chunk as a validator / non-producer.
#[test]
fn test_receive_invalid_chunk_as_validator() {}

/// Receive two different chunks from the same chunk producer.
#[test]
fn test_receive_two_chunks_from_one_producer() {}

/// Receive two different blocks from the same block producer.
#[test]
fn test_receive_two_blocks_from_one_producer() {}
