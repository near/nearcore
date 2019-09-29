use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use borsh::BorshSerialize;

use near_chain::{Block, ChainGenesis, Provenance};
use near_client::test_utils::{setup_client, MockNetworkAdapter, TestEnv};
use near_crypto::{InMemorySigner, KeyType};
use near_network::types::{ChunkOnePartRequestMsg, PeerId};
use near_primitives::block::BlockHeader;
use near_primitives::challenge::Challenge;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::{ChunkHash, EncodedShardChunk};
use near_primitives::test_utils::init_test_logger;
use near_primitives::types::MerkleHash;
use near_store::test_utils::create_test_store;

#[test]
fn test_verify_block_double_sign_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    env.produce_block(0, 1);
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let b1 = env.clients[0].produce_block(2, Duration::from_millis(10)).unwrap().unwrap();
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let b2 = Block::produce(
        &genesis.header,
        2,
        genesis.chunks.clone(),
        b1.header.inner.epoch_id.clone(),
        HashMap::default(),
        0,
        None,
        &signer,
    );
    let valid_challenge = Challenge::BlockDoubleSign {
        left_block_header: b1.header.clone(),
        right_block_header: b2.header.clone(),
    };
    assert!(env.clients[1].chain.verify_challenge(valid_challenge).unwrap());
    let invalid_challenge = Challenge::BlockDoubleSign {
        left_block_header: b1.header.clone(),
        right_block_header: b1.header.clone(),
    };
    assert!(!env.clients[1].chain.verify_challenge(invalid_challenge).unwrap());
    let b3 = env.clients[0].produce_block(3, Duration::from_millis(10)).unwrap().unwrap();
    let invalid_challenge =
        Challenge::BlockDoubleSign { left_block_header: b1.header, right_block_header: b3.header };
    assert!(!env.clients[1].chain.verify_challenge(invalid_challenge).unwrap());
}

#[test]
fn test_verify_chunk_double_sign_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let last_block = env.clients[0].chain.get_block_by_height(1).unwrap().clone();
    let (chunk1, _, _) = env.clients[0]
        .produce_chunk(
            last_block.hash(),
            &last_block.header.inner.epoch_id,
            last_block.chunks[0].clone(),
            2,
            0,
        )
        .unwrap()
        .unwrap();
    let (chunk2, _, _) = env.clients[0]
        .produce_chunk(
            last_block.header.inner.prev_hash,
            &last_block.header.inner.epoch_id,
            last_block.chunks[0].clone(),
            2,
            0,
        )
        .unwrap()
        .unwrap();
    let valid_challenge = Challenge::ChunkDoubleSign {
        left_chunk_header: chunk1.header.clone(),
        right_chunk_header: chunk2.header.clone(),
    };
    assert!(env.clients[0].chain.verify_challenge(valid_challenge).unwrap());
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let last_block = env.clients[0].chain.get_block_by_height(1).unwrap().clone();
    let (mut chunk, merkle_paths, receipts) = env.clients[0]
        .produce_chunk(
            last_block.hash(),
            &last_block.header.inner.epoch_id,
            last_block.chunks[0].clone(),
            2,
            0,
        )
        .unwrap()
        .unwrap();
    chunk.header.inner.tx_root =
        CryptoHash::from_base("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf").unwrap();
    chunk.header.height_included = 2;
    chunk.header.hash = ChunkHash(hash(&chunk.header.inner.try_to_vec().unwrap()));
    chunk.header.signature =
        env.clients[0].block_producer.as_ref().unwrap().signer.sign(chunk.header.hash.as_ref());

    let valid_challenge = Challenge::ChunkProofs { chunk_header: chunk.header.clone() };
    assert_eq!(
        env.clients[0].chain.verify_challenge(valid_challenge.clone()).err().unwrap().kind(),
        near_chain::ErrorKind::ChunksMissing(vec![chunk.header.clone()])
    );

    env.clients[0].shards_mgr.distribute_encoded_chunk(chunk, merkle_paths, receipts);
    assert!(env.clients[0].chain.verify_challenge(valid_challenge).unwrap());
}

#[test]
fn test_request_chunk_restart() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    for i in 1..3 {
        env.produce_block(0, i);
        env.network_adapters[0].pop();
    }
    let block1 = env.clients[0].chain.get_block_by_height(3).unwrap().clone();
    let request = ChunkOnePartRequestMsg {
        shard_id: 0,
        chunk_hash: block1.chunks[0].chunk_hash(),
        height: block1.header.inner.height,
        part_id: 0,
        tracking_shards: HashSet::default(),
    };
    env.clients[0]
        .shards_mgr
        .process_chunk_one_part_request(request.clone(), PeerId::random())
        .unwrap();
    assert!(env.network_adapters[0].pop().is_some());

    env.restart(0);
    env.clients[0].shards_mgr.process_chunk_one_part_request(request, PeerId::random()).unwrap();
    // TODO: should be some() with the same chunk.
    assert!(env.network_adapters[0].pop().is_none());
}

/// Validator signed on block X on fork A, and then signs on block X + 1 on fork B which doesn't have X.
#[test]
fn test_sign_on_competing_fork() {}

fn create_block_with_invalid_chunk(
    prev_block_header: &BlockHeader,
    account_id: &str,
) -> (Block, EncodedShardChunk) {
    let signer = InMemorySigner::from_seed(account_id, KeyType::ED25519, account_id);
    let (invalid_encoded_chunk, _merkle_paths) = EncodedShardChunk::new(
        prev_block_header.hash,
        CryptoHash::from_base("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf").unwrap(),
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
        &signer,
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
        &signer,
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
        Some("test2"),
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
