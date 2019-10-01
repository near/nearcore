use std::collections::{HashMap, HashSet};
use std::time::Duration;

use borsh::BorshSerialize;

use near_chain::{Block, ChainGenesis, Provenance};
use near_client::test_utils::TestEnv;
use near_client::Client;
use near_crypto::InMemoryBlsSigner;
use near_network::types::{ChunkOnePartRequestMsg, PeerId};
use near_primitives::challenge::{BlockDoubleSign, Challenge, ChallengeBody, ChunkProofs};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::MerklePath;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::{ChunkHash, EncodedShardChunk};
use near_primitives::test_utils::init_test_logger;

#[test]
fn test_verify_block_double_sign_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    env.produce_block(0, 1);
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let b1 = env.clients[0].produce_block(2, Duration::from_millis(10)).unwrap().unwrap();
    let signer = InMemoryBlsSigner::from_seed("test0", "test0");
    let b2 = Block::produce(
        &genesis.header,
        2,
        genesis.chunks.clone(),
        b1.header.inner.epoch_id.clone(),
        HashMap::default(),
        0,
        None,
        vec![],
        &signer,
    );
    let valid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b2.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert_eq!(
        env.clients[1].chain.verify_challenge(valid_challenge).unwrap(),
        if b1.hash() > b2.hash() { b1.hash() } else { b2.hash() }
    );
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b1.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert!(env.clients[1].chain.verify_challenge(invalid_challenge).is_err());
    let b3 = env.clients[0].produce_block(3, Duration::from_millis(10)).unwrap().unwrap();
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b3.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert!(env.clients[1].chain.verify_challenge(invalid_challenge).is_err());
}

fn create_invalid_proofs_chunk(
    client: &mut Client,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    let last_block = client.chain.get_block_by_height(1).unwrap().clone();
    let (mut chunk, merkle_paths, receipts) = client
        .produce_chunk(
            last_block.hash(),
            &last_block.header.inner.epoch_id,
            last_block.chunks[0].clone(),
            2,
            0,
            0,
        )
        .unwrap()
        .unwrap();
    chunk.header.inner.tx_root =
        CryptoHash::from_base("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf").unwrap();
    chunk.header.height_included = 2;
    chunk.header.hash = ChunkHash(hash(&chunk.header.inner.try_to_vec().unwrap()));
    chunk.header.signature =
        client.block_producer.as_ref().unwrap().signer.sign(chunk.header.hash.as_ref());
    let block = Block::produce(
        &last_block.header,
        2,
        vec![chunk.header.clone()],
        last_block.header.inner.epoch_id.clone(),
        HashMap::default(),
        0,
        None,
        vec![],
        &*client.block_producer.as_ref().unwrap().signer,
    );
    (chunk, merkle_paths, receipts, block)
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (chunk, merkle_paths, receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let valid_challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header.try_to_vec().unwrap(),
            chunk: chunk.clone(),
            //            merkle_proof: merkle_paths[0].clone(),
        }),
        &*env.clients[0].block_producer.as_ref().unwrap().signer,
    );
    assert_eq!(env.clients[0].chain.verify_challenge(valid_challenge).unwrap(), block.hash());
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

/// Receive invalid state transition in chunk as next chunk producer.
#[test]
fn test_receive_invalid_chunk_as_chunk_producer() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (chunk, merkle_paths, receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);
    env.clients[0].shards_mgr.distribute_encoded_chunk(chunk, merkle_paths, receipts);
    let (_, result) = env.clients[0].process_block(block.clone(), Provenance::NONE);
    // We have declined block with invalid chunk, but everyone who doesn't track this shard have accepted.
    // At this point we should create a challenge and add it.
    //    println!("{:?}", result);
    assert!(result.is_err());
    assert_eq!(env.clients[0].chain.head().unwrap().height, 1);
    println!("{:?}", env.network_adapters[0].pop());
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

/// Receive challenges in the blocks.
#[test]
fn test_block_challenge() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
}
