use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use near_chain::{Block, ChainGenesis, Provenance};
use near_client::test_utils::{setup_client, MockNetworkAdapter};
use near_client::Client;
use near_crypto::{InMemorySigner, KeyType};
use near_network::types::{ChunkOnePartRequestMsg, PeerId};
use near_network::NetworkClientResponses;
use near_primitives::block::BlockHeader;
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, MerkleHash};
use near_store::test_utils::create_test_store;

struct TestEnv {
    chain_genesis: ChainGenesis,
    validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockNetworkAdapter>>,
    pub clients: Vec<Client>,
}

impl TestEnv {
    pub fn new(num_clients: usize, num_validators: usize) -> Self {
        let chain_genesis = ChainGenesis::test();
        let validators: Vec<AccountId> =
            (0..num_validators).map(|i| format!("test{}", i)).collect();
        let network_adapters =
            (0..num_clients).map(|_| Arc::new(MockNetworkAdapter::default())).collect::<Vec<_>>();
        let clients = (0..num_clients)
            .map(|i| {
                let store = create_test_store();
                setup_client(
                    store.clone(),
                    vec![validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
                    1,
                    1,
                    Some(&format!("test{}", i)),
                    network_adapters[i].clone(),
                    chain_genesis.clone(),
                )
            })
            .collect();
        TestEnv { chain_genesis, validators, network_adapters, clients }
    }

    pub fn produce_block(&mut self, id: usize, height: BlockIndex) {
        let block = self.clients[id].produce_block(height, Duration::from_millis(10)).unwrap();
        let (mut accepted_blocks, _) =
            self.clients[id].process_block(block.clone().unwrap(), Provenance::PRODUCED);
        let more_accepted_blocks = self.clients[id].run_catchup().unwrap();
        accepted_blocks.extend(more_accepted_blocks);
        for accepted_block in accepted_blocks.into_iter() {
            self.clients[id].on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }

    pub fn send_money(&mut self, id: usize) -> NetworkClientResponses {
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test1".to_string(),
            &signer,
            100,
            self.clients[id].chain.head().unwrap().last_block_hash,
        );
        self.clients[id].process_tx(tx)
    }

    pub fn restart(&mut self, id: usize) {
        let store = self.clients[id].chain.store().store().clone();
        self.clients[id] = setup_client(
            store,
            vec![self.validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
            1,
            1,
            Some(&format!("test{}", id)),
            self.network_adapters[id].clone(),
            self.chain_genesis.clone(),
        )
    }
}

#[test]
fn test_verify_double_sign_challenge() {
    let mut env = TestEnv::new(2, 1);
    env.produce_block(0, 1);
    let b1 = env.clients[0].produce_block(2, Duration::from_millis(10)).unwrap().unwrap();
    let b2 = env.clients[0].produce_block(2, Duration::from_millis(10)).unwrap().unwrap();
    let valid_challenge = Challenge::BlockDoubleSign {
        left_block_header: b1.header.clone(),
        right_block_header: b2.header.clone(),
    };
    assert!(env.clients[0].verify_challenge(valid_challenge).unwrap());
    let invalid_challenge = Challenge::BlockDoubleSign {
        left_block_header: b1.header.clone(),
        right_block_header: b1.header.clone(),
    };
    assert!(!env.clients[0].verify_challenge(invalid_challenge).unwrap());
    let b3 = env.clients[0].produce_block(3, Duration::from_millis(10)).unwrap().unwrap();
    let invalid_challenge =
        Challenge::BlockDoubleSign { left_block_header: b1.header, right_block_header: b3.header };
    assert!(!env.clients[0].verify_challenge(invalid_challenge).unwrap());
}

#[test]
fn test_request_chunk_restart() {
    init_test_logger();
    let mut env = TestEnv::new(1, 1);
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
    let signer = Arc::new(InMemorySigner::from_seed(account_id, KeyType::ED25519, account_id));
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
