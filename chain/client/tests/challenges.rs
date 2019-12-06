use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use borsh::BorshSerialize;

use near::GenesisConfig;
use near_chain::validate::validate_challenge;
use near_chain::{Block, ChainGenesis, ChainStoreAccess, ErrorKind, Provenance, RuntimeAdapter};
use near_client::test_utils::{MockNetworkAdapter, TestEnv};
use near_client::Client;
use near_crypto::{InMemorySigner, KeyType};
use near_network::NetworkRequests;
use near_primitives::challenge::{BlockDoubleSign, Challenge, ChallengeBody, ChunkProofs};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::{ChunkHash, EncodedShardChunk};
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::StateRoot;
use near_store::test_utils::create_test_store;

#[test]
fn test_verify_block_double_sign_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    env.produce_block(0, 1);
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let b1 = env.clients[0].produce_block(2, Duration::from_millis(10)).unwrap().unwrap();

    env.process_block(0, b1.clone(), Provenance::NONE);

    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let b2 = Block::produce(
        &genesis.header,
        2,
        genesis.chunks.clone(),
        b1.header.inner_lite.epoch_id.clone(),
        vec![],
        0,
        None,
        vec![],
        vec![],
        &signer,
        0.into(),
        CryptoHash::default(),
        CryptoHash::default(),
    );
    let epoch_id = b1.header.inner_lite.epoch_id.clone();
    let valid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b2.header.try_to_vec().unwrap(),
            right_block_header: b1.header.try_to_vec().unwrap(),
        }),
        signer.account_id.clone(),
        &signer,
    );
    assert_eq!(
        validate_challenge(
            &*env.clients[1].chain.runtime_adapter,
            &epoch_id,
            &genesis.hash(),
            &valid_challenge
        )
        .unwrap()
        .0,
        if b1.hash() > b2.hash() { b1.hash() } else { b2.hash() }
    );
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b1.header.try_to_vec().unwrap(),
        }),
        signer.account_id.clone(),
        &signer,
    );
    assert!(validate_challenge(
        &*env.clients[1].chain.runtime_adapter,
        &epoch_id,
        &genesis.hash(),
        &invalid_challenge
    )
    .is_err());
    let b3 = env.clients[0].produce_block(3, Duration::from_millis(10)).unwrap().unwrap();
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b3.header.try_to_vec().unwrap(),
        }),
        signer.account_id.clone(),
        &signer,
    );
    assert!(validate_challenge(
        &*env.clients[1].chain.runtime_adapter,
        &epoch_id,
        &genesis.hash(),
        &invalid_challenge
    )
    .is_err());

    let (_, result) = env.clients[0].process_block(b2, Provenance::NONE);
    let _ = env.network_adapters[0].pop();
    assert!(result.is_ok());
    let last_message = env.network_adapters[0].pop().unwrap();
    if let NetworkRequests::Challenge(network_challenge) = last_message {
        assert_eq!(network_challenge, valid_challenge);
    } else {
        assert!(false);
    }
}

fn create_invalid_proofs_chunk(
    client: &mut Client,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    let last_block =
        client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap().clone();
    let (mut chunk, merkle_paths, receipts) = client
        .produce_chunk(
            last_block.hash(),
            &last_block.header.inner_lite.epoch_id,
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
        last_block.header.inner_lite.epoch_id.clone(),
        vec![],
        0,
        None,
        vec![],
        vec![],
        &*client.block_producer.as_ref().unwrap().signer,
        0.into(),
        last_block.header.prev_hash,
        CryptoHash::default(),
    );
    (chunk, merkle_paths, receipts, block)
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
    let valid_challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header.try_to_vec().unwrap(),
            chunk: chunk.clone(),
            merkle_proof: merkle_paths[chunk.header.inner.shard_id as usize].clone(),
        }),
        env.clients[0].block_producer.as_ref().unwrap().account_id.clone(),
        &*env.clients[0].block_producer.as_ref().unwrap().signer,
    );
    assert_eq!(
        validate_challenge(
            &*env.clients[0].chain.runtime_adapter,
            &block.header.inner_lite.epoch_id,
            &block.header.prev_hash,
            &valid_challenge
        )
        .unwrap(),
        (block.hash(), vec!["test0".to_string()])
    );
}

#[test]
fn test_verify_chunk_invalid_state_challenge() {
    let store1 = create_test_store();
    let genesis_config = GenesisConfig::test(vec!["test0", "test1"], 1);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(near::NightshadeRuntime::new(
        Path::new("."),
        store1,
        genesis_config,
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let genesis_hash = env.clients[0].chain.genesis().hash();
    env.produce_block(0, 1);
    env.clients[0].process_tx(SignedTransaction::send_money(
        0,
        "test0".to_string(),
        "test1".to_string(),
        &signer,
        1000,
        genesis_hash,
    ));
    env.produce_block(0, 2);

    // Invalid chunk & block.
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let last_block = env.clients[0].chain.get_block(&last_block_hash).unwrap().clone();
    let prev_to_last_block =
        env.clients[0].chain.get_block(&last_block.header.prev_hash).unwrap().clone();
    let (mut invalid_chunk, merkle_paths) = env.clients[0]
        .shards_mgr
        .create_encoded_shard_chunk(
            last_block.hash(),
            StateRoot::default(),
            CryptoHash::default(),
            last_block.header.inner_lite.height + 1,
            0,
            0,
            1_000,
            0,
            0,
            0,
            vec![],
            vec![],
            &vec![],
            last_block.chunks[0].inner.outgoing_receipts_root,
            CryptoHash::default(),
            &signer,
        )
        .unwrap();

    let client = &mut env.clients[0];

    // Receive invalid chunk to the validator.
    client
        .shards_mgr
        .distribute_encoded_chunk(
            invalid_chunk.clone(),
            merkle_paths,
            vec![],
            client.chain.mut_store(),
        )
        .unwrap();

    invalid_chunk.header.height_included = last_block.header.inner_lite.height + 1;
    let block = Block::produce(
        &last_block.header,
        last_block.header.inner_lite.height + 1,
        vec![invalid_chunk.header.clone()],
        last_block.header.inner_lite.epoch_id.clone(),
        vec![],
        0,
        None,
        vec![],
        vec![],
        &signer,
        0.into(),
        last_block.header.prev_hash,
        prev_to_last_block.header.prev_hash,
    );

    let challenge_body = {
        use near_chain::chain::{ChainUpdate, OrphanBlockPool};
        let chain = &mut client.chain;
        let adapter = chain.runtime_adapter.clone();
        let validity_period = chain.transaction_validity_period;
        let epoch_length = chain.epoch_length;
        let empty_block_pool = OrphanBlockPool::new();

        let mut chain_update = ChainUpdate::new(
            chain.mut_store(),
            adapter,
            &empty_block_pool,
            &empty_block_pool,
            validity_period,
            epoch_length,
            0,
        );

        chain_update
            .create_chunk_state_challenge(&last_block, &block, &block.chunks[0].clone())
            .unwrap()
    };
    {
        let prev_merkle_proofs = Block::compute_chunk_headers_root(&last_block.chunks).1;
        let merkle_proofs = Block::compute_chunk_headers_root(&block.chunks).1;
        assert_eq!(prev_merkle_proofs[0], challenge_body.prev_merkle_proof);
        assert_eq!(merkle_proofs[0], challenge_body.merkle_proof);
        assert_eq!(
            challenge_body.partial_state.0,
            vec![
                vec![
                    1, 7, 0, 54, 35, 25, 236, 254, 223, 178, 21, 126, 128, 90, 220, 113, 148, 98,
                    170, 10, 132, 196, 225, 254, 113, 142, 127, 70, 11, 237, 232, 9, 235, 151, 182,
                    235, 217, 45, 97, 107, 149, 56, 40, 110, 74, 92, 207, 197, 223, 145, 15, 224,
                    171, 209, 143, 188, 13, 150, 92, 173, 30, 158, 111, 33, 240, 72, 207, 145, 53,
                    243, 62, 68, 133, 149, 26, 124, 103, 244, 149, 119, 73, 162, 6, 36, 96, 152,
                    42, 151, 132, 135, 99, 142, 123, 210, 222, 132, 40, 77, 188, 25, 133, 2, 0, 0,
                    0, 0, 0
                ],
                vec![
                    3, 1, 0, 0, 0, 16, 145, 197, 164, 9, 176, 3, 85, 162, 73, 143, 222, 62, 17, 69,
                    37, 191, 194, 153, 103, 173, 152, 212, 234, 124, 100, 172, 26, 55, 247, 61, 55,
                    183, 67, 133, 2, 0, 0, 0, 0, 0
                ]
            ],
        );
    }
    let challenge =
        Challenge::produce(ChallengeBody::ChunkState(challenge_body), "test0".to_string(), &signer);
    assert_eq!(
        validate_challenge(
            &*client.chain.runtime_adapter,
            &block.header.inner_lite.epoch_id,
            &block.header.prev_hash,
            &challenge
        )
        .unwrap(),
        (block.hash(), vec!["test0".to_string()])
    );

    // Process the block with invalid chunk and make sure it's marked as invalid at the end.
    // And the same challenge created and sent out.
    let (_, tip) = client.process_block(block, Provenance::NONE);
    assert!(tip.is_err());

    let last_message = env.network_adapters[0].pop().unwrap();
    if let NetworkRequests::Challenge(network_challenge) = last_message {
        assert_eq!(challenge, network_challenge);
    } else {
        assert!(false);
    }
}

/// Receive invalid state transition in chunk as next chunk producer.
#[test]
fn test_receive_invalid_chunk_as_chunk_producer() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    env.produce_block(0, 1);
    let block1 = env.clients[0].chain.get_block_by_height(1).unwrap().clone();
    env.process_block(1, block1, Provenance::NONE);
    let (chunk, merkle_paths, receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);
    let client = &mut env.clients[0];
    assert!(client
        .shards_mgr
        .distribute_encoded_chunk(
            chunk.clone(),
            merkle_paths.clone(),
            receipts.clone(),
            client.chain.mut_store()
        )
        .is_err());
    let (_, result) = client.process_block(block.clone(), Provenance::NONE);
    // We have declined block with invalid chunk.
    assert!(result.is_err());
    assert_eq!(client.chain.head().unwrap().height, 1);
    // But everyone who doesn't track this shard have accepted.
    let receipts_hashes = env.clients[0].runtime_adapter.build_receipts_hashes(&receipts).unwrap();
    let (_receipts_root, receipts_proofs) = merklize(&receipts_hashes);
    let one_part_receipt_proofs = env.clients[0].shards_mgr.receipts_recipient_filter(
        0,
        &HashSet::default(),
        &receipts,
        &receipts_proofs,
    );

    assert!(env.clients[1]
        .process_partial_encoded_chunk(chunk.create_partial_encoded_chunk(
            vec![0],
            true,
            one_part_receipt_proofs,
            &vec![merkle_paths[0].clone()]
        ))
        .is_ok());
    env.process_block(1, block.clone(), Provenance::NONE);

    // At this point we should create a challenge and send it out.
    let last_message = env.network_adapters[0].pop().unwrap();
    if let NetworkRequests::Challenge(Challenge {
        body: ChallengeBody::ChunkProofs(chunk_proofs),
        ..
    }) = last_message.clone()
    {
        assert_eq!(chunk_proofs.chunk, chunk);
    } else {
        assert!(false);
    }

    // The other client processes challenge and invalidates the chain.
    if let NetworkRequests::Challenge(challenge) = last_message {
        assert_eq!(env.clients[1].chain.head().unwrap().height, 2);
        assert!(env.clients[1].process_challenge(challenge).is_ok());
        assert_eq!(env.clients[1].chain.head().unwrap().height, 1);
    }
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
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
    let challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header.try_to_vec().unwrap(),
            chunk: chunk.clone(),
            merkle_proof: merkle_paths[chunk.header.inner.shard_id as usize].clone(),
        }),
        env.clients[0].block_producer.as_ref().unwrap().account_id.clone(),
        &*env.clients[0].block_producer.as_ref().unwrap().signer,
    );
    env.clients[0].process_challenge(challenge.clone()).unwrap();
    env.produce_block(0, 2);
    assert_eq!(env.clients[0].chain.get_block_by_height(2).unwrap().challenges, vec![challenge]);
    assert!(env.clients[0].chain.mut_store().is_block_challenged(&block.hash()).unwrap());
}

/// If there are two blocks produced at the same height but by different block producers, no
/// challenge should be generated
#[test]
fn test_challenge_in_different_epoch() {
    init_test_logger();
    let mut genesis_config = GenesisConfig::test(vec!["test0", "test1"], 2);
    genesis_config.epoch_length = 2;
    //    genesis_config.validator_kickout_threshold = 10;
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let runtime1 = Arc::new(near::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis_config.clone(),
        vec![],
        vec![],
    ));
    let runtime2 = Arc::new(near::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis_config,
        vec![],
        vec![],
    ));
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![runtime1, runtime2];
    let networks = vec![network_adapter.clone(), network_adapter.clone()];
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 2;
    let mut env =
        TestEnv::new_with_runtime_and_network_adapter(chain_genesis, 2, 2, runtimes, networks);
    let mut fork_blocks = vec![];
    for i in 1..5 {
        let block1 =
            env.clients[0].produce_block(2 * i - 1, Duration::from_millis(100)).unwrap().unwrap();
        env.process_block(0, block1, Provenance::PRODUCED);

        let block2 =
            env.clients[1].produce_block(2 * i, Duration::from_millis(100)).unwrap().unwrap();
        env.process_block(1, block2.clone(), Provenance::PRODUCED);
        fork_blocks.push(block2);
    }

    let fork1_block = env.clients[0].produce_block(9, Duration::from_millis(100)).unwrap().unwrap();
    env.process_block(0, fork1_block, Provenance::PRODUCED);
    let fork2_block = env.clients[1].produce_block(9, Duration::from_millis(100)).unwrap().unwrap();
    fork_blocks.push(fork2_block);
    for block in fork_blocks {
        let height = block.header.inner_lite.height;
        let (_, result) = env.clients[0].process_block(block, Provenance::NONE);
        match env.clients[0].run_catchup(&vec![]) {
            Ok(accepted_blocks) => {
                for accepted_block in accepted_blocks {
                    env.clients[0].on_block_accepted(
                        accepted_block.hash,
                        accepted_block.status,
                        accepted_block.provenance,
                    );
                }
            }
            Err(e) => panic!("error in catching up: {}", e),
        }
        let len = network_adapter.requests.write().unwrap().len();
        for _ in 0..len {
            match network_adapter.requests.write().unwrap().pop_front() {
                Some(NetworkRequests::Challenge(_)) => {
                    panic!("Unexpected challenge");
                }
                Some(_) => {}
                None => break,
            }
        }
        if height < 9 {
            assert!(result.is_ok());
        } else {
            if let Err(e) = result {
                match e.kind() {
                    ErrorKind::ChunksMissing(_) => {}
                    _ => panic!(format!("unexpected error: {}", e)),
                }
            }
        }
    }
}
