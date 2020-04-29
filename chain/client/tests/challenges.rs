use std::collections::HashSet;
use std::mem::swap;
use std::path::Path;
use std::sync::Arc;

use borsh::BorshSerialize;

use near_chain::chain::BlockEconomicsConfig;
use near_chain::validate::validate_challenge;
use near_chain::{
    Block, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Error, ErrorKind, Provenance,
    RuntimeAdapter,
};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::Client;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_logger_utils::init_test_logger;
use near_network::test_utils::MockNetworkAdapter;
use near_network::NetworkRequests;
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChunkProofs, MaybeEncodedShardChunk,
};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath};
use near_primitives::receipt::Receipt;
use near_primitives::serialize::BaseDecode;
use near_primitives::sharding::{EncodedShardChunk, ReedSolomonWrapper};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::StateRoot;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_store::test_utils::create_test_store;
use neard::config::{GenesisExt, FISHERMEN_THRESHOLD};
use neard::NightshadeRuntime;
use num_rational::Rational;

#[test]
fn test_verify_block_double_sign_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 2, 1);
    env.produce_block(0, 1);
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap().clone();
    let b1 = env.clients[0].produce_block(2).unwrap().unwrap();

    env.process_block(0, b1.clone(), Provenance::NONE);

    let signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
    let b2 = Block::produce(
        &genesis.header,
        2,
        genesis.chunks.clone(),
        b1.header.inner_lite.epoch_id.clone(),
        b1.header.inner_lite.next_epoch_id.clone(),
        vec![],
        Rational::from_integer(0),
        0,
        None,
        vec![],
        vec![],
        &signer,
        b1.header.inner_lite.next_bp_hash.clone(),
    );
    let epoch_id = b1.header.inner_lite.epoch_id.clone();
    let valid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b2.header.try_to_vec().unwrap(),
            right_block_header: b1.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    let runtime_adapter = env.clients[1].chain.runtime_adapter.clone();
    assert_eq!(
        validate_challenge(&*runtime_adapter, &epoch_id, &genesis.hash(), &valid_challenge,)
            .unwrap()
            .0,
        if b1.hash() > b2.hash() { b1.hash() } else { b2.hash() }
    );
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b1.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    let runtime_adapter = env.clients[1].chain.runtime_adapter.clone();
    assert!(validate_challenge(&*runtime_adapter, &epoch_id, &genesis.hash(), &invalid_challenge,)
        .is_err());
    let b3 = env.clients[0].produce_block(3).unwrap().unwrap();
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header.try_to_vec().unwrap(),
            right_block_header: b3.header.try_to_vec().unwrap(),
        }),
        &signer,
    );
    let runtime_adapter = env.clients[1].chain.runtime_adapter.clone();
    assert!(validate_challenge(&*runtime_adapter, &epoch_id, &genesis.hash(), &invalid_challenge,)
        .is_err());

    let (_, result) = env.clients[0].process_block(b2, Provenance::NONE);
    assert!(result.is_ok());
    let mut last_message = env.network_adapters[0].pop().unwrap();
    if let NetworkRequests::Block { .. } = last_message {
        last_message = env.network_adapters[0].pop().unwrap();
    }
    if let NetworkRequests::Challenge(network_challenge) = last_message {
        assert_eq!(network_challenge, valid_challenge);
    } else {
        assert!(false);
    }
}

fn create_invalid_proofs_chunk(
    client: &mut Client,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    create_chunk(
        client,
        None,
        Some(CryptoHash::from_base("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf").unwrap()),
    )
}

fn create_chunk_with_transactions(
    client: &mut Client,
    transactions: Vec<SignedTransaction>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    create_chunk(client, Some(transactions), None)
}

fn create_chunk(
    client: &mut Client,
    replace_transactions: Option<Vec<SignedTransaction>>,
    replace_tx_root: Option<CryptoHash>,
) -> (EncodedShardChunk, Vec<MerklePath>, Vec<Receipt>, Block) {
    let last_block =
        client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap().clone();
    let (mut chunk, mut merkle_paths, receipts) = client
        .produce_chunk(
            last_block.hash(),
            &last_block.header.inner_lite.epoch_id,
            last_block.chunks[0].clone(),
            2,
            0,
        )
        .unwrap()
        .unwrap();
    if let Some(transactions) = replace_transactions {
        // The best way it to decode chunk, replace transactions and then recreate encoded chunk.
        let total_parts = client.chain.runtime_adapter.num_total_parts();
        let data_parts = client.chain.runtime_adapter.num_data_parts();
        let decoded_chunk = chunk.decode_chunk(data_parts).unwrap();
        let parity_parts = total_parts - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);

        let (tx_root, _) = merklize(&transactions);
        let signer = client.validator_signer.as_ref().unwrap().clone();
        let (mut encoded_chunk, mut new_merkle_paths) = EncodedShardChunk::new(
            chunk.header.inner.prev_block_hash,
            chunk.header.inner.prev_state_root,
            chunk.header.inner.outcome_root,
            chunk.header.inner.height_created,
            chunk.header.inner.shard_id,
            &mut rs,
            chunk.header.inner.gas_used,
            chunk.header.inner.gas_limit,
            chunk.header.inner.validator_reward,
            chunk.header.inner.balance_burnt,
            tx_root,
            chunk.header.inner.validator_proposals.clone(),
            transactions,
            &decoded_chunk.receipts,
            chunk.header.inner.outgoing_receipts_root,
            &*signer,
        )
        .unwrap();
        swap(&mut chunk, &mut encoded_chunk);
        swap(&mut merkle_paths, &mut new_merkle_paths);
    }
    if let Some(tx_root) = replace_tx_root {
        chunk.header.inner.tx_root = tx_root;
        chunk.header.height_included = 2;
        let (hash, signature) =
            client.validator_signer.as_ref().unwrap().sign_chunk_header_inner(&chunk.header.inner);
        chunk.header.hash = hash;
        chunk.header.signature = signature;
    }
    let block = Block::produce(
        &last_block.header,
        2,
        vec![chunk.header.clone()],
        last_block.header.inner_lite.epoch_id.clone(),
        last_block.header.inner_lite.next_epoch_id.clone(),
        vec![],
        Rational::from_integer(0),
        0,
        None,
        vec![],
        vec![],
        &*client.validator_signer.as_ref().unwrap().clone(),
        last_block.header.inner_lite.next_bp_hash,
    );
    (chunk, merkle_paths, receipts, block)
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let challenge_result = challenge(
        env,
        chunk.header.inner.shard_id as usize,
        MaybeEncodedShardChunk::Encoded(chunk),
        &block,
    );
    assert_eq!(challenge_result.unwrap(), (block.hash(), vec!["test0".to_string()]));
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge_decoded_chunk() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (encoded_chunk, _merkle_paths, _receipts, block) =
        create_invalid_proofs_chunk(&mut env.clients[0]);
    let chunk =
        encoded_chunk.decode_chunk(env.clients[0].chain.runtime_adapter.num_data_parts()).unwrap();

    let challenge_result = challenge(
        env,
        chunk.header.inner.shard_id as usize,
        MaybeEncodedShardChunk::Decoded(chunk),
        &block,
    );
    assert_eq!(challenge_result.unwrap(), (block.hash(), vec!["test0".to_string()]));
}

#[test]
fn test_verify_chunk_proofs_malicious_challenge_no_changes() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    // Valid chunk
    let (chunk, _merkle_paths, _receipts, block) = create_chunk(&mut env.clients[0], None, None);

    let challenge_result = challenge(
        env,
        chunk.header.inner.shard_id as usize,
        MaybeEncodedShardChunk::Encoded(chunk),
        &block,
    );
    assert_eq!(challenge_result.unwrap_err().kind(), ErrorKind::MaliciousChallenge);
}

#[test]
fn test_verify_chunk_proofs_malicious_challenge_valid_order_transactions() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);

    let genesis_hash = env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");

    let (chunk, _merkle_paths, _receipts, block) = create_chunk_with_transactions(
        &mut env.clients[0],
        vec![
            SignedTransaction::send_money(
                1,
                "test0".to_string(),
                "test1".to_string(),
                &signer,
                1000,
                genesis_hash,
            ),
            SignedTransaction::send_money(
                2,
                "test0".to_string(),
                "test1".to_string(),
                &signer,
                1000,
                genesis_hash,
            ),
        ],
    );

    let challenge_result = challenge(
        env,
        chunk.header.inner.shard_id as usize,
        MaybeEncodedShardChunk::Encoded(chunk),
        &block,
    );
    assert_eq!(challenge_result.unwrap_err().kind(), ErrorKind::MaliciousChallenge);
}

#[test]
fn test_verify_chunk_proofs_challenge_transaction_order() {
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);

    let genesis_hash = env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");

    let (chunk, _merkle_paths, _receipts, block) = create_chunk_with_transactions(
        &mut env.clients[0],
        vec![
            SignedTransaction::send_money(
                2,
                "test0".to_string(),
                "test1".to_string(),
                &signer,
                1000,
                genesis_hash,
            ),
            SignedTransaction::send_money(
                1,
                "test0".to_string(),
                "test1".to_string(),
                &signer,
                1000,
                genesis_hash,
            ),
        ],
    );
    let challenge_result = challenge(
        env,
        chunk.header.inner.shard_id as usize,
        MaybeEncodedShardChunk::Encoded(chunk),
        &block,
    );
    assert_eq!(challenge_result.unwrap(), (block.hash(), vec!["test0".to_string()]));
}

fn challenge(
    env: TestEnv,
    shard_id: usize,
    chunk: MaybeEncodedShardChunk,
    block: &Block,
) -> Result<(CryptoHash, Vec<String>), Error> {
    let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
    let valid_challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header.try_to_vec().unwrap(),
            chunk,
            merkle_proof: merkle_paths[shard_id].clone(),
        }),
        &*env.clients[0].validator_signer.as_ref().unwrap().clone(),
    );
    let runtime_adapter = env.clients[0].chain.runtime_adapter.clone();
    validate_challenge(
        &*runtime_adapter,
        &block.header.inner_lite.epoch_id,
        &block.header.prev_hash,
        &valid_challenge,
    )
}

#[test]
fn test_verify_chunk_invalid_state_challenge() {
    let store1 = create_test_store();
    let genesis = Genesis::test(vec!["test0", "test1"], 1);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        store1,
        Arc::new(genesis),
        vec![],
        vec![],
    ))];
    let mut env = TestEnv::new_with_runtime(ChainGenesis::test(), 1, 1, runtimes);
    let signer = InMemorySigner::from_seed("test0", KeyType::ED25519, "test0");
    let validator_signer = InMemoryValidatorSigner::from_seed("test0", KeyType::ED25519, "test0");
    let genesis_hash = env.clients[0].chain.genesis().hash();
    env.produce_block(0, 1);
    env.clients[0].process_tx(
        SignedTransaction::send_money(
            0,
            "test0".to_string(),
            "test1".to_string(),
            &signer,
            1000,
            genesis_hash,
        ),
        false,
        false,
    );
    env.produce_block(0, 2);

    // Invalid chunk & block.
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let last_block = env.clients[0].chain.get_block(&last_block_hash).unwrap().clone();
    let total_parts = env.clients[0].runtime_adapter.num_total_parts();
    let data_parts = env.clients[0].runtime_adapter.num_data_parts();
    let parity_parts = total_parts - data_parts;
    let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
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
            vec![],
            vec![],
            &vec![],
            last_block.chunks[0].inner.outgoing_receipts_root,
            CryptoHash::default(),
            &validator_signer,
            &mut rs,
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
        last_block.header.inner_lite.next_epoch_id.clone(),
        vec![],
        Rational::from_integer(0),
        0,
        None,
        vec![],
        vec![],
        &validator_signer,
        last_block.header.inner_lite.next_bp_hash,
    );

    let challenge_body = {
        use near_chain::chain::{ChainUpdate, OrphanBlockPool};
        let chain = &mut client.chain;
        let adapter = chain.runtime_adapter.clone();
        let epoch_length = chain.epoch_length;
        let empty_block_pool = OrphanBlockPool::new();
        let economics_config = BlockEconomicsConfig {
            gas_price_adjustment_rate: Rational::from_integer(0),
            min_gas_price: 0,
        };

        let mut chain_update = ChainUpdate::new(
            chain.mut_store(),
            adapter,
            &empty_block_pool,
            &empty_block_pool,
            epoch_length,
            &economics_config,
            DoomslugThresholdMode::NoApprovals,
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
                    1, 5, 0, 10, 178, 228, 151, 124, 13, 70, 6, 146, 31, 193, 111, 108, 60, 102,
                    227, 106, 220, 133, 45, 144, 104, 255, 30, 155, 129, 215, 15, 43, 202, 26, 122,
                    171, 30, 7, 228, 175, 99, 17, 113, 5, 94, 136, 200, 39, 136, 37, 110, 166, 241,
                    148, 128, 55, 131, 173, 97, 98, 201, 68, 82, 244, 223, 70, 86, 161, 5, 0, 0, 0,
                    0, 0, 0
                ],
                vec![
                    3, 1, 0, 0, 0, 16, 49, 233, 115, 11, 86, 10, 193, 50, 45, 253, 137, 126, 230,
                    236, 254, 86, 230, 148, 94, 141, 44, 46, 130, 154, 189, 73, 179, 223, 178, 17,
                    133, 232, 213, 5, 0, 0, 0, 0, 0, 0
                ]
            ],
        );
    }
    let challenge =
        Challenge::produce(ChallengeBody::ChunkState(challenge_body), &validator_signer);
    let runtime_adapter = client.chain.runtime_adapter.clone();
    assert_eq!(
        validate_challenge(
            &*runtime_adapter,
            &block.header.inner_lite.epoch_id,
            &block.header.prev_hash,
            &challenge,
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
/// TODO(2445): Enable challenges when they are working correctly.
#[test]
#[ignore]
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
    let receipts_hashes = env.clients[0].runtime_adapter.build_receipts_hashes(&receipts);
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
        assert_eq!(chunk_proofs.chunk, MaybeEncodedShardChunk::Encoded(chunk));
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
/// TODO(2445): Enable challenges when they are working correctly.
#[test]
#[ignore]
fn test_block_challenge() {
    init_test_logger();
    let mut env = TestEnv::new(ChainGenesis::test(), 1, 1);
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
    let challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header.try_to_vec().unwrap(),
            chunk: MaybeEncodedShardChunk::Encoded(chunk.clone()),
            merkle_proof: merkle_paths[chunk.header.inner.shard_id as usize].clone(),
        }),
        &*env.clients[0].validator_signer.as_ref().unwrap().clone(),
    );
    env.clients[0].process_challenge(challenge.clone()).unwrap();
    env.produce_block(0, 2);
    assert_eq!(env.clients[0].chain.get_block_by_height(2).unwrap().challenges, vec![challenge]);
    assert!(env.clients[0].chain.mut_store().is_block_challenged(&block.hash()).unwrap());
}

/// Make sure that fisherman can initiate challenges while an account that is neither a fisherman nor
/// a validator cannot.
// TODO(2445): Enable challenges when they are working correctly.
#[test]
#[ignore]
fn test_fishermen_challenge() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0", "test1", "test2"], 1);
    genesis.config.epoch_length = 5;
    let genesis = Arc::new(genesis);
    let create_runtime = || -> Arc<NightshadeRuntime> {
        Arc::new(neard::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            Arc::clone(&genesis),
            vec![],
            vec![],
        ))
    };
    let runtime1 = create_runtime();
    let runtime2 = create_runtime();
    let runtime3 = create_runtime();
    let mut env =
        TestEnv::new_with_runtime(ChainGenesis::test(), 3, 1, vec![runtime1, runtime2, runtime3]);
    let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
    let genesis_hash = env.clients[0].chain.genesis().hash();
    let stake_transaction = SignedTransaction::stake(
        1,
        "test1".to_string(),
        &signer,
        FISHERMEN_THRESHOLD,
        signer.public_key(),
        genesis_hash,
    );
    env.clients[0].process_tx(stake_transaction, false, false);
    for i in 1..=11 {
        env.produce_block(0, i);
    }

    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(&block.chunks).1;
    let challenge_body = ChallengeBody::ChunkProofs(ChunkProofs {
        block_header: block.header.try_to_vec().unwrap(),
        chunk: MaybeEncodedShardChunk::Encoded(chunk.clone()),
        merkle_proof: merkle_paths[chunk.header.inner.shard_id as usize].clone(),
    });
    let challenge = Challenge::produce(
        challenge_body.clone(),
        &*env.clients[1].validator_signer.as_ref().unwrap().clone(),
    );
    let challenge1 = Challenge::produce(
        challenge_body,
        &*env.clients[2].validator_signer.as_ref().unwrap().clone(),
    );
    assert!(env.clients[0].process_challenge(challenge1).is_err());
    env.clients[0].process_challenge(challenge.clone()).unwrap();
    env.produce_block(0, 12);
    assert_eq!(env.clients[0].chain.get_block_by_height(12).unwrap().challenges, vec![challenge]);
    assert!(env.clients[0].chain.mut_store().is_block_challenged(&block.hash()).unwrap());
}

/// If there are two blocks produced at the same height but by different block producers, no
/// challenge should be generated
#[test]
fn test_challenge_in_different_epoch() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0", "test1"], 2);
    genesis.config.epoch_length = 2;
    let genesis = Arc::new(genesis);
    //    genesis.config.validator_kickout_threshold = 10;
    let network_adapter = Arc::new(MockNetworkAdapter::default());
    let runtime1 = Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        Arc::clone(&genesis),
        vec![],
        vec![],
    ));
    let runtime2 = Arc::new(neard::NightshadeRuntime::new(
        Path::new("."),
        create_test_store(),
        genesis,
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
        let block1 = env.clients[0].produce_block(2 * i - 1).unwrap().unwrap();
        env.process_block(0, block1, Provenance::PRODUCED);

        let block2 = env.clients[1].produce_block(2 * i).unwrap().unwrap();
        env.process_block(1, block2.clone(), Provenance::PRODUCED);
        fork_blocks.push(block2);
    }

    let fork1_block = env.clients[0].produce_block(9).unwrap().unwrap();
    env.process_block(0, fork1_block, Provenance::PRODUCED);
    let fork2_block = env.clients[1].produce_block(9).unwrap().unwrap();
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
