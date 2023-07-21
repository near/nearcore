use assert_matches::assert_matches;
use borsh::BorshSerialize;
use near_async::messaging::CanSend;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_primitives::test_utils::create_test_signer;

use near_chain::validate::validate_challenge;
use near_chain::{Block, Chain, ChainGenesis, ChainStoreAccess, Error, Provenance};
use near_chain_configs::Genesis;
use near_chunks::ShardsManager;
use near_client::test_utils::{create_chunk, create_chunk_with_transactions, TestEnv};
use near_client::{Client, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::challenge::{
    BlockDoubleSign, Challenge, ChallengeBody, ChunkProofs, MaybeEncodedShardChunk, PartialState,
    TrieValue,
};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::num_rational::Ratio;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{EncodedShardChunk, ReedSolomonWrapper};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, EpochId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Trie;
use nearcore::config::{GenesisExt, FISHERMEN_THRESHOLD};
use std::sync::Arc;

use crate::tests::client::utils::TestEnvNightshadeSetupExt;

/// Check that block containing a challenge is rejected.
/// TODO (#2445): Enable challenges when they are working correctly.
#[test]
fn test_block_with_challenges() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap();

    let mut block = env.clients[0].produce_block(1).unwrap().unwrap();
    let signer = env.clients[0].validator_signer.as_ref().unwrap().clone();

    {
        let challenge_body = ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: genesis.header().try_to_vec().unwrap(),
            right_block_header: genesis.header().try_to_vec().unwrap(),
        });
        let challenge = Challenge::produce(challenge_body, &*signer);
        let challenges = vec![challenge];
        match &mut block {
            Block::BlockV1(_) => unreachable!(),
            Block::BlockV2(body) => {
                let body = Arc::make_mut(body);
                body.challenges = challenges.clone();
            }
            Block::BlockV3(body) => {
                let body = Arc::make_mut(body);
                body.body.challenges = challenges.clone();
            }
        };
        #[cfg(feature = "protocol_feature_block_header_v4")]
        {
            let block_body_hash = block.compute_block_body_hash().unwrap();
            block.mut_header().get_mut().inner_rest.block_body_hash = block_body_hash;
        }
        block.mut_header().get_mut().inner_rest.challenges_root =
            Block::compute_challenges_root(&challenges);
        block.mut_header().resign(&*signer);
    }

    let result = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    assert_matches!(result.unwrap_err(), Error::InvalidChallengeRoot);
}

/// Check that attempt to process block on top of incorrect state root leads to InvalidChunkState error.
#[test]
fn test_invalid_chunk_state() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let mut env = TestEnv::builder(ChainGenesis::test())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();
    env.produce_block(0, 1);
    let block_hash = env.clients[0].chain.get_block_hash_by_height(1).unwrap();

    {
        let mut chunk_extra = ChunkExtra::clone(
            &env.clients[0].chain.get_chunk_extra(&block_hash, &ShardUId::single_shard()).unwrap(),
        );
        let store = env.clients[0].chain.mut_store();
        let mut store_update = store.store_update();
        assert_ne!(chunk_extra.state_root(), &Trie::EMPTY_ROOT);
        *chunk_extra.state_root_mut() = Trie::EMPTY_ROOT;
        store_update.save_chunk_extra(&block_hash, &ShardUId::single_shard(), chunk_extra);
        store_update.commit().unwrap();
    }

    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let result = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    assert_matches!(result.unwrap_err(), Error::InvalidChunkState(_));
}

#[test]
fn test_verify_block_double_sign_challenge() {
    let mut env = TestEnv::builder(ChainGenesis::test()).clients_count(2).build();
    env.produce_block(0, 1);
    let genesis = env.clients[0].chain.get_block_by_height(0).unwrap();
    let b1 = env.clients[0].produce_block(2).unwrap().unwrap();

    env.process_block(0, b1.clone(), Provenance::NONE);

    let signer = create_test_signer("test0");
    let mut block_merkle_tree = PartialMerkleTree::default();
    block_merkle_tree.insert(*genesis.hash());
    let b2 = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        genesis.header(),
        2,
        genesis.header().block_ordinal() + 1,
        genesis.chunks().iter().cloned().collect(),
        b1.header().epoch_id().clone(),
        b1.header().next_epoch_id().clone(),
        None,
        vec![],
        Ratio::from_integer(0),
        0,
        100,
        None,
        vec![],
        vec![],
        &signer,
        *b1.header().next_bp_hash(),
        block_merkle_tree.root(),
        None,
    );
    let epoch_id = b1.header().epoch_id().clone();
    let valid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b2.header().try_to_vec().unwrap(),
            right_block_header: b1.header().try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert_eq!(
        &validate_challenge(
            env.clients[1].chain.epoch_manager.as_ref(),
            env.clients[1].chain.runtime_adapter.as_ref(),
            &epoch_id,
            genesis.hash(),
            &valid_challenge
        )
        .unwrap()
        .0,
        if b1.hash() > b2.hash() { b1.hash() } else { b2.hash() }
    );
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header().try_to_vec().unwrap(),
            right_block_header: b1.header().try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert!(validate_challenge(
        env.clients[1].chain.epoch_manager.as_ref(),
        env.clients[1].chain.runtime_adapter.as_ref(),
        &epoch_id,
        genesis.hash(),
        &invalid_challenge,
    )
    .is_err());
    let b3 = env.clients[0].produce_block(3).unwrap().unwrap();
    let invalid_challenge = Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: b1.header().try_to_vec().unwrap(),
            right_block_header: b3.header().try_to_vec().unwrap(),
        }),
        &signer,
    );
    assert!(validate_challenge(
        env.clients[1].chain.epoch_manager.as_ref(),
        env.clients[1].chain.runtime_adapter.as_ref(),
        &epoch_id,
        genesis.hash(),
        &invalid_challenge,
    )
    .is_err());

    let result = env.clients[0].process_block_test(b2.into(), Provenance::SYNC);
    assert!(result.is_ok());
    let mut last_message = env.network_adapters[0].pop().unwrap().as_network_requests();
    if let NetworkRequests::Block { .. } = last_message {
        last_message = env.network_adapters[0].pop().unwrap().as_network_requests();
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
        Some("F5SvmQcKqekuKPJgLUNFgjB4ZgVmmiHsbDhTBSQbiywf".parse::<CryptoHash>().unwrap()),
    )
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let shard_id = chunk.shard_id();
    let challenge_result =
        challenge(env, shard_id as usize, MaybeEncodedShardChunk::Encoded(chunk), &block);
    assert_eq!(challenge_result.unwrap(), (*block.hash(), vec!["test0".parse().unwrap()]));
}

#[test]
fn test_verify_chunk_invalid_proofs_challenge_decoded_chunk() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);
    let (encoded_chunk, _merkle_paths, _receipts, block) =
        create_invalid_proofs_chunk(&mut env.clients[0]);
    let chunk =
        encoded_chunk.decode_chunk(env.clients[0].chain.epoch_manager.num_data_parts()).unwrap();

    let shard_id = chunk.shard_id();
    let challenge_result =
        challenge(env, shard_id as usize, MaybeEncodedShardChunk::Decoded(chunk), &block);
    assert_eq!(challenge_result.unwrap(), (*block.hash(), vec!["test0".parse().unwrap()]));
}

#[test]
fn test_verify_chunk_proofs_malicious_challenge_no_changes() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);
    // Valid chunk
    let (chunk, _merkle_paths, _receipts, block) = create_chunk(&mut env.clients[0], None, None);

    let shard_id = chunk.shard_id();
    let challenge_result =
        challenge(env, shard_id as usize, MaybeEncodedShardChunk::Encoded(chunk), &block);
    assert_matches!(challenge_result.unwrap_err(), Error::MaliciousChallenge);
}

#[test]
fn test_verify_chunk_proofs_malicious_challenge_valid_order_transactions() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);

    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");

    let (chunk, _merkle_paths, _receipts, block) = create_chunk_with_transactions(
        &mut env.clients[0],
        vec![
            SignedTransaction::send_money(
                1,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
            SignedTransaction::send_money(
                2,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
        ],
    );

    let shard_id = chunk.shard_id();
    let challenge_result =
        challenge(env, shard_id as usize, MaybeEncodedShardChunk::Encoded(chunk), &block);
    assert_matches!(challenge_result.unwrap_err(), Error::MaliciousChallenge);
}

#[test]
fn test_verify_chunk_proofs_challenge_transaction_order() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);

    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");

    let (chunk, _merkle_paths, _receipts, block) = create_chunk_with_transactions(
        &mut env.clients[0],
        vec![
            SignedTransaction::send_money(
                2,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
            SignedTransaction::send_money(
                1,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
        ],
    );

    let shard_id = chunk.shard_id();
    let challenge_result =
        challenge(env, shard_id as usize, MaybeEncodedShardChunk::Encoded(chunk), &block);
    assert_eq!(challenge_result.unwrap(), (*block.hash(), vec!["test0".parse().unwrap()]));
}

fn challenge(
    env: TestEnv,
    shard_id: usize,
    chunk: MaybeEncodedShardChunk,
    block: &Block,
) -> Result<(CryptoHash, Vec<AccountId>), Error> {
    let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
    let valid_challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header().try_to_vec().unwrap(),
            chunk,
            merkle_proof: merkle_paths[shard_id].clone(),
        }),
        &*env.clients[0].validator_signer.as_ref().unwrap().clone(),
    );
    validate_challenge(
        env.clients[0].chain.epoch_manager.as_ref(),
        env.clients[0].chain.runtime_adapter.as_ref(),
        block.header().epoch_id(),
        block.header().prev_hash(),
        &valid_challenge,
    )
}

#[test]
fn test_verify_chunk_invalid_state_challenge() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(ChainGenesis::test())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let validator_signer = create_test_signer("test0");
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    env.produce_block(0, 1);
    assert_eq!(
        env.clients[0].process_tx(
            SignedTransaction::send_money(
                1,
                "test0".parse().unwrap(),
                "test1".parse().unwrap(),
                &signer,
                1000,
                genesis_hash,
            ),
            false,
            false,
        ),
        ProcessTxResponse::ValidTx
    );
    env.produce_block(0, 2);

    // Invalid chunk & block.
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let last_block = env.clients[0].chain.get_block(&last_block_hash).unwrap();
    let total_parts = env.clients[0].epoch_manager.num_total_parts();
    let data_parts = env.clients[0].epoch_manager.num_data_parts();
    let parity_parts = total_parts - data_parts;
    let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
    let (mut invalid_chunk, merkle_paths) = ShardsManager::create_encoded_shard_chunk(
        *last_block.hash(),
        Trie::EMPTY_ROOT,
        CryptoHash::default(),
        last_block.header().height() + 1,
        0,
        0,
        1_000,
        0,
        vec![],
        vec![],
        &[],
        last_block.chunks()[0].outgoing_receipts_root(),
        CryptoHash::default(),
        &validator_signer,
        &mut rs,
        PROTOCOL_VERSION,
    )
    .unwrap();

    let client = &mut env.clients[0];

    // Receive invalid chunk to the validator.
    client
        .persist_and_distribute_encoded_chunk(
            invalid_chunk.clone(),
            merkle_paths,
            vec![],
            validator_signer.validator_id().clone(),
        )
        .unwrap();

    match &mut invalid_chunk {
        EncodedShardChunk::V1(ref mut chunk) => {
            chunk.header.height_included = last_block.header().height() + 1;
        }
        EncodedShardChunk::V2(ref mut chunk) => {
            *chunk.header.height_included_mut() = last_block.header().height() + 1;
        }
    }
    let block_merkle_tree =
        client.chain.mut_store().get_block_merkle_tree(last_block.hash()).unwrap();
    let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
    block_merkle_tree.insert(*last_block.hash());
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        last_block.header(),
        last_block.header().height() + 1,
        last_block.header().block_ordinal() + 1,
        vec![invalid_chunk.cloned_header()],
        last_block.header().epoch_id().clone(),
        last_block.header().next_epoch_id().clone(),
        None,
        vec![],
        Ratio::from_integer(0),
        0,
        100,
        None,
        vec![],
        vec![],
        &validator_signer,
        *last_block.header().next_bp_hash(),
        block_merkle_tree.root(),
        None,
    );

    let challenge_body =
        client.chain.create_chunk_state_challenge(&last_block, &block, &block.chunks()[0]).unwrap();
    {
        let prev_merkle_proofs = Block::compute_chunk_headers_root(last_block.chunks().iter()).1;
        let merkle_proofs = Block::compute_chunk_headers_root(block.chunks().iter()).1;
        assert_eq!(prev_merkle_proofs[0], challenge_body.prev_merkle_proof);
        assert_eq!(merkle_proofs[0], challenge_body.merkle_proof);
        // TODO (#6316): enable storage proof generation
        assert_eq!(challenge_body.partial_state, PartialState::TrieValues(Vec::<TrieValue>::new()));
        // assert_eq!(
        //     challenge_body.partial_state.0,
        //     vec![
        //         vec![
        //             1, 5, 0, 10, 178, 228, 151, 124, 13, 70, 6, 146, 31, 193, 111, 108, 60, 102,
        //             227, 106, 220, 133, 45, 144, 104, 255, 30, 155, 129, 215, 15, 43, 202, 26, 122,
        //             171, 30, 7, 228, 175, 99, 17, 113, 5, 94, 136, 200, 39, 136, 37, 110, 166, 241,
        //             148, 128, 55, 131, 173, 97, 98, 201, 68, 82, 244, 223, 70, 86, 161, 5, 0, 0, 0,
        //             0, 0, 0
        //         ],
        //         vec![
        //             3, 1, 0, 0, 0, 16, 49, 233, 115, 11, 86, 10, 193, 50, 45, 253, 137, 126, 230,
        //             236, 254, 86, 230, 148, 94, 141, 44, 46, 130, 154, 189, 73, 179, 223, 178, 17,
        //             133, 232, 213, 5, 0, 0, 0, 0, 0, 0
        //         ]
        //     ],
        // );
    }
    let challenge =
        Challenge::produce(ChallengeBody::ChunkState(challenge_body), &validator_signer);
    // Invalidate chunk state challenges because they are not supported yet.
    // TODO (#2445): Enable challenges when they are working correctly.
    assert_matches!(
        validate_challenge(
            client.chain.epoch_manager.as_ref(),
            client.chain.runtime_adapter.as_ref(),
            block.header().epoch_id(),
            block.header().prev_hash(),
            &challenge,
        )
        .unwrap_err(),
        Error::MaliciousChallenge
    );
    // assert_eq!(
    //     validate_challenge(
    //         &*runtime_adapter,
    //         block.header().epoch_id(),
    //         block.header().prev_hash(),
    //         &challenge,
    //     )
    //     .unwrap(),
    //     (*block.hash(), vec!["test0".parse().unwrap()])
    // );

    // Process the block with invalid chunk and make sure it's marked as invalid at the end.
    // And the same challenge created and sent out.
    let result = client.process_block_test(block.into(), Provenance::NONE);
    assert!(result.is_err());

    let last_message = env.network_adapters[0].pop().unwrap().as_network_requests();

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
    let mut env = TestEnv::builder(ChainGenesis::test()).clients_count(2).build();
    env.produce_block(0, 1);
    let block1 = env.clients[0].chain.get_block_by_height(1).unwrap();
    env.process_block(1, block1, Provenance::NONE);
    let (chunk, merkle_paths, receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);
    let client = &mut env.clients[0];
    assert!(client
        .persist_and_distribute_encoded_chunk(
            chunk.clone(),
            merkle_paths.clone(),
            receipts.clone(),
            client.validator_signer.as_ref().unwrap().validator_id().clone()
        )
        .is_err());
    let result = client.process_block_test(block.clone().into(), Provenance::NONE);
    // We have declined block with invalid chunk.
    assert!(result.is_err());
    assert_eq!(client.chain.head().unwrap().height, 1);
    // But everyone who doesn't track this shard have accepted.
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
    let receipts_hashes = Chain::build_receipts_hashes(&receipts, &shard_layout);
    let (_receipts_root, receipts_proofs) = merklize(&receipts_hashes);
    let receipts_by_shard = Chain::group_receipts_by_shard(receipts, &shard_layout);
    let one_part_receipt_proofs = ShardsManager::receipts_recipient_filter(
        0,
        Vec::default(),
        &receipts_by_shard,
        &receipts_proofs,
    );

    let partial_encoded_chunk = chunk.create_partial_encoded_chunk(
        vec![0],
        one_part_receipt_proofs,
        &[merkle_paths[0].clone()],
    );
    env.shards_manager_adapters[1]
        .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(partial_encoded_chunk));
    env.process_block(1, block, Provenance::NONE);

    // At this point we should create a challenge and send it out.
    let last_message = env.network_adapters[0].pop().unwrap().as_network_requests();
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
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    env.produce_block(0, 1);
    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
    let shard_id = chunk.cloned_header().shard_id();
    let challenge = Challenge::produce(
        ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: block.header().try_to_vec().unwrap(),
            chunk: MaybeEncodedShardChunk::Encoded(chunk),
            merkle_proof: merkle_paths[shard_id as usize].clone(),
        }),
        &*env.clients[0].validator_signer.as_ref().unwrap().clone(),
    );
    env.clients[0].process_challenge(challenge.clone()).unwrap();
    env.produce_block(0, 2);
    assert_eq!(env.clients[0].chain.get_block_by_height(2).unwrap().challenges(), &[challenge]);
    assert!(env.clients[0].chain.mut_store().is_block_challenged(block.hash()).unwrap());
}

/// Make sure that fisherman can initiate challenges while an account that is neither a fisherman nor
/// a validator cannot.
// TODO(2445): Enable challenges when they are working correctly.
#[test]
#[ignore]
fn test_fishermen_challenge() {
    init_test_logger();
    let mut genesis = Genesis::test(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()],
        1,
    );
    genesis.config.epoch_length = 5;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .clients_count(3)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let stake_transaction = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        FISHERMEN_THRESHOLD,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(
        env.clients[0].process_tx(stake_transaction, false, false),
        ProcessTxResponse::ValidTx
    );
    for i in 1..=11 {
        env.produce_block(0, i);
    }

    let (chunk, _merkle_paths, _receipts, block) = create_invalid_proofs_chunk(&mut env.clients[0]);

    let merkle_paths = Block::compute_chunk_headers_root(block.chunks().iter()).1;
    let shard_id = chunk.cloned_header().shard_id();
    let challenge_body = ChallengeBody::ChunkProofs(ChunkProofs {
        block_header: block.header().try_to_vec().unwrap(),
        chunk: MaybeEncodedShardChunk::Encoded(chunk),
        merkle_proof: merkle_paths[shard_id as usize].clone(),
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
    assert_eq!(env.clients[0].chain.get_block_by_height(12).unwrap().challenges(), &[challenge]);
    assert!(env.clients[0].chain.mut_store().is_block_challenged(block.hash()).unwrap());
}

/// If there are two blocks produced at the same height but by different block producers, no
/// challenge should be generated
#[test]
// Something weird happens here. For len_in_blocks = 13 this test passes for versions up to 49,
// but fails for version 50 (because of chunk validator sampling changes). But if we set it to 20,
// it fails for version 49 as well
#[ignore]
fn test_challenge_in_different_epoch() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    genesis.config.epoch_length = 3;
    //    genesis.config.validator_kickout_threshold = 10;
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = 3;

    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(2)
        .validator_seats(2)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .network_adapters(vec![network_adapter.clone(), network_adapter.clone()])
        .build();

    let mut fork_blocks = vec![];
    let len_in_blocks = 13;
    for h in 1..len_in_blocks {
        if let Some(block) = env.clients[0].produce_block(h).unwrap() {
            env.process_block(0, block, Provenance::PRODUCED);
        }
        if let Some(block) = env.clients[1].produce_block(h).unwrap() {
            env.process_block(1, block.clone(), Provenance::PRODUCED);
            fork_blocks.push(block);
        }
    }

    let fork1_block = env.clients[0].produce_block(len_in_blocks).unwrap().unwrap();
    env.process_block(0, fork1_block, Provenance::PRODUCED);
    let fork2_block = env.clients[1].produce_block(len_in_blocks).unwrap().unwrap();
    fork_blocks.push(fork2_block);
    for block in fork_blocks {
        let height = block.header().height();
        let result = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        let len = network_adapter.requests.write().unwrap().len();
        for _ in 0..len {
            match network_adapter
                .requests
                .write()
                .unwrap()
                .pop_front()
                .map(|f| f.as_network_requests())
            {
                Some(NetworkRequests::Challenge(_)) => {
                    panic!("Unexpected challenge");
                }
                Some(_) => {}
                None => break,
            }
        }
        if height < len_in_blocks {
            assert!(result.is_ok());
        } else {
            if let Err(e) = result {
                match e {
                    Error::ChunksMissing(_) => {}
                    _ => panic!("unexpected error: {}", e),
                }
            }
        }
    }
}
