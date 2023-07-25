use crate::test_utils::TestEnv;
use assert_matches::assert_matches;
use near_chain::{test_utils, ChainGenesis, Provenance};
#[cfg(feature = "protocol_feature_block_header_v4")]
use near_crypto::vrf::Value;
use near_crypto::{KeyType, PublicKey, Signature};
use near_primitives::block::Block;
use near_primitives::network::PeerId;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardChunkHeaderV3;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::utils::MaybeValidated;
use std::sync::Arc;

/// Only process one block per height
/// Test that if a node receives two blocks at the same height, it doesn't process the second one
/// if the second block is not requested
#[test]
fn test_not_process_height_twice() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    // modify the block and resign it
    let mut duplicate_block = block.clone();
    env.process_block(0, block, Provenance::PRODUCED);
    let validator_signer = create_test_signer("test0");

    let proposals =
        vec![ValidatorStake::new("test1".parse().unwrap(), PublicKey::empty(KeyType::ED25519), 0)];
    duplicate_block.mut_header().get_mut().inner_rest.validator_proposals = proposals;
    duplicate_block.mut_header().resign(&validator_signer);
    let dup_block_hash = *duplicate_block.hash();
    // we should have dropped the block before we even tried to process it, so the result should be ok
    env.clients[0]
        .receive_block_impl(
            duplicate_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            Arc::new(|_| {}),
        )
        .unwrap();
    // check that the second block is not being processed
    assert!(!test_utils::is_block_in_processing(&env.clients[0].chain, &dup_block_hash));
    // check that we didn't rebroadcast the second block
    assert!(env.network_adapters[0].pop().is_none());
}

/// Test that if a block contains chunks with invalid shard_ids, the client will return error.
#[test]
fn test_bad_shard_id() {
    let mut env = TestEnv::builder(ChainGenesis::test()).num_shards(4).build();
    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let mut block = env.clients[0].produce_block(2).unwrap().unwrap(); // modify the block and resign it
    let validator_signer = create_test_signer("test0");
    let mut chunks: Vec<_> = block.chunks().iter().cloned().collect();
    // modify chunk 0 to have shard_id 1
    let chunk = chunks.get(0).unwrap();
    let outgoing_receipts_root = chunks.get(1).unwrap().outgoing_receipts_root();
    let mut modified_chunk = ShardChunkHeaderV3::new(
        *chunk.prev_block_hash(),
        chunk.prev_state_root(),
        chunk.outcome_root(),
        chunk.encoded_merkle_root(),
        chunk.encoded_length(),
        2,
        1,
        chunk.gas_used(),
        chunk.gas_limit(),
        chunk.balance_burnt(),
        outgoing_receipts_root,
        chunk.tx_root(),
        chunk.validator_proposals().collect(),
        &validator_signer,
    );
    modified_chunk.height_included = 2;
    chunks[0] = ShardChunkHeader::V3(modified_chunk);
    block.mut_header().get_mut().inner_rest.chunk_headers_root =
        Block::compute_chunk_headers_root(&chunks).0;
    block.mut_header().get_mut().inner_rest.chunk_receipts_root =
        Block::compute_chunk_receipts_root(&chunks);
    block.set_chunks(chunks);
    #[cfg(feature = "protocol_feature_block_header_v4")]
    {
        block.mut_header().get_mut().inner_rest.block_body_hash =
            block.compute_block_body_hash().unwrap();
    }
    block.mut_header().resign(&validator_signer);

    let err = env.clients[0]
        .process_block_test(MaybeValidated::from(block), Provenance::NONE)
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidShardId(1));
}

/// Test that if a block's content (vrf_value) is corrupted, the invalid block will not affect the node's block processing
#[test]
#[cfg(feature = "protocol_feature_block_header_v4")]
fn test_bad_block_content_vrf() {
    let mut env = TestEnv::builder(ChainGenesis::test()).num_shards(4).build();
    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let mut bad_block = block.clone();
    bad_block.get_mut().body.vrf_value = Value([0u8; 32]);

    let err = env.clients[0]
        .receive_block_impl(
            bad_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            Arc::new(|_| {}),
        )
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidSignature);

    let _ =
        env.clients[0].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
}

/// Test that if a block's signature is corrupted, the invalid block will not affect the node's block processing
#[test]
fn test_bad_block_signature() {
    let mut env = TestEnv::builder(ChainGenesis::test()).num_shards(4).build();
    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let mut bad_block = block.clone();
    bad_block.mut_header().get_mut().signature = Signature::default();

    let err = env.clients[0]
        .receive_block_impl(
            bad_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            Arc::new(|_| {}),
        )
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidSignature);

    let _ =
        env.clients[0].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
}
