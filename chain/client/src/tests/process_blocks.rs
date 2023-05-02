use crate::test_utils::TestEnv;
use assert_matches::assert_matches;
use near_chain::{test_utils, ChainGenesis, Provenance};
use near_crypto::{KeyType, PublicKey};
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
    let chunk = chunks.get(0).unwrap();
    let outgoing_receipts_root = chunks.get(1).unwrap().outgoing_receipts_root();
    let modified_chunk = ShardChunkHeaderV3::new(
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
    chunks[0] = ShardChunkHeader::V3(modified_chunk);
    block.set_chunks(chunks);

    let err = env.clients[0]
        .process_block_test(MaybeValidated::from(block), Provenance::NONE)
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidShardId(1));
}
