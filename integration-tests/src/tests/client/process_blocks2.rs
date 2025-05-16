use assert_matches::assert_matches;
use near_chain::test_utils::is_optimistic_block_in_processing;
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{Provenance, test_utils};
use near_chain_configs::Genesis;
use near_crypto::vrf::Value;
use near_crypto::{KeyType, PublicKey, Signature};
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Block;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::network::PeerId;
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardChunkHeaderV3;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::ShardId;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::utils::MaybeValidated;
use near_store::ShardUId;

use crate::env::test_env::TestEnv;
use crate::env::test_env_builder::TestEnvBuilder;

/// Only process one block per height
/// Test that if a node receives two blocks at the same height, it doesn't process the second one
/// if the second block is not requested
#[test]
fn test_not_process_height_twice() {
    let mut env = TestEnv::default_builder().build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    // modify the block and resign it
    let mut duplicate_block = block.clone();
    env.process_block(0, block, Provenance::PRODUCED);
    let validator_signer = create_test_signer("test0");

    let proposals =
        vec![ValidatorStake::new("test1".parse().unwrap(), PublicKey::empty(KeyType::ED25519), 0)];
    duplicate_block.mut_header().set_prev_validator_proposals(proposals);
    duplicate_block.mut_header().resign(&validator_signer);
    let dup_block_hash = *duplicate_block.hash();
    let signer = env.clients[0].validator_signer.get();
    // we should have dropped the block before we even tried to process it, so the result should be ok
    env.clients[0]
        .receive_block_impl(
            duplicate_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            None,
            &signer,
        )
        .unwrap();
    // check that the second block is not being processed
    assert!(!test_utils::is_block_in_processing(&env.clients[0].chain, &dup_block_hash));
    // check that we didn't rebroadcast the second block
    while let Some(msg) = env.network_adapters[0].pop() {
        assert!(!matches!(
            msg,
            PeerManagerMessageRequest::NetworkRequests(NetworkRequests::Block { .. })
        ));
    }
}

/// Test that if a block contains chunks with invalid shard_ids, the client will return error.
#[test]
fn test_bad_shard_id() {
    let accounts = TestEnvBuilder::make_accounts(1);
    let genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();

    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let mut block = env.clients[0].produce_block(2).unwrap().unwrap(); // modify the block and resign it
    let validator_signer = create_test_signer("test0");
    let mut chunks: Vec<_> = block.chunks().iter_deprecated().cloned().collect();
    // modify chunk 0 to have shard_id 1
    let chunk = chunks.get(0).unwrap();
    let outgoing_receipts_root = chunks.get(1).unwrap().prev_outgoing_receipts_root();
    let congestion_info = CongestionInfo::default();
    let mut modified_chunk = ShardChunkHeaderV3::new(
        *chunk.prev_block_hash(),
        chunk.prev_state_root(),
        chunk.prev_outcome_root(),
        chunk.encoded_merkle_root(),
        chunk.encoded_length(),
        2,
        ShardId::new(1),
        chunk.prev_gas_used(),
        chunk.gas_limit(),
        chunk.prev_balance_burnt(),
        outgoing_receipts_root,
        chunk.tx_root(),
        chunk.prev_validator_proposals().collect(),
        congestion_info,
        chunk.bandwidth_requests().cloned().unwrap_or_else(BandwidthRequests::empty),
        &validator_signer,
    );
    modified_chunk.height_included = 2;
    chunks[0] = ShardChunkHeader::V3(modified_chunk);
    block.mut_header().set_chunk_headers_root(Block::compute_chunk_headers_root(&chunks).0);
    block.mut_header().set_prev_chunk_outgoing_receipts_root(
        Block::compute_chunk_prev_outgoing_receipts_root(&chunks),
    );
    block.set_chunks(chunks);
    let body_hash = block.compute_block_body_hash().unwrap();
    block.mut_header().set_block_body_hash(body_hash);
    block.mut_header().resign(&validator_signer);

    let err = env.clients[0]
        .process_block_test(MaybeValidated::from(block), Provenance::NONE)
        .unwrap_err();
    if let near_chain::Error::InvalidShardId(shard_id) = err {
        assert!(shard_id == ShardId::new(1));
    } else {
        panic!("Expected InvalidShardId error, got {:?}", err);
    }
}

/// Test that if a block's content (vrf_value) is corrupted, the invalid block will not affect the node's block processing
#[test]
fn test_bad_block_content_vrf() {
    let accounts = TestEnvBuilder::make_accounts(1);
    let genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();

    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let mut bad_block = block.clone();
    bad_block.set_vrf_value(Value([0u8; 32]));
    let signer = env.clients[0].validator_signer.get();

    let err = env.clients[0]
        .receive_block_impl(
            bad_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            None,
            &signer,
        )
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidSignature);

    let _ =
        env.clients[0].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
}

/// Test that if a block's signature is corrupted, the invalid block will not affect the node's block processing
#[test]
fn test_bad_block_signature() {
    let accounts = TestEnvBuilder::make_accounts(1);
    let genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();

    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let mut bad_block = block.clone();
    bad_block.mut_header().set_signature(Signature::default());
    let signer = env.clients[0].validator_signer.get();

    let err = env.clients[0]
        .receive_block_impl(
            bad_block,
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            None,
            &signer,
        )
        .unwrap_err();
    assert_matches!(err, near_chain::Error::InvalidSignature);

    let _ =
        env.clients[0].process_block_test(MaybeValidated::from(block), Provenance::NONE).unwrap();
}

enum BadCongestionInfoMode {
    CorruptReceiptBytes,
    CorruptDelayedReceiptsBytes,
    CorruptBufferedReceiptsBytes,
    CorruptAllowedShard,
    None,
}

impl BadCongestionInfoMode {
    fn corrupt(&self, congestion_info: &mut CongestionInfo) {
        match self {
            BadCongestionInfoMode::CorruptReceiptBytes => {
                congestion_info.add_receipt_bytes(1).unwrap();
            }
            BadCongestionInfoMode::CorruptDelayedReceiptsBytes => {
                congestion_info.add_delayed_receipt_gas(1).unwrap();
            }
            BadCongestionInfoMode::CorruptBufferedReceiptsBytes => {
                congestion_info.add_buffered_receipt_gas(1).unwrap();
            }
            BadCongestionInfoMode::CorruptAllowedShard => {
                congestion_info.set_allowed_shard(u16::MAX);
            }
            BadCongestionInfoMode::None => {}
        }
    }

    fn is_ok(&self) -> bool {
        match self {
            BadCongestionInfoMode::CorruptReceiptBytes
            | BadCongestionInfoMode::CorruptDelayedReceiptsBytes
            | BadCongestionInfoMode::CorruptBufferedReceiptsBytes
            | BadCongestionInfoMode::CorruptAllowedShard => false,
            BadCongestionInfoMode::None => true,
        }
    }
}

fn test_bad_congestion_info_impl(mode: BadCongestionInfoMode) {
    let accounts = TestEnvBuilder::make_accounts(1);
    let genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();

    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    let block = env.clients[0].produce_block(2).unwrap().unwrap();

    let validator_signer = create_test_signer("test0");

    let chunks: Vec<_> = block.chunks().iter_deprecated().cloned().collect();
    let chunk = chunks.get(0).unwrap();

    let mut congestion_info = chunk.congestion_info();
    mode.corrupt(&mut congestion_info);

    let mut modified_chunk_header = ShardChunkHeaderV3::new(
        *chunk.prev_block_hash(),
        chunk.prev_state_root(),
        chunk.prev_outcome_root(),
        chunk.encoded_merkle_root(),
        chunk.encoded_length(),
        chunk.height_created(),
        chunk.shard_id(),
        chunk.prev_gas_used(),
        chunk.gas_limit(),
        chunk.prev_balance_burnt(),
        chunk.prev_outgoing_receipts_root(),
        chunk.tx_root(),
        chunk.prev_validator_proposals().collect(),
        congestion_info,
        chunk.bandwidth_requests().cloned().unwrap_or_else(BandwidthRequests::empty),
        &validator_signer,
    );
    modified_chunk_header.height_included = 2;

    let modified_chunk = ShardChunkHeader::V3(modified_chunk_header);

    let shard_uid = ShardUId { shard_id: chunk.shard_id().into(), version: 1 };
    let prev_block_hash = block.header().prev_hash();
    let client = &env.clients[0];
    let prev_chunk_extra = client.chain.get_chunk_extra(prev_block_hash, &shard_uid).unwrap();
    let result: Result<(), near_chain::Error> = validate_chunk_with_chunk_extra(
        &client.chain.chain_store,
        client.epoch_manager.as_ref(),
        prev_block_hash,
        &prev_chunk_extra,
        1,
        &modified_chunk,
    );

    let expected_is_ok = mode.is_ok();
    if expected_is_ok {
        result.unwrap();
    } else {
        assert!(result.is_err());
    }
}

#[test]
fn test_bad_congestion_info_receipt_bytes() {
    test_bad_congestion_info_impl(BadCongestionInfoMode::CorruptReceiptBytes);
}

#[test]
fn test_bad_congestion_info_corrupt_delayed_receipts_bytes() {
    test_bad_congestion_info_impl(BadCongestionInfoMode::CorruptDelayedReceiptsBytes);
}

#[test]
fn test_bad_congestion_info_corrupt_buffered_receipts_bytes() {
    test_bad_congestion_info_impl(BadCongestionInfoMode::CorruptBufferedReceiptsBytes);
}

#[test]
fn test_bad_congestion_info_corrupt_allowed_shard() {
    test_bad_congestion_info_impl(BadCongestionInfoMode::CorruptAllowedShard);
}

#[test]
fn test_bad_congestion_info_none() {
    test_bad_congestion_info_impl(BadCongestionInfoMode::None);
}

// Helper function to check that a block was produced from an optimistic block
fn check_block_produced_from_optimistic_block(block: &Block, optimistic_block: &OptimisticBlock) {
    assert_eq!(block.header().height(), optimistic_block.inner.block_height, "height");
    assert_eq!(
        block.header().prev_hash(),
        &optimistic_block.inner.prev_block_hash,
        "previous hash"
    );
    assert_eq!(block.header().raw_timestamp(), optimistic_block.inner.block_timestamp, "timestamp");
    assert_eq!(block.header().random_value(), &optimistic_block.inner.random_value, "random value");
}

// Testing the production and application of optimistic blocks
#[test]
fn test_process_optimistic_block() {
    let accounts = TestEnvBuilder::make_accounts(1);
    let genesis = Genesis::test_sharded_new_version(accounts, 1, vec![1, 1, 1, 1]);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();

    let prev_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, prev_block, Provenance::PRODUCED);
    assert!(!env.clients[0].is_optimistic_block_done(2), "Optimistic block should not be ready");

    // Produce and save optimistic block to be used at block production.
    let optimistic_block = env.clients[0].produce_optimistic_block_on_head(2).unwrap().unwrap();
    env.clients[0].save_optimistic_block(&optimistic_block);
    assert!(env.clients[0].is_optimistic_block_done(2), "Optimistic block should be ready");

    // Check that block data matches optimistic block data.
    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    check_block_produced_from_optimistic_block(&block, &optimistic_block);

    // Start processing block and then optimistic block.
    // Check that optimistic block is not in processing.
    let signer = env.clients[0].validator_signer.get();
    let me = signer.as_ref().map(|signer| signer.validator_id().clone());
    env.clients[0].start_process_block(block.into(), Provenance::NONE, None, &signer).unwrap();
    let optimistic_block_height = optimistic_block.inner.block_height;
    env.clients[0].chain.preprocess_optimistic_block(optimistic_block, &me, None);
    assert!(
        !is_optimistic_block_in_processing(&env.clients[0].chain, optimistic_block_height),
        "Optimistic block should not be in processing because block processing already started"
    );
    // TODO(#10584): Process chunks with optimistic block
}
