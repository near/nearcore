use crate::test_utils::TestEnv;
use near_chain::{test_utils, ChainGenesis, Provenance};
use near_crypto::{KeyType, PublicKey};
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::validator_stake::ValidatorStake;
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
