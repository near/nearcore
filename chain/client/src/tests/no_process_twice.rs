/// This file contains tests to test that client doesn't process the same block twice
use crate::test_utils::TestEnv;
use assert_matches::assert_matches;
use near_chain::{ChainGenesis, Provenance};
use near_chain_primitives::{BlockKnownError, Error};
use near_crypto::{KeyType, PublicKey};
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use std::sync::Arc;

#[test]
fn test_not_process_orphan_twice() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    let mut orphan_block = block;
    let validator_signer = create_test_signer("test0");
    orphan_block.mut_header().get_mut().prev_hash = hash(&[1]);
    orphan_block.mut_header().resign(&validator_signer);
    let res = env.clients[0].process_block_test(orphan_block.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::Orphan);
    let res = env.clients[0].process_block_test(orphan_block.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::BlockKnown(BlockKnownError::KnownInOrphan));
}

// Test that we don't re-process invalid block again
#[test]
fn test_not_process_invalid_block_twice() {
    let mut env = TestEnv::builder(ChainGenesis::test()).build();
    let mut block = env.clients[0].produce_block(1).unwrap().unwrap();
    // modify the block and resign it
    let validator_signer = create_test_signer("test0");
    // make the block invalid
    block.mut_header().get_mut().inner_rest.chunk_mask = vec![];
    block.mut_header().resign(&validator_signer);

    // first time process it, it should return error and we should try to ban the peer
    let err = env.clients[0]
        .receive_block_impl(
            block.clone(),
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            Arc::new(|_| {}),
        )
        .unwrap_err();
    assert_matches!(err, Error::InvalidChunkMask);
    let msg = env.network_adapters[0].pop().unwrap();
    assert_matches!(
        msg,
        PeerManagerMessageRequest::NetworkRequests(NetworkRequests::BanPeer { .. })
    );
    assert!(env.network_adapters[0].pop().is_none());

    // Second time process it, the block is already known
    // so we return that the block is known and we won't ban it again, since
    // we won't re-process it
    let err = env.clients[0]
        .receive_block_impl(
            block.clone(),
            PeerId::new(PublicKey::empty(KeyType::ED25519)),
            false,
            Arc::new(|_| {}),
        )
        .unwrap_err();
    assert_matches!(err, Error::BlockKnown(BlockKnownError::KnownAsInvalid));
    assert!(env.network_adapters[0].pop().is_none());
}
