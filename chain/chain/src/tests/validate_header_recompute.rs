use crate::test_utils::setup;
use crate::{ChainStoreAccess, Error};
use near_async::time::Clock;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::TestBlockBuilder;
use std::sync::Arc;

#[test]
fn validate_header_recomputes_block_ordinal_and_epoch_sync_data_hash() {
    init_test_logger();
    let clock = Clock::real();
    let (chain, epoch_manager, _, signer) = setup(clock.clone());

    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let epoch_sync_data_hash = epoch_manager.compute_epoch_sync_data_hash(genesis.hash()).unwrap();
    assert!(epoch_sync_data_hash.is_some(), "first block after genesis is epoch-start");

    let stored_tree = chain.chain_store().get_block_merkle_tree(genesis.hash()).unwrap();
    let mut tree = PartialMerkleTree::clone(&stored_tree);
    let block = TestBlockBuilder::from_prev_block(clock, &genesis, signer.clone())
        .block_merkle_tree(&mut tree)
        .epoch_sync_data_hash(epoch_sync_data_hash)
        .build();

    // Honest epoch-start header is accepted.
    chain.process_block_header(block.header()).unwrap();

    // Forged block_ordinal is rejected.
    let mut forged = block.clone();
    {
        let header = Arc::make_mut(&mut forged).mut_header();
        header.set_block_ordinal(header.block_ordinal() + 1000);
        header.resign(&signer);
    }
    let result = chain.process_block_header(forged.header());
    assert!(matches!(result, Err(Error::InvalidBlockOrdinal)), "got {:?}", result);

    // Forged epoch_sync_data_hash (the real Some replaced with a different Some) is rejected.
    let mut forged = block;
    {
        let header = Arc::make_mut(&mut forged).mut_header();
        header.set_epoch_sync_data_hash(Some(CryptoHash::hash_bytes(b"forged")));
        header.resign(&signer);
    }
    let result = chain.process_block_header(forged.header());
    assert!(matches!(result, Err(Error::InvalidEpochSyncDataHash)), "got {:?}", result);
}
