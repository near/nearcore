#![no_main]

use std::ops::Deref;
use arbitrary::Arbitrary;

use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::block::Block;
use near_primitives::utils::MaybeValidated;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::challenge::Challenges;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::num_rational::Ratio;
use near_primitives::validator_signer::ValidatorSigner;
use near_chain::test_utils::{setup, process_block_sync};
use near_chain::BlockProcessingArtifact;
use near_chain::Provenance;


#[derive(Debug, Arbitrary, BorshDeserialize, BorshSerialize)]
pub struct BlockParams {
    chunk_headers: Vec<ShardChunkHeader>,
    challenges: Challenges,
}

libfuzzer_sys::fuzz_target!(|params: BlockParams| {
    let tree = PartialMerkleTree::default();
    let (mut chain, _, _, signer) = setup();
    let signer: &dyn ValidatorSigner = signer.deref();
    let prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        prev.header(),
        prev.header().height() + 1,
        prev.header().block_ordinal() + 1,
        params.chunk_headers,
        prev.header().epoch_id().clone(),
        prev.header().next_epoch_id().clone(),
        None,
        vec![],
        Ratio::new(0, 1),
        0,
        0,
        Some(0),
        vec![],
        params.challenges,
        signer,
        *prev.header().next_bp_hash(),
        tree.root(),
        None,
    );
    let _ = block.check_validity();

    let mut block_processing_artifacts = BlockProcessingArtifact::default();
    let _ = process_block_sync(
        &mut chain,
        &None,
        MaybeValidated::from_validated(block),
        Provenance::PRODUCED,
        &mut block_processing_artifacts,
    );
});