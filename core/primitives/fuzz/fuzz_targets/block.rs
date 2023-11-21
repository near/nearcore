#![no_main]

use near_core_primitives_fuzz::BlockParams;

use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::block::Block;
use near_primitives::utils::MaybeValidated;
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::block_header::BlockHeader;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::challenge::Challenges;
use near_primitives::num_rational::Ratio;

use near_chain::test_utils::{setup, process_block_sync};
use near_chain::BlockProcessingArtifact;
use near_chain::Provenance;
use near_chain::hash::CryptoHash;
use near_chain::types::EpochId;

use arbitrary::Arbitrary;

#[derive(Debug, Arbitrary, BorshDeserialize, BorshSerialize)]
pub struct BlockParams {
    chunks: Vec<ShardChunkHeader>,
    challenges: Challenges,
}

libfuzzer_sys::fuzz_target!(|params: BlockParams| {
    let mut tree = PartialMerkleTree::default();
    let chain, _, _, signer = setup();
    let mut prev = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let block = Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        prev.header(),
        height: prev.header().height() + 1,
        block_ordinal: prev.header().block_ordinal() + 1,
        chunks: params.chunks,
        epoch_id: prev.header().epoch_id().clone(),
        next_epoch_id: if prev.header().prev_hash() == &CryptoHash::default() {
           EpochId(*prev.hash())
        } else {
            prev.header().next_epoch_id().clone()
        },
        epoch_sync_data_hash: None,
        approvals: vec![],
        gas_price_adjustment_rate: Ratio::new(0, 1),
        min_gas_price: 0,
        max_gas_price: 0,
        minted_amount: Some(0),
        challenges_result: vec![],
        challenges: params.challenges,
        signer: signer.clone(),
        next_bp_hash: *prev.header().next_bp_hash(),
        block_merkle_root: tree.root(),
        timestamp_override: None,
    );
    block.check_validity();

    let mut block_processing_artifacts = BlockProcessingArtifact::default();
    process_block_sync(&None, MaybeValidated::from(block.clone()), Provenance::PRODUCED, &mut block_processing_artifacts).map(|_| {});

    let serialized_block = match block.try_to_vec() {
        Ok(data) => data,
        Err(err) => {
            // Serialization should not fail; if it does, that's interesting!
            eprintln!("Serialization error: {}", err);
            return;
        }
    };

    let deserialized_block = match BorshDeserialize::try_from_slice(&serialized_block) {
        Ok(block) => block,
        Err(err) => {
            // Deserialization should not fail; if it does, that's a bug!
            eprintln!("Deserialization error: {}", err);
            return;
        }
    };

    assert_eq!(deserialized_block, block, "Deserialized block does not match the original.");
});