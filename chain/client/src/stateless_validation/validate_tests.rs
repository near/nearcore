use crate::stateless_validation::validate::validate_partial_encoded_state_witness;
use near_async::time::Clock;
use near_chain::ChainStoreAccess;
use near_chain::test_utils::setup;
use near_chain_primitives::Error;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitnessV2, VersionedPartialEncodedStateWitness,
};
use near_primitives::types::{Balance, Gas, ShardId};
use near_primitives::version::PROTOCOL_VERSION;

/// `PartialEncodedStateWitnessV2` authenticates (epoch_id, shard_id,
/// height_created, prev_block_hash) under producer signature, but without
/// cross-check in `validate_partial_encoded_state_witness` a producer authorized
/// for (prev_block, shard) could sign a witness with inconsistent
/// `height_created` and have it stored/forwarded under forged key. Test forges
/// mismatch, asserts dedicated rejection fires.
#[test]
fn v2_witness_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // `prev_block.height + 1 == genesis_height + 1` is value cross-check computes
    // for chunk on top of genesis. Forge `+ 2` instead — stays inside relevance
    // window (HEAD == FINAL_HEAD == genesis, so admissible heights are
    // `(genesis_height, genesis_height + MAX_HEIGHTS_AHEAD]`).
    let forged_height = genesis_height + 2;
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        genesis_hash,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        0,
        forged_height,
        shard_id,
        Gas::ZERO,
        Gas::ZERO,
        Balance::ZERO,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        CongestionInfo::default(),
        BandwidthRequests::empty(),
        None,
        signer.as_ref(),
        PROTOCOL_VERSION,
    ));

    let witness = VersionedPartialEncodedStateWitness::V2(PartialEncodedStateWitnessV2::new(
        epoch_id,
        chunk_header,
        0,
        b"payload".to_vec(),
        7,
        signer.as_ref(),
    ));

    let store = chain.chain_store().store();
    let result = validate_partial_encoded_state_witness(
        chain.epoch_manager.as_ref(),
        &witness,
        signer.validator_id(),
        &store,
    );

    let err = result.err().expect("validation must reject mismatched chunk key");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("V2 witness chunk key mismatch"),
        "error message must reference cross-check; got: {msg}"
    );
}
