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

/// Forged `height_created` on a V2 witness must be rejected by the cross-check (full rationale
/// in `validate.rs`).
#[test]
fn v2_witness_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Forge `+ 2` (cross-check expects `+ 1`); still inside the relevance window.
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

    // Signed anchor matches the genesis parent's prev hash (default), so only
    // the forged height trips the cross-check.
    let witness = VersionedPartialEncodedStateWitness::V2(PartialEncodedStateWitnessV2::new(
        epoch_id,
        chunk_header,
        CryptoHash::default(),
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

/// Loose cross-check (parent block absent): a signed `height_created` below the
/// anchor-implied minimum (`anchor.height + 2`) must be rejected. Nightly-only:
/// resolution must first succeed via the anchor's DB row, which only exists when
/// EarlyKickout is enabled.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_height_below_anchor_minimum_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent unknown locally (raced); anchor = genesis. Forge a height equal to
    // anchor height + 1 — below the minimum of anchor height + 2.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let forged_height = genesis_height + 1;
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        unknown_parent,
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
        genesis_hash,
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

    let err = result.err().expect("validation must reject below-minimum height");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("below anchor-implied minimum"),
        "error message must reference the loose cross-check; got: {msg}"
    );
}
