use crate::stateless_validation::validate::validate_partial_encoded_state_witness;
use near_async::time::Clock;
#[cfg(feature = "nightly")]
use near_async::time::{Duration, FakeClock, Utc};
use near_chain::ChainStoreAccess;
use near_chain::test_utils::setup;
#[cfg(feature = "nightly")]
use near_chain::{BlockProcessingArtifact, Provenance, test_utils::process_block_sync};
use near_chain_primitives::Error;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitnessV2, VersionedPartialEncodedStateWitness,
};
#[cfg(feature = "nightly")]
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::types::{Balance, Gas, ShardId};
use near_primitives::version::PROTOCOL_VERSION;

/// A forged `height_created` on a V2 witness is rejected by the cross-check.
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

/// Loose cross-check (parent absent) rejects a signed height below the anchor-implied
/// height (`anchor.height + 2`).
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_height_below_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent unknown locally (raced); anchor = genesis. Forge a height equal to
    // anchor height + 1 — below the anchor-implied height of anchor height + 2.
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

    let err = result.err().expect("validation must reject below-height witness");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("does not match anchor-implied height"),
        "error message must reference the loose cross-check; got: {msg}"
    );
}

/// Loose cross-check (parent absent) rejects a signed height above the anchor-implied
/// height (`anchor.height + 2`) — a skipped slot. The exact pin closes the authenticated
/// cache-spam vector: one anchor authorizes exactly one ChunkProductionKey, even though
/// `MAX_HEIGHTS_AHEAD` alone would let the producer sign every height in the window.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_height_above_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent unknown locally (raced); anchor = genesis. Forge a height of anchor
    // height + 3 (a skipped slot) — above the anchor-implied height of anchor height + 2.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let forged_height = genesis_height + 3;
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

    let err = result.err().expect("validation must reject above-height witness");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("does not match anchor-implied height"),
        "error message must reference the loose cross-check; got: {msg}"
    );
}

/// Loose cross-check accepts a parent-absent witness at the anchor-implied height
/// (`anchor.height + 2`): the 1-block-behind win, resolved before the parent is processed.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_absent_parent_and_valid_anchor_is_accepted() {
    use crate::stateless_validation::validate::ChunkRelevance;

    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent unknown locally (raced); anchor = genesis (seeded at init). Height
    // exactly the anchor-implied height, so the loose check passes.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let height = genesis_height + 2;
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        unknown_parent,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        0,
        height,
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

    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "parent-absent witness with a valid anchor must be accepted; got {result:?}"
    );
}

/// Tight cross-check (parent known) accepts a chunk on a skipped slot. With the parent at
/// `G.height + 2` (slot `G.height + 1` skipped) processed locally, a chunk at `G.height + 3`
/// validates against the parent. Only the parent-absent race drops skipped-slot witnesses; once
/// the parent is known the exact `anchor + 2` pin no longer applies.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_parent_known_and_skipped_slot_is_accepted() {
    use crate::stateless_validation::validate::ChunkRelevance;

    let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap());
    clock.advance(Duration::milliseconds(3444));
    let (mut chain, _epoch_manager, _runtime, signer) = setup(clock.clock());

    let shard_id = ShardId::new(0);

    // Grandparent anchor G, built consecutively on genesis.
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    clock.advance(Duration::milliseconds(1));
    let anchor = TestBlockBuilder::from_prev_block(clock.clock(), &genesis, signer.clone()).build();
    let anchor_hash = *anchor.hash();
    let anchor_height = anchor.header().height();
    process_block_sync(
        &mut chain,
        anchor.into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    // Parent P at G.height + 2: slot G.height + 1 is skipped (no block there).
    let anchor_block = chain.get_block(&anchor_hash).unwrap();
    clock.advance(Duration::milliseconds(1));
    let parent = TestBlockBuilder::from_prev_block(clock.clock(), &anchor_block, signer.clone())
        .height(anchor_height + 2)
        .build();
    let parent_hash = *parent.hash();
    let parent_height = parent.header().height();
    process_block_sync(
        &mut chain,
        parent.into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();

    // Chunk built on the known parent P: height_created = P.height + 1 = G.height + 3.
    let height_created = parent_height + 1;
    assert_eq!(height_created, anchor_height + 3, "skipped slot G.height + 1");
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&parent_hash).unwrap();

    // prev_block = P (locally known) so the tight branch runs; prev_prev = G is the
    // signed anchor, two heights below the chunk because of the skip.
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        parent_hash,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        0,
        height_created,
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
        anchor_hash,
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

    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "parent-known witness on a skipped slot (height anchor + 3) must be accepted; got {result:?}"
    );
}

/// Tight cross-check (parent known) rejects a forged grandparent anchor mismatching the
/// parent's prev hash. On the canonical path (EarlyKickout off); under EarlyKickout the
/// anchored DB lookup rejects it earlier still.
#[cfg(not(feature = "nightly"))]
#[test]
fn v2_witness_with_anchor_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent = genesis (known); its prev hash is the default sentinel. Forge a
    // non-default anchor while keeping height/epoch correct, so only the anchor
    // mismatch trips the cross-check.
    let forged_anchor = CryptoHash::hash_bytes(b"forged_anchor_block");
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        genesis_hash,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        0,
        genesis_height + 1,
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
        forged_anchor,
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

    let err = result.err().expect("validation must reject a forged anchor");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("V2 witness chunk key mismatch"),
        "error message must reference the cross-check; got: {msg}"
    );
}

/// A default anchor (no real grandparent) is only legitimate for a chunk at genesis or
/// genesis + 1. With the parent absent there is no anchor to pin the height, so a
/// default-anchor witness above genesis + 1 is rejected to close the unpinned-height
/// escape hatch on the canonical-sampling path.
#[test]
fn v2_witness_with_default_anchor_above_genesis_plus_one_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent unknown locally (raced) and a default anchor, with a height above
    // genesis + 1 — the escape hatch the guard closes.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let forged_height = genesis_height + 3;
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

    let err =
        result.err().expect("validation must reject default-anchor witness above genesis + 1");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("default anchor") && msg.contains("above genesis + 1"),
        "error message must reference the default-anchor guard; got: {msg}"
    );
}
