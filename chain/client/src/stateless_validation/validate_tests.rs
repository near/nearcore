use crate::stateless_validation::validate::{
    ChunkRelevance, validate_chunk_contract_accesses, validate_partial_encoded_contract_deploys,
    validate_partial_encoded_state_witness,
};
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
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, MainTransitionKey, PartialEncodedContractDeploys,
    PartialEncodedContractDeploysPart,
};
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitnessV2, VersionedPartialEncodedStateWitness,
};
#[cfg(feature = "nightly")]
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{Balance, Gas, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use std::collections::HashSet;

/// Lowest protocol version with `EarlyKickout` enabled — forces the V2 wire
/// variant regardless of the compile-time `nightly` feature.
fn early_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version()
}

/// One protocol version below `EarlyKickout` — forces the V1 wire variant.
fn pre_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version().checked_sub(1).unwrap()
}

fn make_accesses(
    key: ChunkProductionKey,
    prev_block_hash: CryptoHash,
    prev_prev_block_hash: CryptoHash,
    signer: &ValidatorSigner,
    protocol_version: ProtocolVersion,
) -> ChunkContractAccesses {
    let shard_id = key.shard_id;
    ChunkContractAccesses::new(
        key,
        HashSet::new(),
        MainTransitionKey { block_hash: prev_block_hash, shard_id },
        prev_block_hash,
        prev_prev_block_hash,
        signer,
        protocol_version,
    )
}

/// A forged `height_created` on a V2 witness is rejected by the cross-check.
#[test]
fn v2_witness_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Set the height to genesis + 2 where the check expects genesis + 1, still within
    // the relevance window.
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

    // The signed anchor matches the genesis parent's prev hash (the default value), so
    // only the wrong height causes the rejection.
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

/// A V2 witness whose parent has not arrived is rejected when its height is below
/// anchor + 2.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_height_below_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // The parent block is not stored locally. The anchor is genesis. Set the height
    // to anchor + 1, which is below the required anchor + 2.
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

/// A V2 witness whose parent has not arrived is rejected above anchor + 2 (a skipped
/// slot). The exact pin means one anchor allows only one chunk key, blocking cache-spam
/// across the `MAX_HEIGHTS_AHEAD` window.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_height_above_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // The parent block is not stored locally. The anchor is genesis. Set the height
    // to anchor + 3 (a skipped slot), which is above the required anchor + 2.
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

/// A V2 witness whose parent has not arrived is accepted when its height is exactly
/// anchor + 2. This is the case the anchor is meant to handle: we can validate the
/// witness even though its parent block is not here yet.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_with_absent_parent_and_valid_anchor_is_accepted() {
    use crate::stateless_validation::validate::ChunkRelevance;

    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // The parent block is not stored locally. The anchor is genesis (seeded at
    // startup). The height is exactly anchor + 2, so the check passes.
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

    // The parent is genesis and is known locally; its prev hash is the default value.
    // Use a non-default anchor but keep the height and epoch correct, so only the wrong
    // anchor causes the rejection.
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

/// A default anchor (no real grandparent) only happens for a chunk at genesis or
/// genesis + 1. When the parent is also absent, there is nothing to pin the height to,
/// so a default-anchor witness above genesis + 1 is rejected. Otherwise that path would
/// let someone send any height.
#[test]
fn v2_witness_with_default_anchor_above_genesis_plus_one_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // The parent block is not stored locally, the anchor is the default value, and the
    // height is above genesis + 1. This is the case the guard rejects.
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

/// A V1 contract-accesses message signed by the canonical producer is accepted.
#[test]
fn v1_accesses_from_canonical_producer_is_accepted() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let accesses = make_accesses(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        pre_kickout_version(),
    );
    assert!(matches!(accesses, ChunkContractAccesses::V1(_)));

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "V1 accesses must be accepted; got {result:?}"
    );
}

/// A V2 accesses message with a forged height is rejected by the shared cross-check.
#[test]
fn v2_accesses_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // The parent is genesis, so the anchor is its default prev hash. Set the height to
    // genesis + 2 where the parent implies genesis + 1. The anchor stays the default
    // value, so the producer lookup uses the sampler and only the height is wrong. This
    // holds under nightly too.
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 2 };
    let accesses = make_accesses(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(accesses, ChunkContractAccesses::V2(_)));

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    let err = result.err().expect("validation must reject mismatched chunk key");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("contract accesses chunk key mismatch"), "got: {msg}");
}

/// When the parent block is known, the cross-check rejects a forged anchor that does
/// not match the parent's prev hash. This only matters off nightly; under nightly the
/// anchored lookup rejects the forged anchor first, because the block is unknown.
#[cfg(not(feature = "nightly"))]
#[test]
fn v2_accesses_with_anchor_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let forged_anchor = CryptoHash::hash_bytes(b"forged_anchor_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let accesses =
        make_accesses(key, genesis_hash, forged_anchor, signer.as_ref(), early_kickout_version());

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    let err = result.err().expect("validation must reject a forged anchor");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("contract accesses chunk key mismatch"), "got: {msg}");
}

/// V2 accesses with a correct chunk key but signed by a non-producer are rejected
/// at the signature check (the cross-check passes first).
#[test]
fn v2_accesses_with_wrong_signer_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let wrong_signer = create_test_signer("not_the_producer");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let accesses = make_accesses(
        key,
        genesis_hash,
        CryptoHash::default(),
        &wrong_signer,
        early_kickout_version(),
    );

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    assert!(
        matches!(result, Err(Error::Other(ref m)) if m.contains("Invalid witness contract accesses signature")),
        "wrong-signer V2 accesses must be rejected at the signature check; got {result:?}"
    );
}

/// A valid V2 accesses message at genesis + 1 is accepted (the happy path). Parent =
/// genesis (known), so the tight path runs and the key matches the parent exactly; the
/// default anchor routes the producer lookup through the canonical sampler. Acceptance
/// here does not isolate the known-parent branch (an unknown parent at this height with
/// a default anchor is also accepted); the anchored DB-row branch has its own test.
#[test]
fn v2_accesses_with_known_parent_is_accepted() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let accesses = make_accesses(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(accesses, ChunkContractAccesses::V2(_)));

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "V2 accesses with a known parent and correct key must be accepted; got {result:?}"
    );
}

/// A V2 accesses message with a real anchor, a missing parent, and a height above
/// anchor + 2 is rejected on the loose path. Mirrors the witness twin
/// `v2_witness_with_height_above_anchor_height_is_rejected`. Nightly-only: off nightly the
/// anchored lookup ignores the real anchor and falls back to canonical sampling.
#[cfg(feature = "nightly")]
#[test]
fn v2_accesses_with_height_above_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Anchor = genesis (real, known), parent not stored locally, height anchor + 3.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 3 };
    let accesses =
        make_accesses(key, unknown_parent, genesis_hash, signer.as_ref(), early_kickout_version());
    assert!(matches!(accesses, ChunkContractAccesses::V2(_)));

    let store = chain.chain_store().store();
    let err = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    )
    .err()
    .expect("validation must reject above-height accesses");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("contract accesses") && msg.contains("does not match anchor-implied height"),
        "error must reference the accesses loose cross-check; got: {msg}"
    );
}

/// A default anchor on V2 accesses only happens at genesis or genesis + 1. A message
/// that pairs a default anchor with an unknown parent at a higher height is rejected by
/// the shared `verify_anchored_chunk_key` guard, not accepted through the sampler.
/// Mirrors the witness default-anchor test.
#[test]
fn v2_accesses_with_default_anchor_above_genesis_plus_one_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 3 };
    let accesses = make_accesses(
        key,
        unknown_parent,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(accesses, ChunkContractAccesses::V2(_)));

    let store = chain.chain_store().store();
    let result = validate_chunk_contract_accesses(
        chain.epoch_manager.as_ref(),
        &accesses,
        signer.as_ref(),
        &store,
    );
    let err = result.err().expect("validation must reject a default anchor above genesis + 1");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("default anchor") && msg.contains("above genesis + 1"),
        "error message must reference the default-anchor guard; got: {msg}"
    );
}

/// Regression test for the bug this change fixes. Under `EarlyKickout` the V2 accesses
/// verifier must look up the producer from the anchor's `DBCol::ChunkProducers` row, not
/// from the sampler. We overwrite the genesis anchor's row with a producer the sampler
/// would never pick, then sign with that producer's key. If the message is accepted, the
/// verifier read the anchored row.
#[cfg(feature = "nightly")]
#[test]
fn v2_accesses_resolves_anchored_db_row() {
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Overwrite the genesis anchor row with a distinct producer (insert-only column).
    let anchored = create_test_signer("anchored_producer");
    let anchored_stake = ValidatorStake::new(
        "anchored_producer".parse().unwrap(),
        anchored.public_key(),
        Balance::from_yoctonear(1),
    );
    let key_bytes = get_block_shard_id(&genesis_hash, shard_id);
    let mut update = chain.chain_store().store().store_update();
    update.delete(DBCol::ChunkProducers, &key_bytes);
    update.commit();
    let mut update = chain.chain_store().store().store_update();
    update.insert_ser(DBCol::ChunkProducers, &key_bytes, &anchored_stake);
    update.commit();

    // The parent block is not stored locally, the anchor is genesis, and the height is
    // anchor + 2.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let height_created = genesis_height + 2;
    let store = chain.chain_store().store();

    let accepted = make_accesses(
        ChunkProductionKey { shard_id, epoch_id, height_created },
        unknown_parent,
        genesis_hash,
        &anchored,
        early_kickout_version(),
    );
    assert!(
        matches!(
            validate_chunk_contract_accesses(
                chain.epoch_manager.as_ref(),
                &accepted,
                signer.as_ref(),
                &store
            ),
            Ok(ChunkRelevance::Relevant)
        ),
        "accesses signed by the anchored DB-row producer must validate"
    );

    // Control: signed by the canonical producer (harness signer) must be rejected,
    // because the anchored row points at `anchored_producer`.
    let rejected = make_accesses(
        ChunkProductionKey { shard_id, epoch_id, height_created },
        unknown_parent,
        genesis_hash,
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(
        matches!(
            validate_chunk_contract_accesses(
                chain.epoch_manager.as_ref(),
                &rejected,
                signer.as_ref(),
                &store
            ),
            Err(Error::Other(_))
        ),
        "accesses signed by the non-anchored canonical producer must be rejected"
    );
}

fn make_deploys(
    key: ChunkProductionKey,
    prev_block_hash: CryptoHash,
    prev_prev_block_hash: CryptoHash,
    signer: &ValidatorSigner,
    protocol_version: ProtocolVersion,
) -> PartialEncodedContractDeploys {
    PartialEncodedContractDeploys::new(
        key,
        PartialEncodedContractDeploysPart {
            part_ord: 0,
            data: vec![1u8].into_boxed_slice(),
            encoded_length: 1,
        },
        prev_block_hash,
        prev_prev_block_hash,
        signer,
        protocol_version,
    )
}

/// A V1 contract-deploys message signed by the canonical producer is accepted.
#[test]
fn v1_deploys_from_canonical_producer_is_accepted() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let deploys = make_deploys(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        pre_kickout_version(),
    );
    assert!(matches!(deploys, PartialEncodedContractDeploys::V1(_)));

    let store = chain.chain_store().store();
    let result =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store);
    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "V1 deploys must be accepted; got {result:?}"
    );
}

/// A V2 deploys message with a forged height is rejected by the shared cross-check.
#[test]
fn v2_deploys_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 2 };
    let deploys = make_deploys(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(deploys, PartialEncodedContractDeploys::V2(_)));

    let store = chain.chain_store().store();
    let err =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store)
            .err()
            .expect("validation must reject mismatched chunk key");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("contract deploys chunk key mismatch"), "got: {msg}");
}

/// When the parent block is known, the cross-check rejects a forged anchor. This only
/// matters off nightly; under nightly the anchored lookup rejects the forged anchor first.
#[cfg(not(feature = "nightly"))]
#[test]
fn v2_deploys_with_anchor_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let forged_anchor = CryptoHash::hash_bytes(b"forged_anchor_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let deploys =
        make_deploys(key, genesis_hash, forged_anchor, signer.as_ref(), early_kickout_version());

    let store = chain.chain_store().store();
    let err =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store)
            .err()
            .expect("validation must reject a forged anchor");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("contract deploys chunk key mismatch"), "got: {msg}");
}

/// V2 deploys with a correct chunk key but signed by a non-producer are rejected
/// at the signature check (the cross-check passes first).
#[test]
fn v2_deploys_with_wrong_signer_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let _ = signer;

    let wrong_signer = create_test_signer("not_the_producer");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let deploys = make_deploys(
        key,
        genesis_hash,
        CryptoHash::default(),
        &wrong_signer,
        early_kickout_version(),
    );

    let store = chain.chain_store().store();
    let result =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store);
    assert!(
        matches!(result, Err(Error::Other(ref m)) if m.contains("Invalid contract deploys signature")),
        "wrong-signer V2 deploys must be rejected at the signature check; got {result:?}"
    );
}

/// A valid V2 deploys message at genesis + 1 is accepted (the happy path). Parent =
/// genesis (known), so the tight path runs and the key matches the parent exactly; the
/// default anchor routes the producer lookup through the canonical sampler. Acceptance
/// here does not isolate the known-parent branch (an unknown parent at this height with
/// a default anchor is also accepted); the anchored DB-row branch has its own test.
#[test]
fn v2_deploys_with_known_parent_is_accepted() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 1 };
    let deploys = make_deploys(
        key,
        genesis_hash,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(deploys, PartialEncodedContractDeploys::V2(_)));

    let store = chain.chain_store().store();
    let result =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store);
    assert!(
        matches!(result, Ok(ChunkRelevance::Relevant)),
        "V2 deploys with a known parent and correct key must be accepted; got {result:?}"
    );
}

/// A V2 deploys message with a real anchor, a missing parent, and a height above
/// anchor + 2 is rejected on the loose path. Mirrors the witness twin
/// `v2_witness_with_height_above_anchor_height_is_rejected`. Nightly-only: off nightly the
/// anchored lookup ignores the real anchor and falls back to canonical sampling.
#[cfg(feature = "nightly")]
#[test]
fn v2_deploys_with_height_above_anchor_height_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Anchor = genesis (real, known), parent not stored locally, height anchor + 3.
    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 3 };
    let deploys =
        make_deploys(key, unknown_parent, genesis_hash, signer.as_ref(), early_kickout_version());
    assert!(matches!(deploys, PartialEncodedContractDeploys::V2(_)));

    let store = chain.chain_store().store();
    let err =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store)
            .err()
            .expect("validation must reject above-height deploys");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("contract deploys") && msg.contains("does not match anchor-implied height"),
        "error must reference the deploys loose cross-check; got: {msg}"
    );
}

/// A default anchor on V2 deploys only happens at genesis or genesis + 1. A message that
/// pairs a default anchor with an unknown parent at a higher height is rejected by the
/// shared `verify_anchored_chunk_key` guard, not accepted through the sampler. Mirrors the
/// witness and accesses default-anchor tests.
#[test]
fn v2_deploys_with_default_anchor_above_genesis_plus_one_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let key = ChunkProductionKey { shard_id, epoch_id, height_created: genesis_height + 3 };
    let deploys = make_deploys(
        key,
        unknown_parent,
        CryptoHash::default(),
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(matches!(deploys, PartialEncodedContractDeploys::V2(_)));

    let store = chain.chain_store().store();
    let err =
        validate_partial_encoded_contract_deploys(chain.epoch_manager.as_ref(), &deploys, &store)
            .err()
            .expect("validation must reject a default anchor above genesis + 1");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(
        msg.contains("default anchor") && msg.contains("above genesis + 1"),
        "error message must reference the default-anchor guard; got: {msg}"
    );
}

/// Regression test: under `EarlyKickout` the V2 deploys verifier must look up the
/// producer from the anchor's `DBCol::ChunkProducers` row, not from the sampler. Same
/// check as the accesses regression test.
#[cfg(feature = "nightly")]
#[test]
fn v2_deploys_resolves_anchored_db_row() {
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    let anchored = create_test_signer("anchored_producer");
    let anchored_stake = ValidatorStake::new(
        "anchored_producer".parse().unwrap(),
        anchored.public_key(),
        Balance::from_yoctonear(1),
    );
    let key_bytes = get_block_shard_id(&genesis_hash, shard_id);
    let mut update = chain.chain_store().store().store_update();
    update.delete(DBCol::ChunkProducers, &key_bytes);
    update.commit();
    let mut update = chain.chain_store().store().store_update();
    update.insert_ser(DBCol::ChunkProducers, &key_bytes, &anchored_stake);
    update.commit();

    let unknown_parent = CryptoHash::hash_bytes(b"unknown_parent_block");
    let height_created = genesis_height + 2;
    let store = chain.chain_store().store();

    let accepted = make_deploys(
        ChunkProductionKey { shard_id, epoch_id, height_created },
        unknown_parent,
        genesis_hash,
        &anchored,
        early_kickout_version(),
    );
    assert!(
        matches!(
            validate_partial_encoded_contract_deploys(
                chain.epoch_manager.as_ref(),
                &accepted,
                &store
            ),
            Ok(ChunkRelevance::Relevant)
        ),
        "deploys signed by the anchored DB-row producer must validate"
    );

    // Control case: a message signed by the normal sampled producer (the harness signer)
    // must be rejected, because the anchored row now points at `anchored_producer`. This
    // shows the producer really comes from the anchored row, not just any valid producer.
    let rejected = make_deploys(
        ChunkProductionKey { shard_id, epoch_id, height_created },
        unknown_parent,
        genesis_hash,
        signer.as_ref(),
        early_kickout_version(),
    );
    assert!(
        matches!(
            validate_partial_encoded_contract_deploys(
                chain.epoch_manager.as_ref(),
                &rejected,
                &store
            ),
            Err(Error::Other(_))
        ),
        "deploys signed by the non-anchored canonical producer must be rejected"
    );
}
