use crate::stateless_validation::validate::validate_partial_encoded_state_witness;
use near_async::time::Clock;
use near_chain::ChainStoreAccess;
use near_chain::test_utils::setup;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::partial_witness::{
    PartialEncodedStateWitnessV2, VersionedPartialEncodedStateWitness,
};
use near_primitives::types::{Balance, BlockHeight, EpochId, Gas, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;

fn make_v2_witness(
    signer: &ValidatorSigner,
    epoch_id: EpochId,
    prev_block_hash: CryptoHash,
    prev_prev_hash: CryptoHash,
    height_created: BlockHeight,
    shard_id: ShardId,
) -> VersionedPartialEncodedStateWitness {
    let chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        prev_block_hash,
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
        signer,
        PROTOCOL_VERSION,
    ));
    VersionedPartialEncodedStateWitness::V2(PartialEncodedStateWitnessV2::new(
        epoch_id,
        chunk_header,
        prev_prev_hash,
        0,
        b"payload".to_vec(),
        7,
        signer,
    ))
}

/// A V2 witness whose producer signature verifies against the anchored lookup
/// is relevant even though its prev block is unknown locally — the whole point
/// of anchoring on prev_prev (verify-before-cache).
#[test]
fn v2_witness_verifiable_before_prev_block() {
    let (chain, epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let unknown_prev_block = CryptoHash::hash_bytes(b"unprocessed_prev_block");

    let witness = make_v2_witness(
        signer.as_ref(),
        epoch_id,
        unknown_prev_block,
        genesis_hash,
        genesis_height + 2,
        ShardId::new(0),
    );

    let store = chain.chain_store().store();
    let result = validate_partial_encoded_state_witness(
        epoch_manager.as_ref(),
        &witness,
        signer.validator_id(),
        &store,
    )
    .unwrap();
    assert!(result.is_relevant(), "witness must validate without the prev block");
}

/// Signature-before-cache: a V2 part signed by the wrong key must be rejected
/// outright (it would previously have been cached unauthenticated).
#[test]
fn v2_witness_bad_signature_is_rejected() {
    let (chain, epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let stranger = near_primitives::test_utils::create_test_signer("stranger");

    let witness = make_v2_witness(
        &stranger,
        epoch_id,
        CryptoHash::hash_bytes(b"unprocessed_prev_block"),
        genesis_hash,
        genesis_height + 2,
        ShardId::new(0),
    );

    let store = chain.chain_store().store();
    let err = validate_partial_encoded_state_witness(
        epoch_manager.as_ref(),
        &witness,
        signer.validator_id(),
        &store,
    )
    .err()
    .expect("bad signature must be rejected");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("Invalid signature"), "got: {msg}");
}

/// A signed height that leaves no room for the prev block between the anchor
/// and the chunk is forged.
#[test]
fn v2_witness_height_too_low_for_anchor_is_rejected() {
    let (chain, epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Anchored on genesis, the lowest plausible chunk height is genesis + 2.
    let witness = make_v2_witness(
        signer.as_ref(),
        epoch_id,
        CryptoHash::hash_bytes(b"unprocessed_prev_block"),
        genesis_hash,
        genesis_height + 1,
        ShardId::new(0),
    );

    let store = chain.chain_store().store();
    let err = validate_partial_encoded_state_witness(
        epoch_manager.as_ref(),
        &witness,
        signer.validator_id(),
        &store,
    )
    .err()
    .expect("height below anchor + 2 must be rejected");
    let Error::InvalidPartialChunkStateWitness(msg) = err else {
        panic!("expected InvalidPartialChunkStateWitness, got {err:?}");
    };
    assert!(msg.contains("too low for anchor"), "got: {msg}");
}

/// Behind-node: an unknown anchor means the producer signature cannot be
/// verified — the part is dropped (irrelevant), not cached and not an error.
/// The strict anchored read is nightly-only; on stable the identity fallback
/// resolves every anchor.
#[cfg(feature = "nightly")]
#[test]
fn v2_witness_unknown_anchor_is_dropped() {
    use crate::stateless_validation::validate::ChunkRelevance;

    let (chain, epoch_manager, _runtime, signer) = setup(Clock::real());

    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();
    let unknown_anchor = CryptoHash::hash_bytes(b"unknown_anchor");

    let witness = make_v2_witness(
        signer.as_ref(),
        epoch_id,
        genesis_hash,
        unknown_anchor,
        genesis_height + 2,
        ShardId::new(0),
    );

    let store = chain.chain_store().store();
    let result = validate_partial_encoded_state_witness(
        epoch_manager.as_ref(),
        &witness,
        signer.validator_id(),
        &store,
    )
    .unwrap();
    assert!(
        matches!(result, ChunkRelevance::AnchorStateUnavailable),
        "unknown anchor must drop, not error; got {:?}",
        <&str>::from(result),
    );
}
