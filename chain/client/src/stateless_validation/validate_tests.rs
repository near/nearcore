use crate::stateless_validation::validate::{
    ChunkRelevance, validate_chunk_contract_accesses, validate_partial_encoded_contract_deploys,
    validate_partial_encoded_state_witness,
};
use near_async::time::Clock;
use near_chain::ChainStoreAccess;
use near_chain::test_utils::setup;
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
        msg.contains("anchored chunk key mismatch"),
        "error message must reference cross-check; got: {msg}"
    );
}

/// Loose cross-check (parent absent) rejects a signed height below the anchor-implied
/// minimum (`anchor.height + 2`).
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

/// Loose cross-check accepts a parent-absent witness at the anchor-implied minimum
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
    // exactly the anchor-implied minimum, so the loose check passes.
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
        msg.contains("anchored chunk key mismatch"),
        "error message must reference the cross-check; got: {msg}"
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

/// A forged `height_created` on V2 accesses trips the shared cross-check.
#[test]
fn v2_accesses_with_height_mismatch_is_rejected() {
    let (chain, _epoch_manager, _runtime, signer) = setup(Clock::real());
    let genesis_hash = *chain.genesis().hash();
    let genesis_height = chain.genesis().height();
    let shard_id = ShardId::new(0);
    let epoch_id = chain.epoch_manager.get_epoch_id_from_prev_block(&genesis_hash).unwrap();

    // Parent = genesis (anchor = its default prev hash); forge `+2` where the
    // parent implies `+1`. Anchor stays default so resolution uses the sampler
    // and only the height trips the cross-check (holds under nightly too).
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
    assert!(msg.contains("anchored chunk key mismatch"), "got: {msg}");
}

/// Tight cross-check (parent known) rejects a forged grandparent anchor that does
/// not match the parent's prev hash. Canonical path only; under nightly the
/// anchored DB lookup rejects the forged anchor earlier (unknown block).
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
    assert!(msg.contains("anchored chunk key mismatch"), "got: {msg}");
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

/// Regression for the bug this change fixes: under `EarlyKickout` the V2 accesses
/// verifier must resolve the producer from the grandparent anchor's
/// `DBCol::ChunkProducers` row, NOT the canonical sampler. We overwrite the
/// genesis anchor's row with a producer the sampler would never pick and sign
/// with its key; acceptance proves the anchored row was read.
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

    // Parent unknown (loose path); anchor = genesis; height at the anchor-implied minimum.
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

/// A forged `height_created` on V2 deploys trips the shared cross-check.
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
    assert!(msg.contains("anchored chunk key mismatch"), "got: {msg}");
}

/// Tight cross-check (parent known) rejects a forged grandparent anchor. Canonical
/// path only; under nightly the anchored DB lookup rejects the forged anchor earlier.
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
    assert!(msg.contains("anchored chunk key mismatch"), "got: {msg}");
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

/// Regression: under `EarlyKickout` the V2 deploys verifier resolves the producer
/// from the grandparent anchor's `DBCol::ChunkProducers` row, not the canonical
/// sampler. Same mechanism as the accesses regression.
#[cfg(feature = "nightly")]
#[test]
fn v2_deploys_resolves_anchored_db_row() {
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::utils::get_block_shard_id;
    use near_store::DBCol;

    let (chain, _epoch_manager, _runtime, _signer) = setup(Clock::real());
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
}
