use super::partial_witness::{PartialEncodedStateWitness, VersionedPartialEncodedStateWitness};
use crate::test_utils::{create_test_signer, test_chunk_header};
use crate::types::EpochId;
use crate::validator_signer::ValidatorSigner;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::version::ProtocolFeature;

fn test_signer() -> ValidatorSigner {
    create_test_signer("test_account")
}

fn test_epoch_id() -> EpochId {
    EpochId(CryptoHash::hash_bytes(b"test_epoch"))
}

/// Any protocol version strictly below EarlyKickout. `checked_sub` is
/// infallible (EarlyKickout > 0) but we keep it defensive so if the
/// feature ever moves to protocol version 1 a reviewer notices.
fn pre_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version().checked_sub(1).unwrap()
}

fn post_kickout_version() -> ProtocolVersion {
    ProtocolFeature::EarlyKickout.protocol_version()
}

fn make_witness(
    signer: &ValidatorSigner,
    protocol_version: ProtocolVersion,
) -> VersionedPartialEncodedStateWitness {
    let prev_block_hash = CryptoHash::hash_bytes(b"prev_block");
    let prev_prev_block_hash = CryptoHash::hash_bytes(b"prev_prev_block");
    let chunk_header = test_chunk_header(prev_block_hash, signer, protocol_version);

    VersionedPartialEncodedStateWitness::new(
        test_epoch_id(),
        chunk_header,
        prev_prev_block_hash,
        0,
        b"test_witness_data".to_vec(),
        17,
        signer,
        protocol_version,
    )
}

#[test]
fn test_v1_construction_and_accessors() {
    let signer = test_signer();
    let w = make_witness(&signer, pre_kickout_version());
    assert!(matches!(w, VersionedPartialEncodedStateWitness::V1(_)));
    assert!(w.prev_block_hash().is_none());
    assert!(w.prev_prev_block_hash().is_none());
    assert_eq!(w.part_ord(), 0);
    assert_eq!(w.part_size(), 17);
    assert_eq!(w.encoded_length(), 17);
    assert!(w.verify(&signer.public_key()));
}

#[test]
fn test_version_label() {
    let signer = test_signer();
    assert_eq!(make_witness(&signer, pre_kickout_version()).version_label(), "v1");
    assert_eq!(make_witness(&signer, post_kickout_version()).version_label(), "v2");
}

#[test]
fn test_v2_construction_and_accessors() {
    let signer = test_signer();
    let w = make_witness(&signer, post_kickout_version());
    assert!(matches!(w, VersionedPartialEncodedStateWitness::V2(_)));
    let expected_hash = CryptoHash::hash_bytes(b"prev_block");
    assert_eq!(w.prev_block_hash(), Some(&expected_hash));
    let expected_anchor = CryptoHash::hash_bytes(b"prev_prev_block");
    assert_eq!(w.prev_prev_block_hash(), Some(&expected_anchor));
    assert_eq!(w.part_ord(), 0);
    assert_eq!(w.part_size(), 17);
    assert_eq!(w.encoded_length(), 17);
    assert!(w.verify(&signer.public_key()));

    let bad_signer = create_test_signer("wrong_account");
    assert!(!w.verify(&bad_signer.public_key()));
}

#[test]
fn test_borsh_roundtrip_v1() {
    let signer = test_signer();
    let w = make_witness(&signer, pre_kickout_version());
    let bytes = borsh::to_vec(&w).unwrap();
    let decoded: VersionedPartialEncodedStateWitness = borsh::from_slice(&bytes).unwrap();
    assert_eq!(w, decoded);
    assert_eq!(bytes[0], 0, "V1 discriminant must be 0");
}

#[test]
fn test_borsh_roundtrip_v2() {
    let signer = test_signer();
    let w = make_witness(&signer, post_kickout_version());
    let bytes = borsh::to_vec(&w).unwrap();
    let decoded: VersionedPartialEncodedStateWitness = borsh::from_slice(&bytes).unwrap();
    assert_eq!(w, decoded);
    assert_eq!(bytes[0], 1, "V2 discriminant must be 1");
    assert_eq!(decoded.prev_block_hash(), w.prev_block_hash());
}

#[test]
fn test_versioned_discriminants_are_stable() {
    let signer = test_signer();
    let v1 = make_witness(&signer, pre_kickout_version());
    let v2 = make_witness(&signer, post_kickout_version());
    let v1_bytes = borsh::to_vec(&v1).unwrap();
    let v2_bytes = borsh::to_vec(&v2).unwrap();
    assert_eq!(v1_bytes[0], 0, "V1 discriminant must be 0");
    assert_eq!(v2_bytes[0], 1, "V2 discriminant must be 1");
}

/// A signature produced over a V1 witness's inner bytes must not verify
/// when grafted onto a V2 struct, because the V1/V2 inner types use
/// different `signature_differentiator` strings. Exercises the replay
/// attack end-to-end rather than just checking byte-level divergence.
#[test]
fn test_v2_signature_differentiator_prevents_cross_version_replay() {
    let signer = test_signer();
    let v1 = make_witness(&signer, pre_kickout_version());
    let v2 = make_witness(&signer, post_kickout_version());

    let v1_sig = match &v1 {
        VersionedPartialEncodedStateWitness::V1(w) => w.signature.clone(),
        _ => panic!("expected V1"),
    };
    let mut v2_for_graft = v2.clone();
    match &mut v2_for_graft {
        VersionedPartialEncodedStateWitness::V2(w) => {
            w.signature = v1_sig;
        }
        _ => panic!("expected V2"),
    }

    assert!(
        !v2_for_graft.verify(&signer.public_key()),
        "V1 signature must not verify against V2 inner",
    );
    // Sanity: the original V2 witness still verifies, so the only
    // thing the graft changed is the signature.
    assert!(v2.verify(&signer.public_key()));
}

/// Reverse direction: a signature produced over a V2 witness's inner bytes
/// must not verify when grafted onto a V1 struct, for the same
/// `signature_differentiator` reason as the forward case above.
#[test]
fn test_v1_signature_differentiator_prevents_cross_version_replay() {
    let signer = test_signer();
    let v1 = make_witness(&signer, pre_kickout_version());
    let v2 = make_witness(&signer, post_kickout_version());

    let v2_sig = match &v2 {
        VersionedPartialEncodedStateWitness::V2(w) => w.signature.clone(),
        _ => panic!("expected V2"),
    };
    let mut v1_for_graft = v1.clone();
    match &mut v1_for_graft {
        VersionedPartialEncodedStateWitness::V1(w) => {
            w.signature = v2_sig;
        }
        _ => panic!("expected V1"),
    }

    assert!(
        !v1_for_graft.verify(&signer.public_key()),
        "V2 signature must not verify against V1 inner",
    );
    assert!(v1.verify(&signer.public_key()));
}

/// V1 and V2 witnesses signed by the same key must both borsh round-trip
/// and both verify. This guards against regressions where a producer
/// rolls to V2 but a stale-version validator can no longer accept either.
#[test]
fn test_coexistence_v1_and_v2_both_accepted() {
    let signer = test_signer();
    let v1 = make_witness(&signer, pre_kickout_version());
    let v2 = make_witness(&signer, post_kickout_version());

    for (witness, label) in [(&v1, "v1"), (&v2, "v2")] {
        let bytes = borsh::to_vec(witness).unwrap();
        let decoded: VersionedPartialEncodedStateWitness = borsh::from_slice(&bytes).unwrap();
        assert_eq!(witness, &decoded, "{label} borsh round-trip");
        assert!(decoded.verify(&signer.public_key()), "{label} verify");
    }
}

#[test]
fn test_from_partial_encoded_state_witness() {
    let signer = test_signer();
    let prev_block_hash = CryptoHash::hash_bytes(b"prev_block");
    let chunk_header = test_chunk_header(prev_block_hash, &signer, pre_kickout_version());
    let v1 = PartialEncodedStateWitness::new(
        test_epoch_id(),
        chunk_header,
        0,
        b"test_data".to_vec(),
        9,
        &signer,
    );
    let versioned: VersionedPartialEncodedStateWitness = v1.clone().into();
    assert!(matches!(versioned, VersionedPartialEncodedStateWitness::V1(_)));
    if let VersionedPartialEncodedStateWitness::V1(w) = versioned {
        assert_eq!(w, v1);
    }
}
