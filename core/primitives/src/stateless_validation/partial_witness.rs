use super::ChunkProductionKey;
use crate::sharding::ShardChunkHeader;
use crate::types::{EpochId, SignatureDifferentiator};
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, ProtocolVersion, ShardId};
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::{Debug, Formatter};

/// Represents max allowed size of the compressed state witness,
/// corresponds to EncodedChunkStateWitness struct size.
/// The value is set to max network message size when `test_features`
/// is enabled to make it possible to test blockchain behavior with
/// arbitrary large witness (see #11703).
pub const MAX_COMPRESSED_STATE_WITNESS_SIZE: ByteSize =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 48 });

/// Represents the Reed Solomon erasure encoded parts of the `EncodedChunkStateWitness`.
/// These are created and signed by the chunk producer and sent to the chunk validators.
/// Note that the chunk validators do not require all the parts of the state witness to
/// reconstruct the full state witness due to the Reed Solomon erasure encoding.
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedStateWitness {
    inner: PartialEncodedStateWitnessInner,
    pub signature: Signature,
}

impl Debug for PartialEncodedStateWitness {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialEncodedStateWitness")
            .field("epoch_id", &self.inner.epoch_id)
            .field("shard_id", &self.inner.shard_id)
            .field("height_created", &self.inner.height_created)
            .field("part_ord", &self.inner.part_ord)
            .finish()
    }
}

impl PartialEncodedStateWitness {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = PartialEncodedStateWitnessInner::new(
            epoch_id,
            chunk_header,
            part_ord,
            part,
            encoded_length,
        );
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.inner.shard_id,
            epoch_id: self.inner.epoch_id,
            height_created: self.inner.height_created,
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn part_ord(&self) -> usize {
        self.inner.part_ord
    }

    pub fn part_size(&self) -> usize {
        self.inner.part.len()
    }

    pub fn encoded_length(&self) -> usize {
        self.inner.encoded_length
    }

    pub fn into_part(self) -> Box<[u8]> {
        self.inner.part
    }
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedStateWitnessInner {
    epoch_id: EpochId,
    shard_id: ShardId,
    height_created: BlockHeight,
    part_ord: usize,
    part: Box<[u8]>,
    encoded_length: usize,
    signature_differentiator: SignatureDifferentiator,
}

impl PartialEncodedStateWitnessInner {
    fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
    ) -> Self {
        Self {
            epoch_id,
            shard_id: chunk_header.shard_id(),
            height_created: chunk_header.height_created(),
            part_ord,
            part: part.into_boxed_slice(),
            encoded_length,
            signature_differentiator: "PartialEncodedStateWitness".to_owned(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedStateWitnessV2 {
    inner: PartialEncodedStateWitnessInnerV2,
    pub signature: Signature,
}

impl Debug for PartialEncodedStateWitnessV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartialEncodedStateWitnessV2")
            .field("epoch_id", &self.inner.epoch_id)
            .field("shard_id", &self.inner.shard_id)
            .field("height_created", &self.inner.height_created)
            .field("prev_block_hash", &self.inner.prev_block_hash)
            .field("part_ord", &self.inner.part_ord)
            .finish()
    }
}

impl PartialEncodedStateWitnessV2 {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = PartialEncodedStateWitnessInnerV2::new(
            epoch_id,
            chunk_header,
            part_ord,
            part,
            encoded_length,
        );
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        Self { inner, signature }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.inner.shard_id,
            epoch_id: self.inner.epoch_id,
            height_created: self.inner.height_created,
        }
    }

    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_block_hash
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn part_ord(&self) -> usize {
        self.inner.part_ord
    }

    pub fn part_size(&self) -> usize {
        self.inner.part.len()
    }

    pub fn encoded_length(&self) -> usize {
        self.inner.encoded_length
    }

    pub fn into_part(self) -> Box<[u8]> {
        self.inner.part
    }
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct PartialEncodedStateWitnessInnerV2 {
    epoch_id: EpochId,
    shard_id: ShardId,
    height_created: BlockHeight,
    prev_block_hash: CryptoHash,
    part_ord: usize,
    part: Box<[u8]>,
    encoded_length: usize,
    signature_differentiator: SignatureDifferentiator,
}

impl PartialEncodedStateWitnessInnerV2 {
    fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
    ) -> Self {
        Self {
            epoch_id,
            shard_id: chunk_header.shard_id(),
            height_created: chunk_header.height_created(),
            prev_block_hash: *chunk_header.prev_block_hash(),
            part_ord,
            part: part.into_boxed_slice(),
            encoded_length,
            signature_differentiator: "PartialEncodedStateWitnessV2".to_owned(),
        }
    }
}

/// Wire-format versioned partial encoded state witness.
///
/// V1 is the legacy format; V2 adds `prev_block_hash` to enable
/// hash-based chunk-producer lookup against `DBCol::ChunkProducers`.
///
/// Rollout policy:
/// - V1 is accepted at all protocol versions and remains accepted for the
///   duration of the EarlyKickout rollout window.
/// - V2 is emitted only when the epoch's protocol version is at least
///   `ProtocolFeature::EarlyKickout`. [`VersionedPartialEncodedStateWitness::new`]
///   selects the variant based on the `protocol_version` argument.
/// - At/after EarlyKickout activation, producers stop emitting V1. Receivers
///   continue accepting V1 indefinitely (stale-node resilience).
///
/// Cross-version replay resistance: V1 and V2 use distinct
/// `signature_differentiator` strings, so a signature produced over V1's
/// inner bytes cannot be grafted onto a V2 struct and verify.
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum VersionedPartialEncodedStateWitness {
    V1(PartialEncodedStateWitness) = 0,
    V2(PartialEncodedStateWitnessV2) = 1,
}

impl Debug for VersionedPartialEncodedStateWitness {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1(v1) => v1.fmt(f),
            Self::V2(v2) => v2.fmt(f),
        }
    }
}

impl VersionedPartialEncodedStateWitness {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::EarlyKickout.enabled(protocol_version) {
            Self::V2(PartialEncodedStateWitnessV2::new(
                epoch_id,
                chunk_header,
                part_ord,
                part,
                encoded_length,
                signer,
            ))
        } else {
            Self::V1(PartialEncodedStateWitness::new(
                epoch_id,
                chunk_header,
                part_ord,
                part,
                encoded_length,
                signer,
            ))
        }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            Self::V1(v1) => v1.chunk_production_key(),
            Self::V2(v2) => v2.chunk_production_key(),
        }
    }

    pub fn prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2.prev_block_hash()),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(v1) => v1.verify(public_key),
            Self::V2(v2) => v2.verify(public_key),
        }
    }

    pub fn part_ord(&self) -> usize {
        match self {
            Self::V1(v1) => v1.part_ord(),
            Self::V2(v2) => v2.part_ord(),
        }
    }

    pub fn part_size(&self) -> usize {
        match self {
            Self::V1(v1) => v1.part_size(),
            Self::V2(v2) => v2.part_size(),
        }
    }

    pub fn encoded_length(&self) -> usize {
        match self {
            Self::V1(v1) => v1.encoded_length(),
            Self::V2(v2) => v2.encoded_length(),
        }
    }

    pub fn into_part(self) -> Box<[u8]> {
        match self {
            Self::V1(v1) => v1.into_part(),
            Self::V2(v2) => v2.into_part(),
        }
    }
}

impl From<PartialEncodedStateWitness> for VersionedPartialEncodedStateWitness {
    fn from(w: PartialEncodedStateWitness) -> Self {
        Self::V1(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{create_test_signer, test_chunk_header};
    use crate::types::EpochId;
    use near_primitives_core::hash::CryptoHash;

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
        let chunk_header = test_chunk_header(prev_block_hash, signer, protocol_version);

        VersionedPartialEncodedStateWitness::new(
            test_epoch_id(),
            chunk_header,
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
        assert_eq!(w.part_ord(), 0);
        assert_eq!(w.part_size(), 17);
        assert_eq!(w.encoded_length(), 17);
        assert!(w.verify(&signer.public_key()));
    }

    #[test]
    fn test_v2_construction_and_accessors() {
        let signer = test_signer();
        let w = make_witness(&signer, post_kickout_version());
        assert!(matches!(w, VersionedPartialEncodedStateWitness::V2(_)));
        let expected_hash = CryptoHash::hash_bytes(b"prev_block");
        assert_eq!(w.prev_block_hash(), Some(&expected_hash));
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
}
