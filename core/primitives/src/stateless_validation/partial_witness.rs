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

/// V2 of `PartialEncodedStateWitness` that carries `prev_block_hash` inside the
/// signed inner, enabling hash-based chunk producer lookups in the
/// stateless-validation path. Gated behind `ProtocolFeature::EarlyKickout`.
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

    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_block_hash
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

/// Wire-level versioned wrapper around `PartialEncodedStateWitness` variants.
///
/// Discriminant layout:
/// - `V1 = 1` wraps the legacy flat struct.
/// - `V2 = 2` wraps the new `PartialEncodedStateWitnessV2` carrying
///   `prev_block_hash`.
///
/// Discriminant 0 is intentionally unused so an accidentally zero-initialized
/// byte is not a valid variant.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum VersionedPartialEncodedStateWitness {
    V1(PartialEncodedStateWitness) = 1,
    V2(PartialEncodedStateWitnessV2) = 2,
}

impl VersionedPartialEncodedStateWitness {
    /// Constructs a V2 witness when `ProtocolFeature::EarlyKickout` is enabled
    /// for the given protocol version, otherwise a V1 witness.
    pub fn new_versioned(
        protocol_version: ProtocolVersion,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
        signer: &ValidatorSigner,
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
            Self::V1(w) => w.chunk_production_key(),
            Self::V2(w) => w.chunk_production_key(),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        match self {
            Self::V1(w) => w.verify(public_key),
            Self::V2(w) => w.verify(public_key),
        }
    }

    pub fn part_ord(&self) -> usize {
        match self {
            Self::V1(w) => w.part_ord(),
            Self::V2(w) => w.part_ord(),
        }
    }

    pub fn part_size(&self) -> usize {
        match self {
            Self::V1(w) => w.part_size(),
            Self::V2(w) => w.part_size(),
        }
    }

    pub fn encoded_length(&self) -> usize {
        match self {
            Self::V1(w) => w.encoded_length(),
            Self::V2(w) => w.encoded_length(),
        }
    }

    pub fn into_part(self) -> Box<[u8]> {
        match self {
            Self::V1(w) => w.into_part(),
            Self::V2(w) => w.into_part(),
        }
    }

    /// Returns the `prev_block_hash` carried by V2 witnesses. V1 witnesses do
    /// not carry this field and return `None`.
    pub fn prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(w) => Some(w.prev_block_hash()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sharding::ShardChunkHeader;
    use crate::validator_signer::InMemoryValidatorSigner;
    use near_crypto::KeyType;
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::AccountId;

    fn make_signer() -> ValidatorSigner {
        let account_id: AccountId = "test.near".parse().unwrap();
        InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, "test").into()
    }

    fn make_header(prev_block_hash: CryptoHash) -> ShardChunkHeader {
        let shard_id = ShardId::new(0);
        ShardChunkHeader::new_dummy(10, shard_id, prev_block_hash)
    }

    #[test]
    fn v1_raw_borsh_roundtrip() {
        let signer = make_signer();
        let header = make_header(CryptoHash::hash_bytes(b"prev"));
        let v1 = PartialEncodedStateWitness::new(
            EpochId::default(),
            header,
            3,
            vec![1, 2, 3, 4],
            10,
            &signer,
        );
        let bytes = borsh::to_vec(&v1).unwrap();
        let decoded = PartialEncodedStateWitness::try_from_slice(&bytes).unwrap();
        assert_eq!(v1, decoded);
    }

    #[test]
    fn v2_raw_borsh_roundtrip() {
        let signer = make_signer();
        let prev = CryptoHash::hash_bytes(b"prev");
        let header = make_header(prev);
        let v2 = PartialEncodedStateWitnessV2::new(
            EpochId::default(),
            header,
            3,
            vec![1, 2, 3, 4],
            10,
            &signer,
        );
        assert_eq!(v2.prev_block_hash(), &prev);
        let bytes = borsh::to_vec(&v2).unwrap();
        let decoded = PartialEncodedStateWitnessV2::try_from_slice(&bytes).unwrap();
        assert_eq!(v2, decoded);
    }

    #[test]
    fn versioned_v1_borsh_roundtrip_and_discriminant() {
        let signer = make_signer();
        let header = make_header(CryptoHash::hash_bytes(b"prev"));
        let v1 =
            PartialEncodedStateWitness::new(EpochId::default(), header, 0, vec![9; 5], 5, &signer);
        let versioned = VersionedPartialEncodedStateWitness::V1(v1.clone());
        let bytes = borsh::to_vec(&versioned).unwrap();
        assert_eq!(bytes[0], 1, "V1 discriminant must be 1");
        let raw_bytes = borsh::to_vec(&v1).unwrap();
        assert_eq!(&bytes[1..], raw_bytes.as_slice());
        let decoded = VersionedPartialEncodedStateWitness::try_from_slice(&bytes).unwrap();
        assert_eq!(versioned, decoded);
        assert!(versioned.prev_block_hash().is_none());
    }

    #[test]
    fn versioned_v2_borsh_roundtrip_and_discriminant() {
        let signer = make_signer();
        let prev = CryptoHash::hash_bytes(b"prev");
        let header = make_header(prev);
        let v2 = PartialEncodedStateWitnessV2::new(
            EpochId::default(),
            header,
            0,
            vec![9; 5],
            5,
            &signer,
        );
        let versioned = VersionedPartialEncodedStateWitness::V2(v2.clone());
        let bytes = borsh::to_vec(&versioned).unwrap();
        assert_eq!(bytes[0], 2, "V2 discriminant must be 2");
        let raw_bytes = borsh::to_vec(&v2).unwrap();
        assert_eq!(&bytes[1..], raw_bytes.as_slice());
        let decoded = VersionedPartialEncodedStateWitness::try_from_slice(&bytes).unwrap();
        assert_eq!(versioned, decoded);
        assert_eq!(versioned.prev_block_hash(), Some(&prev));
    }

    #[test]
    fn new_versioned_gates_on_protocol_version() {
        let signer = make_signer();
        let header_pre = make_header(CryptoHash::hash_bytes(b"prev-pre"));
        let header_post = make_header(CryptoHash::hash_bytes(b"prev-post"));

        let pre = VersionedPartialEncodedStateWitness::new_versioned(
            85,
            EpochId::default(),
            header_pre,
            0,
            vec![1, 2],
            2,
            &signer,
        );
        assert!(matches!(pre, VersionedPartialEncodedStateWitness::V1(_)));

        let post_version = ProtocolFeature::EarlyKickout.protocol_version();
        let post = VersionedPartialEncodedStateWitness::new_versioned(
            post_version,
            EpochId::default(),
            header_post,
            0,
            vec![1, 2],
            2,
            &signer,
        );
        assert!(matches!(post, VersionedPartialEncodedStateWitness::V2(_)));
    }
}
