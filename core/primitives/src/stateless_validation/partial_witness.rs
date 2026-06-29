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
            .field("prev_prev_block_hash", &self.inner.prev_prev_block_hash)
            .field("part_ord", &self.inner.part_ord)
            .finish()
    }
}

impl PartialEncodedStateWitnessV2 {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        prev_prev_block_hash: CryptoHash,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = PartialEncodedStateWitnessInnerV2::new(
            epoch_id,
            chunk_header,
            prev_prev_block_hash,
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

    /// Grandparent anchor of the chunk; `CryptoHash::default()` when the chunk
    /// has no real grandparent (height <= genesis_height + 1).
    pub fn prev_prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_prev_block_hash
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
    /// Grandparent anchor used for producer resolution; added so a validator
    /// can resolve and signature-verify the part before processing the parent
    /// block. `CryptoHash::default()` when the chunk has no real grandparent.
    prev_prev_block_hash: CryptoHash,
    part_ord: usize,
    part: Box<[u8]>,
    encoded_length: usize,
    signature_differentiator: SignatureDifferentiator,
}

impl PartialEncodedStateWitnessInnerV2 {
    fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        prev_prev_block_hash: CryptoHash,
        part_ord: usize,
        part: Vec<u8>,
        encoded_length: usize,
    ) -> Self {
        Self {
            epoch_id,
            shard_id: chunk_header.shard_id(),
            height_created: chunk_header.height_created(),
            prev_block_hash: *chunk_header.prev_block_hash(),
            prev_prev_block_hash,
            part_ord,
            part: part.into_boxed_slice(),
            encoded_length,
            signature_differentiator: "PartialEncodedStateWitnessV2".to_owned(),
        }
    }
}

/// Wire-format versioned partial encoded state witness.
///
/// V1 is the legacy format; V2 adds `prev_block_hash` and the grandparent
/// anchor `prev_prev_block_hash`, enabling hash-based chunk-producer lookup
/// against `DBCol::ChunkProducers` before the parent block is processed.
///
/// Rollout policy:
/// - Before EarlyKickout activation: only V1 is emitted and accepted.
///   V2 arriving over the wire is dropped.
/// - At/after EarlyKickout activation: only V2 is emitted and accepted.
///   V1 arriving over the wire is dropped.
/// - [`VersionedPartialEncodedStateWitness::new`] selects variant based on
///   `protocol_version` argument.
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
        prev_prev_block_hash: CryptoHash,
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
                prev_prev_block_hash,
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

    /// Grandparent anchor of the chunk (V2 only); see
    /// [`PartialEncodedStateWitnessV2::prev_prev_block_hash`].
    pub fn prev_prev_block_hash(&self) -> Option<&CryptoHash> {
        match self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2.prev_prev_block_hash()),
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

    /// Stable metric-label string identifying the wire version. Used at every
    /// metric emission site that needs to distinguish V1 from V2 traffic, so
    /// the `match` is written once here instead of at each call site.
    pub fn version_label(&self) -> &'static str {
        match self {
            Self::V1(_) => "v1",
            Self::V2(_) => "v2",
        }
    }
}

impl From<PartialEncodedStateWitness> for VersionedPartialEncodedStateWitness {
    fn from(w: PartialEncodedStateWitness) -> Self {
        Self::V1(w)
    }
}
