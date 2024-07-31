use std::fmt::{Debug, Formatter};

use crate::sharding::ShardChunkHeader;
use crate::types::EpochId;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{BlockHeight, ShardId};

use super::{ChunkProductionKey, SignatureDifferentiator};

/// Represents max allowed size of the compressed state witness,
/// corresponds to EncodedChunkStateWitness struct size.
/// The value is set to max network message size when `test_features`
/// is enabled to make it possible to test blockchain behaviour with
/// arbitrary large witness (see #11703).
pub const MAX_COMPRESSED_STATE_WITNESS_SIZE: ByteSize =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 48 });

/// Represents the Reed Solomon erasure encoded parts of the `EncodedChunkStateWitness`.
/// These are created and signed by the chunk producer and sent to the chunk validators.
/// Note that the chunk validators do not require all the parts of the state witness to
/// reconstruct the full state witness due to the Reed Solomon erasure encoding.
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
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
        let signature = signer.sign_partial_encoded_state_witness(&inner);
        Self { inner, signature }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.shard_id(),
            epoch_id: *self.epoch_id(),
            height_created: self.height_created(),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn epoch_id(&self) -> &EpochId {
        &self.inner.epoch_id
    }

    pub fn shard_id(&self) -> ShardId {
        self.inner.shard_id
    }

    pub fn height_created(&self) -> BlockHeight {
        self.inner.height_created
    }

    pub fn part_ord(&self) -> usize {
        self.inner.part_ord
    }

    pub fn part_size(&self) -> usize {
        self.inner.part.len()
    }

    /// Decomposes the partial witness to return (part_ord, part, encoded_length)
    pub fn decompose(self) -> (usize, Box<[u8]>, usize) {
        (self.inner.part_ord, self.inner.part, self.inner.encoded_length)
    }
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
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
