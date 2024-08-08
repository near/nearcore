use std::fmt::Debug;

use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::EpochId;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{AccountId, BlockHeight, ProtocolVersion, ShardId};
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;

use super::SignatureDifferentiator;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkEndorsement {
    V1(ChunkEndorsementV1),
    V2(ChunkEndorsementV2),
}

impl ChunkEndorsement {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> ChunkEndorsement {
        if ProtocolFeature::ChunkEndorsementV2.enabled(protocol_version) {
            ChunkEndorsement::V2(ChunkEndorsementV2::new(epoch_id, chunk_header, signer))
        } else {
            ChunkEndorsement::V1(ChunkEndorsementV1::new(chunk_header.chunk_hash(), signer))
        }
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        match self {
            ChunkEndorsement::V1(endorsement) => &endorsement.inner.chunk_hash,
            ChunkEndorsement::V2(endorsement) => &endorsement.inner.chunk_hash,
        }
    }

    pub fn signature(&self) -> Signature {
        match self {
            ChunkEndorsement::V1(endorsement) => endorsement.signature.clone(),
            ChunkEndorsement::V2(endorsement) => endorsement.signature.clone(),
        }
    }

    pub fn validate_signature(
        chunk_hash: ChunkHash,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let inner = ChunkEndorsementInner::new(chunk_hash);
        let data = borsh::to_vec(&inner).unwrap();
        signature.verify(&data, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkEndorsementV1 {
    inner: ChunkEndorsementInner,
    pub account_id: AccountId,
    pub signature: Signature,
}

impl ChunkEndorsementV1 {
    pub fn new(chunk_hash: ChunkHash, signer: &ValidatorSigner) -> Self {
        let inner = ChunkEndorsementInner::new(chunk_hash);
        let account_id = signer.validator_id().clone();
        let signature = signer.sign_chunk_endorsement(&inner);
        Self { inner, account_id, signature }
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        &self.inner.chunk_hash
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementV2 {
    // This is the part of the chunk endorsement that signed and included in the block header
    inner: ChunkEndorsementInner,
    // This is the signature of the inner field, to be included in the block header
    signature: Signature,
    // This consists of the metadata for chunk endorsement used in validation
    metadata: ChunkEndorsementMetadata,
    // Metadata signature is used to validate that the metadata is produced by the expected validator
    metadata_signature: Signature,
}

impl ChunkEndorsementV2 {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = ChunkEndorsementInner::new(chunk_header.chunk_hash());
        let metadata = ChunkEndorsementMetadata::V1(MetadataV1 {
            account_id: signer.validator_id().clone(),
            shard_id: chunk_header.shard_id(),
            epoch_id,
            height_created: chunk_header.height_created(),
        });
        let signature = signer.sign_chunk_endorsement(&inner);
        let metadata_signature = signer.sign_chunk_endorsement_metadata(&metadata);
        Self { inner, signature, metadata, metadata_signature }
    }

    // TODO(ChunkEndorsementV2): Remove this once we implement tracker_v2
    pub fn into_v1(self) -> ChunkEndorsementV1 {
        ChunkEndorsementV1 {
            inner: self.inner,
            account_id: match self.metadata {
                ChunkEndorsementMetadata::V1(metadata) => metadata.account_id,
            },
            signature: self.signature,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ChunkEndorsementMetadata {
    V1(MetadataV1),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct MetadataV1 {
    account_id: AccountId,
    shard_id: ShardId,
    epoch_id: EpochId,
    height_created: BlockHeight,
}

/// This is the part of the chunk endorsement that is actually being signed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkEndorsementInner {
    chunk_hash: ChunkHash,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementInner {
    fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}
