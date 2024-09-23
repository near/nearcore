use std::fmt::Debug;

use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::EpochId;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{AccountId, BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use super::{ChunkProductionKey, SignatureDifferentiator};

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkEndorsement {
    V1, // Deprecated
    V2(ChunkEndorsementV2),
}

impl ChunkEndorsement {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
    ) -> ChunkEndorsement {
        let inner = ChunkEndorsementInner::new(chunk_header.chunk_hash());
        let metadata = ChunkEndorsementMetadata {
            account_id: signer.validator_id().clone(),
            shard_id: chunk_header.shard_id(),
            epoch_id,
            height_created: chunk_header.height_created(),
        };
        let signature = signer.sign_chunk_endorsement(&inner);
        let metadata_signature = signer.sign_chunk_endorsement_metadata(&metadata);
        let endorsement = ChunkEndorsementV2 { inner, signature, metadata, metadata_signature };
        ChunkEndorsement::V2(endorsement)
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => ChunkProductionKey {
                shard_id: v2.metadata.shard_id,
                epoch_id: v2.metadata.epoch_id,
                height_created: v2.metadata.height_created,
            },
        }
    }

    pub fn account_id(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
        }
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.inner.chunk_hash.clone(),
        }
    }

    pub fn signature(&self) -> Signature {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.signature.clone(),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.verify(public_key),
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

    /// Returns the account ID of the chunk validator that generated this endorsement.
    pub fn validator_account(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
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
    fn verify(&self, public_key: &PublicKey) -> bool {
        let inner = borsh::to_vec(&self.inner).unwrap();
        let metadata = borsh::to_vec(&self.metadata).unwrap();
        self.signature.verify(&inner, public_key)
            && self.metadata_signature.verify(&metadata, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkEndorsementMetadata {
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
