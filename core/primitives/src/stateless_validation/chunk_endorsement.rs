use std::fmt::Debug;

use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::{
    ChunkExecutionResult, ChunkExecutionResultHash, EpochId, SignatureDifferentiator,
};
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use super::ChunkProductionKey;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ChunkEndorsement {
    V1 = 0, // Deprecated
    V2(ChunkEndorsementV2) = 1,
    // Chunk endorsement for spice is larger since it contains execution results. To avoid
    // penalizing memory layout of the whole enum we use Box here. For more details see:
    // https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant
    V3(Box<SpiceChunkEndorsementV3>) = 2,
}

impl ChunkEndorsement {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
    ) -> ChunkEndorsement {
        let metadata = ChunkEndorsementMetadata {
            account_id: signer.validator_id().clone(),
            shard_id: chunk_header.shard_id(),
            epoch_id,
            height_created: chunk_header.height_created(),
        };
        let metadata_signature = signer.sign_bytes(&borsh::to_vec(&metadata).unwrap());
        let inner = ChunkEndorsementInnerV1::new(chunk_header.chunk_hash().clone());
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        let endorsement = ChunkEndorsementV2 { inner, signature, metadata, metadata_signature };
        ChunkEndorsement::V2(endorsement)
    }

    pub fn new_with_execution_result(
        epoch_id: EpochId,
        execution_result: ChunkExecutionResult,
        block_hash: CryptoHash,
        shard_id: ShardId,
        height: BlockHeight,
        signer: &ValidatorSigner,
    ) -> ChunkEndorsement {
        assert!(cfg!(feature = "protocol_feature_spice"));

        let metadata = ChunkEndorsementMetadata {
            account_id: signer.validator_id().clone(),
            shard_id,
            epoch_id,
            height_created: height,
        };
        let metadata_signature = signer.sign_bytes(&borsh::to_vec(&metadata).unwrap());
        let inner = SpiceChunkEndorsementInnerV2::new(block_hash, execution_result);
        let signed_inner = inner.to_signed_inner();
        let signature = signer.sign_bytes(&borsh::to_vec(&signed_inner).unwrap());
        let endorsement =
            SpiceChunkEndorsementV3 { inner, signature, metadata, metadata_signature };
        ChunkEndorsement::V3(Box::new(endorsement))
    }

    pub fn spice_destructure(
        self,
    ) -> Option<(
        ChunkProductionKey,
        AccountId,
        SpiceEndorsementSignedInner,
        ChunkExecutionResult,
        Signature,
    )> {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(_) => None,
            ChunkEndorsement::V3(v3) => Some((
                ChunkProductionKey {
                    shard_id: v3.metadata.shard_id,
                    epoch_id: v3.metadata.epoch_id,
                    height_created: v3.metadata.height_created,
                },
                v3.metadata.account_id,
                v3.inner.to_signed_inner(),
                v3.inner.execution_result,
                v3.signature,
            )),
        }
    }

    pub fn execution_result(&self) -> Option<&ChunkExecutionResult> {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(_) => None,
            ChunkEndorsement::V3(v3) => Some(&v3.inner.execution_result),
        }
    }

    pub fn block_hash(&self) -> Option<&CryptoHash> {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(_) => None,
            ChunkEndorsement::V3(v3) => Some(&v3.inner.block_hash),
        }
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => ChunkProductionKey {
                shard_id: v2.metadata.shard_id,
                epoch_id: v2.metadata.epoch_id,
                height_created: v2.metadata.height_created,
            },
            ChunkEndorsement::V3(v3) => ChunkProductionKey {
                shard_id: v3.metadata.shard_id,
                epoch_id: v3.metadata.epoch_id,
                height_created: v3.metadata.height_created,
            },
        }
    }

    pub fn account_id(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
            ChunkEndorsement::V3(v3) => &v3.metadata.account_id,
        }
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.inner.chunk_hash.clone(),
            ChunkEndorsement::V3(_) => ChunkHash::default(),
        }
    }

    pub fn signature(&self) -> Signature {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.signature.clone(),
            ChunkEndorsement::V3(v3) => v3.signature.clone(),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.verify(public_key),
            ChunkEndorsement::V3(v3) => v3.verify(public_key),
        }
    }

    pub fn validate_signature(
        chunk_hash: ChunkHash,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let inner = ChunkEndorsementInnerV1::new(chunk_hash);
        let data = borsh::to_vec(&inner).unwrap();
        signature.verify(&data, public_key)
    }

    /// Returns the account ID of the chunk validator that generated this endorsement.
    pub fn validator_account(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
            ChunkEndorsement::V3(v3) => &v3.metadata.account_id,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            ChunkEndorsement::V1 => unreachable!("V1 chunk endorsement is deprecated"),
            ChunkEndorsement::V2(v2) => v2.metadata.shard_id,
            ChunkEndorsement::V3(v3) => v3.metadata.shard_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkEndorsementV2 {
    // This is the part of the chunk endorsement that signed and included in the block header
    inner: ChunkEndorsementInnerV1,
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
struct ChunkEndorsementInnerV1 {
    chunk_hash: ChunkHash,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementInnerV1 {
    fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceChunkEndorsementV3 {
    // Part of the chunk endorsement from which signed part can be derived.
    inner: SpiceChunkEndorsementInnerV2,
    // Signature of the signed inner derivable from inner.
    signature: Signature,
    // This consists of the metadata for chunk endorsement used in validation
    // TODO(spice): In metadata include only SpiceChunkId and AccountId. With spice endorsement are
    // always for older existing blocks so we can derive all other information required for
    // processing of endorsements from SpiceChunkId.
    metadata: ChunkEndorsementMetadata,
    // Metadata signature is used to validate that the metadata is produced by the expected validator
    metadata_signature: Signature,
}

impl SpiceChunkEndorsementV3 {
    fn verify(&self, public_key: &PublicKey) -> bool {
        let signed_inner = &self.inner.to_signed_inner();
        let metadata = borsh::to_vec(&self.metadata).unwrap();
        signed_inner.verify(public_key, &self.signature)
            && self.metadata_signature.verify(&metadata, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct SpiceChunkEndorsementInnerV2 {
    // Hash of the block that contains the chunk with specified execution results.
    // TODO(spice): Move block hash to ChunkEndorsementMetadata
    block_hash: CryptoHash,
    execution_result: ChunkExecutionResult,
}

impl SpiceChunkEndorsementInnerV2 {
    fn new(block_hash: CryptoHash, execution_result: ChunkExecutionResult) -> Self {
        Self { block_hash, execution_result }
    }

    fn to_signed_inner(&self) -> SpiceEndorsementSignedInner {
        SpiceEndorsementSignedInner {
            block_hash: self.block_hash,
            execution_result_hash: self.execution_result.compute_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema, Hash)]
pub struct SpiceEndorsementSignedInner {
    // Hash of the block that contains the chunk with specified execution results.
    pub block_hash: CryptoHash,
    pub execution_result_hash: ChunkExecutionResultHash,
}

impl SpiceEndorsementSignedInner {
    pub fn verify(&self, public_key: &PublicKey, signature: &Signature) -> bool {
        let signed_inner = borsh::to_vec(&self).unwrap();
        signature.verify(&signed_inner, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema, Hash)]
pub struct SpiceEndorsementWithSignature {
    pub inner: SpiceEndorsementSignedInner,
    pub signature: Signature,
}
