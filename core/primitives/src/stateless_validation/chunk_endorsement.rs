use std::fmt::Debug;

use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::EpochId;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{AccountId, BlockHeight, ProtocolVersion, ShardId};
use near_primitives_core::version::ProtocolFeature;

use super::SignatureDifferentiator;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ChunkEndorsement {
    V1(ChunkEndorsementV1),
    V2(ChunkEndorsementV2),
}

impl ChunkEndorsement {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        signer: &ValidatorSigner,
        protocol_version: ProtocolVersion,
    ) -> ChunkEndorsement {
        let mut endorsement = if ProtocolFeature::ChunkEndorsementV2.enabled(protocol_version) {
            ChunkEndorsement::V2(ChunkEndorsementV2 {
                inner: ChunkEndorsementInner::V2(ChunkEndorsementV2Inner::new(
                    chunk_header.shard_id(),
                    epoch_id,
                    chunk_header.height_created(),
                )),
                account_id: signer.validator_id().clone(),
                signature: Signature::default(),
            })
        } else {
            ChunkEndorsement::V1(ChunkEndorsementV1 {
                inner: ChunkEndorsementV1Inner::new(chunk_header.chunk_hash()),
                account_id: signer.validator_id().clone(),
                signature: Signature::default(),
            })
        };
        let signature = signer.sign_chunk_endorsement(&endorsement);
        match &mut endorsement {
            ChunkEndorsement::V1(endorsement) => endorsement.signature = signature,
            ChunkEndorsement::V2(endorsement) => endorsement.signature = signature,
        }
        endorsement
    }

    pub fn new_v1(chunk_hash: ChunkHash, signer: &ValidatorSigner) -> ChunkEndorsementV1 {
        let mut endorsement = ChunkEndorsementV1 {
            inner: ChunkEndorsementV1Inner::new(chunk_hash),
            account_id: signer.validator_id().clone(),
            signature: Signature::default(),
        };
        let signature = signer.sign_chunk_endorsement(&ChunkEndorsement::V1(endorsement.clone()));
        endorsement.signature = signature;
        endorsement
    }

    // This function is used in validator_signer to get the correct serialized inner struct to sign.
    pub fn serialized_inner(&self) -> Vec<u8> {
        match self {
            ChunkEndorsement::V1(endorsement) => borsh::to_vec(&endorsement.inner).unwrap(),
            ChunkEndorsement::V2(endorsement) => borsh::to_vec(&endorsement.inner).unwrap(),
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
        let inner = ChunkEndorsementV1Inner::new(chunk_hash);
        let data = borsh::to_vec(&inner).unwrap();
        signature.verify(&data, public_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementV1 {
    inner: ChunkEndorsementV1Inner,
    pub account_id: AccountId,
    pub signature: Signature,
}

impl ChunkEndorsementV1 {
    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        &self.inner.chunk_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementV2 {
    inner: ChunkEndorsementInner,
    pub account_id: AccountId,
    pub signature: Signature,
}

/// This is the part of the chunk endorsement that is actually being signed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
enum ChunkEndorsementInner {
    V1(ChunkEndorsementV1Inner),
    V2(ChunkEndorsementV2Inner),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementV1Inner {
    chunk_hash: ChunkHash,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementV1Inner {
    fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct ChunkEndorsementV2Inner {
    shard_id: ShardId,
    epoch_id: EpochId,
    height_created: BlockHeight,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementV2Inner {
    fn new(shard_id: ShardId, epoch_id: EpochId, height_created: BlockHeight) -> Self {
        Self {
            shard_id,
            epoch_id,
            height_created,
            signature_differentiator: "ChunkEndorsementV2".to_owned(),
        }
    }
}
