use std::fmt::Debug;

use super::SignatureDifferentiator;
use crate::sharding::ChunkHash;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::AccountId;
use near_schema_checker_lib::ProtocolSchema;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkEndorsement {
    V1(ChunkEndorsementV1),
}

impl ChunkEndorsement {
    pub fn new(chunk_hash: ChunkHash, signer: &ValidatorSigner) -> ChunkEndorsement {
        ChunkEndorsement::V1(ChunkEndorsementV1::new(chunk_hash, signer))
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        match self {
            ChunkEndorsement::V1(endorsement) => endorsement.chunk_hash(),
        }
    }

    pub fn signature(&self) -> Signature {
        match self {
            ChunkEndorsement::V1(endorsement) => endorsement.signature.clone(),
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
    pub fn new(chunk_hash: ChunkHash, signer: &ValidatorSigner) -> ChunkEndorsementV1 {
        let inner = ChunkEndorsementInner::new(chunk_hash);
        let account_id = signer.validator_id().clone();
        let signature = signer.sign_chunk_endorsement(&inner);
        ChunkEndorsementV1 { inner, account_id, signature }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        &self.inner.chunk_hash
    }
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
