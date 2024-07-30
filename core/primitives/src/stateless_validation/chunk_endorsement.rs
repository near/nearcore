use std::fmt::Debug;

use crate::sharding::ChunkHash;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::AccountId;

use super::SignatureDifferentiator;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ChunkEndorsement {
    V1(ChunkEndorsementV1),
}

impl ChunkEndorsement {
    pub fn new(chunk_hash: ChunkHash, signer: &ValidatorSigner) -> ChunkEndorsement {
        ChunkEndorsement::V1(Self::new_v1(chunk_hash, signer))
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

/// This is the part of the chunk endorsement that is actually being signed.
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
