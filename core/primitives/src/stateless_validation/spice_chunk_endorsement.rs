use std::fmt::Debug;

use crate::block_body::SpiceCoreStatement;
use crate::types::{
    ChunkExecutionResult, ChunkExecutionResultHash, SpiceChunkId, StaticSignatureDifferentiator,
};
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::AccountId;
use near_schema_checker_lib::ProtocolSchema;

/// The endorsement of a chunk execution results by a chunk validator. By providing this, a
/// chunk validator has verified the chunk execution results.
/// This type intentionally has small amount to getters to make sure that callers verify signature
/// before accessing endorsement data.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpiceChunkEndorsement {
    V1(SpiceChunkEndorsementV1),
}

impl SpiceChunkEndorsement {
    pub fn new(
        chunk_id: SpiceChunkId,
        execution_result: ChunkExecutionResult,
        signer: &ValidatorSigner,
    ) -> SpiceChunkEndorsement {
        let signed_data = SpiceEndorsementSignedData {
            execution_result_hash: execution_result.compute_hash(),
            chunk_id,
        };
        let signature = signer.sign_bytes(&signed_data.serialize_data_for_signing());
        Self::V1(SpiceChunkEndorsementV1 {
            chunk_id: signed_data.chunk_id,
            account_id: signer.validator_id().clone(),
            signature,
            execution_result,
        })
    }

    pub fn account_id(&self) -> &AccountId {
        match self {
            Self::V1(v1) => &v1.account_id,
        }
    }

    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => &v1.chunk_id.block_hash,
        }
    }

    /// Checks signatures are returns SpiceVerifiedEndorsement if it's correct.
    /// NOTE: that it doesn't do any additional validations apart from checking signature.
    pub fn into_verified(self, public_key: &PublicKey) -> Option<SpiceVerifiedEndorsement> {
        match self {
            Self::V1(v1) => v1.into_verified(public_key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceChunkEndorsementV1 {
    execution_result: ChunkExecutionResult,
    chunk_id: SpiceChunkId,
    account_id: AccountId,
    // Signature of the SpiceEndorsementSignedData that can be derived from endorsement.
    signature: Signature,
}

impl SpiceChunkEndorsementV1 {
    fn into_verified(self, public_key: &PublicKey) -> Option<SpiceVerifiedEndorsement> {
        let data = &self.to_signed_data().serialize_data_for_signing();
        if !self.signature.verify(data, public_key) {
            return None;
        }
        Some(SpiceVerifiedEndorsement {
            execution_result: self.execution_result,
            chunk_id: self.chunk_id,
            account_id: self.account_id,
            signature: self.signature,
        })
    }

    fn to_signed_data(&self) -> SpiceEndorsementSignedData {
        SpiceEndorsementSignedData {
            execution_result_hash: self.execution_result.compute_hash(),
            chunk_id: self.chunk_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceVerifiedEndorsement {
    execution_result: ChunkExecutionResult,
    chunk_id: SpiceChunkId,
    account_id: AccountId,
    signature: Signature,
}

impl SpiceVerifiedEndorsement {
    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        &self.chunk_id
    }

    pub fn execution_result(&self) -> &ChunkExecutionResult {
        &self.execution_result
    }

    pub fn to_stored(&self) -> SpiceStoredVerifiedEndorsement {
        SpiceStoredVerifiedEndorsement {
            execution_result_hash: self.execution_result().compute_hash(),
            signature: self.signature.clone(),
        }
    }
}

/// Endorsement Core Statement that is included in blocks.
/// This type intentionally has small amount to getters to make sure that callers verify signature
/// before accessing endorsement data.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceEndorsementCoreStatement {
    account_id: AccountId,
    signature: Signature,
    signed_data: SpiceEndorsementSignedData,
}

impl SpiceEndorsementCoreStatement {
    pub fn chunk_id(&self) -> &SpiceChunkId {
        &self.signed_data.chunk_id
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn verified_signed_data(
        &self,
        public_key: &PublicKey,
    ) -> Option<(&SpiceEndorsementSignedData, &Signature)> {
        let data = &self.signed_data.serialize_data_for_signing();
        if !self.signature.verify(data, public_key) {
            return None;
        }
        Some((&self.signed_data, &self.signature))
    }

    /// Converts to SpiceStoredVerifiedEndorsement without signature verification.
    /// Caller should make sure that relevant core statement is validated.
    pub fn unchecked_to_stored(&self) -> SpiceStoredVerifiedEndorsement {
        SpiceStoredVerifiedEndorsement {
            execution_result_hash: self.signed_data.execution_result_hash.clone(),
            signature: self.signature.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema, Hash)]
pub struct SpiceEndorsementSignedData {
    pub execution_result_hash: ChunkExecutionResultHash,
    pub chunk_id: SpiceChunkId,
}

impl SpiceEndorsementSignedData {
    fn serialize_data_for_signing(&self) -> Vec<u8> {
        static SIGNATURE_DIFFERENTIATOR: StaticSignatureDifferentiator = "SpiceChunkEndorsement";
        let data_for_signing = (self, SIGNATURE_DIFFERENTIATOR);
        borsh::to_vec(&data_for_signing).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceStoredVerifiedEndorsement {
    pub execution_result_hash: ChunkExecutionResultHash,
    pub signature: Signature,
}

impl SpiceStoredVerifiedEndorsement {
    pub fn into_core_statement(
        self,
        chunk_id: SpiceChunkId,
        account_id: AccountId,
    ) -> SpiceCoreStatement {
        SpiceCoreStatement::Endorsement(SpiceEndorsementCoreStatement {
            account_id,
            signature: self.signature,
            signed_data: SpiceEndorsementSignedData {
                execution_result_hash: self.execution_result_hash,
                chunk_id,
            },
        })
    }
}

/// Outside of tests core statement should be created from endorsement after signature verification
/// which makes it not possible to create core statement with invalid signature.
pub fn testonly_create_endorsement_core_statement(
    account_id: AccountId,
    signature: Signature,
    signed_data: SpiceEndorsementSignedData,
) -> SpiceEndorsementCoreStatement {
    SpiceEndorsementCoreStatement { account_id, signature, signed_data }
}

/// Outside of tests it shouldn't be possible to create endorsement with invalid signatures.
pub fn testonly_create_chunk_endorsement(
    chunk_id: SpiceChunkId,
    account_id: AccountId,
    signature: Signature,
    execution_result: ChunkExecutionResult,
) -> SpiceChunkEndorsement {
    SpiceChunkEndorsement::V1(SpiceChunkEndorsementV1 {
        chunk_id,
        account_id,
        signature,
        execution_result,
    })
}

#[cfg(test)]
mod tests {
    use near_primitives_core::types::ShardId;

    use crate::test_utils::create_test_signer;
    use crate::types::chunk_extra::ChunkExtra;

    use super::*;

    #[test]
    fn test_created_endorsement_core_statement_signature_is_valid() {
        let signer = create_test_signer("account");
        let endorsement = new_endorsement(&signer);
        let verified = endorsement.into_verified(&signer.public_key()).unwrap();
        let core_statement = verified
            .to_stored()
            .into_core_statement(verified.chunk_id().clone(), signer.validator_id().clone());
        let SpiceCoreStatement::Endorsement(core_statement) = core_statement else { panic!() };
        assert!(core_statement.verified_signed_data(&signer.public_key()).is_some());
    }

    #[test]
    fn test_created_endorsement_signature_is_valid() {
        let signer = create_test_signer("account");
        let endorsement = new_endorsement(&signer);
        assert!(endorsement.into_verified(&signer.public_key()).is_some());
    }

    fn new_endorsement(signer: &ValidatorSigner) -> SpiceChunkEndorsement {
        SpiceChunkEndorsement::new(
            SpiceChunkId { block_hash: CryptoHash::default(), shard_id: ShardId::new(0) },
            ChunkExecutionResult {
                chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
                outgoing_receipts_root: CryptoHash::default(),
            },
            signer,
        )
    }
}
