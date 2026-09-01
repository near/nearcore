use super::ChunkProductionKey;
use crate::sharding::{ChunkHash, ShardChunkHeader};
use crate::types::{EpochId, SignatureDifferentiator};
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::{AccountId, BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ChunkEndorsement {
    // V1 was deprecated
    V2(ChunkEndorsementV2) = 1,
}

impl ChunkEndorsement {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
    ) -> ChunkEndorsement {
        let (metadata, metadata_signature) = signed_metadata(epoch_id, chunk_header, signer);
        let inner = ChunkEndorsementInnerV1::new(chunk_header.chunk_hash().clone());
        let signature = signer.sign_bytes(&endorsement_signed_bytes(&inner.chunk_hash));
        let endorsement = ChunkEndorsementV2 { inner, signature, metadata, metadata_signature };
        ChunkEndorsement::V2(endorsement)
    }

    /// Test-only constructor that puts a caller-chosen `signature_differentiator` on the wire and
    /// signs the resulting inner verbatim.
    ///
    /// It must not sign through `endorsement_signed_bytes`: that would produce a canonical
    /// signature for a non-canonical inner, and the regression tests built on this constructor
    /// would pass even with the fix reverted.
    #[cfg(any(test, feature = "test_features"))]
    pub fn new_with_differentiator(
        epoch_id: EpochId,
        chunk_header: &ShardChunkHeader,
        signer: &ValidatorSigner,
        signature_differentiator: SignatureDifferentiator,
    ) -> ChunkEndorsement {
        let (metadata, metadata_signature) = signed_metadata(epoch_id, chunk_header, signer);
        let inner = ChunkEndorsementInnerV1 {
            chunk_hash: chunk_header.chunk_hash().clone(),
            signature_differentiator,
        };
        let signature = signer.sign_bytes(&borsh::to_vec(&inner).unwrap());
        let endorsement = ChunkEndorsementV2 { inner, signature, metadata, metadata_signature };
        ChunkEndorsement::V2(endorsement)
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            ChunkEndorsement::V2(v2) => ChunkProductionKey {
                shard_id: v2.metadata.shard_id,
                epoch_id: v2.metadata.epoch_id,
                height_created: v2.metadata.height_created,
            },
        }
    }

    pub fn account_id(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
        }
    }

    pub fn chunk_hash(&self) -> ChunkHash {
        match self {
            ChunkEndorsement::V2(v2) => v2.inner.chunk_hash.clone(),
        }
    }

    pub fn signature(&self) -> Signature {
        match self {
            ChunkEndorsement::V2(v2) => v2.signature.clone(),
        }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        match self {
            ChunkEndorsement::V2(v2) => v2.verify(public_key),
        }
    }

    pub fn validate_signature(
        chunk_hash: ChunkHash,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        signature.verify(&endorsement_signed_bytes(&chunk_hash), public_key)
    }

    /// Returns the account ID of the chunk validator that generated this endorsement.
    pub fn validator_account(&self) -> &AccountId {
        match self {
            ChunkEndorsement::V2(v2) => &v2.metadata.account_id,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        match self {
            ChunkEndorsement::V2(v2) => v2.metadata.shard_id,
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
        // Verify against bytes reconstructed from `chunk_hash`, never against the wire-supplied
        // `inner`. Serializing `self.inner` here would let a peer pick the signed bytes through
        // `signature_differentiator`, which the consensus path never reads, so an endorsement
        // could pass this check and fail `validate_signature` later.
        let metadata = borsh::to_vec(&self.metadata).unwrap();
        self.signature.verify(&endorsement_signed_bytes(&self.inner.chunk_hash), public_key)
            && self.metadata_signature.verify(&metadata, public_key)
    }
}

/// The bytes a chunk endorsement signature is taken over, derived from `chunk_hash` alone.
///
/// Single source of truth for both the receive path ([`ChunkEndorsementV2::verify`]) and the
/// consensus path ([`ChunkEndorsement::validate_signature`]), so the two can never disagree on
/// what was signed.
fn endorsement_signed_bytes(chunk_hash: &ChunkHash) -> Vec<u8> {
    borsh::to_vec(&ChunkEndorsementInnerV1::new(chunk_hash.clone())).unwrap()
}

/// Builds the endorsement metadata and its signature. Shared by the production and test-only
/// constructors, which deliberately do not share the `inner` signing step.
fn signed_metadata(
    epoch_id: EpochId,
    chunk_header: &ShardChunkHeader,
    signer: &ValidatorSigner,
) -> (ChunkEndorsementMetadata, Signature) {
    let metadata = ChunkEndorsementMetadata {
        account_id: signer.validator_id().clone(),
        shard_id: chunk_header.shard_id(),
        epoch_id,
        height_created: chunk_header.height_created(),
    };
    let metadata_signature = signer.sign_bytes(&borsh::to_vec(&metadata).unwrap());
    (metadata, metadata_signature)
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
    /// Kept on the wire for borsh compatibility, but never read back from it: every signature
    /// check reconstructs the canonical value through `endorsement_signed_bytes`.
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementInnerV1 {
    fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::CryptoHash;
    use crate::test_utils::create_test_signer;
    use crate::version::PROTOCOL_VERSION;
    use near_primitives_core::types::ShardId;

    /// An endorsement carrying a non-canonical `signature_differentiator` must be rejected by both
    /// the receive path (`verify`) and the consensus path (`validate_signature`). `verify` used to
    /// sign-check the wire `inner`, so it accepted such an endorsement while `validate_signature`,
    /// which rebuilds the inner from `chunk_hash`, rejected it.
    #[test]
    fn poisoned_differentiator_is_rejected_by_verify() {
        let signer = create_test_signer("test0");
        let chunk_header = ShardChunkHeader::new_dummy(
            42,
            ShardId::new(0),
            CryptoHash::default(),
            PROTOCOL_VERSION,
        );

        let endorsement = ChunkEndorsement::new_with_differentiator(
            EpochId::default(),
            &chunk_header,
            &signer,
            "POISONED".to_owned(),
        );

        assert!(
            !endorsement.verify(&signer.public_key()),
            "verify() must reject a non-canonical signature_differentiator"
        );
        assert!(
            !ChunkEndorsement::validate_signature(
                chunk_header.chunk_hash().clone(),
                &endorsement.signature(),
                &signer.public_key(),
            ),
            "validate_signature() must reject a non-canonical signature_differentiator"
        );

        // Control: a canonical endorsement passes both paths.
        let honest = ChunkEndorsement::new(EpochId::default(), &chunk_header, &signer);
        assert!(honest.verify(&signer.public_key()));
        assert!(ChunkEndorsement::validate_signature(
            chunk_header.chunk_hash().clone(),
            &honest.signature(),
            &signer.public_key(),
        ));
    }

    /// Pins the exact bytes every endorsement signature is taken over. Changing them silently
    /// would invalidate signatures across a version boundary, so this must fail loudly.
    #[test]
    fn signed_bytes_are_stable() {
        assert_eq!(
            hex::encode(endorsement_signed_bytes(&ChunkHash(CryptoHash::default()))),
            "0000000000000000000000000000000000000000000000000000000000000000\
             100000004368756e6b456e646f7273656d656e74",
        );
    }
}
