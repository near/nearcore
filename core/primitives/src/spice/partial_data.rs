use crate::merkle::MerklePath;
use crate::types::StaticSignatureDifferentiator;
use crate::validator_signer::ValidatorSigner;
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, MerkleHash, ShardId};

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum SpiceDataIdentifier {
    ReceiptProof { block_hash: CryptoHash, from_shard_id: ShardId, to_shard_id: ShardId },
    Witness { block_hash: CryptoHash, shard_id: ShardId },
}

impl SpiceDataIdentifier {
    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            SpiceDataIdentifier::ReceiptProof { block_hash, .. } => block_hash,
            SpiceDataIdentifier::Witness { block_hash, .. } => block_hash,
        }
    }
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpiceDataCommitment {
    pub hash: CryptoHash,
    pub root: MerkleHash,
    pub encoded_length: u64,
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SpiceDataPart {
    pub part_ord: u64,
    pub part: Box<[u8]>,
    pub merkle_proof: MerklePath,
}

/// Partial data with unverified signature.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpicePartialData {
    V1(SpicePartialDataV1) = 0,
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SpicePartialDataV1 {
    inner: SpicePartialDataInner,
    sender: AccountId,
    signature: Signature,
}

impl SpicePartialData {
    pub fn new(
        id: SpiceDataIdentifier,
        commitment: SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        signer: &ValidatorSigner,
    ) -> Self {
        let inner = SpicePartialDataInner { id, commitment, parts };
        let signature = signer.sign_bytes(&inner.serialize_for_signing());
        Self::V1(SpicePartialDataV1 { inner, signature, sender: signer.validator_id().clone() })
    }

    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(v1) => v1.inner.id.block_hash(),
        }
    }

    pub fn sender(&self) -> &AccountId {
        match self {
            Self::V1(v1) => &v1.sender,
        }
    }

    pub fn into_verified(self, public_key: &PublicKey) -> Option<SpiceVerifiedPartialData> {
        match self {
            Self::V1(v1) => {
                let data = v1.inner.serialize_for_signing();
                if !v1.signature.verify(&data, public_key) {
                    return None;
                }
                Some(SpiceVerifiedPartialData {
                    id: v1.inner.id,
                    commitment: v1.inner.commitment,
                    parts: v1.inner.parts,
                    sender: v1.sender,
                })
            }
        }
    }
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq)]
struct SpicePartialDataInner {
    // We include id to allow finding recipients and producers when receiving the data.
    id: SpiceDataIdentifier,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
}

impl SpicePartialDataInner {
    fn serialize_for_signing(&self) -> Vec<u8> {
        static SIGNATURE_DIFFERENTIATOR: StaticSignatureDifferentiator = "SpicePartialData";
        let data = (self, SIGNATURE_DIFFERENTIATOR);
        borsh::to_vec(&data).unwrap()
    }
}

/// Spice partial data with verified signature.
#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SpiceVerifiedPartialData {
    pub id: SpiceDataIdentifier,
    pub commitment: SpiceDataCommitment,
    pub parts: Vec<SpiceDataPart>,
    pub sender: AccountId,
}

// Outside of tests it should be impossible to create spice partial data with invalid signature.
pub fn testonly_create_spice_partial_data(
    id: SpiceDataIdentifier,
    commitment: SpiceDataCommitment,
    parts: Vec<SpiceDataPart>,
    signature: Signature,
    sender: AccountId,
) -> SpicePartialData {
    SpicePartialData::V1(SpicePartialDataV1 {
        inner: SpicePartialDataInner { id, commitment, parts },
        signature,
        sender,
    })
}
