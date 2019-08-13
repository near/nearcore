use std::convert::TryFrom;

use protobuf::SingularPtrField;

use near_protos::types as types_proto;

// pub use crate::balance::Balance;
use crate::crypto::aggregate_signature::BlsSignature;
use crate::crypto::signature::{PublicKey, Signature};
use crate::hash::CryptoHash;
use crate::serialize::u128_dec_format;
use crate::utils::proto_to_type;

/// Public key alias. Used to human readable public key.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct ReadablePublicKey(pub String);
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct ReadableBlsPublicKey(pub String);
/// Account identifier. Provides access to user's state.
pub type AccountId = String;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = Signature;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Validator identifier in current group.
pub type ValidatorId = usize;
/// Mask which validators participated in multi sign.
pub type ValidatorMask = Vec<bool>;
/// Part of the signature.
pub type PartialSignature = BlsSignature;
/// StorageUsage is used to count the amount of storage used by a contract.
pub type StorageUsage = u64;
/// StorageUsageChange is used to count the storage usage within a single contract call.
pub type StorageUsageChange = i64;
/// Nonce for transactions.
pub type Nonce = u64;
/// Index of the block.
pub type BlockIndex = u64;
/// Shard index, from 0 to NUM_SHARDS - 1.
pub type ShardId = u64;
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;
/// Gas is a type for storing amount of gas.
pub type Gas = u64;

pub type ReceiptIndex = usize;
pub type PromiseId = Vec<ReceiptIndex>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Hash, Clone)]
pub enum BlockId {
    Best,
    Number(BlockIndex),
    Hash(CryptoHash),
}

/// Stores validator and its stake.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ValidatorStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
}

impl ValidatorStake {
    pub fn new(account_id: AccountId, public_key: PublicKey, amount: Balance) -> Self {
        ValidatorStake { account_id, public_key, amount }
    }
}

impl TryFrom<types_proto::ValidatorStake> for ValidatorStake {
    type Error = Box<dyn std::error::Error>;

    fn try_from(proto: types_proto::ValidatorStake) -> Result<Self, Self::Error> {
        Ok(ValidatorStake {
            account_id: proto.account_id,
            public_key: proto_to_type(proto.public_key)?,
            amount: proto_to_type(proto.amount)?,
        })
    }
}

impl From<ValidatorStake> for types_proto::ValidatorStake {
    fn from(validator: ValidatorStake) -> Self {
        types_proto::ValidatorStake {
            account_id: validator.account_id,
            public_key: SingularPtrField::some(validator.public_key.into()),
            amount: SingularPtrField::some(validator.amount.into()),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl PartialEq for ValidatorStake {
    fn eq(&self, other: &Self) -> bool {
        self.account_id == other.account_id && self.public_key == other.public_key
    }
}

impl Eq for ValidatorStake {}

/// Data structure for semver version and github tag or commit.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
}
