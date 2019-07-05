use std::convert::{TryFrom, TryInto};

use protobuf::SingularPtrField;

use near_protos::types as types_proto;

// pub use crate::balance::Balance;
use crate::crypto::aggregate_signature::BlsSignature;
use crate::crypto::signature::{PublicKey, Signature};
use crate::hash::CryptoHash;

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

pub type BlockIndex = u64;

pub type ShardId = u64;

pub type Balance = u128;

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum PromiseId {
    Receipt(ReceiptId),
    Callback(CallbackId),
    Joiner(Vec<ReceiptId>),
}

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
    /// ED25591 Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
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
            public_key: PublicKey::try_from(proto.public_key.as_str())?,
            amount: proto.amount.unwrap_or_default().try_into()?,
        })
    }
}

impl From<ValidatorStake> for types_proto::ValidatorStake {
    fn from(validator: ValidatorStake) -> Self {
        types_proto::ValidatorStake {
            account_id: validator.account_id,
            public_key: validator.public_key.to_string(),
            amount: SingularPtrField::some(validator.amount.into()),
            ..Default::default()
        }
    }
}

impl PartialEq for ValidatorStake {
    fn eq(&self, other: &Self) -> bool {
        self.account_id == other.account_id && self.public_key == other.public_key
    }
}

impl Eq for ValidatorStake {}
