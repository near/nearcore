use std::convert::TryFrom;

use near_protos::types as types_proto;

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
/// Authority identifier in current group.
pub type AuthorityId = usize;
/// Mask which authorities participated in multi sign.
pub type AuthorityMask = Vec<bool>;
/// Part of the signature.
pub type PartialSignature = BlsSignature;
/// Monetary balance of an account or an amount for transfer.
pub type Balance = u64;
/// StorageUsage is used to count the amount of storage used by a contract.
pub type StorageUsage = u64;
/// StorageUsageChange is used to count the storage usage within a single contract call.
pub type StorageUsageChange = i64;
/// Nonce for transactions.
pub type Nonce = u64;

pub type BlockIndex = u64;

pub type ShardId = u32;

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

/// epoch for authority
pub type Epoch = u64;
/// slot for authority
pub type Slot = u64;

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
    /// Stake / weight of the authority.
    pub amount: Balance,
}

impl ValidatorStake {
    pub fn new(account_id: AccountId, public_key: PublicKey, amount: Balance) -> Self {
        ValidatorStake { account_id, public_key, amount }
    }
}

impl TryFrom<types_proto::ValidatorStake> for ValidatorStake {
    type Error = Box<std::error::Error>;

    fn try_from(proto: types_proto::ValidatorStake) -> Result<Self, Self::Error> {
        Ok(ValidatorStake {
            account_id: proto.account_id,
            public_key: PublicKey::try_from(proto.public_key.as_str())?,
            amount: proto.amount,
        })
    }
}

impl From<ValidatorStake> for types_proto::ValidatorStake {
    fn from(authority: ValidatorStake) -> Self {
        types_proto::ValidatorStake {
            account_id: authority.account_id,
            public_key: authority.public_key.to_string(),
            amount: authority.amount,
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
