use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::io::Cursor;
use std::ops::{Add, AddAssign, Div, Sub, SubAssign};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use protobuf::SingularPtrField;

use near_protos::types as types_proto;
use near_protos::uint128 as uint128_proto;

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

pub type ShardId = u32;

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

/// Epoch for rotating validators.
pub type Epoch = u64;

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

/// Monetary balance of an account or an amount for transfer.
#[derive(Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash, Clone, Debug, Copy)]
pub struct Balance(pub u128);

impl AddAssign for Balance {
    fn add_assign(&mut self, rhs: Balance) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Balance {
    fn sub_assign(&mut self, rhs: Balance) {
        self.0 -= rhs.0;
    }
}

impl Add for Balance {
    type Output = Self;

    fn add(self, rhs: Balance) -> Self::Output {
        Balance(self.0 + rhs.0)
    }
}

impl Sub for Balance {
    type Output = Self;

    fn sub(self, rhs: Balance) -> Self::Output {
        Balance(self.0 - rhs.0)
    }
}

impl Div for Balance {
    type Output = Self;

    fn div(self, rhs: Balance) -> Self::Output {
        Balance(self.0 / rhs.0)
    }
}

impl<'a> std::iter::Sum<&'a Balance> for Balance {
    fn sum<I: Iterator<Item = &'a Balance>>(iter: I) -> Self {
        Balance(iter.map(|x| x.0).sum())
    }
}

impl From<Balance> for u128 {
    fn from(value: Balance) -> Self {
        value.0
    }
}

impl From<u128> for Balance {
    fn from(value: u128) -> Self {
        Balance(value)
    }
}

impl From<u64> for Balance {
    fn from(value: u64) -> Self {
        Balance(value as u128)
    }
}

impl From<u32> for Balance {
    fn from(value: u32) -> Self {
        Balance(value as u128)
    }
}

impl TryFrom<uint128_proto::Uint128> for Balance {
    type Error = Box<std::error::Error>;

    fn try_from(value: uint128_proto::Uint128) -> Result<Self, Self::Error> {
        if value.number.len() != 16 {
            return Err(format!(
                "uint128 proto has {} bytes, but expected 16.",
                value.number.len()
            )
            .into());
        }
        let mut rdr = Cursor::new(value.number);
        Ok(Balance(rdr.read_uint128::<LittleEndian>(16)?))
    }
}

impl TryFrom<SingularPtrField<uint128_proto::Uint128>> for Balance {
    type Error = Box<std::error::Error>;

    fn try_from(value: SingularPtrField<uint128_proto::Uint128>) -> Result<Self, Self::Error> {
        let t: Result<uint128_proto::Uint128, std::io::Error> = value
            .into_option()
            .ok_or(std::io::Error::new(std::io::ErrorKind::Other, "Missing bytes for uint128"));
        Balance::try_from(t?)
    }
}

impl From<Balance> for uint128_proto::Uint128 {
    fn from(value: Balance) -> Self {
        let mut p = uint128_proto::Uint128 {
            number: [0; 16].to_vec(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        };
        let mut wrt = Cursor::new(&mut p.number);
        wrt.write_uint128::<LittleEndian>(value.0, 16).expect("Must not happened");
        p
    }
}

impl From<Balance> for SingularPtrField<uint128_proto::Uint128> {
    fn from(value: Balance) -> Self {
        SingularPtrField::some(value.into())
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
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
    type Error = Box<std::error::Error>;

    fn try_from(proto: types_proto::ValidatorStake) -> Result<Self, Self::Error> {
        Ok(ValidatorStake {
            account_id: proto.account_id,
            public_key: PublicKey::try_from(proto.public_key.as_str())?,
            amount: proto.amount.try_into()?,
        })
    }
}

impl From<ValidatorStake> for types_proto::ValidatorStake {
    fn from(validator: ValidatorStake) -> Self {
        types_proto::ValidatorStake {
            account_id: validator.account_id,
            public_key: validator.public_key.to_string(),
            amount: validator.amount.into(),
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
