use std::fmt::{Display, Formatter};

use borsh::{BorshDeserialize, BorshSerialize};
use serde;

use crate::hash::CryptoHash;

/// Account identifier. Provides access to user's state.
pub use crate::account::id::AccountId;
use crate::serialize::dec_format::DecType;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Validator identifier in current group.
pub type ValidatorId = u64;
/// Mask which validators participated in multi sign.
pub type ValidatorMask = Vec<bool>;
/// StorageUsage is used to count the amount of storage used by a contract.
pub type StorageUsage = u64;
/// StorageUsageChange is used to count the storage usage within a single contract call.
pub type StorageUsageChange = i64;
/// Nonce for transactions.
pub type Nonce = u64;
/// Height of the block.
pub type BlockHeight = u64;
/// Height of the epoch.
pub type EpochHeight = u64;
/// Shard index, from 0 to NUM_SHARDS - 1.
pub type ShardId = u64;
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;

/// Gas is a type for storing amount of gas.
#[derive(
    Debug,
    Default,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct Gas(u64);

impl Gas {
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Self::from)
    }

    pub fn wrapping_add(self, rhs: Self) -> Self {
        Self(self.0.wrapping_add(rhs.0))
    }

    pub fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Self::from)
    }

    pub fn wrapping_sub(self, rhs: Self) -> Self {
        Self(self.0.wrapping_sub(rhs.0))
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Gas {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl Display for Gas {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Gas: {}", self.0)
    }
}

impl DecType for Gas {
    fn serialize(&self) -> Option<String> {
        <u64 as DecType>::serialize(&self.0)
    }

    fn try_from_str(value: &str) -> Result<Self, std::num::ParseIntError> {
        let value = u64::try_from_str(value)?;
        Ok(Self::from(value))
    }

    fn from_u64(value: u64) -> Self {
        Self::from(value)
    }
}

/// Weight of unused gas to distribute to scheduled function call actions.
/// Used in `promise_batch_action_function_call_weight` host function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GasWeight(pub u64);

/// Result from a gas distribution among function calls with ratios.
#[must_use]
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum GasDistribution {
    /// All remaining gas was distributed to functions.
    All,
    /// There were no function call actions with a ratio specified.
    NoRatios,
}

/// Number of blocks in current group.
pub type NumBlocks = u64;
/// Number of shards in current group.
pub type NumShards = u64;
/// Number of seats of validators (block producer or hidden ones) in current group (settlement).
pub type NumSeats = u64;
/// Block height delta that measures the difference between `BlockHeight`s.
pub type BlockHeightDelta = u64;

pub type ReceiptIndex = usize;
pub type PromiseId = Vec<ReceiptIndex>;

pub type ProtocolVersion = u32;
