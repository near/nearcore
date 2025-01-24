use std::num::ParseIntError;
use std::ops::Add;
use std::str::FromStr;

use crate::hash::CryptoHash;

/// Account identifier. Provides access to user's state.
pub use crate::account::id::AccountId;
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
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;
/// Gas is a type for storing amount of gas.
pub type Gas = u64;
/// Compute is a type for storing compute time. Measured in femtoseconds (10^-15 seconds).
pub type Compute = u64;

/// Weight of unused gas to distribute to scheduled function call actions.
/// Used in `promise_batch_action_function_call_weight` host function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GasWeight(pub u64);

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

/// The ShardIndex is the index of the shard in an array of shard data.
/// Historically the ShardId was always in the range 0..NUM_SHARDS and was used
/// as the shard index. This is no longer the case, and the ShardIndex should be
/// used instead.
pub type ShardIndex = usize;

/// The shard identifier. It may be an arbitrary number - it does not need to be
/// a number in the range 0..NUM_SHARDS. The shard ids do not need to be
/// sequential or contiguous.
///
/// The shard id is wrapped in a new type to prevent the old pattern of using
/// indices in range 0..NUM_SHARDS and casting to ShardId. Once the transition
/// if fully complete it potentially may be simplified to a regular type alias.
#[derive(
    arbitrary::Arbitrary,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct ShardId(u64);

impl ShardId {
    /// Create a new shard id. Please note that this function should not be used
    /// to convert a shard index (a number in 0..num_shards range) to ShardId.
    /// Instead the ShardId should be obtained from the shard_layout.
    ///
    /// ```rust, ignore
    /// // BAD USAGE:
    /// for shard_index in 0..num_shards {
    ///     let shard_id = ShardId::new(shard_index); // Incorrect!!!
    /// }
    /// ```
    /// ```rust, ignore
    /// // GOOD USAGE 1:
    /// for shard_index in 0..num_shards {
    ///     let shard_id = shard_layout.get_shard_id(shard_index);
    /// }
    /// // GOOD USAGE 2:
    /// for shard_id in shard_layout.shard_ids() {
    ///     let shard_id = shard_layout.get_shard_id(shard_index);
    /// }
    /// ```
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    // TODO This is not great, in ShardUId shard_id is u32.
    // Currently used for some metrics so kinda ok.
    pub fn max() -> Self {
        Self(u64::MAX)
    }
}

impl std::fmt::Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for ShardId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl Into<u64> for ShardId {
    fn into(self) -> u64 {
        self.0
    }
}

impl From<u32> for ShardId {
    fn from(id: u32) -> Self {
        Self(id as u64)
    }
}

impl Into<u32> for ShardId {
    fn into(self) -> u32 {
        self.0 as u32
    }
}

impl From<i32> for ShardId {
    fn from(id: i32) -> Self {
        Self(id as u64)
    }
}

impl From<usize> for ShardId {
    fn from(id: usize) -> Self {
        Self(id as u64)
    }
}

impl From<u16> for ShardId {
    fn from(id: u16) -> Self {
        Self(id as u64)
    }
}

impl Into<u16> for ShardId {
    fn into(self) -> u16 {
        self.0 as u16
    }
}

impl Into<usize> for ShardId {
    fn into(self) -> usize {
        self.0 as usize
    }
}

impl<T> Add<T> for ShardId
where
    T: Add<u64, Output = u64>,
{
    type Output = Self;

    fn add(self, rhs: T) -> Self::Output {
        Self(T::add(rhs, self.0))
    }
}

impl PartialEq<u64> for ShardId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl FromStr for ShardId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let shard_id = s.parse::<u64>()?;
        Ok(ShardId(shard_id))
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ShardId;

    // Check that the ShardId is serialized the same as u64. This is to make
    // sure that the transition from ShardId being a type alias to being a
    // new type is not a protocol upgrade.
    #[test]
    fn test_shard_id_borsh() {
        let shard_id_u64 = 42;
        let shard_id = ShardId::new(shard_id_u64);

        assert_eq!(borsh::to_vec(&shard_id_u64).unwrap(), borsh::to_vec(&shard_id).unwrap());
    }
}
