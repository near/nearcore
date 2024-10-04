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

/// The shard identifier. It may be a arbitrary number - it does not need to be
/// a number in the range 0..NUM_SHARDS. The shard ids do not need to be
/// sequential or contiguous.
///
/// The shard id is wrapped in a newtype to prevent the old pattern of using
/// indices in range 0..NUM_SHARDS and casting to ShardId. Once the transition
/// if fully complete it potentially may be simplified to a regular type alias.
///
/// TODO get rid of serde
// #[derive(
//     arbitrary::Arbitrary,
//     borsh::BorshSerialize,
//     borsh::BorshDeserialize,
//     serde::Serialize,
//     serde::Deserialize,
//     Hash,
//     Clone,
//     Copy,
//     Debug,
//     PartialEq,
//     Eq,
//     PartialOrd,
//     Ord,
// )]
// pub struct ShardId(u64);

pub type ShardId = u64;

/// The ShardIndex is the index of the shard in an array of shard data.
/// Historically the ShardId was always in the range 0..NUM_SHARDS and was used
/// as the shard index. This is no longer the case, and the ShardIndex should be
/// used instead.
pub type ShardIndex = usize;

// TODO(wacban) This is a temporary solution to aid the transition to having
// ShardId as a newtype. It should be removed / inlined once the transition
// is complete.
pub const fn new_shard_id_tmp(id: u64) -> ShardId {
    id
}

// TODO(wacban) This is a temporary solution to aid the transition to having
// ShardId as a newtype. It should be removed / inlined once the transition
// is complete.
pub fn new_shard_id_vec_tmp(vec: &[u64]) -> Vec<ShardId> {
    vec.iter().copied().map(new_shard_id_tmp).collect()
}

// TODO(wacban) This is a temporary solution to aid the transition to having
// ShardId as a newtype. It should be removed / inlined once the transition
// is complete.
pub const fn shard_id_as_u32(id: ShardId) -> u32 {
    id as u32
}

// impl ShardId {
//     /// Create a new shard id. Please note that this function should not be used
//     /// directly. Instead the ShardId should be obtained from the shard_layout.
//     pub const fn new(id: u64) -> Self {
//         Self(id)
//     }

//     /// Get the numerical value of the shard id. This should not be used as an
//     /// index into an array, as the shard id may be any arbitrary number.
//     pub fn get(self) -> u64 {
//         self.0
//     }

//     pub fn to_le_bytes(self) -> [u8; 8] {
//         self.0.to_le_bytes()
//     }

//     pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
//         Self(u64::from_le_bytes(bytes))
//     }

//     // TODO This is not great, in ShardUId shard_id is u32.
//     // Currently used for some metrics so kinda ok.
//     pub fn max() -> Self {
//         Self(u64::MAX)
//     }
// }

// impl Display for ShardId {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// impl From<u64> for ShardId {
//     fn from(id: u64) -> Self {
//         Self(id)
//     }
// }

// impl Into<u64> for ShardId {
//     fn into(self) -> u64 {
//         self.0
//     }
// }

// impl From<u32> for ShardId {
//     fn from(id: u32) -> Self {
//         Self(id as u64)
//     }
// }

// impl Into<u32> for ShardId {
//     fn into(self) -> u32 {
//         self.0 as u32
//     }
// }

// impl From<i32> for ShardId {
//     fn from(id: i32) -> Self {
//         Self(id as u64)
//     }
// }

// impl From<usize> for ShardId {
//     fn from(id: usize) -> Self {
//         Self(id as u64)
//     }
// }

// impl From<u16> for ShardId {
//     fn from(id: u16) -> Self {
//         Self(id as u64)
//     }
// }

// impl Into<u16> for ShardId {
//     fn into(self) -> u16 {
//         self.0 as u16
//     }
// }
