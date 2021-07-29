use crate::hash::CryptoHash;

/// Account identifier. Provides access to user's state.
pub type AccountId = String;
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
pub type Gas = u64;

/// Number of blocks in current group.
pub type NumBlocks = u64;
/// Number of shards in current group.
pub type NumShards = u64;
/// Number of seats of validators (block producer or hidden ones) in current group (settlement).
pub type NumSeats = u64;
/// Block height delta that measures the difference between `BlockHeight`s.
pub type BlockHeightDelta = u64;

pub type GCCount = u64;

pub type ReceiptIndex = usize;
pub type PromiseId = Vec<ReceiptIndex>;

pub type ProtocolVersion = u32;
