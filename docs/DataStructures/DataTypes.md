# Data Types

## type CryptoHash = [u8; 32]

A sha256 or keccak256 hash.

## AccountId = String

Account identifier. Provides access to user's state.

## type MerkleHash = CryptoHash

Hash used by a struct implementing the Merkle tree.

## type ValidatorId = usize

Validator identifier in current group.

## type ValidatorMask = [bool]

Mask which validators participated in multi sign.

## type StorageUsage = u64

StorageUsage is used to count the amount of storage used by a contract.

## type StorageUsageChange = i64

StorageUsageChange is used to count the storage usage within a single contract call.

## type Nonce = u64

Nonce for transactions.

## type BlockIndex = u64

Index of the block.

## type ShardId = u64

Shard index, from 0 to NUM_SHARDS - 1.

## type Balance = u128

Balance is type for storing amounts of tokens.

## Gas = u64

Gas is a type for storing amount of gas.
