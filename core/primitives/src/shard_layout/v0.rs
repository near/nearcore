use crate::shard_layout::{ShardLayoutError, ShardVersion};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{NumShards, ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;

/// A shard layout that maps accounts evenly across all shards -- by calculate the hash of account
/// id and mod number of shards. This is added to capture the old `account_id_to_shard_id` algorithm,
/// to keep backward compatibility for some existing tests.
/// `parent_shards` for `ShardLayoutV1` is always `None`, meaning it can only be the first shard layout
/// a chain uses.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ShardLayoutV0 {
    /// Map accounts evenly across all shards
    pub(crate) num_shards: NumShards,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    pub(crate) version: ShardVersion,
}

impl ShardLayoutV0 {
    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let hash = CryptoHash::hash_bytes(account_id.as_bytes());
        let (bytes, _) = stdx::split_array::<32, 8, 24>(hash.as_bytes());
        let shard_id = u64::from_le_bytes(*bytes) % self.num_shards;
        shard_id.into()
    }
    pub fn get_shard_id(&self, shard_index: ShardIndex) -> Result<ShardId, ShardLayoutError> {
        if shard_index >= self.num_shards as usize {
            Err(ShardLayoutError::InvalidShardIndex { shard_index })
        } else {
            Ok(ShardId::new(shard_index as u64))
        }
    }
}
