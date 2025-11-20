use crate::shard_layout::ShardVersion;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::NumShards;
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
