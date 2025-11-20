use crate::shard_layout::{ShardVersion, ShardsSplitMap};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

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
pub struct ShardLayoutV1 {
    /// The boundary accounts are the accounts on boundaries between shards.
    /// Each shard contains a range of accounts from one boundary account to
    /// another - or the smallest or largest account possible. The total
    /// number of shards is equal to the number of boundary accounts plus 1.
    pub(crate) boundary_accounts: Vec<AccountId>,
    /// Maps shards from the last shard layout to shards that it splits to in this shard layout,
    /// Useful for constructing states for the shards.
    /// None for the genesis shard layout
    pub(crate) shards_split_map: Option<ShardsSplitMap>,
    /// Maps shard in this shard layout to their parent shard
    /// Since shard_ids always range from 0 to num_shards - 1, we use vec instead of a hashmap
    pub(crate) to_parent_shard_map: Option<Vec<ShardId>>,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    pub(crate) version: ShardVersion,
}

impl ShardLayoutV1 {
    // In this shard layout the accounts are divided into ranges, each range is
    // mapped to a shard. The shards are contiguous and start from 0.
    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let mut shard_id: u64 = 0;
        for boundary_account in &self.boundary_accounts {
            if account_id < boundary_account {
                break;
            }
            shard_id += 1;
        }
        shard_id.into()
    }
}
