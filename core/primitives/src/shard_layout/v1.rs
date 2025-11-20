use crate::shard_layout::{ShardLayoutError, ShardVersion, ShardsSplitMap};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{NumShards, ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

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

fn derive_to_parent_shard_map(
    shards_split_map: &ShardsSplitMap,
    num_shards: NumShards,
) -> Vec<ShardId> {
    let mut to_parent_shard_map = BTreeMap::new();
    for (parent_shard_id, shard_ids) in shards_split_map.iter().enumerate() {
        let parent_shard_id = ShardId::new(parent_shard_id as u64);
        for &shard_id in shard_ids {
            let prev = to_parent_shard_map.insert(shard_id, parent_shard_id);
            assert!(prev.is_none(), "no shard should appear in the map twice");
            let shard_id: u64 = shard_id.into();
            assert!(shard_id < num_shards, "shard id should be valid");
        }
    }
    (0..num_shards).map(|shard_id| to_parent_shard_map[&shard_id.into()]).collect()
}

impl ShardLayoutV1 {
    pub fn new(
        boundary_accounts: Vec<AccountId>,
        shards_split_map: Option<ShardsSplitMap>,
        version: ShardVersion,
    ) -> Self {
        let to_parent_shard_map = shards_split_map.as_ref().map(|shards_split_map| {
            let num_shards = (boundary_accounts.len() + 1) as NumShards;
            derive_to_parent_shard_map(shards_split_map, num_shards)
        });
        Self { boundary_accounts, shards_split_map, to_parent_shard_map, version }
    }

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

    pub fn get_children_shards_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        match &self.shards_split_map {
            Some(shards_split_map) => {
                // In V1 the shard id and the shard index are the same. It
                // is ok to cast the id to index here. The same is not the
                // case in V2.
                let parent_shard_index: ShardIndex = parent_shard_id.into();
                shards_split_map.get(parent_shard_index).cloned()
            }
            None => None,
        }
    }

    pub fn num_shards(&self) -> usize {
        self.boundary_accounts.len() + 1
    }
    pub fn try_get_parent_shard_id(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardId>, ShardLayoutError> {
        let shard_index: usize = shard_id.into();
        if shard_index >= self.num_shards() {
            return Err(ShardLayoutError::InvalidShardIdError { shard_id });
        }
        match &self.to_parent_shard_map {
            // we can safely unwrap here because the construction of to_parent_shard_map guarantees
            // that every shard has a parent shard
            Some(to_parent_shard_map) => {
                let parent_shard_id = to_parent_shard_map.get(shard_index).unwrap();
                Ok(Some(*parent_shard_id))
            }
            None => Ok(None),
        }
    }

    pub fn get_shard_id(&self, shard_index: ShardIndex) -> Result<ShardId, ShardLayoutError> {
        if shard_index >= self.num_shards() {
            Err(ShardLayoutError::InvalidShardIndexError { shard_index })
        } else {
            Ok(ShardId::new(shard_index as u64))
        }
    }
}
