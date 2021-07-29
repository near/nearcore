use crate::borsh::maybestd::io::Cursor;
use crate::types::{AccountId, NumShards};
use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, ReadBytesExt};
use near_primitives_core::hash::hash;
use near_primitives_core::types::ShardId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering::Greater;

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShardLayout {
    V0(ShardLayoutV0),
    V1(ShardLayoutV1),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV0 {
    /// Map accounts evenly across all shards
    num_shards: NumShards,
    /// Parent shards for the shards, useful for constructing states for the shards.
    /// None for the genesis shard layout
    parent_shards: Option<Vec<ShardId>>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV1 {
    /// num_shards = fixed_shards.len() + boundary_accounts.len() + 1
    /// Each account and all subaccounts map to the shard of position in this array.
    fixed_shards: Vec<AccountId>,
    /// The rest are divided by boundary_accounts to ranges, each range is mapped to a shard
    boundary_accounts: Vec<AccountId>,
    /// Parent shards for the shards, useful for constructing states for the shards.
    /// None for the genesis shard layout
    parent_shards: Option<Vec<ShardId>>,
}

impl ShardLayout {
    pub fn default(num_shards: NumShards) -> Self {
        ShardLayout::V0(ShardLayoutV0 { num_shards, parent_shards: None })
    }
    #[inline]
    pub fn parent_shards(&self) -> &Option<Vec<ShardId>> {
        match self {
            Self::V0(v0) => &v0.parent_shards,
            Self::V1(v1) => &v1.parent_shards,
        }
    }

    #[inline]
    pub fn num_shards(&self) -> NumShards {
        match self {
            Self::V0(v0) => v0.num_shards,
            Self::V1(v1) => (v1.fixed_shards.len() + v1.boundary_accounts.len() + 1) as NumShards,
        }
    }
}

pub fn account_id_to_shard_id(account_id: &AccountId, shard_layout: &ShardLayout) -> ShardId {
    match shard_layout {
        ShardLayout::V0(ShardLayoutV0 { num_shards, .. }) => {
            let mut cursor = Cursor::new(hash(&account_id.clone().into_bytes()).0);
            cursor.read_u64::<LittleEndian>().expect("Must not happened") % (num_shards)
        }
        ShardLayout::V1(ShardLayoutV1 { fixed_shards, boundary_accounts, .. }) => {
            for (shard_id, fixed_account) in fixed_shards.iter().enumerate() {
                if is_top_level_account(fixed_account, account_id) {
                    return shard_id as ShardId;
                }
            }
            let mut shard_id = fixed_shards.len() as ShardId;
            for boundary_account in boundary_accounts {
                if boundary_account.cmp(account_id) == Greater {
                    break;
                }
                shard_id += 1;
            }
            shard_id
        }
    }
}

fn is_top_level_account(_top_account: &AccountId, _account: &AccountId) -> bool {
    false
}
