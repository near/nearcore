use crate::borsh::maybestd::io::Cursor;
use crate::types::{AccountId, NumShards};
use byteorder::{LittleEndian, ReadBytesExt};
use near_primitives_core::hash::hash;
use near_primitives_core::types::ShardId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering::Greater;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShardLayout {
    V0(ShardLayoutV0),
    V1(ShardLayoutV1),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV0 {
    /// Map accounts evenly across all shards
    num_shards: NumShards,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
    pub fn v0(num_shards: NumShards) -> Self {
        Self::V0(ShardLayoutV0 { num_shards })
    }

    pub fn v1(
        fixed_shards: Vec<AccountId>,
        boundary_accounts: Vec<AccountId>,
        parent_shards: Option<Vec<ShardId>>,
    ) -> Self {
        Self::V1(ShardLayoutV1 { fixed_shards, boundary_accounts, parent_shards })
    }

    #[inline]
    pub fn parent_shards(&self) -> Option<&Vec<ShardId>> {
        match self {
            Self::V0(_) => None,
            Self::V1(v1) => v1.parent_shards.as_ref(),
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

/// Maps account_id to shard_id given a shard_layout
/// For V0, maps according to hash of account id
/// For V1, accounts are divided to ranges, each range of account is mapped to a shard.
/// There are also some fixed shards, each of which is mapped to an account and all subaccounts.
///     For example, for ShardLayoutV1{ fixed_shards: ["aurora"], boundary_accounts: ["near"]}
///     Account "aurora" and all its subaccounts will be mapped to shard_id 0.
///     For the rest of accounts, accounts <= "near" will be mapped to shard_id 1 and
///     accounts > "near" will be mapped shard_id 2.
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

fn is_top_level_account(top_account: &AccountId, account: &AccountId) -> bool {
    match account.strip_suffix(top_account) {
        None => false,
        Some(rest) => rest.is_empty() || rest.ends_with("."),
    }
}
#[cfg(test)]
mod tests {
    use crate::shard_layout::{account_id_to_shard_id, ShardLayout};
    use crate::types::AccountId;
    use std::collections::HashMap;

    #[test]
    fn test_account_id_to_shard_id_v0() {
        let num_shards = 4;
        let shard_layout = ShardLayout::v0(num_shards);
        let mut shard_id_distribution: HashMap<_, _> =
            (0..num_shards).map(|x| (x, 0)).into_iter().collect();
        for i in 1..=1000 {
            let account_id = String::from_utf8(vec![b'a'; i]).unwrap() as AccountId;
            let shard_id = account_id_to_shard_id(&account_id, &shard_layout);
            assert!(shard_id < num_shards);
            *shard_id_distribution.get_mut(&shard_id).unwrap() += 1;
        }
        let expected_distribution: HashMap<_, _> =
            vec![(0, 237), (1, 253), (2, 256), (3, 254)].into_iter().collect();
        assert_eq!(shard_id_distribution, expected_distribution);
    }

    #[test]
    fn test_account_id_to_shard_id_v1() {
        let shard_layout = ShardLayout::v1(
            vec!["aurora", "bar", "foo", "foo.baz"].into_iter().map(|s| String::from(s)).collect(),
            vec!["abc", "foo", "paz"].into_iter().map(|s| String::from(s)).collect(),
            None,
        );
        assert_eq!(account_id_to_shard_id(&AccountId::from("aurora"), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&AccountId::from("foo.aurora"), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&AccountId::from("bar.foo.aurora"), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&AccountId::from("bar"), &shard_layout), 1);
        assert_eq!(account_id_to_shard_id(&AccountId::from("bar.bar"), &shard_layout), 1);
        assert_eq!(account_id_to_shard_id(&AccountId::from("foo"), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&AccountId::from("baz.foo"), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&AccountId::from("foo.baz"), &shard_layout), 3);
        assert_eq!(account_id_to_shard_id(&AccountId::from("a.foo.baz"), &shard_layout), 3);

        assert_eq!(account_id_to_shard_id(&AccountId::from("a"), &shard_layout), 4);
        assert_eq!(account_id_to_shard_id(&AccountId::from("abc"), &shard_layout), 5);
        assert_eq!(account_id_to_shard_id(&AccountId::from("b"), &shard_layout), 5);
        assert_eq!(account_id_to_shard_id(&AccountId::from("foo.goo"), &shard_layout), 6);
        assert_eq!(account_id_to_shard_id(&AccountId::from("goo"), &shard_layout), 6);
        assert_eq!(account_id_to_shard_id(&AccountId::from("zoo"), &shard_layout), 7);
    }
}
