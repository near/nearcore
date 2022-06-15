use std::cmp::Ordering::Greater;

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use near_primitives_core::hash::hash;
use near_primitives_core::types::ShardId;

use crate::borsh::maybestd::io::Cursor;
use crate::hash::CryptoHash;
use crate::types::{AccountId, NumShards};
use std::collections::HashMap;

/// This file implements two data structure `ShardLayout` and `ShardUId`
///
/// `ShardLayout`
/// A versioned struct that contains all information needed to assign accounts
/// to shards. Because of re-sharding, the chain may use different shard layout to
/// split shards at different times.
/// Currently, `ShardLayout` is stored as part of `EpochConfig`, which is generated each epoch
/// given the epoch protocol version.
/// In mainnet/testnet, we use two shard layouts since re-sharding has only happened once.
/// It is stored as part of genesis config, see default_simple_nightshade_shard_layout()
/// Below is an overview for some important functionalities of ShardLayout interface.
///
/// `version`
/// `ShardLayout` has a version number. The version number should increment as when sharding changes.
/// This guarantees the version number is unique across different shard layouts, which in turn guarantees
/// `ShardUId` is different across shards from different shard layouts, as `ShardUId` includes
/// `version` and `shard_id`
///
/// `get_parent_shard_id` and `get_split_shard_ids`
/// `ShardLayout` also includes information needed for splitting shards. In particular, it encodes
/// which shards from the previous shard layout split to which shards in the following shard layout.
/// If shard A in shard layout 0 splits to shard B and C in shard layout 1,
/// we call shard A the parent shard of shard B and C.
/// Note that a shard can only have one parent shard. For example, the following case will be prohibited,
/// a shard C in shard layout 1 contains accounts in both shard A and B in shard layout 0.
/// Parent/split shard information can be accessed through these two functions.
///
/// `account_id_to_shard_id`
///  Maps an account to the shard that it belongs to given a shard_layout
///
/// `ShardUId`
/// `ShardUId` is a unique representation for shards from different shard layouts.  
/// Comparing to `ShardId`, which is just an ordinal number ranging from 0 to NUM_SHARDS-1,
/// `ShardUId` provides a way to unique identify shards when shard layouts may change across epochs.
/// This is important because we store states indexed by shards in our database, so we need a
/// way to unique identify shard even when shards change across epochs.
/// Another difference between `ShardUId` and `ShardId` is that `ShardUId` should only exist in
/// a node's internal state while `ShardId` can be exposed to outside APIs and used in protocol
/// level information (for example, `ShardChunkHeader` contains `ShardId` instead of `ShardUId`)

pub type ShardVersion = u32;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ShardLayout {
    V0(ShardLayoutV0),
    V1(ShardLayoutV1),
}

/// A shard layout that maps accounts evenly across all shards -- by calculate the hash of account
/// id and mod number of shards. This is added to capture the old `account_id_to_shard_id` algorithm,
/// to keep backward compatibility for some existing tests.
/// `parent_shards` for `ShardLayoutV1` is always `None`, meaning it can only be the first shard layout
/// a chain uses.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV0 {
    /// Map accounts evenly across all shards
    num_shards: NumShards,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    version: ShardVersion,
}

/// A map that maps shards from the last shard layout to shards that it splits to in this shard layout.
/// Instead of using map, we just use a vec here because shard_id ranges from 0 to num_shards-1
/// For example, if a shard layout with only shard 0 splits into shards 0, 1, 2, 3, the ShardsSplitMap
/// will be `[[0, 1, 2, 3]]`
type ShardSplitMap = Vec<Vec<ShardId>>;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV1 {
    /// num_shards = fixed_shards.len() + boundary_accounts.len() + 1
    /// Each account and all sub-accounts map to the shard of position in this array.
    fixed_shards: Vec<AccountId>,
    /// The rest are divided by boundary_accounts to ranges, each range is mapped to a shard
    boundary_accounts: Vec<AccountId>,
    /// Maps shards from the last shard layout to shards that it splits to in this shard layout,
    /// Useful for constructing states for the shards.
    /// None for the genesis shard layout
    shards_split_map: Option<ShardSplitMap>,
    /// Maps shard in this shard layout to their parent shard
    /// Since shard_ids always range from 0 to num_shards - 1, we use vec instead of a hashmap
    to_parent_shard_map: Option<Vec<ShardId>>,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    version: ShardVersion,
}

#[derive(Debug)]
pub enum ShardLayoutError {
    InvalidShardIdError { shard_id: ShardId },
}

impl ShardLayout {
    /* Some constructors */
    pub fn v0_single_shard() -> Self {
        Self::v0(1, 0)
    }

    /// Return a V0 Shardlayout
    pub fn v0(num_shards: NumShards, version: ShardVersion) -> Self {
        Self::V0(ShardLayoutV0 { num_shards, version })
    }

    /// Return a V1 Shardlayout
    pub fn v1(
        fixed_shards: Vec<AccountId>,
        boundary_accounts: Vec<AccountId>,
        shards_split_map: Option<ShardSplitMap>,
        version: ShardVersion,
    ) -> Self {
        let to_parent_shard_map = if let Some(shards_split_map) = &shards_split_map {
            let mut to_parent_shard_map = HashMap::new();
            let num_shards = (fixed_shards.len() + boundary_accounts.len() + 1) as NumShards;
            for (parent_shard_id, shard_ids) in shards_split_map.iter().enumerate() {
                for &shard_id in shard_ids {
                    let prev = to_parent_shard_map.insert(shard_id, parent_shard_id as ShardId);
                    assert!(prev.is_none(), "no shard should appear in the map twice");
                    assert!(shard_id < num_shards, "shard id should be valid");
                }
            }
            Some((0..num_shards).map(|shard_id| to_parent_shard_map[&shard_id]).collect())
        } else {
            None
        };
        Self::V1(ShardLayoutV1 {
            fixed_shards,
            boundary_accounts,
            shards_split_map,
            to_parent_shard_map,
            version,
        })
    }

    /// Returns a V1 ShardLayout. It is only used in tests
    pub fn v1_test() -> Self {
        ShardLayout::v1(
            vec!["test0"].into_iter().map(|s| s.parse().unwrap()).collect(),
            vec!["abc", "foo"].into_iter().map(|s| s.parse().unwrap()).collect(),
            Some(vec![vec![0, 1, 2, 3]]),
            1,
        )
    }

    /// Given a parent shard id, return the shard uids for the shards in the current shard layout that
    /// are split from this parent shard. If this shard layout has no parent shard layout, return None
    pub fn get_split_shard_uids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardUId>> {
        self.get_split_shard_ids(parent_shard_id).map(|shards| {
            shards.into_iter().map(|id| ShardUId::from_shard_id_and_layout(id, self)).collect()
        })
    }

    /// Given a parent shard id, return the shard ids for the shards in the current shard layout that
    /// are split from this parent shard. If this shard layout has no parent shard layout, return None
    pub fn get_split_shard_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        match self {
            Self::V0(_) => None,
            Self::V1(v1) => match &v1.shards_split_map {
                Some(shards_split_map) => shards_split_map.get(parent_shard_id as usize).cloned(),
                None => None,
            },
        }
    }

    /// Return the parent shard id for a given shard in the shard layout
    /// Only calls this function for shard layout that has parent shard layouts
    /// Returns error if `shard_id` is an invalid shard id in the current layout
    /// Panics if `self` has no parent shard layout
    pub fn get_parent_shard_id(&self, shard_id: ShardId) -> Result<ShardId, ShardLayoutError> {
        if shard_id > self.num_shards() {
            return Err(ShardLayoutError::InvalidShardIdError { shard_id });
        }
        let parent_shard_id = match self {
            Self::V0(_) => panic!("shard layout has no parent shard"),
            Self::V1(v1) => match &v1.to_parent_shard_map {
                // we can safely unwrap here because the construction of to_parent_shard_map guarantees
                // that every shard has a parent shard
                Some(to_parent_shard_map) => *to_parent_shard_map.get(shard_id as usize).unwrap(),
                None => panic!("shard_layout has no parent shard"),
            },
        };
        Ok(parent_shard_id)
    }

    #[inline]
    pub fn version(&self) -> ShardVersion {
        match self {
            Self::V0(v0) => v0.version,
            Self::V1(v1) => v1.version,
        }
    }

    #[inline]
    pub fn num_shards(&self) -> NumShards {
        match self {
            Self::V0(v0) => v0.num_shards,
            Self::V1(v1) => (v1.fixed_shards.len() + v1.boundary_accounts.len() + 1) as NumShards,
        }
    }

    /// Returns shard uids for all shards in the shard layout
    pub fn get_shard_uids(&self) -> Vec<ShardUId> {
        (0..self.num_shards()).map(|x| ShardUId::from_shard_id_and_layout(x, self)).collect()
    }
}

/// Maps an account to the shard that it belongs to given a shard_layout
/// For V0, maps according to hash of account id
/// For V1, accounts are divided to ranges, each range of account is mapped to a shard.
/// There are also some fixed shards, each of which is mapped to an account and all sub-accounts.
///     For example, for ShardLayoutV1{ fixed_shards: ["aurora"], boundary_accounts: ["near"]}
///     Account "aurora" and all its sub-accounts will be mapped to shard_id 0.
///     For the rest of accounts, accounts <= "near" will be mapped to shard_id 1 and
///     accounts > "near" will be mapped shard_id 2.
pub fn account_id_to_shard_id(account_id: &AccountId, shard_layout: &ShardLayout) -> ShardId {
    match shard_layout {
        ShardLayout::V0(ShardLayoutV0 { num_shards, .. }) => {
            let mut cursor = Cursor::new(hash(account_id.as_ref().as_bytes()).0);
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

/// Maps an account to the shard that it belongs to given a shard_layout
pub fn account_id_to_shard_uid(account_id: &AccountId, shard_layout: &ShardLayout) -> ShardUId {
    ShardUId::from_shard_id_and_layout(
        account_id_to_shard_id(account_id, shard_layout),
        shard_layout,
    )
}

fn is_top_level_account(top_account: &AccountId, account: &AccountId) -> bool {
    match account.as_ref().strip_suffix(top_account.as_ref()) {
        None => false,
        Some(rest) => rest.is_empty() || rest.ends_with('.'),
    }
}

/// ShardUId is an unique representation for shards from different shard layout
#[derive(Serialize, Deserialize, Hash, Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardUId {
    pub version: ShardVersion,
    pub shard_id: u32,
}

impl ShardUId {
    pub fn single_shard() -> Self {
        Self { version: 0, shard_id: 0 }
    }

    /// Byte representation of the shard uid
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut res = [0; 8];
        res[0..4].copy_from_slice(&u32::to_le_bytes(self.version));
        res[4..].copy_from_slice(&u32::to_le_bytes(self.shard_id));
        res
    }

    /// Constructs a shard uid from shard id and a shard layout
    pub fn from_shard_id_and_layout(shard_id: ShardId, shard_layout: &ShardLayout) -> Self {
        assert!(shard_id < shard_layout.num_shards());
        Self { shard_id: shard_id as u32, version: shard_layout.version() }
    }

    /// Returns shard id
    pub fn shard_id(&self) -> ShardId {
        ShardId::from(self.shard_id)
    }
}

impl TryFrom<&[u8]> for ShardUId {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    /// Deserialize `bytes` to shard uid
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 8 {
            return Err("incorrect length for ShardUId".into());
        }
        let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let shard_id = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        Ok(Self { version, shard_id })
    }
}

/// Returns the byte representation for (block, shard_uid)
pub fn get_block_shard_uid(block_hash: &CryptoHash, shard_uid: &ShardUId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(block_hash.as_ref());
    res.extend_from_slice(&shard_uid.to_bytes());
    res
}

/// Deserialize from a byte representation to (block, shard_uid)
#[allow(unused)]
pub fn get_block_shard_uid_rev(
    key: &[u8],
) -> Result<(CryptoHash, ShardUId), Box<dyn std::error::Error + Send + Sync>> {
    if key.len() != 40 {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid key length").into()
        );
    }
    let block_hash = CryptoHash::try_from(&key[..32])?;
    let shard_id = ShardUId::try_from(&key[32..])?;
    Ok((block_hash, shard_id))
}

#[cfg(test)]
mod tests {
    use crate::shard_layout::{account_id_to_shard_id, ShardLayout, ShardUId};
    use rand::distributions::Alphanumeric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;

    #[test]
    fn test_shard_layout_v0() {
        let num_shards = 4;
        let shard_layout = ShardLayout::v0(num_shards, 0);
        let mut shard_id_distribution: HashMap<_, _> =
            (0..num_shards).map(|x| (x, 0)).into_iter().collect();
        let mut rng = StdRng::from_seed([0; 32]);
        for _i in 0..1000 {
            let s: String = (&mut rng).sample_iter(&Alphanumeric).take(10).collect();
            let account_id = s.to_lowercase().parse().unwrap();
            let shard_id = account_id_to_shard_id(&account_id, &shard_layout);
            assert!(shard_id < num_shards);
            *shard_id_distribution.get_mut(&shard_id).unwrap() += 1;
        }
        let expected_distribution: HashMap<_, _> =
            vec![(0, 246), (1, 252), (2, 230), (3, 272)].into_iter().collect();
        assert_eq!(shard_id_distribution, expected_distribution);
    }

    #[test]
    fn test_shard_layout_v1() {
        let shard_layout = ShardLayout::v1(
            vec!["aurora", "bar", "foo", "foo.baz"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            vec!["abc", "foo", "paz"].into_iter().map(|s| s.parse().unwrap()).collect(),
            Some(vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]]),
            1,
        );
        assert_eq!(
            shard_layout.get_split_shard_uids(0).unwrap(),
            (0..4).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        assert_eq!(
            shard_layout.get_split_shard_uids(1).unwrap(),
            (4..8).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        for x in 0..4 {
            assert_eq!(shard_layout.get_parent_shard_id(x).unwrap(), 0);
            assert_eq!(shard_layout.get_parent_shard_id(x + 4).unwrap(), 1);
        }

        assert_eq!(account_id_to_shard_id(&"aurora".parse().unwrap(), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&"foo.aurora".parse().unwrap(), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&"bar.foo.aurora".parse().unwrap(), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&"bar".parse().unwrap(), &shard_layout), 1);
        assert_eq!(account_id_to_shard_id(&"bar.bar".parse().unwrap(), &shard_layout), 1);
        assert_eq!(account_id_to_shard_id(&"foo".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"baz.foo".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"foo.baz".parse().unwrap(), &shard_layout), 3);
        assert_eq!(account_id_to_shard_id(&"a.foo.baz".parse().unwrap(), &shard_layout), 3);

        assert_eq!(account_id_to_shard_id(&"aaa".parse().unwrap(), &shard_layout), 4);
        assert_eq!(account_id_to_shard_id(&"abc".parse().unwrap(), &shard_layout), 5);
        assert_eq!(account_id_to_shard_id(&"bbb".parse().unwrap(), &shard_layout), 5);
        assert_eq!(account_id_to_shard_id(&"foo.goo".parse().unwrap(), &shard_layout), 6);
        assert_eq!(account_id_to_shard_id(&"goo".parse().unwrap(), &shard_layout), 6);
        assert_eq!(account_id_to_shard_id(&"zoo".parse().unwrap(), &shard_layout), 7);
    }
}
