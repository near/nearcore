//! This file implements two data structure `ShardLayout` and `ShardUId`
//!
//! ## `get_parent_shard_id` and `get_split_shard_ids`
//!
//! `ShardLayout` also includes information needed for resharding. In particular, it encodes
//! which shards from the previous shard layout split to which shards in the following shard layout.
//! If shard A in shard layout 0 splits to shard B and C in shard layout 1,
//! we call shard A the parent shard of shard B and C.
//! Note that a shard can only have one parent shard. For example, the following case will be prohibited,
//! a shard C in shard layout 1 contains accounts in both shard A and B in shard layout 0.
//! Parent/split shard information can be accessed through these two functions.

use crate::hash::CryptoHash;
use crate::types::{AccountId, NumShards};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives_core::types::{ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{BTreeMap, BTreeSet};
use std::{fmt, str};

/// `ShardLayout` has a version number.
///
/// The version number should increment as when sharding changes. This guarantees the version
/// number is unique across different shard layouts, which in turn guarantees `ShardUId` is
/// different across shards from different shard layouts, as `ShardUId` includes `version` and
/// `shard_id`
pub type ShardVersion = u32;

/// A versioned struct that contains all information needed to assign accounts to shards.
///
/// Because of re-sharding, the chain may use different shard layout to split shards at different
/// times. Currently, `ShardLayout` is stored as part of `EpochConfig`, which is generated each
/// epoch given the epoch protocol version. In mainnet/testnet, we use two shard layouts since
/// re-sharding has only happened once. It is stored as part of genesis config, see
/// default_simple_nightshade_shard_layout() Below is an overview for some important
/// functionalities of ShardLayout interface.
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
pub enum ShardLayout {
    V0(ShardLayoutV0),
    V1(ShardLayoutV1),
    V2(ShardLayoutV2),
}

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
pub struct ShardLayoutV0 {
    /// Map accounts evenly across all shards
    num_shards: NumShards,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    version: ShardVersion,
}

/// Maps shards from the last shard layout to shards that it splits to in this
/// shard layout. Instead of using map, we just use a vec here because shard_id
/// ranges from 0 to num_shards-1.
///
/// For example, if a shard layout with only shard 0 splits into shards 0, 1,
/// 2, 3, the ShardsSplitMap will be `[[0, 1, 2, 3]]`
type ShardsSplitMap = Vec<Vec<ShardId>>;

/// A mapping from the parent shard to child shards. It maps shards from the
/// previous shard layout to shards that they split to in this shard layout.
/// This structure is first used in ShardLayoutV2.
///
/// For example if a shard layout with shards [0, 2, 5] splits shard 2 into
/// shards [6, 7] the ShardSplitMapV3 will be: 0 => [0] 2 => [6, 7] 5 => [5]
type ShardsSplitMapV2 = BTreeMap<ShardId, Vec<ShardId>>;

/// A mapping from the child shard to the parent shard.
type ShardsParentMapV2 = BTreeMap<ShardId, ShardId>;

pub fn shard_uids_to_ids(shard_uids: &[ShardUId]) -> Vec<ShardId> {
    shard_uids.iter().map(|shard_uid| shard_uid.shard_id()).collect_vec()
}

fn new_shard_ids_vec(shard_ids: Vec<u64>) -> Vec<ShardId> {
    shard_ids.into_iter().map(Into::into).collect()
}

fn new_shards_split_map(shards_split_map: Vec<Vec<u64>>) -> ShardsSplitMap {
    shards_split_map.into_iter().map(new_shard_ids_vec).collect()
}

#[allow(dead_code)]
fn new_shards_split_map_v2(shards_split_map: BTreeMap<u64, Vec<u64>>) -> ShardsSplitMapV2 {
    shards_split_map.into_iter().map(|(k, v)| (k.into(), new_shard_ids_vec(v))).collect()
}

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
pub struct ShardLayoutV1 {
    /// The boundary accounts are the accounts on boundaries between shards.
    /// Each shard contains a range of accounts from one boundary account to
    /// another - or the smallest or largest account possible. The total
    /// number of shards is equal to the number of boundary accounts plus 1.
    boundary_accounts: Vec<AccountId>,
    /// Maps shards from the last shard layout to shards that it splits to in this shard layout,
    /// Useful for constructing states for the shards.
    /// None for the genesis shard layout
    shards_split_map: Option<ShardsSplitMap>,
    /// Maps shard in this shard layout to their parent shard
    /// Since shard_ids always range from 0 to num_shards - 1, we use vec instead of a hashmap
    to_parent_shard_map: Option<Vec<ShardId>>,
    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    version: ShardVersion,
}

impl ShardLayoutV1 {
    // In this shard layout the accounts are divided into ranges, each range is
    // mapped to a shard. The shards are contiguous and start from 0.
    fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
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

/// Making the shard ids non-contiguous.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ShardLayoutV2 {
    /// The boundary accounts are the accounts on boundaries between shards.
    /// Each shard contains a range of accounts from one boundary account to
    /// another - or the smallest or largest account possible. The total
    /// number of shards is equal to the number of boundary accounts plus 1.
    ///
    /// The shard ids do not need to be contiguous or sorted.
    boundary_accounts: Vec<AccountId>,

    /// The shard ids corresponding to the shards defined by the boundary
    /// accounts. The invariant between boundary_accounts and shard_ids is that
    /// boundary_accounts.len() + 1 == shard_ids.len().
    ///
    /// The shard id at index i corresponds to the shard with account range:
    /// [boundary_accounts[i -1], boundary_accounts[i]).
    shard_ids: Vec<ShardId>,

    /// The mapping from shard id to shard index.
    id_to_index_map: BTreeMap<ShardId, ShardIndex>,

    /// The mapping from shard index to shard id.
    /// TODO(wacban) this is identical to the shard_ids, remove it.
    index_to_id_map: BTreeMap<ShardIndex, ShardId>,

    /// A mapping from the parent shard to child shards. Maps shards from the
    /// previous shard layout to shards that they split to in this shard layout.
    shards_split_map: Option<ShardsSplitMapV2>,
    /// A mapping from the child shard to the parent shard. Maps shards in this
    /// shard layout to their parent shards.
    shards_parent_map: Option<ShardsParentMapV2>,

    /// The version of the shard layout. Starting from the ShardLayoutV2 the
    /// version is no longer updated with every shard layout change and it does
    /// not uniquely identify the shard layout.
    version: ShardVersion,
}

/// Counterpart to `ShardLayoutV2` composed of maps with string keys to aid
/// serde serialization.
#[derive(serde::Serialize, serde::Deserialize)]
struct SerdeShardLayoutV2 {
    boundary_accounts: Vec<AccountId>,
    shard_ids: Vec<ShardId>,
    id_to_index_map: BTreeMap<String, ShardIndex>,
    index_to_id_map: BTreeMap<String, ShardId>,
    shards_split_map: Option<BTreeMap<String, Vec<ShardId>>>,
    shards_parent_map: Option<BTreeMap<String, ShardId>>,
    version: ShardVersion,
}

impl From<&ShardLayoutV2> for SerdeShardLayoutV2 {
    fn from(layout: &ShardLayoutV2) -> Self {
        fn key_to_string<K, V>(map: &BTreeMap<K, V>) -> BTreeMap<String, V>
        where
            K: std::fmt::Display,
            V: Clone,
        {
            map.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
        }

        Self {
            boundary_accounts: layout.boundary_accounts.clone(),
            shard_ids: layout.shard_ids.clone(),
            id_to_index_map: key_to_string(&layout.id_to_index_map),
            index_to_id_map: key_to_string(&layout.index_to_id_map),
            shards_split_map: layout.shards_split_map.as_ref().map(key_to_string),
            shards_parent_map: layout.shards_parent_map.as_ref().map(key_to_string),
            version: layout.version,
        }
    }
}

impl TryFrom<SerdeShardLayoutV2> for ShardLayoutV2 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(layout: SerdeShardLayoutV2) -> Result<Self, Self::Error> {
        fn key_to_shard_id<V>(
            map: BTreeMap<String, V>,
        ) -> Result<BTreeMap<ShardId, V>, Box<dyn std::error::Error + Send + Sync>> {
            map.into_iter().map(|(k, v)| Ok((k.parse::<u64>()?.into(), v))).collect()
        }

        let SerdeShardLayoutV2 {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map,
            shards_parent_map,
            version,
        } = layout;

        let id_to_index_map = key_to_shard_id(id_to_index_map)?;
        let shards_split_map = shards_split_map.map(key_to_shard_id).transpose()?;
        let shards_parent_map = shards_parent_map.map(key_to_shard_id).transpose()?;
        let index_to_id_map = index_to_id_map
            .into_iter()
            .map(|(k, v)| Ok((k.parse()?, v)))
            .collect::<Result<_, Self::Error>>()?;

        match (&shards_split_map, &shards_parent_map) {
            (None, None) => {}
            (Some(shard_split_map), Some(shards_parent_map)) => {
                let expected_shards_parent_map =
                    validate_and_derive_shard_parent_map_v2(&shard_ids, &shard_split_map);
                if &expected_shards_parent_map != shards_parent_map {
                    return Err("shards_parent_map does not match the expected value".into());
                }
            }
            _ => {
                return Err(
                    "shards_split_map and shards_parent_map must be both present or both absent"
                        .into(),
                );
            }
        }

        Ok(Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map,
            shards_parent_map,
            version,
        })
    }
}

impl serde::Serialize for ShardLayoutV2 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerdeShardLayoutV2::from(self).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ShardLayoutV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serde_layout = SerdeShardLayoutV2::deserialize(deserializer)?;
        ShardLayoutV2::try_from(serde_layout).map_err(serde::de::Error::custom)
    }
}

impl ShardLayoutV2 {
    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        // TODO(resharding) - This could be optimized.

        let mut shard_id_index = 0;
        for boundary_account in &self.boundary_accounts {
            if account_id < boundary_account {
                break;
            }
            shard_id_index += 1;
        }
        self.shard_ids[shard_id_index]
    }

    pub fn shards_split_map(&self) -> &Option<ShardsSplitMapV2> {
        &self.shards_split_map
    }

    pub fn boundary_accounts(&self) -> &Vec<AccountId> {
        &self.boundary_accounts
    }
}

#[derive(Debug)]
pub enum ShardLayoutError {
    InvalidShardIdError { shard_id: ShardId },
    InvalidShardIndexError { shard_index: ShardIndex },
    NoParentError { shard_id: ShardId },
}

impl fmt::Display for ShardLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ShardLayoutError {}

impl ShardLayout {
    /// Handy constructor for a single-shard layout, mostly for test purposes
    pub fn single_shard() -> Self {
        let shard_id = ShardId::new(0);
        Self::V2(ShardLayoutV2 {
            boundary_accounts: vec![],
            shard_ids: vec![shard_id],
            id_to_index_map: [(shard_id, 0)].into(),
            index_to_id_map: [(0, shard_id)].into(),
            shards_split_map: None,
            shards_parent_map: None,
            version: 0,
        })
    }

    /// Creates a multi-shard ShardLayout using the most recent ShardLayout
    /// version and default boundary accounts. It should be used for tests only.
    /// The shard ids are deterministic but arbitrary in order to test the
    /// non-contiguous ShardIds.
    #[cfg(all(feature = "test_utils", feature = "rand"))]
    pub fn multi_shard(num_shards: NumShards, version: ShardVersion) -> Self {
        assert!(num_shards > 0, "at least 1 shard is required");

        let boundary_accounts = (1..num_shards)
            .map(|i| format!("test{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>();

        Self::multi_shard_custom(boundary_accounts, version)
    }

    /// Creates a multi-shard ShardLayout using the most recent ShardLayout
    /// version and provided boundary accounts. It should be used for tests
    /// only. The shard ids are deterministic but arbitrary in order to test the
    /// non-contiguous ShardIds.
    #[cfg(all(feature = "test_utils", feature = "rand"))]
    pub fn multi_shard_custom(boundary_accounts: Vec<AccountId>, version: ShardVersion) -> Self {
        use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};

        let num_shards = (boundary_accounts.len() + 1) as u64;

        // In order to test the non-contiguous shard ids randomize the order and
        // TODO(wacban) randomize the range of shard ids.
        let mut rng = StdRng::seed_from_u64(42);
        let mut shard_ids = (0..num_shards).map(ShardId::new).collect::<Vec<ShardId>>();
        shard_ids.shuffle(&mut rng);

        let (id_to_index_map, index_to_id_map) = shard_ids
            .iter()
            .enumerate()
            .map(|(i, &shard_id)| ((shard_id, i), (i, shard_id)))
            .unzip();

        Self::V2(ShardLayoutV2 {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map: None,
            shards_parent_map: None,
            version,
        })
    }

    /// Test-only helper to create a simple multi-shard ShardLayout with the provided boundaries.
    /// The shard ids are deterministic but arbitrary in order to test the non-contiguous ShardIds.
    #[cfg(all(feature = "test_utils", feature = "rand"))]
    pub fn simple_v1(boundary_accounts: &[&str]) -> ShardLayout {
        // TODO these test methods should go into a different namespace
        let boundary_accounts = boundary_accounts.iter().map(|a| a.parse().unwrap()).collect();
        Self::multi_shard_custom(boundary_accounts, 1)
    }

    /// Return a V0 ShardLayout
    #[deprecated(note = "Use multi_shard() instead")]
    pub fn v0(num_shards: NumShards, version: ShardVersion) -> Self {
        Self::V0(ShardLayoutV0 { num_shards, version })
    }

    /// Return a V1 ShardLayout
    #[deprecated(note = "Use multi_shard() instead")]
    pub fn v1(
        boundary_accounts: Vec<AccountId>,
        shards_split_map: Option<ShardsSplitMap>,
        version: ShardVersion,
    ) -> Self {
        let to_parent_shard_map = if let Some(shards_split_map) = &shards_split_map {
            let mut to_parent_shard_map = BTreeMap::new();
            let num_shards = (boundary_accounts.len() + 1) as NumShards;
            for (parent_shard_id, shard_ids) in shards_split_map.iter().enumerate() {
                let parent_shard_id = ShardId::new(parent_shard_id as u64);
                for &shard_id in shard_ids {
                    let prev = to_parent_shard_map.insert(shard_id, parent_shard_id);
                    assert!(prev.is_none(), "no shard should appear in the map twice");
                    let shard_id: u64 = shard_id.into();
                    assert!(shard_id < num_shards, "shard id should be valid");
                }
            }
            Some((0..num_shards).map(|shard_id| to_parent_shard_map[&shard_id.into()]).collect())
        } else {
            None
        };
        Self::V1(ShardLayoutV1 {
            boundary_accounts,
            shards_split_map,
            to_parent_shard_map,
            version,
        })
    }

    /// Return a V2 ShardLayout
    pub fn v2(
        boundary_accounts: Vec<AccountId>,
        shard_ids: Vec<ShardId>,
        shards_split_map: Option<ShardsSplitMapV2>,
    ) -> Self {
        // In the v2 layout the version is not updated with every shard layout.
        const VERSION: ShardVersion = 3;

        assert_eq!(boundary_accounts.len() + 1, shard_ids.len());
        assert_eq!(boundary_accounts, boundary_accounts.iter().sorted().cloned().collect_vec());

        let mut id_to_index_map = BTreeMap::new();
        let mut index_to_id_map = BTreeMap::new();
        for (shard_index, &shard_id) in shard_ids.iter().enumerate() {
            id_to_index_map.insert(shard_id, shard_index);
            index_to_id_map.insert(shard_index, shard_id);
        }

        let Some(shards_split_map) = shards_split_map else {
            return Self::V2(ShardLayoutV2 {
                boundary_accounts,
                shard_ids,
                id_to_index_map,
                index_to_id_map,
                shards_split_map: None,
                shards_parent_map: None,
                version: VERSION,
            });
        };

        let shards_parent_map =
            validate_and_derive_shard_parent_map_v2(&shard_ids, &shards_split_map);
        let shards_split_map = Some(shards_split_map);
        let shards_parent_map = Some(shards_parent_map);
        Self::V2(ShardLayoutV2 {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map,
            shards_parent_map,
            version: VERSION,
        })
    }

    /// Returns the simple nightshade layout that we use in production
    pub fn get_simple_nightshade_layout() -> ShardLayout {
        #[allow(deprecated)]
        ShardLayout::v1(
            vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            Some(new_shards_split_map(vec![vec![0, 1, 2, 3]])),
            1,
        )
    }

    /// Returns the simple nightshade layout, version 2, that will be used in production.
    pub fn get_simple_nightshade_layout_v2() -> ShardLayout {
        #[allow(deprecated)]
        ShardLayout::v1(
            vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near", "tge-lockup.sweat"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            Some(new_shards_split_map(vec![vec![0], vec![1], vec![2], vec![3, 4]])),
            2,
        )
    }

    /// Returns the simple nightshade layout, version 3, that will be used in production.
    pub fn get_simple_nightshade_layout_v3() -> ShardLayout {
        #[allow(deprecated)]
        ShardLayout::v1(
            vec![
                "aurora",
                "aurora-0",
                "game.hot.tg",
                "kkuuue2akv_1630967379.near",
                "tge-lockup.sweat",
            ]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect(),
            Some(new_shards_split_map(vec![vec![0], vec![1], vec![2, 3], vec![4], vec![5]])),
            3,
        )
    }

    /// Returns the simple nightshade layout, version 4, that will be used in
    /// production. It adds a new boundary account "game.hot.tg".
    ///
    /// This is the first layout used in the Resharding V3 and it is the first
    /// one where the arbitrary shard ids are used.
    pub fn get_simple_nightshade_layout_v4() -> ShardLayout {
        let base_shard_layout = Self::get_simple_nightshade_layout_v3();
        let new_boundary_account = "game.hot.tg-0".parse().unwrap();
        ShardLayout::derive_shard_layout(&base_shard_layout, new_boundary_account)
    }

    /// Returns the simple nightshade layout, version 5, that will be used in
    /// production. It adds a new boundary account "earn.kaiching".
    pub fn get_simple_nightshade_layout_v5() -> ShardLayout {
        let base_shard_layout = Self::get_simple_nightshade_layout_v4();
        let new_boundary_account = "earn.kaiching".parse().unwrap();
        ShardLayout::derive_shard_layout(&base_shard_layout, new_boundary_account)
    }

    /// Returns the simple nightshade layout, version 6, with new boundary account "750".
    pub fn get_simple_nightshade_layout_v6() -> ShardLayout {
        let base_shard_layout = Self::get_simple_nightshade_layout_v5();
        let new_boundary_account = "750".parse().unwrap();
        ShardLayout::derive_shard_layout(&base_shard_layout, new_boundary_account)
    }

    /// This layout is used only in resharding tests. It allows testing of any features which were
    /// introduced after the last layout upgrade in production. Currently, it is built on top of V3.
    #[cfg(feature = "nightly")]
    pub fn get_simple_nightshade_layout_testonly() -> ShardLayout {
        #[allow(deprecated)]
        ShardLayout::v1(
            vec![
                "aurora",
                "aurora-0",
                "game.hot.tg",
                "kkuuue2akv_1630967379.near",
                "nightly",
                "tge-lockup.sweat",
            ]
            .into_iter()
            .map(|s| s.parse().unwrap())
            .collect(),
            Some(new_shards_split_map(vec![
                vec![0],
                vec![1],
                vec![2],
                vec![3],
                vec![4, 5],
                vec![6],
            ])),
            4,
        )
    }

    /// Maps an account to the shard_id that it belongs to in this shard_layout
    /// For V0, maps according to hash of account id
    /// For V1 and V2, accounts are divided to ranges, each range of account is mapped to a shard.
    /// Note: Calling function with receipt receiver_id to determine target shard is incorrect,
    ///       `Receipt::receiver_shard` should be used instead.
    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        match self {
            ShardLayout::V0(v0) => {
                let hash = CryptoHash::hash_bytes(account_id.as_bytes());
                let (bytes, _) = stdx::split_array::<32, 8, 24>(hash.as_bytes());
                let shard_id = u64::from_le_bytes(*bytes) % v0.num_shards;
                shard_id.into()
            }
            ShardLayout::V1(v1) => v1.account_id_to_shard_id(account_id),
            ShardLayout::V2(v2) => v2.account_id_to_shard_id(account_id),
        }
    }

    /// Maps an account to the shard_uid that it belongs to in this shard_layout
    #[inline]
    pub fn account_id_to_shard_uid(&self, account_id: &AccountId) -> ShardUId {
        ShardUId::from_shard_id_and_layout(self.account_id_to_shard_id(account_id), self)
    }

    /// Given a parent shard id, return the shard uids for the shards in the current shard layout that
    /// are split from this parent shard. If this shard layout has no parent shard layout, return None
    #[inline]
    pub fn get_children_shards_uids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardUId>> {
        self.get_children_shards_ids(parent_shard_id).map(|shards| {
            shards.into_iter().map(|id| ShardUId::from_shard_id_and_layout(id, self)).collect()
        })
    }

    /// Given a parent shard id, return the shard ids for the shards in the current shard layout that
    /// are split from this parent shard. If this shard layout has no parent shard layout, return None
    pub fn get_children_shards_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        match self {
            Self::V0(_) => None,
            Self::V1(v1) => match &v1.shards_split_map {
                Some(shards_split_map) => {
                    // In V1 the shard id and the shard index are the same. It
                    // is ok to cast the id to index here. The same is not the
                    // case in V2.
                    let parent_shard_index: ShardIndex = parent_shard_id.into();
                    shards_split_map.get(parent_shard_index).cloned()
                }
                None => None,
            },
            Self::V2(v2) => match &v2.shards_split_map {
                Some(shards_split_map) => shards_split_map.get(&parent_shard_id).cloned(),
                None => None,
            },
        }
    }

    /// Return the parent shard id for a given shard in the shard layout.
    /// Returns an error if `shard_id` is an invalid shard id in the current
    /// layout. Returns None if the shard layout has no parent shard layout.
    pub fn try_get_parent_shard_id(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardId>, ShardLayoutError> {
        if !self.shard_ids().any(|id| id == shard_id) {
            return Err(ShardLayoutError::InvalidShardIdError { shard_id });
        }
        let parent_shard_id = match self {
            Self::V0(_) => None,
            Self::V1(v1) => match &v1.to_parent_shard_map {
                // we can safely unwrap here because the construction of to_parent_shard_map guarantees
                // that every shard has a parent shard
                Some(to_parent_shard_map) => {
                    let shard_index = self.get_shard_index(shard_id).unwrap();
                    let parent_shard_id = to_parent_shard_map.get(shard_index).unwrap();
                    Some(*parent_shard_id)
                }
                None => None,
            },
            Self::V2(v2) => match &v2.shards_parent_map {
                // we can safely unwrap here because the construction of to_parent_shard_map guarantees
                // that every shard has a parent shard
                Some(to_parent_shard_map) => {
                    let parent_shard_id = to_parent_shard_map.get(&shard_id).unwrap();
                    Some(*parent_shard_id)
                }
                None => None,
            },
        };
        Ok(parent_shard_id)
    }

    /// Return the parent shard id for a given shard in the shard layout. Only
    /// calls this function for shard layout that has parent shard layout.
    /// Returns an error if `shard_id` is an invalid shard id in the current
    /// layout or if the shard has no parent in this shard layout.
    pub fn get_parent_shard_id(&self, shard_id: ShardId) -> Result<ShardId, ShardLayoutError> {
        let parent_shard_id = self.try_get_parent_shard_id(shard_id)?;
        parent_shard_id.ok_or(ShardLayoutError::NoParentError { shard_id })
    }

    /// Derive new shard layout from an existing one
    pub fn derive_shard_layout(
        base_shard_layout: &ShardLayout,
        new_boundary_account: AccountId,
    ) -> ShardLayout {
        let mut boundary_accounts = base_shard_layout.boundary_accounts().clone();
        let mut shard_ids = base_shard_layout.shard_ids().collect::<Vec<_>>();
        let mut shards_split_map = shard_ids
            .iter()
            .map(|id| (*id, vec![*id]))
            .collect::<BTreeMap<ShardId, Vec<ShardId>>>();

        assert!(!boundary_accounts.contains(&new_boundary_account), "duplicated boundary account");

        // boundary accounts should be sorted such that the index points to the shard to be split
        boundary_accounts.push(new_boundary_account.clone());
        boundary_accounts.sort();
        let new_boundary_account_index = boundary_accounts
            .iter()
            .position(|acc| acc == &new_boundary_account)
            .expect("account should be guaranteed to exist at this point");

        // new shard ids start from the current max
        let max_shard_id =
            *shard_ids.iter().max().expect("there should always be at least one shard");
        let new_shards = vec![max_shard_id + 1, max_shard_id + 2];
        let parent_shard_id = shard_ids
            .splice(new_boundary_account_index..new_boundary_account_index + 1, new_shards.clone())
            .collect::<Vec<_>>();
        let [parent_shard_id] = parent_shard_id.as_slice() else {
            panic!("should only splice one shard");
        };
        shards_split_map.insert(*parent_shard_id, new_shards);

        ShardLayout::v2(boundary_accounts, shard_ids, Some(shards_split_map))
    }

    #[inline]
    pub fn version(&self) -> ShardVersion {
        match self {
            Self::V0(v0) => v0.version,
            Self::V1(v1) => v1.version,
            Self::V2(v2) => v2.version,
        }
    }

    pub fn boundary_accounts(&self) -> &Vec<AccountId> {
        match self {
            Self::V1(v1) => &v1.boundary_accounts,
            Self::V2(v2) => &v2.boundary_accounts,
            _ => panic!("ShardLayout::V0 doesn't have boundary accounts"),
        }
    }

    pub fn num_shards(&self) -> NumShards {
        match self {
            Self::V0(v0) => v0.num_shards,
            Self::V1(v1) => (v1.boundary_accounts.len() + 1) as NumShards,
            Self::V2(v2) => v2.shard_ids.len() as NumShards,
        }
    }

    /// Returns an iterator that iterates over all the shard ids in the shard layout.
    /// Please also see the `shard_infos` method that returns the ShardInfo for each
    /// shard and should be used when ShardIndex is needed.
    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> {
        match self {
            Self::V0(_) => (0..self.num_shards()).map(Into::into).collect_vec().into_iter(),
            Self::V1(_) => (0..self.num_shards()).map(Into::into).collect_vec().into_iter(),
            Self::V2(v2) => v2.shard_ids.clone().into_iter(),
        }
    }

    /// Returns an iterator that iterates over all the shard uids for all the
    /// shards in the shard layout
    pub fn shard_uids(&self) -> impl Iterator<Item = ShardUId> + '_ {
        self.shard_ids().map(|shard_id| ShardUId::from_shard_id_and_layout(shard_id, self))
    }

    pub fn shard_indexes(&self) -> impl Iterator<Item = ShardIndex> + 'static {
        let num_shards: usize =
            self.num_shards().try_into().expect("Number of shards doesn't fit in usize");
        match self {
            Self::V0(_) | Self::V1(_) | Self::V2(_) => (0..num_shards).into_iter(),
        }
    }

    /// Returns an iterator that returns the ShardInfos for every shard in
    /// this shard layout. This method should be preferred over calling
    /// shard_ids().enumerate(). Today the result of shard_ids() is sorted, but
    /// it may be changed in the future.
    pub fn shard_infos(&self) -> impl Iterator<Item = ShardInfo> + '_ {
        self.shard_uids()
            .enumerate()
            .map(|(shard_index, shard_uid)| ShardInfo { shard_index, shard_uid })
    }

    /// Returns the shard index for a given shard id. The shard index should be
    /// used when indexing into an array of chunk data.
    pub fn get_shard_index(&self, shard_id: ShardId) -> Result<ShardIndex, ShardLayoutError> {
        match self {
            // In V0 the shard id and shard index are the same.
            Self::V0(_) => Ok(shard_id.into()),
            // In V1 the shard id and shard index are the same.
            Self::V1(_) => Ok(shard_id.into()),
            // In V2 the shard id and shard index are **not** the same.
            Self::V2(v2) => v2
                .id_to_index_map
                .get(&shard_id)
                .copied()
                .ok_or(ShardLayoutError::InvalidShardIdError { shard_id }),
        }
    }

    /// Get the shard id for a given shard index. The shard id should be used to
    /// identify the shard and starting from the ShardLayoutV2 it is unique.
    pub fn get_shard_id(&self, shard_index: ShardIndex) -> Result<ShardId, ShardLayoutError> {
        let num_shards = self.num_shards() as usize;
        match self {
            Self::V0(_) | Self::V1(_) => {
                if shard_index >= num_shards {
                    return Err(ShardLayoutError::InvalidShardIndexError { shard_index });
                }
                Ok(ShardId::new(shard_index as u64))
            }
            Self::V2(v2) => v2
                .shard_ids
                .get(shard_index)
                .copied()
                .ok_or(ShardLayoutError::InvalidShardIndexError { shard_index }),
        }
    }

    /// Returns all the shards from the previous shard layout that were
    /// split into multiple shards in this shard layout.
    pub fn get_split_parent_shard_ids(&self) -> Result<BTreeSet<ShardId>, ShardLayoutError> {
        let mut parent_shard_ids = BTreeSet::new();
        for shard_id in self.shard_ids() {
            let parent_shard_id = self.try_get_parent_shard_id(shard_id)?;
            let Some(parent_shard_id) = parent_shard_id else {
                continue;
            };
            if parent_shard_id == shard_id {
                continue;
            }
            parent_shard_ids.insert(parent_shard_id);
        }
        Ok(parent_shard_ids)
    }
}

// Validates the shards_split_map and derives the shards_parent_map from it.
fn validate_and_derive_shard_parent_map_v2(
    shard_ids: &Vec<ShardId>,
    shards_split_map: &ShardsSplitMapV2,
) -> ShardsParentMapV2 {
    let mut shards_parent_map = ShardsParentMapV2::new();
    for (&parent_shard_id, child_shard_ids) in shards_split_map {
        for &child_shard_id in child_shard_ids {
            let prev = shards_parent_map.insert(child_shard_id, parent_shard_id);
            assert!(prev.is_none(), "no shard should appear in the map twice");
        }
        if let &[child_shard_id] = child_shard_ids.as_slice() {
            // The parent shards with only one child shard are not split and
            // should keep the same shard id.
            assert_eq!(parent_shard_id, child_shard_id);
        } else {
            // The parent shards with multiple children shards are split.
            // The parent shard id should no longer be used.
            assert!(!shard_ids.contains(&parent_shard_id));
        }
    }

    assert_eq!(
        shard_ids.iter().copied().sorted().collect_vec(),
        shards_parent_map.keys().copied().collect_vec()
    );
    shards_parent_map
}

/// `ShardUId` is a unique representation for shards from different shard layouts.
///
/// Comparing to `ShardId`, which is just an ordinal number ranging from 0 to NUM_SHARDS-1,
/// `ShardUId` provides a way to unique identify shards when shard layouts may change across epochs.
/// This is important because we store states indexed by shards in our database, so we need a
/// way to unique identify shard even when shards change across epochs.
/// Another difference between `ShardUId` and `ShardId` is that `ShardUId` should only exist in
/// a node's internal state while `ShardId` can be exposed to outside APIs and used in protocol
/// level information (for example, `ShardChunkHeader` contains `ShardId` instead of `ShardUId`)
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ProtocolSchema,
)]
pub struct ShardUId {
    pub version: ShardVersion,
    pub shard_id: u32,
}

impl ShardUId {
    pub fn new(version: ShardVersion, shard_id: ShardId) -> Self {
        Self { version, shard_id: shard_id.into() }
    }

    /// Returns the only shard uid in the ShardLayout::single_shard layout.
    /// It is not suitable for use with any other shard layout.
    #[cfg(feature = "test_utils")]
    pub fn single_shard() -> Self {
        ShardLayout::single_shard().shard_uids().next().unwrap()
    }

    /// Byte representation of the shard uid
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut res = [0; 8];
        res[0..4].copy_from_slice(&u32::to_le_bytes(self.version));
        res[4..].copy_from_slice(&u32::to_le_bytes(self.shard_id));
        res
    }

    /// Get the db key which is strictly bigger than all keys in DB for this
    /// shard and still doesn't include keys from other shards.
    ///
    /// Please note that the returned db key may not correspond to a valid shard
    /// uid and it may not be used to get the next shard uid.
    pub fn get_upper_bound_db_key(shard_uid_bytes: &[u8; 8]) -> [u8; 8] {
        let mut result = *shard_uid_bytes;
        for i in (0..8).rev() {
            if result[i] == u8::MAX {
                result[i] = 0;
            } else {
                result[i] += 1;
                return result;
            }
        }
        panic!("Next shard prefix for shard bytes {shard_uid_bytes:?} does not exist");
    }

    /// Constructs a shard uid from shard id and a shard layout
    pub fn from_shard_id_and_layout(shard_id: ShardId, shard_layout: &ShardLayout) -> Self {
        assert!(shard_layout.shard_ids().any(|i| i == shard_id));
        Self::new(shard_layout.version(), shard_id)
    }

    /// Returns shard id
    pub fn shard_id(&self) -> ShardId {
        self.shard_id.into()
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

impl fmt::Display for ShardUId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}.v{}", self.shard_id, self.version)
    }
}

impl fmt::Debug for ShardUId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl str::FromStr for ShardUId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (shard_str, version_str) = s
            .split_once(".")
            .ok_or_else(|| "shard version and number must be separated by \".\"".to_string())?;

        let version = version_str
            .strip_prefix("v")
            .ok_or_else(|| "shard version must start with \"v\"".to_string())?
            .parse::<ShardVersion>()
            .map_err(|e| format!("shard version after \"v\" must be a number, {e}"))?;

        let shard_str = shard_str
            .strip_prefix("s")
            .ok_or_else(|| "shard id must start with \"s\"".to_string())?;
        let shard_id = shard_str
            .parse::<u32>()
            .map_err(|e| format!("shard id after \"s\" must be a number, {e}"))?;

        Ok(ShardUId { shard_id, version })
    }
}

impl<'de> serde::Deserialize<'de> for ShardUId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ShardUIdVisitor)
    }
}

impl serde::Serialize for ShardUId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct ShardUIdVisitor;
impl<'de> serde::de::Visitor<'de> for ShardUIdVisitor {
    type Value = ShardUId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "either string format of `ShardUId` like 's0.v3' for shard 0 version 3, or a map"
        )
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(|e| E::custom(e))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // custom struct deserialization for backwards compatibility
        // TODO(#7894): consider removing this code after checking
        // `ShardUId` is nowhere serialized in the old format
        let mut version = None;
        let mut shard_id = None;

        while let Some((field, value)) = map.next_entry()? {
            match field {
                "version" => version = Some(value),
                "shard_id" => shard_id = Some(value),
                _ => return Err(serde::de::Error::unknown_field(field, &["version", "shard_id"])),
            }
        }

        match (version, shard_id) {
            (None, _) => Err(serde::de::Error::missing_field("version")),
            (_, None) => Err(serde::de::Error::missing_field("shard_id")),
            (Some(version), Some(shard_id)) => Ok(ShardUId { version, shard_id }),
        }
    }
}

pub struct ShardInfo {
    pub shard_index: ShardIndex,
    pub shard_uid: ShardUId,
}

impl ShardInfo {
    pub fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_uid.shard_id()
    }

    pub fn shard_uid(&self) -> ShardUId {
        self.shard_uid
    }
}

#[cfg(test)]
mod tests {
    use crate::epoch_manager::EpochConfigStore;
    use crate::shard_layout::{
        ShardLayout, ShardLayoutV1, ShardUId, new_shard_ids_vec, new_shards_split_map,
    };
    use itertools::Itertools;
    use near_primitives_core::types::ProtocolVersion;
    use near_primitives_core::types::{AccountId, ShardId};
    use near_primitives_core::version::ProtocolFeature;
    use rand::distributions::Alphanumeric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::{BTreeMap, HashMap};

    use super::{ShardVersion, ShardsSplitMap, new_shards_split_map_v2};

    // The old ShardLayoutV1, before fixed shards were removed. tests only
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
    pub struct OldShardLayoutV1 {
        /// num_shards = fixed_shards.len() + boundary_accounts.len() + 1
        /// Each account and all sub-accounts map to the shard of position in this array.
        fixed_shards: Vec<AccountId>,
        /// The rest are divided by boundary_accounts to ranges, each range is mapped to a shard
        boundary_accounts: Vec<AccountId>,
        /// Maps shards from the last shard layout to shards that it splits to in this shard layout,
        /// Useful for constructing states for the shards.
        /// None for the genesis shard layout
        shards_split_map: Option<ShardsSplitMap>,
        /// Maps shard in this shard layout to their parent shard
        /// Since shard_ids always range from 0 to num_shards - 1, we use vec instead of a hashmap
        to_parent_shard_map: Option<Vec<ShardId>>,
        /// Version of the shard layout, this is useful for uniquely identify the shard layout
        version: ShardVersion,
    }

    impl ShardLayout {
        /// Constructor for tests that need a shard layout for a specific protocol version.
        pub fn for_protocol_version(protocol_version: ProtocolVersion) -> Self {
            let config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
            config_store.get_config(protocol_version).shard_layout.clone()
        }
    }

    #[test]
    fn test_shard_layout_v0() {
        let num_shards = 4;
        #[allow(deprecated)]
        let shard_layout = ShardLayout::v0(num_shards, 0);
        let mut shard_id_distribution: HashMap<ShardId, _> =
            shard_layout.shard_ids().map(|shard_id| (shard_id.into(), 0)).collect();
        let mut rng = StdRng::from_seed([0; 32]);
        for _i in 0..1000 {
            let s: Vec<u8> = (&mut rng).sample_iter(&Alphanumeric).take(10).collect();
            let s = String::from_utf8(s).unwrap();
            let account_id = s.to_lowercase().parse().unwrap();
            let shard_id = shard_layout.account_id_to_shard_id(&account_id);
            *shard_id_distribution.get_mut(&shard_id).unwrap() += 1;

            let shard_id: u64 = shard_id.into();
            assert!(shard_id < num_shards);
        }
        let expected_distribution: HashMap<ShardId, _> = [
            (ShardId::new(0), 247),
            (ShardId::new(1), 268),
            (ShardId::new(2), 233),
            (ShardId::new(3), 252),
        ]
        .into_iter()
        .collect();
        assert_eq!(shard_id_distribution, expected_distribution);
    }

    #[test]
    fn test_shard_layout_v1() {
        let aid = |s: &str| s.parse().unwrap();
        let sid = |s: u64| ShardId::new(s);

        #[allow(deprecated)]
        let shard_layout = ShardLayout::v1(
            parse_account_ids(&["aurora", "bar", "foo", "foo.baz", "paz"]),
            Some(new_shards_split_map(vec![vec![0, 1, 2], vec![3, 4, 5]])),
            1,
        );
        assert_eq!(
            shard_layout.get_children_shards_uids(ShardId::new(0)).unwrap(),
            (0..3).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        assert_eq!(
            shard_layout.get_children_shards_uids(ShardId::new(1)).unwrap(),
            (3..6).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        for x in 0..3 {
            assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(x)).unwrap(), sid(0));
            assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(x + 3)).unwrap(), sid(1));
        }

        assert_eq!(shard_layout.account_id_to_shard_id(&aid("aurora")), sid(1));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.aurora")), sid(3));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar.foo.aurora")), sid(2));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar")), sid(2));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar.bar")), sid(2));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo")), sid(3));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("baz.foo")), sid(2));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.baz")), sid(4));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("a.foo.baz")), sid(0));

        assert_eq!(shard_layout.account_id_to_shard_id(&aid("aaa")), sid(0));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("abc")), sid(0));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("bbb")), sid(2));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.goo")), sid(4));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("goo")), sid(4));
        assert_eq!(shard_layout.account_id_to_shard_id(&aid("zoo")), sid(5));
    }

    // check that after removing the fixed shards from the shard layout v1
    // the fixed shards are skipped in deserialization
    // this should be the default as long as serde(deny_unknown_fields) is not set
    #[test]
    fn test_remove_fixed_shards() {
        let old = OldShardLayoutV1 {
            fixed_shards: vec![],
            boundary_accounts: parse_account_ids(&["aaa", "bbb"]),
            shards_split_map: Some(new_shards_split_map(vec![vec![0, 1, 2]])),
            to_parent_shard_map: Some(new_shard_ids_vec(vec![0, 0, 0])),
            version: 1,
        };
        let json = serde_json::to_string_pretty(&old).unwrap();
        println!("json");
        println!("{json:#?}");

        let new = serde_json::from_str::<ShardLayoutV1>(json.as_str()).unwrap();
        assert_eq!(old.boundary_accounts, new.boundary_accounts);
        assert_eq!(old.shards_split_map, new.shards_split_map);
        assert_eq!(old.to_parent_shard_map, new.to_parent_shard_map);
        assert_eq!(old.version, new.version);
    }

    fn parse_account_ids(ids: &[&str]) -> Vec<AccountId> {
        ids.into_iter().map(|a| a.parse().unwrap()).collect()
    }

    #[test]
    fn test_shard_layout_v2() {
        let sid = |s: u64| ShardId::new(s);
        let shard_layout = get_test_shard_layout_v2();

        // check accounts mapping in the middle of each range
        assert_eq!(shard_layout.account_id_to_shard_id(&"aaa".parse().unwrap()), sid(3));
        assert_eq!(shard_layout.account_id_to_shard_id(&"ddd".parse().unwrap()), sid(8));
        assert_eq!(shard_layout.account_id_to_shard_id(&"mmm".parse().unwrap()), sid(4));
        assert_eq!(shard_layout.account_id_to_shard_id(&"rrr".parse().unwrap()), sid(7));

        // check accounts mapping for the boundary accounts
        assert_eq!(shard_layout.account_id_to_shard_id(&"ccc".parse().unwrap()), sid(8));
        assert_eq!(shard_layout.account_id_to_shard_id(&"kkk".parse().unwrap()), sid(4));
        assert_eq!(shard_layout.account_id_to_shard_id(&"ppp".parse().unwrap()), sid(7));

        // check shard ids
        assert_eq!(shard_layout.shard_ids().collect_vec(), new_shard_ids_vec(vec![3, 8, 4, 7]));

        // check shard uids
        let version = 3;
        let u = |shard_id| ShardUId { shard_id, version };
        assert_eq!(shard_layout.shard_uids().collect_vec(), vec![u(3), u(8), u(4), u(7)]);

        // check parent
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(3)).unwrap(), sid(3));
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(8)).unwrap(), sid(1));
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(4)).unwrap(), sid(4));
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(7)).unwrap(), sid(1));

        // check child
        assert_eq!(
            shard_layout.get_children_shards_ids(ShardId::new(1)).unwrap(),
            new_shard_ids_vec(vec![7, 8])
        );
        assert_eq!(
            shard_layout.get_children_shards_ids(ShardId::new(3)).unwrap(),
            new_shard_ids_vec(vec![3])
        );
        assert_eq!(
            shard_layout.get_children_shards_ids(ShardId::new(4)).unwrap(),
            new_shard_ids_vec(vec![4])
        );
    }

    fn get_test_shard_layout_v2() -> ShardLayout {
        let b0 = "ccc".parse().unwrap();
        let b1 = "kkk".parse().unwrap();
        let b2 = "ppp".parse().unwrap();

        let boundary_accounts = vec![b0, b1, b2];
        let shard_ids = vec![3, 8, 4, 7];
        let shard_ids = new_shard_ids_vec(shard_ids);

        // the mapping from parent to the child
        // shard 1 is split into shards 7 & 8 while other shards stay the same
        let shards_split_map = BTreeMap::from([(1, vec![7, 8]), (3, vec![3]), (4, vec![4])]);
        let shards_split_map = new_shards_split_map_v2(shards_split_map);
        let shards_split_map = Some(shards_split_map);

        ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
    }

    #[test]
    fn test_shard_layout_all() {
        #[allow(deprecated)]
        let v0 = ShardLayout::v0(1, 0);
        let v1 = ShardLayout::get_simple_nightshade_layout();
        let v2 = ShardLayout::get_simple_nightshade_layout_v2();
        let v3 = ShardLayout::get_simple_nightshade_layout_v3();
        let v4 = ShardLayout::get_simple_nightshade_layout_v4();
        let v5 = ShardLayout::get_simple_nightshade_layout_v5();
        let v6 = ShardLayout::get_simple_nightshade_layout_v6();

        insta::assert_snapshot!(serde_json::to_string_pretty(&v0).unwrap(), @r###"
        {
          "V0": {
            "num_shards": 1,
            "version": 0
          }
        }
        "###);
        insta::assert_snapshot!(serde_json::to_string_pretty(&v1).unwrap(), @r###"
        {
          "V1": {
            "boundary_accounts": [
              "aurora",
              "aurora-0",
              "kkuuue2akv_1630967379.near"
            ],
            "shards_split_map": [
              [
                0,
                1,
                2,
                3
              ]
            ],
            "to_parent_shard_map": [
              0,
              0,
              0,
              0
            ],
            "version": 1
          }
        }
        "###);
        insta::assert_snapshot!(serde_json::to_string_pretty(&v2).unwrap(), @r###"
        {
          "V1": {
            "boundary_accounts": [
              "aurora",
              "aurora-0",
              "kkuuue2akv_1630967379.near",
              "tge-lockup.sweat"
            ],
            "shards_split_map": [
              [
                0
              ],
              [
                1
              ],
              [
                2
              ],
              [
                3,
                4
              ]
            ],
            "to_parent_shard_map": [
              0,
              1,
              2,
              3,
              3
            ],
            "version": 2
          }
        }
        "###);
        insta::assert_snapshot!(serde_json::to_string_pretty(&v3).unwrap(), @r###"
        {
          "V1": {
            "boundary_accounts": [
              "aurora",
              "aurora-0",
              "game.hot.tg",
              "kkuuue2akv_1630967379.near",
              "tge-lockup.sweat"
            ],
            "shards_split_map": [
              [
                0
              ],
              [
                1
              ],
              [
                2,
                3
              ],
              [
                4
              ],
              [
                5
              ]
            ],
            "to_parent_shard_map": [
              0,
              1,
              2,
              2,
              3,
              4
            ],
            "version": 3
          }
        }
        "###);

        insta::assert_snapshot!(serde_json::to_string_pretty(&v4).unwrap(), @r###"
        {
          "V2": {
            "boundary_accounts": [
              "aurora",
              "aurora-0",
              "game.hot.tg",
              "game.hot.tg-0",
              "kkuuue2akv_1630967379.near",
              "tge-lockup.sweat"
            ],
            "shard_ids": [
              0,
              1,
              2,
              6,
              7,
              4,
              5
            ],
            "id_to_index_map": {
              "0": 0,
              "1": 1,
              "2": 2,
              "4": 5,
              "5": 6,
              "6": 3,
              "7": 4
            },
            "index_to_id_map": {
              "0": 0,
              "1": 1,
              "2": 2,
              "3": 6,
              "4": 7,
              "5": 4,
              "6": 5
            },
            "shards_split_map": {
              "0": [
                0
              ],
              "1": [
                1
              ],
              "2": [
                2
              ],
              "3": [
                6,
                7
              ],
              "4": [
                4
              ],
              "5": [
                5
              ]
            },
            "shards_parent_map": {
              "0": 0,
              "1": 1,
              "2": 2,
              "4": 4,
              "5": 5,
              "6": 3,
              "7": 3
            },
            "version": 3
          }
        }
        "###);

        insta::assert_snapshot!(serde_json::to_string_pretty(&v5).unwrap(), @r###"
        {
          "V2": {
            "boundary_accounts": [
              "aurora",
              "aurora-0",
              "earn.kaiching",
              "game.hot.tg",
              "game.hot.tg-0",
              "kkuuue2akv_1630967379.near",
              "tge-lockup.sweat"
            ],
            "shard_ids": [
              0,
              1,
              8,
              9,
              6,
              7,
              4,
              5
            ],
            "id_to_index_map": {
              "0": 0,
              "1": 1,
              "4": 6,
              "5": 7,
              "6": 4,
              "7": 5,
              "8": 2,
              "9": 3
            },
            "index_to_id_map": {
              "0": 0,
              "1": 1,
              "2": 8,
              "3": 9,
              "4": 6,
              "5": 7,
              "6": 4,
              "7": 5
            },
            "shards_split_map": {
              "0": [
                0
              ],
              "1": [
                1
              ],
              "2": [
                8,
                9
              ],
              "4": [
                4
              ],
              "5": [
                5
              ],
              "6": [
                6
              ],
              "7": [
                7
              ]
            },
            "shards_parent_map": {
              "0": 0,
              "1": 1,
              "4": 4,
              "5": 5,
              "6": 6,
              "7": 7,
              "8": 2,
              "9": 2
            },
            "version": 3
          }
        }
        "###);

        insta::assert_snapshot!(serde_json::to_string_pretty(&v6).unwrap(), @r###"
        {
          "V2": {
            "boundary_accounts": [
              "750",
              "aurora",
              "aurora-0",
              "earn.kaiching",
              "game.hot.tg",
              "game.hot.tg-0",
              "kkuuue2akv_1630967379.near",
              "tge-lockup.sweat"
            ],
            "shard_ids": [
              10,
              11,
              1,
              8,
              9,
              6,
              7,
              4,
              5
            ],
            "id_to_index_map": {
              "1": 2,
              "10": 0,
              "11": 1,
              "4": 7,
              "5": 8,
              "6": 5,
              "7": 6,
              "8": 3,
              "9": 4
            },
            "index_to_id_map": {
              "0": 10,
              "1": 11,
              "2": 1,
              "3": 8,
              "4": 9,
              "5": 6,
              "6": 7,
              "7": 4,
              "8": 5
            },
            "shards_split_map": {
              "0": [
                10,
                11
              ],
              "1": [
                1
              ],
              "4": [
                4
              ],
              "5": [
                5
              ],
              "6": [
                6
              ],
              "7": [
                7
              ],
              "8": [
                8
              ],
              "9": [
                9
              ]
            },
            "shards_parent_map": {
              "1": 1,
              "10": 0,
              "11": 0,
              "4": 4,
              "5": 5,
              "6": 6,
              "7": 7,
              "8": 8,
              "9": 9
            },
            "version": 3
          }
        }
        "###);
    }

    #[test]
    fn test_shard_layout_for_protocol_version() {
        assert_eq!(
            ShardLayout::get_simple_nightshade_layout(),
            ShardLayout::for_protocol_version(ProtocolFeature::SimpleNightshade.protocol_version())
        );
        assert_eq!(
            ShardLayout::get_simple_nightshade_layout_v2(),
            ShardLayout::for_protocol_version(
                ProtocolFeature::SimpleNightshadeV2.protocol_version()
            )
        );
        assert_eq!(
            ShardLayout::get_simple_nightshade_layout_v3(),
            ShardLayout::for_protocol_version(
                ProtocolFeature::SimpleNightshadeV3.protocol_version()
            )
        );
    }

    #[test]
    fn test_deriving_shard_layout() {
        fn to_boundary_accounts<const N: usize>(accounts: [&str; N]) -> Vec<AccountId> {
            accounts.into_iter().map(|a| a.parse().unwrap()).collect()
        }

        fn to_shard_ids<const N: usize>(ids: [u32; N]) -> Vec<ShardId> {
            ids.into_iter().map(|id| ShardId::new(id as u64)).collect()
        }

        fn to_shards_split_map<const N: usize>(
            xs: [(u32, Vec<u32>); N],
        ) -> BTreeMap<ShardId, Vec<ShardId>> {
            xs.into_iter()
                .map(|(k, xs)| {
                    (
                        ShardId::new(k as u64),
                        xs.into_iter().map(|x| ShardId::new(x as u64)).collect(),
                    )
                })
                .collect()
        }

        // [] -> ["test1"]
        // [(0, [1,2])]
        // [0] -> [1,2]
        let base_layout = ShardLayout::v2(vec![], vec![ShardId::new(0)], None);
        let derived_layout =
            ShardLayout::derive_shard_layout(&base_layout, "test1.near".parse().unwrap());
        assert_eq!(
            derived_layout,
            ShardLayout::v2(
                to_boundary_accounts(["test1.near"]),
                to_shard_ids([1, 2]),
                Some(to_shards_split_map([(0, vec![1, 2])])),
            ),
        );

        // ["test1"] -> ["test1", "test3"]
        // [(1, [1]), (2, [3, 4])]
        // [1, 2] -> [1, 3, 4]
        let base_layout = derived_layout;
        let derived_layout =
            ShardLayout::derive_shard_layout(&base_layout, "test3.near".parse().unwrap());
        assert_eq!(
            derived_layout,
            ShardLayout::v2(
                to_boundary_accounts(["test1.near", "test3.near"]),
                to_shard_ids([1, 3, 4]),
                Some(to_shards_split_map([(1, vec![1]), (2, vec![3, 4])])),
            ),
        );

        // ["test1", "test3"] -> ["test0", "test1", "test3"]
        // [(1, [5, 6]), (3, [3]), (4, [4])]
        // [1, 3, 4] -> [5, 6, 3, 4]
        let base_layout = derived_layout;
        let derived_layout =
            ShardLayout::derive_shard_layout(&base_layout, "test0.near".parse().unwrap());
        assert_eq!(
            derived_layout,
            ShardLayout::v2(
                to_boundary_accounts(["test0.near", "test1.near", "test3.near"]),
                to_shard_ids([5, 6, 3, 4]),
                Some(to_shards_split_map([(1, vec![5, 6]), (3, vec![3]), (4, vec![4]),])),
            ),
        );

        // ["test0", "test1", "test3"] -> ["test0", "test1", "test2", "test3"]
        // [(5, [5]), (6, [6]), (3, [7, 8]), (4, [4])]
        // [5, 6, 3, 4] -> [5, 6, 7, 8, 4]
        let base_layout = derived_layout;
        let derived_layout =
            ShardLayout::derive_shard_layout(&base_layout, "test2.near".parse().unwrap());
        assert_eq!(
            derived_layout,
            ShardLayout::v2(
                to_boundary_accounts(["test0.near", "test1.near", "test2.near", "test3.near"]),
                to_shard_ids([5, 6, 7, 8, 4]),
                Some(to_shards_split_map([
                    (5, vec![5]),
                    (6, vec![6]),
                    (3, vec![7, 8]),
                    (4, vec![4]),
                ])),
            )
        );
    }

    // Check that the ShardLayout::multi_shard method returns interesting shard
    // layouts. A shard layout is interesting if it has non-contiguous shard
    // ids.
    #[test]
    fn test_multi_shard_non_contiguous() {
        for n in 2..10 {
            let shard_layout = ShardLayout::multi_shard(n, 0);
            assert!(!shard_layout.shard_ids().is_sorted());
        }
    }
}
