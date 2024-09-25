use crate::hash::CryptoHash;
use crate::types::{AccountId, NumShards};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::{fmt, str};

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
/// `ShardLayout` also includes information needed for resharding. In particular, it encodes
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
type ShardsSplitMap = Vec<Vec<ShardId>>;

/// A mapping from the parent shard to child shards. A map that maps shards from the
/// last shard layout to shards that it splits to in this shard layout. This
/// structure is first used in ShardLayoutV2. For example if a shard layout with
/// shards [0, 2, 5] splits shard 2 into shards [6, 7] the ShardSplitMapV3 will
/// be: 0 => [0] 2 => [6, 7] 5 => [5]
type ShardsSplitMapV2 = BTreeMap<ShardId, Vec<ShardId>>;

/// A mapping from the child shard to the parent shard.
type ShardsParentMapV2 = BTreeMap<ShardId, ShardId>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
        // Note: As we scale up the number of shards we can consider
        // changing this method to do a binary search rather than linear
        // scan. For the time being, with only 4 shards, this is perfectly fine.
        let mut shard_id: ShardId = 0;
        for boundary_account in &self.boundary_accounts {
            if account_id < boundary_account {
                break;
            }
            shard_id += 1;
        }
        shard_id
    }
}

/// A boundary of an account range. Can be the start, the end or an account id
/// in between two shards. For example a shard layout with four shards would
/// have one start boundary, three middle boundaries and one end boundary.
/// e.g. Start, Middle("ccc"), Middle("kkk"), Middle("ppp"), End
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Ord)]
pub enum AccountBoundary {
    Start,
    End,
    Middle(AccountId),
}

impl fmt::Debug for AccountBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountBoundary::Start => write!(f, "start"),
            AccountBoundary::End => write!(f, "end"),
            AccountBoundary::Middle(account_id) => write!(f, "{}", account_id.as_str()),
        }
    }
}

impl PartialOrd for AccountBoundary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (AccountBoundary::Start, AccountBoundary::Start) => Some(Ordering::Equal),
            (AccountBoundary::End, AccountBoundary::End) => Some(Ordering::Equal),

            (AccountBoundary::Start, AccountBoundary::End) => Some(Ordering::Less),
            (AccountBoundary::End, AccountBoundary::Start) => Some(Ordering::Greater),

            (AccountBoundary::Start, AccountBoundary::Middle(_)) => Some(Ordering::Less),
            (AccountBoundary::Middle(_), AccountBoundary::Start) => Some(Ordering::Greater),

            (AccountBoundary::End, AccountBoundary::Middle(_)) => Some(Ordering::Greater),
            (AccountBoundary::Middle(_), AccountBoundary::End) => Some(Ordering::Less),

            (AccountBoundary::Middle(a), AccountBoundary::Middle(b)) => a.partial_cmp(b),
        }
    }
}

/// The account range of a single shard.
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Ord)]
pub struct AccountRange {
    start: AccountBoundary,
    end: AccountBoundary,
}

impl PartialOrd for AccountRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.start.partial_cmp(&other.start) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.end.partial_cmp(&other.end)
    }
}

impl fmt::Debug for AccountRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let range = format!("{:?}..{:?}", self.start, self.end);
        f.debug_struct("AccountRange").field("range", &range).finish()
    }
}

impl AccountRange {
    pub fn new_single_shard() -> Self {
        Self { start: AccountBoundary::Start, end: AccountBoundary::End }
    }

    pub fn new_start(end: &str) -> Self {
        Self { start: AccountBoundary::Start, end: AccountBoundary::Middle(end.parse().unwrap()) }
    }

    pub fn new_end(start: &str) -> Self {
        Self { start: AccountBoundary::Middle(start.parse().unwrap()), end: AccountBoundary::End }
    }

    pub fn new_mid(start: &str, end: &str) -> Self {
        Self {
            start: AccountBoundary::Middle(start.parse().unwrap()),
            end: AccountBoundary::Middle(end.parse().unwrap()),
        }
    }

    fn contains(&self, account_id: &AccountId) -> bool {
        match (&self.start, &self.end) {
            (AccountBoundary::Start, AccountBoundary::End) => true,
            (AccountBoundary::Start, AccountBoundary::Middle(end)) => account_id < end,
            (AccountBoundary::Middle(start), AccountBoundary::End) => account_id >= start,
            (AccountBoundary::Middle(start), AccountBoundary::Middle(end)) => {
                account_id >= start && account_id < end
            }
            (AccountBoundary::End, _) => panic!("invalid account range"),
            (_, AccountBoundary::Start) => panic!("invalid account range"),
        }
    }
}

/// A mapping from the shard id to the account range of this shard.
type ShardsAccountRange = BTreeMap<ShardId, AccountRange>;

fn validate_shards_account_range(
    shards_account_range: &ShardsAccountRange,
) -> Result<(), ShardLayoutError> {
    let err =
        |reason: &str| ShardLayoutError::InvalidShardsAccountRange { reason: reason.to_string() };

    let values = shards_account_range.values().sorted().collect_vec();

    let (Some(first), Some(last)) = (values.first(), values.last()) else {
        return Err(err("account range empty"));
    };
    if first.start != AccountBoundary::Start {
        return Err(err("first account range should start with Start"));
    }
    if last.end != AccountBoundary::End {
        return Err(err("last account range should end with End"));
    }

    for i in 1..values.len() {
        let prev = values[i - 1];
        let curr = values[i];
        if prev.end != curr.start {
            return Err(err(&format!(
                "account ranges should be contiguous, {:?} != {:?}",
                prev.end, curr.start
            )));
        }
    }

    Ok(())
}

/// Making the shard ids non-contiguous.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ShardLayoutV2 {
    /// A mapping from the shard id to the account range of this shard.
    shards_account_range: ShardsAccountRange,

    /// A mapping from the parent shard to child shards. Maps shards from the
    /// previous shard layout to shards that they split to in this shard layout.
    shards_split_map: Option<ShardsSplitMapV2>,
    /// A mapping from the child shard to the parent shard. Maps shards in this
    /// shard layout to their parent shards.
    shards_parent_map: Option<ShardsParentMapV2>,

    /// Version of the shard layout, this is useful for uniquely identify the shard layout
    version: ShardVersion,
}

impl ShardLayoutV2 {
    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        // TODO(resharding) - This could be optimized.
        for (&shard_id, account_range) in &self.shards_account_range {
            if account_range.contains(account_id) {
                return shard_id;
            }
        }
        panic!("account_id_to_shard_id: account_id not found in any shard")
    }
}

#[derive(Debug)]
pub enum ShardLayoutError {
    InvalidShardIdError { shard_id: ShardId },
    InvalidShardsAccountRange { reason: String },
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
        boundary_accounts: Vec<AccountId>,
        shards_split_map: Option<ShardsSplitMap>,
        version: ShardVersion,
    ) -> Self {
        let to_parent_shard_map = if let Some(shards_split_map) = &shards_split_map {
            let mut to_parent_shard_map = HashMap::new();
            let num_shards = (boundary_accounts.len() + 1) as NumShards;
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
            boundary_accounts,
            shards_split_map,
            to_parent_shard_map,
            version,
        })
    }

    /// Return a V2 Shardlayout
    pub fn v2(
        shards_account_range: BTreeMap<ShardId, AccountRange>,
        shards_split_map: Option<ShardsSplitMapV2>,
        version: ShardVersion,
    ) -> Self {
        validate_shards_account_range(&shards_account_range).unwrap();

        let Some(shards_split_map) = shards_split_map else {
            return Self::V2(ShardLayoutV2 {
                shards_account_range,
                shards_split_map: None,
                shards_parent_map: None,
                version,
            });
        };

        let mut shards_parent_map = ShardsParentMapV2::new();
        for (&parent_shard_id, shard_ids) in shards_split_map.iter() {
            for &shard_id in shard_ids {
                let prev = shards_parent_map.insert(shard_id, parent_shard_id);
                assert!(prev.is_none(), "no shard should appear in the map twice");
            }
        }

        assert_eq!(
            shards_account_range.keys().collect_vec(),
            shards_parent_map.keys().collect_vec()
        );

        let shards_split_map = Some(shards_split_map);
        let shards_parent_map = Some(shards_parent_map);
        Self::V2(ShardLayoutV2 {
            shards_account_range,
            shards_split_map,
            shards_parent_map,
            version,
        })
    }

    /// Returns a V1 ShardLayout. It is only used in tests
    pub fn v1_test() -> Self {
        ShardLayout::v1(
            vec!["abc", "foo", "test0"].into_iter().map(|s| s.parse().unwrap()).collect(),
            Some(vec![vec![0, 1, 2, 3]]),
            1,
        )
    }

    /// Returns the simple nightshade layout that we use in production
    pub fn get_simple_nightshade_layout() -> ShardLayout {
        ShardLayout::v1(
            vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            Some(vec![vec![0, 1, 2, 3]]),
            1,
        )
    }

    /// Returns the simple nightshade layout, version 2, that will be used in production.
    pub fn get_simple_nightshade_layout_v2() -> ShardLayout {
        ShardLayout::v1(
            vec!["aurora", "aurora-0", "kkuuue2akv_1630967379.near", "tge-lockup.sweat"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            Some(vec![vec![0], vec![1], vec![2], vec![3, 4]]),
            2,
        )
    }

    /// Returns the simple nightshade layout, version 3, that will be used in production.
    pub fn get_simple_nightshade_layout_v3() -> ShardLayout {
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
            Some(vec![vec![0], vec![1], vec![2, 3], vec![4], vec![5]]),
            3,
        )
    }

    /// Returns the simple nightshade layout, version 4, that will be used in
    /// production. It adds a new boundary account splitting the "game.hot.tg"
    /// shard into two smaller shards. This is the first layout used in the
    /// Instant Resharding and it is the first one where the shard id contiguity
    /// is broken.
    ///
    /// TODO(resharding) Determine the shard layout for v4.
    /// This layout is provisional, the actual shard layout should be determined
    /// based on the fresh data before the resharding.
    pub fn get_simple_nightshade_layout_v4() -> ShardLayout {
        let s0 = AccountRange::new_start("aurora");
        let s1 = AccountRange::new_mid("aurora", "aurora-0");
        let s6 = AccountRange::new_mid("aurora-0", "game.hot.tg");
        let s7 = AccountRange::new_mid("game.hot.tg", "game.hot.tg-0");
        let s3 = AccountRange::new_mid("game.hot.tg-0", "kkuuue2akv_1630967379.near");
        let s4 = AccountRange::new_mid("kkuuue2akv_1630967379.near", "tge-lockup.sweat");
        let s5 = AccountRange::new_end("tge-lockup.sweat");
        let shards_account_range =
            BTreeMap::from([(0, s0), (1, s1), (6, s6), (7, s7), (3, s3), (4, s4), (5, s5)]);

        let shards_split_map = BTreeMap::from([
            (0, vec![0]),
            (1, vec![1]),
            (2, vec![6, 7]),
            (3, vec![3]),
            (4, vec![4]),
            (5, vec![5]),
        ]);
        let shards_split_map = Some(shards_split_map);

        // The shard layout version stays the same. Starting from version 3 the
        // shard version is no longer updated with every shard layout change.
        let version = 3;

        ShardLayout::v2(shards_account_range, shards_split_map, version)
    }

    /// This layout is used only in resharding tests. It allows testing of any features which were
    /// introduced after the last layout upgrade in production. Currently it is built on top of V3.
    #[cfg(feature = "nightly")]
    pub fn get_simple_nightshade_layout_testonly() -> ShardLayout {
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
            Some(vec![vec![0], vec![1], vec![2], vec![3], vec![4, 5], vec![6]]),
            4,
        )
    }

    /// Given a parent shard id, return the shard uids for the shards in the current shard layout that
    /// are split from this parent shard. If this shard layout has no parent shard layout, return None
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
                Some(shards_split_map) => shards_split_map.get(parent_shard_id as usize).cloned(),
                None => None,
            },
            Self::V2(v2) => match &v2.shards_split_map {
                Some(shards_split_map) => shards_split_map.get(&parent_shard_id).cloned(),
                None => None,
            },
        }
    }

    /// Return the parent shard id for a given shard in the shard layout
    /// Only calls this function for shard layout that has parent shard layouts
    /// Returns error if `shard_id` is an invalid shard id in the current layout
    /// Panics if `self` has no parent shard layout
    pub fn get_parent_shard_id(&self, shard_id: ShardId) -> Result<ShardId, ShardLayoutError> {
        if !self.shard_ids().any(|id| id == shard_id) {
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
            Self::V2(v2) => match &v2.shards_parent_map {
                Some(to_parent_shard_map) => *to_parent_shard_map.get(&shard_id).unwrap(),
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
            Self::V2(v2) => v2.version,
        }
    }

    fn num_shards(&self) -> NumShards {
        match self {
            Self::V0(v0) => v0.num_shards,
            Self::V1(v1) => (v1.boundary_accounts.len() + 1) as NumShards,
            Self::V2(v2) => (v2.shards_account_range.len()) as NumShards,
        }
    }

    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        match self {
            Self::V0(_) => (0..self.num_shards()).collect_vec().into_iter(),
            Self::V1(_) => (0..self.num_shards()).collect_vec().into_iter(),
            Self::V2(v2) => v2.shards_account_range.keys().copied().collect_vec().into_iter(),
        }
    }

    /// Returns an iterator that iterates over all the shard uids for all the
    /// shards in the shard layout
    pub fn shard_uids(&self) -> impl Iterator<Item = ShardUId> + '_ {
        self.shard_ids().map(|shard_id| ShardUId::from_shard_id_and_layout(shard_id, self))
    }
}

/// Maps an account to the shard that it belongs to given a shard_layout
/// For V0, maps according to hash of account id
/// For V1 and V2, accounts are divided to ranges, each range of account is mapped to a shard.
pub fn account_id_to_shard_id(account_id: &AccountId, shard_layout: &ShardLayout) -> ShardId {
    match shard_layout {
        ShardLayout::V0(ShardLayoutV0 { num_shards, .. }) => {
            let hash = CryptoHash::hash_bytes(account_id.as_bytes());
            let (bytes, _) = stdx::split_array::<32, 8, 24>(hash.as_bytes());
            u64::from_le_bytes(*bytes) % num_shards
        }
        ShardLayout::V1(v1) => v1.account_id_to_shard_id(account_id),
        ShardLayout::V2(v2) => v2.account_id_to_shard_id(account_id),
    }
}

/// Maps an account to the shard that it belongs to given a shard_layout
pub fn account_id_to_shard_uid(account_id: &AccountId, shard_layout: &ShardLayout) -> ShardUId {
    ShardUId::from_shard_id_and_layout(
        account_id_to_shard_id(account_id, shard_layout),
        shard_layout,
    )
}

/// ShardUId is an unique representation for shards from different shard layout
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

    // TODO(wacban) What the heck is that and how does it play with
    // non-contiguous shard ids?
    pub fn next_shard_prefix(shard_uid_bytes: &[u8; 8]) -> [u8; 8] {
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

#[cfg(test)]
mod tests {
    use crate::epoch_manager::{AllEpochConfig, EpochConfig, ValidatorSelectionConfig};
    use crate::shard_layout::{
        account_id_to_shard_id, validate_shards_account_range, AccountRange, ShardLayout,
        ShardLayoutV1, ShardUId, ShardsAccountRange,
    };
    use near_primitives_core::types::ProtocolVersion;
    use near_primitives_core::types::{AccountId, ShardId};
    use near_primitives_core::version::{ProtocolFeature, PROTOCOL_VERSION};
    use rand::distributions::Alphanumeric;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;

    use super::{ShardVersion, ShardsSplitMap};

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
            // none of the epoch config fields matter, we only need the shard layout
            // constructed through [`AllEpochConfig::for_protocol_version()`].
            let genesis_epoch_config = EpochConfig {
                epoch_length: 0,
                num_block_producer_seats: 0,
                num_block_producer_seats_per_shard: vec![],
                avg_hidden_validator_seats_per_shard: vec![],
                block_producer_kickout_threshold: 0,
                chunk_producer_kickout_threshold: 0,
                chunk_validator_only_kickout_threshold: 0,
                target_validator_mandates_per_shard: 0,
                validator_max_kickout_stake_perc: 0,
                online_min_threshold: 0.into(),
                online_max_threshold: 0.into(),
                fishermen_threshold: 0,
                minimum_stake_divisor: 0,
                protocol_upgrade_stake_threshold: 0.into(),
                shard_layout: ShardLayout::get_simple_nightshade_layout(),
                validator_selection_config: ValidatorSelectionConfig::default(),
            };

            let genesis_protocol_version = PROTOCOL_VERSION;
            let all_epoch_config = AllEpochConfig::new(
                true,
                genesis_protocol_version,
                genesis_epoch_config,
                "test-chain",
            );
            let latest_epoch_config = all_epoch_config.for_protocol_version(protocol_version);
            latest_epoch_config.shard_layout
        }
    }

    #[test]
    fn test_shard_layout_v0() {
        let num_shards = 4;
        let shard_layout = ShardLayout::v0(num_shards, 0);
        let mut shard_id_distribution: HashMap<_, _> =
            shard_layout.shard_ids().map(|shard_id| (shard_id, 0)).collect();
        let mut rng = StdRng::from_seed([0; 32]);
        for _i in 0..1000 {
            let s: Vec<u8> = (&mut rng).sample_iter(&Alphanumeric).take(10).collect();
            let s = String::from_utf8(s).unwrap();
            let account_id = s.to_lowercase().parse().unwrap();
            let shard_id = account_id_to_shard_id(&account_id, &shard_layout);
            assert!(shard_id < num_shards);
            *shard_id_distribution.get_mut(&shard_id).unwrap() += 1;
        }
        let expected_distribution: HashMap<_, _> =
            [(0, 247), (1, 268), (2, 233), (3, 252)].into_iter().collect();
        assert_eq!(shard_id_distribution, expected_distribution);
    }

    #[test]
    fn test_shard_layout_v1() {
        let shard_layout = ShardLayout::v1(
            parse_account_ids(&["aurora", "bar", "foo", "foo.baz", "paz"]),
            Some(vec![vec![0, 1, 2], vec![3, 4, 5]]),
            1,
        );
        assert_eq!(
            shard_layout.get_children_shards_uids(0).unwrap(),
            (0..3).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        assert_eq!(
            shard_layout.get_children_shards_uids(1).unwrap(),
            (3..6).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
        );
        for x in 0..3 {
            assert_eq!(shard_layout.get_parent_shard_id(x).unwrap(), 0);
            assert_eq!(shard_layout.get_parent_shard_id(x + 3).unwrap(), 1);
        }

        assert_eq!(account_id_to_shard_id(&"aurora".parse().unwrap(), &shard_layout), 1);
        assert_eq!(account_id_to_shard_id(&"foo.aurora".parse().unwrap(), &shard_layout), 3);
        assert_eq!(account_id_to_shard_id(&"bar.foo.aurora".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"bar".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"bar.bar".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"foo".parse().unwrap(), &shard_layout), 3);
        assert_eq!(account_id_to_shard_id(&"baz.foo".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"foo.baz".parse().unwrap(), &shard_layout), 4);
        assert_eq!(account_id_to_shard_id(&"a.foo.baz".parse().unwrap(), &shard_layout), 0);

        assert_eq!(account_id_to_shard_id(&"aaa".parse().unwrap(), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&"abc".parse().unwrap(), &shard_layout), 0);
        assert_eq!(account_id_to_shard_id(&"bbb".parse().unwrap(), &shard_layout), 2);
        assert_eq!(account_id_to_shard_id(&"foo.goo".parse().unwrap(), &shard_layout), 4);
        assert_eq!(account_id_to_shard_id(&"goo".parse().unwrap(), &shard_layout), 4);
        assert_eq!(account_id_to_shard_id(&"zoo".parse().unwrap(), &shard_layout), 5);
    }

    // check that after removing the fixed shards from the shard layout v1
    // the fixed shards are skipped in deserialization
    // this should be the default as long as serde(deny_unknown_fields) is not set
    #[test]
    fn test_remove_fixed_shards() {
        let old = OldShardLayoutV1 {
            fixed_shards: vec![],
            boundary_accounts: parse_account_ids(&["aaa", "bbb"]),
            shards_split_map: Some(vec![vec![0, 1, 2]]),
            to_parent_shard_map: Some(vec![0, 0, 0]),
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
    fn test_validate_shards_account_range() {
        // one shard
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(0, AccountRange::new_single_shard());
        validate_shards_account_range(&shards_account_range).unwrap();

        // two shards
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(0, AccountRange::new_start("middle"));
        shards_account_range.insert(1, AccountRange::new_end("middle"));
        validate_shards_account_range(&shards_account_range).unwrap();

        // more shards, non-contiguous
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(10, AccountRange::new_start("ccc"));
        shards_account_range.insert(5, AccountRange::new_mid("ccc", "kkk"));
        shards_account_range.insert(7, AccountRange::new_mid("kkk", "ppp"));
        shards_account_range.insert(1, AccountRange::new_end("ppp"));
        validate_shards_account_range(&shards_account_range).unwrap();

        // no start
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(5, AccountRange::new_mid("ccc", "kkk"));
        shards_account_range.insert(7, AccountRange::new_mid("kkk", "ppp"));
        shards_account_range.insert(1, AccountRange::new_end("ppp"));
        validate_shards_account_range(&shards_account_range).unwrap_err();

        // no end
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(10, AccountRange::new_start("ccc"));
        shards_account_range.insert(5, AccountRange::new_mid("ccc", "kkk"));
        shards_account_range.insert(7, AccountRange::new_mid("kkk", "ppp"));
        validate_shards_account_range(&shards_account_range).unwrap_err();

        // hole
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(10, AccountRange::new_start("ccc"));
        shards_account_range.insert(5, AccountRange::new_mid("ccc", "kkk"));
        shards_account_range.insert(1, AccountRange::new_end("ppp"));
        validate_shards_account_range(&shards_account_range).unwrap_err();

        // overlap
        let mut shards_account_range = ShardsAccountRange::new();
        shards_account_range.insert(10, AccountRange::new_start("ccc"));
        shards_account_range.insert(5, AccountRange::new_mid("ccc", "mmm"));
        shards_account_range.insert(7, AccountRange::new_mid("kkk", "ppp"));
        shards_account_range.insert(1, AccountRange::new_end("ppp"));
        validate_shards_account_range(&shards_account_range).unwrap_err();
    }

    #[test]
    fn test_shard_layout_v4() {
        // Test ShardsAccountRange validation

        // Test account id to shard id
    }

    #[test]
    fn test_shard_layout_all() {
        let v0 = ShardLayout::v0(1, 0);
        let v1 = ShardLayout::get_simple_nightshade_layout();
        let v2 = ShardLayout::get_simple_nightshade_layout_v2();
        let v3 = ShardLayout::get_simple_nightshade_layout_v3();
        let v4 = ShardLayout::get_simple_nightshade_layout_v4();

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
            "shards_account_range": {
              "0": {
                "start": "Start",
                "end": {
                  "Middle": "aurora"
                }
              },
              "1": {
                "start": {
                  "Middle": "aurora"
                },
                "end": {
                  "Middle": "aurora-0"
                }
              },
              "3": {
                "start": {
                  "Middle": "game.hot.tg-0"
                },
                "end": {
                  "Middle": "kkuuue2akv_1630967379.near"
                }
              },
              "4": {
                "start": {
                  "Middle": "kkuuue2akv_1630967379.near"
                },
                "end": {
                  "Middle": "tge-lockup.sweat"
                }
              },
              "5": {
                "start": {
                  "Middle": "tge-lockup.sweat"
                },
                "end": "End"
              },
              "6": {
                "start": {
                  "Middle": "aurora-0"
                },
                "end": {
                  "Middle": "game.hot.tg"
                }
              },
              "7": {
                "start": {
                  "Middle": "game.hot.tg"
                },
                "end": {
                  "Middle": "game.hot.tg-0"
                }
              }
            },
            "shards_split_map": {
              "0": [
                0
              ],
              "1": [
                1
              ],
              "2": [
                6,
                7
              ],
              "3": [
                3
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
              "3": 3,
              "4": 4,
              "5": 5,
              "6": 2,
              "7": 2
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
}
