use crate::shard_layout::utils::{map_keys_to_shard_id, map_keys_to_string};
use crate::shard_layout::{ShardLayout, ShardLayoutError, ShardUId, ShardVersion};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives_core::types::{ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

/// A mapping from the parent shard to child shards. It maps shards from the
/// previous shard layout to shards that they split to in this shard layout.
/// Unlike previous versions of `ShardsSplitMap`, this one:
///   * Only includes shards that are actually split.
///   * Includes the full history of shard splits, i.e. split map of the current
///     layout is a superset of the split map of its parent layout.
///
/// For example if a shard layout with shards `[0, 2, 3, 4]` and split map `{1 => [3, 4]}`
/// splits shard 2 into shards [5, 6] the ShardSplitMap in the resulting layout will be:
/// `{1 => [3, 4], 2 => [5, 6]}`.
pub type ShardsSplitMapV3 = BTreeMap<ShardId, Vec<ShardId>>;

/// A mapping from the child shard to all its ancestors. Parent shard is the first
/// element of the ancestors vector, 'grandparent' shard is the second element, etc.
/// IDs of shards which have no ancestors (i.e. were *not* created by a split) are
/// not present in the mapping.
type ShardsAncestorMapV3 = BTreeMap<ShardId, Vec<ShardId>>;

const VERSION: ShardVersion = 3;

fn validate_and_derive_shard_ancestor_map(
    shard_ids: &Vec<ShardId>,
    shards_split_map: &ShardsSplitMapV3,
) -> ShardsAncestorMapV3 {
    let mut shards_parent_map = BTreeMap::new();
    for (&parent_shard_id, child_shard_ids) in shards_split_map {
        assert!(
            !shard_ids.contains(&parent_shard_id),
            "shard that is split should no longer be used"
        );
        assert!(child_shard_ids.len() > 1, "shard must be split into at least two children");
        for &child_shard_id in child_shard_ids {
            let prev = shards_parent_map.insert(child_shard_id, parent_shard_id);
            assert!(prev.is_none(), "no shard should appear in the map twice");
        }
    }

    let mut shards_ancestor_map = ShardsAncestorMapV3::new();
    for shard_id in shard_ids {
        let mut ancestors = vec![];
        let mut current_id = *shard_id;
        while let Some(parent_shard_id) = shards_parent_map.get(&current_id) {
            ancestors.push(*parent_shard_id);
            current_id = *parent_shard_id;
        }
        shards_ancestor_map.insert(*shard_id, ancestors);
    }
    shards_ancestor_map
}

/// Build `ShardsSplitMapV3` from a sequence of previous shard layouts.
///
/// Assumes that layouts are ordered from newest to oldest, and that there are no duplicates.
/// Ignores layouts with `version()` lower than `VERSION` (this is **not** the struct version).
pub fn build_shard_split_map(layout_history: &[ShardLayout]) -> ShardsSplitMapV3 {
    let mut split_history = ShardsSplitMapV3::new();

    for window in layout_history.windows(2) {
        let current_layout = &window[0];
        let prev_layout = &window[1];

        if current_layout.version() < VERSION || prev_layout.version() < VERSION {
            break;
        }

        debug_assert_ne!(current_layout, prev_layout);

        for shard_id in current_layout.shard_ids() {
            match current_layout.try_get_parent_shard_id(shard_id).expect("invalid shard_id") {
                Some(parent_id) if parent_id != shard_id => {
                    split_history.entry(parent_id).or_default().push(shard_id);
                }
                _ => continue,
            }
        }
    }

    split_history
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ShardLayoutV3 {
    /// The boundary accounts are the accounts on boundaries between shards.
    /// Each shard contains a range of accounts from one boundary account to
    /// another - or the smallest or largest account possible. The total
    /// number of shards is equal to the number of boundary accounts plus 1.
    pub(crate) boundary_accounts: Vec<AccountId>,

    /// The shard ids corresponding to the shards defined by the boundary
    /// accounts. The invariant between `boundary_accounts` and `shard_ids` is that
    /// `boundary_accounts.len() + 1 == shard_ids.len()`.
    ///
    /// The shard id at index `i` corresponds to the shard with account range:
    /// `[boundary_accounts[i -1], boundary_accounts[i])`.
    ///
    /// The shard ids do not need to be contiguous or sorted.
    pub(crate) shard_ids: Vec<ShardId>,

    /// The mapping from shard id to shard index.
    pub(crate) id_to_index_map: BTreeMap<ShardId, ShardIndex>,

    /// A mapping from the parent shard to child shards. Maps shards from all
    /// previous shard layouts to shards that they were split into.
    pub(crate) shards_split_map: ShardsSplitMapV3,

    /// The most recent shard split (parent shard ID).
    pub(crate) last_split: ShardId,

    /// A mapping from the child shard to ancestor shards.
    pub(crate) shards_ancestor_map: ShardsAncestorMapV3,
}

/// Counterpart to `ShardLayoutV3` composed of maps with string keys to aid
/// serde serialization.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
struct SerdeShardLayoutV3 {
    boundary_accounts: Vec<AccountId>,
    shard_ids: Vec<ShardId>,
    id_to_index_map: BTreeMap<String, ShardIndex>,
    shards_split_map: BTreeMap<String, Vec<ShardId>>,
    last_split: ShardId,
}

impl From<&ShardLayoutV3> for SerdeShardLayoutV3 {
    fn from(layout: &ShardLayoutV3) -> Self {
        Self {
            boundary_accounts: layout.boundary_accounts.clone(),
            shard_ids: layout.shard_ids.clone(),
            id_to_index_map: map_keys_to_string(&layout.id_to_index_map),
            shards_split_map: map_keys_to_string(&layout.shards_split_map),
            last_split: layout.last_split,
        }
    }
}

impl TryFrom<SerdeShardLayoutV3> for ShardLayoutV3 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(layout: SerdeShardLayoutV3) -> Result<Self, Self::Error> {
        let SerdeShardLayoutV3 {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            shards_split_map,
            last_split,
        } = layout;

        let id_to_index_map = map_keys_to_shard_id(id_to_index_map)?;
        let shards_split_map = map_keys_to_shard_id(shards_split_map)?;
        let shards_ancestor_map =
            validate_and_derive_shard_ancestor_map(&shard_ids, &shards_split_map);

        Ok(Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            shards_split_map,
            last_split,
            shards_ancestor_map,
        })
    }
}

impl serde::Serialize for ShardLayoutV3 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerdeShardLayoutV3::from(self).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ShardLayoutV3 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serde_layout = SerdeShardLayoutV3::deserialize(deserializer)?;
        ShardLayoutV3::try_from(serde_layout).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ShardLayoutV3 {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ShardLayoutV3".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        SerdeShardLayoutV3::json_schema(generator)
    }
}

impl ShardLayoutV3 {
    pub fn new(
        boundary_accounts: Vec<AccountId>,
        shard_ids: Vec<ShardId>,
        shards_split_map: ShardsSplitMapV3,
        last_split: ShardId,
    ) -> Self {
        assert_eq!(boundary_accounts.len() + 1, shard_ids.len());
        assert!(boundary_accounts.is_sorted());
        assert!(shards_split_map.get(&last_split).is_some_and(|children| !children.is_empty()));

        let id_to_index_map = shard_ids.iter().enumerate().map(|(idx, id)| (*id, idx)).collect();
        let shards_ancestor_map =
            validate_and_derive_shard_ancestor_map(&shard_ids, &shards_split_map);

        Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            shards_split_map,
            last_split,
            shards_ancestor_map,
        }
    }

    /// Derive a V3 shard layout from an existing V3 `base_shard_layout`.
    ///
    /// Returns an error if `new_boundary_account` already exists in `base_shard_layout`.
    pub fn derive(
        base_shard_layout: &Self,
        new_boundary_account: AccountId,
    ) -> Result<Self, ShardLayoutError> {
        let shard_ids = base_shard_layout.shard_ids.clone();
        let boundary_accounts = base_shard_layout.boundary_accounts.clone();
        let shards_split_map = base_shard_layout.shards_split_map.clone();
        Self::derive_impl(shard_ids, boundary_accounts, new_boundary_account, shards_split_map)
    }

    /// Derive a V3 shard layout from an earlier version (V1/V2) using a sequence
    /// of previous shard layouts. The `layout_history` should be ordered from most
    /// recent to oldest.
    ///
    /// Returns an error if `new_boundary_account` already exists in `base_shard_layout`.
    pub fn derive_with_layout_history(
        base_shard_layout: &ShardLayout,
        new_boundary_account: AccountId,
        layout_history: &[ShardLayout],
    ) -> Result<Self, ShardLayoutError> {
        let shard_ids = base_shard_layout.shard_ids().collect();
        let boundary_accounts = base_shard_layout.boundary_accounts().clone();
        let shards_split_map = build_shard_split_map(layout_history);
        Self::derive_impl(shard_ids, boundary_accounts, new_boundary_account, shards_split_map)
    }

    fn derive_impl(
        mut shard_ids: Vec<ShardId>,
        mut boundary_accounts: Vec<AccountId>,
        new_boundary_account: AccountId,
        mut shards_split_map: ShardsSplitMapV3,
    ) -> Result<Self, ShardLayoutError> {
        let Err(new_boundary_idx) = boundary_accounts.binary_search(&new_boundary_account) else {
            return Err(ShardLayoutError::DuplicateBoundaryAccount {
                account_id: new_boundary_account,
            });
        };
        boundary_accounts.insert(new_boundary_idx, new_boundary_account);

        let max_shard_id =
            *shard_ids.iter().max().expect("there should always be at least one shard");
        let new_shards = vec![max_shard_id + 1, max_shard_id + 2];

        let [last_split] = shard_ids
            .splice(new_boundary_idx..new_boundary_idx + 1, new_shards.clone())
            .collect_array()
            .expect("should only splice one shard");
        shards_split_map.insert(last_split, new_shards);

        Ok(Self::new(boundary_accounts, shard_ids, shards_split_map, last_split))
    }

    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let shard_idx = self.boundary_accounts.partition_point(|x| x <= account_id);
        self.shard_ids[shard_idx]
    }

    pub fn shards_split_map(&self) -> &ShardsSplitMapV3 {
        &self.shards_split_map
    }

    #[inline]
    fn last_split_children(&self) -> &Vec<ShardId> {
        self.shards_split_map.get(&self.last_split).expect("last split should have children")
    }

    /// Get a map containing only the most recent shard split.
    pub fn recent_split(&self) -> BTreeMap<ShardId, Vec<ShardId>> {
        [(self.last_split, self.last_split_children().clone())].into_iter().collect()
    }

    /// Get UIDs of all the shard's ancestors (parents, grandparents, etc.)
    pub fn ancestor_uids(&self, shard_id: ShardId) -> Vec<ShardUId> {
        self.shards_ancestor_map
            .get(&shard_id)
            .map(|ancestor_ids| ancestor_ids.iter().map(|id| ShardUId::new(VERSION, *id)).collect())
            .unwrap_or_default()
    }

    pub fn boundary_accounts(&self) -> &Vec<AccountId> {
        &self.boundary_accounts
    }

    /// Get children shard IDs if the given parent shard was split during the most recent resharding.
    /// Otherwise, return `parent_shard_id` if shard exists in the parent layout, or `None` if it doesn't.
    pub fn get_children_shards_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        if parent_shard_id == self.last_split {
            Some(self.last_split_children().clone())
        } else if self.shard_ids.contains(&parent_shard_id)
            && !self.last_split_children().contains(&parent_shard_id)
        {
            Some(vec![parent_shard_id])
        } else {
            None
        }
    }

    /// Get parent shard ID if the given shard was created in the most recent resharding.
    /// Otherwise, return `shard_id`, or `InvalidShardId` error if the shard doesn't exist.
    pub fn try_get_parent_shard_id(&self, shard_id: ShardId) -> Result<ShardId, ShardLayoutError> {
        if !self.shard_ids.contains(&shard_id) {
            return Err(ShardLayoutError::InvalidShardId { shard_id });
        }

        if self.last_split_children().contains(&shard_id) {
            Ok(self.last_split)
        } else {
            Ok(shard_id)
        }
    }

    pub fn get_shard_index(&self, shard_id: ShardId) -> Result<ShardIndex, ShardLayoutError> {
        self.id_to_index_map
            .get(&shard_id)
            .copied()
            .ok_or(ShardLayoutError::InvalidShardId { shard_id })
    }

    pub fn get_shard_id(&self, shard_index: ShardIndex) -> Result<ShardId, ShardLayoutError> {
        self.shard_ids
            .get(shard_index)
            .copied()
            .ok_or(ShardLayoutError::InvalidShardIndex { shard_index })
    }

    #[inline]
    pub fn version(&self) -> ShardVersion {
        VERSION
    }
}
