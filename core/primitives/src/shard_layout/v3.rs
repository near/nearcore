use crate::shard_layout::{
    ShardLayout, ShardLayoutError, ShardVersion, ShardsSplitMapV2, map_keys_to_shard_id,
    map_keys_to_string,
};
use crate::types::{AccountId, EpochId};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives_core::types::{ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

/// A mapping from the parent shard to child shards. It maps shards from the
/// previous shard layout to shards that they split to in this shard layout.
/// **Only includes shards that are actually split.**
///
/// For example if a shard layout with shards [0, 2, 5] splits shard 2 into
/// shards [6, 7] the ShardSplitMap will be just: 2 => [6, 7]
pub type ShardsSplitMapV3 = BTreeMap<ShardId, Vec<ShardId>>;

/// A mapping from the child shard to the parent shard.
/// **Only includes shards that were actually split.**
type ShardsParentMapV3 = BTreeMap<ShardId, ShardId>;

fn validate_and_derive_shard_parent_map(
    shard_ids: &Vec<ShardId>,
    shards_split_map: &ShardsSplitMapV3,
) -> ShardsParentMapV3 {
    let mut shards_parent_map = ShardsParentMapV3::new();
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
    shards_parent_map
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

    /// A mapping from the parent shard to child shards. Maps shards from the
    /// previous shard layout to shards that they split to in this shard layout.
    pub(crate) shards_split_map: ShardsSplitMapV3,

    /// A mapping from the child shard to the parent shard.
    pub(crate) shards_parent_map: ShardsParentMapV3,

    /// The version of the shard layout. Starting from the `ShardLayoutV2` the
    /// version is no longer updated with every shard layout change, and it does
    /// not uniquely identify the shard layout.
    pub(crate) version: ShardVersion,

    /// ID of the last epoch of the previous layout. It is used to efficiently
    /// retrieve the previous layout (without iterating through all epochs).
    pub(crate) valid_after_epoch: EpochId,
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
    version: ShardVersion,
    valid_after_epoch: EpochId,
}

impl From<&ShardLayoutV3> for SerdeShardLayoutV3 {
    fn from(layout: &ShardLayoutV3) -> Self {
        Self {
            boundary_accounts: layout.boundary_accounts.clone(),
            shard_ids: layout.shard_ids.clone(),
            id_to_index_map: map_keys_to_string(&layout.id_to_index_map),
            shards_split_map: map_keys_to_string(&layout.shards_split_map),
            version: layout.version,
            valid_after_epoch: layout.valid_after_epoch,
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
            version,
            valid_after_epoch: valid_since_epoch,
        } = layout;

        let id_to_index_map = map_keys_to_shard_id(id_to_index_map)?;
        let shards_split_map = map_keys_to_shard_id(shards_split_map)?;
        let shards_parent_map = validate_and_derive_shard_parent_map(&shard_ids, &shards_split_map);

        Ok(Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            shards_split_map,
            shards_parent_map,
            version,
            valid_after_epoch: valid_since_epoch,
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
        shards_split_map: ShardsSplitMapV2,
        valid_since_epoch: EpochId,
    ) -> Self {
        const VERSION: ShardVersion = 3;

        assert_eq!(boundary_accounts.len() + 1, shard_ids.len());
        assert!(boundary_accounts.is_sorted());

        let id_to_index_map = shard_ids.iter().enumerate().map(|(idx, id)| (*id, idx)).collect();
        let shards_parent_map = validate_and_derive_shard_parent_map(&shard_ids, &shards_split_map);

        Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            shards_split_map,
            shards_parent_map,
            version: VERSION,
            valid_after_epoch: valid_since_epoch,
        }
    }

    pub fn derive(
        base_shard_layout: &ShardLayout,
        new_boundary_account: AccountId,
        valid_since_epoch: EpochId,
    ) -> Self {
        let mut boundary_accounts = base_shard_layout.boundary_accounts().clone();
        let new_boundary_idx = match boundary_accounts.binary_search(&new_boundary_account) {
            Ok(_) => panic!("duplicated boundary account"),
            Err(idx) => idx,
        };
        boundary_accounts.insert(new_boundary_idx, new_boundary_account);

        let mut shard_ids = base_shard_layout.shard_ids().collect_vec();
        let max_shard_id =
            *shard_ids.iter().max().expect("there should always be at least one shard");
        let new_shards = vec![max_shard_id + 1, max_shard_id + 2];

        let [parent_shard_id] = shard_ids
            .splice(new_boundary_idx..new_boundary_idx + 1, new_shards.clone())
            .collect_array()
            .expect("should only splice one shard");
        let shards_split_map = [(parent_shard_id, new_shards)].into_iter().collect();

        Self::new(boundary_accounts, shard_ids, shards_split_map, valid_since_epoch)
    }

    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let shard_idx = self.boundary_accounts.partition_point(|x| x <= account_id);
        self.shard_ids[shard_idx]
    }

    pub fn shards_split_map(&self) -> &ShardsSplitMapV3 {
        &self.shards_split_map
    }

    pub fn boundary_accounts(&self) -> &Vec<AccountId> {
        &self.boundary_accounts
    }

    pub fn get_children_shards_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        if self.shards_split_map.is_empty() {
            return None;
        }
        if let Some(children) = self.shards_split_map.get(&parent_shard_id).cloned() {
            return Some(children);
        }
        // This method is supposed to return `None` only if the layout has no parent layout.
        // Otherwise, if shard has no parent shard, it's considered its own parent.
        Some(vec![parent_shard_id])
    }

    pub fn try_get_parent_shard_id(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardId>, ShardLayoutError> {
        if !self.shard_ids.contains(&shard_id) {
            return Err(ShardLayoutError::InvalidShardIdError { shard_id });
        }
        // This method is supposed to return `None` only if the layout has no parent layout.
        // Otherwise, if shard has no parent shard, it's considered its own parent.
        if self.shards_split_map.is_empty() {
            return Ok(None);
        }
        match self.shards_parent_map.get(&shard_id) {
            Some(parent_shard_id) => Ok(Some(*parent_shard_id)),
            None => Ok(Some(shard_id)),
        }
    }

    pub fn get_shard_index(&self, shard_id: ShardId) -> Result<ShardIndex, ShardLayoutError> {
        self.id_to_index_map
            .get(&shard_id)
            .copied()
            .ok_or(ShardLayoutError::InvalidShardIdError { shard_id })
    }

    pub fn get_shard_id(&self, shard_index: ShardIndex) -> Result<ShardId, ShardLayoutError> {
        self.shard_ids
            .get(shard_index)
            .copied()
            .ok_or(ShardLayoutError::InvalidShardIndexError { shard_index })
    }
}
