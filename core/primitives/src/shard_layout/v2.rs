use crate::shard_layout::utils::{map_keys_to_shard_id, map_keys_to_string};
use crate::shard_layout::{ShardLayout, ShardLayoutError, ShardVersion};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_primitives_core::types::{ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

/// A mapping from the parent shard to child shards. It maps shards from the
/// previous shard layout to shards that they split to in this shard layout.
/// This structure is first used in ShardLayoutV2.
///
/// For example if a shard layout with shards [0, 2, 5] splits shard 2 into
/// shards [6, 7] the ShardSplitMapV2 will be: 0 => [0] 2 => [6, 7] 5 => [5]
pub type ShardsSplitMapV2 = BTreeMap<ShardId, Vec<ShardId>>;

/// A mapping from the child shard to the parent shard.
pub type ShardsParentMapV2 = BTreeMap<ShardId, ShardId>;

// Validates the shards_split_map and derives the shards_parent_map from it.
fn validate_and_derive_shard_parent_map(
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

/// Making the shard ids non-contiguous.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ShardLayoutV2 {
    /// The boundary accounts are the accounts on boundaries between shards.
    /// Each shard contains a range of accounts from one boundary account to
    /// another - or the smallest or largest account possible. The total
    /// number of shards is equal to the number of boundary accounts plus 1.
    ///
    /// The shard ids do not need to be contiguous or sorted.
    pub(crate) boundary_accounts: Vec<AccountId>,

    /// The shard ids corresponding to the shards defined by the boundary
    /// accounts. The invariant between boundary_accounts and shard_ids is that
    /// boundary_accounts.len() + 1 == shard_ids.len().
    ///
    /// The shard id at index i corresponds to the shard with account range:
    /// [boundary_accounts[i -1], boundary_accounts[i]).
    pub(crate) shard_ids: Vec<ShardId>,

    /// The mapping from shard id to shard index.
    pub(crate) id_to_index_map: BTreeMap<ShardId, ShardIndex>,

    /// The mapping from shard index to shard id.
    /// TODO(wacban) this is identical to the shard_ids, remove it.
    pub(crate) index_to_id_map: BTreeMap<ShardIndex, ShardId>,

    /// A mapping from the parent shard to child shards. Maps shards from the
    /// previous shard layout to shards that they split to in this shard layout.
    pub(crate) shards_split_map: Option<ShardsSplitMapV2>,
    /// A mapping from the child shard to the parent shard. Maps shards in this
    /// shard layout to their parent shards.
    pub(crate) shards_parent_map: Option<ShardsParentMapV2>,

    /// The version of the shard layout. Starting from the ShardLayoutV2 the
    /// version is no longer updated with every shard layout change and it does
    /// not uniquely identify the shard layout.
    pub(crate) version: ShardVersion,
}

/// Counterpart to `ShardLayoutV2` composed of maps with string keys to aid
/// serde serialization.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
        Self {
            boundary_accounts: layout.boundary_accounts.clone(),
            shard_ids: layout.shard_ids.clone(),
            id_to_index_map: map_keys_to_string(&layout.id_to_index_map),
            index_to_id_map: map_keys_to_string(&layout.index_to_id_map),
            shards_split_map: layout.shards_split_map.as_ref().map(map_keys_to_string),
            shards_parent_map: layout.shards_parent_map.as_ref().map(map_keys_to_string),
            version: layout.version,
        }
    }
}

impl TryFrom<SerdeShardLayoutV2> for ShardLayoutV2 {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(layout: SerdeShardLayoutV2) -> Result<Self, Self::Error> {
        let SerdeShardLayoutV2 {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map,
            shards_parent_map,
            version,
        } = layout;

        let id_to_index_map = map_keys_to_shard_id(id_to_index_map)?;
        let shards_split_map = shards_split_map.map(map_keys_to_shard_id).transpose()?;
        let shards_parent_map = shards_parent_map.map(map_keys_to_shard_id).transpose()?;
        let index_to_id_map = index_to_id_map
            .into_iter()
            .map(|(k, v)| Ok((k.parse()?, v)))
            .collect::<Result<_, Self::Error>>()?;

        match (&shards_split_map, &shards_parent_map) {
            (None, None) => {}
            (Some(shard_split_map), Some(shards_parent_map)) => {
                let expected_shards_parent_map =
                    validate_and_derive_shard_parent_map(&shard_ids, &shard_split_map);
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

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for ShardLayoutV2 {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ShardLayoutV2".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        SerdeShardLayoutV2::json_schema(generator)
    }
}

impl ShardLayoutV2 {
    pub fn new(
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

        let shards_parent_map = shards_split_map.as_ref().map(|shards_split_map| {
            validate_and_derive_shard_parent_map(&shard_ids, &shards_split_map)
        });

        Self {
            boundary_accounts,
            shard_ids,
            id_to_index_map,
            index_to_id_map,
            shards_split_map,
            shards_parent_map,
            version: VERSION,
        }
    }

    pub fn derive(base_shard_layout: &ShardLayout, new_boundary_account: AccountId) -> Self {
        let mut boundary_accounts = base_shard_layout.boundary_accounts().clone();
        let mut shard_ids = base_shard_layout.shard_ids().collect_vec();
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
            .collect_vec();
        let [parent_shard_id] = parent_shard_id.as_slice() else {
            panic!("should only splice one shard");
        };
        shards_split_map.insert(*parent_shard_id, new_shards);

        Self::new(boundary_accounts, shard_ids, Some(shards_split_map))
    }

    pub fn account_id_to_shard_id(&self, account_id: &AccountId) -> ShardId {
        let shard_idx = self.boundary_accounts.partition_point(|x| x <= account_id);
        self.shard_ids[shard_idx]
    }

    pub fn shards_split_map(&self) -> &Option<ShardsSplitMapV2> {
        &self.shards_split_map
    }

    pub fn boundary_accounts(&self) -> &Vec<AccountId> {
        &self.boundary_accounts
    }

    pub fn get_children_shards_ids(&self, parent_shard_id: ShardId) -> Option<Vec<ShardId>> {
        match &self.shards_split_map {
            Some(shards_split_map) => shards_split_map.get(&parent_shard_id).cloned(),
            None => None,
        }
    }

    pub fn try_get_parent_shard_id(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<ShardId>, ShardLayoutError> {
        if !self.shard_ids.contains(&shard_id) {
            return Err(ShardLayoutError::InvalidShardId { shard_id });
        }
        match &self.shards_parent_map {
            // we can safely unwrap here because the construction of to_parent_shard_map guarantees
            // that every shard has a parent shard
            Some(to_parent_shard_map) => {
                let parent_shard_id = to_parent_shard_map.get(&shard_id).unwrap();
                Ok(Some(*parent_shard_id))
            }
            None => Ok(None),
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
}
