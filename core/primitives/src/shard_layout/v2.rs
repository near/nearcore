use crate::shard_layout::{
    ShardVersion, ShardsParentMapV2, ShardsSplitMapV2, validate_and_derive_shard_parent_map_v2,
};
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{ShardId, ShardIndex};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

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
