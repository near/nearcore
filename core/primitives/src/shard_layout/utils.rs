use near_primitives_core::types::ShardId;
use std::collections::BTreeMap;

pub fn map_keys_to_string<K, V>(map: &BTreeMap<K, V>) -> BTreeMap<String, V>
where
    K: std::fmt::Display,
    V: Clone,
{
    map.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

pub fn map_keys_to_shard_id<V>(
    map: BTreeMap<String, V>,
) -> Result<BTreeMap<ShardId, V>, Box<dyn std::error::Error + Send + Sync>> {
    map.into_iter().map(|(k, v)| Ok((k.parse::<u64>()?.into(), v))).collect()
}
