use near_primitives::epoch_manager::DynamicReshardingConfig;
use near_primitives::trie_split::TrieSplit;
use near_primitives::types::ShardId;
use std::collections::HashMap;

use crate::pick_shard_to_split;

const SHARD_0: ShardId = ShardId::new(0);
const SHARD_1: ShardId = ShardId::new(1);
const SHARD_2: ShardId = ShardId::new(2);

fn default_config() -> DynamicReshardingConfig {
    DynamicReshardingConfig::default()
}

fn proposed_splits<'a>(
    splits: impl IntoIterator<Item = (ShardId, &'a str, u64, u64)>,
) -> HashMap<ShardId, TrieSplit> {
    splits
        .into_iter()
        .map(|(shard_id, boundary, left, right)| {
            (shard_id, TrieSplit::new(boundary.parse().unwrap(), left, right))
        })
        .collect()
}

fn assert_pick_shard(
    config: &DynamicReshardingConfig,
    proposed_splits: &HashMap<ShardId, TrieSplit>,
    exp_shard_id: ShardId,
) {
    let exp_split = proposed_splits.get(&exp_shard_id).unwrap();
    let (shard_id, split) = pick_shard_to_split(proposed_splits, config).unwrap();
    assert_eq!(shard_id, exp_shard_id);
    assert_eq!(&split, exp_split);
}

#[test]
fn empty_proposed_splits() {
    let config = default_config();
    let proposed_splits = HashMap::new();

    assert!(pick_shard_to_split(&proposed_splits, &config).is_none());
}

#[test]
fn single_shard() {
    let config = default_config();
    let proposed_splits = proposed_splits([(SHARD_0, "aaa.near", 100, 100)]);

    assert_pick_shard(&config, &proposed_splits, SHARD_0);
}

#[test]
fn highest_total_memory() {
    let config = default_config();
    let proposed_splits = proposed_splits([
        (SHARD_0, "aaa.near", 100, 100),
        (SHARD_1, "bbb.near", 300, 400), // highest total memory (700)
        (SHARD_2, "ccc.near", 150, 200),
    ]);

    assert_pick_shard(&config, &proposed_splits, SHARD_1);
}

#[test]
fn forced_split() {
    let mut config = default_config();
    let proposed_splits = proposed_splits([
        (SHARD_0, "aaa.near", 500, 500), // highest total memory (1000)
        (SHARD_1, "bbb.near", 100, 100),
    ]);
    config.force_split_shards = vec![SHARD_1]; // but force split takes priority

    assert_pick_shard(&config, &proposed_splits, SHARD_1);
}

#[test]
fn multiple_forced_splits() {
    let mut config = default_config();
    let proposed_splits =
        proposed_splits([(SHARD_0, "aaa.near", 100, 100), (SHARD_1, "bbb.near", 100, 100)]);
    // both shards are in the force split list, but shard 1 is listed first
    config.force_split_shards = vec![SHARD_1, SHARD_0];

    assert_pick_shard(&config, &proposed_splits, SHARD_1);
}

#[test]
#[should_panic]
fn block_split() {
    let mut config = default_config();
    let proposed_splits = proposed_splits([(SHARD_0, "aaa.near", 100, 100)]);
    config.block_split_shards = vec![SHARD_0];

    pick_shard_to_split(&proposed_splits, &config).unwrap();
}
