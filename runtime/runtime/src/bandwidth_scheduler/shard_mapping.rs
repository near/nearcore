//! Helpers for bandwidth scheduler shard mapping.
//! Bandwidth scheduler maps the non-contiguous shard IDs to contiguous indexes and operates on these indexes.
//! For example shard ids [0, 1, 3, 10] would be mapped to shard indexes [0, 1, 2, 3].
//! Operating on indexes is more efficient than operating on shard IDs,
//! as we can index into arrays instead of looking up the shard ids in maps.
//! TODO(bandwidth_scheduler) - consider merging with the logic in ShardLayout?

use std::collections::BTreeMap;

use near_primitives::types::ShardId;

/// Mapping between shard ids and shard indexes.
/// Maps non-contiguous shard ids to contiguous indexes.
/// For example shard ids [0, 1, 3, 10] would be mapped to shard indexes [0, 1, 2, 3].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchedulerShardMapping {
    /// Mapping from shard id to shard index.
    index_map: BTreeMap<ShardId, SchedulerShardIndex>,
    /// Mapping from shard index to shard id.
    /// No need for a BTreeMap, the index can be used to index into this Vec.
    reverse_map: Vec<ShardId>,
}

/// Shard index, corresponds to a mapped ShardId.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchedulerShardIndex(usize);

impl SchedulerShardMapping {
    /// Create a new mapping for the given shard ids.
    /// Shard ids are assigned consecutive indexes until all shard ids are mapped to some index.
    /// Each shard id gets a single, unique index.
    pub fn new(shard_ids: impl Iterator<Item = ShardId>) -> Self {
        let mut index_map = BTreeMap::new();
        let mut reverse_map = Vec::new();
        for shard_id in shard_ids {
            let next_shard_index = SchedulerShardIndex(index_map.len());
            let index_for_this_id = index_map.entry(shard_id).or_insert(next_shard_index);
            if index_for_this_id.0 == reverse_map.len() {
                reverse_map.push(shard_id);
            }
        }
        Self { index_map, reverse_map }
    }

    /// Convert shard id to shard index.
    pub fn get_index_for_shard_id(&self, shard_id: ShardId) -> Option<SchedulerShardIndex> {
        self.index_map.get(&shard_id).copied()
    }

    /// Convert shard index to shard id.
    pub fn get_shard_id_for_index(&self, index: SchedulerShardIndex) -> Option<ShardId> {
        self.reverse_map.get(index.0).copied()
    }

    /// Get the number of indexes assigned in the mapping.
    /// Shard ids are mapped to indexes 0..indexes_len()
    pub fn indexes_len(&self) -> usize {
        self.index_map.len()
    }
}
