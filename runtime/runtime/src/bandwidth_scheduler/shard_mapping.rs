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

/// Equivalent to Map<SchedulerShardIndex, T> for some shard mapping.
/// Accessing a value is done by indexing into an array, which is faster than a lookup in BTreeMap or HashMap.
/// Should be used only with indexes from the same mapping that was given in the constructor!
pub struct SchedulerShardIndexMap<T> {
    data: Vec<Option<T>>,
}

impl<T> SchedulerShardIndexMap<T> {
    pub fn new(mapping: &SchedulerShardMapping) -> Self {
        let mut data = Vec::with_capacity(mapping.indexes_len());
        // T might not implement Clone, so we can't use vec![None; mapping.indexes_len()]
        for _ in 0..mapping.indexes_len() {
            data.push(None);
        }
        Self { data }
    }

    pub fn get(&self, index: &SchedulerShardIndex) -> Option<&T> {
        self.debug_check_index(&index);
        self.data[index.0].as_ref()
    }

    pub fn insert(&mut self, index: SchedulerShardIndex, value: T) {
        self.debug_check_index(&index);
        self.data[index.0] = Some(value);
    }

    // Provides a nice error message on debug builds if the index is out of bounds.
    // Making it a debug_assert avoids doing the check twice on release builds.
    fn debug_check_index(&self, index: &SchedulerShardIndex) {
        debug_assert!(
            index.0 < self.data.len(),
            "Shard index out of bounds! len: {}, index: {}",
            self.data.len(),
            index.0
        );
    }
}

/// Represents a link between a sender shard that sends receipts and a receiver shard that receives them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardLink {
    /// Sender shard
    pub sender: SchedulerShardIndex,
    /// Receiver shard
    pub receiver: SchedulerShardIndex,
}

impl ShardLink {
    pub fn new(sender: SchedulerShardIndex, receiver: SchedulerShardIndex) -> Self {
        Self { sender, receiver }
    }
}

/// Equivalent to Map<ShardLink, T>
/// Accessing a value is done by indexing into an array, which is faster than a lookup in BTreeMap or HashMap.
/// Should be used only with indexes from the same mapping that was given in the constructor!
pub struct SchedulerShardLinkMap<T> {
    data: Vec<Option<T>>,
    num_indexes: usize,
}

impl<T> SchedulerShardLinkMap<T> {
    pub fn new(mapping: &SchedulerShardMapping) -> Self {
        let num_indexes = mapping.indexes_len();
        let data_len = num_indexes * num_indexes;
        let mut data = Vec::with_capacity(data_len);
        for _ in 0..data_len {
            data.push(None);
        }
        Self { data, num_indexes }
    }

    pub fn get(&self, link: &ShardLink) -> Option<&T> {
        self.data[self.data_index_for_link(link)].as_ref()
    }

    pub fn insert(&mut self, link: ShardLink, value: T) {
        let data_index = self.data_index_for_link(&link);
        self.data[data_index] = Some(value);
    }

    fn data_index_for_link(&self, link: &ShardLink) -> usize {
        debug_assert!(
            link.sender.0 < self.num_indexes,
            "Sender index out of bounds! num_indexes: {}, link: {:?}",
            self.num_indexes,
            link
        );
        debug_assert!(
            link.receiver.0 < self.num_indexes,
            "Receiver index out of bounds! num_indexes: {}, link: {:?}",
            self.num_indexes,
            link
        );
        link.sender.0 * self.num_indexes + link.receiver.0
    }
}
