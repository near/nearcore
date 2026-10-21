//! A pool that keeps its items in groups, and the iterator that takes the groups out of it.
//! What the items are, and how the items of a group are sorted, is up to `PoolItem`.

use crate::InsertTransactionResult;
use crate::item::{GroupId, PoolItem};
use crate::types::{Group, PoolKey};
use near_o11y::metrics::IntGaugeVec;
use near_o11y::metrics::prometheus::core::{AtomicI64, GenericGauge};
use near_primitives::epoch_info::RngSeed;
use near_primitives::hash::CryptoHash;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;

/// Metrics tracked for a pool.
pub(crate) struct PoolMetrics {
    count: GenericGauge<AtomicI64>,
    size: GenericGauge<AtomicI64>,
}

impl PoolMetrics {
    pub(crate) fn new(count: &IntGaugeVec, size: &IntGaugeVec, metrics_label: &str) -> Self {
        let count = count.with_label_values(&[metrics_label]);
        let size = size.with_label_values(&[metrics_label]);
        // A `get()` call initializes a metric even if its value is zero.
        count.get();
        size.get();
        Self { count, size }
    }
}

/// Keeps track of items that were not yet accepted into the block chain.
pub(crate) struct GroupedPool<T: PoolItem> {
    /// Items are grouped by the key of their group.
    groups: BTreeMap<PoolKey, VecDeque<T>>,
    /// Set of all hashes to quickly check if the given item is in the pool.
    unique_items: HashSet<CryptoHash>,
    /// A uniquely generated key seed to randomize PoolKey order.
    key_seed: RngSeed,
    /// The key after which the pool iterator starts. Doesn't have to be present in the pool.
    last_used_key: PoolKey,
    /// Total size of items in the pool measured in bytes.
    total_size: u64,
    metrics: PoolMetrics,
}

impl<T: PoolItem> GroupedPool<T> {
    pub(crate) fn new(key_seed: RngSeed, metrics: PoolMetrics) -> Self {
        Self {
            groups: BTreeMap::new(),
            unique_items: HashSet::new(),
            key_seed,
            last_used_key: CryptoHash::default(),
            total_size: 0,
            metrics,
        }
    }

    /// Inserts an item into the pool. The item is rejected if it brings the size of the pool over
    /// `size_limit`.
    pub(crate) fn insert(&mut self, item: T, size_limit: Option<u64>) -> InsertTransactionResult {
        self.insert_with(item, size_limit, |_, group, item| T::add_to_group(group, item))
    }

    /// Like `insert`, but the item is added to its group by `place`, which is given the key of
    /// the group. `place` is only called if the item was accepted.
    pub(crate) fn insert_with(
        &mut self,
        item: T,
        size_limit: Option<u64>,
        place: impl FnOnce(PoolKey, &mut VecDeque<T>, T),
    ) -> InsertTransactionResult {
        let item_hash = item.hash();
        if self.unique_items.contains(&item_hash) {
            return InsertTransactionResult::Duplicate;
        }
        // We never expect the total size to go over `u64` during real operation as that would
        // be more than 10^9 GiB of RAM consumed for transaction pool, so panicking here is intended
        // to catch a logic error in estimation of transaction size.
        let new_total_size =
            self.total_size.checked_add(item.size()).expect("Total transaction size is too large");
        if let Some(limit) = size_limit {
            if new_total_size > limit {
                return InsertTransactionResult::NoSpaceLeft;
            }
        }

        // At this point the item is accepted to the pool.

        // This is guaranteed to succeed because of the check above that the
        // hashset does not contain this hash.  This can be improved once the
        // entries API is stabilized
        // (https://github.com/rust-lang/rust/issues/60896).
        assert_eq!(self.unique_items.insert(item_hash), true);
        self.total_size = new_total_size;
        let key = item.group().key(&self.key_seed);
        place(key, self.groups.entry(key).or_default(), item);

        self.metrics.count.inc();
        self.metrics.size.set(self.total_size as i64);
        InsertTransactionResult::Success
    }

    /// Removes the items with given hashes from the pool. Each of them comes with the group it
    /// is in.
    ///
    /// In practice, used to evict items that have already been included into the block or
    /// became invalid.
    pub(crate) fn remove_items<'a>(
        &mut self,
        items: impl IntoIterator<Item = (CryptoHash, GroupId<'a>)>,
    ) {
        let mut grouped_items = HashMap::new();
        for (item_hash, group) in items {
            // If the item is not present in the pool, skip it.
            if !self.unique_items.remove(&item_hash) {
                continue;
            }

            grouped_items
                .entry(group.key(&self.key_seed))
                .or_insert_with(HashSet::new)
                .insert(item_hash);
        }
        for (key, hashes) in grouped_items {
            if let Entry::Occupied(mut entry) = self.groups.entry(key) {
                entry.get_mut().retain(|item| {
                    if !hashes.contains(&item.hash()) {
                        return true;
                    }
                    // See the comment in `insert_with` where we increase the size for reasoning
                    // why panicking here catches a logic error.
                    self.total_size = self
                        .total_size
                        .checked_sub(item.size())
                        .expect("Total transaction size dropped below zero");
                    false
                });
                if entry.get().is_empty() {
                    entry.remove_entry();
                }
            }
        }

        // We can update metrics only once for the whole batch of items.
        self.update_metrics();
    }

    /// Returns the number of unique items in the pool.
    pub(crate) fn len(&self) -> usize {
        self.unique_items.len()
    }

    /// Returns the total size of items in the pool in bytes.
    pub(crate) fn size(&self) -> u64 {
        self.total_size
    }

    #[cfg(test)]
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Takes the next group out of the pool: the one after the group that was taken last in the
    /// order of keys, or the first one if there is no such group. Returns `None` if the pool has
    /// no groups.
    fn take_next_group(&mut self) -> Option<(PoolKey, VecDeque<T>)> {
        let after_last_used = (Bound::Excluded(self.last_used_key), Bound::Unbounded);
        let key = *self
            .groups
            .range(after_last_used)
            .next()
            .or_else(|| self.groups.first_key_value())
            .map(|(key, _)| key)?;
        self.last_used_key = key;
        self.groups.remove_entry(&key)
    }

    /// Accounts for the items that were pulled from `group` using `.next()`.
    fn release_pulled(&mut self, group: &Group<T>) {
        group.removed_hashes.iter().for_each(|hash| {
            self.unique_items.remove(hash);
        });
        // See the comment in `insert_with` where we increase the size for reasoning
        // why panicking here catches a logic error.
        self.total_size = self
            .total_size
            .checked_sub(group.removed_size)
            .expect("Total transaction size dropped below zero");
    }

    fn update_metrics(&self) {
        self.metrics.count.set(self.unique_items.len() as i64);
        self.metrics.size.set(self.total_size as i64);
    }
}

/// GroupedPoolIterator is a structure to pull items from the pool.
/// It iterates over groups one by one.
/// When the iterator is dropped the remaining items are returned back to the pool.
pub(crate) struct GroupedPoolIterator<'a, T: PoolItem> {
    /// Mutable reference to the pool, to avoid exposing it while the iterator exists.
    pool: &'a mut GroupedPool<T>,

    /// Queue of groups that were pulled from the pool. Each group there is sorted.
    sorted_groups: VecDeque<Group<T>>,

    /// Groups that made no progress (no items popped) since the last
    /// yield. Parked here so the iterator stops cycling them, but still
    /// returned to the pool on Drop.
    stalled_groups: Vec<Group<T>>,
}

impl<'a, T: PoolItem> GroupedPoolIterator<'a, T> {
    pub(crate) fn new(pool: &'a mut GroupedPool<T>) -> Self {
        Self { pool, sorted_groups: Default::default(), stalled_groups: Vec::new() }
    }

    /// The iterator works with the following algorithm:
    /// On next(), the iterator tries to get a group from the pool, sorts it, and adds it to the
    /// back of the sorted groups queue.
    /// Remembers the last used key, so it can continue from the next key.
    ///
    /// If the pool is empty, the iterator gets the group from the front of the sorted groups queue.
    ///
    /// If this group is empty (no items left inside), then the iterator discards it and
    /// updates `unique_items` in the pool. Then gets the next one.
    ///
    /// If the group made no progress since the last yield (no items popped), it is moved to
    /// `stalled_groups` and will not be yielded again. This prevents infinite cycling when a group
    /// cannot make progress (handles strict-nonce gap).
    ///
    /// Once a non-empty group that made progress is found, this group is pushed to the back of the
    /// sorted groups queue and the iterator returns a mutable reference to this group.
    ///
    /// If the sorted groups queue is empty, the iterator returns None.
    ///
    /// When the iterator is dropped, `unique_items` in the pool is updated for every group.
    /// All non-empty groups from both sorted_groups and stalled_groups are inserted back into the
    /// pool.
    pub(crate) fn next(&mut self) -> Option<&mut Group<T>> {
        if let Some((key, mut items)) = self.pool.take_next_group() {
            T::sort_group(&mut items);
            let group = Group {
                key,
                items,
                removed_hashes: vec![],
                removed_size: 0,
                removed_any_since_yield: false,
            };
            Some(self.sorted_groups.push_back_mut(group))
        } else {
            while let Some(mut group) = self.sorted_groups.pop_front() {
                if group.items.is_empty() {
                    self.pool.release_pulled(&group);
                    self.pool.update_metrics();
                } else if !group.removed_any_since_yield {
                    // No items were popped since last yield, keep as a stalled group.
                    self.stalled_groups.push(group);
                } else {
                    group.removed_any_since_yield = false;
                    return Some(self.sorted_groups.push_back_mut(group));
                }
            }
            None
        }
    }
}

/// When a pool iterator is dropped, all remaining non empty groups from the sorted groups queue
/// are inserted back into the pool. And removed items hashes from groups are removed from the
/// pool's unique_items.
impl<'a, T: PoolItem> Drop for GroupedPoolIterator<'a, T> {
    fn drop(&mut self) {
        for group in self.sorted_groups.drain(..).chain(self.stalled_groups.drain(..)) {
            self.pool.release_pulled(&group);
            if !group.items.is_empty() {
                self.pool.groups.insert(group.key, group.items);
            }
        }
        // We can update metrics only once for the whole batch of items.
        self.pool.update_metrics();
    }
}
