use super::item::FetchItem;
use super::{DataId, remove_indexed};
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::RangeBounds;

/// Tracked items, indexed by their block's height.
#[derive(Default)]
struct ItemMap {
    items: HashMap<DataId, FetchItem>,
    by_height: BTreeMap<BlockHeight, HashSet<DataId>>,
    /// The ids of the items that may be pulled, by height.
    pullable: BTreeMap<BlockHeight, HashSet<DataId>>,
    /// The ids of the done items, by height.
    done: BTreeMap<BlockHeight, HashSet<DataId>>,
}

impl ItemMap {
    fn insert(&mut self, id: DataId, item: FetchItem) {
        self.by_height.entry(item.height).or_default().insert(id.clone());
        self.items.insert(id, item);
    }

    fn remove(&mut self, id: &DataId) -> Option<FetchItem> {
        let item = self.items.remove(id)?;
        remove_indexed(&mut self.by_height, item.height, id);
        remove_indexed(&mut self.pullable, item.height, id);
        remove_indexed(&mut self.done, item.height, id);
        Some(item)
    }

    fn mark_pullable(&mut self, id: &DataId) {
        if let Some(item) = self.items.get(id) {
            self.pullable.entry(item.height).or_default().insert(id.clone());
        }
    }

    fn stop_pulling(&mut self, id: &DataId) {
        if let Some(item) = self.items.get(id) {
            remove_indexed(&mut self.pullable, item.height, id);
        }
    }

    fn mark_done(&mut self, id: &DataId) {
        if let Some(item) = self.items.get(id) {
            self.done.entry(item.height).or_default().insert(id.clone());
        }
    }

    fn is_marked_done(&self, id: &DataId) -> bool {
        self.items
            .get(id)
            .is_some_and(|item| self.done.get(&item.height).is_some_and(|ids| ids.contains(id)))
    }

    fn done_ids_in(&self, heights: impl RangeBounds<BlockHeight>) -> impl Iterator<Item = &DataId> {
        self.done.range(heights).flat_map(|(_, ids)| ids)
    }

    /// The pullable ids at the `heights` lowest heights holding any.
    fn lowest_pullable(&self, heights: NonZeroUsize) -> impl Iterator<Item = &DataId> {
        self.pullable.values().take(heights.get()).flatten()
    }

    fn ids_in(&self, heights: impl RangeBounds<BlockHeight>) -> impl Iterator<Item = &DataId> {
        self.by_height.range(heights).flat_map(|(_, ids)| ids)
    }

    fn heights_in(
        &self,
        heights: impl RangeBounds<BlockHeight>,
    ) -> impl Iterator<Item = BlockHeight> + '_ {
        self.by_height.range(heights).map(|(height, _)| *height)
    }

    fn heights_and_ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight>,
    ) -> impl Iterator<Item = (BlockHeight, &DataId)> {
        self.by_height.range(heights).flat_map(|(height, ids)| ids.iter().map(|id| (*height, id)))
    }
}

/// Every tracked item. An id is in one map, decided when it is first tracked by whether it is
/// pushed to this node, and never moves.
#[derive(Default)]
pub(super) struct Items {
    /// Items pushed to this node. They expire at the final execution head.
    push: ItemMap,
    /// Items only pulled, by destination shard. The chunk executor declares and releases them.
    // TODO(spice-data-distribution): consider a fallback for a shard whose release never arrives.
    pull_only: HashMap<ShardId, ItemMap>,
}

impl Items {
    fn map_of(&self, id: &DataId) -> Option<&ItemMap> {
        if self.push.items.contains_key(id) {
            return Some(&self.push);
        }
        self.pull_only.get(&to_shard(id)?).filter(|map| map.items.contains_key(id))
    }

    fn map_of_mut(&mut self, id: &DataId) -> Option<&mut ItemMap> {
        if self.push.items.contains_key(id) {
            return Some(&mut self.push);
        }
        self.pull_only.get_mut(&to_shard(id)?).filter(|map| map.items.contains_key(id))
    }

    pub(super) fn get(&self, id: &DataId) -> Option<&FetchItem> {
        self.map_of(id)?.items.get(id)
    }

    pub(super) fn get_mut(&mut self, id: &DataId) -> Option<&mut FetchItem> {
        self.map_of_mut(id)?.items.get_mut(id)
    }

    pub(super) fn contains(&self, id: &DataId) -> bool {
        self.map_of(id).is_some()
    }

    /// Tracks the untracked `id`, pushed to this node.
    pub(super) fn insert_push(&mut self, id: DataId, item: FetchItem) {
        debug_assert!(!self.contains(&id), "an item is tracked once");
        self.push.insert(id, item);
    }

    /// Tracks the untracked receipt proof `id`, which is only pulled.
    pub(super) fn insert_pull_only(&mut self, id: DataId, item: FetchItem) {
        debug_assert!(!self.contains(&id), "an item is tracked once");
        let to_shard = to_shard(&id).expect("only receipt proofs are pulled only");
        self.pull_only.entry(to_shard).or_default().insert(id, item);
    }

    pub(super) fn remove(&mut self, id: &DataId) -> Option<FetchItem> {
        if let Some(item) = self.push.remove(id) {
            return Some(item);
        }
        let to_shard = to_shard(id)?;
        let map = self.pull_only.get_mut(&to_shard)?;
        let item = map.remove(id);
        if map.items.is_empty() {
            self.pull_only.remove(&to_shard);
        }
        item
    }

    /// Makes the tracked item `id` open to pulling; does nothing for an untracked one.
    pub(super) fn mark_pullable(&mut self, id: &DataId) {
        if let Some(map) = self.map_of_mut(id) {
            map.mark_pullable(id);
        }
    }

    pub(super) fn stop_pulling(&mut self, id: &DataId) {
        if let Some(map) = self.map_of_mut(id) {
            map.stop_pulling(id);
        }
    }

    /// Marks the tracked item `id` done; does nothing for an untracked one.
    pub(super) fn mark_done(&mut self, id: &DataId) {
        if let Some(map) = self.map_of_mut(id) {
            map.mark_done(id);
        }
    }

    pub(super) fn is_marked_done(&self, id: &DataId) -> bool {
        self.map_of(id).is_some_and(|map| map.is_marked_done(id))
    }

    /// The done items of every map at `heights`.
    pub(super) fn done_ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight> + Clone,
    ) -> impl Iterator<Item = &DataId> {
        self.push
            .done_ids_in(heights.clone())
            .chain(self.pull_only.values().flat_map(move |map| map.done_ids_in(heights.clone())))
    }

    /// The pullable pushed items at the `heights` lowest heights holding any.
    pub(super) fn lowest_pullable_pushed(
        &self,
        heights: NonZeroUsize,
    ) -> impl Iterator<Item = &DataId> {
        self.push.lowest_pullable(heights)
    }

    /// The pullable pull-only items at the `heights` lowest heights holding any, for each
    /// destination shard.
    pub(super) fn lowest_pullable_pull_only(
        &self,
        heights: NonZeroUsize,
    ) -> impl Iterator<Item = &DataId> {
        self.pull_only.values().flat_map(move |map| map.lowest_pullable(heights))
    }

    /// The pushed items at `heights`.
    pub(super) fn push_ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight>,
    ) -> impl Iterator<Item = &DataId> {
        self.push.ids_in(heights)
    }

    /// The heights in `heights` at which any map holds an item.
    pub(super) fn heights_in(
        &self,
        heights: impl RangeBounds<BlockHeight> + Clone,
    ) -> BTreeSet<BlockHeight> {
        self.push
            .heights_in(heights.clone())
            .chain(self.pull_only.values().flat_map(move |map| map.heights_in(heights.clone())))
            .collect()
    }

    /// The items of every map at `heights`, with their heights.
    pub(super) fn ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight> + Clone,
    ) -> impl Iterator<Item = (BlockHeight, &DataId)> {
        self.push.heights_and_ids_in(heights.clone()).chain(
            self.pull_only.values().flat_map(move |map| map.heights_and_ids_in(heights.clone())),
        )
    }

    /// The pulled-only items into `to_shard` at `heights`.
    pub(super) fn pull_only_ids_in(
        &self,
        to_shard: ShardId,
        heights: impl RangeBounds<BlockHeight>,
    ) -> Vec<DataId> {
        self.pull_only
            .get(&to_shard)
            .map_or_else(Vec::new, |map| map.ids_in(heights).cloned().collect())
    }

    #[cfg(test)]
    pub(super) fn is_pullable(&self, id: &DataId) -> bool {
        self.map_of(id).is_some_and(|map| map.pullable.values().any(|ids| ids.contains(id)))
    }

    #[cfg(test)]
    pub(super) fn is_pull_only(&self, id: &DataId) -> bool {
        self.contains(id) && !self.push.items.contains_key(id)
    }
}

/// The destination shard of a receipt proof; `None` for other kinds.
fn to_shard(id: &DataId) -> Option<ShardId> {
    match id {
        DataId::ReceiptProof(id) => Some(id.to_shard),
        DataId::Witness(_) => None,
    }
}
