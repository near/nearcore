use super::DataId;
use super::item::FetchItem;
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeBounds;

/// Tracked items, indexed by their block's height.
#[derive(Default)]
struct ItemMap {
    items: HashMap<DataId, FetchItem>,
    by_height: BTreeMap<BlockHeight, HashSet<DataId>>,
}

impl ItemMap {
    fn insert(&mut self, id: DataId, item: FetchItem) {
        self.by_height.entry(item.height).or_default().insert(id.clone());
        self.items.insert(id, item);
    }

    fn remove(&mut self, id: &DataId) -> Option<FetchItem> {
        let item = self.items.remove(id)?;
        if let Some(ids) = self.by_height.get_mut(&item.height) {
            ids.remove(id);
            if ids.is_empty() {
                self.by_height.remove(&item.height);
            }
        }
        Some(item)
    }

    fn ids_in(&self, heights: impl RangeBounds<BlockHeight>) -> impl Iterator<Item = &DataId> {
        self.by_height.range(heights).flat_map(|(_, ids)| ids)
    }
}

/// How a tracked item reaches this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Delivery {
    Pushed,
    /// Only pulled, as a receipt proof into this destination shard.
    PullOnly(ShardId),
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

    pub(super) fn delivery(&self, id: &DataId) -> Option<Delivery> {
        if self.push.items.contains_key(id) {
            return Some(Delivery::Pushed);
        }
        let to_shard = to_shard(id)?;
        self.pull_only
            .get(&to_shard)
            .is_some_and(|map| map.items.contains_key(id))
            .then_some(Delivery::PullOnly(to_shard))
    }

    pub(super) fn get(&self, id: &DataId) -> Option<&FetchItem> {
        self.map_of(id)?.items.get(id)
    }

    pub(super) fn get_mut(&mut self, id: &DataId) -> Option<&mut FetchItem> {
        if self.push.items.contains_key(id) {
            return self.push.items.get_mut(id);
        }
        self.pull_only.get_mut(&to_shard(id)?)?.items.get_mut(id)
    }

    pub(super) fn contains(&self, id: &DataId) -> bool {
        self.map_of(id).is_some()
    }

    /// Tracks `id`, pushed to this node.
    pub(super) fn insert_push(&mut self, id: DataId, item: FetchItem) {
        self.push.insert(id, item);
    }

    /// Tracks the receipt proof `id`, which is only pulled.
    pub(super) fn insert_pull_only(&mut self, id: DataId, item: FetchItem) {
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

    /// The pushed items at `heights`.
    pub(super) fn push_ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight>,
    ) -> impl Iterator<Item = &DataId> {
        self.push.ids_in(heights)
    }

    /// The items of every map at `heights`.
    pub(super) fn ids_in(
        &self,
        heights: impl RangeBounds<BlockHeight> + Clone,
    ) -> impl Iterator<Item = &DataId> {
        self.push
            .ids_in(heights.clone())
            .chain(self.pull_only.values().flat_map(move |map| map.ids_in(heights.clone())))
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
}

/// The destination shard of a receipt proof; `None` for other kinds.
fn to_shard(id: &DataId) -> Option<ShardId> {
    match id {
        DataId::ReceiptProof(id) => Some(id.to_shard),
        DataId::Witness(_) => None,
    }
}
