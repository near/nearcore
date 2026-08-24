//! Core state for SPICE data distribution.

// Nothing is wired up to the actors yet; the cutover PRs consume this.
#![allow(dead_code, unused_imports)]

mod fetchable;
mod item;

pub(crate) use fetchable::{DataKind, FetchContext, Interest};
pub(crate) use item::{
    AssembledData, Assembly, AssemblyError, CodedTracker, CompletedCodedData, DataAttribution,
    DataId, FetchItem, FetchState, Item, Lane, PartInsertResult, ProduceItem, ProduceState,
    SpiceData, TransferUnit,
};
use near_primitives::types::{BlockHeight, SpiceChunkId};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::ops::Bound::{Excluded, Included};

/// Owns the lifecycle state and height index for every SPICE data item.
#[derive(Default)]
pub(crate) struct SpiceDataManager {
    items: HashMap<DataId, Item>,
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,
}

impl SpiceDataManager {
    /// Seeds an item. An existing item is kept, except that a produce role takes over a
    /// fetch role for the same id — a node that authors the data never fetches it.
    pub(crate) fn insert(&mut self, id: DataId, item: Item) {
        let height = item.height();
        match self.items.entry(id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(item);
                self.items_by_height.entry(height).or_default().push(id);
            }
            Entry::Occupied(mut entry) => {
                if matches!(entry.get(), Item::Fetch(_)) && matches!(item, Item::Produce(_)) {
                    let old_height = entry.get().height();
                    entry.insert(item);
                    if old_height != height {
                        self.items_by_height.entry(height).or_default().push(id);
                    }
                }
            }
        }
    }

    pub(crate) fn get(&self, id: &DataId) -> Option<&Item> {
        self.items.get(id)
    }

    pub(crate) fn get_mut(&mut self, id: &DataId) -> Option<&mut Item> {
        self.items.get_mut(id)
    }

    /// Re-aims a contract-code item at another chunk. The height may go down; the index
    /// entry it leaves behind is dropped when a range pass reaches it. Whether an anchor
    /// may be overwritten at all is the caller's call.
    pub(crate) fn update_anchor(
        &mut self,
        id: &DataId,
        anchor: SpiceChunkId,
        height: BlockHeight,
    ) -> bool {
        if !matches!(id, DataId::ContractCode { .. }) {
            return false;
        }
        let Some(Item::Fetch(item)) = self.items.get_mut(id) else {
            return false;
        };
        let old_height = item.height;
        item.anchor = Some(anchor);
        item.height = height;
        if old_height != height {
            self.items_by_height.entry(height).or_default().push(id.clone());
        }
        true
    }

    /// Returns live items in `(start, end]` without changing the expiry index.
    /// `start` is not included. Starting a cursor at genesis height is safe: the genesis
    /// block has no chunks, so no item ever has that height.
    pub(crate) fn items_in_height_range(
        &self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Vec<DataId> {
        if start >= end {
            return Vec::new();
        }
        let mut seen = HashSet::new();
        self.items_by_height
            .range((Excluded(start), Included(end)))
            .flat_map(|(height, ids)| {
                ids.iter().filter(move |id| {
                    self.items.get(*id).is_some_and(|item| item.height() == *height)
                })
            })
            .filter(|&id| seen.insert(id))
            .cloned()
            .collect()
    }

    /// Removes live items at or below `height` and drains their index entries.
    pub(crate) fn expire_through(&mut self, height: BlockHeight) -> Vec<(DataId, Item)> {
        let mut expiring = take(&mut self.items_by_height);
        if let Some(next) = height.checked_add(1) {
            self.items_by_height = expiring.split_off(&next);
        }
        let mut expired = Vec::new();
        for (indexed_height, ids) in expiring {
            for id in ids {
                let Entry::Occupied(entry) = self.items.entry(id) else {
                    continue;
                };
                if entry.get().height() == indexed_height {
                    expired.push(entry.remove_entry());
                }
            }
        }
        expired
    }
}

#[cfg(test)]
mod tests;
