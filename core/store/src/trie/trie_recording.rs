use super::mem::ArenaMemory;
use super::mem::node::MemTrieNodeView;
use super::{Trie, TrieChanges, TrieRefcountDeltaMap};
use crate::{NibbleSlice, PartialStorage, RawTrieNode, RawTrieNodeWithSize, StorageError};
use ahash::RandomState as AHashRandomState;
use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::state::PartialState;
use near_primitives::trie_key::col::ALL_COLUMNS_WITH_NAMES;
use near_primitives::types::AccountId;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A simple struct to capture a state proof as it's being accumulated.
pub struct TrieRecorder {
    recorded: dashmap::DashMap<CryptoHash, TrieNodeWithRefcount, AHashRandomState>,
    size: crossbeam::utils::CachePadded<AtomicUsize>,
    /// Size of the recorded state proof plus some additional size added to cover removals.
    ///
    /// An upper-bound estimation of the true recorded size after finalization. See
    /// https://github.com/near/nearcore/issues/10890 and
    /// https://github.com/near/nearcore/pull/11000 for details.
    upper_bound_size: crossbeam::utils::CachePadded<AtomicUsize>,
    /// Counts removals performed while recording.
    ///
    /// recorded_storage_size_upper_bound takes it into account when calculating the total size.
    removal_counter: crossbeam::utils::CachePadded<AtomicUsize>,
    /// Limit on the maximum size of the state proof that can be recorded.
    ///
    /// This may get set to u64::MAX to effectively impose no useful limit.
    proof_size_limit: u64,
    /// While a recording budget is set, the `upper_bound_size` past which recording fails;
    /// `usize::MAX` otherwise.
    budget_end: AtomicUsize,
    /// The recording budget that is set, in bytes; zero if there is none. Only reported in the
    /// error.
    budget_limit: AtomicUsize,
    /// Account IDs for which the code should be recorded.
    pub codes_to_record: dashmap::DashSet<AccountId, AHashRandomState>,
}
struct TrieNodeWithRefcount(Arc<[u8]>, u32);

impl From<Arc<[u8]>> for TrieNodeWithRefcount {
    fn from(value: Arc<[u8]>) -> Self {
        Self(value, 0)
    }
}

impl TrieNodeWithRefcount {
    /// Increment the reference count for this node, returning the new count.
    fn increment(&mut self) -> u32 {
        self.1 += 1;
        self.1
    }
}

#[derive(Clone, Debug)]
pub struct TrieRecorderStats {
    pub items_count: usize,
    pub total_size: usize,
    pub removal_counter: usize,
    pub trie_column_sizes: Vec<TrieColumnSize>,
}

#[derive(Clone, Copy, Debug)]
pub struct TrieColumnSize {
    pub column: u8,
    pub column_name: &'static str,
    pub size: SubtreeSize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SubtreeSize {
    /// Size of trie nodes in a subtree.
    pub nodes_size: usize,
    /// Size of all values in a subtree.
    pub values_size: usize,
}

impl TrieRecorder {
    pub fn new(proof_size_limit: Option<u64>) -> Self {
        Self {
            // Use a faster hash builder and 256 shards (vs the default 16) to
            // shorten lock hold times when many rayon workers record trie nodes.
            recorded: dashmap::DashMap::with_capacity_and_hasher_and_shard_amount(
                4096,
                AHashRandomState::new(),
                256,
            ),
            proof_size_limit: proof_size_limit.unwrap_or(u64::MAX),
            budget_end: AtomicUsize::new(usize::MAX),
            budget_limit: AtomicUsize::new(0),
            size: Default::default(),
            upper_bound_size: Default::default(),
            removal_counter: Default::default(),
            codes_to_record: dashmap::DashSet::with_capacity_and_hasher(
                128,
                AHashRandomState::new(),
            ),
        }
    }

    /// Records value without increasing the recorded size.
    /// This is used to bypass witness size checks in order to generate
    /// large witness for testing.
    #[cfg(feature = "test_features")]
    pub fn record_unaccounted(&self, hash: &CryptoHash, node: Arc<[u8]>) {
        self.recorded.entry(*hash).or_insert_with(|| node.into()).increment();
    }

    pub fn record(&self, hash: &CryptoHash, node: Arc<[u8]>) {
        self.record_with(hash, move || node);
    }

    /// Just like "record", but takes a function which returns the serialized node.
    /// Allows to avoid re-serializing the node when it has already been recorded.
    pub fn record_with(&self, hash: &CryptoHash, get_serialized_node: impl FnOnce() -> Arc<[u8]>) {
        let mut size_from_first_insert: Option<usize> = None;
        self.recorded
            .entry(*hash)
            .or_insert_with(|| {
                let serialized = get_serialized_node();
                size_from_first_insert = Some(serialized.len());
                serialized.into()
            })
            .increment();

        // Only do size accounting if this is the first time we see this value.
        if let Some(size) = size_from_first_insert {
            self.upper_bound_size.fetch_add(size, Ordering::Release).checked_add(size).unwrap();
            self.size.fetch_add(size, Ordering::Release);
        }
    }

    /// Convenience function to record memtrie nodes
    pub fn record_memtrie_node<M: ArenaMemory>(&self, node_view: &MemTrieNodeView<'_, M>) {
        self.record_with(&node_view.node_hash(), || {
            borsh::to_vec(&node_view.to_raw_trie_node_with_size()).unwrap().into()
        });
    }

    pub fn record_key_removal(&self) {
        // Charge 2000 bytes for every removal
        self.upper_bound_size.fetch_add(2000, Ordering::Release).checked_add(2000).unwrap();
        // No need to check for overflows here as the `upper_bound_size` would overflow sooner than
        // this if there was an overflow.
        self.removal_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Caps what may be recorded from now on at `limit` more bytes, counted as the recorded upper
    /// bound. See `Trie::set_recording_budget`.
    pub fn set_budget(&self, limit: usize) {
        let end = self.upper_bound_size.load(Ordering::Acquire).saturating_add(limit);
        self.budget_limit.store(limit, Ordering::Release);
        self.budget_end.store(end, Ordering::Release);
    }

    pub fn clear_budget(&self) {
        self.budget_end.store(usize::MAX, Ordering::Release);
        self.budget_limit.store(0, Ordering::Release);
    }

    /// Fails if what was recorded since the budget was set is past it.
    pub fn check_budget(&self) -> Result<(), StorageError> {
        self.check_budget_with(0)
    }

    /// Fails if recording the value with `hash`, of `len` bytes, would pass the budget. Checked
    /// before the value is fetched, so that a value past the budget never enters the proof. A
    /// value that is already recorded adds nothing, so it always fits.
    pub fn check_value_fits(&self, hash: &CryptoHash, len: usize) -> Result<(), StorageError> {
        if self.budget_end.load(Ordering::Acquire) == usize::MAX || self.recorded.contains_key(hash)
        {
            return Ok(());
        }
        self.check_budget_with(len)
    }

    fn check_budget_with(&self, len: usize) -> Result<(), StorageError> {
        let budget_end = self.budget_end.load(Ordering::Acquire);
        if budget_end == usize::MAX {
            return Ok(());
        }
        let size = self.upper_bound_size.load(Ordering::Acquire).saturating_add(len);
        if size > budget_end {
            let limit = self.budget_limit.load(Ordering::Acquire);
            let limit = u64::try_from(limit).unwrap_or(u64::MAX);
            return Err(StorageError::RecordingBudgetExceeded { limit });
        }
        Ok(())
    }

    pub fn check_proof_size_limit_exceed(&self) -> bool {
        self.upper_bound_size.load(Ordering::Acquire) as u64 > self.proof_size_limit
    }

    pub fn recorded_storage(self) -> PartialStorage {
        let mut nodes = Vec::with_capacity(1024);
        for shard in self.recorded.into_shards() {
            let map = shard.into_inner().into_inner();
            nodes.reserve(map.len());
            for (_key, node) in map {
                nodes.push(node.into_inner().0);
            }
        }
        nodes.sort_unstable();
        PartialStorage { nodes: PartialState::TrieValues(nodes) }
    }

    pub fn recorded_trie_changes(self, state_root: CryptoHash) -> TrieChanges {
        let mut refcounts = TrieRefcountDeltaMap::new();
        for shard in self.recorded.into_shards() {
            let map = shard.into_inner().into_inner();
            for (key, node) in map {
                let node = node.into_inner();
                // FIXME(nagisa): lets not reallocate all the values
                refcounts.add(key, node.0.to_vec(), node.1);
            }
        }
        let (insertions, deletions) = refcounts.into_changes();
        TrieChanges {
            old_root: Trie::EMPTY_ROOT,
            new_root: state_root,
            insertions,
            deletions,
            memtrie_changes: None,
            children_memtrie_changes: Default::default(),
        }
    }

    pub fn recorded_storage_size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    pub fn recorded_storage_size_upper_bound(&self) -> usize {
        self.upper_bound_size.load(Ordering::Acquire)
    }

    /// Get statistics about the recorded trie. Useful for observability and debugging.
    /// This scans all of the recorded data, so could potentially be expensive to run.
    pub fn get_stats(&self, trie_root: &CryptoHash) -> TrieRecorderStats {
        let mut trie_column_sizes = Vec::new();
        for (col, col_name) in ALL_COLUMNS_WITH_NAMES {
            let subtree_size = self.get_subtree_size_by_key(trie_root, NibbleSlice::new(&[col]));
            trie_column_sizes.push(TrieColumnSize {
                column: col,
                column_name: col_name,
                size: subtree_size,
            });
        }
        TrieRecorderStats {
            items_count: self.recorded.len(),
            total_size: self.size.load(Ordering::Relaxed),
            removal_counter: self.removal_counter.load(Ordering::Relaxed),
            trie_column_sizes,
        }
    }

    /// Get total size of all recorded nodes and values belonging to the subtree with the given key.
    fn get_subtree_size_by_key(
        &self,
        trie_root: &CryptoHash,
        subtree_key: NibbleSlice<'_>,
    ) -> SubtreeSize {
        self.get_subtree_root_by_key(trie_root, subtree_key)
            .map(|subtree_root| self.get_subtree_size(&subtree_root))
            .unwrap_or_default()
    }

    /// Find the highest node whose trie key starts with `subtree_key`.
    fn get_subtree_root_by_key(
        &self,
        trie_root: &CryptoHash,
        mut subtree_key: NibbleSlice<'_>,
    ) -> Option<CryptoHash> {
        let mut cur_node_hash = *trie_root;

        while !subtree_key.is_empty() {
            let node = self.recorded.get(&cur_node_hash);
            let Some(TrieNodeWithRefcount(raw_node_bytes, _)) = node.as_deref() else {
                // This node wasn't recorded.
                return None;
            };
            let raw_node = match RawTrieNodeWithSize::try_from_slice(&raw_node_bytes) {
                Ok(raw_node_with_size) => raw_node_with_size.node,
                Err(_) => {
                    tracing::error!(
                        "get_subtree_root_by_key: failed to decode node, this shouldn't happen"
                    );
                    return None;
                }
            };

            match raw_node {
                RawTrieNode::Leaf(_, _) => {
                    return None;
                }
                RawTrieNode::BranchNoValue(children)
                | RawTrieNode::BranchWithValue(_, children) => {
                    let child = children[subtree_key.at(0)];
                    match child {
                        Some(child) => {
                            cur_node_hash = child;
                            subtree_key = subtree_key.mid(1);
                        }
                        None => return None,
                    }
                }
                RawTrieNode::Extension(existing_key, child) => {
                    let existing_key = NibbleSlice::from_encoded(&existing_key).0;
                    if subtree_key.starts_with(&existing_key) {
                        cur_node_hash = child;
                        subtree_key = subtree_key.mid(existing_key.len());
                    } else if existing_key.starts_with(&subtree_key) {
                        // The `subtree_key` ends in the middle of this extension, result is the extension's child.
                        return Some(child);
                    } else {
                        // No match.
                        return None;
                    }
                }
            }
        }

        Some(cur_node_hash)
    }

    /// Get size of all recorded nodes and values which are under `subtree_root` (including `subtree_root`).
    fn get_subtree_size(&self, subtree_root: &CryptoHash) -> SubtreeSize {
        let mut nodes_size: usize = 0;
        let mut values_size: usize = 0;

        // Non recursive approach to avoid any potential stack overflows.
        let mut queue: VecDeque<CryptoHash> = VecDeque::new();
        queue.push_back(*subtree_root);

        let mut seen_items: HashSet<CryptoHash> = HashSet::new();

        while let Some(cur_node_hash) = queue.pop_front() {
            if seen_items.contains(&cur_node_hash) {
                // This node (or value with the same hash) has already been processed.
                continue;
            }

            let node = self.recorded.get(&cur_node_hash);
            let Some(TrieNodeWithRefcount(raw_node_bytes, _)) = node.as_deref() else {
                // This node wasn't recorded.
                continue;
            };
            nodes_size = nodes_size.saturating_add(raw_node_bytes.len());
            seen_items.insert(cur_node_hash);

            let raw_node = match RawTrieNodeWithSize::try_from_slice(&raw_node_bytes) {
                Ok(raw_node_with_size) => raw_node_with_size.node,
                Err(_) => {
                    tracing::error!(
                        "get_subtree_size: failed to decode node, this shouldn't happen"
                    );
                    continue;
                }
            };

            match raw_node {
                RawTrieNode::Leaf(_key, value) => {
                    let node = self.recorded.get(&value.hash);
                    if let Some(TrieNodeWithRefcount(value_bytes, _)) = node.as_deref() {
                        if !seen_items.contains(&value.hash) {
                            values_size = values_size.saturating_add(value_bytes.len());
                            seen_items.insert(value.hash);
                        }
                    }
                }
                RawTrieNode::BranchNoValue(children) => {
                    for child_opt in children.0 {
                        if let Some(child) = child_opt {
                            queue.push_back(child);
                        }
                    }
                }
                RawTrieNode::BranchWithValue(value, children) => {
                    for child_opt in children.0 {
                        if let Some(child) = child_opt {
                            queue.push_back(child);
                        }
                    }

                    let node = self.recorded.get(&value.hash);
                    if let Some(TrieNodeWithRefcount(value_bytes, _)) = node.as_deref() {
                        if !seen_items.contains(&value.hash) {
                            values_size = values_size.saturating_add(value_bytes.len());
                            seen_items.insert(value.hash);
                        }
                    }
                }
                RawTrieNode::Extension(_key, child) => {
                    queue.push_back(child);
                }
            };
        }

        SubtreeSize { nodes_size, values_size }
    }
}

impl SubtreeSize {
    pub fn saturating_add(self, other: Self) -> Self {
        SubtreeSize {
            nodes_size: self.nodes_size.saturating_add(other.nodes_size),
            values_size: self.values_size.saturating_add(other.values_size),
        }
    }
}

#[cfg(test)]
mod trie_recording_tests {
    use crate::adapter::trie_store::TrieStoreAdapter;
    use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
    use crate::db::refcount::decode_value_with_rc;
    use crate::test_utils::{
        TestTriesBuilder, gen_larger_changes, simplify_changes, test_populate_flat_storage,
        test_populate_trie,
    };
    use crate::trie::mem::metrics::MEMTRIE_NUM_LOOKUPS;
    use crate::trie::{AccessOptions, AccessTracker, TrieNodesCount};
    use crate::{DBCol, KeyLookupMode, PartialStorage, ShardTries, StorageError, Store, Trie};
    use borsh::BorshDeserialize;
    use itertools::Itertools;
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
    use near_primitives::state::PartialState;
    use near_primitives::state::ValueRef;
    use near_primitives::types::StateRoot;
    use near_primitives::types::chunk_extra::ChunkExtra;
    use rand::prelude::SliceRandom;
    use rand::{Rng, random, thread_rng};
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::num::NonZeroU32;
    use std::sync::Arc;

    const NUM_ITERATIONS_PER_TEST: usize = 300;

    /// Prepared on-disk trie and flat storage for testing.
    struct PreparedTrie {
        store: Store,
        shard_uid: ShardUId,
        /// All the data we've put into the trie.
        data_in_trie: HashMap<Vec<u8>, Vec<u8>>,
        /// The keys that we should be using to call get() on the trie with.
        keys_to_get: Vec<Vec<u8>>,
        /// The keys that we should be using to call get_optimized_ref() on the
        /// trie with.
        keys_to_get_ref: Vec<Vec<u8>>,
        /// The keys to be updated after trie reads.
        updates: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        state_root: StateRoot,
    }

    /// Prepare a trie for testing; this will prepare both a trie and a flat
    /// storage with some dummy block info. If `use_missing_keys` is true,
    /// the keys to test with will also include some keys that are not in the
    /// trie.
    fn prepare_trie(
        use_missing_keys: bool,
        p_existing_key: f64,
        p_missing_key: f64,
    ) -> PreparedTrie {
        let tries_for_building = TestTriesBuilder::new().with_flat_storage(true).build();
        let shard_uid = ShardUId::single_shard();
        let trie_changes = gen_larger_changes(&mut thread_rng(), 50);
        let trie_changes = simplify_changes(&trie_changes);
        if trie_changes.is_empty() {
            // try again
            return prepare_trie(use_missing_keys, p_existing_key, p_missing_key);
        }
        let state_root = test_populate_trie(
            &tries_for_building,
            &Trie::EMPTY_ROOT,
            shard_uid,
            trie_changes.clone(),
        );
        test_populate_flat_storage(
            &tries_for_building,
            shard_uid,
            &CryptoHash::default(),
            &CryptoHash::default(),
            &trie_changes,
        );

        // ChunkExtra is needed for in-memory trie loading code to query state roots.
        let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
        let mut update_for_chunk_extra = tries_for_building.store_update();
        update_for_chunk_extra.store_update().set_ser(
            DBCol::ChunkExtra,
            &get_block_shard_uid(&CryptoHash::default(), &shard_uid),
            &chunk_extra,
        );
        update_for_chunk_extra.commit();

        let data_in_trie = trie_changes
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().unwrap()))
            .collect::<HashMap<_, _>>();
        let existing_keys: HashSet<_> = trie_changes
            .into_iter()
            .map(|(key, _)| key)
            .filter(|_| thread_rng().gen_bool(p_existing_key))
            .collect();
        let missing_keys = if use_missing_keys {
            existing_keys
                .iter()
                .cloned()
                .map(|mut key| {
                    *key.last_mut().unwrap() = 100;
                    key
                })
                .filter(|key| !existing_keys.contains(key) && thread_rng().gen_bool(p_missing_key))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let mut keys: Vec<_> =
            existing_keys.iter().cloned().chain(missing_keys.into_iter()).collect();
        keys.shuffle(&mut thread_rng());
        let updates = keys
            .iter()
            .map(|key| {
                let value = if thread_rng().gen_bool(0.5) {
                    Some(vec![thread_rng().gen_range(0..10) as u8])
                } else {
                    None
                };
                (key.clone(), value)
            })
            .filter(|_| random())
            .collect::<Vec<_>>();
        let (keys_to_get, keys_to_get_ref) =
            keys.into_iter().filter(|_| random()).partition::<Vec<_>, _>(|_| random());
        PreparedTrie {
            store: tries_for_building.store().store(),
            shard_uid,
            data_in_trie,
            keys_to_get,
            keys_to_get_ref,
            updates,
            state_root,
        }
    }

    /// Delete state that we should not be relying on if in-memory tries are
    /// loaded, to help make sure that in-memory tries are used.
    ///
    /// The only thing we don't delete are the values, which may not be
    /// inlined.
    fn destructively_delete_in_memory_state_from_disk(
        store: &TrieStoreAdapter,
        data_in_trie: &HashMap<Vec<u8>, Vec<u8>>,
    ) {
        let key_hashes_to_keep = data_in_trie.iter().map(|(_, v)| hash(&v)).collect::<HashSet<_>>();
        let mut update = store.store_update();
        for (key, value) in store.iter_raw_bytes() {
            let (_, refcount) = decode_value_with_rc(&value);
            let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
            let key_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
            if !key_hashes_to_keep.contains(&key_hash) {
                update.decrement_refcount_by(
                    shard_uid,
                    &key_hash,
                    NonZeroU32::new(refcount as u32).unwrap(),
                );
            }
        }
        update.store_update().delete_all(DBCol::FlatState);
        update.commit();
    }

    fn get_trie_for_shard(
        tries: &ShardTries,
        shard_uid: ShardUId,
        state_root: StateRoot,
        use_flat_storage: bool,
    ) -> Trie {
        if use_flat_storage {
            tries.get_trie_with_block_hash_for_shard(
                shard_uid,
                state_root,
                &CryptoHash::default(),
                false,
            )
        } else {
            let mut trie = tries.get_trie_for_shard(shard_uid, state_root);
            trie.use_access_tracker = true;
            trie
        }
    }

    /// Assert equality of partial storages with human-readable output.
    fn assert_partial_storage(storage: &PartialStorage, other_storage: &PartialStorage) {
        let PartialState::TrieValues(nodes) = &storage.nodes;
        let PartialState::TrieValues(other_nodes) = &other_storage.nodes;
        let nodes: HashSet<Vec<u8>> = HashSet::from_iter(nodes.into_iter().map(|key| key.to_vec()));
        let other_nodes: HashSet<Vec<u8>> =
            HashSet::from_iter(other_nodes.into_iter().map(|key| key.to_vec()));
        let d: Vec<&Vec<u8>> = other_nodes.difference(&nodes).collect();
        assert_eq!(d, Vec::<&Vec<u8>>::default(), "Missing nodes in first storage");
        let d: Vec<&Vec<u8>> = nodes.difference(&other_nodes).collect();
        assert_eq!(d, Vec::<&Vec<u8>>::default(), "Missing nodes in second storage");
    }

    #[derive(Debug, Default)]
    struct AccountingAccessTracker {
        allow_insert: bool,
        mem_reads: Cell<u64>,
        db_reads: Cell<u64>,
        cache: RefCell<BTreeMap<CryptoHash, Arc<[u8]>>>,
    }

    impl AccessTracker for AccountingAccessTracker {
        fn track_mem_lookup(&self, key: &CryptoHash) -> Option<std::sync::Arc<[u8]>> {
            let value = Arc::clone(self.cache.borrow().get(key)?);
            self.mem_reads.set(self.mem_reads.get() + 1);
            Some(value)
        }

        fn track_disk_lookup(&self, key: CryptoHash, value: std::sync::Arc<[u8]>) {
            self.db_reads.set(self.db_reads.get() + 1);
            if self.allow_insert {
                self.cache.borrow_mut().insert(key, value);
            }
        }
    }

    impl AccountingAccessTracker {
        fn get_trie_nodes_count(&self) -> TrieNodesCount {
            TrieNodesCount { mem_reads: self.mem_reads.get(), db_reads: self.db_reads.get() }
        }
    }

    /// Verifies that when operating on a trie, the results are completely consistent
    /// regardless of whether we're operating on the real storage (with or without chunk
    /// cache), while recording reads, or when operating on recorded partial storage.
    fn test_trie_recording_consistency(
        enable_trie_accounting_cache_insertion: bool,
        use_missing_keys: bool,
        use_flat_storage: bool,
    ) {
        for _ in 0..NUM_ITERATIONS_PER_TEST {
            let p_existing_key = thread_rng().gen_range(0.3..1.0);
            let p_missing_key = thread_rng().gen_range(0.7..1.0);
            let PreparedTrie {
                store,
                shard_uid,
                data_in_trie,
                keys_to_get,
                keys_to_get_ref,
                updates,
                state_root,
            } = prepare_trie(use_missing_keys, p_existing_key, p_missing_key);
            let tries = TestTriesBuilder::new()
                .with_store(store.clone())
                .with_flat_storage(use_flat_storage)
                .build();
            let lookup_mode = if use_flat_storage {
                KeyLookupMode::MemOrFlatOrTrie
            } else {
                KeyLookupMode::MemOrTrie
            };
            let memtrie_lookup_counts_before = MEMTRIE_NUM_LOOKUPS.get();

            // Check that while using flat storage counters are all zero.
            // Only use get_optimized_ref(), because get() will actually
            // dereference values which can cause trie reads.
            if use_flat_storage {
                let tracker = AccountingAccessTracker::default();
                let opts =
                    AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
                let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage);
                for key in data_in_trie.keys() {
                    trie.get_optimized_ref(key, lookup_mode, opts).unwrap();
                }
                assert_eq!(
                    tracker.get_trie_nodes_count(),
                    TrieNodesCount { db_reads: 0, mem_reads: 0 }
                );
            }

            // Let's capture the baseline node counts - this is what will happen
            // in production.
            let tracker = AccountingAccessTracker::default();
            let opts = AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage);
            for key in &keys_to_get {
                assert_eq!(trie.get(key, opts).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode, opts)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            let baseline_trie_nodes_count = tracker.get_trie_nodes_count();
            println!("Baseline trie nodes count: {:?}", baseline_trie_nodes_count);
            trie.update(updates.iter().cloned(), opts).unwrap();

            // Now let's do this again while recording, and make sure that the counters
            // we get are exactly the same.
            let tracker = AccountingAccessTracker::default();
            let opts = AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage)
                .recording_reads_new_recorder();
            for key in &keys_to_get {
                assert_eq!(trie.get(key, opts).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode, opts)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(tracker.get_trie_nodes_count(), baseline_trie_nodes_count);
            println!("enable_tac_insertion = {:?}", enable_trie_accounting_cache_insertion);
            trie.update(updates.iter().cloned(), opts).unwrap();
            let baseline_partial_storage = trie.recorded_storage().unwrap();

            // Now let's do this again with memtries enabled. Check that counters
            // are the same.
            assert_eq!(MEMTRIE_NUM_LOOKUPS.get(), memtrie_lookup_counts_before);
            tries.load_memtrie(&shard_uid, None, false).unwrap();
            // Delete the on-disk state so that we really know we're using
            // in-memory tries.
            destructively_delete_in_memory_state_from_disk(&store.trie_store(), &data_in_trie);
            let tracker = AccountingAccessTracker::default();
            let opts = AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
            let trie = get_trie_for_shard(&tries, shard_uid, state_root, use_flat_storage)
                .recording_reads_new_recorder();
            for key in &keys_to_get {
                assert_eq!(trie.get(key, opts).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode, opts)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(tracker.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned(), opts).unwrap();

            // Now, let's check that when doing the same lookups with the captured partial storage,
            // we still get the same counters.
            let partial_storage = trie.recorded_storage().unwrap();
            assert_partial_storage(&baseline_partial_storage, &partial_storage);
            println!(
                "Partial storage has {} nodes from {} entries",
                partial_storage.nodes.len(),
                data_in_trie.len()
            );
            let tracker = AccountingAccessTracker::default();
            let opts = AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
            let trie =
                Trie::from_recorded_storage(partial_storage.clone(), state_root, use_flat_storage);
            for key in &keys_to_get {
                assert_eq!(trie.get(key, opts).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode, opts)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(tracker.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned(), opts).unwrap();

            // Build a Trie using recorded storage and enable recording_reads on this Trie
            let tracker = AccountingAccessTracker::default();
            let opts = AccessOptions { trie_access_tracker: &tracker, ..AccessOptions::DEFAULT };
            let trie = Trie::from_recorded_storage(partial_storage, state_root, use_flat_storage)
                .recording_reads_new_recorder();
            for key in &keys_to_get {
                assert_eq!(trie.get(key, opts).unwrap(), data_in_trie.get(key).cloned());
            }
            for key in &keys_to_get_ref {
                assert_eq!(
                    trie.get_optimized_ref(key, lookup_mode, opts)
                        .unwrap()
                        .map(|value| value.into_value_ref()),
                    data_in_trie.get(key).map(|value| ValueRef::new(&value))
                );
            }
            assert_eq!(tracker.get_trie_nodes_count(), baseline_trie_nodes_count);
            trie.update(updates.iter().cloned(), opts).unwrap();
            assert_partial_storage(&baseline_partial_storage, &trie.recorded_storage().unwrap());

            if !keys_to_get.is_empty() || !keys_to_get_ref.is_empty() {
                // sanity check that we did indeed use in-memory tries.
                assert!(MEMTRIE_NUM_LOOKUPS.get() > memtrie_lookup_counts_before);
            }
        }
    }

    /// The storage a trie reads from, while recording.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Storage {
        Disk,
        FlatStorage,
        Memtrie,
    }

    const ALL_STORAGES: [Storage; 3] = [Storage::Disk, Storage::FlatStorage, Storage::Memtrie];

    /// A trie with many keys of small values, which memtrie and flat storage inline, and two
    /// keys of large values: `INLINED_KEY` is still inlined, `LARGE_KEY` is not.
    struct BudgetTrie {
        store: Store,
        state_root: StateRoot,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    const INLINED_KEY: &[u8] = b"inlined";
    const LARGE_KEY: &[u8] = b"large";

    impl BudgetTrie {
        fn new() -> Self {
            let shard_uid = ShardUId::single_shard();
            let tries = TestTriesBuilder::new().with_flat_storage(true).build();
            let mut data: BTreeMap<Vec<u8>, Vec<u8>> =
                (0..200u8).map(|i| (format!("key{i:03}").into_bytes(), vec![i; 100])).collect();
            data.insert(INLINED_KEY.to_vec(), vec![1; 3000]);
            data.insert(LARGE_KEY.to_vec(), vec![2; 5000]);
            let changes =
                data.iter().map(|(key, value)| (key.clone(), Some(value.clone()))).collect_vec();
            let state_root =
                test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, changes.clone());
            test_populate_flat_storage(
                &tries,
                shard_uid,
                &CryptoHash::default(),
                &CryptoHash::default(),
                &changes,
            );
            // ChunkExtra is needed for in-memory trie loading code to query state roots.
            let chunk_extra = ChunkExtra::new_with_only_state_root(&state_root);
            let mut update = tries.store_update();
            update.store_update().set_ser(
                DBCol::ChunkExtra,
                &get_block_shard_uid(&CryptoHash::default(), &shard_uid),
                &chunk_extra,
            );
            update.commit();
            Self { store: tries.store().store(), state_root, data }
        }

        /// A trie that records its reads, from `storage`.
        fn recording_trie(&self, storage: Storage) -> Trie {
            let shard_uid = ShardUId::single_shard();
            let use_flat_storage = storage == Storage::FlatStorage;
            let tries = TestTriesBuilder::new()
                .with_store(self.store.clone())
                .with_flat_storage(use_flat_storage)
                .build();
            if storage == Storage::Memtrie {
                tries.load_memtrie(&shard_uid, None, false).unwrap();
            }
            let trie = get_trie_for_shard(&tries, shard_uid, self.state_root, use_flat_storage)
                .recording_reads_new_recorder();
            assert_eq!(trie.has_memtries(), storage == Storage::Memtrie);
            assert_eq!(trie.has_flat_storage_chunk_view(), use_flat_storage);
            trie
        }

        /// The keys of the small values, in order.
        fn small_keys(&self) -> Vec<Vec<u8>> {
            self.data.keys().filter(|key| key.starts_with(b"key")).cloned().collect()
        }
    }

    /// What reading keys under a recording budget came to.
    #[derive(Debug, PartialEq)]
    struct BudgetedReads {
        /// The index of the read that failed, and its error.
        failure: Option<(usize, StorageError)>,
        /// What the reads recorded, counted as the recorded upper bound.
        recorded_size: usize,
    }

    /// Reads `keys` in order from `trie` under a recording budget of `limit`, until a read fails.
    /// Checks the values that are read.
    fn read_with_budget(
        trie: &Trie,
        data: &BTreeMap<Vec<u8>, Vec<u8>>,
        keys: &[Vec<u8>],
        limit: usize,
    ) -> BudgetedReads {
        let start = trie.recorded_storage_size_upper_bound();
        trie.set_recording_budget(limit);
        let mut failure = None;
        for (index, key) in keys.iter().enumerate() {
            match trie.get(key, AccessOptions::DEFAULT) {
                Ok(value) => assert_eq!(value.as_ref(), data.get(key)),
                Err(err) => {
                    failure = Some((index, err));
                    break;
                }
            }
        }
        trie.clear_recording_budget();
        BudgetedReads { failure, recorded_size: trie.recorded_storage_size_upper_bound() - start }
    }

    /// Reads under a budget stop where they would pass it: at the trie node that crosses it,
    /// which stays recorded, or before a value that would. They never pass it by more than one
    /// node. The stop is the same whether the trie is read from disk, flat storage or memtrie, and
    /// when it is replayed from the recorded proof.
    #[test]
    fn test_recording_budget_stops_at_the_node_past_it() {
        let budget_trie = BudgetTrie::new();
        let keys = budget_trie.small_keys();
        let mut stopped_at_node = false;
        let mut stopped_before_value = false;
        for limit in (1500..2500).step_by(10) {
            let mut runs = vec![];
            for storage in ALL_STORAGES {
                let trie = budget_trie.recording_trie(storage);
                let reads = read_with_budget(&trie, &budget_trie.data, &keys, limit);
                let (index, err) =
                    reads.failure.as_ref().expect("the reads should pass the budget");
                assert!(*index > 0, "{storage:?}: the first read already passed the budget");
                assert_eq!(*err, StorageError::RecordingBudgetExceeded { limit: limit as u64 });
                let proof = trie.recorded_storage().unwrap();
                // Past the budget by at most one node: no more than the largest thing recorded.
                let PartialState::TrieValues(nodes) = &proof.nodes;
                let largest = nodes.iter().map(|node| node.len()).max().unwrap();
                assert!(reads.recorded_size <= limit + largest, "{storage:?}: {reads:?}");
                runs.push((storage, reads, proof));
            }
            let (_, disk_reads, disk_proof) = &runs[0];
            for (storage, reads, proof) in &runs[1..] {
                assert_eq!(reads, disk_reads, "{storage:?} differs from disk, limit {limit}");
                assert_partial_storage(proof, disk_proof);
            }
            if disk_reads.recorded_size > limit {
                stopped_at_node = true;
            } else {
                stopped_before_value = true;
            }

            // A validator replays the reads from the proof alone, and stops at the same place.
            for flat_storage_used in [false, true] {
                let trie = Trie::from_recorded_storage(
                    disk_proof.clone(),
                    budget_trie.state_root,
                    flat_storage_used,
                )
                .recording_reads_new_recorder();
                let reads = read_with_budget(&trie, &budget_trie.data, &keys, limit);
                assert_eq!(&reads, disk_reads, "replay, flat storage used: {flat_storage_used}");
                assert_partial_storage(&trie.recorded_storage().unwrap(), disk_proof);
            }
        }
        assert!(stopped_at_node, "no budget was crossed by a trie node");
        assert!(stopped_before_value, "no budget stopped a read before its value");
    }

    /// A budget crossed partway along the path to a key stops the read at the node that crosses
    /// it, before the rest of the path: here the root, on every storage and in a replay.
    #[test]
    fn test_recording_budget_stops_partway_along_a_path() {
        const LIMIT: usize = 1;
        let budget_trie = BudgetTrie::new();
        let keys = [LARGE_KEY.to_vec()];
        let expected = StorageError::RecordingBudgetExceeded { limit: LIMIT as u64 };

        let mut proofs = vec![];
        for storage in ALL_STORAGES {
            let trie = budget_trie.recording_trie(storage);
            let reads = read_with_budget(&trie, &budget_trie.data, &keys, LIMIT);
            assert_eq!(reads.failure, Some((0, expected.clone())), "{storage:?}");
            let proof = trie.recorded_storage().unwrap();
            let PartialState::TrieValues(nodes) = &proof.nodes;
            assert_eq!(nodes.len(), 1, "{storage:?}: only the root should be recorded");
            assert_eq!(hash(&nodes[0]), budget_trie.state_root, "{storage:?}");
            assert_eq!(reads.recorded_size, nodes[0].len(), "{storage:?}");
            proofs.push(proof);
        }

        let trie = Trie::from_recorded_storage(proofs[0].clone(), budget_trie.state_root, false)
            .recording_reads_new_recorder();
        let reads = read_with_budget(&trie, &budget_trie.data, &keys, LIMIT);
        assert_eq!(reads.failure, Some((0, expected)), "replay");
        assert_partial_storage(&trie.recorded_storage().unwrap(), &proofs[0]);
    }

    /// A value whose length would pass the budget is not fetched, and stays out of the proof,
    /// whether the value is separate from its leaf or inlined. The path to it is recorded, so a
    /// replay from the proof refuses it the same way, without missing a value.
    #[test]
    fn test_recording_budget_refuses_value_before_fetch() {
        let budget_trie = BudgetTrie::new();
        for key in [INLINED_KEY, LARGE_KEY] {
            let value = &budget_trie.data[key];
            // Room for the path to the value, but not for the value.
            let limit = value.len() - 1;
            let keys = [key.to_vec()];

            let mut proofs = vec![];
            for storage in ALL_STORAGES {
                let trie = budget_trie.recording_trie(storage);
                let reads = read_with_budget(&trie, &budget_trie.data, &keys, limit);
                let expected = StorageError::RecordingBudgetExceeded { limit: limit as u64 };
                assert_eq!(reads.failure, Some((0, expected)), "{storage:?}");
                assert!(reads.recorded_size > 0, "{storage:?}: the path should be recorded");
                assert!(reads.recorded_size <= limit, "{storage:?}: {reads:?}");
                let proof = trie.recorded_storage().unwrap();
                let PartialState::TrieValues(nodes) = &proof.nodes;
                assert!(
                    nodes.iter().all(|node| hash(node) != hash(value)),
                    "{storage:?}: the value is in the proof"
                );
                proofs.push((storage, reads, proof));
            }
            let (_, disk_reads, disk_proof) = &proofs[0];
            for (storage, reads, proof) in &proofs[1..] {
                assert_eq!(reads, disk_reads, "{storage:?} differs from disk");
                assert_partial_storage(proof, disk_proof);
            }

            let trie =
                Trie::from_recorded_storage(disk_proof.clone(), budget_trie.state_root, false)
                    .recording_reads_new_recorder();
            let reads = read_with_budget(&trie, &budget_trie.data, &keys, limit);
            assert_eq!(&reads, disk_reads, "replay");
        }
    }

    /// A value that is already in the proof adds nothing to it, so reading it again fits in any
    /// budget, even one that has no room for its length. On every storage and in a replay.
    #[test]
    fn test_recording_budget_rereads_recorded_value() {
        let budget_trie = BudgetTrie::new();
        for key in [INLINED_KEY, LARGE_KEY] {
            let value = &budget_trie.data[key];
            // Room for the value once, with its path, but not twice.
            let limit = 2 * value.len() - 1;
            let once = [key.to_vec()];
            let twice = [key.to_vec(), key.to_vec()];

            let mut runs = vec![];
            for storage in ALL_STORAGES {
                let trie = budget_trie.recording_trie(storage);
                let reads = read_with_budget(&trie, &budget_trie.data, &twice, limit);
                assert_eq!(reads.failure, None, "{storage:?}");
                assert!(reads.recorded_size > value.len(), "{storage:?}: {reads:?}");
                assert!(reads.recorded_size <= limit, "{storage:?}: {reads:?}");
                // The second read recorded nothing more than the first alone does.
                let trie_once = budget_trie.recording_trie(storage);
                let reads_once = read_with_budget(&trie_once, &budget_trie.data, &once, limit);
                assert_eq!(reads.recorded_size, reads_once.recorded_size, "{storage:?}");
                runs.push((storage, reads, trie.recorded_storage().unwrap()));
            }
            let (_, disk_reads, disk_proof) = &runs[0];
            for (storage, reads, proof) in &runs[1..] {
                assert_eq!(reads, disk_reads, "{storage:?} differs from disk");
                assert_partial_storage(proof, disk_proof);
            }

            // Already in the proof before the budget is set: rereading it needs no room at all.
            let trie = budget_trie.recording_trie(Storage::Memtrie);
            trie.get(key, AccessOptions::DEFAULT).unwrap();
            let reads = read_with_budget(&trie, &budget_trie.data, &once, 1);
            assert_eq!(reads, BudgetedReads { failure: None, recorded_size: 0 });

            let trie =
                Trie::from_recorded_storage(disk_proof.clone(), budget_trie.state_root, false)
                    .recording_reads_new_recorder();
            let reads = read_with_budget(&trie, &budget_trie.data, &twice, limit);
            assert_eq!(&reads, disk_reads, "replay");
        }
    }

    /// A budget does nothing on a trie that doesn't record, and nothing once it is cleared.
    #[test]
    fn test_recording_budget_without_recorder_and_cleared() {
        let budget_trie = BudgetTrie::new();
        let mut keys = budget_trie.small_keys();
        keys.push(LARGE_KEY.to_vec());
        let read_all = |trie: &Trie| {
            for key in &keys {
                let value = trie.get(key, AccessOptions::DEFAULT).unwrap();
                assert_eq!(value.as_ref(), budget_trie.data.get(key));
            }
        };

        let shard_uid = ShardUId::single_shard();
        let tries = TestTriesBuilder::new().with_store(budget_trie.store.clone()).build();
        let trie = tries.get_trie_for_shard(shard_uid, budget_trie.state_root);
        trie.set_recording_budget(1);
        read_all(&trie);
        assert_eq!(trie.check_recording_budget(), Ok(()));

        let trie = budget_trie.recording_trie(Storage::Disk);
        trie.set_recording_budget(1);
        trie.clear_recording_budget();
        read_all(&trie);
        assert_eq!(trie.check_recording_budget(), Ok(()));
    }

    /// A removal grows the recorded upper bound without recording a node, so only an explicit
    /// check finds that it passed the budget.
    #[test]
    fn test_recording_budget_counts_removals() {
        let budget_trie = BudgetTrie::new();
        let trie = budget_trie.recording_trie(Storage::Disk);
        trie.set_recording_budget(1000);
        assert_eq!(trie.check_recording_budget(), Ok(()));
        trie.recorder.as_ref().unwrap().record_key_removal();
        assert_eq!(
            trie.check_recording_budget(),
            Err(StorageError::RecordingBudgetExceeded { limit: 1000 })
        );
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache() {
        test_trie_recording_consistency(false, false, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache() {
        test_trie_recording_consistency(true, false, false);
    }

    #[test]
    fn test_trie_recording_consistency_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency(false, true, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency(true, true, false);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache() {
        test_trie_recording_consistency(false, false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache() {
        test_trie_recording_consistency(true, false, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_no_accounting_cache_with_missing_keys() {
        test_trie_recording_consistency(false, true, true);
    }

    #[test]
    fn test_trie_recording_consistency_with_flat_storage_with_accounting_cache_and_missing_keys() {
        test_trie_recording_consistency(true, true, true);
    }
}

#[cfg(test)]
mod memtrie_batch_iteration_tests {
    use super::*;
    use crate::Trie;
    use crate::test_utils::{
        TestTriesBuilder, create_test_store, simplify_changes, test_populate_flat_storage,
        test_populate_trie,
    };
    use crate::trie::AccessOptions;
    use crate::trie::trie_tests::merge_trie_changes;
    use near_primitives::hash::hash;
    use near_primitives::shard_layout::ShardUId;
    use std::ops::Bound;

    /// Returns the hash of height (as le_bytes) for use as a fake block hash in tests.
    fn fake_hash(height: usize) -> CryptoHash {
        hash(height.to_le_bytes().as_ref())
    }

    fn iterate_batch(
        trie: &Trie,
        previous_batch_last_key: Option<Vec<u8>>,
        batch_limit: usize,
    ) -> Option<Vec<u8>> {
        let read_trie = trie.lock_for_iter();
        // Get the iterator for the trie, skipping the first key if needed
        let mut iter = read_trie.iter().expect("failed to get iterator");
        if let Some(key) = previous_batch_last_key {
            iter.seek(Bound::Excluded(key)).expect("failed to seek");
        }

        // Iterate over the trie, stopping when we reach the batch size
        let mut items = 0;
        while let Some(result) = iter.next() {
            let Ok((key, _value)) = result else {
                panic!("failed to iterate");
            };
            if items >= batch_limit {
                return Some(key);
            }
            items += 1;
        }

        None // No more items to iterate
    }

    fn test_batched_iteration_impl(use_memtries: bool) {
        let store = create_test_store();
        let tries = TestTriesBuilder::new()
            .with_store(store)
            .with_flat_storage(use_memtries)
            .with_in_memory_tries(use_memtries)
            .build();
        let shard_uid = ShardUId::single_shard();
        let block_id = CryptoHash::default();

        // Create arbitrary data to populate the trie
        // Deliberately contains duplicate values to test reference counting
        let initial =
            (0..1000).map(|i| (Vec::from(fake_hash(i)), Some(vec![i as u8]))).collect::<Vec<_>>();

        test_populate_flat_storage(&tries, shard_uid, &block_id, &block_id, &initial);
        let root = test_populate_trie(&tries, &Trie::EMPTY_ROOT, shard_uid, initial.clone());
        let trie = tries.get_trie_for_shard(shard_uid, root);

        let batch_size = 20;
        let mut last_key: Option<Vec<u8>> = None;
        let mut change_batches: Vec<TrieChanges> = Vec::new();
        loop {
            let trie = trie.recording_reads_new_recorder();
            last_key = iterate_batch(&trie, last_key, batch_size);
            let trie_changes =
                trie.recorded_trie_changes(root).expect("failed to get trie changes");
            change_batches.push(trie_changes);

            if last_key.is_none() {
                break;
            }
        }
        let all_changes = merge_trie_changes(change_batches);

        // Inserting the same key/values into an empty trie should result in the same TrieChanges
        let new_trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);
        let trie_changes = new_trie
            .update_with_trie_storage(initial.clone(), AccessOptions::DEFAULT)
            .expect("failed to update trie");
        assert_eq!(trie_changes, all_changes);

        // Create a new store and apply the changes to it, then iterate the trie
        // as a consistency check. We should get the same key/values and
        // recorded changes.
        let new_store = create_test_store();
        let new_tries = TestTriesBuilder::new().with_store(new_store).build();
        let mut store_update = new_tries.store_update();
        new_tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();

        let trie = new_tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder();
        {
            let read_trie = trie.lock_for_iter();
            let iter = read_trie.iter().expect("failed to get iterator");
            let got = iter
                .map(|item| item.expect("got error iterating"))
                .map(|(k, v)| (k, Some(v)))
                .collect::<Vec<_>>();
            assert_eq!(simplify_changes(&initial), got);
        }

        let recorded_changes =
            trie.recorded_trie_changes(root).expect("failed to get recorded changes");
        assert_eq!(trie_changes, recorded_changes);
    }

    #[test]
    fn test_batched_iteration_memtrie() {
        for use_memtries in [true, false] {
            test_batched_iteration_impl(use_memtries);
        }
    }
}
