//! Logic for splitting state defined by `Trie` into parts.
//!
//! Needed when a node wants to download state from scratch. Single shard state size
//! is on the order of GBs, and state part size is expected to be on the order of MBs.
//!
//! State partition is defined using DFS traversal of trie. All parts are disjoint and
//! contiguous in this order. Left boundary of i-th part is determined by computing
//! prefix sums of memory usages of nodes.
//! A bit more precisely - memory usage threshold for i-th part is approximately
//! `total_size / num_parts * part_id`. The boundary itself is a first node which
//! corresponds to some key-value pair and prefix memory usage for it is greater
//! than a threshold.
//!
//! We include paths from root to both boundaries in state parts, because they are
//! necessary for receiver to prove existence of all nodes knowing just a state root.
//! Moreover, we include all left siblings for each path, because they are
//! necessary to prove its position in the list of prefix sums.

use super::ops::iter::TrieTraversalItem;
use super::trie_storage_update::{TrieStorageNode, TrieStorageNodeWithSize};
use super::{AccessOptions, TrieRefcountDeltaMap};
use crate::flat::{FlatStateChanges, FlatStateIterator};
use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::trie_storage::TrieMemoryPartialStorage;
use crate::trie::{ApplyStatePartResult, RawTrieNodeWithSize};
use crate::{PartialStorage, StorageError, Trie, TrieChanges, metrics};
use borsh::BorshDeserialize;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::state::FlatStateValue;
use near_primitives::state::PartialState;
use near_primitives::state_part::PartId;
use near_primitives::state_record::is_contract_code_key;
use near_primitives::types::{ShardId, StateRoot};
use near_vm_runner::ContractCode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Trie {
    /// Descends into node corresponding to `part_id`-th boundary node if state
    /// is divided into `num_parts` parts.
    /// Visits all the left siblings of nodes on the way to the boundary node.
    /// Guarantees that the node corresponds to some KV pair or returns `StorageError`,
    /// except corner cases:
    /// - for part 0, boundary is Some(empty_key);
    /// - for part `num_parts`, boundary is None.
    /// This guarantees that parts cover all nodes in trie.
    /// Returning `None` corresponds to the right boundary of the trie.
    ///
    /// Note that it is used for both boundary generation and verifying. To verify
    /// a boundary, it is enough to check that method doesn't return `StorageError`
    /// and visits all provided nodes.
    fn find_state_part_boundary(
        &self,
        part_id: u64,
        num_parts: u64,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if part_id > num_parts {
            return Err(StorageError::StorageInternalError);
        }
        if part_id == 0 {
            return Ok(Some(vec![]));
        }
        if part_id == num_parts {
            return Ok(None);
        }
        let root_node = self.retrieve_storage_node(&self.root)?;
        let total_size = root_node.memory_usage;
        let size_start = total_size / num_parts * part_id + part_id.min(total_size % num_parts);
        if root_node.memory_usage <= size_start {
            return Ok(None);
        }
        let nibbles = self.find_node_in_dfs_order(&root_node, size_start)?;
        Ok(Some(NibbleSlice::nibbles_to_bytes(&nibbles)))
    }

    /// Generates state parts using the trie storage (i.e. State) and not using
    /// flat storage (i.e. FlatState).
    #[cfg(test)]
    fn get_trie_nodes_for_part_without_flat_storage(
        &self,
        part_id: PartId,
    ) -> Result<PartialState, StorageError> {
        let with_recording = self.recording_reads_new_recorder();
        with_recording.visit_nodes_for_state_part(part_id)?;
        let recorded = with_recording.recorded_storage().unwrap();
        Ok(recorded.nodes)
    }

    /// Helper to create iterator over flat storage entries corresponding to
    /// its head, shard for which trie was created and the range of keys
    fn iter_flat_state_entries<'a>(
        &'a self,
        path_begin: Option<Vec<u8>>,
        path_end: Option<Vec<u8>>,
    ) -> Result<FlatStateIterator<'a>, StorageError> {
        let flat_storage_chunk_view = match &self.flat_storage_chunk_view {
            None => {
                return Err(StorageError::StorageInconsistentState(
                    "Flat storage chunk view not found".to_string(),
                ));
            }
            Some(chunk_view) => chunk_view,
        };

        // If left key in nibbles is already the right trie boundary, return empty iterator.
        if path_begin.is_none() {
            return Ok(Box::new(std::iter::empty()));
        }

        Ok(flat_storage_chunk_view.iter_range(path_begin.as_deref(), path_end.as_deref()))
    }

    /// Determines the boundaries of a state part by accessing the Trie (i.e. State column).
    /// Returns the keys of the boundaries and also a set of Trie nodes needed to validate the state parts.
    fn get_state_part_boundaries(
        &self,
        part_id: PartId,
    ) -> Result<(PartialState, Option<Vec<u8>>, Option<Vec<u8>>), StorageError> {
        // If chunk view is missing us ShardId::max() as fake value for metrics.
        let shard_id = self
            .flat_storage_chunk_view
            .as_ref()
            .map_or_else(ShardId::max, |chunk_view| chunk_view.shard_uid().shard_id());
        let _span = tracing::debug_span!(
            target: "state-parts",
            "get_state_part_boundaries",
            %shard_id,
            part_id = part_id.idx,
            num_parts = part_id.total)
        .entered();
        let _timer = metrics::GET_STATE_PART_BOUNDARIES_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();

        let PartId { idx, total } = part_id;

        // 1. Extract nodes corresponding to state part boundaries.
        let recording_trie = self.recording_reads_new_recorder();
        let boundaries_read_timer = metrics::GET_STATE_PART_BOUNDARIES_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        let path_begin = recording_trie.find_state_part_boundary(part_id.idx, part_id.total)?;
        let path_end = recording_trie.find_state_part_boundary(part_id.idx + 1, part_id.total)?;
        let boundaries_read_duration = boundaries_read_timer.stop_and_record();
        let recorded_trie = recording_trie.recorded_storage().unwrap();

        tracing::debug!(
            target: "state-parts",
            idx,
            total,
            ?boundaries_read_duration,
            "Found state part boundaries",
        );
        Ok((recorded_trie.nodes, path_begin, path_end))
    }

    /// Creates state part using only the flat storage (i.e. FlatState).
    ///
    /// * part_id - number of the state part, mainly for metrics.
    /// * state_trie - provides access to State for random lookups of values by hash.
    pub fn get_trie_nodes_for_part_with_flat_storage(
        &self,
        part_id: PartId,
        state_trie: &Trie,
    ) -> Result<PartialState, StorageError> {
        // If chunk view is missing us ShardId::max() as fake value for metrics.
        let shard_id = self
            .flat_storage_chunk_view
            .as_ref()
            .map_or_else(ShardId::max, |chunk_view| chunk_view.shard_uid().shard_id());
        let _span = tracing::debug_span!(
            target: "state-parts",
            "get_trie_nodes_for_part_with_flat_storage",
            %shard_id,
            part_id = part_id.idx,
            num_parts = part_id.total)
        .entered();
        let _timer = metrics::GET_STATE_PART_NODES_WITH_FS_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        // TODO(nikurt): Simplify. This is a long function with complex logic.

        let (path_boundary_nodes, path_begin, path_end) =
            state_trie.get_state_part_boundaries(part_id)?;

        let PartialState::TrieValues(path_boundary_nodes) = path_boundary_nodes;

        // 1. Extract all key-value pairs in state part from flat storage.
        let values_read_timer = metrics::GET_STATE_PART_READ_FS_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        let flat_state_iter = self.iter_flat_state_entries(path_begin, path_end)?;
        let mut value_refs = vec![];
        let mut values_inlined = 0;
        let mut all_state_part_items: Vec<_> = flat_state_iter
            .filter_map(|result| {
                let (k, v) = result.expect("failed to read FlatState entry");
                match v {
                    FlatStateValue::Ref(value_ref) => {
                        value_refs.push((k, value_ref.hash));
                        None
                    }
                    FlatStateValue::Inlined(value) => {
                        values_inlined += 1;
                        Some((k, Some(value)))
                    }
                }
            })
            .collect::<Vec<_>>();
        let values_read_duration = values_read_timer.stop_and_record();

        // 2. Lookup Referenced values in State. Note that FlatStorage snapshots don't have State.
        let lookup_values_timer = metrics::GET_STATE_PART_LOOKUP_REF_VALUES_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        let looked_up_value_refs: Vec<_> = value_refs
            .iter()
            .map(|(k, hash)| {
                Ok((
                    k.clone(),
                    Some(state_trie.retrieve_value(hash, AccessOptions::DEFAULT)?.to_vec()),
                ))
            })
            .collect::<Result<_, StorageError>>()
            .unwrap();
        all_state_part_items.extend(looked_up_value_refs.iter().cloned());
        let lookup_values_duration = lookup_values_timer.stop_and_record();

        // 3. Create trie out of all key-value pairs.
        let local_trie_creation_timer = metrics::GET_STATE_PART_CREATE_TRIE_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        let local_state_part_trie = Trie::new(
            Arc::new(TrieMemoryPartialStorage::<false>::default()),
            StateRoot::new(),
            None,
        );
        let local_state_part_nodes = local_state_part_trie
            .update(all_state_part_items.into_iter(), AccessOptions::DEFAULT)?
            .insertions;
        let local_trie_creation_duration = local_trie_creation_timer.stop_and_record();

        // 4. Unite all nodes in memory, traverse trie based on them, return set of visited nodes.
        let final_part_creation_timer = metrics::GET_STATE_PART_COMBINE_ELAPSED
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        let boundary_nodes_storage: HashMap<_, _> =
            path_boundary_nodes.iter().map(|entry| (hash(entry), entry.clone())).collect();
        let mut disk_read_hashes: HashSet<_> = boundary_nodes_storage.keys().cloned().collect();
        disk_read_hashes.extend(value_refs.iter().map(|(_, hash)| hash));
        let mut all_nodes: HashMap<CryptoHash, Arc<[u8]>> = HashMap::new();
        all_nodes.extend(boundary_nodes_storage);
        all_nodes.extend(
            local_state_part_nodes
                .iter()
                .map(|entry| (*entry.hash(), entry.payload().to_vec().into())),
        );
        let partial_storage = Arc::new(TrieMemoryPartialStorage::<true>::new(all_nodes));
        let final_trie = Trie::new(Arc::clone(&partial_storage) as _, self.root, None);
        final_trie.visit_nodes_for_state_part(part_id)?;
        drop(final_trie);
        let final_trie_storage = Arc::into_inner(partial_storage).unwrap();
        let final_state_part_nodes = final_trie_storage.partial_state();
        let PartialState::TrieValues(trie_values) = &final_state_part_nodes;
        let final_part_creation_duration = final_part_creation_timer.stop_and_record();

        // Compute how many nodes were recreated from memory.
        let state_part_num_nodes = trie_values.len();
        let in_memory_created_nodes =
            trie_values.iter().filter(|entry| !disk_read_hashes.contains(&hash(*entry))).count();
        tracing::debug!(
            target: "state-parts",
            ?part_id,
            values_ref = value_refs.len(),
            %values_inlined,
            %in_memory_created_nodes,
            %state_part_num_nodes,
            ?values_read_duration,
            ?lookup_values_duration,
            ?local_trie_creation_duration,
            ?final_part_creation_duration,
            "Created state part",
        );

        metrics::GET_STATE_PART_WITH_FS_VALUES_INLINED
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(values_inlined);
        metrics::GET_STATE_PART_WITH_FS_VALUES_REF
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(value_refs.len() as u64);
        metrics::GET_STATE_PART_WITH_FS_NODES_FROM_DISK
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(disk_read_hashes.len() as u64);
        metrics::GET_STATE_PART_WITH_FS_NODES_IN_MEMORY
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(in_memory_created_nodes as u64);
        metrics::GET_STATE_PART_WITH_FS_NODES
            .with_label_values(&[&shard_id.to_string()])
            .inc_by(state_part_num_nodes as u64);

        Ok(final_state_part_nodes)
    }

    /// Assume we lay out all trie nodes in dfs order visiting children after the parent.
    /// We take all node sizes (memory_usage_direct()) and take all nodes intersecting with
    /// [size_start, size_end) interval, also all nodes necessary to prove it and some
    /// additional nodes defined by the current implementation (TODO #1603 strict spec).
    ///
    /// Creating a StatePart takes all these nodes, validating a StatePart checks that it has the
    /// right set of nodes.
    fn visit_nodes_for_state_part(&self, part_id: PartId) -> Result<(), StorageError> {
        let path_begin = self.find_state_part_boundary(part_id.idx, part_id.total)?;
        let path_end = self.find_state_part_boundary(part_id.idx + 1, part_id.total)?;

        let mut iterator = self.disk_iter()?;
        let nodes_list =
            iterator.visit_nodes_interval(path_begin.as_deref(), path_end.as_deref())?;
        tracing::debug!(target: "state-parts", num_nodes = nodes_list.len());

        Ok(())
    }

    /// Helper method for `find_node_in_dfs_order`.
    /// Tells a child in which we should go to find a node in dfs order corresponding
    /// to `memory_threshold`.
    /// Accumulates `memory_skipped` as memory used by all skipped nodes.
    /// Returns false if we already found desired node and should stop the process.
    fn find_child_in_dfs_order(
        &self,
        memory_threshold: u64,
        node: &mut TrieStorageNodeWithSize,
        memory_skipped: &mut u64,
        key_nibbles: &mut Vec<u8>,
    ) -> Result<bool, StorageError> {
        *memory_skipped += node.node.memory_usage_direct();

        match &node.node {
            TrieStorageNode::Empty => Ok(false),
            TrieStorageNode::Leaf { extension, .. } => {
                let (slice, _) = NibbleSlice::from_encoded(extension);
                key_nibbles.extend(slice.iter());

                // Leaf must contain value, so we found the boundary.
                Ok(false)
            }
            TrieStorageNode::Branch { children, value } => {
                if *memory_skipped > memory_threshold && value.is_some() {
                    // If we skipped enough memory and found some value, we found the boundary.
                    return Ok(false);
                }

                for index in 0..16 {
                    if let Some(child) = &children[index] {
                        let child = self.retrieve_storage_node(child)?;
                        if *memory_skipped + child.memory_usage > memory_threshold {
                            key_nibbles.push(index as u8);
                            *node = child;
                            return Ok(true);
                        }
                        *memory_skipped += child.memory_usage;
                    }
                }

                // This line should be unreachable if we descended into current node.
                // TODO (#8997): test this case properly by simulating trie data corruption.
                Err(StorageError::StorageInconsistentState(format!(
                    "Skipped all children of node {node:?} while searching for memory \
                    threshold {memory_threshold} and skipped {memory_skipped}"
                )))
            }
            TrieStorageNode::Extension { extension, child } => {
                let child = self.retrieve_storage_node(child)?;
                let (slice, _) = NibbleSlice::from_encoded(extension);
                key_nibbles.extend(slice.iter());
                *node = child;
                Ok(true)
            }
        }
    }

    /// Finds first node corresponding to some key-value pair, for which prefix memory usage
    /// is greater than a given threshold.
    /// Returns trie key in nibbles corresponding to this node.
    fn find_node_in_dfs_order(
        &self,
        root_node: &TrieStorageNodeWithSize,
        memory_threshold: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let mut key_nibbles: Vec<u8> = Vec::new();
        let mut node = root_node.clone();
        let mut memory_skipped = 0u64;
        while self.find_child_in_dfs_order(
            memory_threshold,
            &mut node,
            &mut memory_skipped,
            &mut key_nibbles,
        )? {}
        Ok(key_nibbles)
    }

    /// Validates state part for given state root.
    /// Returns error if state part is invalid and Ok otherwise.
    pub fn validate_state_part(
        state_root: &StateRoot,
        part_id: PartId,
        partial_state: PartialState,
    ) -> Result<(), StorageError> {
        let PartialState::TrieValues(nodes) = &partial_state;
        let num_nodes = nodes.len();
        let (trie, storage) = Trie::from_recorded_storage_with_storage::<true>(
            PartialStorage { nodes: partial_state },
            *state_root,
            false,
        );
        trie.visit_nodes_for_state_part(part_id)?;
        drop(trie);
        let storage = Arc::into_inner(storage).unwrap();
        if storage.visited_nodes.len() != num_nodes {
            // As all nodes belonging to state part were visited, there is some
            // unexpected data in downloaded state part.
            return Err(StorageError::UnexpectedTrieValue);
        }
        Ok(())
    }

    fn apply_state_part_impl(
        state_root: &StateRoot,
        part_id: PartId,
        part: PartialState,
    ) -> Result<ApplyStatePartResult, StorageError> {
        if state_root == &Trie::EMPTY_ROOT {
            return Ok(ApplyStatePartResult {
                trie_changes: TrieChanges::empty(Trie::EMPTY_ROOT),
                flat_state_delta: Default::default(),
                contract_codes: vec![],
            });
        }
        let trie = Trie::from_recorded_storage(PartialStorage { nodes: part }, *state_root, false);
        let path_begin = trie.find_state_part_boundary(part_id.idx, part_id.total)?;
        let path_end = trie.find_state_part_boundary(part_id.idx + 1, part_id.total)?;
        let mut iterator = trie.disk_iter()?;
        let trie_traversal_items =
            iterator.visit_nodes_interval(path_begin.as_deref(), path_end.as_deref())?;
        let mut refcount_changes = TrieRefcountDeltaMap::new();
        let mut flat_state_delta = FlatStateChanges::default();
        let mut contract_codes = Vec::new();
        for TrieTraversalItem { hash, key } in trie_traversal_items {
            let value = trie.retrieve_value(&hash, AccessOptions::DEFAULT)?;
            refcount_changes.add(hash, value.to_vec(), 1);
            if let Some(trie_key) = key {
                let flat_state_value = FlatStateValue::on_disk(&value);
                flat_state_delta.insert(trie_key.clone(), Some(flat_state_value));
                if is_contract_code_key(&trie_key) {
                    contract_codes.push(ContractCode::new(value.to_vec(), None));
                }
            }
        }
        let (insertions, deletions) = refcount_changes.into_changes();
        Ok(ApplyStatePartResult {
            trie_changes: TrieChanges {
                old_root: Trie::EMPTY_ROOT,
                new_root: *state_root,
                insertions,
                deletions,
                memtrie_changes: None,
                children_memtrie_changes: Default::default(),
            },
            flat_state_delta,
            contract_codes,
        })
    }

    /// Applies state part and returns the storage changes for the state part and all contract codes extracted from it.
    /// Writing all storage changes gives the complete trie.
    pub fn apply_state_part(
        state_root: &StateRoot,
        part_id: PartId,
        part: PartialState,
    ) -> ApplyStatePartResult {
        Self::apply_state_part_impl(state_root, part_id, part)
            .expect("apply_state_part is guaranteed to succeed when each part is valid")
    }

    pub fn get_memory_usage_from_serialized(bytes: &[u8]) -> Result<u64, StorageError> {
        RawTrieNodeWithSize::try_from_slice(bytes).map(|raw_node| raw_node.memory_usage).map_err(
            |err| StorageError::StorageInconsistentState(format!("Failed to decode node: {err}")),
        )
    }

    fn retrieve_storage_node(
        &self,
        hash: &CryptoHash,
    ) -> Result<TrieStorageNodeWithSize, StorageError> {
        let node = self.retrieve_raw_node(hash, true, AccessOptions::DEFAULT)?;
        Ok(match node.map(|node| node.1) {
            Some(node) => TrieStorageNodeWithSize::from_raw_trie_node_with_size(node),
            None => Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::sync::Arc;

    use rand::Rng;
    use rand::prelude::ThreadRng;
    use rand::seq::SliceRandom;

    use near_primitives::hash::{CryptoHash, hash};

    use crate::adapter::StoreUpdateAdapter;
    use crate::test_utils::{TestTriesBuilder, gen_changes, test_populate_trie};
    use crate::trie::ops::iter::CrumbStatus;
    use crate::trie::trie_tests::merge_trie_changes;
    use crate::trie::{TrieRefcountAddition, ValueHandle};

    use super::*;
    use crate::MissingTrieValue;
    use crate::MissingTrieValueContext;
    use near_primitives::shard_layout::ShardUId;

    /// Checks that sampling state boundaries always gives valid state keys
    /// even if trie contains intermediate nodes.
    #[test]
    fn boundary_is_state_key() {
        // Trie should contain at least two intermediate branches for strings
        // "a" and "b".
        let trie_changes = vec![
            (b"after".to_vec(), Some(vec![1])),
            (b"alley".to_vec(), Some(vec![2])),
            (b"berry".to_vec(), Some(vec![3])),
            (b"brave".to_vec(), Some(vec![4])),
        ];

        // Number of state parts. Must be larger than number of state items to ensure
        // that boundaries are nontrivial.
        let num_parts = 10u64;

        let tries = TestTriesBuilder::new().build();
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);

        let key_boundary = trie.find_state_part_boundary(0, num_parts).unwrap();
        assert!(key_boundary.unwrap().is_empty());

        // Check that all boundaries correspond to some state key by calling `Trie::get`.
        // Note that some state parts can be trivial, which is not a concern.
        for part_id in 1..num_parts {
            let key_boundary = trie.find_state_part_boundary(part_id, num_parts).unwrap().unwrap();
            assert_matches!(trie.get(&key_boundary, AccessOptions::DEFAULT), Ok(Some(_)));
        }

        let key_boundary = trie.find_state_part_boundary(num_parts, num_parts).unwrap();
        assert_eq!(key_boundary, None);
    }

    /// Checks that on degenerate case when trie is a single path, state
    /// parts are still distributed evenly.
    #[test]
    fn single_path_trie() {
        // Values should be big enough to ensure that node and key overhead are
        // not significant.
        let value_len = 1000usize;
        // Corner case when trie is a single path from empty string to "aaaa".
        let trie_changes = vec![
            (b"a".to_vec(), Some(vec![1; value_len])),
            (b"aa".to_vec(), Some(vec![2; value_len])),
            (b"aaa".to_vec(), Some(vec![3; value_len])),
            (b"aaaa".to_vec(), Some(vec![4; value_len])),
        ];
        // We reshard into `num_keys + 1` parts for convenience of testing,
        // because right boundaries are exclusive. This way first part is
        // empty and other parts contain exactly one key.
        let num_parts = trie_changes.len() + 1;

        let tries = TestTriesBuilder::new().build();
        let state_root = test_populate_trie(
            &tries,
            &Trie::EMPTY_ROOT,
            ShardUId::single_shard(),
            trie_changes.clone(),
        );
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);

        for part_id in 1..num_parts {
            let key_boundary =
                trie.find_state_part_boundary(part_id as u64, num_parts as u64).unwrap().unwrap();
            assert_eq!(key_boundary, trie_changes[part_id - 1].0);
        }
    }

    impl Trie {
        /// Combines all parts and returns TrieChanges that can be applied to storage.
        ///
        /// # Input
        /// parts[i] has trie nodes for part i
        ///
        /// # Errors
        /// StorageError if data is inconsistent. Should never happen if each part was validated.
        pub fn combine_state_parts_naive(
            state_root: &StateRoot,
            parts: &[PartialState],
        ) -> Result<TrieChanges, StorageError> {
            let nodes = PartialState::TrieValues(
                parts
                    .iter()
                    .flat_map(|PartialState::TrieValues(nodes)| nodes.iter())
                    .cloned()
                    .collect(),
            );
            let trie = Trie::from_recorded_storage(PartialStorage { nodes }, *state_root, false);
            let mut insertions = <HashMap<CryptoHash, (Vec<u8>, u32)>>::new();
            trie.traverse_all_nodes(|hash| {
                if let Some((_bytes, rc)) = insertions.get_mut(hash) {
                    *rc += 1;
                } else {
                    let bytes = trie.retrieve_value(hash, AccessOptions::DEFAULT).unwrap();
                    insertions.insert(*hash, (bytes.to_vec(), 1));
                }
                Ok(())
            })?;
            let mut insertions = insertions
                .into_iter()
                .map(|(k, (v, rc))| TrieRefcountAddition {
                    trie_node_or_value_hash: k,
                    trie_node_or_value: v,
                    rc: std::num::NonZeroU32::new(rc).unwrap(),
                })
                .collect::<Vec<_>>();
            insertions.sort();
            Ok(TrieChanges {
                old_root: Default::default(),
                new_root: *state_root,
                insertions,
                deletions: vec![],
                memtrie_changes: None,
                children_memtrie_changes: Default::default(),
            })
        }

        /// on_enter is applied for nodes as well as values
        fn traverse_all_nodes<F: FnMut(&CryptoHash) -> Result<(), StorageError>>(
            &self,
            mut on_enter: F,
        ) -> Result<(), StorageError> {
            if self.root == Trie::EMPTY_ROOT {
                return Ok(());
            }
            let mut stack: Vec<(CryptoHash, TrieStorageNodeWithSize, CrumbStatus)> = Vec::new();
            let root_node = self.retrieve_storage_node(&self.root)?;
            stack.push((self.root, root_node, CrumbStatus::Entering));
            while let Some((hash, node, position)) = stack.pop() {
                if let CrumbStatus::Entering = position {
                    on_enter(&hash)?;
                }
                let mut on_enter_value = |value: &ValueHandle| {
                    if let ValueHandle::HashAndSize(value) = value {
                        on_enter(&value.hash)
                    } else {
                        unreachable!("only possible while mutating")
                    }
                };
                match &node.node {
                    TrieStorageNode::Empty => {
                        continue;
                    }
                    TrieStorageNode::Leaf { value, .. } => {
                        on_enter_value(value)?;
                        continue;
                    }
                    TrieStorageNode::Branch { children, value } => match position {
                        CrumbStatus::Entering => {
                            if let Some(value) = value {
                                on_enter_value(value)?;
                            }
                            stack.push((hash, node, CrumbStatus::AtChild(0)));
                            continue;
                        }
                        CrumbStatus::AtChild(mut i) => loop {
                            if i >= 16 {
                                stack.push((hash, node, CrumbStatus::Exiting));
                                break;
                            }
                            if let Some(child_hash) = children[i as usize] {
                                let child = self.retrieve_storage_node(&child_hash)?;
                                stack.push((child_hash, node, CrumbStatus::AtChild(i + 1)));
                                stack.push((child_hash, child, CrumbStatus::Entering));
                                break;
                            }
                            i += 1;
                        },
                        CrumbStatus::Exiting => {
                            continue;
                        }
                        CrumbStatus::At => {
                            continue;
                        }
                    },
                    TrieStorageNode::Extension { child, .. } => {
                        if let CrumbStatus::Entering = position {
                            let child_hash = *child;
                            let child = self.retrieve_storage_node(&child_hash)?;
                            stack.push((child_hash, node, CrumbStatus::Exiting));
                            stack.push((child_hash, child, CrumbStatus::Entering));
                        }
                    }
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test_combine_empty_trie_parts() {
        let state_root = Trie::EMPTY_ROOT;
        let _ = Trie::combine_state_parts_naive(&state_root, &[]).unwrap();
        let _ = Trie::validate_state_part(
            &state_root,
            PartId::new(0, 1),
            PartialState::TrieValues(vec![]),
        )
        .unwrap();
        let _ = Trie::apply_state_part(
            &state_root,
            PartId::new(0, 1),
            PartialState::TrieValues(vec![]),
        );
    }

    fn construct_trie_for_big_parts_1(
        rng: &mut ThreadRng,
        max_key_length: u64,
        big_value_length: u64,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        // Test #1: a long path where every node on the path has a large value
        let mut trie_changes = Vec::new();
        for i in 0..max_key_length {
            // ([255,255,..,255], big_value)
            let key = (0..(i + 1)).map(|_| 255u8).collect::<Vec<_>>();
            let value = (0..big_value_length).map(|_| rng.r#gen::<u8>()).collect::<Vec<_>>();
            trie_changes.push((key, Some(value)));
        }
        trie_changes
    }

    fn construct_trie_for_big_parts_2(
        rng: &mut ThreadRng,
        max_key_length: u64,
        big_value_length: u64,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        // Test #2: a long path where every node on the path has a large value, is a branch
        // and each of its children is a branch.
        let mut trie_changes =
            construct_trie_for_big_parts_1(rng, max_key_length, big_value_length);
        let small_value_length = 20;
        for i in 0..max_key_length {
            for x in 0u8..15u8 {
                for y in 0u8..15u8 {
                    {
                        // ([255,255,..,255]xy, small_value)
                        // this means every 000..000 node is a branch and all of its children are branches
                        let mut key = (0..(i + 1)).map(|_| 255u8).collect::<Vec<_>>();
                        key.push(x * 16 + y);
                        let value =
                            (0..small_value_length).map(|_| rng.r#gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                    {
                        let mut key = (0..i).map(|_| 255u8).collect::<Vec<_>>();
                        key.push(16 * 15 + x);
                        key.push(y * 16);
                        let value =
                            (0..small_value_length).map(|_| rng.r#gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                    {
                        let mut key = (0..i).map(|_| 255u8).collect::<Vec<_>>();
                        key.push(16 * x + 15);
                        key.push(y);
                        let value =
                            (0..small_value_length).map(|_| rng.r#gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                }
            }
        }
        trie_changes
    }

    /// Helper function checking that for given trie generator size of each
    /// part is approximately bounded by `total_size / num_parts` with overhead
    /// for proof and trie items irregularity.
    /// TODO (#8997): run it on largest keys (2KB) and values (4MB) allowed in mainnet.
    fn run_test_parts_not_huge<F>(gen_trie_changes: F, big_value_length: u64)
    where
        F: FnOnce(&mut ThreadRng, u64, u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut rng = rand::thread_rng();
        let max_key_length = 50u64;
        let max_key_length_in_nibbles = max_key_length * 2;
        let max_node_children = 16;
        let max_node_serialized_size = 32 * max_node_children + 100; // Full branch node overhead.
        let max_proof_overhead =
            max_key_length_in_nibbles * max_node_children * max_node_serialized_size;
        let max_part_overhead =
            big_value_length.max(max_key_length_in_nibbles * max_node_serialized_size * 2);
        let trie_changes = gen_trie_changes(&mut rng, max_key_length, big_value_length);
        let tries = TestTriesBuilder::new().build();
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_changes);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
        let memory_size = trie.retrieve_root_node().unwrap().memory_usage;
        for num_parts in [2, 3, 5, 10, 50].iter().cloned() {
            let part_size_limit = (memory_size + num_parts - 1) / num_parts;

            for part_id in 0..num_parts {
                // Compute proof with size and check that it doesn't exceed theoretical boundary for
                // the path with full set of left siblings of maximal possible size.
                let trie_recording = trie.recording_reads_new_recorder();
                let left_key_boundary =
                    trie_recording.find_state_part_boundary(part_id, num_parts).unwrap().unwrap();
                if part_id != 0 {
                    assert_matches!(
                        trie.get(&left_key_boundary, AccessOptions::DEFAULT),
                        Ok(Some(_))
                    );
                }
                let PartialState::TrieValues(proof_nodes) =
                    trie_recording.recorded_storage().unwrap().nodes;
                let proof_size = proof_nodes.iter().map(|node| node.len()).sum::<usize>() as u64;
                assert!(
                    proof_size <= max_proof_overhead,
                    "For part {}/{} left boundary proof size {} exceeds limit {}",
                    part_id,
                    num_parts,
                    proof_size,
                    max_proof_overhead
                );

                let PartialState::TrieValues(part_nodes) = trie
                    .get_trie_nodes_for_part_without_flat_storage(PartId::new(part_id, num_parts))
                    .unwrap();
                // TODO (#8997): it's a bit weird that raw lengths are compared to
                // config values. Consider better defined assertion.
                let total_size = part_nodes.iter().map(|node| node.len()).sum::<usize>() as u64;
                assert!(
                    total_size <= part_size_limit + proof_size + max_part_overhead,
                    "Part {}/{} is too big. Size: {}, size limit: {}",
                    part_id,
                    num_parts,
                    total_size,
                    part_size_limit + proof_size + max_part_overhead,
                );
            }
        }
    }

    #[test]
    fn test_parts_not_huge_1() {
        run_test_parts_not_huge(construct_trie_for_big_parts_1, 100_000);
    }

    /// TODO (#8997): consider:
    /// * adding more testcases for big and small key/value lengths, other trie structures;
    /// * speeding this test up.
    #[test]
    fn test_parts_not_huge_2() {
        run_test_parts_not_huge(construct_trie_for_big_parts_2, 100_000);
    }

    #[test]
    fn test_combine_state_parts() {
        let mut rng = rand::thread_rng();
        for _ in 0..2000 {
            let tries = TestTriesBuilder::new().build();
            let trie_changes = gen_changes(&mut rng, 20);
            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);
            let root_memory_usage = trie.retrieve_root_node().unwrap().memory_usage;
            {
                // Test that combining all parts gets all nodes
                let num_parts = rng.gen_range(2..10);
                let parts = (0..num_parts)
                    .map(|part_id| {
                        trie.get_trie_nodes_for_part_without_flat_storage(PartId::new(
                            part_id, num_parts,
                        ))
                        .unwrap()
                    })
                    .collect::<Vec<_>>();

                let trie_changes = check_combine_state_parts(trie.get_root(), num_parts, &parts);

                let mut nodes = <HashMap<CryptoHash, Arc<[u8]>>>::new();
                let sizes_vec = parts
                    .iter()
                    .map(|PartialState::TrieValues(nodes)| {
                        nodes.iter().map(|node| node.len()).sum::<usize>()
                    })
                    .collect::<Vec<_>>();

                for part in parts {
                    let PartialState::TrieValues(part_nodes) = part;
                    for node in part_nodes {
                        nodes.insert(hash(&node), node);
                    }
                }
                let all_nodes = nodes.into_iter().map(|(_hash, node)| node).collect::<Vec<_>>();
                assert_eq!(all_nodes.len(), trie_changes.insertions.len());
                let size_of_all = all_nodes.iter().map(|node| node.len()).sum::<usize>();
                let num_nodes = all_nodes.len();
                assert_eq!(
                    Trie::validate_state_part(
                        trie.get_root(),
                        PartId::new(0, 1),
                        PartialState::TrieValues(all_nodes),
                    ),
                    Ok(())
                );

                let sum_of_sizes = sizes_vec.iter().sum::<usize>();
                // Manually check that sizes are reasonable
                println!("------------------------------");
                println!("Number of nodes: {:?}", num_nodes);
                println!("Sizes of parts: {:?}", sizes_vec);
                println!(
                    "All nodes size: {:?}, sum_of_sizes: {:?}, memory_usage: {:?}",
                    size_of_all, sum_of_sizes, root_memory_usage
                );
                // borsh serialize should be about this size
                assert!(size_of_all + 8 * num_nodes <= root_memory_usage as usize);
            }
        }
    }

    fn format_simple_trie_refcount_diff<T: Hash + Debug + Eq>(left: &[T], right: &[T]) -> String {
        let left_set: HashSet<_> = HashSet::from_iter(left.iter());
        let right_set: HashSet<_> = HashSet::from_iter(right.iter());
        format!(
            "left: {:?} right: {:?}",
            left_set.difference(&right_set),
            right_set.difference(&left_set)
        )
    }

    fn format_simple_trie_changes_diff(left: &TrieChanges, right: &TrieChanges) -> String {
        format!(
            "insertions diff: {}, deletions diff: {}",
            format_simple_trie_refcount_diff(&left.insertions, &right.insertions),
            format_simple_trie_refcount_diff(&left.deletions, &right.deletions)
        )
    }

    /// Helper function checking that two ways of combining state parts are identical:
    /// 1) Create partial storage over all nodes in state parts and traverse all
    /// nodes in the storage;
    /// 2) Traverse each part separately and merge all trie changes.
    fn check_combine_state_parts(
        state_root: &CryptoHash,
        num_parts: u64,
        parts: &[PartialState],
    ) -> TrieChanges {
        let trie_changes = Trie::combine_state_parts_naive(state_root, parts).unwrap();

        let trie_changes_new = {
            let changes = (0..num_parts)
                .map(|part_id| {
                    Trie::apply_state_part(
                        state_root,
                        PartId::new(part_id, num_parts),
                        parts[part_id as usize].clone(),
                    )
                    .trie_changes
                })
                .collect::<Vec<_>>();

            merge_trie_changes(changes)
        };
        assert_eq!(
            trie_changes,
            trie_changes_new,
            "{}",
            format_simple_trie_changes_diff(&trie_changes, &trie_changes_new)
        );
        trie_changes
    }

    /// Checks that state part with unexpected data or not enough data doesn't
    /// pass validation.
    /// Doesn't use FlatStorage.
    #[test]
    fn invalid_state_parts() {
        let tries = TestTriesBuilder::new().build();
        let shard_uid = ShardUId::single_shard();
        let part_id = PartId::new(1, 2);
        let trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);

        let state_items = vec![
            (b"a".to_vec(), vec![1]),
            (b"aa".to_vec(), vec![2]),
            (b"ab".to_vec(), vec![3]),
            (b"b".to_vec(), vec![4]),
            (b"ba".to_vec(), vec![5]),
        ];

        let changes_for_trie = state_items.iter().cloned().map(|(k, v)| (k, Some(v)));
        let trie_changes = trie.update(changes_for_trie, AccessOptions::DEFAULT).unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();

        let trie = tries.get_view_trie_for_shard(shard_uid, root);
        let PartialState::TrieValues(trie_values) = trie
            .get_trie_nodes_for_part_without_flat_storage(part_id)
            .expect("State part generation using Trie must work");
        let num_trie_values = trie_values.len();
        assert!(num_trie_values >= 2);

        // Check that shuffled state part also passes validation.
        let mut rng = rand::thread_rng();
        for _ in 0..5 {
            let mut trie_values_shuffled = trie_values.clone();
            trie_values_shuffled.shuffle(&mut rng);
            let state_part = PartialState::TrieValues(trie_values_shuffled);
            assert_eq!(Trie::validate_state_part(&root, part_id, state_part), Ok(()));
        }

        // Remove middle element from state part, check that validation fails.
        let mut trie_values_missing = trie_values.clone();
        trie_values_missing.remove(num_trie_values / 2);
        let wrong_state_part = PartialState::TrieValues(trie_values_missing);

        assert_matches!(
            Trie::validate_state_part(&root, part_id, wrong_state_part),
            Err(StorageError::MissingTrieValue(MissingTrieValue {
                context: MissingTrieValueContext::TrieMemoryPartialStorage,
                hash: _
            }))
        );

        // Add extra value to the state part, check that validation fails.
        let mut trie_values_extra = trie_values.clone();
        trie_values_extra.push(vec![11].into());
        let wrong_state_part = PartialState::TrieValues(trie_values_extra);
        assert_eq!(
            Trie::validate_state_part(&root, part_id, wrong_state_part),
            Err(StorageError::UnexpectedTrieValue)
        );

        // Duplicate a value in the state part, check that validation fails, because
        // values in state part must be deduplicated.
        let mut trie_values_extra_same = trie_values;
        trie_values_extra_same
            .push(trie_values_extra_same[trie_values_extra_same.len() / 2].clone());
        let wrong_state_part = PartialState::TrieValues(trie_values_extra_same);
        assert_eq!(
            Trie::validate_state_part(&root, part_id, wrong_state_part),
            Err(StorageError::UnexpectedTrieValue)
        );
    }

    /// Check on random samples that state parts can be validated independently
    /// from the entire trie.
    #[test]
    fn test_get_trie_nodes_for_part() {
        let mut rng = rand::thread_rng();
        for _ in 0..20 {
            let tries = TestTriesBuilder::new().build();
            let trie_changes = gen_changes(&mut rng, 10);

            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let trie = tries.get_trie_for_shard(ShardUId::single_shard(), state_root);

            for _ in 0..10 {
                // Test that creating and validating are consistent
                let num_parts: u64 = rng.gen_range(1..10);
                let part_id = rng.gen_range(0..num_parts);
                let trie_nodes = trie
                    .get_trie_nodes_for_part_without_flat_storage(PartId::new(part_id, num_parts))
                    .unwrap();
                assert_eq!(
                    Trie::validate_state_part(
                        trie.get_root(),
                        PartId::new(part_id, num_parts),
                        trie_nodes,
                    ),
                    Ok(())
                );
            }
        }
    }

    /// Checks sanity of generating state part using flat storage.
    #[test]
    fn get_trie_nodes_for_part_with_flat_storage() {
        let value_len = 1000usize;

        let tries = TestTriesBuilder::new().with_flat_storage(true).build();
        let shard_uid = ShardUId::single_shard();
        let block_hash = CryptoHash::default();
        let part_id = PartId::new(1, 3);
        let trie = tries.get_trie_for_shard(shard_uid, Trie::EMPTY_ROOT);

        // Trie with three big independent children.
        let state_items = vec![
            (b"a".to_vec(), vec![1; value_len]),
            (b"aa".to_vec(), vec![2; value_len]),
            (b"ab".to_vec(), vec![3; value_len]),
            (b"b".to_vec(), vec![4; value_len]),
            (b"ba".to_vec(), vec![5; value_len]),
            (b"bb".to_vec(), vec![6; value_len]),
            (b"c".to_vec(), vec![7; value_len]),
            (b"ca".to_vec(), vec![8; value_len]),
            (b"cb".to_vec(), vec![9; value_len]),
        ];
        let changes_for_trie = state_items.iter().cloned().map(|(k, v)| (k, Some(v)));
        let trie_changes = trie.update(changes_for_trie, AccessOptions::DEFAULT).unwrap();
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit().unwrap();

        // Get correct state part using trie without flat storage.
        let trie_without_flat = tries.get_view_trie_for_shard(shard_uid, root);
        let state_part = trie_without_flat
            .get_trie_nodes_for_part_without_flat_storage(part_id)
            .expect("State part generation using Trie must work");
        assert_eq!(Trie::validate_state_part(&root, part_id, state_part.clone()), Ok(()));
        assert!(state_part.len() > 0);

        let view_chunk_trie =
            tries.get_trie_with_block_hash_for_shard(shard_uid, root, &block_hash, true);

        assert_matches!(
            view_chunk_trie.get_trie_nodes_for_part_with_flat_storage(part_id, &trie_without_flat,),
            Err(StorageError::MissingTrieValue(MissingTrieValue {
                context: MissingTrieValueContext::TrieMemoryPartialStorage,
                hash: _
            }))
        );

        // Fill flat storage and check that state part creation succeeds.
        let changes_for_delta =
            state_items.into_iter().map(|(k, v)| (k, Some(FlatStateValue::inlined(&v))));
        let delta = FlatStateChanges::from(changes_for_delta);
        let mut store_update = tries.store_update();
        delta.apply_to_flat_state(&mut store_update.flat_store_update(), shard_uid);
        store_update.commit().unwrap();

        let view_chunk_trie =
            tries.get_trie_with_block_hash_for_shard(shard_uid, root, &block_hash, true);
        let state_part_with_flat = view_chunk_trie
            .get_trie_nodes_for_part_with_flat_storage(PartId::new(1, 3), &trie_without_flat);
        assert_eq!(state_part_with_flat, Ok(state_part.clone()));

        // Remove some key from state part from trie storage.
        // Check that trie-only state part generation fails but trie & flat
        // storage generation succeeds, as it doesn't access intermediate nodes.
        let mut store_update = tries.store_update();
        let store_value = vec![5; value_len];
        let value_hash = hash(&store_value);
        store_update.decrement_refcount(shard_uid, &value_hash);
        store_update.commit().unwrap();

        assert_eq!(
            trie_without_flat.get_trie_nodes_for_part_without_flat_storage(part_id),
            Err(StorageError::MissingTrieValue(MissingTrieValue {
                context: MissingTrieValueContext::TrieStorage,
                hash: value_hash
            })),
        );

        assert_eq!(
            view_chunk_trie.get_trie_nodes_for_part_with_flat_storage(part_id, &trie_without_flat,),
            Ok(state_part)
        );

        // Remove some key from state part from flat storage.
        // Check that state part creation succeeds but generated state part
        // is invalid.
        let mut store_update = tries.store_update();
        let delta = FlatStateChanges::from(vec![(b"ba".to_vec(), None)]);
        delta.apply_to_flat_state(&mut store_update.flat_store_update(), shard_uid);
        store_update.commit().unwrap();

        assert_matches!(
            view_chunk_trie.get_trie_nodes_for_part_with_flat_storage(part_id, &trie_without_flat,),
            Err(StorageError::MissingTrieValue(MissingTrieValue {
                context: MissingTrieValueContext::TrieMemoryPartialStorage,
                hash: _
            }))
        );
    }
}
