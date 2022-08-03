use std::collections::HashMap;

use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::state::PartId;
use near_primitives::types::StateRoot;
use tracing::error;

use crate::trie::iterator::TrieTraversalItem;
use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::{
    ApplyStatePartResult, NodeHandle, RawTrieNodeWithSize, TrieNode, TrieNodeWithSize,
};
use crate::{PartialStorage, StorageError, Trie, TrieChanges};
use near_primitives::contract::ContractCode;
use near_primitives::state_record::is_contract_code_key;

impl Trie {
    /// Computes the set of trie nodes for a state part.
    ///
    /// # Panics
    /// storage must be a TrieCachingStorage
    /// part_id must be in [0..num_parts)
    ///
    /// # Errors
    /// StorageError if the storage is corrupted
    pub fn get_trie_nodes_for_part(
        &self,
        part_id: PartId,
        state_root: &StateRoot,
    ) -> Result<PartialState, StorageError> {
        assert!(self.storage.as_caching_storage().is_some());

        let with_recording = self.recording_reads();
        with_recording.visit_nodes_for_state_part(state_root, part_id)?;
        let recorded = with_recording.recorded_storage().unwrap();

        let trie_nodes = recorded.nodes;
        Ok(trie_nodes)
    }

    /// Assume we lay out all trie nodes in dfs order visiting children after the parent.
    /// We take all node sizes (memory_usage_direct()) and take all nodes intersecting with
    /// [size_start, size_end) interval, also all nodes necessary to prove it and some
    /// additional nodes defined by the current implementation (TODO #1603 strict spec).
    ///
    /// Creating a StatePart takes all these nodes, validating a StatePart checks that it has the
    /// right set of nodes.
    fn visit_nodes_for_state_part(
        &self,
        root_hash: &CryptoHash,
        part_id: PartId,
    ) -> Result<(), StorageError> {
        let path_begin = self.find_path_for_part_boundary(root_hash, part_id.idx, part_id.total)?;
        let path_end =
            self.find_path_for_part_boundary(root_hash, part_id.idx + 1, part_id.total)?;
        let mut iterator = self.iter(root_hash)?;
        iterator.visit_nodes_interval(&path_begin, &path_end)?;

        // Extra nodes for compatibility with the previous version of computing state parts
        if part_id.idx + 1 != part_id.total {
            let mut iterator = self.iter(root_hash)?;
            let path_end_encoded = NibbleSlice::encode_nibbles(&path_end, false);
            iterator.seek_nibble_slice(NibbleSlice::from_encoded(&path_end_encoded[..]).0)?;
            if let Some(item) = iterator.next() {
                item?;
            }
        }

        Ok(())
    }

    /// Part part_id has nodes with paths [ path(part_id) .. path(part_id + 1) )
    /// path is returned as nibbles, last path is vec![16], previous paths end in nodes
    pub(crate) fn find_path_for_part_boundary(
        &self,
        state_root: &StateRoot,
        part_id: u64,
        num_parts: u64,
    ) -> Result<Vec<u8>, StorageError> {
        assert!(part_id <= num_parts);
        if part_id == num_parts {
            return Ok(vec![16]);
        }
        let root_node = self.retrieve_node(state_root)?;
        let total_size = root_node.memory_usage;
        let size_start = (total_size + num_parts - 1) / num_parts * part_id;
        self.find_path(&root_node, size_start)
    }

    fn find_child(
        &self,
        size_start: u64,
        node: &mut TrieNodeWithSize,
        size_skipped: &mut u64,
        key_nibbles: &mut Vec<u8>,
    ) -> Result<bool, StorageError> {
        let node_size = node.node.memory_usage_direct_no_memory();
        if *size_skipped + node_size <= size_start {
            *size_skipped += node_size;
        } else {
            return Ok(false);
        }
        match &node.node {
            TrieNode::Empty => Ok(false),
            TrieNode::Leaf(key, _) => {
                let (slice, _is_leaf) = NibbleSlice::from_encoded(key);
                key_nibbles.extend(slice.iter());
                Ok(false)
            }
            TrieNode::Branch(children, _) => {
                for child_index in 0..children.len() {
                    let child = match &children[child_index] {
                        None => {
                            continue;
                        }
                        Some(NodeHandle::InMemory(_)) => {
                            unreachable!("only possible while mutating")
                        }
                        Some(NodeHandle::Hash(h)) => self.retrieve_node(h)?,
                    };
                    if *size_skipped + child.memory_usage <= size_start {
                        *size_skipped += child.memory_usage;
                        continue;
                    } else {
                        key_nibbles.push(child_index as u8);
                        *node = child;
                        return Ok(true);
                    }
                }
                // This line should not be reached if node.memory_usage > size_start
                // To avoid changing production behavior, I'm just adding a debug_assert here
                // to indicate that
                debug_assert!(
                    false,
                    "This should not be reached target size {} node memory usage {} size skipped {}",
                    size_start, node.memory_usage, size_skipped,
                );
                error!(
                    target: "state_parts",
                    "This should not be reached target size {} node memory usage {} size skipped {}",
                    size_start, node.memory_usage, size_skipped,
                );
                key_nibbles.push(16u8);
                Ok(false)
            }
            TrieNode::Extension(key, child_handle) => {
                let child = match child_handle {
                    NodeHandle::InMemory(_) => unreachable!("only possible while mutating"),
                    NodeHandle::Hash(h) => self.retrieve_node(h)?,
                };
                let (slice, _is_leaf) = NibbleSlice::from_encoded(key);
                key_nibbles.extend(slice.iter());
                *node = child;
                Ok(true)
            }
        }
    }

    // find the first node so that including this node, the traversed size is larger than size
    fn find_path(&self, root_node: &TrieNodeWithSize, size: u64) -> Result<Vec<u8>, StorageError> {
        if root_node.memory_usage <= size {
            return Ok(vec![16u8]);
        }
        let mut key_nibbles: Vec<u8> = Vec::new();
        let mut node = root_node.clone();
        let mut size_skipped = 0u64;
        while self.find_child(size, &mut node, &mut size_skipped, &mut key_nibbles)? {}
        Ok(key_nibbles)
    }

    /// Validate state part
    ///
    /// # Panics
    /// part_id must be in [0..num_parts)
    ///
    /// # Errors
    /// StorageError::TrieNodeWithMissing if some nodes are missing
    pub fn validate_trie_nodes_for_part(
        state_root: &StateRoot,
        part_id: PartId,
        trie_nodes: PartialState,
    ) -> Result<(), StorageError> {
        let num_nodes = trie_nodes.0.len();
        let trie = Trie::from_recorded_storage(PartialStorage { nodes: trie_nodes });

        trie.visit_nodes_for_state_part(state_root, part_id)?;
        let storage = trie.storage.as_partial_storage().unwrap();

        if storage.visited_nodes.borrow().len() != num_nodes {
            // TODO #1603 not actually TrieNodeMissing.
            // The error is that the proof has more nodes than needed.
            return Err(StorageError::TrieNodeMissing);
        }
        Ok(())
    }

    fn apply_state_part_impl(
        state_root: &StateRoot,
        part_id: PartId,
        part: Vec<Vec<u8>>,
    ) -> Result<ApplyStatePartResult, StorageError> {
        if state_root == &Trie::EMPTY_ROOT {
            return Ok(ApplyStatePartResult {
                trie_changes: TrieChanges::empty(Trie::EMPTY_ROOT),
                contract_codes: vec![],
            });
        }
        let trie = Trie::from_recorded_storage(PartialStorage { nodes: PartialState(part) });
        let path_begin =
            trie.find_path_for_part_boundary(state_root, part_id.idx, part_id.total)?;
        let path_end =
            trie.find_path_for_part_boundary(state_root, part_id.idx + 1, part_id.total)?;
        let mut iterator = trie.iter(state_root)?;
        let trie_traversal_items = iterator.visit_nodes_interval(&path_begin, &path_end)?;
        let mut map = HashMap::new();
        let mut contract_codes = Vec::new();
        for TrieTraversalItem { hash, key } in trie_traversal_items {
            let value = trie.storage.retrieve_raw_bytes(&hash)?;
            map.entry(hash).or_insert_with(|| (value.to_vec(), 0)).1 += 1;
            if let Some(trie_key) = key {
                if is_contract_code_key(&trie_key) {
                    contract_codes.push(ContractCode::new(value.to_vec(), None));
                }
            }
        }
        let (insertions, deletions) = Trie::convert_to_insertions_and_deletions(map);
        Ok(ApplyStatePartResult {
            trie_changes: TrieChanges {
                old_root: Trie::EMPTY_ROOT,
                new_root: *state_root,
                insertions,
                deletions,
            },
            contract_codes,
        })
    }

    /// Applies state part and returns the storage changes for the state part and all contract codes extracted from it.
    /// Writing all storage changes gives the complete trie.
    pub fn apply_state_part(
        state_root: &StateRoot,
        part_id: PartId,
        part: Vec<Vec<u8>>,
    ) -> ApplyStatePartResult {
        Self::apply_state_part_impl(state_root, part_id, part)
            .expect("apply_state_part is guaranteed to succeed when each part is valid")
    }

    pub fn get_memory_usage_from_serialized(bytes: &Vec<u8>) -> Result<u64, StorageError> {
        match RawTrieNodeWithSize::decode(bytes) {
            Ok(value) => Ok(TrieNodeWithSize::from_raw(value).memory_usage),
            Err(_) => {
                Err(StorageError::StorageInconsistentState("Failed to decode node".to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::prelude::ThreadRng;
    use rand::Rng;

    use near_primitives::hash::{hash, CryptoHash};

    use crate::test_utils::{create_tries, gen_changes, test_populate_trie};
    use crate::trie::iterator::CrumbStatus;
    use crate::trie::{TrieRefcountChange, ValueHandle};

    use super::*;
    use near_primitives::shard_layout::ShardUId;

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
            parts: &Vec<Vec<Vec<u8>>>,
        ) -> Result<TrieChanges, StorageError> {
            let nodes = parts
                .iter()
                .map(|part| part.iter())
                .flatten()
                .map(|data| data.to_vec())
                .collect::<Vec<_>>();
            let trie = Trie::from_recorded_storage(PartialStorage { nodes: PartialState(nodes) });
            let mut insertions = <HashMap<CryptoHash, (Vec<u8>, u32)>>::new();
            trie.traverse_all_nodes(state_root, |hash| {
                if let Some((_bytes, rc)) = insertions.get_mut(hash) {
                    *rc += 1;
                } else {
                    let bytes = trie.storage.retrieve_raw_bytes(hash)?;
                    insertions.insert(*hash, (bytes.to_vec(), 1));
                }
                Ok(())
            })?;
            let mut insertions = insertions
                .into_iter()
                .map(|(k, (v, rc))| TrieRefcountChange {
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
            })
        }

        /// on_enter is applied for nodes as well as values
        fn traverse_all_nodes<F: FnMut(&CryptoHash) -> Result<(), StorageError>>(
            &self,
            root: &CryptoHash,
            mut on_enter: F,
        ) -> Result<(), StorageError> {
            if root == &Trie::EMPTY_ROOT {
                return Ok(());
            }
            let mut stack: Vec<(CryptoHash, TrieNodeWithSize, CrumbStatus)> = Vec::new();
            let root_node = self.retrieve_node(root)?;
            stack.push((*root, root_node, CrumbStatus::Entering));
            while let Some((hash, node, position)) = stack.pop() {
                if let CrumbStatus::Entering = position {
                    on_enter(&hash)?;
                }
                match &node.node {
                    TrieNode::Empty => {
                        continue;
                    }
                    TrieNode::Leaf(_, value) => {
                        match value {
                            ValueHandle::HashAndSize(_, hash) => {
                                on_enter(hash)?;
                            }
                            ValueHandle::InMemory(_) => {
                                unreachable!("only possible while mutating")
                            }
                        }
                        continue;
                    }
                    TrieNode::Branch(children, value) => match position {
                        CrumbStatus::Entering => {
                            match value {
                                Some(ValueHandle::HashAndSize(_, hash)) => {
                                    on_enter(hash)?;
                                }
                                _ => {}
                            }
                            stack.push((hash, node, CrumbStatus::AtChild(0)));
                            continue;
                        }
                        CrumbStatus::AtChild(mut i) => {
                            while i < 16 {
                                if let Some(NodeHandle::Hash(_h)) = children[i].as_ref() {
                                    break;
                                }
                                i += 1;
                            }
                            if i < 16 {
                                if let Some(NodeHandle::Hash(h)) = children[i].clone() {
                                    let child = self.retrieve_node(&h)?;
                                    stack.push((hash, node, CrumbStatus::AtChild(i + 1)));
                                    stack.push((h, child, CrumbStatus::Entering));
                                } else {
                                    stack.push((hash, node, CrumbStatus::Exiting));
                                }
                            } else {
                                stack.push((hash, node, CrumbStatus::Exiting));
                            }
                        }
                        CrumbStatus::Exiting => {
                            continue;
                        }
                        CrumbStatus::At => {
                            continue;
                        }
                    },
                    TrieNode::Extension(_key, child) => {
                        if let CrumbStatus::Entering = position {
                            match child.clone() {
                                NodeHandle::InMemory(_) => {
                                    unreachable!("only possible while mutating")
                                }
                                NodeHandle::Hash(h) => {
                                    let child = self.retrieve_node(&h)?;
                                    stack.push((hash, node, CrumbStatus::Exiting));
                                    stack.push((h, child, CrumbStatus::Entering));
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        fn visit_nodes_for_size_range_old(
            &self,
            root_hash: &CryptoHash,
            size_start: u64,
            size_end: u64,
        ) -> Result<(), StorageError> {
            let root_node = self.retrieve_node(root_hash)?;
            let path_begin = self.find_path(&root_node, size_start)?;
            let path_end = self.find_path(&root_node, size_end)?;

            let mut iterator = self.iter(root_hash)?;
            let path_begin_encoded = NibbleSlice::encode_nibbles(&path_begin, false);
            iterator.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded[..]).0)?;
            loop {
                match iterator.next() {
                    None => break,
                    Some(Err(e)) => {
                        return Err(e);
                    }
                    Some(Ok(_item)) => {
                        // The last iteration actually reads a value we don't need.
                    }
                }
                // TODO #1603 this is bad for large keys
                if iterator.key_nibbles >= path_end {
                    break;
                }
            }
            Ok(())
        }

        pub fn get_trie_nodes_for_part_old(
            &self,
            part_id: PartId,
            state_root: &StateRoot,
        ) -> Result<PartialState, StorageError> {
            assert!(self.storage.as_caching_storage().is_some());
            let root_node = self.retrieve_node(state_root)?;
            let total_size = root_node.memory_usage;
            let size_start = (total_size + part_id.total - 1) / part_id.total * part_id.idx;
            let size_end = std::cmp::min(
                (total_size + part_id.total - 1) / part_id.total * (part_id.idx + 1),
                total_size,
            );

            let with_recording = self.recording_reads();
            with_recording.visit_nodes_for_size_range_old(state_root, size_start, size_end)?;
            let recorded = with_recording.recorded_storage().unwrap();

            let trie_nodes = recorded.nodes;

            Ok(trie_nodes)
        }
    }

    #[test]
    fn test_combine_empty_trie_parts() {
        let state_root = Trie::EMPTY_ROOT;
        let _ = Trie::combine_state_parts_naive(&state_root, &vec![]).unwrap();
        let _ = Trie::validate_trie_nodes_for_part(
            &state_root,
            PartId::new(0, 1),
            PartialState(vec![]),
        )
        .unwrap();
        let _ = Trie::apply_state_part(&state_root, PartId::new(0, 1), vec![]);
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
            let value = (0..big_value_length).map(|_| rng.gen::<u8>()).collect::<Vec<_>>();
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
                            (0..small_value_length).map(|_| rng.gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                    {
                        let mut key = (0..i).map(|_| 255u8).collect::<Vec<_>>();
                        key.push(16 * 15 + x);
                        key.push(y * 16);
                        let value =
                            (0..small_value_length).map(|_| rng.gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                    {
                        let mut key = (0..i).map(|_| 255u8).collect::<Vec<_>>();
                        key.push(16 * x + 15);
                        key.push(y);
                        let value =
                            (0..small_value_length).map(|_| rng.gen::<u8>()).collect::<Vec<_>>();
                        trie_changes.push((key, Some(value)));
                    }
                }
            }
        }
        trie_changes
    }

    fn run_test_parts_not_huge<F>(gen_trie_changes: F, big_value_length: u64)
    where
        F: FnOnce(&mut ThreadRng, u64, u64) -> Vec<(Vec<u8>, Option<Vec<u8>>)>,
    {
        let mut rng = rand::thread_rng();
        let max_key_length = 50u64;
        let max_key_length_in_nibbles = max_key_length * 2;
        let max_node_serialized_size = 32 * 16 + 100; // DEVNOTE nodes can be pretty big
        let max_node_children = 16;
        let max_part_overhead =
            max_key_length_in_nibbles * max_node_serialized_size * max_node_children * 2
                + big_value_length * 2;
        println!("Max allowed overhead: {}", max_part_overhead);
        let trie_changes = gen_trie_changes(&mut rng, max_key_length, big_value_length);
        println!("Number of nodes: {}", trie_changes.len());
        let tries = create_tries();
        let trie = tries.get_trie_for_shard(ShardUId::single_shard());
        let state_root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_changes);
        let memory_size = trie.retrieve_root_node(&state_root).unwrap().memory_usage;
        println!("Total memory size: {}", memory_size);
        for num_parts in [2, 3, 5, 10, 50].iter().cloned() {
            let approximate_size_per_part = memory_size / num_parts;
            let parts = (0..num_parts)
                .map(|part_id| {
                    trie.get_trie_nodes_for_part(PartId::new(part_id, num_parts), &state_root)
                        .unwrap()
                        .0
                })
                .collect::<Vec<_>>();
            let part_nodecounts_vec = parts.iter().map(|nodes| nodes.len()).collect::<Vec<_>>();
            let sizes_vec = parts
                .iter()
                .map(|nodes| nodes.iter().map(|node| node.len()).sum::<usize>())
                .collect::<Vec<_>>();

            println!("Node counts of parts: {:?}", part_nodecounts_vec);
            println!("Sizes of parts: {:?}", sizes_vec);
            println!("Max size we allow: {}", approximate_size_per_part + max_part_overhead);
            for size in sizes_vec {
                assert!((size as u64) < approximate_size_per_part + max_part_overhead);
            }
        }
    }

    #[test]
    fn test_parts_not_huge_1() {
        run_test_parts_not_huge(construct_trie_for_big_parts_1, 100_000);
    }

    #[test]
    fn test_parts_not_huge_2() {
        run_test_parts_not_huge(construct_trie_for_big_parts_2, 100_000);
    }

    fn merge_trie_changes(changes: Vec<TrieChanges>) -> TrieChanges {
        if changes.is_empty() {
            return TrieChanges::empty(Trie::EMPTY_ROOT);
        }
        let new_root = changes[0].new_root;
        let mut map = HashMap::new();
        for changes_set in changes {
            assert!(changes_set.deletions.is_empty(), "state parts only have insertions");
            for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
                changes_set.insertions
            {
                map.entry(trie_node_or_value_hash).or_insert_with(|| (trie_node_or_value, 0)).1 +=
                    rc.get() as i32;
            }
            for TrieRefcountChange { trie_node_or_value_hash, trie_node_or_value, rc } in
                changes_set.deletions
            {
                map.entry(trie_node_or_value_hash).or_insert_with(|| (trie_node_or_value, 0)).1 -=
                    rc.get() as i32;
            }
        }
        let (insertions, deletions) = Trie::convert_to_insertions_and_deletions(map);
        TrieChanges { old_root: Default::default(), new_root, insertions, deletions }
    }

    #[test]
    fn test_combine_state_parts() {
        let mut rng = rand::thread_rng();
        for _ in 0..2000 {
            let tries = create_tries();
            let trie = tries.get_trie_for_shard(ShardUId::single_shard());
            let trie_changes = gen_changes(&mut rng, 20);
            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            let root_memory_usage = trie.retrieve_root_node(&state_root).unwrap().memory_usage;

            {
                // Test that combining all parts gets all nodes
                let num_parts = rng.gen_range(2, 10);
                let parts = (0..num_parts)
                    .map(|part_id| {
                        trie.get_trie_nodes_for_part(PartId::new(part_id, num_parts), &state_root)
                            .unwrap()
                            .0
                    })
                    .collect::<Vec<_>>();

                let trie_changes = check_combine_state_parts(&state_root, num_parts, &parts);

                let mut nodes = <HashMap<CryptoHash, Vec<u8>>>::new();
                let sizes_vec = parts
                    .iter()
                    .map(|nodes| nodes.iter().map(|node| node.len()).sum::<usize>())
                    .collect::<Vec<_>>();

                for part in parts {
                    for node in part {
                        nodes.insert(hash(&node), node);
                    }
                }
                let all_nodes = nodes.into_iter().map(|(_hash, node)| node).collect::<Vec<_>>();
                assert_eq!(all_nodes.len(), trie_changes.insertions.len());
                let size_of_all = all_nodes.iter().map(|node| node.len()).sum::<usize>();
                let num_nodes = all_nodes.len();
                Trie::validate_trie_nodes_for_part(
                    &state_root,
                    PartId::new(0, 1),
                    PartialState(all_nodes),
                )
                .expect("validate ok");

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

    fn check_combine_state_parts(
        state_root: &CryptoHash,
        num_parts: u64,
        parts: &Vec<Vec<Vec<u8>>>,
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
        assert_eq!(trie_changes, trie_changes_new);
        trie_changes
    }

    #[test]
    fn test_get_trie_nodes_for_part() {
        let mut rng = rand::thread_rng();
        for _ in 0..20 {
            let tries = create_tries();
            let trie = tries.get_trie_for_shard(ShardUId::single_shard());
            let trie_changes = gen_changes(&mut rng, 10);

            let state_root = test_populate_trie(
                &tries,
                &Trie::EMPTY_ROOT,
                ShardUId::single_shard(),
                trie_changes.clone(),
            );
            for _ in 0..10 {
                // Test that creating and validating are consistent
                let num_parts: u64 = rng.gen_range(1, 10);
                let part_id = rng.gen_range(0, num_parts);
                let trie_nodes = trie
                    .get_trie_nodes_for_part(PartId::new(part_id, num_parts), &state_root)
                    .unwrap();
                let trie_nodes2 = trie
                    .get_trie_nodes_for_part_old(PartId::new(part_id, num_parts), &state_root)
                    .unwrap();
                assert_eq!(trie_nodes, trie_nodes2);
                Trie::validate_trie_nodes_for_part(
                    &state_root,
                    PartId::new(part_id, num_parts),
                    trie_nodes,
                )
                .expect("validate ok");
            }
        }
    }
}
