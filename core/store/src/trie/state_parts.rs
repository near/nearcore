use std::cmp::min;
use std::collections::HashMap;

use near_primitives::challenge::PartialStateStruct;
use near_primitives::hash::CryptoHash;
use near_primitives::types::StateRoot;

use crate::trie::iterator::CrumbStatus;
use crate::trie::nibble_slice::NibbleSlice;
use crate::trie::{NodeHandle, TrieNode, TrieNodeWithSize, POISONED_LOCK_ERR};
use crate::{PartialStorage, StorageError, Trie, TrieChanges, TrieIterator};

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
        part_id: u64,
        num_parts: u64,
        state_root: &StateRoot,
    ) -> Result<PartialStateStruct, StorageError> {
        assert!(part_id < num_parts);
        assert!(self.storage.as_caching_storage().is_some());
        let root_node = self.retrieve_node(&state_root)?;
        let total_size = root_node.memory_usage;
        let size_start = (total_size + num_parts - 1) / num_parts * part_id;
        let size_end = min((total_size + num_parts - 1) / num_parts * (part_id + 1), total_size);

        let with_recording = self.recording_reads();
        with_recording.visit_nodes_for_size_range(&state_root, size_start, size_end)?;
        let recorded = with_recording.recorded_storage().unwrap();

        let trie_nodes = recorded.nodes;

        Ok(PartialStateStruct(trie_nodes))
    }

    /// Assume we lay out all trie nodes in dfs order visiting children before the parent.
    /// We take all node sizes (memory_usage_direct()) and take all nodes intersecting with
    /// [size_start, size_end) interval, also all nodes necessary to prove it and some
    /// additional nodes defined by the current implementation (TODO #1603 strict spec).
    ///
    /// Creating a StatePart takes all these nodes, validating a StatePart checks that it has the
    /// right set of nodes.
    fn visit_nodes_for_size_range(
        &self,
        root_hash: &CryptoHash,
        size_start: u64,
        size_end: u64,
    ) -> Result<(), StorageError> {
        let root_node = self.retrieve_node(&root_hash)?;
        let path_begin = self.find_path(&root_node, size_start)?;
        let mut path_end = self.find_path(&root_node, size_end)?;
        // We want to include up to everything that starts with path_end, for simplicity we append
        // a large character to the end and use (key_nibbles > path_end) as stopping condition
        path_end.push(0x10);

        let mut iterator = TrieIterator::new(&self, root_hash)?;
        let path_begin = NibbleSlice::encode_nibbles(&path_begin, false);
        iterator.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin[..]).0)?;
        loop {
            match iterator.next() {
                None => break,
                Some(Err(e)) => {
                    return Err(e);
                }
                Some(Ok(_)) => {}
            }
            // TODO #1603 this is bad for large keys
            if iterator.key_nibbles > path_end {
                break;
            }
        }
        Ok(())
    }

    fn find_child(
        &self,
        size_start: u64,
        node: &mut TrieNodeWithSize,
        size_skipped: &mut u64,
        key_nibbles: &mut Vec<u8>,
    ) -> Result<bool, StorageError> {
        match &node.node {
            TrieNode::Empty => Ok(false),
            TrieNode::Leaf(key, _value) => {
                let (slice, _is_leaf) = NibbleSlice::from_encoded(key);
                key_nibbles.extend(slice.iter());
                Ok(false)
            }
            TrieNode::Branch(children, _value) => {
                let mut skipped_children = 0u64;
                for child_index in 0..children.len() {
                    let child = match &children[child_index] {
                        None => {
                            continue;
                        }
                        Some(NodeHandle::InMemory(_)) => {
                            unreachable!("only possible while mutating")
                        }
                        Some(NodeHandle::Hash(h)) => self.retrieve_node(&h)?,
                    };
                    if *size_skipped + skipped_children + child.memory_usage <= size_start {
                        skipped_children += child.memory_usage;
                        continue;
                    } else {
                        key_nibbles.push(child_index as u8);
                        *node = child;
                        *size_skipped += skipped_children;
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            TrieNode::Extension(key, child_handle) => {
                let child = match child_handle {
                    NodeHandle::InMemory(_) => unreachable!("only possible while mutating"),
                    NodeHandle::Hash(h) => self.retrieve_node(&h)?,
                };
                let (slice, _is_leaf) = NibbleSlice::from_encoded(key);
                key_nibbles.extend(slice.iter());
                *node = child;
                Ok(true)
            }
        }
    }

    fn find_path(&self, root_node: &TrieNodeWithSize, size: u64) -> Result<Vec<u8>, StorageError> {
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
        part_id: u64,
        num_parts: u64,
        trie_nodes: &PartialStateStruct,
    ) -> Result<(), StorageError> {
        let trie_nodes = trie_nodes.clone().0;
        assert!(part_id < num_parts);
        let trie = Trie::from_recorded_storage(PartialStorage { nodes: trie_nodes.to_vec() });

        let root_node = trie.retrieve_node(&state_root)?;
        let total_size = root_node.memory_usage;
        let size_start = (total_size + num_parts - 1) / num_parts * part_id;
        let size_end = min((total_size + num_parts - 1) / num_parts * (part_id + 1), total_size);

        trie.visit_nodes_for_size_range(&state_root, size_start, size_end)?;
        let storage = trie.storage.as_partial_storage().unwrap();

        if storage.visited_nodes.lock().expect(POISONED_LOCK_ERR).len() != trie_nodes.len() {
            // TODO #1603 not actually TrieNodeMissing.
            // The error is that the proof has more nodes than needed.
            return Err(StorageError::TrieNodeMissing);
        }
        Ok(())
    }

    fn traverse_all_nodes<F: FnMut(&CryptoHash) -> Result<(), StorageError>>(
        &self,
        root: &CryptoHash,
        mut on_enter: F,
    ) -> Result<(), StorageError> {
        if root == &CryptoHash::default() {
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
                TrieNode::Leaf(_, _) => {
                    continue;
                }
                TrieNode::Branch(children, _value) => match position {
                    CrumbStatus::Entering => {
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
                            NodeHandle::InMemory(_) => unreachable!("only possible while mutating"),
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

    /// Combines all parts and returns TrieChanges that can be applied to storage.
    ///
    /// # Input
    /// parts[i] has trie nodes for part i
    ///
    /// # Errors
    /// StorageError if data is inconsistent. Should never happen if each part was validated.
    pub fn combine_state_parts(
        state_root: &StateRoot,
        parts: &Vec<Vec<Vec<u8>>>,
    ) -> Result<TrieChanges, StorageError> {
        let nodes = parts
            .iter()
            .map(|part| part.iter())
            .flatten()
            .map(|data| data.to_vec())
            .collect::<Vec<_>>();
        let trie = Trie::from_recorded_storage(PartialStorage { nodes });
        let mut insertions = <HashMap<CryptoHash, (Vec<u8>, u32)>>::new();
        trie.traverse_all_nodes(&state_root, |hash| {
            if let Some((_bytes, rc)) = insertions.get_mut(hash) {
                *rc += 1;
            } else {
                let bytes = trie.storage.retrieve_raw_bytes(hash)?;
                insertions.insert(*hash, (bytes, 1));
            }
            Ok(())
        })?;
        let mut insertions =
            insertions.into_iter().map(|(k, (v, rc))| (k, v, rc)).collect::<Vec<_>>();
        insertions.sort();
        Ok(TrieChanges {
            old_root: Default::default(),
            new_root: *state_root,
            insertions,
            deletions: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::Rng;

    use near_primitives::hash::{hash, CryptoHash};

    use crate::test_utils::create_trie;
    use crate::trie::tests::gen_changes;

    use super::*;

    #[test]
    fn test_combine_empty_trie_parts() {
        let state_root = StateRoot::default();
        let _ = Trie::combine_state_parts(&state_root, &vec![]).unwrap();
    }

    #[test]
    fn test_parts() {
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let trie = create_trie();
            let trie_changes = gen_changes(&mut rng, 500);

            let (store_update, state_root) = trie
                .update(&Trie::empty_root(), trie_changes.iter().cloned())
                .unwrap()
                .into(trie.clone())
                .unwrap();
            store_update.commit().ok();
            for _ in 0..100 {
                // Test that creating and validating are consistent
                let num_parts = rng.gen_range(1, 10);
                let part_id = rng.gen_range(0, num_parts);
                let trie_nodes =
                    trie.get_trie_nodes_for_part(part_id, num_parts, &state_root).unwrap();
                Trie::validate_trie_nodes_for_part(&state_root, part_id, num_parts, &trie_nodes)
                    .expect("validate ok");
            }

            {
                // Test that combining all parts gets all nodes
                let num_parts = rng.gen_range(2, 10);
                let parts = (0..num_parts)
                    .map(|part_id| {
                        trie.get_trie_nodes_for_part(part_id, num_parts, &state_root).unwrap().0
                    })
                    .collect::<Vec<_>>();

                let trie_changes = Trie::combine_state_parts(&state_root, &parts).unwrap();
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
                Trie::validate_trie_nodes_for_part(
                    &state_root,
                    0,
                    1,
                    &PartialStateStruct(all_nodes.clone()),
                )
                .expect("validate ok");

                let sum_of_sizes = sizes_vec.iter().sum::<usize>();
                // Manually check that sizes are reasonable
                println!("------------------------------");
                println!("Number of nodes: {:?}", all_nodes.len());
                println!("Sizes of parts: {:?}", sizes_vec);
                println!("All nodes size: {:?}, sum_of_sizes: {:?}", size_of_all, sum_of_sizes);
            }
        }
    }
}
