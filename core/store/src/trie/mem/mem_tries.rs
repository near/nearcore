use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, StateRoot};

use crate::trie::mem::arena::ArenaMut;
use crate::trie::mem::metrics::MEM_TRIE_NUM_ROOTS;
use crate::trie::MemTrieChanges;
use crate::Trie;

use super::arena::single_thread::{STArena, STArenaMemory};
use super::arena::Arena;
use super::flexible_data::value::ValueView;
use super::iter::STMemTrieIterator;
use super::lookup::memtrie_lookup;
use super::node::{MemTrieNodeId, MemTrieNodePtr};
use super::updating::{construct_root_from_changes, MemTrieUpdate};

/// `MemTries` (logically) owns the memory of multiple tries.
/// Tries may share nodes with each other via refcounting. The way the
/// refcounting works is very similar to as if each node held a Rc of
/// its children nodes. The `roots` field of this struct logically
/// holds an Rc of the root of each trie.
pub struct MemTries {
    arena: STArena,
    /// Maps a state root to a list of nodes that have the same root hash.
    /// The reason why this is a list is because we do not have a node
    /// deduplication mechanism so we can't guarantee that nodes of the
    /// same hash are unique. During lookup, any of these nodes can be provided
    /// as they all logically represent the same trie.
    roots: HashMap<StateRoot, Vec<MemTrieNodeId>>,
    /// Maps a block height to a list of state roots present at that height.
    /// This is used for GC. The invariant is that for any state root, the
    /// number of times the state root appears in this map is equal to the
    /// sum of the refcounts of each `MemTrieNodeId`s in `roots[state hash]`.
    heights: BTreeMap<BlockHeight, Vec<StateRoot>>,
    /// Shard UID, for exporting metrics only.
    shard_uid: ShardUId,
}

impl MemTries {
    pub fn new(shard_uid: ShardUId) -> Self {
        Self {
            arena: STArena::new(shard_uid.to_string()),
            roots: HashMap::new(),
            heights: Default::default(),
            shard_uid,
        }
    }

    pub fn new_from_arena_and_root(
        shard_uid: ShardUId,
        block_height: BlockHeight,
        arena: STArena,
        root: MemTrieNodeId,
    ) -> Self {
        let mut tries =
            Self { arena, roots: HashMap::new(), heights: Default::default(), shard_uid };
        tries.insert_root(root.as_ptr(tries.arena.memory()).view().node_hash(), root, block_height);
        tries
    }

    /// This function should perform the entire construction of the new trie, possibly based on some existing
    /// trie nodes. This internally takes care of refcounting and inserts a new root into the memtrie.
    pub fn apply_memtrie_changes(
        &mut self,
        block_height: BlockHeight,
        changes: &MemTrieChanges,
    ) -> CryptoHash {
        if let Some(root) = construct_root_from_changes(&mut self.arena, changes) {
            let state_root = root.as_ptr(self.arena.memory()).view().node_hash();
            self.insert_root(state_root, root, block_height);
            state_root
        } else {
            CryptoHash::default()
        }
    }

    fn insert_root(
        &mut self,
        state_root: StateRoot,
        mem_root: MemTrieNodeId,
        block_height: BlockHeight,
    ) {
        assert_ne!(state_root, CryptoHash::default());
        let heights = self.heights.entry(block_height).or_default();
        heights.push(state_root);
        let new_ref = mem_root.add_ref(self.arena.memory_mut());
        if new_ref == 1 {
            self.roots.entry(state_root).or_default().push(mem_root);
        }
        MEM_TRIE_NUM_ROOTS
            .with_label_values(&[&self.shard_uid.to_string()])
            .set(self.roots.len() as i64);
    }

    /// Returns the root node corresponding to the given state root.
    pub(super) fn get_root(
        &self,
        state_root: &CryptoHash,
    ) -> Result<MemTrieNodePtr<STArenaMemory>, StorageError> {
        assert_ne!(state_root, &CryptoHash::default());
        self.roots.get(state_root).map(|ids| ids[0].as_ptr(self.arena.memory())).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Failed to find root node {:?} in memtrie",
                state_root
            ))
        })
    }

    /// Expires all trie roots corresponding to a height smaller than
    /// `block_height`. This internally manages refcounts. If a trie root
    /// is expired but is still used at a higher height, it will still be
    /// valid until all references to that root expires.
    pub fn delete_until_height(&mut self, block_height: BlockHeight) {
        let mut to_delete = vec![];
        self.heights.retain(|height, state_roots| {
            if *height < block_height {
                for state_root in state_roots {
                    to_delete.push(*state_root)
                }
                false
            } else {
                true
            }
        });
        for state_root in to_delete {
            self.delete_root(&state_root);
        }
    }

    fn delete_root(&mut self, state_root: &CryptoHash) {
        if let Some(ids) = self.roots.get_mut(state_root) {
            let last_id = ids.last().unwrap();
            let new_ref = last_id.remove_ref(&mut self.arena);
            if new_ref == 0 {
                ids.pop();
                if ids.is_empty() {
                    self.roots.remove(state_root);
                }
            }
        } else {
            debug_assert!(false, "Deleting non-existent root: {}", state_root);
        }
        MEM_TRIE_NUM_ROOTS
            .with_label_values(&[&self.shard_uid.to_string()])
            .set(self.roots.len() as i64);
    }

    pub fn update(
        &self,
        root: CryptoHash,
        track_trie_changes: bool,
    ) -> Result<MemTrieUpdate<STArenaMemory>, StorageError> {
        let root_id =
            if root == CryptoHash::default() { None } else { Some(self.get_root(&root)?.id()) };
        Ok(MemTrieUpdate::new(
            root_id,
            &self.arena.memory(),
            self.shard_uid.to_string(),
            track_trie_changes,
        ))
    }

    /// Returns an iterator over the memtrie for the given trie root.
    pub fn get_iter<'a>(&'a self, trie: &'a Trie) -> Result<STMemTrieIterator, StorageError> {
        let root = if trie.root == CryptoHash::default() {
            None
        } else {
            Some(self.get_root(&trie.root)?)
        };
        Ok(STMemTrieIterator::new(root, trie))
    }

    /// Looks up a key in the memtrie with the given state_root and returns the value if found.
    /// Additionally, it returns a list of nodes that were accessed during the lookup.
    pub fn lookup(
        &self,
        state_root: &CryptoHash,
        key: &[u8],
        nodes_accessed: Option<&mut Vec<(CryptoHash, Arc<[u8]>)>>,
    ) -> Result<Option<ValueView>, StorageError> {
        let root = self.get_root(state_root)?;
        Ok(memtrie_lookup(root, key, nodes_accessed))
    }

    #[cfg(test)]
    pub fn arena(&self) -> &STArena {
        &self.arena
    }

    /// Used for unit testing and integration testing.
    pub fn num_roots(&self) -> usize {
        self.heights.iter().map(|(_, v)| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::MemTries;
    use crate::trie::mem::arena::Arena;
    use crate::trie::mem::node::{InputMemTrieNode, MemTrieNodeId};
    use crate::NibbleSlice;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;
    use near_primitives::types::BlockHeight;
    use rand::seq::SliceRandom;
    use rand::Rng;

    #[test]
    fn test_refcount() {
        // Here we test multiple cases:
        //  - Each height possibly having multiple state roots (due to forks)
        //    (and possibly with the same state roots)
        //  - Each state root possibly having multiple actual nodes that have
        //    the same hash (as we don't deduplicate in general)
        //  - A state root being possibly the same as another of a different
        //    height.
        //
        // And we make sure that the GC refcounting works correctly.
        let mut tries = MemTries::new(ShardUId::single_shard());
        let mut available_hashes: Vec<(BlockHeight, CryptoHash)> = Vec::new();
        for height in 100..=200 {
            let num_roots_at_height = rand::thread_rng().gen_range(1..=4);
            for _ in 0..num_roots_at_height {
                match rand::thread_rng().gen_range(0..4) {
                    0 if !available_hashes.is_empty() => {
                        // Reuse an existing root 25% of the time.
                        let (_, root) = available_hashes.choose(&mut rand::thread_rng()).unwrap();
                        let root = tries.get_root(root).unwrap().id();
                        let state_root = root.as_ptr(tries.arena.memory()).view().node_hash();
                        tries.insert_root(state_root, root, height);
                        available_hashes.push((height, state_root));
                    }
                    _ => {
                        // Construct a new root 75% of the time.
                        let root = MemTrieNodeId::new(
                            &mut tries.arena,
                            InputMemTrieNode::Leaf {
                                value: &FlatStateValue::Inlined(format!("{}", height).into_bytes()),
                                extension: &NibbleSlice::new(&[]).encoded(true),
                            },
                        );
                        let state_root = root.as_ptr(tries.arena.memory()).view().node_hash();
                        tries.insert_root(state_root, root, height);
                        available_hashes.push((height, state_root));
                    }
                }
            }
            // Expire some roots.
            tries.delete_until_height(height - 20);
            available_hashes.retain(|(h, _)| *h >= height - 20);
            // Sanity check that the roots that are supposed to exist still exist.
            for (_, state_root) in &available_hashes {
                let root = tries.get_root(state_root).unwrap().id();
                assert_eq!(root.as_ptr(tries.arena.memory()).view().node_hash(), *state_root);
            }
        }
        // Expire all roots, and now the number of allocs should be zero.
        tries.delete_until_height(201);
        assert_eq!(tries.arena.num_active_allocs(), 0);
        assert_eq!(tries.num_roots(), 0);
    }
}
