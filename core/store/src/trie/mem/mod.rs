use self::arena::{Arena, STArena, STArenaMemory};
use self::metrics::MEM_TRIE_NUM_ROOTS;
use self::node::{MemTrieNodeId, MemTrieNodePtr};
use self::updating::MemTrieUpdate;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockHeight, StateRoot};
use std::collections::{BTreeMap, HashMap};

mod arena;
mod construction;
pub(crate) mod flexible_data;
mod freelist;
pub mod iter;
pub mod loading;
pub mod lookup;
pub mod metrics;
pub mod node;
pub mod parallel_loader;
pub mod updating;

/// Check this, because in the code we conveniently assume usize is 8 bytes.
/// In-memory trie can't possibly work under 32-bit anyway.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("In-memory trie requires a 64 bit platform");

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

    /// Inserts a new root into the trie. The given function should perform
    /// the entire construction of the new trie, possibly based on some existing
    /// trie nodes. This internally takes care of refcounting.
    pub fn construct_root<Error>(
        &mut self,
        block_height: BlockHeight,
        f: impl FnOnce(&mut STArena) -> Result<Option<MemTrieNodeId>, Error>,
    ) -> Result<CryptoHash, Error> {
        let root = f(&mut self.arena)?;
        if let Some(root) = root {
            let state_root = root.as_ptr(self.arena.memory()).view().node_hash();
            self.insert_root(state_root, root, block_height);
            Ok(state_root)
        } else {
            Ok(CryptoHash::default())
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

    /// Returns a root node corresponding to the given state root.
    pub fn get_root(&self, state_root: &CryptoHash) -> Option<MemTrieNodePtr<STArenaMemory>> {
        assert_ne!(state_root, &CryptoHash::default());
        self.roots.get(state_root).map(|ids| ids[0].as_ptr(self.arena.memory()))
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

    /// Used for unit testing and integration testing.
    pub fn num_roots(&self) -> usize {
        self.heights.iter().map(|(_, v)| v.len()).sum()
    }

    pub fn update(
        &self,
        root: CryptoHash,
        track_trie_changes: bool,
    ) -> Result<MemTrieUpdate, StorageError> {
        let root_id = if root == CryptoHash::default() {
            None
        } else {
            let root_id = self
                .get_root(&root)
                .ok_or_else(|| {
                    StorageError::StorageInconsistentState(format!(
                        "Failed to find root node {:?} in memtrie",
                        root
                    ))
                })?
                .id();
            Some(root_id)
        };
        Ok(MemTrieUpdate::new(
            root_id,
            &self.arena.memory(),
            self.shard_uid.to_string(),
            track_trie_changes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::node::{InputMemTrieNode, MemTrieNodeId};
    use super::MemTries;
    use crate::trie::mem::arena::Arena;
    use crate::NibbleSlice;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardUId;
    use near_primitives::state::FlatStateValue;
    use near_primitives::types::BlockHeight;
    use rand::seq::SliceRandom;
    use rand::Rng;

    #[test]
    fn test_construct_empty_trie() {
        let mut tries = MemTries::new(ShardUId::single_shard());
        let state_root =
            tries.construct_root(123, |_| -> Result<Option<MemTrieNodeId>, ()> { Ok(None) });
        assert_eq!(state_root, Ok(CryptoHash::default()));
        assert_eq!(tries.num_roots(), 0);
    }

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
                        let state_root = tries
                            .construct_root(height, |_| -> Result<Option<MemTrieNodeId>, ()> {
                                Ok(Some(root))
                            });
                        available_hashes.push((height, state_root.unwrap()));
                    }
                    _ => {
                        // Construct a new root 75% of the time.
                        let state_root = tries
                            .construct_root(height, |arena| -> Result<Option<MemTrieNodeId>, ()> {
                                let root = MemTrieNodeId::new(
                                    arena,
                                    InputMemTrieNode::Leaf {
                                        value: &FlatStateValue::Inlined(
                                            format!("{}", height).into_bytes(),
                                        ),
                                        extension: &NibbleSlice::new(&[]).encoded(true),
                                    },
                                );
                                Ok(Some(root))
                            })
                            .unwrap();
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
