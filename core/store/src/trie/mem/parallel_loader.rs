use super::arena::concurrent::{ConcurrentArena, ConcurrentArenaForThread};
use super::arena::{Arena, STArena};
use super::construction::TrieConstructor;
use super::node::{InputMemTrieNode, MemTrieNodeId};
use crate::flat::FlatStorageError;
use crate::{DBCol, NibbleSlice, RawTrieNode, RawTrieNodeWithSize, Store};
use borsh::BorshDeserialize;
use near_primitives::errors::{MissingTrieValueContext, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::FlatStateValue;
use near_primitives::types::StateRoot;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::fmt::Debug;

/// Top-level entry function to load a memtrie in parallel.
pub fn load_memtrie_in_parallel(
    store: Store,
    shard_uid: ShardUId,
    root: StateRoot,
    subtree_size: usize,
    name: String,
) -> Result<(STArena, MemTrieNodeId), StorageError> {
    let reader = ParallelMemTrieLoader::new(store, shard_uid, root, subtree_size);
    let plan = reader.make_loading_plan()?;
    println!("Loading {} subtrees in parallel", plan.subtrees_to_load.len());
    reader.load_in_parallel(plan, name)
}

/// Logic to load a memtrie in parallel. It consists of three stages:
///  - First, we use the State column to visit the trie starting from the root. We recursively
///    expand the trie until all the unexpanded subtrees are small enough
///    (memory_usage <= `subtree_size`). The trie we have expanded is represented as a "plan",
///    which is a structure similar to the trie itself.
///  - Then, we load each small subtree (the keys under which all share a common prefix) in
///    parallel, by reading the FlatState column for keys that correspond to the prefix of that
///    subtree. The result of each construction is a `MemTrieNodeId` representing the root of that
///    subtree.
///  - Finally, We construct the final trie by using the loaded subtree roots and converting the
///    plan into a complete memtrie, returning the final root.
///
/// This loader is only suitable for loading a single trie. It does not load multiple state roots,
/// or multiple shards.
pub struct ParallelMemTrieLoader {
    store: Store,
    shard_uid: ShardUId,
    root: StateRoot,
    subtree_size: usize,
}

impl ParallelMemTrieLoader {
    pub fn new(store: Store, shard_uid: ShardUId, root: StateRoot, subtree_size: usize) -> Self {
        Self { store, shard_uid, root, subtree_size }
    }

    /// Implements stage 1; recursively expanding the trie until all subtrees are small enough.
    fn make_loading_plan(&self) -> Result<PartialTrieLoadingPlan, StorageError> {
        let mut specs = Vec::new();
        let root = self.make_loading_plan_recursive(self.root, NibblePrefix::new(), &mut specs)?;
        Ok(PartialTrieLoadingPlan { root, subtrees_to_load: specs })
    }

    /// Helper function to implement stage 1, visiting a single node identified by this hash,
    /// whose prefix is the given prefix. While expanding this node, any small subtrees
    /// encountered are appended to the `subtrees_to_load` array.
    fn make_loading_plan_recursive(
        &self,
        hash: CryptoHash,
        mut prefix: NibblePrefix,
        subtrees_to_load: &mut Vec<NibblePrefix>,
    ) -> Result<TrieLoadingPlanNode, StorageError> {
        // Read the node from the State column.
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&self.shard_uid.to_bytes());
        key[8..40].copy_from_slice(&hash.0);
        let node = RawTrieNodeWithSize::try_from_slice(
            &self
                .store
                .get(DBCol::State, &key)
                .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                .ok_or(StorageError::MissingTrieValue(MissingTrieValueContext::TrieStorage, hash))?
                .as_slice(),
        )
        .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?;

        // If subtree is small enough, add it to the list of subtrees to load, and we're done.
        if node.memory_usage <= self.subtree_size as u64 {
            subtrees_to_load.push(prefix);
            return Ok(TrieLoadingPlanNode::Load { subtree_id: subtrees_to_load.len() - 1 });
        }

        match node.node {
            RawTrieNode::Leaf(extension, value_ref) => {
                // If we happen to visit a leaf, we'll have to just read the leaf's value. This is
                // almost like a corner case because we're not really interested in values here
                // (that's the job of the parallel loading part), but if we do get here, we have to
                // deal with it.
                key[8..40].copy_from_slice(&value_ref.hash.0);
                let value = self
                    .store
                    .get(DBCol::State, &key)
                    .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                    .ok_or(StorageError::MissingTrieValue(
                        MissingTrieValueContext::TrieStorage,
                        hash,
                    ))?;
                let flat_value = FlatStateValue::on_disk(&value);
                Ok(TrieLoadingPlanNode::Leaf {
                    extension: extension.into_boxed_slice(),
                    value: flat_value,
                })
            }
            RawTrieNode::BranchNoValue(children_hashes) => {
                // If we visit a branch, recursively visit all children.
                let mut children = Vec::new();
                for i in 0..16 {
                    if let Some(child_hash) = children_hashes[i] {
                        let mut prefix = prefix.clone();
                        prefix.push(i as u8);
                        let child =
                            self.make_loading_plan_recursive(child_hash, prefix, subtrees_to_load)?;
                        children.push((i as u8, Box::new(child)));
                    }
                }
                Ok(TrieLoadingPlanNode::Branch { children, value: None })
            }
            RawTrieNode::BranchWithValue(value_ref, children_hashes) => {
                // Similar here, except we have to also look up the value.
                key[8..40].copy_from_slice(&value_ref.hash.0);
                let value = self
                    .store
                    .get(DBCol::State, &key)
                    .map_err(|e| StorageError::StorageInconsistentState(e.to_string()))?
                    .ok_or(StorageError::MissingTrieValue(
                        MissingTrieValueContext::TrieStorage,
                        hash,
                    ))?;
                let flat_value = FlatStateValue::on_disk(&value);

                let mut children = Vec::new();
                for i in 0..16 {
                    if let Some(child_hash) = children_hashes[i] {
                        let mut prefix = prefix.clone();
                        prefix.push(i as u8);
                        let child =
                            self.make_loading_plan_recursive(child_hash, prefix, subtrees_to_load)?;
                        children.push((i as u8, Box::new(child)));
                    }
                }
                Ok(TrieLoadingPlanNode::Branch { children, value: Some(flat_value) })
            }
            RawTrieNode::Extension(extension, child) => {
                let nibbles = NibbleSlice::from_encoded(&extension).0;
                prefix.append(&nibbles);
                let child = self.make_loading_plan_recursive(child, prefix, subtrees_to_load)?;
                Ok(TrieLoadingPlanNode::Extension {
                    extension: extension.into_boxed_slice(),
                    child: Box::new(child),
                })
            }
        }
    }

    /// This implements the loading of each subtree in stage 2.
    fn load_one_subtree(
        &self,
        subtree_to_load: &NibblePrefix,
        arena: &mut impl Arena,
    ) -> Result<MemTrieNodeId, StorageError> {
        // Figure out which range corresponds to the prefix of this subtree.
        let (start, end) = subtree_to_load.to_iter_range(self.shard_uid);

        // Load all the keys in this range from the FlatState column.
        let mut recon = TrieConstructor::new(arena);
        for item in self.store.iter_range(DBCol::FlatState, Some(&start), Some(&end)) {
            let (key, value) = item.map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "Error iterating over FlatState: {err}"
                ))
            })?;
            // Since we're constructing a subtree under a prefix, ignore the prefix part of the key.
            // (The first 8 bytes of the key is the ShardUId).
            let key = NibbleSlice::new(&key[8..]).mid(subtree_to_load.prefix.len());
            let value = FlatStateValue::try_from_slice(&value).map_err(|err| {
                FlatStorageError::StorageInternalError(format!(
                    "invalid FlatState value format: {err}"
                ))
            })?;
            recon.add_leaf(key, value);
        }
        Ok(recon.finalize().unwrap())
    }

    /// This implements stage 2 and 3, loading the subtrees in parallel an then constructing the
    /// final trie.
    fn load_in_parallel(
        &self,
        plan: PartialTrieLoadingPlan,
        name: String,
    ) -> Result<(STArena, MemTrieNodeId), StorageError> {
        let arena = ConcurrentArena::new();

        // A bit of an awkward Rayon dance. We run a multi-threaded fold; the fold state contains
        // both a sparse vector of the loading results as well as the arena used for the thread.
        // We need to collect both in the end, so fold is the only suitable method.
        let (roots, threads): (
            Vec<Vec<Result<(usize, MemTrieNodeId), StorageError>>>,
            Vec<ConcurrentArenaForThread>,
        ) = plan
            .subtrees_to_load
            .into_par_iter()
            .enumerate()
            .fold(|| -> (Vec<Result<(usize, MemTrieNodeId), StorageError>>, ConcurrentArenaForThread) {
                (Vec::new(), arena.for_thread())
            }, |(mut roots, mut arena), (i, prefix)| {
                roots.push(self.load_one_subtree(&prefix, &mut arena).map(|root| (i, root)));
                (roots, arena)
            })
            .unzip();

        let mut roots = roots.into_iter().flatten().collect::<Result<Vec<_>, _>>()?;
        roots.sort_by_key(|(i, _)| *i);
        let roots = roots.into_iter().map(|(_, root)| root).collect::<Vec<_>>();

        let mut arena = arena.to_single_threaded(name, threads);
        let root = plan.root.to_node(&mut arena, &roots);
        Ok((arena, root))
    }
}

/// Specifies exactly what to do to create a node in the final trie.
#[derive(Debug)]
enum TrieLoadingPlanNode {
    // The first three cases correspond exactly to the trie structure.
    Branch { children: Vec<(u8, Box<TrieLoadingPlanNode>)>, value: Option<FlatStateValue> },
    Extension { extension: Box<[u8]>, child: Box<TrieLoadingPlanNode> },
    Leaf { extension: Box<[u8]>, value: FlatStateValue },
    // This means this trie node is whatever loading this subtree yields.
    Load { subtree_id: usize },
}

impl TrieLoadingPlanNode {
    /// This implements the construction part of stage 3, where we convert a plan node to
    /// a memtrie node. The `subtree_roots` is the parallel loading results.
    fn to_node(self, arena: &mut impl Arena, subtree_roots: &[MemTrieNodeId]) -> MemTrieNodeId {
        match self {
            TrieLoadingPlanNode::Branch { children, value } => {
                let mut res_children = [None; 16];
                for (nibble, child) in children {
                    res_children[nibble as usize] = Some(child.to_node(arena, subtree_roots));
                }
                let input = match &value {
                    Some(value) => {
                        InputMemTrieNode::BranchWithValue { children: res_children, value }
                    }
                    None => InputMemTrieNode::Branch { children: res_children },
                };
                MemTrieNodeId::new(arena, input)
            }
            TrieLoadingPlanNode::Extension { extension, child } => {
                let child = child.to_node(arena, subtree_roots);
                let input = InputMemTrieNode::Extension { extension: &extension, child };
                MemTrieNodeId::new(arena, input)
            }
            TrieLoadingPlanNode::Leaf { extension, value } => {
                let input = InputMemTrieNode::Leaf { extension: &extension, value: &value };
                MemTrieNodeId::new(arena, input)
            }
            TrieLoadingPlanNode::Load { subtree_id: spec_id } => subtree_roots[spec_id],
        }
    }
}

#[derive(Debug)]
struct PartialTrieLoadingPlan {
    root: TrieLoadingPlanNode,
    subtrees_to_load: Vec<NibblePrefix>,
}

/// Represents a prefix of nibbles. Allows appending to the prefix, and implements logic of
/// calculating a range of keys that correspond to this prefix.
///
/// A nibble just means a 4 bit number.
#[derive(Clone)]
struct NibblePrefix {
    /// Big endian encoding of the nibbles. If there are an odd number of nibbles, this is
    /// the encoding of the nibbles as if there were one more nibble at the end being zero.
    prefix: Vec<u8>,
    /// Whether the last byte of `prefix` represents one nibble rather than two.
    odd: bool,
}

impl Debug for NibblePrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.odd {
            write!(
                f,
                "{}{:x}",
                hex::encode(&self.prefix[..self.prefix.len() - 1]),
                self.prefix.last().unwrap() >> 4
            )
        } else {
            write!(f, "{}", hex::encode(&self.prefix))
        }
    }
}

impl NibblePrefix {
    pub fn new() -> Self {
        Self { prefix: Vec::new(), odd: false }
    }

    pub fn push(&mut self, nibble: u8) {
        debug_assert!(nibble < 16, "nibble must be less than 16");
        if self.odd {
            *self.prefix.last_mut().unwrap() |= nibble;
        } else {
            self.prefix.push(nibble << 4);
        }
        self.odd = !self.odd;
    }

    pub fn append(&mut self, nibbles: &NibbleSlice) {
        for nibble in nibbles.iter() {
            self.push(nibble);
        }
    }

    /// Converts the nibble prefix to an equivalent range of keys.
    ///
    /// If the number of nibbles is even, this is straight-forward; the keys will be in the form of
    /// e.g. 0x123456 - 0x123457. If the number of nibbles is odd, the keys will cover the whole
    /// range for the last 4 bits, e.g. 0x123450 - 0x123460.
    pub fn to_iter_range(&self, shard_uid: ShardUId) -> (Vec<u8>, Vec<u8>) {
        let start = shard_uid
            .to_bytes()
            .into_iter()
            .chain(self.prefix.clone().into_iter())
            .collect::<Vec<u8>>();
        // The end key should always exist because we have a shard UID prefix to absorb the overflow.
        let end = increment_vec_as_num(&start, if self.odd { 16 } else { 1 })
            .expect("Should not overflow");
        (start, end)
    }
}

/// Generic function to add the given increment to the last byte of the vector, pretending that
/// the vector is a big-endian encoding of a big number. If the increment causes an overflow within
/// the given number of bytes, return None.
fn increment_vec_as_num(orig: &Vec<u8>, by: u8) -> Option<Vec<u8>> {
    let mut v = orig.clone();
    let mut carry = by;
    for i in (0..v.len()).rev() {
        let (new_val, new_carry) = v[i].overflowing_add(carry);
        v[i] = new_val;
        if new_carry {
            carry = 1;
        } else {
            carry = 0;
            break;
        }
    }
    if carry != 0 {
        None
    } else {
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::NibblePrefix;
    use crate::trie::mem::parallel_loader::increment_vec_as_num;
    use crate::NibbleSlice;
    use near_primitives::shard_layout::ShardUId;

    #[test]
    fn test_increment_vec_as_num() {
        assert_eq!(increment_vec_as_num(&vec![0, 0, 0], 1), Some(vec![0, 0, 1]));
        assert_eq!(increment_vec_as_num(&vec![0, 0, 255], 1), Some(vec![0, 1, 0]));
        assert_eq!(increment_vec_as_num(&vec![0, 5, 255], 1), Some(vec![0, 6, 0]));
        assert_eq!(increment_vec_as_num(&vec![0, 255, 255], 1), Some(vec![1, 0, 0]));
        assert_eq!(increment_vec_as_num(&vec![255, 255, 254], 2), None);
    }

    #[test]
    fn test_nibble_prefix() {
        let shard_uid = ShardUId { shard_id: 3, version: 2 };
        let iter_range = |prefix: &NibblePrefix| {
            let (start, end) = prefix.to_iter_range(shard_uid);
            format!("{}..{}", hex::encode(&start), hex::encode(&end))
        };

        let mut prefix = NibblePrefix::new();
        assert_eq!(format!("{:?}", prefix), "");
        assert_eq!(iter_range(&prefix), "0200000003000000..0200000003000001");

        prefix.push(4);
        assert_eq!(format!("{:?}", prefix), "4");
        assert_eq!(iter_range(&prefix), "020000000300000040..020000000300000050");

        prefix.push(15);
        assert_eq!(format!("{:?}", prefix), "4f");
        assert_eq!(iter_range(&prefix), "02000000030000004f..020000000300000050");

        prefix.append(&NibbleSlice::new(&hex::decode("5123").unwrap()).mid(1));
        assert_eq!(format!("{:?}", prefix), "4f123");
        assert_eq!(iter_range(&prefix), "02000000030000004f1230..02000000030000004f1240");

        prefix.append(&NibbleSlice::new(&hex::decode("ff").unwrap()));
        assert_eq!(format!("{:?}", prefix), "4f123ff");
        assert_eq!(iter_range(&prefix), "02000000030000004f123ff0..02000000030000004f124000");

        let mut prefix = NibblePrefix::new();
        prefix.push(15);
        prefix.push(15);
        assert_eq!(format!("{:?}", prefix), "ff");
        assert_eq!(iter_range(&prefix), "0200000003000000ff..020000000300000100");
    }
}
